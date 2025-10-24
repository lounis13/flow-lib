from __future__ import annotations

import asyncio
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Dict, Any, List, Optional

from app.infra.flow.models import WorkflowRun, State, TaskInstance, Store


class Context:
    def __init__(self, run: WorkflowRun, task: TaskInstance):
        self.run = run
        self.task = task
        self._pushed: Optional[dict] = None

    @property
    def params(self): return self.run.params

    @property
    def task_id(self): return self.task.task_id

    @property
    def try_number(self): return self.task.try_number

    def pull(self, task_id: str):
        t = self.run.tasks.get(task_id)
        return t.output_json if t and t.state == State.SUCCESS else None

    def push(self, value: Any): self._pushed = value

    def _get_pushed(self): return self._pushed

    def log(self, msg: str):
        now = datetime.now().isoformat(timespec="seconds")
        print(f"[{now}] [{self.run.id}::{self.task.task_id}#{self.task.try_number}] {msg}")


@dataclass
class TaskDef:
    func: Callable[[Context], Any]
    depends_on: List[str] = field(default_factory=list)
    retries: int = 0
    retry_delay: Optional[timedelta] = None


class AsyncFlow:
    def __init__(self, name: str, store: Optional[Store] = None):
        self.name = name
        self._tasks: Dict[str, TaskDef] = {}
        self._store = store
        self._lock = asyncio.Lock()

    def task(self, task_id: str, *, depends_on=None, retries=0, retry_delay=None):
        depends_on = depends_on or []

        def decorator(fn: Callable[[Context], Any]):
            self._tasks[task_id] = TaskDef(fn, depends_on, retries, retry_delay)
            return fn

        return decorator

    def start_run(self, params=None) -> str:
        run_id = str(uuid.uuid4())
        run = WorkflowRun(id=run_id, flow_name=self.name, params=params or {})
        for t in self._tasks:
            run.tasks[t] = TaskInstance(task_id=t)
        self._store.save(run)
        return run_id

    async def run_until_complete(self, run_id: str, poll_interval: float = 0.02, max_concurrency: int = 0):
        run = self._store.load(run_id)
        run.state = State.RUNNING
        self._store.save(run)

        sem = asyncio.Semaphore(max_concurrency) if max_concurrency and max_concurrency > 0 else None
        running: Dict[str, asyncio.Task] = {}

        async def submit(ti: TaskInstance):
            async def _runner():
                if sem:
                    async with sem:
                        await self._execute_one(run, ti.task_id)
                else:
                    await self._execute_one(run, ti.task_id)

            task = asyncio.create_task(_runner())
            running[ti.task_id] = task
            task.add_done_callback(lambda _: running.pop(ti.task_id, None))

        while True:
            if all(t.state in (State.SUCCESS, State.FAILED, State.SKIPPED) for t in run.tasks.values()):
                run.state = State.SUCCESS if all(t.state == State.SUCCESS for t in run.tasks.values()) else State.FAILED
                run.end_date = datetime.now()
                async with self._lock:
                    self._store.save(run)
                if running:
                    await asyncio.gather(*running.values(), return_exceptions=True)
                return

            ready = [
                t for t in run.tasks.values()
                if t.state == State.SCHEDULED
                   and t.task_id not in running
                   and all(run.tasks[d].state == State.SUCCESS for d in self._tasks[t.task_id].depends_on)
            ]
            for ti in ready:
                await submit(ti)

            blocked = [
                t for t in run.tasks.values()
                if t.state == State.SCHEDULED
                   and any(run.tasks[d].state == State.FAILED for d in self._tasks[t.task_id].depends_on)
            ]
            if blocked:
                now = datetime.now()
                async with self._lock:
                    for t in blocked:
                        t.state = State.SKIPPED
                        t.end_date = now
                    self._store.save(run)
                continue

            await asyncio.sleep(poll_interval)

    async def _execute_one(self, run: WorkflowRun, task_id: str):
        ti = run.tasks[task_id]
        taskdef = self._tasks[task_id]
        async with self._lock:
            if ti.state != State.SCHEDULED:
                return
            ti.state = State.RUNNING
            ti.start_date = datetime.now()
            self._store.save(run)

        ctx = Context(run, ti)
        try:
            res = self._maybe_await(taskdef.func, ctx)
            if asyncio.iscoroutine(res):
                res = await res
            output = ctx._get_pushed() or res
            async with self._lock:
                ti.output_json = output
                ti.state = State.SUCCESS
                ti.end_date = datetime.now()
                self._store.save(run)

        except Exception:
            async with self._lock:
                ti.try_number += 1
                ti.error = traceback.format_exc()
                ti.end_date = datetime.now()
                if ti.try_number <= taskdef.retries:
                    ti.state = State.SCHEDULED
                    self._store.save(run)
                else:
                    ti.state = State.FAILED
                    self._store.save(run)

            if ti.try_number <= taskdef.retries and taskdef.retry_delay:
                await asyncio.sleep(taskdef.retry_delay.total_seconds())

            if ti.state == State.FAILED:
                children = [t for t, td in self._tasks.items() if ti.task_id in td.depends_on]
                now = datetime.now()
                async with self._lock:
                    for cid in children:
                        cti = run.tasks[cid]
                        if cti.state == State.SCHEDULED:
                            cti.state = State.SKIPPED
                            cti.end_date = now
                    self._store.save(run)

    def _maybe_await(self, fn, ctx):
        return fn(ctx)

    def manual_retry(self, run_id: str, task_id: str, *, reset_downstream: bool = False) -> None:
        run = self._store.load(run_id)
        if task_id not in run.tasks:
            raise KeyError(f"task_id inconnu: {task_id}")
        ti = run.tasks[task_id]
        ti.state = State.SCHEDULED
        ti.end_date = None
        if run.state in (State.SUCCESS, State.FAILED):
            run.state = State.RUNNING
            run.end_date = None
        if reset_downstream:
            for child in self._downstream_of(task_id):
                if child in run.tasks:
                    cti = run.tasks[child]
                    if cti.state in (State.SUCCESS, State.FAILED, State.SKIPPED):
                        cti.state = State.SCHEDULED
                        cti.end_date = None
        self._store.save(run)

    def _downstream_of(self, root: str) -> List[str]:
        rev: Dict[str, List[str]] = {}
        for t, td in self._tasks.items():
            for d in td.depends_on:
                rev.setdefault(d, []).append(t)
        out, stack, seen = [], [root], {root}
        while stack:
            cur = stack.pop()
            for child in rev.get(cur, []):
                if child not in seen:
                    seen.add(child)
                    out.append(child)
                    stack.append(child)
        return out
