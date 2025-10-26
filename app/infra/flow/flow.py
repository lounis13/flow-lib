# app/infra/flow/flow.py
from __future__ import annotations

import asyncio
import inspect
import traceback
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable, Dict, Any, List, Optional, Union

from app.infra.flow.models import WorkflowRun, State, TaskInstance, Store, TaskType


# ============================================================
# Context
# ============================================================
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


# ============================================================
# Core flow data types
# ============================================================
@dataclass
class TaskDef:
    func: Callable[[Context], Any]
    type: TaskType = TaskType.TASK
    depends_on: List[str] = field(default_factory=list)
    retries: int = 0
    retry_delay: Optional[timedelta] = None


@dataclass
class SubFlowDef:
    flow: AsyncFlow
    depends_on: List[str] = field(default_factory=list)
    retries: int = 0
    retry_delay: Optional[timedelta] = None


class EventType(Enum):
    TASK_DONE = "TASK_DONE"
    TASK_FAILED = "TASK_FAILED"
    EXIT = "EXIT"


@dataclass(frozen=True)
class TaskEvent:
    type: EventType
    task_id: str
    run_id: str


@dataclass(frozen=True)
class ExitEvent:
    type: EventType
    run_id: str


Event = Union[TaskEvent, ExitEvent]


# ============================================================
# AsyncFlow
# ============================================================
class AsyncFlow:
    def __init__(self, name: str, store: Optional[Store] = None):
        self.name = name
        self._store = store
        self._tasks: Dict[str, TaskDef] = {}
        self._subflows: Dict[str, SubFlowDef] = {}

        # IPC & concurrency per run
        self._event_queues: Dict[str, asyncio.Queue[Event]] = {}  # run_id -> queue
        self._locks: Dict[str, asyncio.Lock] = {}                 # run_id -> lock
        self._sem: Optional[asyncio.Semaphore] = None             # global concurrency limit

    # ---------------- IPC helpers ----------------
    def _get_event_queue(self, run_id: str) -> asyncio.Queue[Event]:
        q = self._event_queues.get(run_id)
        if q is None:
            # Option: asyncio.Queue(maxsize=1000) for backpressure
            q = asyncio.Queue()
            self._event_queues[run_id] = q
        return q

    def _get_run_lock(self, run_id: str) -> asyncio.Lock:
        lock = self._locks.get(run_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[run_id] = lock
        return lock

    @asynccontextmanager
    async def _run_lock(self, run_id: str):
        lock = self._get_run_lock(run_id)
        async with lock:
            yield

    def _cleanup_run_ipc(self, run_id: str):
        self._event_queues.pop(run_id, None)
        self._locks.pop(run_id, None)

    async def _emit(self, event: Event):
        await self._get_event_queue(event.run_id).put(event)

    async def _call_task_fn(self, fn: Callable[[Context], Any], ctx: Context):
        res = fn(ctx)
        if inspect.isawaitable(res):
            return await res
        return res

    # ---------------- Task registration ----------------
    def task(self, task_id: str, *, type: TaskType = TaskType.TASK, depends_on=None, retries=0, retry_delay=None):
        depends_on = depends_on or []

        def decorator(fn: Callable[[Context], Any]):
            self._tasks[task_id] = TaskDef(fn, type, depends_on, retries, retry_delay)
            return fn

        return decorator

    def subflow(self, subflow_id: str, *, depends_on=None, retries=0, retry_delay=None):
        """Decorator to register a subflow"""
        depends_on = depends_on or []

        def decorator(flow: AsyncFlow):
            self._subflows[subflow_id] = SubFlowDef(flow, depends_on, retries, retry_delay)
            return flow

        return decorator

    def add_subflow(self, subflow_id: str, flow: AsyncFlow, *, depends_on=None, retries=0, retry_delay=None):
        """Registers a subflow manually"""
        depends_on = depends_on or []
        self._subflows[subflow_id] = SubFlowDef(flow, depends_on, retries, retry_delay)
        return flow

    # ---------------- Run lifecycle ----------------
    def start_run(self, params=None, parent_run_id: str | None = None, parent_task_id: str | None = None,
                  run_id: str | None = None) -> str:
        # If a run_id is provided and exists, update it instead of creating a new one
        if run_id and self._store.exists(run_id):
            run = self._store.load(run_id)
            run.params = params or {}
            run.parent_run_id = parent_run_id
            run.parent_task_id = parent_task_id
        else:
            # Create a new run
            run_id = run_id or str(uuid.uuid4())
            run = WorkflowRun(
                id=run_id,
                flow_name=self.name,
                params=params or {},
                parent_run_id=parent_run_id,
                parent_task_id=parent_task_id,
            )
            # Create TaskInstances for normal tasks
            for t in self._tasks:
                run.tasks[t] = TaskInstance(task_id=t, type=self._tasks.get(t).type)

            # Create TaskInstances for subflows (type FLOW)
            for sf_id in self._subflows:
                run.tasks[sf_id] = TaskInstance(task_id=sf_id, type=TaskType.FLOW)

        self._store.save(run)

        # Recursively create runs for subflows and their tasks
        for sf_id, sf_def in self._subflows.items():
            child_flow = sf_def.flow
            child_flow._store = self._store

            # Check if a child_run_id already exists
            existing_child_run_id = None
            if sf_id in run.tasks and run.tasks[sf_id].output_json:
                existing_child_run_id = run.tasks[sf_id].output_json.get("child_run_id")

            child_run_id = child_flow.start_run(
                params=params or {},
                parent_run_id=run.id,
                parent_task_id=sf_id,
                run_id=existing_child_run_id  # Reuse existing ID if available
            )
            # Store the child_run_id in the subflow's TaskInstance
            run.tasks[sf_id].output_json = {"child_run_id": child_run_id}

        self._store.save(run)
        return run.id

    async def run_until_complete(self, run_id: str, max_concurrency: int = 0):
        run = self._store.load(run_id)
        async with self._run_lock(run_id):
            run.state = State.RUNNING
            self._store.save(run)

        self._sem = asyncio.Semaphore(max_concurrency) if max_concurrency and max_concurrency > 0 else None
        q = self._get_event_queue(run_id)

        def ready_to_run_task(task_id: str) -> bool:
            td = self._tasks[task_id]
            ti = run.tasks[task_id]
            return ti.state == State.SCHEDULED and all(run.tasks[d].state == State.SUCCESS for d in td.depends_on)

        def ready_to_run_subflow(subflow_id: str) -> bool:
            sf = self._subflows[subflow_id]
            ti = run.tasks[subflow_id]
            return ti.state == State.SCHEDULED and all(run.tasks[d].state == State.SUCCESS for d in sf.depends_on)

        # Launch ready tasks
        for tid in self._tasks:
            if ready_to_run_task(tid):
                asyncio.create_task(self._execute_one(run, tid))

        # Launch ready subflows
        for sf_id in self._subflows:
            if ready_to_run_subflow(sf_id):
                asyncio.create_task(self._execute_subflow(run, sf_id))

        try:
            while True:
                evt = await q.get()

                if evt.type is EventType.TASK_DONE:
                    await self._on_task_done(run, evt.task_id)
                elif evt.type is EventType.TASK_FAILED:
                    await self._on_task_failed(run, evt.task_id)
                elif evt.type is EventType.EXIT:
                    break

                # Check run completion
                async with self._run_lock(run_id):
                    done = all(t.state in (State.SUCCESS, State.FAILED, State.SKIPPED) for t in run.tasks.values())
                    if done:
                        run.state = State.SUCCESS if all(t.state == State.SUCCESS for t in run.tasks.values()) else State.FAILED
                        run.end_date = datetime.now()
                        self._store.save(run)
                if done:
                    await self._emit(ExitEvent(type=EventType.EXIT, run_id=run_id))
        finally:
            # Cleanup to avoid leaks (queues/locks per run)
            self._cleanup_run_ipc(run_id)

    # ---------------- Task execution ----------------
    async def _execute_one(self, run: WorkflowRun, task_id: str):
        run_id = run.id
        td = self._tasks[task_id]

        async with self._run_lock(run_id):
            ti = run.tasks[task_id]
            if ti.state != State.SCHEDULED:
                return
            ti.state = State.RUNNING
            ti.start_date = datetime.now()
            self._store.save(run)

        ctx = Context(run, ti)
        try:
            if self._sem:
                async with self._sem:
                    res = await self._call_task_fn(td.func, ctx)
            else:
                res = await self._call_task_fn(td.func, ctx)

            output = ctx._get_pushed() or res
            async with self._run_lock(run_id):
                ti = run.tasks[task_id]
                ti.output_json = output
                ti.state = State.SUCCESS
                ti.end_date = datetime.now()
                self._store.save(run)

            await self._emit(TaskEvent(type=EventType.TASK_DONE, task_id=task_id, run_id=run_id))

        except Exception:
            delay = None
            async with self._run_lock(run_id):
                ti = run.tasks[task_id]
                ti.try_number += 1
                ti.error = traceback.format_exc()
                ti.end_date = datetime.now()

                if ti.try_number <= td.retries:
                    ti.state = State.SCHEDULED
                    self._store.save(run)
                    delay = td.retry_delay.total_seconds() if td.retry_delay else 0
                else:
                    ti.state = State.FAILED
                    self._store.save(run)

            if delay is not None:
                if delay:
                    await asyncio.sleep(delay)  # outside lock
                asyncio.create_task(self._execute_one(run, task_id))
            else:
                await self._emit(TaskEvent(type=EventType.TASK_FAILED, task_id=task_id, run_id=run_id))

    # ---------------- Subflow execution ----------------
    async def _execute_subflow(self, run: WorkflowRun, subflow_id: str):
        run_id = run.id
        sf = self._subflows[subflow_id]

        async with self._run_lock(run_id):
            ti = run.tasks[subflow_id]
            if ti.state != State.SCHEDULED:
                return
            ti.state = State.RUNNING
            ti.start_date = datetime.now()

            # Prepare input_json by merging dependency outputs (first attempt only)
            if ti.try_number == 0:
                merged_input = {**run.params}
                for dep_id in sf.depends_on:
                    dep_task = run.tasks.get(dep_id)
                    if dep_task and dep_task.state == State.SUCCESS and dep_task.output_json:
                        merged_input.update(dep_task.output_json)
                ti.input_json = merged_input

            self._store.save(run)

        try:
            # Get the child_run_id
            child_run_id = ti.output_json.get("child_run_id") if ti.output_json else None
            if not child_run_id:
                raise RuntimeError(f"No child_run_id found for subflow {subflow_id}")

            # Configure the store for the subflow
            child_flow = sf.flow
            child_flow._store = self._store

            # Update the child's params with the subflow input
            child_run = self._store.load(child_run_id)
            child_run.params = ti.input_json or {}
            self._store.save(child_run)

            # Execute the subflow until completion (option: same semaphore)
            if self._sem:
                async with self._sem:
                    await child_flow.run_until_complete(child_run_id)
            else:
                await child_flow.run_until_complete(child_run_id)

            # Check the result
            child_run = self._store.load(child_run_id)
            if child_run.state == State.SUCCESS:
                async with self._run_lock(run_id):
                    ti = run.tasks[subflow_id]
                    ti.state = State.SUCCESS
                    ti.end_date = datetime.now()
                    self._store.save(run)
                await self._emit(TaskEvent(type=EventType.TASK_DONE, task_id=subflow_id, run_id=run_id))
                print(f"Event Sent {TaskEvent(type=EventType.TASK_DONE, task_id=subflow_id, run_id=run_id)}")
            else:
                raise RuntimeError(f"Subflow {subflow_id} failed")

        except Exception:
            delay = None
            async with self._run_lock(run_id):
                ti = run.tasks[subflow_id]
                ti.try_number += 1
                ti.error = traceback.format_exc()
                ti.end_date = datetime.now()

                if ti.try_number <= sf.retries:
                    ti.state = State.SCHEDULED
                    self._store.save(run)
                    delay = sf.retry_delay.total_seconds() if sf.retry_delay else 0
                else:
                    ti.state = State.FAILED
                    self._store.save(run)

            if delay is not None:
                if delay:
                    await asyncio.sleep(delay)
                asyncio.create_task(self._execute_subflow(run, subflow_id))
            else:
                await self._emit(TaskEvent(type=EventType.TASK_FAILED, task_id=subflow_id, run_id=run_id))

    # ---------------- Event reactions ----------------
    async def _on_task_done(self, run: WorkflowRun, task_id: str):
        # Schedule ready dependent tasks
        ready_tasks = [
            t for t, td in self._tasks.items()
            if task_id in td.depends_on
               and all(run.tasks[d].state == State.SUCCESS for d in td.depends_on)
               and run.tasks[t].state == State.SCHEDULED
        ]
        for r in ready_tasks:
            asyncio.create_task(self._execute_one(run, r))

        # Schedule ready dependent subflows
        ready_subflows = [
            sf_id for sf_id, sf in self._subflows.items()
            if task_id in sf.depends_on
               and all(run.tasks[d].state == State.SUCCESS for d in sf.depends_on)
               and run.tasks[sf_id].state == State.SCHEDULED
        ]
        for sf_id in ready_subflows:
            asyncio.create_task(self._execute_subflow(run, sf_id))

    async def _on_task_failed(self, run: WorkflowRun, task_id: str):
        # Find child tasks/subflows
        task_children = [t for t, td in self._tasks.items() if task_id in td.depends_on]
        subflow_children = [sf_id for sf_id, sf in self._subflows.items() if task_id in sf.depends_on]
        now = datetime.now()

        async with self._run_lock(run.id):
            # Skip child tasks
            for cid in task_children:
                cti = run.tasks[cid]
                if cti.state == State.SCHEDULED:
                    cti.state = State.SKIPPED
                    cti.end_date = now
            # Skip child subflows
            for sf_id in subflow_children:
                sf_ti = run.tasks[sf_id]
                if sf_ti.state == State.SCHEDULED:
                    sf_ti.state = State.SKIPPED
                    sf_ti.end_date = now
            self._store.save(run)

    # ---------------- Retry ----------------
    def manual_retry(self, run_id: str, task_id: str, *, reset_downstream: bool = False) -> None:
        run = self._store.load(run_id)
        if task_id not in run.tasks:
            raise KeyError(f"unknown task_id: {task_id}")

        ti = run.tasks[task_id]
        ti.state = State.SCHEDULED
        ti.end_date = None
        ti.error = None
        ti.try_number = 0

        # If it's a subflow (type FLOW), reset the subflow's tasks
        if ti.type == TaskType.FLOW:
            child_run_id = ti.output_json.get("child_run_id") if ti.output_json else None
            if child_run_id:
                try:
                    child_run = self._store.load(child_run_id)
                    # Restore params from input_json for retry
                    if ti.input_json:
                        child_run.params = ti.input_json
                    # Reset all subflow tasks
                    for child_ti in child_run.tasks.values():
                        if child_ti.state in (State.SUCCESS, State.FAILED, State.SKIPPED):
                            child_ti.state = State.SCHEDULED
                            child_ti.end_date = None
                            child_ti.try_number = 0
                            child_ti.error = None
                    child_run.state = State.SCHEDULED
                    child_run.end_date = None
                    self._store.save(child_run)
                except Exception:
                    # The child run may not exist yet or other store issue
                    pass

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
                        cti.error = None
                        cti.try_number = 0

                        # If the child is also a subflow, reset its tasks too
                        if cti.type == TaskType.FLOW:
                            child_run_id = cti.output_json.get("child_run_id") if cti.output_json else None
                            if child_run_id:
                                try:
                                    child_run = self._store.load(child_run_id)
                                    # Restore params from input_json for retry
                                    if cti.input_json:
                                        child_run.params = cti.input_json
                                    for child_ti in child_run.tasks.values():
                                        if child_ti.state in (State.SUCCESS, State.FAILED, State.SKIPPED):
                                            child_ti.state = State.SCHEDULED
                                            child_ti.end_date = None
                                            child_ti.try_number = 0
                                            child_ti.error = None
                                    child_run.state = State.SCHEDULED
                                    child_run.end_date = None
                                    self._store.save(child_run)
                                except Exception:
                                    pass

        self._store.save(run)

    async def retry(self, run_id: str, task_id: str, *, reset_downstream: bool = True, max_concurrency: int = 0):
        self.manual_retry(run_id, task_id, reset_downstream=reset_downstream)
        await self.run_until_complete(run_id, max_concurrency=max_concurrency)

    # ---------------- Helpers ----------------
    def _downstream_of(self, root: str) -> List[str]:
        """Returns all descendants (tasks and subflows) of a given node"""
        rev: Dict[str, List[str]] = {}
        # Add task dependencies
        for t, td in self._tasks.items():
            for d in td.depends_on:
                rev.setdefault(d, []).append(t)
        # Add subflow dependencies
        for sf_id, sf in self._subflows.items():
            for d in sf.depends_on:
                rev.setdefault(d, []).append(sf_id)

        out, stack, seen = [], [root], {root}
        while stack:
            cur = stack.pop()
            for child in rev.get(cur, []):
                if child not in seen:
                    seen.add(child)
                    out.append(child)
                    stack.append(child)
        return out

    # ---------------- Serialization ----------------
    def get_execution_details(self, run_id: str):
        """
        Returns the complete execution details of a flow with strong typing.
        Uses dataclasses for clean abstraction.
        """
        from app.infra.flow.flow_schema import (
            FlowExecution,
            TaskExecution,
            FlowDefinition,
            TaskDefinition,
        )

        run = self._store.load(run_id)

        def build_flow_execution(run_obj, flow_def) -> FlowExecution:
            # Build flow definition
            flow_tasks_def = {}
            for task_id, task_def in flow_def._tasks.items():
                flow_tasks_def[task_id] = TaskDefinition(
                    task_id=task_id,
                    type=task_def.type,
                    depends_on=task_def.depends_on,
                    retries=task_def.retries,
                )

            for subflow_id, subflow_def in flow_def._subflows.items():
                flow_tasks_def[subflow_id] = TaskDefinition(
                    task_id=subflow_id,
                    type="flow",
                    depends_on=subflow_def.depends_on,
                    retries=subflow_def.retries,
                    flow_name=subflow_def.flow.name,
                )

            flow_definition = FlowDefinition(
                name=flow_def.name,
                tasks=flow_tasks_def,
            )

            # Build task executions
            tasks_execution = {}
            for task_id, task in run_obj.tasks.items():
                # Get dependencies
                depends_on = []
                if task_id in flow_def._tasks:
                    depends_on = flow_def._tasks[task_id].depends_on
                elif task_id in flow_def._subflows:
                    depends_on = flow_def._subflows[task_id].depends_on

                task_exec = TaskExecution(
                    task_id=task.task_id,
                    type=task.type,
                    state=task.state,
                    try_number=task.try_number,
                    depends_on=depends_on,
                    input_json=task.input_json,
                    output_json=task.output_json,
                    error=task.error,
                    start_date=task.start_date.isoformat() if task.start_date else None,
                    end_date=task.end_date.isoformat() if task.end_date else None,
                )

                # If it's a subflow, add recursively
                if task.type == "flow" and task.output_json and "child_run_id" in task.output_json:
                    child_run_id = task.output_json["child_run_id"]
                    try:
                        child_run = self._store.load(child_run_id)
                        child_flow_def = flow_def._subflows[task_id].flow
                        task_exec.subflow = build_flow_execution(child_run, child_flow_def)
                    except Exception:
                        task_exec.subflow = None

                tasks_execution[task_id] = task_exec

            return FlowExecution(
                id=run_obj.id,
                flow_name=run_obj.flow_name,
                params=run_obj.params,
                state=run_obj.state,
                start_date=run_obj.start_date.isoformat() if run_obj.start_date else None,
                end_date=run_obj.end_date.isoformat() if run_obj.end_date else None,
                parent_run_id=run_obj.parent_run_id,
                parent_task_id=run_obj.parent_task_id,
                flow=flow_definition,
                tasks=tasks_execution,
            )

        return build_flow_execution(run, self)
