# app/infra/flow/executors.py
"""
Execution engine for workflow tasks and subflows.

This module contains the core execution logic for running tasks,
handling retries, and managing task lifecycle.
"""
from __future__ import annotations

import asyncio
import inspect
import traceback
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Optional

from app.infra.flow.models import (
    Context,
    ExecutionState,
    SubFlowDefinition,
    TaskDefinition,
    TaskEvent,
    TaskInstance,
    WorkflowEvent,
    WorkflowRun,
    EventType,
    Store,
)


class TaskExecutor(ABC):
    """
    Abstract base class for task execution.

    Provides common functionality for executing tasks with retry logic
    and state management.
    """

    def __init__(self, store: Store, concurrency_semaphore: Optional[asyncio.Semaphore] = None):
        """
        Initialize the task executor.

        Args:
            store: Storage backend for persisting workflow state
            concurrency_semaphore: Optional semaphore to limit concurrent task execution
        """
        self.store = store
        self.concurrency_semaphore = concurrency_semaphore

    async def _execute_with_semaphore(self, coro: Any) -> Any:
        """
        Execute a coroutine with optional concurrency limiting.

        Args:
            coro: Coroutine to execute

        Returns:
            Result of the coroutine
        """
        if self.concurrency_semaphore:
            async with self.concurrency_semaphore:
                return await coro
        return await coro

    async def _acquire_lock(self, run_id: str, lock_registry: dict) -> asyncio.Lock:
        """
        Get or create a lock for a specific workflow run.

        Args:
            run_id: ID of the workflow run
            lock_registry: Dictionary mapping run IDs to locks

        Returns:
            The lock for the specified run
        """
        if run_id not in lock_registry:
            lock_registry[run_id] = asyncio.Lock()
        return lock_registry[run_id]

    async def _invoke_task_function(
        self, task_function: Callable[[Context], Any], context: Context
    ) -> Any:
        """
        Invoke a task function, handling both sync and async functions.

        Args:
            task_function: The function to invoke
            context: Execution context to pass to the function

        Returns:
            The result of the function
        """
        result = task_function(context)
        if inspect.isawaitable(result):
            return await result
        return result

    async def _update_task_state(
        self,
        workflow_run: WorkflowRun,
        task_id: str,
        state: str,
        run_lock: asyncio.Lock,
        **kwargs,
    ) -> None:
        """
        Update task state with lock protection.

        Args:
            workflow_run: The workflow run containing the task
            task_id: ID of the task to update
            state: New state for the task
            run_lock: Lock for the workflow run
            **kwargs: Additional task instance attributes to update
        """
        async with run_lock:
            task_instance = workflow_run.tasks[task_id]
            task_instance.state = state
            for key, value in kwargs.items():
                setattr(task_instance, key, value)
                self.store.update_task(workflow_run.id, task_instance)

    async def _handle_task_failure(
        self,
        workflow_run: WorkflowRun,
        task_id: str,
        max_retries: int,
        retry_delay_seconds: Optional[float],
        run_lock: asyncio.Lock,
    ) -> Optional[float]:
        """
        Handle task failure and determine if retry is needed.

        Args:
            workflow_run: The workflow run containing the task
            task_id: ID of the failed task
            max_retries: Maximum number of retry attempts
            retry_delay_seconds: Delay in seconds before retry
            run_lock: Lock for the workflow run

        Returns:
            Delay in seconds before retry, or None if no retry should occur
        """
        async with run_lock:
            task_instance = workflow_run.tasks[task_id]
            task_instance.try_number += 1
            task_instance.error = traceback.format_exc()
            task_instance.end_date = datetime.now()

            if task_instance.try_number <= max_retries:
                task_instance.state = ExecutionState.SCHEDULED
                self.store.update_task(workflow_run.id, task_instance)
                return retry_delay_seconds if retry_delay_seconds else 0
            else:
                task_instance.state = ExecutionState.FAILED
                self.store.update_task(workflow_run.id, task_instance)
                return None

    @abstractmethod
    async def execute(
        self,
        workflow_run: WorkflowRun,
        task_id: str,
        run_lock: asyncio.Lock,
        event_emitter: Callable[[WorkflowEvent], Any],
    ) -> None:
        """
        Execute a task or subflow.

        Args:
            workflow_run: The workflow run containing the task
            task_id: ID of the task to execute
            run_lock: Lock for the workflow run
            event_emitter: Function to emit workflow events
        """
        pass


class StandardTaskExecutor(TaskExecutor):
    """
    Executor for standard (non-subflow) tasks.

    Handles execution of user-defined task functions with retry logic.
    """

    def __init__(
        self,
        task_definition: TaskDefinition,
        store: Store,
        concurrency_semaphore: Optional[asyncio.Semaphore] = None,
    ):
        """
        Initialize the standard task executor.

        Args:
            task_definition: Definition of the task to execute
            store: Storage backend for persisting workflow state
            concurrency_semaphore: Optional semaphore to limit concurrent task execution
        """
        super().__init__(store, concurrency_semaphore)
        self.task_definition = task_definition

    async def execute(
        self,
        workflow_run: WorkflowRun,
        task_id: str,
        run_lock: asyncio.Lock,
        event_emitter: Callable[[WorkflowEvent], Any],
    ) -> None:
        """
        Execute a standard task with retry logic.

        Args:
            workflow_run: The workflow run containing the task
            task_id: ID of the task to execute
            run_lock: Lock for the workflow run
            event_emitter: Function to emit workflow events
        """
        run_id = workflow_run.id

        # Check if task should run
        async with run_lock:
            task_instance = workflow_run.tasks[task_id]
            if task_instance.state != ExecutionState.SCHEDULED:
                return
            task_instance.state = ExecutionState.RUNNING
            task_instance.start_date = datetime.now()
            self.store.update_task(workflow_run.id, task_instance)

        # Execute task
        context = Context(workflow_run, task_instance)
        try:
            result = await self._execute_with_semaphore(
                self._invoke_task_function(self.task_definition.task_function, context)
            )

            # Save output
            output = context.get_pushed_value() or result
            await self._update_task_state(
                workflow_run,
                task_id,
                ExecutionState.SUCCESS,
                run_lock,
                output_json=output,
                end_date=datetime.now(),
            )

            # Emit success event
            await event_emitter(
                TaskEvent(
                    event_type=EventType.TASK_COMPLETED,
                    task_id=task_id,
                    run_id=run_id,
                )
            )

        except Exception:
            # Handle failure and retry
            retry_delay = await self._handle_task_failure(
                workflow_run,
                task_id,
                self.task_definition.max_retries,
                self.task_definition.retry_delay.total_seconds()
                if self.task_definition.retry_delay
                else None,
                run_lock,
            )

            if retry_delay is not None:
                # Schedule retry
                if retry_delay > 0:
                    await asyncio.sleep(retry_delay)
                asyncio.create_task(self.execute(workflow_run, task_id, run_lock, event_emitter))
            else:
                # Emit failure event
                await event_emitter(
                    TaskEvent(
                        event_type=EventType.TASK_FAILED,
                        task_id=task_id,
                        run_id=run_id,
                    )
                )


class SubFlowExecutor(TaskExecutor):
    """
    Executor for subflow tasks.

    Handles execution of nested workflows with parameter passing and retry logic.
    """

    def __init__(
        self,
        subflow_definition: SubFlowDefinition,
        store: Store,
        concurrency_semaphore: Optional[asyncio.Semaphore] = None,
    ):
        """
        Initialize the subflow executor.

        Args:
            subflow_definition: Definition of the subflow to execute
            store: Storage backend for persisting workflow state
            concurrency_semaphore: Optional semaphore to limit concurrent task execution
        """
        super().__init__(store, concurrency_semaphore)
        self.subflow_definition = subflow_definition

    async def execute(
        self,
        workflow_run: WorkflowRun,
        task_id: str,
        run_lock: asyncio.Lock,
        event_emitter: Callable[[WorkflowEvent], Any],
    ) -> None:
        """
        Execute a subflow with retry logic.

        Args:
            workflow_run: The workflow run containing the subflow
            task_id: ID of the subflow task to execute
            run_lock: Lock for the workflow run
            event_emitter: Function to emit workflow events
        """
        run_id = workflow_run.id

        # Check if subflow should run
        async with run_lock:
            task_instance = workflow_run.tasks[task_id]
            if task_instance.state != ExecutionState.SCHEDULED:
                return
            task_instance.state = ExecutionState.RUNNING
            task_instance.start_date = datetime.now()

            # Prepare input parameters on first attempt
            if task_instance.try_number == 0:
                merged_params = {**workflow_run.params}
                for dependency_id in self.subflow_definition.dependencies:
                    dependency_task = workflow_run.tasks.get(dependency_id)
                    if (
                        dependency_task
                        and dependency_task.state == ExecutionState.SUCCESS
                        and dependency_task.output_json is not None
                    ):
                        # Only merge if output is a dict, otherwise store under dependency name
                        if isinstance(dependency_task.output_json, dict):
                            merged_params.update(dependency_task.output_json)
                        else:
                            merged_params[dependency_id] = dependency_task.output_json
                task_instance.input_json = {**task_instance.input_json, **merged_params}

            self.store.update_task(workflow_run.id, task_instance)

        # Execute subflow
        try:
            # Get child run ID
            child_run_id = (
                task_instance.output_json.get("child_run_id")
                if task_instance.output_json
                else None
            )
            if not child_run_id:
                raise RuntimeError(f"No child_run_id found for subflow {task_id}")

            # Configure and execute child flow
            child_flow = self.subflow_definition.child_flow
            child_flow._store = self.store

            # Update child run parameters
            child_run = self.store.load(child_run_id)
            child_run.params = task_instance.input_json or {}
            self.store.save(child_run)

            # Execute child flow only if not already complete
            # If child is already SUCCESS, reuse that result
            # If child is FAILED or not terminal, execute it
            if child_run.state != ExecutionState.SUCCESS:
                await self._execute_with_semaphore(child_flow.run_until_complete(child_run_id))

            # Check result
            child_run = self.store.load(child_run_id)
            if child_run.state == ExecutionState.SUCCESS:
                # Get the output from the child workflow
                # Find leaf tasks (tasks with no downstream dependencies)
                # and return the output from one of them
                child_output = None

                # Build dependency map to find leaf tasks
                has_downstream = set()
                for child_task_id, child_task in child_run.tasks.items():
                    # Check if this task is a dependency of any other task
                    # We need to look at the flow's task definitions for this
                    pass  # Will determine from task execution order

                # For now: get the last task that completed
                # (tasks are typically added in topological order)
                task_completion_order = []
                for child_task_id, child_task in child_run.tasks.items():
                    if child_task.state == ExecutionState.SUCCESS and child_task.end_date:
                        task_completion_order.append((child_task.end_date, child_task_id, child_task))

                if task_completion_order:
                    # Sort by completion time and get the last one
                    task_completion_order.sort()
                    _, _, last_task = task_completion_order[-1]
                    child_output = last_task.output_json

                # Store the child workflow's output as the subflow task's output
                # Keep child_run_id for future retries
                async with run_lock:
                    task_instance = workflow_run.tasks[task_id]
                    # For subflow tasks, store both child_run_id and result
                    task_instance.output_json = {
                        "child_run_id": child_run_id,
                        "result": child_output
                    }

                await self._update_task_state(
                    workflow_run,
                    task_id,
                    ExecutionState.SUCCESS,
                    run_lock,
                    end_date=datetime.now(),
                )
                await event_emitter(
                    TaskEvent(
                        event_type=EventType.TASK_COMPLETED,
                        task_id=task_id,
                        run_id=run_id,
                    )
                )
            else:
                raise RuntimeError(f"Subflow {task_id} failed")

        except Exception:
            # Handle failure and retry
            retry_delay = await self._handle_task_failure(
                workflow_run,
                task_id,
                self.subflow_definition.max_retries,
                self.subflow_definition.retry_delay.total_seconds()
                if self.subflow_definition.retry_delay
                else None,
                run_lock,
            )

            if retry_delay is not None:
                # Schedule retry
                if retry_delay > 0:
                    await asyncio.sleep(retry_delay)
                asyncio.create_task(self.execute(workflow_run, task_id, run_lock, event_emitter))
            else:
                # Emit failure event
                await event_emitter(
                    TaskEvent(
                        event_type=EventType.TASK_FAILED,
                        task_id=task_id,
                        run_id=run_id,
                    )
                )
