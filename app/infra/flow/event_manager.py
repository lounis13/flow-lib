# app/infra/flow/event_manager.py
"""
Event management system for workflow execution.

This module handles event-based communication between workflow components,
including task completion events and workflow coordination.
"""
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Callable, Dict, List, Any

from app.infra.flow.models import (
    ExecutionState,
    Store,
    SubFlowDefinition,
    TaskDefinition,
    WorkflowEvent,
    WorkflowRun,
)


class EventManager:
    """
    Manages event queuing and dispatching for workflow execution.

    Handles inter-task communication and workflow lifecycle events.
    """

    def __init__(self):
        """Initialize the event manager with empty event queues."""
        self._event_queues: Dict[str, asyncio.Queue[WorkflowEvent]] = {}

    def get_event_queue(self, run_id: str) -> asyncio.Queue[WorkflowEvent]:
        """
        Get or create an event queue for a workflow run.

        Args:
            run_id: ID of the workflow run

        Returns:
            Event queue for the specified run
        """
        if run_id not in self._event_queues:
            self._event_queues[run_id] = asyncio.Queue()
        return self._event_queues[run_id]

    async def emit_event(self, event: WorkflowEvent) -> None:
        """
        Emit an event to the appropriate event queue.

        Args:
            event: Event to emit
        """
        queue = self.get_event_queue(event.run_id)
        await queue.put(event)

    def cleanup_run_resources(self, run_id: str) -> None:
        """
        Clean up resources associated with a workflow run.

        Args:
            run_id: ID of the workflow run to clean up
        """
        self._event_queues.pop(run_id, None)


class DependencyResolver:
    """
    Resolves task dependencies and determines which tasks are ready to execute.

    Handles both standard tasks and subflows.
    """

    def __init__(
        self,
        task_definitions: Dict[str, TaskDefinition],
        subflow_definitions: Dict[str, SubFlowDefinition],
    ):
        """
        Initialize the dependency resolver.

        Args:
            task_definitions: Map of task IDs to task definitions
            subflow_definitions: Map of subflow IDs to subflow definitions
        """
        self.task_definitions = task_definitions
        self.subflow_definitions = subflow_definitions

    def is_task_ready(self, workflow_run: WorkflowRun, task_id: str) -> bool:
        """
        Check if a task is ready to execute.

        A task is ready if:
        - Its state is SCHEDULED
        - All its dependencies have completed successfully

        Args:
            workflow_run: The workflow run containing the task
            task_id: ID of the task to check

        Returns:
            True if the task is ready to execute
        """
        task_definition = self.task_definitions.get(task_id)
        if not task_definition:
            return False

        task_instance = workflow_run.tasks.get(task_id)
        if not task_instance or task_instance.state != ExecutionState.SCHEDULED:
            return False

        return all(
            workflow_run.tasks[dep_id].state == ExecutionState.SUCCESS
            for dep_id in task_definition.dependencies
        )

    def is_subflow_ready(self, workflow_run: WorkflowRun, subflow_id: str) -> bool:
        """
        Check if a subflow is ready to execute.

        A subflow is ready if:
        - Its state is SCHEDULED
        - All its dependencies have completed successfully

        Args:
            workflow_run: The workflow run containing the subflow
            subflow_id: ID of the subflow to check

        Returns:
            True if the subflow is ready to execute
        """
        subflow_definition = self.subflow_definitions.get(subflow_id)
        if not subflow_definition:
            return False

        task_instance = workflow_run.tasks.get(subflow_id)
        if not task_instance or task_instance.state != ExecutionState.SCHEDULED:
            return False

        return all(
            workflow_run.tasks[dep_id].state == ExecutionState.SUCCESS
            for dep_id in subflow_definition.dependencies
        )

    def get_ready_tasks(self, workflow_run: WorkflowRun) -> List[str]:
        """
        Get all tasks that are ready to execute.

        Args:
            workflow_run: The workflow run to check

        Returns:
            List of task IDs that are ready to execute
        """
        return [
            task_id
            for task_id in self.task_definitions.keys()
            if self.is_task_ready(workflow_run, task_id)
        ]

    def get_ready_subflows(self, workflow_run: WorkflowRun) -> List[str]:
        """
        Get all subflows that are ready to execute.

        Args:
            workflow_run: The workflow run to check

        Returns:
            List of subflow IDs that are ready to execute
        """
        return [
            subflow_id
            for subflow_id in self.subflow_definitions.keys()
            if self.is_subflow_ready(workflow_run, subflow_id)
        ]

    def get_downstream_tasks(self, task_id: str) -> List[str]:
        """
        Get all tasks that directly depend on the specified task.

        Args:
            task_id: ID of the task to find dependents for

        Returns:
            List of task IDs that depend on the specified task
        """
        downstream_tasks = []

        # Check standard tasks
        for tid, task_def in self.task_definitions.items():
            if task_id in task_def.dependencies:
                downstream_tasks.append(tid)

        # Check subflows
        for sf_id, subflow_def in self.subflow_definitions.items():
            if task_id in subflow_def.dependencies:
                downstream_tasks.append(sf_id)

        return downstream_tasks

    def get_all_downstream_tasks(self, root_task_id: str) -> List[str]:
        """
        Recursively get all downstream tasks (transitive dependencies).

        Args:
            root_task_id: ID of the root task

        Returns:
            List of all task IDs that transitively depend on the root task
        """
        downstream = []
        visited = {root_task_id}
        queue = [root_task_id]

        while queue:
            current_task_id = queue.pop(0)
            for child_task_id in self.get_downstream_tasks(current_task_id):
                if child_task_id not in visited:
                    visited.add(child_task_id)
                    downstream.append(child_task_id)
                    queue.append(child_task_id)

        return downstream


class WorkflowEventHandler:
    """
    Handles workflow events and coordinates task execution.

    Processes events such as task completion and failure, and triggers
    dependent tasks accordingly.
    """

    def __init__(
        self,
        dependency_resolver: DependencyResolver,
        store: Store,
        task_launcher: Callable[[WorkflowRun, str], Any],
    ):
        """
        Initialize the workflow event handler.

        Args:
            dependency_resolver: Resolver for task dependencies
            store: Storage backend for persisting workflow state
            task_launcher: Function to launch task execution (returns Task or None)
        """
        self.dependency_resolver = dependency_resolver
        self.store = store
        self.task_launcher = task_launcher

    async def handle_task_completion(self, workflow_run: WorkflowRun, task_id: str) -> None:
        """
        Handle a task completion event.

        Triggers execution of any downstream tasks that are now ready.

        Args:
            workflow_run: The workflow run containing the completed task
            task_id: ID of the completed task
        """
        # Find and launch ready downstream tasks
        for downstream_task_id in self.dependency_resolver.get_downstream_tasks(task_id):
            if self.dependency_resolver.is_task_ready(workflow_run, downstream_task_id):
                self.task_launcher(workflow_run, downstream_task_id)
            elif self.dependency_resolver.is_subflow_ready(workflow_run, downstream_task_id):
                self.task_launcher(workflow_run, downstream_task_id)

    async def handle_task_failure(
        self, workflow_run: WorkflowRun, task_id: str, run_lock: asyncio.Lock
    ) -> None:
        """
        Handle a task failure event.

        Marks all downstream tasks as SKIPPED (transitively).

        Args:
            workflow_run: The workflow run containing the failed task
            task_id: ID of the failed task
            run_lock: Lock for the workflow run
        """
        # Get ALL downstream tasks transitively (not just direct dependencies)
        downstream_task_ids = self.dependency_resolver.get_all_downstream_tasks(task_id)
        now = datetime.now()

        async with run_lock:
            for downstream_task_id in downstream_task_ids:
                task_instance = workflow_run.tasks.get(downstream_task_id)
                if task_instance and task_instance.state == ExecutionState.SCHEDULED:
                    task_instance.state = ExecutionState.SKIPPED
                    task_instance.end_date = now

            self.store.save(workflow_run)

    def is_workflow_complete(self, workflow_run: WorkflowRun) -> bool:
        """
        Check if the workflow execution is complete.

        A workflow is complete when all tasks are in a terminal state.

        Args:
            workflow_run: The workflow run to check

        Returns:
            True if all tasks have completed (success, failed, or skipped)
        """
        return all(
            ExecutionState.is_terminal(task_instance.state)
            for task_instance in workflow_run.tasks.values()
        )

    def compute_workflow_final_state(self, workflow_run: WorkflowRun) -> str:
        """
        Compute the final state of a completed workflow.

        Args:
            workflow_run: The completed workflow run

        Returns:
            ExecutionState.SUCCESS if all tasks succeeded, ExecutionState.FAILED otherwise
        """
        all_success = all(
            task_instance.state == ExecutionState.SUCCESS
            for task_instance in workflow_run.tasks.values()
        )
        return ExecutionState.SUCCESS if all_success else ExecutionState.FAILED
