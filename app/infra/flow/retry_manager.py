# app/infra/flow/retry_manager.py
"""
Retry management for workflow tasks.

This module handles manual and automatic retry logic for failed tasks
and subflows, including downstream task reset capabilities.
"""
from __future__ import annotations

from typing import Optional

from app.infra.flow.models import (
    ExecutionState,
    Store,
    SubFlowDefinition,
    TaskDefinition,
    TaskType,
    WorkflowRun,
)


class RetryManager:
    """
    Manages retry logic for workflow tasks and subflows.

    Handles both automatic retries (configured in task definitions) and
    manual retries triggered by users.
    """

    def __init__(
        self,
        store: Store,
        task_definitions: dict[str, TaskDefinition],
        subflow_definitions: dict[str, SubFlowDefinition],
        downstream_resolver: Optional[object] = None,
    ):
        """
        Initialize the retry manager.

        Args:
            store: Storage backend for persisting workflow state
            task_definitions: Map of task IDs to task definitions
            subflow_definitions: Map of subflow IDs to subflow definitions
            downstream_resolver: Optional resolver for finding downstream tasks
        """
        self.store = store
        self.task_definitions = task_definitions
        self.subflow_definitions = subflow_definitions
        self.downstream_resolver = downstream_resolver

    def reset_task_state(self, workflow_run: WorkflowRun, task_id: str) -> None:
        """
        Reset a task to its initial state for retry.

        Args:
            workflow_run: The workflow run containing the task
            task_id: ID of the task to reset

        Raises:
            KeyError: If the task_id does not exist
        """
        if task_id not in workflow_run.tasks:
            raise KeyError(f"Task '{task_id}' not found in workflow run")

        task_instance = workflow_run.tasks[task_id]
        task_instance.state = ExecutionState.SCHEDULED
        task_instance.end_date = None
        task_instance.error = None
        task_instance.try_number = 0

        # If it's a subflow, reset the child run as well
        if task_instance.type == TaskType.FLOW:
            self._reset_subflow_tasks(task_instance)

    def _reset_subflow_tasks(self, subflow_task_instance: object) -> None:
        """
        Reset all tasks within a subflow for retry.

        Args:
            subflow_task_instance: The subflow task instance to reset
        """
        child_run_id = (
            subflow_task_instance.output_json.get("child_run_id")
            if subflow_task_instance.output_json
            else None
        )

        if not child_run_id:
            return

        try:
            child_run = self.store.load(child_run_id)

            # Restore parameters from input_json
            if subflow_task_instance.input_json:
                child_run.params = subflow_task_instance.input_json

            # Reset all child tasks
            for child_task_instance in child_run.tasks.values():
                if ExecutionState.is_terminal(child_task_instance.state):
                    child_task_instance.state = ExecutionState.SCHEDULED
                    child_task_instance.end_date = None
                    child_task_instance.try_number = 0
                    child_task_instance.error = None

            child_run.state = ExecutionState.SCHEDULED
            child_run.end_date = None
            self.store.save(child_run)
        except Exception:
            # Child run may not exist yet or other storage issue
            pass

    def reset_downstream_tasks(
        self, workflow_run: WorkflowRun, root_task_id: str
    ) -> None:
        """
        Reset all downstream tasks of a given task.

        This is useful when retrying a task and you want to re-execute
        all dependent tasks as well.

        Args:
            workflow_run: The workflow run containing the tasks
            root_task_id: ID of the root task whose downstream tasks should be reset
        """
        if not self.downstream_resolver:
            return

        downstream_task_ids = self.downstream_resolver.get_all_downstream_tasks(
            root_task_id
        )

        for task_id in downstream_task_ids:
            if task_id in workflow_run.tasks:
                task_instance = workflow_run.tasks[task_id]
                if ExecutionState.is_terminal(task_instance.state):
                    task_instance.state = ExecutionState.SCHEDULED
                    task_instance.end_date = None
                    task_instance.error = None
                    task_instance.try_number = 0

                    # If it's a subflow, reset its tasks too
                    if task_instance.type == TaskType.FLOW:
                        self._reset_subflow_tasks(task_instance)

    def manual_retry(
        self,
        run_id: str,
        task_id: str,
        reset_downstream: bool = False,
    ) -> None:
        """
        Manually retry a failed task.

        This resets the task state and optionally resets all downstream tasks.

        Args:
            run_id: ID of the workflow run
            task_id: ID of the task to retry
            reset_downstream: Whether to also reset downstream tasks

        Raises:
            KeyError: If the run_id or task_id does not exist
        """
        workflow_run = self.store.load(run_id)

        # Reset the target task
        self.reset_task_state(workflow_run, task_id)

        # Reset workflow state if it was terminal
        if ExecutionState.is_terminal(workflow_run.state):
            workflow_run.state = ExecutionState.RUNNING
            workflow_run.end_date = None

        # Optionally reset downstream tasks
        if reset_downstream and self.downstream_resolver:
            self.reset_downstream_tasks(workflow_run, task_id)

        self.store.save(workflow_run)

    def can_retry_automatically(
        self, workflow_run: WorkflowRun, task_id: str
    ) -> bool:
        """
        Check if a task can be automatically retried.

        A task can be automatically retried if it has not exceeded
        its configured maximum retry attempts.

        Args:
            workflow_run: The workflow run containing the task
            task_id: ID of the task to check

        Returns:
            True if the task can be automatically retried
        """
        task_instance = workflow_run.tasks.get(task_id)
        if not task_instance:
            return False

        # Check if it's a standard task
        if task_id in self.task_definitions:
            task_def = self.task_definitions[task_id]
            return task_instance.try_number < task_def.max_retries

        # Check if it's a subflow
        if task_id in self.subflow_definitions:
            subflow_def = self.subflow_definitions[task_id]
            return task_instance.try_number < subflow_def.max_retries

        return False

    def get_retry_delay_seconds(self, task_id: str) -> Optional[float]:
        """
        Get the retry delay for a task in seconds.

        Args:
            task_id: ID of the task

        Returns:
            Retry delay in seconds, or None if no delay is configured
        """
        # Check standard tasks
        if task_id in self.task_definitions:
            task_def = self.task_definitions[task_id]
            return (
                task_def.retry_delay.total_seconds()
                if task_def.retry_delay
                else None
            )

        # Check subflows
        if task_id in self.subflow_definitions:
            subflow_def = self.subflow_definitions[task_id]
            return (
                subflow_def.retry_delay.total_seconds()
                if subflow_def.retry_delay
                else None
            )

        return None
