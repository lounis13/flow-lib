# app/infra/flow/retry_manager.py
"""
Retry management for workflow tasks.

This module handles manual and automatic retry logic for failed tasks
and subflows, including downstream task reset capabilities.
"""
from __future__ import annotations

import logging
from typing import Optional

from app.infra.flow.models import (
    ExecutionState,
    Store,
    SubFlowDefinition,
    TaskDefinition,
    TaskType,
    WorkflowRun,
)

# Configure logger for retry manager
logger = logging.getLogger(__name__)


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
        Supports hierarchical task IDs (e.g., "ftb_flow.pricing_flow.calculate_price").

        Args:
            run_id: ID of the workflow run
            task_id: ID of the task to retry (can be nested path)
            reset_downstream: Whether to also reset downstream tasks

        Raises:
            KeyError: If the run_id or task_id does not exist
        """
        workflow_run = self.store.load(run_id)

        logger.info(
            f"Manual retry initiated: run_id={run_id}, task_id={task_id}, "
            f"reset_downstream={reset_downstream}"
        )

        # Check if this is a hierarchical task ID (contains ".")
        if "." in task_id:
            self._retry_hierarchical_task(workflow_run, task_id, reset_downstream)
            logger.info(
                f"Hierarchical retry completed: run_id={run_id}, task_id={task_id}"
            )
        else:
            # Simple case: task is at the current level
            self.reset_task_state(workflow_run, task_id)

            # Reset workflow state if it was terminal
            if ExecutionState.is_terminal(workflow_run.state):
                workflow_run.state = ExecutionState.RUNNING
                workflow_run.end_date = None

            # Optionally reset downstream tasks
            if reset_downstream and self.downstream_resolver:
                self.reset_downstream_tasks(workflow_run, task_id)

            self.store.save(workflow_run)
            logger.info(
                f"Manual retry completed: run_id={run_id}, task_id={task_id}"
            )

    def _retry_hierarchical_task(
        self,
        workflow_run: WorkflowRun,
        hierarchical_task_id: str,
        reset_downstream: bool,
    ) -> None:
        """
        Retry a task in a nested subflow using hierarchical task ID.

        This method:
        1. Parses the hierarchical path (e.g., "ftb_flow.pricing_flow.calculate_price")
        2. Navigates through subflows to find the target task
        3. Resets all parent subflows to RUNNING
        4. Resets the target task to SCHEDULED
        5. Optionally resets downstream tasks in the target subflow

        Args:
            workflow_run: The parent workflow run
            hierarchical_task_id: Full path to the task (e.g., "sub1.sub2.task")
            reset_downstream: Whether to reset downstream tasks

        Raises:
            KeyError: If any part of the path doesn't exist
        """
        # Parse the hierarchical path
        path_parts = hierarchical_task_id.split(".")

        # Navigate through the hierarchy
        current_run = workflow_run
        subflow_chain = []  # Track (run, subflow_task_id) pairs to set to RUNNING

        # Navigate to the deepest subflow containing the target task
        for i, part in enumerate(path_parts[:-1]):  # All parts except the last (task name)
            # Check if this part is a subflow in the current run
            if part not in current_run.tasks:
                raise KeyError(
                    f"Subflow '{part}' not found in run '{current_run.id}'. "
                    f"Available tasks: {list(current_run.tasks.keys())}"
                )

            subflow_task = current_run.tasks[part]
            if subflow_task.type != TaskType.FLOW:
                raise KeyError(
                    f"Task '{part}' is not a subflow (type: {subflow_task.type})"
                )

            # Track this subflow for later state update
            subflow_chain.append((current_run, part))

            # Get child run ID
            if not subflow_task.output_json or "child_run_id" not in subflow_task.output_json:
                raise KeyError(
                    f"Subflow '{part}' has no child_run_id. "
                    f"Output: {subflow_task.output_json}"
                )

            child_run_id = subflow_task.output_json["child_run_id"]
            current_run = self.store.load(child_run_id)

        # Now current_run is the deepest subflow, and path_parts[-1] is the task name
        target_task_id = path_parts[-1]

        # Reset the target task in the deepest subflow
        self.reset_task_state(current_run, target_task_id)

        # Reset the deepest subflow state if it was terminal
        if ExecutionState.is_terminal(current_run.state):
            current_run.state = ExecutionState.RUNNING
            current_run.end_date = None

        # Optionally reset downstream tasks in the target subflow
        # We reset downstream tasks in the CHILD flow (not the parent)
        if reset_downstream:
            # Reset all downstream tasks of the target task in the child flow
            # We need to iterate through all tasks and reset those that depend on the target
            self._reset_downstream_in_subflow(current_run, target_task_id)

        # Save the deepest subflow
        self.store.save(current_run)

        # Now walk back up the chain and reset all parent subflows to RUNNING
        for parent_run, subflow_task_id in reversed(subflow_chain):
            subflow_task = parent_run.tasks[subflow_task_id]

            # Reset subflow task state to SCHEDULED (will re-execute)
            subflow_task.state = ExecutionState.SCHEDULED
            subflow_task.end_date = None
            subflow_task.error = None
            # Don't reset try_number for subflows

            # Reset parent workflow state if it was terminal
            if ExecutionState.is_terminal(parent_run.state):
                parent_run.state = ExecutionState.RUNNING
                parent_run.end_date = None

            # Also reset downstream tasks in parent flows if reset_downstream is True
            if reset_downstream:
                self._reset_downstream_in_subflow(parent_run, subflow_task_id)

            self.store.save(parent_run)

    def _reset_downstream_in_subflow(
        self, workflow_run: WorkflowRun, target_task_id: str
    ) -> None:
        """
        Reset downstream tasks in a subflow without using dependency resolver.

        This is a simple implementation that resets all tasks that are in
        terminal state (SUCCESS, FAILED, SKIPPED) after the target task.
        It assumes tasks are ordered or we want to reset all terminal tasks.

        Args:
            workflow_run: The workflow run containing the tasks
            target_task_id: The task whose downstream tasks should be reset
        """
        # Simple approach: reset all tasks that are in terminal state
        # This is safe because they will only re-execute if their dependencies are met
        for task_id, task_instance in workflow_run.tasks.items():
            if task_id != target_task_id and ExecutionState.is_terminal(task_instance.state):
                task_instance.state = ExecutionState.SCHEDULED
                task_instance.end_date = None
                task_instance.error = None
                task_instance.try_number = 0

                # If it's a subflow, reset its tasks too
                if task_instance.type == TaskType.FLOW:
                    self._reset_subflow_tasks(task_instance)

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

    def get_all_failed_tasks(self, run_id: str) -> list[str]:
        """
        Get all tasks in failed state, including tasks in subflows.

        Args:
            run_id: ID of the workflow run

        Returns:
            List of task IDs that are in failed state
        """
        workflow_run = self.store.load(run_id)
        all_tasks = workflow_run.get_all_tasks_recursive(self.store)
        failed = [task_id for task_id, task in all_tasks.items() if task.state == ExecutionState.FAILED]

        if failed:
            logger.info(f"Found {len(failed)} failed tasks in run_id={run_id}: {failed}")

        return failed

    def retry_all_failed(self, run_id: str) -> list[str]:
        """
        Retry all failed tasks in a workflow run.

        This method finds all tasks in failed state and resets them
        without resetting downstream tasks (to avoid cascading resets).

        Args:
            run_id: ID of the workflow run

        Returns:
            List of task IDs that were retried

        Raises:
            ValueError: If no failed tasks are found
        """
        failed_tasks = self.get_all_failed_tasks(run_id)

        if not failed_tasks:
            logger.warning(f"No failed tasks found in run '{run_id}'")
            raise ValueError(f"No failed tasks found in run '{run_id}'")

        logger.info(f"Retrying {len(failed_tasks)} failed tasks in run_id={run_id}")

        # Retry each failed task without reset_downstream
        for task_id in failed_tasks:
            self.manual_retry(run_id=run_id, task_id=task_id, reset_downstream=False)

        logger.info(f"All failed tasks retried successfully in run_id={run_id}")
        return failed_tasks

    def validate_task_exists(self, run_id: str, task_id: str) -> None:
        """
        Validate that a task exists in the workflow run.

        Args:
            run_id: ID of the workflow run
            task_id: ID of the task to validate

        Raises:
            KeyError: If the run_id or task_id does not exist
        """
        workflow_run = self.store.load(run_id)
        all_tasks = workflow_run.get_all_tasks_recursive(self.store)

        if task_id not in all_tasks:
            available_tasks = list(all_tasks.keys())
            raise KeyError(
                f"Task '{task_id}' not found in run '{run_id}'. "
                f"Available tasks: {available_tasks}"
            )
