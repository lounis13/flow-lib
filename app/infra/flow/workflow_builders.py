# app/infra/flow/workflow_builders.py
"""
Builder pattern for workflow construction.

This module provides a fluent builder API for constructing workflows programmatically.
"""
from __future__ import annotations

from datetime import timedelta
from typing import Any, Callable, List, Optional

from app.infra.flow.flow import AsyncFlow
from app.infra.flow.models import Context, Store, TaskType


# ============================================================
#                   BUILDER PATTERN
# ============================================================
class FlowBuilder:
    """
    Fluent builder for constructing workflows programmatically.

    Provides a chainable API for adding tasks, subflows, and dependencies.

    Example:
        ```python
        builder = FlowBuilder("etl_pipeline", store)
        flow = (builder
            .add_task("extract", extract_func)
            .add_task("transform", transform_func, depends_on=["extract"])
            .add_task("load", load_func, depends_on=["transform"])
            .with_retry("extract", max_retries=3, delay=timedelta(seconds=5))
            .build())
        ```
    """

    def __init__(self, flow_name: str, store: Optional[Store] = None):
        """
        Initialize the flow builder.

        Args:
            flow_name: Name of the workflow
            store: Optional storage backend
        """
        self._flow = AsyncFlow(flow_name, store)
        self._parallel_groups: List[List[str]] = []

    def add_task(
        self,
        task_id: str,
        task_function: Callable[[Context], Any],
        *,
        depends_on: Optional[List[str]] = None,
        max_retries: int = 0,
        retry_delay: Optional[timedelta] = None,
        task_type: TaskType = TaskType.TASK,
    ) -> FlowBuilder:
        """
        Add a task to the workflow.

        Args:
            task_id: Unique identifier for the task
            task_function: Function to execute for this task
            depends_on: List of task IDs that must complete before this task
            max_retries: Maximum retry attempts on failure
            retry_delay: Delay between retry attempts
            task_type: Type of task (usually TASK)

        Returns:
            Self for method chaining
        """
        self._flow.add_task_definition(
            task_id=task_id,
            task_function=task_function,
            dependencies=depends_on,
            max_retries=max_retries,
            retry_delay=retry_delay,
            task_type=task_type,
        )
        return self

    def add_subflow(
        self,
        subflow_id: str,
        child_flow: AsyncFlow,
        *,
        depends_on: Optional[List[str]] = None,
        max_retries: int = 0,
        retry_delay: Optional[timedelta] = None,
    ) -> FlowBuilder:
        """
        Add a subflow to the workflow.

        Args:
            subflow_id: Unique identifier for the subflow
            child_flow: AsyncFlow instance to execute as subflow
            depends_on: List of task IDs that must complete before this subflow
            max_retries: Maximum retry attempts on failure
            retry_delay: Delay between retry attempts

        Returns:
            Self for method chaining
        """
        self._flow.add_subflow_definition(
            subflow_id=subflow_id,
            child_flow=child_flow,
            dependencies=depends_on,
            max_retries=max_retries,
            retry_delay=retry_delay,
        )
        return self

    def add_parallel(self, task_ids: List[str]) -> FlowBuilder:
        """
        Mark a group of tasks as parallel (they all depend on the same upstream).

        This is a convenience method that doesn't modify dependencies,
        but documents parallel execution intent.

        Args:
            task_ids: List of task IDs that will run in parallel

        Returns:
            Self for method chaining
        """
        self._parallel_groups.append(task_ids)
        return self

    def with_retry(
        self,
        task_id: str,
        max_retries: int,
        delay: Optional[timedelta] = None,
    ) -> FlowBuilder:
        """
        Configure retry policy for an existing task.

        Args:
            task_id: ID of the task to configure
            max_retries: Maximum number of retry attempts
            delay: Optional delay between retries

        Returns:
            Self for method chaining

        Raises:
            KeyError: If task_id doesn't exist
        """
        self._flow.update_task_retry_policy(task_id, max_retries, delay)
        return self

    def build(self) -> AsyncFlow:
        """
        Build and return the AsyncFlow instance.

        Returns:
            The constructed AsyncFlow ready for execution
        """
        return self._flow
