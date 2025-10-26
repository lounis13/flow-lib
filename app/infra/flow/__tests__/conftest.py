"""
Pytest configuration and DRY test utilities.

This module provides reusable fixtures, task factories, and helpers
for declarative workflow testing.
"""
import asyncio
import pytest
from typing import Any, Callable, Dict, List, Optional

from app.infra.flow import AsyncFlow, SQLiteStore, Context, ExecutionState


# ============================================================
#                   FIXTURES
# ============================================================

@pytest.fixture
def store(tmp_path):
    """
    Create a temporary SQLite store.

    The database file is automatically cleaned up after the test
    thanks to pytest's tmp_path fixture.

    Args:
        tmp_path: Pytest fixture providing temporary directory

    Yields:
        SQLiteStore: Opened store instance
    """
    db_path = tmp_path / "test_flow.db"
    store = SQLiteStore(str(db_path))
    store.open()
    yield store
    store.close()
    # tmp_path automatically cleans up the file


# ============================================================
#                   TASK FACTORIES (DRY)
# ============================================================

def create_simple_task(output_value: Any) -> Callable[[Context], Any]:
    """
    Factory: Create a task that returns a fixed value.

    Args:
        output_value: Value to return from the task

    Returns:
        Task function

    Example:
        task_fn = create_simple_task({"result": "success"})
    """
    def task(ctx: Context):
        return output_value
    return task


def create_delayed_task(delay_ms: int, output_value: Any) -> Callable:
    """
    Factory: Create an async task with a delay.

    Args:
        delay_ms: Delay in milliseconds
        output_value: Value to return after delay

    Returns:
        Async task function
    """
    async def task(ctx: Context):
        await asyncio.sleep(delay_ms / 1000)
        return output_value
    return task


def create_failing_task(error_msg: str) -> Callable[[Context], Any]:
    """
    Factory: Create a task that always fails.

    Args:
        error_msg: Error message to raise

    Returns:
        Task function that raises ValueError
    """
    def task(ctx: Context):
        raise ValueError(error_msg)
    return task


def create_pulldata_task(
    upstream_id: str,
    transform_fn: Optional[Callable[[Any], Any]] = None
) -> Callable[[Context], Any]:
    """
    Factory: Create a task that pulls data from upstream and optionally transforms it.

    Args:
        upstream_id: ID of upstream task to pull from
        transform_fn: Optional transformation function

    Returns:
        Task function

    Example:
        task_fn = create_pulldata_task("extract", lambda x: x * 2)
    """
    def task(ctx: Context):
        data = ctx.pull_output(upstream_id)
        if transform_fn:
            return transform_fn(data)
        return data
    return task


def create_counter_task() -> tuple[Callable[[Context], int], list]:
    """
    Factory: Create a task that increments a counter.

    Useful for tracking execution order and concurrency.

    Returns:
        Tuple of (task_function, execution_log)

    Example:
        task_fn, log = create_counter_task()
        # After execution, log contains execution timestamps
    """
    execution_log = []
    counter = [0]

    def task(ctx: Context):
        counter[0] += 1
        execution_log.append(counter[0])
        return counter[0]

    return task, execution_log


# ============================================================
#                   WORKFLOW HELPERS (DRY)
# ============================================================

async def run_and_assert(
    flow: AsyncFlow,
    store: SQLiteStore,
    expected_state: str,
    **params
) -> Any:
    """
    Execute workflow and assert final state.

    Args:
        flow: Workflow to execute
        store: Storage backend
        expected_state: Expected final state (e.g., ExecutionState.SUCCESS)
        **params: Workflow input parameters

    Returns:
        WorkflowRun instance after execution

    Raises:
        AssertionError: If final state doesn't match expected

    Example:
        run = await run_and_assert(flow, store, ExecutionState.SUCCESS, input=123)
    """
    # Ensure flow has store set
    if flow._store is None:
        flow._store = store

    run_id = flow.init_run(params=params)
    await flow.run_until_complete(run_id)
    workflow_run = store.load(run_id)
    assert workflow_run.state == expected_state, (
        f"Expected state {expected_state}, got {workflow_run.state}"
    )
    return workflow_run


def build_workflow(
    store: SQLiteStore,
    flow_name: str,
    tasks_spec: List[Dict[str, Any]]
) -> AsyncFlow:
    """
    Build a workflow from a declarative specification.

    This is the core DRY helper that eliminates repetitive workflow setup code.

    Args:
        store: Storage backend
        flow_name: Name of the workflow
        tasks_spec: List of task specifications, each containing:
            - id: Task identifier (required)
            - fn: Task function (required)
            - depends_on: List of upstream task IDs (optional)
            - max_retries: Max retry attempts (optional, default 0)
            - retry_delay: Delay between retries (optional)
            - task_type: Type of task (optional, default TASK)

    Returns:
        Configured AsyncFlow instance

    Example:
        workflow = build_workflow(store, "my_flow", [
            {"id": "task_a", "fn": create_simple_task("A")},
            {"id": "task_b", "fn": create_simple_task("B"), "depends_on": ["task_a"]},
            {"id": "task_c", "fn": create_simple_task("C"), "depends_on": ["task_a"], "max_retries": 3},
        ])
    """
    flow = AsyncFlow(flow_name, store)

    for spec in tasks_spec:
        flow.add_task_definition(
            task_id=spec["id"],
            task_function=spec["fn"],
            dependencies=spec.get("depends_on"),
            max_retries=spec.get("max_retries", 0),
            retry_delay=spec.get("retry_delay"),
            task_type=spec.get("task_type"),
        )

    return flow


# ============================================================
#                   ASSERTION HELPERS
# ============================================================

def assert_task_success(workflow_run, task_id: str, expected_output: Any = None):
    """
    Assert that a task completed successfully.

    Args:
        workflow_run: WorkflowRun instance
        task_id: ID of task to check
        expected_output: Optional expected output value
    """
    task = workflow_run.tasks[task_id]
    assert task.state == ExecutionState.SUCCESS, (
        f"Task {task_id} expected SUCCESS, got {task.state}"
    )
    if expected_output is not None:
        # For subflow tasks, extract the result from the structure
        actual_output = task.output_json
        if (task.type == "flow" and
            isinstance(actual_output, dict) and
            "result" in actual_output):
            actual_output = actual_output["result"]
        assert actual_output == expected_output, (
            f"Task {task_id} output mismatch: expected {expected_output}, got {actual_output}"
        )


def assert_task_failed(workflow_run, task_id: str, error_contains: Optional[str] = None):
    """
    Assert that a task failed.

    Args:
        workflow_run: WorkflowRun instance
        task_id: ID of task to check
        error_contains: Optional substring to check in error message
    """
    task = workflow_run.tasks[task_id]
    assert task.state == ExecutionState.FAILED, (
        f"Task {task_id} expected FAILED, got {task.state}"
    )
    if error_contains:
        assert error_contains in (task.error or ""), (
            f"Task {task_id} error message should contain '{error_contains}', got '{task.error}'"
        )


def assert_task_skipped(workflow_run, task_id: str):
    """
    Assert that a task was skipped.

    Args:
        workflow_run: WorkflowRun instance
        task_id: ID of task to check
    """
    task = workflow_run.tasks[task_id]
    assert task.state == ExecutionState.SKIPPED, (
        f"Task {task_id} expected SKIPPED, got {task.state}"
    )


def assert_all_tasks_success(workflow_run, task_ids: List[str]):
    """
    Assert that all specified tasks succeeded.

    Args:
        workflow_run: WorkflowRun instance
        task_ids: List of task IDs to check
    """
    for task_id in task_ids:
        assert_task_success(workflow_run, task_id)
