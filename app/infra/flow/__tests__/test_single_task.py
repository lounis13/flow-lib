"""
Tests for workflows with a single task.

This module tests the most basic workflow scenarios to ensure
core functionality works correctly.
"""
import pytest

from app.infra.flow import AsyncFlow, Context, ExecutionState
from conftest import (
    build_workflow,
    create_simple_task,
    run_and_assert,
    assert_task_success,
)


@pytest.mark.asyncio
async def test_single_task_success(store):
    """
    Test: Workflow with a single task that succeeds.

    Verifies:
    - Workflow completes with SUCCESS state
    - Task completes with SUCCESS state
    - Task execution timestamps are set
    """
    workflow = build_workflow(store, "single_task_success", [
        {"id": "task_1", "fn": create_simple_task("success")},
    ])

    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)

    # Assert workflow state
    assert run.state == ExecutionState.SUCCESS
    assert run.start_date is not None
    assert run.end_date is not None

    # Assert task state
    task = run.tasks["task_1"]
    assert task.state == ExecutionState.SUCCESS
    assert task.start_date is not None
    assert task.end_date is not None
    assert task.try_number == 0


@pytest.mark.asyncio
async def test_single_task_with_output(store):
    """
    Test: Single task that returns output data.

    Verifies:
    - Task output is correctly captured
    - Different data types (dict, list, string, int) work
    """
    test_cases = [
        ({"result": "data", "count": 42}, "dict_output"),
        ([1, 2, 3, 4, 5], "list_output"),
        ("simple_string", "string_output"),
        (12345, "int_output"),
        ({"nested": {"deep": {"value": True}}}, "nested_output"),
    ]

    for output_value, flow_name in test_cases:
        workflow = build_workflow(store, flow_name, [
            {"id": "task", "fn": create_simple_task(output_value)},
        ])

        run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)
        assert_task_success(run, "task", expected_output=output_value)


@pytest.mark.asyncio
async def test_single_task_with_params(store):
    """
    Test: Single task that accesses workflow parameters.

    Verifies:
    - Workflow params are accessible via context
    - Task can use params in its logic
    """
    def task_using_params(ctx: Context):
        # Access workflow params
        multiplier = ctx.workflow_run.params.get("multiplier", 1)
        base = ctx.workflow_run.params.get("base", 0)
        return base * multiplier

    workflow = build_workflow(store, "task_with_params", [
        {"id": "calculator", "fn": task_using_params},
    ])

    run = await run_and_assert(
        workflow,
        store,
        ExecutionState.SUCCESS,
        multiplier=3,
        base=10
    )

    assert_task_success(run, "calculator", expected_output=30)


@pytest.mark.asyncio
async def test_single_task_decorator(store):
    """
    Test: Single task registered using @task decorator.

    Verifies:
    - Decorator-based registration works
    - Task executes correctly
    """
    flow = AsyncFlow("decorator_test", store)

    @flow.task("my_task")
    def my_task(ctx: Context):
        return {"method": "decorator", "value": 100}

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)
    assert_task_success(run, "my_task", expected_output={"method": "decorator", "value": 100})


@pytest.mark.asyncio
async def test_single_task_builder(store):
    """
    Test: Single task registered using add_task_definition.

    Verifies:
    - Programmatic registration works
    - Task executes correctly
    - Both methods (decorator vs builder) produce same result
    """
    flow = AsyncFlow("builder_test", store)

    def my_task(ctx: Context):
        return {"method": "builder", "value": 200}

    # Use add_task_definition directly
    flow.add_task_definition(
        task_id="my_task",
        task_function=my_task,
    )

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)
    assert_task_success(run, "my_task", expected_output={"method": "builder", "value": 200})


@pytest.mark.asyncio
async def test_single_task_no_output(store):
    """
    Test: Single task that returns None (no explicit output).

    Verifies:
    - Task still completes successfully
    - output_json is None
    """
    def task_no_return(ctx: Context):
        # Task that doesn't return anything
        pass

    workflow = build_workflow(store, "no_output", [
        {"id": "silent_task", "fn": task_no_return},
    ])

    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)
    task = run.tasks["silent_task"]
    assert task.state == ExecutionState.SUCCESS
    assert task.output_json is None


@pytest.mark.asyncio
async def test_single_task_push_value(store):
    """
    Test: Single task using ctx.push() to set output.

    Verifies:
    - push() method works for setting output
    - Pushed value takes precedence over return value
    """
    def task_with_push(ctx: Context):
        ctx.push({"pushed": "value"})
        return {"returned": "ignored"}  # This should be ignored

    workflow = build_workflow(store, "push_test", [
        {"id": "pusher", "fn": task_with_push},
    ])

    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)
    # Pushed value should be the output, not the return value
    assert_task_success(run, "pusher", expected_output={"pushed": "value"})


@pytest.mark.asyncio
@pytest.mark.parametrize("task_id", ["task_a", "my_task", "extract_data", "task_123"])
async def test_single_task_various_ids(store, task_id):
    """
    Test: Single task with various valid task IDs.

    Verifies:
    - Different task ID naming conventions work
    """
    workflow = build_workflow(store, f"test_{task_id}", [
        {"id": task_id, "fn": create_simple_task("ok")},
    ])

    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)
    assert_task_success(run, task_id, expected_output="ok")
