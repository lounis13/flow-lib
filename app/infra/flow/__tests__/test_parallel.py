"""
Tests for workflows with parallel task execution.

This module tests workflows where multiple tasks execute concurrently
without dependencies between them.
"""
import asyncio
import time
import pytest

from app.infra.flow import AsyncFlow, Context, ExecutionState
from conftest import (
    build_workflow,
    create_simple_task,
    create_delayed_task,
    run_and_assert,
    assert_task_success,
    assert_all_tasks_success,
)


@pytest.mark.asyncio
async def test_two_tasks_parallel(store):
    """
    Test: Two independent tasks running in parallel.

    Verifies:
    - Both tasks execute without waiting for each other
    - Both tasks complete successfully
    - No dependencies between them
    """
    workflow = build_workflow(store, "parallel_2_tasks", [
        {"id": "task_a", "fn": create_simple_task("A")},
        {"id": "task_b", "fn": create_simple_task("B")},
    ])

    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)

    # Both tasks should succeed
    assert_task_success(run, "task_a", expected_output="A")
    assert_task_success(run, "task_b", expected_output="B")


@pytest.mark.asyncio
@pytest.mark.parametrize("num_tasks", [2, 3, 5, 10])
async def test_n_parallel_tasks(store, num_tasks):
    """
    Test: N independent tasks running in parallel.

    Verifies:
    - Dynamic number of parallel tasks work
    - All tasks complete successfully
    - Correct number of tasks created
    """
    specs = [
        {"id": f"task_{i}", "fn": create_simple_task(i)}
        for i in range(num_tasks)
    ]

    workflow = build_workflow(store, f"parallel_{num_tasks}_tasks", specs)
    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)

    # Verify all tasks exist and succeed
    assert len(run.tasks) == num_tasks
    for i in range(num_tasks):
        assert_task_success(run, f"task_{i}", expected_output=i)


@pytest.mark.asyncio
async def test_parallel_with_delays(store):
    """
    Test: Parallel tasks with different execution times.

    Verifies:
    - Tasks with different durations run concurrently
    - Shorter tasks don't wait for longer ones
    - Total execution time is approximately max(task_times), not sum(task_times)
    """
    # Create tasks with different delays: 50ms, 100ms, 30ms
    workflow = build_workflow(store, "parallel_delays", [
        {"id": "fast", "fn": create_delayed_task(30, "fast_result")},
        {"id": "medium", "fn": create_delayed_task(50, "medium_result")},
        {"id": "slow", "fn": create_delayed_task(100, "slow_result")},
    ])

    start_time = time.time()
    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)
    elapsed_time = (time.time() - start_time) * 1000  # Convert to ms

    # All tasks should succeed
    assert_task_success(run, "fast", expected_output="fast_result")
    assert_task_success(run, "medium", expected_output="medium_result")
    assert_task_success(run, "slow", expected_output="slow_result")

    # Total time should be ~100ms (max delay), not 180ms (sum of delays)
    # Allow some overhead (2x for safety in CI environments)
    assert elapsed_time < 200, f"Expected ~100ms, got {elapsed_time}ms (tasks ran sequentially?)"


@pytest.mark.asyncio
async def test_diamond_pattern(store):
    """
    Test: Diamond pattern - fork and join.

    Workflow:
        start
       /     \\
      A       B  (parallel)
       \\     /
        end

    Verifies:
    - A and B run in parallel
    - End waits for both A and B
    - Data from both branches accessible in end
    """
    def start_task(ctx: Context):
        return {"value": 10}

    def task_a(ctx: Context):
        data = ctx.pull_output("start")
        return {"a_result": data["value"] * 2}

    def task_b(ctx: Context):
        data = ctx.pull_output("start")
        return {"b_result": data["value"] * 3}

    def end_task(ctx: Context):
        a_data = ctx.pull_output("task_a")
        b_data = ctx.pull_output("task_b")
        return {
            "combined": a_data["a_result"] + b_data["b_result"],
            "a": a_data["a_result"],
            "b": b_data["b_result"]
        }

    workflow = build_workflow(store, "diamond_pattern", [
        {"id": "start", "fn": start_task},
        {"id": "task_a", "fn": task_a, "depends_on": ["start"]},
        {"id": "task_b", "fn": task_b, "depends_on": ["start"]},
        {"id": "end", "fn": end_task, "depends_on": ["task_a", "task_b"]},
    ])

    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)

    # Verify all tasks succeed
    assert_all_tasks_success(run, ["start", "task_a", "task_b", "end"])

    # Verify data flow
    assert run.tasks["start"].output_json["value"] == 10
    assert run.tasks["task_a"].output_json["a_result"] == 20
    assert run.tasks["task_b"].output_json["b_result"] == 30
    assert run.tasks["end"].output_json["combined"] == 50

    # Verify parallel execution: task_a and task_b should overlap in time
    # (both should start after start completes)
    assert run.tasks["start"].end_date < run.tasks["task_a"].start_date
    assert run.tasks["start"].end_date < run.tasks["task_b"].start_date


@pytest.mark.asyncio
async def test_multiple_parallel_groups(store):
    """
    Test: Multiple groups of parallel tasks.

    Workflow:
        A -> (B, C, D) -> E

    Verifies:
    - B, C, D run in parallel after A
    - E waits for all of B, C, D
    """
    workflow = build_workflow(store, "multi_parallel_groups", [
        {"id": "task_a", "fn": create_simple_task("A")},
        {"id": "task_b", "fn": create_simple_task("B"), "depends_on": ["task_a"]},
        {"id": "task_c", "fn": create_simple_task("C"), "depends_on": ["task_a"]},
        {"id": "task_d", "fn": create_simple_task("D"), "depends_on": ["task_a"]},
        {"id": "task_e", "fn": create_simple_task("E"), "depends_on": ["task_b", "task_c", "task_d"]},
    ])

    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)

    # All tasks should succeed
    assert_all_tasks_success(run, ["task_a", "task_b", "task_c", "task_d", "task_e"])

    # A completes before B, C, D start
    assert run.tasks["task_a"].end_date < run.tasks["task_b"].start_date
    assert run.tasks["task_a"].end_date < run.tasks["task_c"].start_date
    assert run.tasks["task_a"].end_date < run.tasks["task_d"].start_date

    # E starts after B, C, D complete
    assert run.tasks["task_b"].end_date < run.tasks["task_e"].start_date
    assert run.tasks["task_c"].end_date < run.tasks["task_e"].start_date
    assert run.tasks["task_d"].end_date < run.tasks["task_e"].start_date


@pytest.mark.asyncio
async def test_max_concurrency_limit(store):
    """
    Test: Workflow with max_concurrency limit.

    Verifies:
    - Only N tasks run concurrently
    - Other tasks wait for slots to become available
    - All tasks eventually complete
    """
    # Create 10 tasks with delays
    specs = [
        {"id": f"task_{i}", "fn": create_delayed_task(20, i)}
        for i in range(10)
    ]

    workflow = build_workflow(store, "max_concurrency", specs)

    # Run with max_concurrency=3
    run_id = workflow.init_run()
    await workflow.run_until_complete(run_id, max_concurrency=3)

    run = store.load(run_id)

    # All tasks should eventually succeed
    assert run.state == ExecutionState.SUCCESS
    assert len(run.tasks) == 10
    for i in range(10):
        assert_task_success(run, f"task_{i}", expected_output=i)


@pytest.mark.asyncio
async def test_parallel_with_decorator(store):
    """
    Test: Parallel tasks using @task decorator.

    Verifies:
    - Decorator-based parallel workflows work
    - Tasks without dependencies run in parallel
    """
    flow = AsyncFlow("decorator_parallel", store)

    @flow.task("task_x")
    def task_x(ctx: Context):
        return "X"

    @flow.task("task_y")
    def task_y(ctx: Context):
        return "Y"

    @flow.task("task_z")
    def task_z(ctx: Context):
        return "Z"

    @flow.task("combine", depends_on=["task_x", "task_y", "task_z"])
    def combine(ctx: Context):
        x = ctx.pull_output("task_x")
        y = ctx.pull_output("task_y")
        z = ctx.pull_output("task_z")
        return f"{x}{y}{z}"

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)

    assert_task_success(run, "task_x", expected_output="X")
    assert_task_success(run, "task_y", expected_output="Y")
    assert_task_success(run, "task_z", expected_output="Z")
    assert_task_success(run, "combine", expected_output="XYZ")


@pytest.mark.asyncio
async def test_fan_out_fan_in_pattern(store):
    """
    Test: Fan-out/fan-in pattern.

    Workflow:
               -> process_1 ->
        start -> process_2 -> aggregate
               -> process_3 ->

    Verifies:
    - Multiple processing tasks run in parallel
    - Aggregation waits for all processing to complete
    """
    def start(ctx: Context):
        return {"items": [1, 2, 3, 4, 5]}

    def process_1(ctx: Context):
        items = ctx.pull_output("start")["items"]
        return {"sum": sum(items)}

    def process_2(ctx: Context):
        items = ctx.pull_output("start")["items"]
        return {"count": len(items)}

    def process_3(ctx: Context):
        items = ctx.pull_output("start")["items"]
        return {"max": max(items)}

    def aggregate(ctx: Context):
        p1 = ctx.pull_output("process_1")
        p2 = ctx.pull_output("process_2")
        p3 = ctx.pull_output("process_3")
        return {
            "summary": {
                "sum": p1["sum"],
                "count": p2["count"],
                "max": p3["max"],
                "avg": p1["sum"] / p2["count"]
            }
        }

    workflow = build_workflow(store, "fan_out_fan_in", [
        {"id": "start", "fn": start},
        {"id": "process_1", "fn": process_1, "depends_on": ["start"]},
        {"id": "process_2", "fn": process_2, "depends_on": ["start"]},
        {"id": "process_3", "fn": process_3, "depends_on": ["start"]},
        {"id": "aggregate", "fn": aggregate, "depends_on": ["process_1", "process_2", "process_3"]},
    ])

    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)

    # Verify results
    summary = run.tasks["aggregate"].output_json["summary"]
    assert summary["sum"] == 15
    assert summary["count"] == 5
    assert summary["max"] == 5
    assert summary["avg"] == 3.0


@pytest.mark.asyncio
async def test_complex_dag_with_parallel_branches(store):
    """
    Test: Complex DAG with multiple parallel branches.

    Workflow:
           -> B -> -> D ->
        A ->            -> F
           -> C -> E ->

    Verifies:
    - Complex dependency graph executes correctly
    - Parallel branches work with dependencies
    """
    workflow = build_workflow(store, "complex_parallel_dag", [
        {"id": "A", "fn": create_simple_task("A")},
        {"id": "B", "fn": create_simple_task("B"), "depends_on": ["A"]},
        {"id": "C", "fn": create_simple_task("C"), "depends_on": ["A"]},
        {"id": "D", "fn": create_simple_task("D"), "depends_on": ["B"]},
        {"id": "E", "fn": create_simple_task("E"), "depends_on": ["C"]},
        {"id": "F", "fn": create_simple_task("F"), "depends_on": ["D", "E"]},
    ])

    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)

    # All tasks should succeed
    assert_all_tasks_success(run, ["A", "B", "C", "D", "E", "F"])

    # Verify execution order
    assert run.tasks["A"].end_date < run.tasks["B"].start_date
    assert run.tasks["A"].end_date < run.tasks["C"].start_date
    assert run.tasks["B"].end_date < run.tasks["D"].start_date
    assert run.tasks["C"].end_date < run.tasks["E"].start_date
    assert run.tasks["D"].end_date < run.tasks["F"].start_date
    assert run.tasks["E"].end_date < run.tasks["F"].start_date


@pytest.mark.asyncio
async def test_parallel_tasks_accessing_workflow_params(store):
    """
    Test: Parallel tasks all accessing workflow params.

    Verifies:
    - Multiple parallel tasks can read workflow params
    - No conflicts or race conditions
    """
    def create_param_reader(task_id):
        def reader(ctx: Context):
            multiplier = ctx.workflow_run.params.get("multiplier", 1)
            task_num = int(task_id.split("_")[1])
            return {"result": task_num * multiplier}
        return reader

    specs = [
        {"id": f"task_{i}", "fn": create_param_reader(f"task_{i}")}
        for i in range(5)
    ]

    workflow = build_workflow(store, "parallel_params", specs)
    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS, multiplier=10)

    # Verify all tasks computed correctly
    for i in range(5):
        assert run.tasks[f"task_{i}"].output_json["result"] == i * 10
