"""
Tests for workflows with sequential task execution.

This module tests workflows where tasks execute in a specific order
based on their dependencies (A -> B -> C).
"""
import pytest

from app.infra.flow import AsyncFlow, Context, ExecutionState
from conftest import (
    build_workflow,
    create_simple_task,
    create_pulldata_task,
    run_and_assert,
    assert_task_success,
    assert_all_tasks_success,
)


@pytest.mark.asyncio
async def test_two_tasks_sequential(store):
    """
    Test: A -> B (two tasks in sequence).

    Verifies:
    - Task A completes before task B starts
    - Both tasks succeed
    - Execution order is respected
    """
    workflow = build_workflow(store, "seq_2_tasks", [
        {"id": "task_a", "fn": create_simple_task("A")},
        {"id": "task_b", "fn": create_simple_task("B"), "depends_on": ["task_a"]},
    ])

    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)

    # Both tasks should succeed
    assert_task_success(run, "task_a", expected_output="A")
    assert_task_success(run, "task_b", expected_output="B")

    # Task B should complete after task A
    assert run.tasks["task_a"].end_date < run.tasks["task_b"].start_date


@pytest.mark.asyncio
async def test_three_tasks_chain(store):
    """
    Test: A -> B -> C (three tasks in a chain).

    Verifies:
    - Tasks execute in correct order
    - All tasks complete successfully
    """
    workflow = build_workflow(store, "seq_3_tasks", [
        {"id": "task_a", "fn": create_simple_task(1)},
        {"id": "task_b", "fn": create_simple_task(2), "depends_on": ["task_a"]},
        {"id": "task_c", "fn": create_simple_task(3), "depends_on": ["task_b"]},
    ])

    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)

    # All tasks succeed
    assert_all_tasks_success(run, ["task_a", "task_b", "task_c"])

    # Verify execution order
    assert run.tasks["task_a"].end_date < run.tasks["task_b"].start_date
    assert run.tasks["task_b"].end_date < run.tasks["task_c"].start_date


@pytest.mark.asyncio
@pytest.mark.parametrize("chain_length", [2, 3, 5, 10])
async def test_chain_of_n_tasks(store, chain_length):
    """
    Test: Parameterized chain of N tasks.

    Verifies:
    - Dynamic chain creation works
    - All N tasks execute sequentially
    - Correct number of tasks created
    """
    specs = [{"id": f"task_{i}", "fn": create_simple_task(i)} for i in range(chain_length)]

    # Add dependencies to create a chain
    for i in range(1, chain_length):
        specs[i]["depends_on"] = [f"task_{i-1}"]

    workflow = build_workflow(store, f"chain_{chain_length}", specs)
    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)

    # Verify all tasks exist and succeed
    assert len(run.tasks) == chain_length
    for i in range(chain_length):
        assert_task_success(run, f"task_{i}", expected_output=i)

    # Verify sequential execution order
    for i in range(chain_length - 1):
        task_current = run.tasks[f"task_{i}"]
        task_next = run.tasks[f"task_{i+1}"]
        assert task_current.end_date < task_next.start_date


@pytest.mark.asyncio
async def test_data_flow_between_tasks(store):
    """
    Test: Data flows correctly through sequential tasks using pull_output().

    Workflow: extract -> transform -> aggregate

    Verifies:
    - Downstream tasks can access upstream outputs
    - Data transformation works correctly
    - Context.pull_output() works as expected
    """
    def extract(ctx: Context):
        return {"raw_data": [1, 2, 3, 4, 5]}

    def transform(ctx: Context):
        raw = ctx.pull_output("extract")
        return {"doubled": [x * 2 for x in raw["raw_data"]]}

    def aggregate(ctx: Context):
        transformed = ctx.pull_output("transform")
        return {"sum": sum(transformed["doubled"])}

    workflow = build_workflow(store, "data_flow", [
        {"id": "extract", "fn": extract},
        {"id": "transform", "fn": transform, "depends_on": ["extract"]},
        {"id": "aggregate", "fn": aggregate, "depends_on": ["transform"]},
    ])

    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)

    # Verify data transformations
    assert_task_success(run, "extract", expected_output={"raw_data": [1, 2, 3, 4, 5]})
    assert_task_success(run, "transform", expected_output={"doubled": [2, 4, 6, 8, 10]})
    assert_task_success(run, "aggregate", expected_output={"sum": 30})


@pytest.mark.asyncio
async def test_sequential_with_pulldata_helper(store):
    """
    Test: Sequential tasks using create_pulldata_task helper.

    Verifies:
    - Helper function works correctly
    - Data transformation via lambda functions
    """
    workflow = build_workflow(store, "pulldata_helper", [
        {"id": "source", "fn": create_simple_task(10)},
        {"id": "double", "fn": create_pulldata_task("source", lambda x: x * 2), "depends_on": ["source"]},
        {"id": "triple", "fn": create_pulldata_task("double", lambda x: x * 3), "depends_on": ["double"]},
    ])

    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)

    assert_task_success(run, "source", expected_output=10)
    assert_task_success(run, "double", expected_output=20)
    assert_task_success(run, "triple", expected_output=60)


@pytest.mark.asyncio
async def test_sequential_with_decorator(store):
    """
    Test: Sequential tasks using @task decorator.

    Verifies:
    - Decorator-based sequential workflows work
    - Dependencies are respected
    """
    flow = AsyncFlow("decorator_sequential", store)

    @flow.task("step_1")
    def step_1(ctx: Context):
        return "first"

    @flow.task("step_2", depends_on=["step_1"])
    def step_2(ctx: Context):
        prev = ctx.pull_output("step_1")
        return prev + "_second"

    @flow.task("step_3", depends_on=["step_2"])
    def step_3(ctx: Context):
        prev = ctx.pull_output("step_2")
        return prev + "_third"

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)

    assert_task_success(run, "step_1", expected_output="first")
    assert_task_success(run, "step_2", expected_output="first_second")
    assert_task_success(run, "step_3", expected_output="first_second_third")


@pytest.mark.asyncio
async def test_etl_pattern(store):
    """
    Test: Classic ETL pattern (Extract -> Transform -> Load).

    Verifies:
    - Real-world ETL workflow works
    - Data passes correctly through pipeline
    """
    def extract(ctx: Context):
        # Simulate data extraction
        return {
            "records": [
                {"id": 1, "value": 100},
                {"id": 2, "value": 200},
                {"id": 3, "value": 300},
            ]
        }

    def transform(ctx: Context):
        # Transform data: add 10% tax
        data = ctx.pull_output("extract")
        transformed = []
        for record in data["records"]:
            transformed.append({
                "id": record["id"],
                "value": record["value"],
                "with_tax": record["value"] * 1.1
            })
        return {"transformed_records": transformed}

    def load(ctx: Context):
        # Load data: just count records
        data = ctx.pull_output("transform")
        return {
            "loaded": True,
            "count": len(data["transformed_records"]),
            "total_with_tax": sum(r["with_tax"] for r in data["transformed_records"])
        }

    workflow = build_workflow(store, "etl_workflow", [
        {"id": "extract", "fn": extract},
        {"id": "transform", "fn": transform, "depends_on": ["extract"]},
        {"id": "load", "fn": load, "depends_on": ["transform"]},
    ])

    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)

    # Verify ETL results
    assert run.tasks["extract"].output_json["records"][0]["id"] == 1
    assert round(run.tasks["transform"].output_json["transformed_records"][0]["with_tax"], 2) == 110.0
    assert run.tasks["load"].output_json["count"] == 3
    assert round(run.tasks["load"].output_json["total_with_tax"], 2) == 660.0


@pytest.mark.asyncio
async def test_long_chain_with_state_accumulation(store):
    """
    Test: Long chain where each task accumulates state.

    Verifies:
    - Complex data passing through many tasks
    - State accumulation works correctly
    """
    def start(ctx: Context):
        return {"chain": ["start"]}

    def add_step(step_name):
        def task(ctx: Context):
            prev = ctx.pull_output(f"step_{step_name - 1}" if step_name > 1 else "start")
            chain = prev["chain"].copy()
            chain.append(f"step_{step_name}")
            return {"chain": chain}
        return task

    specs = [{"id": "start", "fn": start}]
    for i in range(1, 6):
        specs.append({
            "id": f"step_{i}",
            "fn": add_step(i),
            "depends_on": [f"step_{i-1}" if i > 1 else "start"]
        })

    workflow = build_workflow(store, "accumulation_chain", specs)
    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)

    # Verify final accumulated state
    final_output = run.tasks["step_5"].output_json
    assert final_output["chain"] == ["start", "step_1", "step_2", "step_3", "step_4", "step_5"]


@pytest.mark.asyncio
async def test_sequential_multiple_dependencies(store):
    """
    Test: Task depending on multiple upstream tasks in sequence.

    Workflow:
      A -> B -> C
           B -> D (D depends on B, which depends on A)

    Verifies:
    - Task can have dependency on middle of chain
    - Execution order is correct
    """
    workflow = build_workflow(store, "multi_dep_seq", [
        {"id": "task_a", "fn": create_simple_task("A")},
        {"id": "task_b", "fn": create_simple_task("B"), "depends_on": ["task_a"]},
        {"id": "task_c", "fn": create_simple_task("C"), "depends_on": ["task_b"]},
        {"id": "task_d", "fn": create_simple_task("D"), "depends_on": ["task_b"]},
    ])

    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)

    # Verify all succeed
    assert_all_tasks_success(run, ["task_a", "task_b", "task_c", "task_d"])

    # Verify task_a completes before task_b
    assert run.tasks["task_a"].end_date < run.tasks["task_b"].start_date

    # Verify task_b completes before both task_c and task_d start
    assert run.tasks["task_b"].end_date < run.tasks["task_c"].start_date
    assert run.tasks["task_b"].end_date < run.tasks["task_d"].start_date


@pytest.mark.asyncio
async def test_pull_from_non_immediate_upstream(store):
    """
    Test: Task pulling data from non-immediate upstream (skip a level).

    Workflow: A -> B -> C (C pulls from A, not just B)

    Verifies:
    - Can pull from any upstream task, not just immediate parent
    """
    def task_a(ctx: Context):
        return {"value": "from_A"}

    def task_b(ctx: Context):
        return {"value": "from_B"}

    def task_c(ctx: Context):
        # Pull from both B and A (skip level)
        from_a = ctx.pull_output("task_a")
        from_b = ctx.pull_output("task_b")
        return {
            "from_a": from_a["value"],
            "from_b": from_b["value"],
            "combined": f"{from_a['value']}_{from_b['value']}"
        }

    workflow = build_workflow(store, "skip_level", [
        {"id": "task_a", "fn": task_a},
        {"id": "task_b", "fn": task_b, "depends_on": ["task_a"]},
        {"id": "task_c", "fn": task_c, "depends_on": ["task_b"]},
    ])

    run = await run_and_assert(workflow, store, ExecutionState.SUCCESS)

    # Verify task_c can access both upstream tasks
    assert run.tasks["task_c"].output_json["from_a"] == "from_A"
    assert run.tasks["task_c"].output_json["from_b"] == "from_B"
    assert run.tasks["task_c"].output_json["combined"] == "from_A_from_B"
