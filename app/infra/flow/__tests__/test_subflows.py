"""
Tests for workflows with nested subflows.

This module tests workflows that contain subflows, including:
- Single subflow execution
- Nested subflows (subflow within subflow)
- Subflows with dependencies
- Subflows accessing parent workflow params
"""
import pytest

from app.infra.flow import AsyncFlow, Context, ExecutionState
from conftest import (
    run_and_assert,
    assert_task_success,
    assert_all_tasks_success,
)


@pytest.mark.asyncio
async def test_single_subflow(store):
    """
    Test: Workflow with a single subflow.

    Verifies:
    - Subflow executes successfully
    - Parent workflow waits for subflow completion
    - Subflow tasks are tracked correctly
    """
    # Create the subflow
    subflow = AsyncFlow("sub")

    @subflow.task("sub_task")
    def sub_task(ctx: Context):
        return "subflow_result"

    # Create parent flow
    parent = AsyncFlow("parent")

    @parent.subflow("execute_subflow")
    def execute_subflow(ctx: Context):
        return subflow

    run = await run_and_assert(parent, store, ExecutionState.SUCCESS)
    assert_task_success(run, "execute_subflow", "subflow_result")


@pytest.mark.asyncio
async def test_subflow_with_dependencies(store):
    """
    Test: Subflow with task dependencies before and after.

    Topology:
        task_a → subflow → task_b

    Verifies:
    - task_a completes before subflow starts
    - task_b waits for subflow completion
    - All tasks execute in correct order
    """
    subflow = AsyncFlow("sub")

    @subflow.task("sub_task")
    def sub_task(ctx: Context):
        return "sub_result"

    parent = AsyncFlow("parent")

    @parent.task("task_a")
    def task_a(ctx: Context):
        return "a_result"

    @parent.subflow("execute_subflow", depends_on=["task_a"])
    def execute_subflow(ctx: Context):
        return subflow

    @parent.task("task_b", depends_on=["execute_subflow"])
    def task_b(ctx: Context):
        a_result = ctx.pull("task_a")
        sub_result = ctx.pull("execute_subflow")
        return f"{a_result}_{sub_result}"

    run = await run_and_assert(parent, store, ExecutionState.SUCCESS)
    assert_all_tasks_success(run, ["task_a", "execute_subflow", "task_b"])
    assert_task_success(run, "task_b", "a_result_sub_result")


@pytest.mark.asyncio
async def test_nested_subflows_two_levels(store):
    """
    Test: Subflow containing another subflow (2 levels deep).

    Structure:
        parent → level1_subflow → level2_subflow

    Verifies:
    - Nested subflows execute correctly
    - Parent waits for all nested executions
    - Results propagate correctly
    """
    # Level 2 (deepest)
    level2 = AsyncFlow("level2")

    @level2.task("deep_task")
    def deep_task(ctx: Context):
        return "deep_value"

    # Level 1 (contains level2)
    level1 = AsyncFlow("level1")

    @level1.subflow("nested_subflow")
    def nested_subflow(ctx: Context):
        return level2

    @level1.task("level1_task", depends_on=["nested_subflow"])
    def level1_task(ctx: Context):
        deep_result = ctx.pull("nested_subflow")
        return f"level1_{deep_result}"

    # Parent (contains level1)
    parent = AsyncFlow("parent")

    @parent.subflow("execute_level1")
    def execute_level1(ctx: Context):
        return level1

    @parent.task("parent_task", depends_on=["execute_level1"])
    def parent_task(ctx: Context):
        level1_result = ctx.pull("execute_level1")
        return f"parent_{level1_result}"

    run = await run_and_assert(parent, store, ExecutionState.SUCCESS)
    assert_task_success(run, "execute_level1", "level1_deep_value")
    assert_task_success(run, "parent_task", "parent_level1_deep_value")


@pytest.mark.asyncio
async def test_nested_subflows_three_levels(store):
    """
    Test: Three levels of nested subflows.

    Structure:
        parent → sub1 → sub2 → sub3

    Verifies:
    - Deep nesting works correctly
    - Results bubble up through all levels
    """
    # Level 3 (deepest)
    sub3 = AsyncFlow("sub3")

    @sub3.task("task3")
    def task3(ctx: Context):
        return "L3"

    # Level 2
    sub2 = AsyncFlow("sub2")

    @sub2.subflow("call_sub3")
    def call_sub3(ctx: Context):
        return sub3

    @sub2.task("task2", depends_on=["call_sub3"])
    def task2(ctx: Context):
        result3 = ctx.pull("call_sub3")
        return f"L2_{result3}"

    # Level 1
    sub1 = AsyncFlow("sub1")

    @sub1.subflow("call_sub2")
    def call_sub2(ctx: Context):
        return sub2

    @sub1.task("task1", depends_on=["call_sub2"])
    def task1(ctx: Context):
        result2 = ctx.pull("call_sub2")
        return f"L1_{result2}"

    # Parent
    parent = AsyncFlow("parent")

    @parent.subflow("call_sub1")
    def call_sub1(ctx: Context):
        return sub1

    run = await run_and_assert(parent, store, ExecutionState.SUCCESS)
    assert_task_success(run, "call_sub1", "L1_L2_L3")


@pytest.mark.asyncio
async def test_multiple_subflows_parallel(store):
    """
    Test: Multiple subflows executing in parallel.

    Topology:
        parent → [subflow_a, subflow_b, subflow_c] → aggregator

    Verifies:
    - Multiple subflows can execute concurrently
    - Aggregator waits for all subflows
    - All results are collected correctly
    """
    # Subflow A
    sub_a = AsyncFlow("sub_a")

    @sub_a.task("task_a")
    def task_a(ctx: Context):
        return "A"

    # Subflow B
    sub_b = AsyncFlow("sub_b")

    @sub_b.task("task_b")
    def task_b(ctx: Context):
        return "B"

    # Subflow C
    sub_c = AsyncFlow("sub_c")

    @sub_c.task("task_c")
    def task_c(ctx: Context):
        return "C"

    # Parent
    parent = AsyncFlow("parent")

    @parent.subflow("run_sub_a")
    def run_sub_a(ctx: Context):
        return sub_a

    @parent.subflow("run_sub_b")
    def run_sub_b(ctx: Context):
        return sub_b

    @parent.subflow("run_sub_c")
    def run_sub_c(ctx: Context):
        return sub_c

    @parent.task("aggregate", depends_on=["run_sub_a", "run_sub_b", "run_sub_c"])
    def aggregate(ctx: Context):
        a = ctx.pull("run_sub_a")
        b = ctx.pull("run_sub_b")
        c = ctx.pull("run_sub_c")
        return f"{a}{b}{c}"

    run = await run_and_assert(parent, store, ExecutionState.SUCCESS)
    assert_all_tasks_success(run, ["run_sub_a", "run_sub_b", "run_sub_c", "aggregate"])
    assert_task_success(run, "aggregate", "ABC")


@pytest.mark.asyncio
async def test_subflow_with_multiple_tasks(store):
    """
    Test: Subflow containing multiple sequential tasks.

    Verifies:
    - Subflow with complex internal DAG works correctly
    - Subflow returns result from its last task
    - Parent can access subflow's final result
    """
    subflow = AsyncFlow("sub")

    @subflow.task("extract")
    def extract(ctx: Context):
        return [1, 2, 3]

    @subflow.task("transform", depends_on=["extract"])
    def transform(ctx: Context):
        data = ctx.pull("extract")
        return [x * 2 for x in data]

    @subflow.task("load", depends_on=["transform"])
    def load(ctx: Context):
        data = ctx.pull("transform")
        return sum(data)  # 2+4+6 = 12

    parent = AsyncFlow("parent")

    @parent.subflow("etl_subflow")
    def etl_subflow(ctx: Context):
        return subflow

    @parent.task("verify", depends_on=["etl_subflow"])
    def verify(ctx: Context):
        result = ctx.pull("etl_subflow")
        return result == 12

    run = await run_and_assert(parent, store, ExecutionState.SUCCESS)
    assert_task_success(run, "etl_subflow", 12)
    assert_task_success(run, "verify", True)


@pytest.mark.asyncio
async def test_subflow_accessing_parent_params(store):
    """
    Test: Subflow accessing parent workflow parameters.

    Verifies:
    - Subflow can access params from parent workflow
    - Context correctly provides parent params to subflow tasks
    """
    subflow = AsyncFlow("sub")

    @subflow.task("use_parent_param")
    def use_parent_param(ctx: Context):
        # Access parent workflow params
        multiplier = ctx.params.get("multiplier", 1)
        return 10 * multiplier

    parent = AsyncFlow("parent")

    @parent.subflow("execute_subflow")
    def execute_subflow(ctx: Context):
        return subflow

    run = await run_and_assert(parent, store, ExecutionState.SUCCESS, multiplier=5)
    assert_task_success(run, "execute_subflow", 50)


@pytest.mark.asyncio
async def test_subflow_with_parallel_tasks(store):
    """
    Test: Subflow containing parallel tasks.

    Topology inside subflow:
        [task_a, task_b, task_c] → aggregator

    Verifies:
    - Subflow can contain parallel execution
    - Parent waits for entire subflow to complete
    """
    subflow = AsyncFlow("sub")

    @subflow.task("parallel_a")
    def parallel_a(ctx: Context):
        return 1

    @subflow.task("parallel_b")
    def parallel_b(ctx: Context):
        return 2

    @subflow.task("parallel_c")
    def parallel_c(ctx: Context):
        return 3

    @subflow.task("sum_all", depends_on=["parallel_a", "parallel_b", "parallel_c"])
    def sum_all(ctx: Context):
        a = ctx.pull("parallel_a")
        b = ctx.pull("parallel_b")
        c = ctx.pull("parallel_c")
        return a + b + c

    parent = AsyncFlow("parent")

    @parent.subflow("parallel_subflow")
    def parallel_subflow(ctx: Context):
        return subflow

    run = await run_and_assert(parent, store, ExecutionState.SUCCESS)
    assert_task_success(run, "parallel_subflow", 6)


@pytest.mark.asyncio
async def test_diamond_pattern_with_subflows(store):
    """
    Test: Diamond pattern where branches are subflows.

    Topology:
        start → [subflow_a, subflow_b] → end

    Verifies:
    - Subflows in diamond pattern work correctly
    - End task waits for both subflows
    """
    sub_a = AsyncFlow("sub_a")

    @sub_a.task("process_a")
    def process_a(ctx: Context):
        return "branch_a"

    sub_b = AsyncFlow("sub_b")

    @sub_b.task("process_b")
    def process_b(ctx: Context):
        return "branch_b"

    parent = AsyncFlow("parent")

    @parent.task("start")
    def start(ctx: Context):
        return "start_data"

    @parent.subflow("branch_a", depends_on=["start"])
    def branch_a(ctx: Context):
        return sub_a

    @parent.subflow("branch_b", depends_on=["start"])
    def branch_b(ctx: Context):
        return sub_b

    @parent.task("end", depends_on=["branch_a", "branch_b"])
    def end(ctx: Context):
        a = ctx.pull("branch_a")
        b = ctx.pull("branch_b")
        return f"{a}+{b}"

    run = await run_and_assert(parent, store, ExecutionState.SUCCESS)
    assert_task_success(run, "end", "branch_a+branch_b")


@pytest.mark.asyncio
async def test_complex_nested_parallel_subflows(store):
    """
    Test: Complex scenario with nested subflows and parallel execution.

    Structure:
        parent → [sub1 (contains parallel tasks), sub2 (nested subflow)] → final

    Verifies:
    - Complex nesting with parallel execution works correctly
    - All nested tasks complete successfully
    """
    # Sub1: Contains parallel tasks
    sub1 = AsyncFlow("sub1")

    @sub1.task("sub1_a")
    def sub1_a(ctx: Context):
        return 10

    @sub1.task("sub1_b")
    def sub1_b(ctx: Context):
        return 20

    @sub1.task("sub1_sum", depends_on=["sub1_a", "sub1_b"])
    def sub1_sum(ctx: Context):
        return ctx.pull("sub1_a") + ctx.pull("sub1_b")

    # Sub2: Contains nested subflow
    sub2_inner = AsyncFlow("sub2_inner")

    @sub2_inner.task("inner_task")
    def inner_task(ctx: Context):
        return 5

    sub2 = AsyncFlow("sub2")

    @sub2.subflow("nested")
    def nested(ctx: Context):
        return sub2_inner

    @sub2.task("multiply", depends_on=["nested"])
    def multiply(ctx: Context):
        nested_result = ctx.pull("nested")
        return nested_result * 2

    # Parent
    parent = AsyncFlow("parent")

    @parent.subflow("run_sub1")
    def run_sub1(ctx: Context):
        return sub1

    @parent.subflow("run_sub2")
    def run_sub2(ctx: Context):
        return sub2

    @parent.task("final", depends_on=["run_sub1", "run_sub2"])
    def final(ctx: Context):
        result1 = ctx.pull("run_sub1")  # 30
        result2 = ctx.pull("run_sub2")  # 10
        return result1 + result2  # 40

    run = await run_and_assert(parent, store, ExecutionState.SUCCESS)
    assert_task_success(run, "run_sub1", 30)
    assert_task_success(run, "run_sub2", 10)
    assert_task_success(run, "final", 40)
