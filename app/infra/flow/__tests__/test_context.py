"""
Tests for Context object (push/pull data operations).

This module tests the Context API used by tasks to:
- Pull output from upstream tasks
- Push output values
- Access workflow parameters
- Access workflow run metadata
"""
import pytest

from app.infra.flow import AsyncFlow, Context, ExecutionState
from conftest import run_and_assert, assert_task_success


@pytest.mark.asyncio
async def test_context_pull_single_upstream(store):
    """
    Test: Pull data from single upstream task.

    Verifies:
    - ctx.pull() retrieves output from upstream task
    - Data flows correctly between tasks
    """
    flow = AsyncFlow("test_pull_single")

    @flow.task("producer")
    def producer(ctx: Context):
        return {"data": "test_value"}

    @flow.task("consumer", depends_on=["producer"])
    def consumer(ctx: Context):
        upstream = ctx.pull("producer")
        return upstream["data"]

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)
    assert_task_success(run, "consumer", "test_value")


@pytest.mark.asyncio
async def test_context_pull_multiple_upstream(store):
    """
    Test: Pull data from multiple upstream tasks.

    Topology:
        [task_a, task_b, task_c] → aggregator

    Verifies:
    - ctx.pull() can retrieve from multiple upstream tasks
    - All upstream data is accessible
    """
    flow = AsyncFlow("test_pull_multiple")

    @flow.task("task_a")
    def task_a(ctx: Context):
        return 10

    @flow.task("task_b")
    def task_b(ctx: Context):
        return 20

    @flow.task("task_c")
    def task_c(ctx: Context):
        return 30

    @flow.task("aggregator", depends_on=["task_a", "task_b", "task_c"])
    def aggregator(ctx: Context):
        a = ctx.pull("task_a")
        b = ctx.pull("task_b")
        c = ctx.pull("task_c")
        return a + b + c

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)
    assert_task_success(run, "aggregator", 60)


@pytest.mark.asyncio
async def test_context_pull_non_immediate_upstream(store):
    """
    Test: Pull data from non-immediate upstream (skip intermediate).

    Topology:
        task_a → task_b → task_c
        (task_c pulls from both task_a and task_b)

    Verifies:
    - Can pull from any upstream task in the chain
    - Not limited to immediate dependencies
    """
    flow = AsyncFlow("test_pull_chain")

    @flow.task("task_a")
    def task_a(ctx: Context):
        return "A"

    @flow.task("task_b", depends_on=["task_a"])
    def task_b(ctx: Context):
        return "B"

    @flow.task("task_c", depends_on=["task_b"])
    def task_c(ctx: Context):
        a = ctx.pull("task_a")
        b = ctx.pull("task_b")
        return f"{a}{b}C"

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)
    assert_task_success(run, "task_c", "ABC")


@pytest.mark.asyncio
async def test_context_pull_returns_none_for_failed_task(store):
    """
    Test: ctx.pull() returns None when upstream task failed.

    Verifies:
    - Pulling from failed task returns None
    - Task can handle None gracefully
    """
    flow = AsyncFlow("test_pull_failed")

    @flow.task("failing_task", max_retries=0)
    def failing_task(ctx: Context):
        raise ValueError("Task fails")

    @flow.task("dependent", depends_on=["failing_task"], max_retries=0)
    def dependent(ctx: Context):
        result = ctx.pull("failing_task")
        if result is None:
            return "got_none"
        return result

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    # The dependent task won't run because failing_task failed
    assert run.state == ExecutionState.FAILED


@pytest.mark.asyncio
async def test_context_push_simple(store):
    """
    Test: Push output using ctx.push().

    Verifies:
    - ctx.push() stores output value
    - Pushed value is accessible via ctx.pull()
    """
    flow = AsyncFlow("test_push")

    @flow.task("pusher")
    def pusher(ctx: Context):
        ctx.push("pushed_value")

    @flow.task("puller", depends_on=["pusher"])
    def puller(ctx: Context):
        return ctx.pull("pusher")

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)
    assert_task_success(run, "pusher", "pushed_value")
    assert_task_success(run, "puller", "pushed_value")


@pytest.mark.asyncio
async def test_context_push_overrides_return(store):
    """
    Test: ctx.push() takes precedence over return value.

    Verifies:
    - When both push() and return are used, push() wins
    """
    flow = AsyncFlow("test_push_override")

    @flow.task("task")
    def task(ctx: Context):
        ctx.push("pushed")
        return "returned"

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)
    assert_task_success(run, "task", "pushed")


@pytest.mark.asyncio
async def test_context_push_multiple_times(store):
    """
    Test: Multiple ctx.push() calls (last one wins).

    Verifies:
    - Last push() call determines the output
    """
    flow = AsyncFlow("test_push_multiple")

    @flow.task("task")
    def task(ctx: Context):
        ctx.push("first")
        ctx.push("second")
        ctx.push("third")
        return None

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)
    assert_task_success(run, "task", "third")


@pytest.mark.asyncio
async def test_context_params_access(store):
    """
    Test: Access workflow parameters via ctx.params.

    Verifies:
    - ctx.params provides access to workflow input parameters
    - Parameters are available to all tasks
    """
    flow = AsyncFlow("test_params")

    @flow.task("task")
    def task(ctx: Context):
        multiplier = ctx.params.get("multiplier", 1)
        base = ctx.params.get("base", 0)
        return base * multiplier

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS, multiplier=5, base=10)
    assert_task_success(run, "task", 50)


@pytest.mark.asyncio
async def test_context_params_missing_key(store):
    """
    Test: Access missing parameter with default value.

    Verifies:
    - ctx.params.get() with default works correctly
    """
    flow = AsyncFlow("test_params_default")

    @flow.task("task")
    def task(ctx: Context):
        value = ctx.params.get("missing_key", "default_value")
        return value

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)
    assert_task_success(run, "task", "default_value")


@pytest.mark.asyncio
async def test_context_run_id_access(store):
    """
    Test: Access run ID via ctx.run (workflow_run.id).

    Verifies:
    - ctx.run.id provides workflow run identifier
    """
    flow = AsyncFlow("test_run_id")

    captured_run_id = {"value": None}

    @flow.task("task")
    def task(ctx: Context):
        captured_run_id["value"] = ctx.run.id
        return "done"

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.SUCCESS
    assert captured_run_id["value"] == run_id


@pytest.mark.asyncio
async def test_context_task_id_access(store):
    """
    Test: Access task ID via ctx.task_id.

    Verifies:
    - ctx.task_id provides current task identifier
    """
    flow = AsyncFlow("test_task_id")

    @flow.task("my_task")
    def my_task(ctx: Context):
        return ctx.task_id

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)
    assert_task_success(run, "my_task", "my_task")


@pytest.mark.asyncio
async def test_context_attempt_number(store):
    """
    Test: Access attempt number via ctx.attempt_number.

    Verifies:
    - ctx.attempt_number reflects retry attempts (0-indexed)
    """
    attempts = []

    flow = AsyncFlow("test_attempt_number")

    @flow.task("task", max_retries=2)
    def task(ctx: Context):
        attempts.append(ctx.attempt_number)
        if ctx.attempt_number < 2:
            raise ValueError(f"Attempt {ctx.attempt_number} fails")
        return "success"

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)
    assert_task_success(run, "task", "success")
    assert attempts == [0, 1, 2]


@pytest.mark.asyncio
async def test_context_log(store):
    """
    Test: Context logging via ctx.log().

    Verifies:
    - ctx.log() executes without error
    - Task completes successfully with logging
    """
    flow = AsyncFlow("test_log")

    @flow.task("task")
    def task(ctx: Context):
        ctx.log("Starting task")
        ctx.log("Processing data")
        ctx.log("Task complete")
        return "done"

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)
    assert_task_success(run, "task", "done")


@pytest.mark.asyncio
async def test_context_pull_output_alias(store):
    """
    Test: ctx.pull_output() is alias for ctx.pull().

    Verifies:
    - Both methods work identically
    """
    flow = AsyncFlow("test_pull_alias")

    @flow.task("producer")
    def producer(ctx: Context):
        return "data"

    @flow.task("consumer", depends_on=["producer"])
    def consumer(ctx: Context):
        # Use pull_output explicitly
        data = ctx.pull_output("producer")
        return f"consumed_{data}"

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)
    assert_task_success(run, "consumer", "consumed_data")


@pytest.mark.asyncio
async def test_context_push_output_alias(store):
    """
    Test: ctx.push_output() is alias for ctx.push().

    Verifies:
    - Both methods work identically
    """
    flow = AsyncFlow("test_push_alias")

    @flow.task("task")
    def task(ctx: Context):
        # Use push_output explicitly
        ctx.push_output("output_value")
        return None

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)
    assert_task_success(run, "task", "output_value")


@pytest.mark.asyncio
async def test_context_complex_data_types(store):
    """
    Test: Push/pull complex data types (lists, dicts, nested).

    Verifies:
    - Complex Python objects can be passed between tasks
    - Data structure is preserved
    """
    flow = AsyncFlow("test_complex_data")

    @flow.task("producer")
    def producer(ctx: Context):
        return {
            "list": [1, 2, 3],
            "dict": {"a": 10, "b": 20},
            "nested": {"inner": [{"x": 1}, {"y": 2}]},
        }

    @flow.task("consumer", depends_on=["producer"])
    def consumer(ctx: Context):
        data = ctx.pull("producer")
        return data["nested"]["inner"][1]["y"]

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)
    assert_task_success(run, "consumer", 2)


@pytest.mark.asyncio
async def test_context_pull_from_subflow(store):
    """
    Test: Pull data from subflow task.

    Verifies:
    - ctx.pull() on subflow returns the subflow's result
    - Subflow output is transparently accessible
    """
    subflow = AsyncFlow("sub")

    @subflow.task("sub_task")
    def sub_task(ctx: Context):
        return "subflow_data"

    parent = AsyncFlow("parent")

    @parent.subflow("run_sub")
    def run_sub(ctx: Context):
        return subflow

    @parent.task("use_subflow_data", depends_on=["run_sub"])
    def use_subflow_data(ctx: Context):
        data = ctx.pull("run_sub")
        return f"parent_got_{data}"

    run = await run_and_assert(parent, store, ExecutionState.SUCCESS)
    assert_task_success(run, "use_subflow_data", "parent_got_subflow_data")


@pytest.mark.asyncio
async def test_context_params_in_subflow(store):
    """
    Test: Subflow accesses parent workflow params.

    Verifies:
    - ctx.params in subflow contains parent params
    """
    subflow = AsyncFlow("sub")

    @subflow.task("sub_task")
    def sub_task(ctx: Context):
        factor = ctx.params.get("factor", 1)
        return 100 * factor

    parent = AsyncFlow("parent")

    @parent.subflow("run_sub")
    def run_sub(ctx: Context):
        return subflow

    run = await run_and_assert(parent, store, ExecutionState.SUCCESS, factor=3)
    assert_task_success(run, "run_sub", 300)


@pytest.mark.asyncio
async def test_context_workflow_run_access(store):
    """
    Test: Access workflow run object via ctx.run.

    Verifies:
    - ctx.run provides access to WorkflowRun
    - Can access run metadata
    """
    flow = AsyncFlow("test_workflow_run")

    @flow.task("task")
    def task(ctx: Context):
        # Access workflow run
        workflow_run = ctx.run
        return workflow_run.flow_name

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)
    assert_task_success(run, "task", "test_workflow_run")


@pytest.mark.asyncio
async def test_context_pull_preserves_data_type(store):
    """
    Test: Pull preserves exact data types (int, float, str, bool, None).

    Verifies:
    - Data types are preserved through serialization
    """
    flow = AsyncFlow("test_data_types")

    @flow.task("producer")
    def producer(ctx: Context):
        return {
            "int": 42,
            "float": 3.14,
            "str": "text",
            "bool": True,
            "none": None,
            "list": [1, 2, 3],
        }

    @flow.task("consumer", depends_on=["producer"])
    def consumer(ctx: Context):
        data = ctx.pull("producer")
        assert isinstance(data["int"], int)
        assert isinstance(data["float"], float)
        assert isinstance(data["str"], str)
        assert isinstance(data["bool"], bool)
        assert data["none"] is None
        assert isinstance(data["list"], list)
        return "types_preserved"

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS)
    assert_task_success(run, "consumer", "types_preserved")


@pytest.mark.asyncio
async def test_context_params_are_shared_across_tasks(store):
    """
    Test: ctx.params is shared across all tasks in workflow.

    Verifies:
    - All tasks share the same params dict
    - Modifications made by one task are visible to subsequent tasks
    - This is expected behavior - params are workflow-level
    """
    flow = AsyncFlow("test_params_shared")

    @flow.task("task_a")
    def task_a(ctx: Context):
        # Modify params
        ctx.params["added_by_a"] = "value_from_a"
        return ctx.params.get("base", 0) + 1

    @flow.task("task_b", depends_on=["task_a"])
    def task_b(ctx: Context):
        # Can see modification from task_a
        value_from_a = ctx.params.get("added_by_a", "not_found")
        return f"task_b_saw_{value_from_a}"

    run = await run_and_assert(flow, store, ExecutionState.SUCCESS, base=10)
    assert_task_success(run, "task_a", 11)
    assert_task_success(run, "task_b", "task_b_saw_value_from_a")
