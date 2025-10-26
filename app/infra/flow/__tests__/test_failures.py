"""
Tests for task failure scenarios.

This module tests how the workflow handles task failures:
- Single task failures
- Cascading failures
- Partial failures in parallel execution
- Subflow failures
- Failure propagation
"""
import pytest

from app.infra.flow import AsyncFlow, Context, ExecutionState
from conftest import assert_task_failed


@pytest.mark.asyncio
async def test_single_task_failure(store):
    """
    Test: Single task fails, workflow fails.

    Verifies:
    - Task failure causes workflow failure
    - Error message is captured
    """
    flow = AsyncFlow("test_single_failure")

    @flow.task("failing_task", max_retries=0)
    def failing_task(ctx: Context):
        raise ValueError("Task intentionally fails")

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    assert_task_failed(run, "failing_task", "Task intentionally fails")


@pytest.mark.asyncio
async def test_failure_blocks_downstream(store):
    """
    Test: Task failure blocks downstream tasks.

    Topology:
        task_a (fails) → task_b → task_c

    Verifies:
    - task_a fails
    - task_b and task_c are skipped
    """
    flow = AsyncFlow("test_failure_blocks")

    @flow.task("task_a", max_retries=0)
    def task_a(ctx: Context):
        raise ValueError("task_a fails")

    @flow.task("task_b", depends_on=["task_a"])
    def task_b(ctx: Context):
        return "should_not_run"

    @flow.task("task_c", depends_on=["task_b"])
    def task_c(ctx: Context):
        return "should_not_run"

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    assert run.tasks["task_a"].state == ExecutionState.FAILED
    assert run.tasks["task_b"].state in [ExecutionState.SCHEDULED, ExecutionState.SKIPPED]
    assert run.tasks["task_c"].state in [ExecutionState.SCHEDULED, ExecutionState.SKIPPED]


@pytest.mark.asyncio
async def test_parallel_partial_failure(store):
    """
    Test: One parallel task fails, others succeed.

    Topology:
        [task_a, task_b (fails), task_c] → aggregator

    Verifies:
    - task_a and task_c complete successfully
    - task_b fails
    - aggregator is blocked
    - Workflow fails
    """
    flow = AsyncFlow("test_parallel_failure")

    @flow.task("task_a")
    def task_a(ctx: Context):
        return "A"

    @flow.task("task_b", max_retries=0)
    def task_b(ctx: Context):
        raise ValueError("task_b fails")

    @flow.task("task_c")
    def task_c(ctx: Context):
        return "C"

    @flow.task("aggregator", depends_on=["task_a", "task_b", "task_c"])
    def aggregator(ctx: Context):
        return "should_not_run"

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    assert run.tasks["task_a"].state == ExecutionState.SUCCESS
    assert run.tasks["task_b"].state == ExecutionState.FAILED
    assert run.tasks["task_c"].state == ExecutionState.SUCCESS
    assert run.tasks["aggregator"].state in [ExecutionState.SCHEDULED, ExecutionState.SKIPPED]


@pytest.mark.asyncio
async def test_failure_in_diamond_pattern(store):
    """
    Test: Failure in one branch of diamond.

    Topology:
        start → [branch_a (fails), branch_b] → end

    Verifies:
    - start succeeds
    - branch_a fails, branch_b succeeds
    - end is blocked
    """
    flow = AsyncFlow("test_diamond_failure")

    @flow.task("start")
    def start(ctx: Context):
        return "start"

    @flow.task("branch_a", depends_on=["start"], max_retries=0)
    def branch_a(ctx: Context):
        raise ValueError("branch_a fails")

    @flow.task("branch_b", depends_on=["start"])
    def branch_b(ctx: Context):
        return "B"

    @flow.task("end", depends_on=["branch_a", "branch_b"])
    def end(ctx: Context):
        return "should_not_run"

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    assert run.tasks["start"].state == ExecutionState.SUCCESS
    assert run.tasks["branch_a"].state == ExecutionState.FAILED
    assert run.tasks["branch_b"].state == ExecutionState.SUCCESS
    assert run.tasks["end"].state in [ExecutionState.SCHEDULED, ExecutionState.SKIPPED]


@pytest.mark.asyncio
async def test_exception_types(store):
    """
    Test: Different exception types are handled.

    Verifies:
    - ValueError, TypeError, RuntimeError all cause failures
    - Exception messages are preserved
    """
    test_cases = [
        ("ValueError", ValueError("value error")),
        ("TypeError", TypeError("type error")),
        ("RuntimeError", RuntimeError("runtime error")),
        ("KeyError", KeyError("key error")),
    ]

    for task_id, exception in test_cases:
        flow = AsyncFlow(f"test_exception_{task_id}")

        def make_task(exc):
            def task(ctx: Context):
                raise exc
            return task

        flow.add_task_definition(
            task_id=task_id,
            task_function=make_task(exception),
            max_retries=0,
        )

        flow._store = store
        run_id = flow.init_run()
        await flow.run_until_complete(run_id)

        run = store.load(run_id)
        assert run.state == ExecutionState.FAILED
        assert run.tasks[task_id].state == ExecutionState.FAILED
        assert str(exception) in run.tasks[task_id].error


@pytest.mark.asyncio
async def test_subflow_task_failure(store):
    """
    Test: Task inside subflow fails.

    Verifies:
    - Subflow task failure causes subflow failure
    - Subflow failure causes parent workflow failure
    """
    subflow = AsyncFlow("sub")

    @subflow.task("sub_task", max_retries=0)
    def sub_task(ctx: Context):
        raise ValueError("Subflow task fails")

    parent = AsyncFlow("parent")

    @parent.subflow("run_subflow")
    def run_subflow(ctx: Context):
        return subflow

    @parent.task("after_subflow", depends_on=["run_subflow"])
    def after_subflow(ctx: Context):
        return "should_not_run"

    parent._store = store
    run_id = parent.init_run()
    await parent.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    assert run.tasks["run_subflow"].state == ExecutionState.FAILED
    assert run.tasks["after_subflow"].state in [ExecutionState.SCHEDULED, ExecutionState.SKIPPED]


@pytest.mark.asyncio
async def test_cascading_failures(store):
    """
    Test: Multiple dependent tasks, early failure stops all.

    Topology:
        a → b → c → d → e
        (a fails)

    Verifies:
    - Only a executes and fails
    - All downstream tasks are skipped
    """
    flow = AsyncFlow("test_cascading")

    @flow.task("a", max_retries=0)
    def a(ctx: Context):
        raise ValueError("a fails")

    @flow.task("b", depends_on=["a"])
    def b(ctx: Context):
        return "b"

    @flow.task("c", depends_on=["b"])
    def c(ctx: Context):
        return "c"

    @flow.task("d", depends_on=["c"])
    def d(ctx: Context):
        return "d"

    @flow.task("e", depends_on=["d"])
    def e(ctx: Context):
        return "e"

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    assert run.tasks["a"].state == ExecutionState.FAILED
    for task_id in ["b", "c", "d", "e"]:
        assert run.tasks[task_id].state in [ExecutionState.SCHEDULED, ExecutionState.SKIPPED]


@pytest.mark.asyncio
async def test_failure_with_independent_branch(store):
    """
    Test: Failure in one branch doesn't affect independent branch.

    Topology:
        [branch_a (fails), branch_b]
        (no common downstream)

    Verifies:
    - branch_a fails
    - branch_b succeeds independently
    - Workflow still fails (one task failed)
    """
    flow = AsyncFlow("test_independent_branches")

    @flow.task("branch_a", max_retries=0)
    def branch_a(ctx: Context):
        raise ValueError("branch_a fails")

    @flow.task("branch_b")
    def branch_b(ctx: Context):
        return "B"

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    assert run.tasks["branch_a"].state == ExecutionState.FAILED
    assert run.tasks["branch_b"].state == ExecutionState.SUCCESS


@pytest.mark.asyncio
async def test_failure_error_traceback(store):
    """
    Test: Error traceback is captured and stored.

    Verifies:
    - Full traceback is available in error field
    - Can identify where exception occurred
    """
    flow = AsyncFlow("test_traceback")

    @flow.task("task", max_retries=0)
    def task(ctx: Context):
        def inner_function():
            raise ValueError("Error in inner function")
        inner_function()

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    error = run.tasks["task"].error
    assert "ValueError: Error in inner function" in error
    assert "inner_function" in error
    assert "Traceback" in error


@pytest.mark.asyncio
async def test_multiple_independent_failures(store):
    """
    Test: Multiple independent tasks fail.

    Topology:
        [task_a (fails), task_b (fails), task_c]

    Verifies:
    - Both failures are recorded
    - Workflow fails
    - Independent success task still completes
    """
    flow = AsyncFlow("test_multiple_failures")

    @flow.task("task_a", max_retries=0)
    def task_a(ctx: Context):
        raise ValueError("task_a fails")

    @flow.task("task_b", max_retries=0)
    def task_b(ctx: Context):
        raise ValueError("task_b fails")

    @flow.task("task_c")
    def task_c(ctx: Context):
        return "C"

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    assert run.tasks["task_a"].state == ExecutionState.FAILED
    assert run.tasks["task_b"].state == ExecutionState.FAILED
    assert run.tasks["task_c"].state == ExecutionState.SUCCESS


@pytest.mark.asyncio
async def test_failure_with_complex_dependencies(store):
    """
    Test: Failure in complex DAG.

    Topology:
        start → [a, b] → c → [d, e] → end
        (c fails)

    Verifies:
    - start, a, b execute
    - c fails
    - d, e, end are skipped
    """
    flow = AsyncFlow("test_complex_failure")

    @flow.task("start")
    def start(ctx: Context):
        return "start"

    @flow.task("a", depends_on=["start"])
    def a(ctx: Context):
        return "A"

    @flow.task("b", depends_on=["start"])
    def b(ctx: Context):
        return "B"

    @flow.task("c", depends_on=["a", "b"], max_retries=0)
    def c(ctx: Context):
        raise ValueError("c fails")

    @flow.task("d", depends_on=["c"])
    def d(ctx: Context):
        return "D"

    @flow.task("e", depends_on=["c"])
    def e(ctx: Context):
        return "E"

    @flow.task("end", depends_on=["d", "e"])
    def end(ctx: Context):
        return "end"

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    assert run.tasks["start"].state == ExecutionState.SUCCESS
    assert run.tasks["a"].state == ExecutionState.SUCCESS
    assert run.tasks["b"].state == ExecutionState.SUCCESS
    assert run.tasks["c"].state == ExecutionState.FAILED
    for task_id in ["d", "e", "end"]:
        assert run.tasks[task_id].state in [ExecutionState.SCHEDULED, ExecutionState.SKIPPED]


@pytest.mark.asyncio
async def test_nested_subflow_failure_propagation(store):
    """
    Test: Failure in nested subflow propagates to parent.

    Structure:
        parent → sub1 → sub2 (task fails)

    Verifies:
    - Task failure in sub2 causes sub2 to fail
    - sub2 failure causes sub1 to fail
    - sub1 failure causes parent to fail
    """
    # Deepest subflow with failing task
    sub2 = AsyncFlow("sub2")

    @sub2.task("deep_task", max_retries=0)
    def deep_task(ctx: Context):
        raise ValueError("Deep task fails")

    # Middle subflow
    sub1 = AsyncFlow("sub1")

    @sub1.subflow("nested")
    def nested(ctx: Context):
        return sub2

    # Parent
    parent = AsyncFlow("parent")

    @parent.subflow("sub")
    def sub(ctx: Context):
        return sub1

    @parent.task("after", depends_on=["sub"])
    def after(ctx: Context):
        return "should_not_run"

    parent._store = store
    sub1._store = store
    sub2._store = store

    run_id = parent.init_run()
    await parent.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    assert run.tasks["sub"].state == ExecutionState.FAILED
    assert run.tasks["after"].state in [ExecutionState.SCHEDULED, ExecutionState.SKIPPED]


@pytest.mark.asyncio
async def test_assertion_error_as_failure(store):
    """
    Test: AssertionError is treated as task failure.

    Verifies:
    - Assertion failures cause task failure
    - Error message includes assertion details
    """
    flow = AsyncFlow("test_assertion")

    @flow.task("task", max_retries=0)
    def task(ctx: Context):
        result = 2 + 2
        assert result == 5, "Math is broken"
        return result

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    assert run.tasks["task"].state == ExecutionState.FAILED
    assert "AssertionError" in run.tasks["task"].error
    assert "Math is broken" in run.tasks["task"].error


@pytest.mark.asyncio
async def test_failure_preserves_successful_task_outputs(store):
    """
    Test: Successful task outputs are preserved even when workflow fails.

    Topology:
        task_a → task_b (fails) → task_c

    Verifies:
    - task_a output is preserved
    - Can access task_a output even after workflow failure
    """
    flow = AsyncFlow("test_preserve_outputs")

    @flow.task("task_a")
    def task_a(ctx: Context):
        return {"important": "data"}

    @flow.task("task_b", depends_on=["task_a"], max_retries=0)
    def task_b(ctx: Context):
        raise ValueError("task_b fails")

    @flow.task("task_c", depends_on=["task_b"])
    def task_c(ctx: Context):
        return "not_reached"

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    assert run.tasks["task_a"].state == ExecutionState.SUCCESS
    assert run.tasks["task_a"].output_json == {"important": "data"}
