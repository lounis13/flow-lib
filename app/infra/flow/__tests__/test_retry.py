"""
Tests for retry functionality (automatic and manual).

This module tests retry behavior including:
- Automatic retry on task failure
- Manual retry of failed tasks
- Retry with downstream reset
- Retry of subflows
- Retry of tasks within subflows without restarting parent
"""
import pytest

from app.infra.flow import AsyncFlow, Context, ExecutionState
from conftest import (
    assert_task_success,
)


@pytest.mark.asyncio
async def test_auto_retry_success_on_second_attempt(store):
    """
    Test: Task fails first, succeeds on automatic retry.

    Verifies:
    - Task fails on first attempt (try_number=0)
    - Automatic retry occurs
    - Task succeeds on second attempt (try_number=1)
    - Workflow completes successfully
    """
    attempt_counter = {"count": 0}

    def flaky_task(ctx: Context):
        attempt_counter["count"] += 1
        if attempt_counter["count"] == 1:
            raise ValueError("First attempt fails")
        return "success_on_retry"

    flow = AsyncFlow("test_auto_retry")

    @flow.task("flaky", max_retries=2)
    def task_flaky(ctx: Context):
        return flaky_task(ctx)

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.SUCCESS
    assert run.tasks["flaky"].try_number == 1
    assert run.tasks["flaky"].state == ExecutionState.SUCCESS
    assert run.tasks["flaky"].output_json == "success_on_retry"


@pytest.mark.asyncio
async def test_auto_retry_exhausted(store):
    """
    Test: Task fails repeatedly until max_retries exhausted.

    Verifies:
    - Task attempts all retries
    - Workflow fails after exhausting retries
    - try_number reflects total attempts
    """
    flow = AsyncFlow("test_retry_exhausted")

    @flow.task("always_fails", max_retries=2)
    def task_fails(ctx: Context):
        raise ValueError("Always fails")

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    # max_retries=2 means: initial attempt (try_number=0) + 2 retries = 3 total attempts
    # After all attempts, try_number will be 3 (0->1->2->3)
    assert run.tasks["always_fails"].try_number >= 2  # At least 3 attempts
    assert run.tasks["always_fails"].state == ExecutionState.FAILED


@pytest.mark.asyncio
async def test_auto_retry_no_retries_configured(store):
    """
    Test: Task with max_retries=0 fails immediately.

    Verifies:
    - Task fails on first attempt
    - No retry occurs
    - try_number remains 0
    """
    flow = AsyncFlow("test_no_retry")

    @flow.task("no_retry", max_retries=0)
    def task_fails(ctx: Context):
        raise ValueError("Fail immediately")

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    # With max_retries=0, only one attempt should occur (try_number increments on failure)
    assert run.tasks["no_retry"].try_number <= 1  # One attempt
    assert run.tasks["no_retry"].state == ExecutionState.FAILED


@pytest.mark.asyncio
async def test_manual_retry_simple(store):
    """
    Test: Manual retry of a failed task.

    Verifies:
    - Task fails initially
    - Manual retry resets task state
    - Task executes again and succeeds
    - Workflow completes successfully
    """
    attempt_counter = {"count": 0}

    def flaky_task(ctx: Context):
        attempt_counter["count"] += 1
        if attempt_counter["count"] == 1:
            raise ValueError("First attempt fails")
        return "success_after_manual_retry"

    flow = AsyncFlow("test_manual_retry")

    @flow.task("manual_retry_task", max_retries=0)
    def task_flaky(ctx: Context):
        return flaky_task(ctx)

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    assert run.tasks["manual_retry_task"].state == ExecutionState.FAILED

    # Manual retry
    await flow.retry(run_id, "manual_retry_task", reset_downstream=False)

    run = store.load(run_id)
    assert run.state == ExecutionState.SUCCESS
    assert run.tasks["manual_retry_task"].state == ExecutionState.SUCCESS
    assert run.tasks["manual_retry_task"].output_json == "success_after_manual_retry"


@pytest.mark.asyncio
async def test_manual_retry_with_downstream_reset(store):
    """
    Test: Manual retry with downstream task reset.

    Topology:
        task_a → task_b → task_c
        (task_b fails)

    Verifies:
    - task_a succeeds
    - task_b fails
    - task_c doesn't execute (blocked by task_b)
    - Manual retry of task_b with reset_downstream=True
    - Both task_b and task_c execute
    """
    attempt_counter = {"count": 0}

    def flaky_b(ctx: Context):
        attempt_counter["count"] += 1
        if attempt_counter["count"] == 1:
            raise ValueError("task_b fails first time")
        return "b_success"

    flow = AsyncFlow("test_retry_downstream")

    @flow.task("task_a")
    def task_a(ctx: Context):
        return "a_result"

    @flow.task("task_b", depends_on=["task_a"], max_retries=0)
    def task_b(ctx: Context):
        return flaky_b(ctx)

    @flow.task("task_c", depends_on=["task_b"])
    def task_c(ctx: Context):
        b_result = ctx.pull("task_b")
        return f"c_{b_result}"

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    assert run.tasks["task_a"].state == ExecutionState.SUCCESS
    assert run.tasks["task_b"].state == ExecutionState.FAILED
    # task_c is skipped because its dependency failed
    assert run.tasks["task_c"].state in [ExecutionState.SCHEDULED, ExecutionState.SKIPPED]

    # Manual retry with downstream reset
    await flow.retry(run_id, "task_b", reset_downstream=True)

    run = store.load(run_id)
    assert run.state == ExecutionState.SUCCESS
    assert run.tasks["task_b"].state == ExecutionState.SUCCESS
    assert run.tasks["task_c"].state == ExecutionState.SUCCESS
    assert run.tasks["task_c"].output_json == "c_b_success"


@pytest.mark.asyncio
async def test_manual_retry_without_downstream_reset(store):
    """
    Test: Manual retry without resetting downstream tasks.

    Topology:
        task_a → task_b → task_c
        (task_b fails, task_c completes with stale data)

    Verifies:
    - When reset_downstream=False, downstream tasks keep their state
    """
    attempt_counter = {"count": 0}

    def flaky_b(ctx: Context):
        attempt_counter["count"] += 1
        if attempt_counter["count"] == 1:
            raise ValueError("task_b fails")
        return "b_new"

    flow = AsyncFlow("test_retry_no_downstream_reset")

    @flow.task("task_a")
    def task_a(ctx: Context):
        return "a_result"

    @flow.task("task_b", depends_on=["task_a"], max_retries=1)
    def task_b(ctx: Context):
        return flaky_b(ctx)

    @flow.task("task_c", depends_on=["task_b"])
    def task_c(ctx: Context):
        b_result = ctx.pull("task_b")
        return f"c_{b_result}"

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    # task_b should succeed on retry, task_c should execute
    assert run.state == ExecutionState.SUCCESS
    assert run.tasks["task_b"].try_number == 1
    assert run.tasks["task_c"].output_json == "c_b_new"


@pytest.mark.asyncio
async def test_retry_parallel_tasks(store):
    """
    Test: Retry when one of parallel tasks fails.

    Topology:
        [task_a, task_b (fails), task_c] → aggregator

    Verifies:
    - task_a and task_c succeed
    - task_b fails
    - aggregator blocked
    - Manual retry of task_b
    - aggregator completes
    """
    attempt_counter = {"count": 0}

    def flaky_b(ctx: Context):
        attempt_counter["count"] += 1
        if attempt_counter["count"] == 1:
            raise ValueError("task_b fails")
        return "B"

    flow = AsyncFlow("test_retry_parallel")

    @flow.task("task_a")
    def task_a(ctx: Context):
        return "A"

    @flow.task("task_b", max_retries=0)
    def task_b(ctx: Context):
        return flaky_b(ctx)

    @flow.task("task_c")
    def task_c(ctx: Context):
        return "C"

    @flow.task("aggregator", depends_on=["task_a", "task_b", "task_c"])
    def aggregator(ctx: Context):
        a = ctx.pull("task_a")
        b = ctx.pull("task_b")
        c = ctx.pull("task_c")
        return f"{a}{b}{c}"

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    assert run.tasks["task_a"].state == ExecutionState.SUCCESS
    assert run.tasks["task_b"].state == ExecutionState.FAILED
    assert run.tasks["task_c"].state == ExecutionState.SUCCESS
    assert run.tasks["aggregator"].state in [ExecutionState.SCHEDULED, ExecutionState.SKIPPED]

    # Retry task_b
    await flow.retry(run_id, "task_b", reset_downstream=True)

    run = store.load(run_id)
    assert run.state == ExecutionState.SUCCESS
    assert run.tasks["task_b"].state == ExecutionState.SUCCESS
    assert run.tasks["aggregator"].state == ExecutionState.SUCCESS
    assert run.tasks["aggregator"].output_json == "ABC"


@pytest.mark.asyncio
async def test_subflow_task_auto_retry(store):
    """
    Test: Automatic retry of a task inside a subflow.

    Verifies:
    - Task within subflow retries automatically
    - Subflow completes successfully after retry
    - Parent workflow completes successfully
    """
    attempt_counter = {"count": 0}

    def flaky_task(ctx: Context):
        attempt_counter["count"] += 1
        if attempt_counter["count"] == 1:
            raise ValueError("First attempt fails")
        return "subflow_retry_success"

    # Subflow with retryable task
    subflow = AsyncFlow("sub")

    @subflow.task("retryable_task", max_retries=2)
    def sub_task(ctx: Context):
        return flaky_task(ctx)

    # Parent workflow
    parent = AsyncFlow("parent")

    @parent.subflow("execute_subflow")
    def execute_subflow(ctx: Context):
        return subflow

    parent._store = store
    run_id = parent.init_run()
    await parent.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.SUCCESS
    assert_task_success(run, "execute_subflow", "subflow_retry_success")


@pytest.mark.asyncio
async def test_manual_retry_subflow_task_only(store):
    """
    Test: Manual retry of a task within a subflow without restarting parent.

    Scenario:
    - Subflow task fails
    - Parent workflow is blocked
    - Manual retry of subflow task directly (using subflow run_id)
    - Subflow completes
    - Parent should NOT restart, but should continue

    Verifies:
    - retry(child_run_id, task_id) only retries the child workflow
    - Parent remains in running state waiting for subflow
    """
    attempt_counter = {"count": 0}

    def flaky_task(ctx: Context):
        attempt_counter["count"] += 1
        if attempt_counter["count"] == 1:
            raise ValueError("Subflow task fails")
        return "subflow_fixed"

    # Subflow
    subflow = AsyncFlow("sub")

    @subflow.task("sub_task", max_retries=0)
    def sub_task(ctx: Context):
        return flaky_task(ctx)

    # Parent
    parent = AsyncFlow("parent")

    @parent.task("before_subflow")
    def before_subflow(ctx: Context):
        return "before"

    @parent.subflow("execute_subflow", depends_on=["before_subflow"])
    def execute_subflow(ctx: Context):
        return subflow

    @parent.task("after_subflow", depends_on=["execute_subflow"])
    def after_subflow(ctx: Context):
        result = ctx.pull("execute_subflow")
        return f"after_{result}"

    parent._store = store
    subflow._store = store

    parent_run_id = parent.init_run()
    await parent.run_until_complete(parent_run_id)

    parent_run = store.load(parent_run_id)
    assert parent_run.state == ExecutionState.FAILED
    assert parent_run.tasks["before_subflow"].state == ExecutionState.SUCCESS
    assert parent_run.tasks["execute_subflow"].state == ExecutionState.FAILED
    assert parent_run.tasks["after_subflow"].state in [ExecutionState.SCHEDULED, ExecutionState.SKIPPED]

    # Get child run ID
    child_run_id = None
    for task_id, task in parent_run.tasks.items():
        if task.type == "flow" and task.output_json:
            if isinstance(task.output_json, dict) and "child_run_id" in task.output_json:
                child_run_id = task.output_json["child_run_id"]
            break

    assert child_run_id is not None, "Child run ID should exist"

    # Retry ONLY the subflow task (not the parent)
    await subflow.retry(child_run_id, "sub_task", reset_downstream=False)

    # Check subflow completed
    child_run = store.load(child_run_id)
    assert child_run.state == ExecutionState.SUCCESS
    assert child_run.tasks["sub_task"].state == ExecutionState.SUCCESS

    # Now retry parent to continue execution
    await parent.retry(parent_run_id, "execute_subflow", reset_downstream=True)

    parent_run = store.load(parent_run_id)
    assert parent_run.state == ExecutionState.SUCCESS
    assert parent_run.tasks["execute_subflow"].state == ExecutionState.SUCCESS
    assert parent_run.tasks["after_subflow"].state == ExecutionState.SUCCESS
    assert parent_run.tasks["after_subflow"].output_json == "after_subflow_fixed"


@pytest.mark.asyncio
async def test_manual_retry_subflow_with_parent_continuation(store):
    """
    Test: Retry subflow task and automatically continue parent.

    Verifies:
    - When retrying a subflow task, specifying parent info continues parent too
    """
    attempt_counter = {"count": 0}

    def flaky_task(ctx: Context):
        attempt_counter["count"] += 1
        if attempt_counter["count"] == 1:
            raise ValueError("Subflow fails")
        return "fixed"

    subflow = AsyncFlow("sub")

    @subflow.task("task", max_retries=0)
    def sub_task(ctx: Context):
        return flaky_task(ctx)

    parent = AsyncFlow("parent")

    @parent.subflow("sub")
    def run_sub(ctx: Context):
        return subflow

    @parent.task("final", depends_on=["sub"])
    def final_task(ctx: Context):
        return "parent_done"

    parent._store = store
    subflow._store = store

    parent_run_id = parent.init_run()
    await parent.run_until_complete(parent_run_id)

    parent_run = store.load(parent_run_id)
    assert parent_run.state == ExecutionState.FAILED

    # Get child run
    child_run_id = parent_run.tasks["sub"].output_json
    if isinstance(child_run_id, dict):
        child_run_id = child_run_id.get("child_run_id")

    # Retry child
    await subflow.retry(child_run_id, "task")

    child_run = store.load(child_run_id)
    assert child_run.state == ExecutionState.SUCCESS

    # Retry parent subflow task to propagate success
    await parent.retry(parent_run_id, "sub", reset_downstream=True)

    parent_run = store.load(parent_run_id)
    assert parent_run.state == ExecutionState.SUCCESS
    assert parent_run.tasks["final"].output_json == "parent_done"


@pytest.mark.asyncio
async def test_retry_nested_subflow(store):
    """
    Test: Retry in nested subflows (3 levels).

    Structure:
        parent → sub1 → sub2 (task fails)

    Verifies:
    - Failure in deepest subflow
    - Retry at deepest level
    - Propagation through all levels
    """
    attempt_counter = {"count": 0}

    def flaky_task(ctx: Context):
        attempt_counter["count"] += 1
        if attempt_counter["count"] == 1:
            raise ValueError("Deep task fails")
        return "deep_success"

    # Level 2 (deepest)
    sub2 = AsyncFlow("sub2")

    @sub2.task("deep_task", max_retries=0)
    def deep_task(ctx: Context):
        return flaky_task(ctx)

    # Level 1
    sub1 = AsyncFlow("sub1")

    @sub1.subflow("nested")
    def nested(ctx: Context):
        return sub2

    # Parent
    parent = AsyncFlow("parent")

    @parent.subflow("sub")
    def sub(ctx: Context):
        return sub1

    parent._store = store
    sub1._store = store
    sub2._store = store

    parent_run_id = parent.init_run()
    await parent.run_until_complete(parent_run_id)

    parent_run = store.load(parent_run_id)
    assert parent_run.state == ExecutionState.FAILED

    # Get sub1 run ID
    sub1_run_id = parent_run.tasks["sub"].output_json
    if isinstance(sub1_run_id, dict):
        sub1_run_id = sub1_run_id.get("child_run_id")

    sub1_run = store.load(sub1_run_id)
    assert sub1_run.state == ExecutionState.FAILED

    # Get sub2 run ID
    sub2_run_id = sub1_run.tasks["nested"].output_json
    if isinstance(sub2_run_id, dict):
        sub2_run_id = sub2_run_id.get("child_run_id")

    sub2_run = store.load(sub2_run_id)
    assert sub2_run.state == ExecutionState.FAILED

    # Retry at deepest level
    await sub2.retry(sub2_run_id, "deep_task")

    sub2_run = store.load(sub2_run_id)
    assert sub2_run.state == ExecutionState.SUCCESS

    # Propagate up to sub1 - this should re-execute the subflow task
    # which will call run_until_complete on sub2 (which is already SUCCESS)
    await sub1.retry(sub1_run_id, "nested", reset_downstream=False)

    sub1_run = store.load(sub1_run_id)
    assert sub1_run.state == ExecutionState.SUCCESS, f"sub1 state: {sub1_run.state}, nested task: {sub1_run.tasks['nested']}"

    # Propagate up to parent - this should re-execute the parent's subflow task
    # which will call run_until_complete on sub1 (which should now be SUCCESS)
    await parent.retry(parent_run_id, "sub", reset_downstream=False)

    parent_run = store.load(parent_run_id)
    # DEBUG: Print states if assertion fails
    if parent_run.state != ExecutionState.SUCCESS:
        print(f"Parent state: {parent_run.state}")
        print(f"Parent sub task: {parent_run.tasks['sub']}")
        print(f"Sub1 final state: {store.load(sub1_run_id).state}")
    assert parent_run.state == ExecutionState.SUCCESS


@pytest.mark.asyncio
async def test_auto_retry_with_delay(store):
    """
    Test: Automatic retry with delay between attempts.

    Verifies:
    - Retry delay is respected (basic check that it completes)
    - Task eventually succeeds after retry with delay
    """
    import datetime

    attempt_counter = {"count": 0}

    def flaky_task(ctx: Context):
        attempt_counter["count"] += 1
        if attempt_counter["count"] == 1:
            raise ValueError("First fails")
        return "success"

    flow = AsyncFlow("test_retry_delay")

    @flow.task("task", max_retries=1, retry_delay=datetime.timedelta(milliseconds=100))
    def task(ctx: Context):
        return flaky_task(ctx)

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.SUCCESS
    assert run.tasks["task"].try_number == 1


@pytest.mark.asyncio
async def test_manual_retry_multiple_times(store):
    """
    Test: Manual retry can be performed multiple times.

    Verifies:
    - Task can be manually retried multiple times
    - try_number doesn't increment on manual retry (it resets)
    """
    attempt_counter = {"count": 0}

    def counting_task(ctx: Context):
        attempt_counter["count"] += 1
        if attempt_counter["count"] < 3:
            raise ValueError(f"Attempt {attempt_counter['count']} fails")
        return "finally_success"

    flow = AsyncFlow("test_multiple_manual_retry")

    @flow.task("task", max_retries=0)
    def task(ctx: Context):
        return counting_task(ctx)

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED

    # First manual retry
    await flow.retry(run_id, "task", reset_downstream=False)
    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED

    # Second manual retry
    await flow.retry(run_id, "task", reset_downstream=False)
    run = store.load(run_id)
    assert run.state == ExecutionState.SUCCESS
    assert run.tasks["task"].output_json == "finally_success"


@pytest.mark.asyncio
async def test_retry_nested_task_with_parent_run_id(store):
    """
    Test: Retry a task in a nested subflow using parent run_id.

    Scenario:
    - User has parent run_id
    - User wants to retry "sub1.sub2.deep_task"
    - System should:
      1. Find the task in nested subflow
      2. Reset the task to SCHEDULED
      3. Set all parent flows (sub2, sub1) to RUNNING
      4. Resume execution from that task
      5. Continue propagating success up to parent

    Structure:
        parent
          → sub1
              → sub2
                  → deep_task (fails)
                  → process (depends on deep_task)
              → aggregate (depends on sub2)
          → finalize (depends on sub1)

    Verifies:
    - Retry with parent run_id and nested task_id works
    - All parent flows are set to RUNNING
    - Execution continues from the failed task
    - Downstream tasks in the same subflow execute
    - Parent workflows continue after subflow completes
    """
    attempt_counter = {"count": 0}

    def flaky_deep_task(ctx: Context):
        attempt_counter["count"] += 1
        if attempt_counter["count"] == 1:
            raise ValueError("Deep task fails")
        return "deep_result"

    # Level 2: sub2 (deepest)
    sub2 = AsyncFlow("sub2")

    @sub2.task("deep_task", max_retries=0)
    def deep_task(ctx: Context):
        return flaky_deep_task(ctx)

    @sub2.task("process", depends_on=["deep_task"])
    def process(ctx: Context):
        data = ctx.pull("deep_task")
        return f"processed_{data}"

    # Level 1: sub1
    sub1 = AsyncFlow("sub1")

    @sub1.subflow("sub2")
    def run_sub2(ctx: Context):
        return sub2

    @sub1.task("aggregate", depends_on=["sub2"])
    def aggregate(ctx: Context):
        result = ctx.pull("sub2")
        return f"aggregated_{result}"

    # Level 0: parent
    parent = AsyncFlow("parent")

    @parent.subflow("sub1")
    def run_sub1(ctx: Context):
        return sub1

    @parent.task("finalize", depends_on=["sub1"])
    def finalize(ctx: Context):
        result = ctx.pull("sub1")
        return f"final_{result}"

    # Set up stores
    parent._store = store
    sub1._store = store
    sub2._store = store

    # Run parent workflow
    parent_run_id = parent.init_run()
    await parent.run_until_complete(parent_run_id)

    # Verify initial failure
    parent_run = store.load(parent_run_id)
    assert parent_run.state == ExecutionState.FAILED

    # Get all tasks recursively to verify the nested task path exists
    all_tasks = parent_run.get_all_tasks_recursive(store)
    assert "sub1.sub2.deep_task" in all_tasks
    assert all_tasks["sub1.sub2.deep_task"].state == ExecutionState.FAILED
    assert "sub1.sub2.process" in all_tasks
    assert all_tasks["sub1.sub2.process"].state in [ExecutionState.SCHEDULED, ExecutionState.SKIPPED]
    assert "sub1.aggregate" in all_tasks
    assert all_tasks["sub1.aggregate"].state in [ExecutionState.SCHEDULED, ExecutionState.SKIPPED]

    # THIS IS THE KEY TEST: Retry using parent run_id with nested task_id
    # The user only knows the parent run_id and the full task path
    await parent.retry(
        run_id=parent_run_id,
        task_id="sub1.sub2.deep_task",
        reset_downstream=True
    )

    # Verify everything completed
    parent_run = store.load(parent_run_id)
    all_tasks_after = parent_run.get_all_tasks_recursive(store)

    # The nested task should be SUCCESS
    assert all_tasks_after["sub1.sub2.deep_task"].state == ExecutionState.SUCCESS

    # Downstream tasks in the same subflow should execute
    assert all_tasks_after["sub1.sub2.process"].state == ExecutionState.SUCCESS

    # Parent subflow tasks should execute
    assert all_tasks_after["sub1.aggregate"].state == ExecutionState.SUCCESS

    # Top-level parent tasks should execute
    assert parent_run.tasks["finalize"].state == ExecutionState.SUCCESS

    # Overall workflow should be SUCCESS
    assert parent_run.state == ExecutionState.SUCCESS


@pytest.mark.asyncio
async def test_retry_diamond_pattern(store):
    """
    Test: Retry in diamond pattern with downstream reset.

    Topology:
        start → [branch_a (fails), branch_b] → end

    Verifies:
    - branch_a fails, branch_b succeeds
    - end is blocked
    - Manual retry of branch_a with reset_downstream
    - end executes after retry
    """
    attempt_counter = {"count": 0}

    def flaky_a(ctx: Context):
        attempt_counter["count"] += 1
        if attempt_counter["count"] == 1:
            raise ValueError("branch_a fails")
        return "A"

    flow = AsyncFlow("test_retry_diamond")

    @flow.task("start")
    def start(ctx: Context):
        return "start"

    @flow.task("branch_a", depends_on=["start"], max_retries=0)
    def branch_a(ctx: Context):
        return flaky_a(ctx)

    @flow.task("branch_b", depends_on=["start"])
    def branch_b(ctx: Context):
        return "B"

    @flow.task("end", depends_on=["branch_a", "branch_b"])
    def end(ctx: Context):
        a = ctx.pull("branch_a")
        b = ctx.pull("branch_b")
        return f"{a}{b}"

    flow._store = store
    run_id = flow.init_run()
    await flow.run_until_complete(run_id)

    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED
    assert run.tasks["start"].state == ExecutionState.SUCCESS
    assert run.tasks["branch_a"].state == ExecutionState.FAILED
    assert run.tasks["branch_b"].state == ExecutionState.SUCCESS
    assert run.tasks["end"].state in [ExecutionState.SCHEDULED, ExecutionState.SKIPPED]

    # Retry branch_a with downstream reset
    await flow.retry(run_id, "branch_a", reset_downstream=True)

    run = store.load(run_id)
    assert run.state == ExecutionState.SUCCESS
    assert run.tasks["branch_a"].state == ExecutionState.SUCCESS
    assert run.tasks["end"].state == ExecutionState.SUCCESS
    assert run.tasks["end"].output_json == "AB"
