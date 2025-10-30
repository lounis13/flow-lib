"""
Tests for subflow failure scenarios and downstream task skipping.

This module tests how the workflow handles failures in subflows and
ensures that downstream tasks are properly marked as SKIPPED.
"""
import pytest

from app.infra.flow import AsyncFlow, Context, ExecutionState
from conftest import (
    assert_task_failed,
)


@pytest.mark.asyncio
async def test_subflow_failure_should_skip_downstream_in_parent(store):
    """
    Test: When a task inside a subflow fails, downstream tasks in parent should be SKIPPED.

    Topology:
        parent:
            firebird_flow (subflow - fails) → pricing_flow (subflow) → calculate_diff → send_notification

    Verifies:
    - firebird_flow fails because internal task fails
    - pricing_flow is SKIPPED (not SCHEDULED)
    - calculate_diff is SKIPPED
    - send_notification is SKIPPED
    - Parent workflow fails
    """
    # Create firebird subflow with failing task
    firebird_flow = AsyncFlow("firebird_flow")

    @firebird_flow.task("build_image", max_retries=0)
    def build_image(ctx: Context):
        raise ValueError("Firebird build failed intentionally")

    # Create pricing subflow
    pricing_flow = AsyncFlow("pricing_flow")

    @pricing_flow.task("run_pricing")
    def run_pricing(ctx: Context):
        return "pricing_done"

    # Create parent flow
    parent = AsyncFlow("night_batch_flow")

    @parent.subflow("firebird", name="Build Firebird Image")
    def firebird(ctx: Context):
        return firebird_flow

    @parent.subflow("pricing", depends_on=["firebird"], name="Run Pricing")
    def pricing(ctx: Context):
        return pricing_flow

    @parent.task("calculate_diff", depends_on=["pricing"])
    def calculate_diff(ctx: Context):
        return "diff_calculated"

    @parent.task("send_notification", depends_on=["calculate_diff"])
    def send_notification(ctx: Context):
        return "notification_sent"

    # Execute parent workflow
    parent._store = store
    firebird_flow._store = store
    pricing_flow._store = store

    run_id = parent.init_run()
    await parent.run_until_complete(run_id)

    # Assert final states
    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED, "Parent workflow should be FAILED"
    assert run.tasks["firebird"].state == ExecutionState.FAILED, "firebird subflow should be FAILED"

    # BUG: These tasks should be SKIPPED, not SCHEDULED
    assert run.tasks["pricing"].state == ExecutionState.SKIPPED, \
        f"pricing should be SKIPPED but was {run.tasks['pricing'].state}"
    assert run.tasks["calculate_diff"].state == ExecutionState.SKIPPED, \
        f"calculate_diff should be SKIPPED but was {run.tasks['calculate_diff'].state}"
    assert run.tasks["send_notification"].state == ExecutionState.SKIPPED, \
        f"send_notification should be SKIPPED but was {run.tasks['send_notification'].state}"


@pytest.mark.asyncio
async def test_subflow_inside_subflow_failure_should_skip_siblings(store):
    """
    Test: When a deeply nested subflow fails, sibling tasks in parent subflow should be SKIPPED.

    This reproduces the exact night_batch scenario where:
    - run_type_flow is a subflow that contains multiple library flows
    - If one library's firebird fails, its pricing should be SKIPPED
    - Other libraries' flows should still execute
    - But the calculate_diff task (that depends on ALL pricing flows) should be SKIPPED

    Topology:
        parent (night_batch):
            run_type_flow (subflow):
                ftb_firebird (subflow - fails) → ftb_pricing (subflow)
                hpl_firebird (subflow - succeeds) → hpl_pricing (subflow)
                calculate_diff (depends on [ftb_pricing, hpl_pricing])
                send_notification (depends on calculate_diff)
    """
    # FTB flows
    ftb_firebird = AsyncFlow("ftb_firebird")

    @ftb_firebird.task("build_ftb_image", max_retries=0)
    def build_ftb_image(ctx: Context):
        raise ValueError("FTB build failed")

    ftb_pricing = AsyncFlow("ftb_pricing")

    @ftb_pricing.task("run_ftb_pricing")
    def run_ftb_pricing(ctx: Context):
        return "ftb_pricing_done"

    # HPL flows
    hpl_firebird = AsyncFlow("hpl_firebird")

    @hpl_firebird.task("build_hpl_image")
    def build_hpl_image(ctx: Context):
        return "hpl_image_built"

    hpl_pricing = AsyncFlow("hpl_pricing")

    @hpl_pricing.task("run_hpl_pricing")
    def run_hpl_pricing(ctx: Context):
        return "hpl_pricing_done"

    # Run type flow (contains all library flows)
    run_type_flow = AsyncFlow("run_type_flow")

    @run_type_flow.subflow("ftb_flow")
    def ftb_flow(ctx: Context):
        return ftb_firebird

    @run_type_flow.subflow("ftb_pricing_flow", depends_on=["ftb_flow"])
    def ftb_pricing_flow(ctx: Context):
        return ftb_pricing

    @run_type_flow.subflow("hpl_flow")
    def hpl_flow(ctx: Context):
        return hpl_firebird

    @run_type_flow.subflow("hpl_pricing_flow", depends_on=["hpl_flow"])
    def hpl_pricing_flow(ctx: Context):
        return hpl_pricing

    @run_type_flow.task("calculate_diff", depends_on=["ftb_pricing_flow", "hpl_pricing_flow"])
    def calculate_diff(ctx: Context):
        return "diff_calculated"

    @run_type_flow.task("send_notification", depends_on=["calculate_diff"])
    def send_notification(ctx: Context):
        return "notification_sent"

    # Parent flow (night batch)
    parent = AsyncFlow("night_batch")

    @parent.subflow("run_type")
    def run_type(ctx: Context):
        return run_type_flow

    # Execute
    parent._store = store
    run_type_flow._store = store
    ftb_firebird._store = store
    ftb_pricing._store = store
    hpl_firebird._store = store
    hpl_pricing._store = store

    run_id = parent.init_run()
    await parent.run_until_complete(run_id)

    # Load parent run
    parent_run = store.load(run_id)
    assert parent_run.state == ExecutionState.FAILED, "Parent should be FAILED"
    assert parent_run.tasks["run_type"].state == ExecutionState.FAILED, "run_type should be FAILED"

    # Load run_type_flow run to check internal tasks
    run_type_task = parent_run.tasks["run_type"]
    run_type_run_id = run_type_task.output_json.get("child_run_id")
    run_type_run = store.load(run_type_run_id)

    # Verify states inside run_type_flow
    assert run_type_run.tasks["ftb_flow"].state == ExecutionState.FAILED, "ftb_flow should be FAILED"
    assert run_type_run.tasks["ftb_pricing_flow"].state == ExecutionState.SKIPPED, \
        f"BUG: ftb_pricing_flow should be SKIPPED but was {run_type_run.tasks['ftb_pricing_flow'].state}"

    assert run_type_run.tasks["hpl_flow"].state == ExecutionState.SUCCESS, "hpl_flow should be SUCCESS"
    assert run_type_run.tasks["hpl_pricing_flow"].state == ExecutionState.SUCCESS, \
        f"hpl_pricing_flow should be SUCCESS but was {run_type_run.tasks['hpl_pricing_flow'].state}"

    # These should be SKIPPED because ftb_pricing_flow is SKIPPED (one dependency failed)
    assert run_type_run.tasks["calculate_diff"].state == ExecutionState.SKIPPED, \
        f"calculate_diff should be SKIPPED (ftb_pricing_flow is SKIPPED) but was {run_type_run.tasks['calculate_diff'].state}"
    assert run_type_run.tasks["send_notification"].state == ExecutionState.SKIPPED, \
        f"send_notification should be SKIPPED but was {run_type_run.tasks['send_notification'].state}"


@pytest.mark.asyncio
async def test_retry_failed_subflow_from_parent_should_work(store):
    """
    Test: Retry a failed task inside a nested subflow from the parent level.

    Scenario:
    - Parent contains run_type_flow subflow
    - run_type_flow contains ftb_flow subflow
    - ftb_flow contains a task that fails
    - User wants to retry from the parent level and it should work

    This tests the retry mechanism across nested subflows.
    """
    # Create ftb flow with initially failing task
    ftb_firebird = AsyncFlow("ftb_firebird")
    call_count = {"count": 0}

    @ftb_firebird.task("build_ftb_image", max_retries=0)
    def build_ftb_image(ctx: Context):
        call_count["count"] += 1
        if call_count["count"] == 1:
            raise ValueError("FTB build failed on first attempt")
        return "ftb_image_built"

    # Create run_type_flow
    run_type_flow = AsyncFlow("run_type_flow")

    @run_type_flow.subflow("ftb_flow")
    def ftb_flow(ctx: Context):
        return ftb_firebird

    @run_type_flow.task("notify", depends_on=["ftb_flow"])
    def notify(ctx: Context):
        return "notification_sent"

    # Create parent
    parent = AsyncFlow("night_batch")

    @parent.subflow("run_type")
    def run_type(ctx: Context):
        return run_type_flow

    # Setup store
    parent._store = store
    run_type_flow._store = store
    ftb_firebird._store = store

    # First run - should fail
    run_id = parent.init_run()
    await parent.run_until_complete(run_id)

    parent_run = store.load(run_id)
    assert parent_run.state == ExecutionState.FAILED, "First run should fail"

    # Get the child run IDs
    run_type_run_id = parent_run.tasks["run_type"].output_json.get("child_run_id")
    run_type_run = store.load(run_type_run_id)

    ftb_flow_run_id = run_type_run.tasks["ftb_flow"].output_json.get("child_run_id")
    ftb_flow_run = store.load(ftb_flow_run_id)

    assert ftb_flow_run.tasks["build_ftb_image"].state == ExecutionState.FAILED, \
        "build_ftb_image should be FAILED"

    # Now retry from the ftb_firebird level
    await ftb_firebird.retry(ftb_flow_run_id, "build_ftb_image", reset_downstream=True)

    # Check that it succeeded
    ftb_flow_run = store.load(ftb_flow_run_id)
    assert ftb_flow_run.tasks["build_ftb_image"].state == ExecutionState.SUCCESS, \
        "After retry, build_ftb_image should be SUCCESS"
    assert ftb_flow_run.state == ExecutionState.SUCCESS, \
        "After retry, ftb_flow_run should be SUCCESS"

    # Now we need to retry the parent run_type_flow to pick up the success
    await run_type_flow.retry(run_type_run_id, "ftb_flow", reset_downstream=True)

    # Check the parent run_type_flow
    run_type_run = store.load(run_type_run_id)

    # After retrying the parent, the ftb_flow subflow should now be SUCCESS
    assert run_type_run.tasks["ftb_flow"].state == ExecutionState.SUCCESS, \
        f"After parent retry, ftb_flow should be SUCCESS but was {run_type_run.tasks['ftb_flow'].state}"

    # And the downstream task should also complete
    assert run_type_run.tasks["notify"].state == ExecutionState.SUCCESS, \
        f"After parent retry, notify should be SUCCESS but was {run_type_run.tasks['notify'].state}"

    assert run_type_run.state == ExecutionState.SUCCESS, \
        f"After parent retry, run_type_run should be SUCCESS but was {run_type_run.state}"


@pytest.mark.asyncio
async def test_complex_flow_with_multiple_subflows_and_failure(store):
    """
    Test: Complex scenario like night_batch with multiple parallel subflows where one fails.

    Topology:
        parent:
            [ftb_flow (subflow - fails), hpl_flow (subflow)] → calculate_diff → send_notification

    Verifies:
    - ftb_flow fails
    - hpl_flow succeeds
    - Both ftb_pricing and hpl_pricing are SKIPPED (they depend on failed/partial parent flows)
    - calculate_diff is SKIPPED (depends on both pricing flows)
    - send_notification is SKIPPED
    """
    # Create ftb firebird subflow (will fail)
    ftb_firebird = AsyncFlow("ftb_firebird")

    @ftb_firebird.task("build_ftb_image", max_retries=0)
    def build_ftb_image(ctx: Context):
        raise ValueError("FTB build failed")

    # Create ftb pricing subflow
    ftb_pricing = AsyncFlow("ftb_pricing")

    @ftb_pricing.task("run_ftb_pricing")
    def run_ftb_pricing(ctx: Context):
        return "ftb_pricing_done"

    # Create hpl firebird subflow (will succeed)
    hpl_firebird = AsyncFlow("hpl_firebird")

    @hpl_firebird.task("build_hpl_image")
    def build_hpl_image(ctx: Context):
        return "hpl_image_built"

    # Create hpl pricing subflow
    hpl_pricing = AsyncFlow("hpl_pricing")

    @hpl_pricing.task("run_hpl_pricing")
    def run_hpl_pricing(ctx: Context):
        return "hpl_pricing_done"

    # Create parent flow
    parent = AsyncFlow("night_batch_flow")

    # FTB flow chain
    @parent.subflow("ftb_flow")
    def ftb_flow(ctx: Context):
        return ftb_firebird

    @parent.subflow("ftb_pricing_flow", depends_on=["ftb_flow"])
    def ftb_pricing_flow(ctx: Context):
        return ftb_pricing

    # HPL flow chain
    @parent.subflow("hpl_flow")
    def hpl_flow(ctx: Context):
        return hpl_firebird

    @parent.subflow("hpl_pricing_flow", depends_on=["hpl_flow"])
    def hpl_pricing_flow(ctx: Context):
        return hpl_pricing

    # Final tasks
    @parent.task("calculate_diff", depends_on=["ftb_pricing_flow", "hpl_pricing_flow"])
    def calculate_diff(ctx: Context):
        return "diff_calculated"

    @parent.task("send_notification", depends_on=["calculate_diff"])
    def send_notification(ctx: Context):
        return "notification_sent"

    # Execute parent workflow
    parent._store = store
    ftb_firebird._store = store
    ftb_pricing._store = store
    hpl_firebird._store = store
    hpl_pricing._store = store

    run_id = parent.init_run()
    await parent.run_until_complete(run_id)

    # Assert final states
    run = store.load(run_id)
    assert run.state == ExecutionState.FAILED, "Parent workflow should be FAILED"

    # FTB chain
    assert run.tasks["ftb_flow"].state == ExecutionState.FAILED, "ftb_flow should be FAILED"
    assert run.tasks["ftb_pricing_flow"].state == ExecutionState.SKIPPED, \
        f"ftb_pricing_flow should be SKIPPED but was {run.tasks['ftb_pricing_flow'].state}"

    # HPL chain - should execute successfully since hpl_flow succeeds
    assert run.tasks["hpl_flow"].state == ExecutionState.SUCCESS, "hpl_flow should be SUCCESS"
    assert run.tasks["hpl_pricing_flow"].state == ExecutionState.SUCCESS, \
        f"hpl_pricing_flow should execute SUCCESS (only depends on hpl_flow) but was {run.tasks['hpl_pricing_flow'].state}"

    # Final tasks
    assert run.tasks["calculate_diff"].state == ExecutionState.SKIPPED, \
        f"calculate_diff should be SKIPPED but was {run.tasks['calculate_diff'].state}"
    assert run.tasks["send_notification"].state == ExecutionState.SKIPPED, \
        f"send_notification should be SKIPPED but was {run.tasks['send_notification'].state}"
