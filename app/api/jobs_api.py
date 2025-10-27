from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from app.application.flow_store import store
from app.application.night_batch_flow import build_night_batch_flow
from app.application.pricing_flow import build_pricing_flow
from app.infra.flow import AsyncFlow

job_router = APIRouter(prefix="/jobs", tags=["Jobs"])


class RetryRequest(BaseModel):
    """Request schema for retrying a task."""
    task_id: str = Field(..., description="ID of the task to retry (can be nested like 'ftb_flow.pricing_flow.task1')")
    reset_downstream: bool = Field(default=True, description="Whether to reset all downstream dependent tasks")
    max_concurrency: int = Field(default=4, ge=1, description="Maximum number of concurrent tasks during execution")


@job_router.get("/", summary="Get all jobs")
async def get_all():
    """
    Get all jobs with summary information and aggregated statistics.

    Returns:
        - List of all jobs with basic info and task statistics
        - Aggregated stats across all jobs (total, running, failed, success)
        - Total task count and average duration
    """
    return AsyncFlow.list_all_jobs(store)


@job_router.post("/", summary="Trigger a job")
async def trigger(background_tasks: BackgroundTasks):
    params = {
        "libraries": [{"version": "1.2.0"}, {"version": "1.3.1"}],
        "run_types": [{"type": "ftb"}, {"type": "hpl"}]
    }
    flow = build_night_batch_flow(params=params)
    run_id = flow.init_run(params=params)
    background_tasks.add_task(
        flow.run_until_complete,
        run_id=run_id,
        max_concurrency=4
    )
    return run_id


@job_router.get("/{job_id}", summary="Get a job (flat format)")
async def get_job_flat(job_id: str):
    job = store.load(job_id)
    flow = build_night_batch_flow(job.params)
    return flow.get_flat_execution_details(job_id)


@job_router.get("/{job_id}/flow", summary="Get a job")
async def get_job(job_id: str):
    """Get job details in hierarchical format (original endpoint)."""
    job = store.load(job_id)
    flow = build_night_batch_flow(job.params)
    return flow.get_execution_details(job_id)


@job_router.post("/{job_id}/retry", summary="Retry a task")
async def retry_task(job_id: str, retry_request: RetryRequest, background_tasks: BackgroundTasks):
    """
    Retry a specific task within a job.

    This endpoint allows you to retry any task in the workflow hierarchy:
    - Simple task: `task_id = "calculate_difference"`
    - Task in subflow: `task_id = "ftb_flow.calculate_price"`
    - Task in nested subflow: `task_id = "ftb_flow.pricing_flow.task1"`
    - Entire subflow: `task_id = "ftb_flow"` (retries all tasks within the subflow)

    Args:
        job_id: ID of the job (workflow run)
        retry_request: Retry configuration
        background_tasks: FastAPI background tasks handler

    Returns:
        Message confirming the retry was triggered

    Raises:
        HTTPException: If the job or task doesn't exist
    """
    # Verify job exists
    if not store.exists(job_id):
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    # Load job and rebuild flow
    try:
        job = store.load(job_id)
        flow = build_night_batch_flow(job.params) if job.flow_name == "Night Batch Flow" else build_pricing_flow("Pricing")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load job: {str(e)}")

    # Validate task exists (delegated to flow)
    try:
        flow.validate_task_exists(run_id=job_id, task_id=retry_request.task_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))

    # Schedule retry in background (delegated to flow)
    background_tasks.add_task(
        flow.retry,
        run_id=job_id,
        task_id=retry_request.task_id,
        reset_downstream=retry_request.reset_downstream,
        max_concurrency=retry_request.max_concurrency
    )

    return {
        "message": f"Retry triggered for task '{retry_request.task_id}' in job '{job_id}'",
        "job_id": job_id,
        "task_id": retry_request.task_id,
        "reset_downstream": retry_request.reset_downstream,
        "max_concurrency": retry_request.max_concurrency
    }


@job_router.post("/{job_id}/retry-all", summary="Retry all failed tasks")
async def retry_all_failed_tasks(job_id: str, background_tasks: BackgroundTasks, max_concurrency: int = 4):
    """
    Retry all failed tasks in a job.

    This endpoint will:
    1. Find all tasks in 'failed' state
    2. Reset them to 'scheduled' state
    3. Resume workflow execution

    Args:
        job_id: ID of the job (workflow run)
        background_tasks: FastAPI background tasks handler
        max_concurrency: Maximum number of concurrent tasks during execution

    Returns:
        Message confirming the retry was triggered with the list of tasks being retried

    Raises:
        HTTPException: If the job doesn't exist or has no failed tasks
    """
    # Verify job exists
    if not store.exists(job_id):
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    # Load job and rebuild flow
    try:
        job = store.load(job_id)
        flow = build_night_batch_flow(job.params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load job: {str(e)}")

    # Retry all failed tasks (delegated to flow)
    async def retry_all_wrapper():
        try:
            return await flow.retry_all_failed(run_id=job_id, max_concurrency=max_concurrency)
        except ValueError as e:
            # Will be caught but task is already started, we can't return HTTPException here
            return []

    # Check if there are failed tasks before scheduling
    try:
        failed_tasks = flow.get_all_failed_tasks(run_id=job_id)
        if not failed_tasks:
            raise HTTPException(status_code=400, detail=f"No failed tasks found in job '{job_id}'")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Schedule retry in background
    background_tasks.add_task(retry_all_wrapper)

    return {
        "message": f"Retry triggered for {len(failed_tasks)} failed task(s) in job '{job_id}'",
        "job_id": job_id,
        "failed_tasks": failed_tasks,
        "max_concurrency": max_concurrency
    }
