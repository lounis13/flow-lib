from fastapi import APIRouter, BackgroundTasks

from app.application.flow_store import store
from app.application.night_batch_flow import build_night_batch_flow
from app.infra.flow import AsyncFlow

job_router = APIRouter(prefix="/jobs", tags=["Jobs"])


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


@job_router.delete("/{job_id}", summary="Delete a job")
async def delete_job():
    pass
