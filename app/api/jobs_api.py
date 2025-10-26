from fastapi import APIRouter, BackgroundTasks

from app.application.night_batch_flow import night_batch_flow

job_router = APIRouter(prefix="/jobs", tags=["Jobs"])


@job_router.get("/", summary="Get all jobs")
async def get_all():
    pass


@job_router.post("/", summary="Trigger a job")
async def trigger(background_tasks: BackgroundTasks):
    run_id = night_batch_flow.start_run(params={
        "run_types": ["hpl", "ftb"]
    })
    background_tasks.add_task(
        night_batch_flow.run_until_complete,
        run_id=run_id,
        max_concurrency=4
    )
    return run_id


@job_router.get("/{job_id}", summary="Get a job")
async def get_job(job_id: str):
    return night_batch_flow.get_execution_details(job_id)


@job_router.delete("/{job_id}", summary="Delete a job")
async def delete_job():
    pass
