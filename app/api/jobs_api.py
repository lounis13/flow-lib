from fastapi import APIRouter

job_router = APIRouter(prefix="/jobs", tags=["Jobs"])


@job_router.get("/", summary="Get all jobs")
async def get_all():
    pass


@job_router.post("/", summary="Trigger a job")
async def trigger():
    pass

@job_router.get("/{job_id}", summary="Get a job")
async def get_job():
    pass

@job_router.delete("/{job_id}", summary="Delete a job")
async def delete_job():
    pass

