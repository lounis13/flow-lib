from fastapi import APIRouter

from app.api.jobs_api import job_router
from app.api.runs_api import run_router

api_router = APIRouter(prefix="/api")

api_router.include_router(job_router)
api_router.include_router(run_router)
