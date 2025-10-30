from fastapi import APIRouter

run_router = APIRouter(prefix="/runs", tags=["Runs"])

@run_router.get("/", summary="Get all runs")
async def get_all():
    pass


@run_router.post("/", summary="Trigger a run")
async def trigger():
    pass

@run_router.get("/{run_id}", summary="Get a run")
async def get_run():

    pass