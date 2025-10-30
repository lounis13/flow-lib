import asyncio
import uuid

from app.application.flow_store import store
from app.infra.flow import AsyncFlow, Context


def build_firebird_image_flow(name: str) -> AsyncFlow:
    flow = AsyncFlow(f"firebird_image_flow", store)

    @flow.task(f"{name}_last_commit", name="Get last commit")
    async def last_commit(ctx: Context):
        ctx.log(f"Getting last commit for {ctx.params['version']}")
        await asyncio.sleep(3)
        ctx.log(f"Firebird last commit for {ctx.params['version']} is : {str(uuid.uuid4())}")
        ctx.push({**ctx.params, "last_commit": str(uuid.uuid4())})


    return flow
