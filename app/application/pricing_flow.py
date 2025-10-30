from app.application.flow_store import store
from app.infra.flow import AsyncFlow


def build_pricing_flow(name: str):
    flow = AsyncFlow("Pricing", store)
    @flow.task(f"{name}_run_multiple_price", name="Run multiple price")
    async def run_multiple_price(ctx):
        ctx.log(f"Trigger multi price {ctx.params}")
    return flow
