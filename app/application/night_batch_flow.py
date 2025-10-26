from app.application.firebird_image_flow import build_firebird_image_flow
from app.application.flow_store import store
from app.application.pricing_flow import build_pricing_flow
from app.infra.flow import AsyncFlow, Context


def build_run_type_flow(params: dict):
    flow = AsyncFlow("night_barch_flow", store)
    libraries = params.get("libraries")
    pricing_flows = []
    for library in libraries:
        version = library.get("version")

        @flow.subflow(f"{version}_flow", params=library)
        def build_library_flow(ctx: Context):
            return build_firebird_image_flow()

        pricing_flows.append(f"{version}_pricing_flow")

        @flow.subflow(f"{version}_pricing_flow", depends_on=[f"{version}_flow"])
        def build_library_flow(ctx: Context):
            return build_pricing_flow()

    @flow.task("calculate_difference", depends_on=pricing_flows)
    def calculate_difference(ctx: Context):
        ctx.log(f"Calculating difference for {ctx.params}")

    @flow.task("send_notification", depends_on=["calculate_difference"])
    def send_notification(ctx: Context):
        ctx.log(f"send notification for this run {ctx.params}")

    return flow


def build_night_batch_flow(params: dict = None):
    flow = AsyncFlow("night_barch_flow", store)
    run_type_flows = [f"{run_type.get('type')}_flow" for run_type in params.get("run_types")]
    for run_type_flow_name in run_type_flows:
        @flow.subflow(run_type_flow_name)
        def run_type_flow(ctx: Context):
            return build_run_type_flow(params)

    @flow.task("send_notification", depends_on=run_type_flows)
    def end(ctx: Context):
        ctx.log(f"send final notification")

    return flow
