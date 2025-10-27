from app.application.firebird_image_flow import build_firebird_image_flow
from app.application.flow_store import store
from app.application.pricing_flow import build_pricing_flow
from app.infra.flow import AsyncFlow, Context


def build_run_type_flow(params: dict, type: str):
    flow = AsyncFlow("night_barch_flow", store)
    libraries = params.get("libraries")
    pricing_flows = []
    for library in libraries:
        version = library.get("version")

        @flow.subflow(f"{type}_{version}_flow", name="Build Fire Bird Image", params=library)
        def build_library_flow(ctx: Context):
            return build_firebird_image_flow(f"{type}_{version}")

        pricing_flows.append(f"{type}_{version}_pricing_flow")

        @flow.subflow(f"{type}_{version}_pricing_flow", depends_on=[f"{type}_{version}_flow"])
        def build_library_flow(ctx: Context):
            return build_pricing_flow(f"{type}_{version}_pricing")

    @flow.task(f"{type}_calculate_difference", depends_on=pricing_flows)
    def calculate_difference(ctx: Context):
        ctx.log(f"Calculating difference for {ctx.params}")

    @flow.task(f"{type}_send_notification", depends_on=[f"{type}_calculate_difference"])
    def send_notification(ctx: Context):
        ctx.log(f"send notification for this run {ctx.params}")

    return flow


def build_night_batch_flow(params: dict = None):
    flow = AsyncFlow("Night Batch Flow", store)

    @flow.task("Start Night Batch Flow", name="Start Night Batch Flow")
    def end(ctx: Context):
        ctx.log(f"Start Night Batch Flow")

    @flow.subflow(f"Pricing Flow", depends_on=["Start Night Batch Flow"])
    def build_library_flow(ctx: Context):
        return build_pricing_flow("Pricing")

    # run_type_flows = [f"{run_type.get('type')}_flow" for run_type in params.get("run_types")]
    # for run_type_flow_name in run_type_flows:
    #    @flow.subflow(run_type_flow_name)
    #    def run_type_flow(ctx: Context):
    #        return build_run_type_flow(params, type=run_type_flow_name)

    # @flow.task("send_notification", depends_on=run_type_flows, name="Send Symphony notification")
    # def end(ctx: Context):
    #    ctx.log(f"send final notification")

    return flow
