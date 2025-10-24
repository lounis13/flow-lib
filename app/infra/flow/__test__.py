# run_async_example_fail.py
import asyncio
import random
from datetime import timedelta

from app.infra.flow.flow import AsyncFlow, Context
from app.infra.flow.sqlite_store import SQLiteStore

store = SQLiteStore("database.db")
store.open()

flow = AsyncFlow("hpl_ftb_async", store=store)

def should_fail(ctx: Context, times: int = 1) -> bool:
    m = ctx.params.get("fail_times", {})
    n = int(m.get(ctx.task_id, 0))
    return ctx.try_number < n

@flow.task("trigger")
async def trigger(ctx: Context):
    ctx.log("trigger")
    ctx.push({"ok": True})

@flow.task("job", depends_on=["trigger"])
async def job(ctx: Context):
    ctx.log("job")
    ctx.push({"started": True})

# -------- HPL (candidate / reference) --------
for i in ["candidate", "reference"]:
    p = f"hpl_{i}"

    @flow.task(f"{p}_build", depends_on=["job"], retries=1, retry_delay=timedelta(milliseconds=200))
    async def build(ctx: Context, name=p):
        ctx.log(f"build {name}")
        ctx.push({"build": name})

    @flow.task(f"{p}_pricing", depends_on=[f"{p}_build"], retries=2, retry_delay=timedelta(milliseconds=250))
    async def pricing(ctx: Context, name=p):
        ctx.log(f"pricing {name} (try={ctx.try_number})")
        if should_fail(ctx):  # simuler un fail selon params.fail_times[task_id]
            raise RuntimeError("simulated failure in pricing")
        ctx.push({"pricing": True})

    @flow.task(f"{p}_collation", depends_on=[f"{p}_pricing"], retries=2, retry_delay=timedelta(milliseconds=250))
    async def collation(ctx: Context, name=p):
        if should_fail(ctx):  # tu peux simuler ici aussi si tu veux
            raise RuntimeError("simulated failure in collation")
        v = round(random.uniform(50, 150), 2)
        ctx.log(f"collation {name} -> {v}")
        ctx.push({"collation": v})

@flow.task(
    "hpl_comparing",
    depends_on=[f"hpl_{x}_collation" for x in ["candidate", "reference"]],
    retries=1,
    retry_delay=timedelta(milliseconds=200),
)
async def hpl_compare(ctx: Context):
    a = ctx.pull("hpl_candidate_collation")["collation"]
    b = ctx.pull("hpl_reference_collation")["collation"]
    diff = round(a - b, 2)
    ctx.log(f"HPL {a} - {b} = {diff}")
    ctx.push({"diff": diff})

@flow.task("hpl_notify", depends_on=["hpl_comparing"])
async def hpl_notify(ctx: Context):
    ctx.log(f"HPL diff={ctx.pull('hpl_comparing')['diff']}")

# -------- FTB (candidate / reference) --------
for i in ["candidate", "reference"]:
    p = f"ftb_{i}"

    @flow.task(f"{p}_build", depends_on=["job"], retries=1)
    async def build(ctx: Context, name=p):
        ctx.log(f"build {name}")
        ctx.push({"build": name})

    @flow.task(f"{p}_pricing", depends_on=[f"{p}_build"], retries=1)
    async def pricing(ctx: Context, name=p):
        ctx.log(f"pricing {name}")
        ctx.push({"pricing": True})

    @flow.task(f"{p}_collation", depends_on=[f"{p}_pricing"], retries=3, retry_delay=timedelta(milliseconds=200))
    async def collation(ctx: Context, name=p):
        if should_fail(ctx):  # possible fail ici aussi
            raise RuntimeError("simulated failure in collation")
        v = round(random.uniform(50, 150), 2)
        ctx.log(f"collation {name} -> {v}")
        ctx.push({"collation": v})

@flow.task("ftb_comparing", depends_on=[f"ftb_{x}_collation" for x in ["candidate", "reference"]], retries=1)
async def ftb_compare(ctx: Context):
    a = ctx.pull("ftb_candidate_collation")["collation"]
    b = ctx.pull("ftb_reference_collation")["collation"]
    diff = round(a - b, 2)
    ctx.log(f"FTB {a} - {b} = {diff}")
    ctx.push({"diff": diff})

@flow.task("ftb_notify", depends_on=["ftb_comparing"])
async def ftb_notify(ctx: Context):
    ctx.log(f"FTB diff={ctx.pull('ftb_comparing')['diff']}")

@flow.task("end", depends_on=["hpl_notify", "ftb_notify"])
async def end(ctx: Context):
    ctx.log("END ✅")

run_id = flow.start_run(params={
    "version": "async-fail-1",
    # Simule 1 échec sur hpl_candidate_pricing puis succès au retry,
    # et 2 échecs sur ftb_reference_collation avant succès.
    "fail_times": {
        "hpl_candidate_pricing": 1,
        "ftb_reference_collation": 2
    }
})
asyncio.run(flow.run_until_complete(run_id, max_concurrency=8))
store.close()
