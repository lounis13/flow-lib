# Test complet: flow avec subflows parallèles
# Structure: A -> B -> [C_A, C_B] -> D
import asyncio

from app.infra.flow.flow import AsyncFlow
from app.infra.flow.sqlite_store import SQLiteStore

# ============================================================
# Configuration du store
# ============================================================
store = SQLiteStore("nb.db")
store.open()



# ============================================================
# Création des flows
# ============================================================
night_batch_flow = AsyncFlow("main_flow", store)
subflow_c = AsyncFlow("subflow_c", store)  # Sera réutilisé pour C_A et C_B


# ============================================================
# Définition du flow principal
# ============================================================
@night_batch_flow.task("task_A")
def task_a(ctx):
    ctx.log("🚀 Exécution de Task A")
    ctx.push({"data_from_a": "value_a", "count": 10})


@night_batch_flow.task("task_B", depends_on=["task_A"])
def task_b(ctx):
    ctx.log("📦 Exécution de Task B")
    data_a = ctx.pull("task_A")
    ctx.log(f"   Données reçues de A: {data_a}")
    ctx.push({"data_from_b": "value_b", "count": data_a.get("count", 0) + 5})


@night_batch_flow.task("task_D", depends_on=["subflow_C_A", "subflow_C_B"])
def task_d(ctx):
    ctx.log("🎯 Exécution de Task D (agrégation)")
    data_c_a = ctx.pull("subflow_C_A")
    data_c_b = ctx.pull("subflow_C_B")
    ctx.log(f"   Données reçues de C_A: {data_c_a}")
    ctx.log(f"   Données reçues de C_B: {data_c_b}")

    # Agréger les résultats
    total = 0
    if data_c_a and "result" in data_c_a:
        total += data_c_a["result"]
    if data_c_b and "result" in data_c_b:
        total += data_c_b["result"]

    ctx.log(f"   ✅ Total agrégé: {total}")
    ctx.push({"aggregated_total": total})


# ============================================================
# Définition du subflow C (réutilisé pour C_A et C_B)
# ============================================================
@subflow_c.task("c_task_1")
def c_task_1(ctx):
    ctx.log(f"  🔹 Subflow {ctx.run.flow_name} - Task 1")
    # Récupérer les données passées depuis le parent
    count = ctx.params.get("count", 0)
    data_b = ctx.params.get("data_from_b", "no_data")
    ctx.log(f"     Params reçus: count={count}, data_from_b={data_b}")
    ctx.push({"intermediate": count * 2})


@subflow_c.task("c_task_2", depends_on=["c_task_1"])
def c_task_2(ctx):
    ctx.log(f"  🔹 Subflow {ctx.run.flow_name} - Task 2")
    data_1 = ctx.pull("c_task_1")
    intermediate = data_1.get("intermediate", 0) if data_1 else 0
    ctx.log(f"     Intermediate value: {intermediate}")
    ctx.push({"result": intermediate + 10})


# ============================================================
# Enregistrement des subflows dans le flow principal
# ============================================================
night_batch_flow.add_subflow("subflow_C_A", subflow_c, depends_on=["task_B"])
night_batch_flow.add_subflow("subflow_C_B", subflow_c, depends_on=["task_B"])
