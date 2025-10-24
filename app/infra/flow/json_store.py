import json
import os
from dataclasses import asdict
from datetime import datetime
from typing import List

from app.infra.flow.models import Store, WorkflowRun, TaskInstance


class JsonStore(Store):
    def __init__(self, folder="./runs"):
        self.folder = folder
        os.makedirs(folder, exist_ok=True)

    def open(self): pass

    def close(self): pass

    def _path(self, run_id: str): return os.path.join(self.folder, f"{run_id}.json")

    def save(self, run: WorkflowRun):
        data = asdict(run)

        def conv(o): return o.isoformat() if isinstance(o, datetime) else o

        with open(self._path(run.id), "w") as f:
            json.dump(data, f, indent=2, default=conv)

    def load(self, run_id: str) -> WorkflowRun:
        with open(self._path(run_id)) as f:
            data = json.load(f)
        tasks = {tid: TaskInstance(**t) for tid, t in data["tasks"].items()}
        run = WorkflowRun(
            id=data["id"],
            flow_name=data["flow_name"],
            params=data.get("params", {}),
            state=data["state"],
            start_date=datetime.fromisoformat(data["start_date"]),
            end_date=datetime.fromisoformat(data["end_date"]) if data["end_date"] else None,
            tasks=tasks,
        )
        return run

    def exists(self, run_id: str) -> bool: return os.path.exists(self._path(run_id))

    def list_runs(self) -> List[str]: return [p.replace(".json", "") for p in os.listdir(self.folder) if
                                              p.endswith(".json")]
