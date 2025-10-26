import json
import sqlite3
from datetime import datetime
from typing import List

from app.infra.flow.models import Store, WorkflowRun, TaskInstance

class SQLiteStore(Store):
    def __init__(self, path: str = "runs.db"):
        self.path = path
        self.conn: sqlite3.Connection | None = None

    def open(self):
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def _init_schema(self):
        cur = self.conn.cursor()
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS workflow_run (
                                                                id TEXT PRIMARY KEY,
                                                                flow_name TEXT,
                                                                params_json TEXT,
                                                                state TEXT,
                                                                start_date TEXT,
                                                                end_date TEXT,
                                                                parent_run_id TEXT,
                                                                parent_task_id TEXT
                    )
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS task_instance (
                                                                 run_id TEXT,
                                                                 task_id TEXT,
                                                                 type TEXT,
                                                                 state TEXT,
                                                                 try_number INTEGER,
                                                                 input_json TEXT,
                                                                 output_json TEXT,
                                                                 error TEXT,
                                                                 start_date TEXT,
                                                                 end_date TEXT,
                                                                 PRIMARY KEY (run_id, task_id)
                        )
                    """)
        self.conn.commit()

    def _dt_to_str(self, d: datetime | None) -> str | None:
        return d.isoformat() if d else None

    def _str_to_dt(self, s: str | None) -> datetime | None:
        return datetime.fromisoformat(s) if s else None

    def save(self, run: WorkflowRun):
        if not self.conn:
            raise RuntimeError("open() must be called before save()")
        cur = self.conn.cursor()
        cur.execute("""
        INSERT OR REPLACE INTO workflow_run
        (id, flow_name, params_json, state, start_date, end_date, parent_run_id, parent_task_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            run.id,
            run.flow_name,
            json.dumps(run.params),
            run.state,
            self._dt_to_str(run.start_date),
            self._dt_to_str(run.end_date),
            run.parent_run_id,
            run.parent_task_id,
        ))
        for tid, t in run.tasks.items():
            cur.execute("""
            INSERT OR REPLACE INTO task_instance
            (run_id, task_id, type, state, try_number, input_json, output_json, error, start_date, end_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run.id,
                t.task_id,
                t.type,
                t.state,
                t.try_number,
                json.dumps(t.input_json),
                json.dumps(t.output_json),
                t.error,
                self._dt_to_str(t.start_date),
                self._dt_to_str(t.end_date),
            ))
        self.conn.commit()

    def load(self, run_id: str) -> WorkflowRun:
        if not self.conn:
            raise RuntimeError("open() must be called before load()")
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM workflow_run WHERE id = ?", (run_id,))
        row = cur.fetchone()
        if not row:
            raise KeyError(run_id)
        run = WorkflowRun(
            id=row["id"],
            flow_name=row["flow_name"],
            params=json.loads(row["params_json"] or "{}"),
            state=row["state"],
            start_date=self._str_to_dt(row["start_date"]),
            end_date=self._str_to_dt(row["end_date"]),
            tasks={},
            parent_run_id=row["parent_run_id"],
            parent_task_id=row["parent_task_id"],
        )
        cur.execute("SELECT * FROM task_instance WHERE run_id = ?", (run_id,))
        for tr in cur.fetchall():
            run.tasks[tr["task_id"]] = TaskInstance(
                task_id=tr["task_id"],
                type=tr["type"],
                state=tr["state"],
                try_number=tr["try_number"],
                input_json=json.loads(tr["input_json"] or "{}"),
                output_json=json.loads(tr["output_json"] or "null"),
                error=tr["error"],
                start_date=self._str_to_dt(tr["start_date"]),
                end_date=self._str_to_dt(tr["end_date"]),
            )
        return run

    def exists(self, run_id: str) -> bool:
        cur = self.conn.cursor()
        cur.execute("SELECT 1 FROM workflow_run WHERE id = ?", (run_id,))
        return cur.fetchone() is not None

    def list_runs(self) -> List[str]:
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM workflow_run ORDER BY start_date DESC")
        return [r["id"] for r in cur.fetchall()]

    def list_children(self, parent_run_id: str) -> List[str]:
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM workflow_run WHERE parent_run_id = ?", (parent_run_id,))
        return [r["id"] for r in cur.fetchall()]

    def get_all_tasks_with_subflows(self, run_id: str) -> List[dict]:
        """
        Retourne toutes les tâches d'un run incluant celles des subflows de manière récursive.
        Chaque tâche contient son chemin complet (ex: 'parent_flow.subflow.task')
        """
        run = self.load(run_id)
        all_tasks_dict = run.get_all_tasks_recursive(self)

        # Convertir en liste de dicts pour faciliter l'utilisation
        result = []
        for full_task_id, task_instance in all_tasks_dict.items():
            result.append({
                "full_task_id": full_task_id,
                "task_id": task_instance.task_id,
                "type": task_instance.type,
                "state": task_instance.state,
                "try_number": task_instance.try_number,
                "start_date": self._dt_to_str(task_instance.start_date),
                "end_date": self._dt_to_str(task_instance.end_date),
                "error": task_instance.error,
                "output_json": task_instance.output_json,
            })

        return result
