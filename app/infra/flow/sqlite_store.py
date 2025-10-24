import json
import sqlite3
from datetime import datetime
from typing import List

from flow import Store, WorkflowRun, TaskInstance


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
                    CREATE TABLE IF NOT EXISTS workflow_run
                    (
                        id
                        TEXT
                        PRIMARY
                        KEY,
                        flow_name
                        TEXT,
                        params_json
                        TEXT,
                        state
                        TEXT,
                        start_date
                        TEXT,
                        end_date
                        TEXT
                    )
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS task_instance
                    (
                        run_id
                        TEXT,
                        task_id
                        TEXT,
                        state
                        TEXT,
                        try_number
                        INTEGER,
                        input_json
                        TEXT,
                        output_json
                        TEXT,
                        error
                        TEXT,
                        start_date
                        TEXT,
                        end_date
                        TEXT,
                        PRIMARY
                        KEY
                    (
                        run_id,
                        task_id
                    )
                        )
                    """)
        self.conn.commit()

    def save(self, run: WorkflowRun):
        cur = self.conn.cursor()
        cur.execute("""
        INSERT OR REPLACE INTO workflow_run (id, flow_name, params_json, state, start_date, end_date)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (
            run.id,
            run.flow_name,
            json.dumps(run.params),
            run.state,
            run.start_date.isoformat(),
            run.end_date.isoformat() if run.end_date else None
        ))
        for task_id, t in run.tasks.items():
            cur.execute("""
            INSERT OR REPLACE INTO task_instance (
                run_id, task_id, state, try_number, input_json, output_json, error, start_date, end_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run.id,
                task_id,
                t.state,
                t.try_number,
                json.dumps(t.input_json),
                json.dumps(t.output_json),
                t.error,
                t.start_date.isoformat() if t.start_date else None,
                t.end_date.isoformat() if t.end_date else None,
            ))
        self.conn.commit()

    def load(self, run_id: str) -> WorkflowRun:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM workflow_run WHERE id = ?", (run_id,))
        row = cur.fetchone()
        if not row:
            raise KeyError(f"Run {run_id} not found")
        run = WorkflowRun(
            id=row["id"],
            flow_name=row["flow_name"],
            params=json.loads(row["params_json"] or "{}"),
            state=row["state"],
            start_date=datetime.fromisoformat(row["start_date"]),
            end_date=datetime.fromisoformat(row["end_date"]) if row["end_date"] else None,
            tasks={}
        )
        cur.execute("SELECT * FROM task_instance WHERE run_id = ?", (run_id,))
        for tr in cur.fetchall():
            run.tasks[tr["task_id"]] = TaskInstance(
                task_id=tr["task_id"],
                state=tr["state"],
                try_number=tr["try_number"],
                input_json=json.loads(tr["input_json"] or "{}"),
                output_json=json.loads(tr["output_json"] or "null"),
                error=tr["error"],
                start_date=datetime.fromisoformat(tr["start_date"]) if tr["start_date"] else None,
                end_date=datetime.fromisoformat(tr["end_date"]) if tr["end_date"] else None
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
