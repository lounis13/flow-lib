import json
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional

from app.infra.flow.models import Store, WorkflowRun, TaskInstance


class SQLiteStore(Store):
    """
    Store persistant basé SQLite pour les runs et leurs tâches.

    Tables:
      - workflow_run(
          id TEXT PRIMARY KEY,
          flow_name TEXT,
          params_json TEXT,
          state TEXT,
          start_date TEXT,
          end_date TEXT,
          parent_run_id TEXT,
          parent_task_id TEXT
        )

      - task_instance(
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
          PRIMARY KEY (run_id, task_id, try_number),
          FOREIGN KEY (run_id) REFERENCES workflow_run(id) ON DELETE CASCADE
        )

    Indexes utiles sur (flow_name, state, parent_run_id, start_date).
    """

    def __init__(self, path: str = "runs.db"):
        self.path = path
        self.conn: sqlite3.Connection | None = None

    # ---------- Lifecycle ----------

    def open(self):
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        assert self.conn is not None
        cur = self.conn.cursor()

        cur.execute(
            """
            PRAGMA foreign_keys = ON
            """
        )

        cur.execute(
            """
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
                TEXT,
                parent_run_id
                TEXT,
                parent_task_id
                TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS task_instance
            (
                run_id
                TEXT,
                id
                TEXT,
                task_id
                TEXT,
                type
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
                task_id,
                try_number
            ),
                FOREIGN KEY
            (
                run_id
            ) REFERENCES workflow_run
            (
                id
            ) ON DELETE CASCADE
                )
            """
        )

        cur.execute("CREATE INDEX IF NOT EXISTS idx_run_flow ON workflow_run(flow_name)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_run_state ON workflow_run(state)")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_parent ON workflow_run(parent_run_id)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_start ON workflow_run(start_date)"
        )
        self.conn.commit()

    # ---------- Helpers ----------

    @staticmethod
    def _dt_to_str(d: Optional[datetime]) -> Optional[str]:
        return d.isoformat() if d else None

    @staticmethod
    def _str_to_dt(s: Optional[str]) -> Optional[datetime]:
        return datetime.fromisoformat(s) if s else None

    # ---------- Run CRUD ----------

    def save(self, run: WorkflowRun):
        """
        Upsert du run et écriture des tâches courantes (telles que présentes dans run.tasks).
        Remplace les tâches du run par celles passées.
        """
        if not self.conn:
            raise RuntimeError("open() must be called before save()")

        cur = self.conn.cursor()

        cur.execute(
            """
            INSERT INTO workflow_run (id, flow_name, params_json, state, start_date, end_date, parent_run_id,
                                      parent_task_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO
            UPDATE SET
                flow_name=excluded.flow_name,
                params_json=excluded.params_json,
                state =excluded.state,
                start_date=excluded.start_date,
                end_date=excluded.end_date,
                parent_run_id=excluded.parent_run_id,
                parent_task_id=excluded.parent_task_id
            """,
            (
                run.id,
                run.flow_name,
                json.dumps(run.params or {}),
                str(run.state) if not isinstance(run.state, str) else run.state,
                self._dt_to_str(run.start_date),
                self._dt_to_str(run.end_date),
                run.parent_run_id,
                run.parent_task_id,
            ),
        )

        # Pour simplifier: on remplace l'état des tâches par celles du run donné
        cur.execute("DELETE FROM task_instance WHERE run_id = ?", (run.id,))
        for t in (run.tasks or {}).values():
            cur.execute(
                """
                INSERT INTO task_instance (run_id, id, task_id, type, state, try_number, input_json, output_json, error,
                                           start_date, end_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run.id,
                    t.id,
                    t.task_id,
                    str(t.type) if not isinstance(t.type, str) else t.type,
                    str(t.state) if not isinstance(t.state, str) else t.state,
                    int(t.try_number or 0),
                    json.dumps(t.input_json) if t.input_json is not None else None,
                    json.dumps(t.output_json) if t.output_json is not None else None,
                    t.error,
                    self._dt_to_str(t.start_date),
                    self._dt_to_str(t.end_date),
                ),
            )

        self.conn.commit()

    def load(self, run_id: str) -> WorkflowRun:
        """
        Charge un run + toutes ses tâches (toutes tentatives). Pour run.tasks,
        on expose la dernière tentative (max try_number) par task_id.
        """
        if not self.conn:
            raise RuntimeError("open() must be called before load()")

        cur = self.conn.cursor()
        cur.execute("SELECT * FROM workflow_run WHERE id = ?", (run_id,))
        row = cur.fetchone()
        if not row:
            raise KeyError(f"WorkflowRun not found: {run_id}")

        # Charge toutes les tentatives de tâches
        cur.execute(
            """
            SELECT *
            FROM task_instance
            WHERE run_id = ?
            ORDER BY task_id ASC, try_number ASC
            """,
            (run_id,),
        )
        task_rows = cur.fetchall()

        # Ne garder que la dernière tentative par task_id
        latest_by_task_id: Dict[str, TaskInstance] = {}
        for tr in task_rows:
            ti = TaskInstance(
                id=tr["id"],
                task_id=tr["task_id"],
                type=tr["type"],
                state=tr["state"],
                try_number=tr["try_number"],
                input_json=json.loads(tr["input_json"]) if tr["input_json"] else None,
                output_json=json.loads(tr["output_json"]) if tr["output_json"] else None,
                error=tr["error"],
                start_date=self._str_to_dt(tr["start_date"]),
                end_date=self._str_to_dt(tr["end_date"]),
            )
            latest_by_task_id[ti.task_id] = ti  # écrase avec la plus récente (ordre ASC)

        run = WorkflowRun(
            id=row["id"],
            flow_name=row["flow_name"],
            params=json.loads(row["params_json"] or "{}"),
            state=row["state"],
            start_date=self._str_to_dt(row["start_date"]),
            end_date=self._str_to_dt(row["end_date"]),
            tasks=latest_by_task_id,
            parent_run_id=row["parent_run_id"],
            parent_task_id=row["parent_task_id"],
        )
        return run

    def mark_run_state(
            self,
            run_id: str,
            state: str,
            end_date: Optional[datetime] = None,
    ):
        """
        Met à jour l'état global du run (et éventuellement end_date).
        """
        if not self.conn:
            raise RuntimeError("open() must be called before mark_run_state()")

        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE workflow_run
            SET state    = ?,
                end_date = ?
            WHERE id = ?
            """,
            (str(state) if not isinstance(state, str) else state, self._dt_to_str(end_date), run_id),
        )
        self.conn.commit()

    def update_task(self, run_id: str, task: TaskInstance):
        """
        Upsert d'une tentative de tâche (clé: run_id, task_id, try_number).
        """
        if not self.conn:
            raise RuntimeError("open() must be called before update_task()")

        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO task_instance (run_id, task_id, type, state, try_number, input_json, output_json, error,
                                       start_date, end_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(run_id, task_id, try_number)
            DO
            UPDATE SET
                type=excluded.type,
                state =excluded.state,
                input_json=excluded.input_json,
                output_json=excluded.output_json,
                error=excluded.error,
                start_date=excluded.start_date,
                end_date=excluded.end_date
            """,
            (
                run_id,
                task.task_id,
                str(task.type) if not isinstance(task.type, str) else task.type,
                str(task.state) if not isinstance(task.state, str) else task.state,
                int(task.try_number or 0),
                json.dumps(task.input_json) if task.input_json is not None else None,
                json.dumps(task.output_json) if task.output_json is not None else None,
                task.error,
                self._dt_to_str(task.start_date),
                self._dt_to_str(task.end_date),
            ),
        )
        self.conn.commit()

    # ---------- Queries ----------

    def list_runs(self) -> List[str]:
        if not self.conn:
            raise RuntimeError("open() must be called before list_runs()")

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT id
            FROM workflow_run
            ORDER BY start_date DESC, id DESC
            """
        )
        return [r["id"] for r in cur.fetchall()]

    def list_children(self, parent_run_id: str) -> List[str]:
        if not self.conn:
            raise RuntimeError("open() must be called before list_children()")

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT id
            FROM workflow_run
            WHERE parent_run_id = ?
            ORDER BY start_date DESC, id DESC
            """,
            (parent_run_id,),
        )
        return [r["id"] for r in cur.fetchall()]

    def get_all_runs(self, include_tasks: bool = True) -> List[WorkflowRun]:
        if not self.conn:
            raise RuntimeError("open() must be called before get_all_runs()")

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM workflow_run
            ORDER BY start_date DESC, id DESC
            """
        )
        rows = cur.fetchall()

        runs: List[WorkflowRun] = []
        for row in rows:
            if include_tasks:
                runs.append(self.load(row["id"]))
            else:
                runs.append(
                    WorkflowRun(
                        id=row["id"],
                        flow_name=row["flow_name"],
                        params=json.loads(row["params_json"] or "{}"),
                        state=row["state"],
                        start_date=self._str_to_dt(row["start_date"]),
                        end_date=self._str_to_dt(row["end_date"]),
                        tasks={},  # non hydraté
                        parent_run_id=row["parent_run_id"],
                        parent_task_id=row["parent_task_id"],
                    )
                )
        return runs
