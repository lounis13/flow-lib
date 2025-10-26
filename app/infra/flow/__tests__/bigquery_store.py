from __future__ import annotations

import json
from datetime import datetime
from typing import Dict, List, Optional, Any

from google.cloud import bigquery

from app.infra.flow.models import Store, WorkflowRun, TaskInstance


def _json_out(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, (bytes, bytearray)):
        try:
            return json.loads(v.decode("utf-8"))
        except Exception:
            return None
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return None
    return v


class BigQueryStore(Store):
    def __init__(self, project: Optional[str] = None, dataset: str = "flows", runs_table: str = "workflow_run",
                 tasks_table: str = "task_instance", location: Optional[str] = None):
        self.project = project
        self.dataset_id = dataset
        self.runs_table_name = runs_table
        self.tasks_table_name = tasks_table
        self.location = location
        self._client: Optional[bigquery.Client] = None

    def open(self):
        self._client = bigquery.Client(project=self.project, location=self.location)
        self._ensure_dataset()
        self._ensure_tables()

    @property
    def client(self) -> bigquery.Client:
        if not self._client:
            raise RuntimeError("open() must be called before using BigQueryStore")
        return self._client

    @property
    def runs_table(self) -> str:
        return f"`{self.client.project}.{self.dataset_id}.{self.runs_table_name}`"

    @property
    def tasks_table(self) -> str:
        return f"`{self.client.project}.{self.dataset_id}.{self.tasks_table_name}`"

    def _ensure_dataset(self):
        ref = bigquery.DatasetReference(self.client.project, self.dataset_id)
        try:
            self.client.get_dataset(ref)
        except Exception:
            ds = bigquery.Dataset(ref)
            if self.location:
                ds.location = self.location
            self.client.create_dataset(ds, exists_ok=True)

    def _ensure_tables(self):
        self.client.query(
            f"CREATE TABLE IF NOT EXISTS {self.runs_table} (id STRING NOT NULL, flow_name STRING, params_json JSON, state STRING, start_date TIMESTAMP, end_date TIMESTAMP, parent_run_id STRING, parent_task_id STRING)").result()
        self.client.query(
            f"CREATE TABLE IF NOT EXISTS {self.tasks_table} (run_id STRING NOT NULL, task_id STRING NOT NULL, try_number INT64 NOT NULL, type STRING, state STRING, input_json JSON, output_json JSON, error STRING, start_date TIMESTAMP, end_date TIMESTAMP)").result()

    def save(self, run: WorkflowRun):
        q = f"""
        MERGE {self.runs_table} T
        USING (
          SELECT
            @id AS id, @flow_name AS flow_name, @params_json AS params_json, @state AS state,
            @start_date AS start_date, @end_date AS end_date,
            @parent_run_id AS parent_run_id, @parent_task_id AS parent_task_id
        ) S
        ON T.id = S.id
        WHEN MATCHED THEN UPDATE SET
          flow_name = S.flow_name,
          params_json = S.params_json,
          state = S.state,
          start_date = S.start_date,
          end_date = S.end_date,
          parent_run_id = S.parent_run_id,
          parent_task_id = S.parent_task_id
        WHEN NOT MATCHED THEN INSERT (
          id, flow_name, params_json, state, start_date, end_date, parent_run_id, parent_task_id
        ) VALUES (
          S.id, S.flow_name, S.params_json, S.state, S.start_date, S.end_date, S.parent_run_id, S.parent_task_id
        )
        """
        self.client.query(
            q,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("id", "STRING", run.id),
                    bigquery.ScalarQueryParameter("flow_name", "STRING", run.flow_name),
                    bigquery.ScalarQueryParameter("params_json", "JSON", json.dumps(run.params or {})),
                    bigquery.ScalarQueryParameter("state", "STRING",
                                                  str(run.state) if not isinstance(run.state, str) else run.state),
                    bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", run.start_date),
                    bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", run.end_date),
                    bigquery.ScalarQueryParameter("parent_run_id", "STRING", run.parent_run_id),
                    bigquery.ScalarQueryParameter("parent_task_id", "STRING", run.parent_task_id),
                ]
            ),
        ).result()
        self.client.query(
            f"DELETE FROM {self.tasks_table} WHERE run_id = @rid",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("rid", "STRING", run.id)]
            ),
        ).result()
        rows = []
        for t in (run.tasks or {}).values():
            rows.append(
                {
                    "run_id": run.id,
                    "task_id": t.task_id,
                    "try_number": int(t.try_number or 0),
                    "type": str(t.type) if not isinstance(t.type, str) else t.type,
                    "state": str(t.state) if not isinstance(t.state, str) else t.state,
                    "input_json": t.input_json if t.input_json is not None else None,
                    "output_json": t.output_json if t.output_json is not None else None,
                    "error": t.error,
                    "start_date": t.start_date,
                    "end_date": t.end_date,
                }
            )
        if rows:
            errors = self.client.insert_rows_json(f"{self.client.project}.{self.dataset_id}.{self.tasks_table_name}",
                                                  rows)
            if errors:
                raise RuntimeError(f"insert_rows_json errors: {errors}")

    def load(self, run_id: str) -> WorkflowRun:
        r_meta = list(
            self.client.query(
                f"""
                SELECT id, flow_name, params_json, state, start_date, end_date, parent_run_id, parent_task_id
                FROM {self.runs_table}
                WHERE id = @rid
                """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("rid", "STRING", run_id)]
                ),
            ).result()
        )
        if not r_meta:
            raise KeyError(f"WorkflowRun not found: {run_id}")
        r = r_meta[0]
        latest_by_task: Dict[str, TaskInstance] = {}
        for tr in self.client.query(
                f"""
            SELECT run_id, task_id, try_number, type, state, input_json, output_json, error, start_date, end_date
            FROM {self.tasks_table}
            WHERE run_id = @rid
            ORDER BY task_id ASC, try_number ASC
            """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("rid", "STRING", run_id)]
                ),
        ).result():
            ti = TaskInstance(
                task_id=tr["task_id"],
                type=tr["type"],
                state=tr["state"],
                try_number=tr["try_number"],
                input_json=_json_out(tr["input_json"]),
                output_json=_json_out(tr["output_json"]),
                error=tr["error"],
                start_date=tr["start_date"],
                end_date=tr["end_date"],
            )
            latest_by_task[ti.task_id] = ti
        return WorkflowRun(
            id=r["id"],
            flow_name=r["flow_name"],
            params=_json_out(r["params_json"]) or {},
            state=r["state"],
            start_date=r["start_date"],
            end_date=r["end_date"],
            tasks=latest_by_task,
            parent_run_id=r["parent_run_id"],
            parent_task_id=r["parent_task_id"],
        )

    def mark_run_state(self, run_id: str, state: str, end_date: Optional[datetime] = None):
        self.client.query(
            f"""
            UPDATE {self.runs_table}
            SET state = @st, end_date = @ed
            WHERE id = @rid
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("st", "STRING", state),
                    bigquery.ScalarQueryParameter("ed", "TIMESTAMP", end_date),
                    bigquery.ScalarQueryParameter("rid", "STRING", run_id),
                ]
            ),
        ).result()

    def update_task(self, run_id: str, task: TaskInstance):
        q = f"""
        MERGE {self.tasks_table} T
        USING (
          SELECT
            @run_id AS run_id, @task_id AS task_id, @try_number AS try_number,
            @type AS type, @state AS state, @input_json AS input_json, @output_json AS output_json,
            @error AS error, @start_date AS start_date, @end_date AS end_date
        ) S
        ON T.run_id = S.run_id AND T.task_id = S.task_id AND T.try_number = S.try_number
        WHEN MATCHED THEN UPDATE SET
          type = S.type,
          state = S.state,
          input_json = S.input_json,
          output_json = S.output_json,
          error = S.error,
          start_date = S.start_date,
          end_date = S.end_date
        WHEN NOT MATCHED THEN INSERT (
          run_id, task_id, try_number, type, state, input_json, output_json, error, start_date, end_date
        ) VALUES (
          S.run_id, S.task_id, S.try_number, S.type, S.state, S.input_json, S.output_json, S.error, S.start_date, S.end_date
        )
        """
        self.client.query(
            q,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                    bigquery.ScalarQueryParameter("task_id", "STRING", task.task_id),
                    bigquery.ScalarQueryParameter("try_number", "INT64", int(task.try_number or 0)),
                    bigquery.ScalarQueryParameter("type", "STRING",
                                                  str(task.type) if not isinstance(task.type, str) else task.type),
                    bigquery.ScalarQueryParameter("state", "STRING",
                                                  str(task.state) if not isinstance(task.state, str) else task.state),
                    bigquery.ScalarQueryParameter("input_json", "JSON",
                                                  json.dumps(task.input_json) if task.input_json is not None else None),
                    bigquery.ScalarQueryParameter("output_json", "JSON", json.dumps(
                        task.output_json) if task.output_json is not None else None),
                    bigquery.ScalarQueryParameter("error", "STRING", task.error),
                    bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", task.start_date),
                    bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", task.end_date),
                ]
            ),
        ).result()

    def list_runs(self) -> List[str]:
        return [r["id"] for r in
                self.client.query(f"SELECT id FROM {self.runs_table} ORDER BY start_date DESC, id DESC").result()]

    def list_children(self, parent_run_id: str) -> List[str]:
        rows = self.client.query(
            f"""
            SELECT id
            FROM {self.runs_table}
            WHERE parent_run_id = @pid
            ORDER BY start_date DESC, id DESC
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("pid", "STRING", parent_run_id)]
            ),
        ).result()
        return [r["id"] for r in rows]

    def get_all_runs(self, include_tasks: bool = True) -> List[WorkflowRun]:
        rows = list(self.client.query(
            f"SELECT id, flow_name, params_json, state, start_date, end_date, parent_run_id, parent_task_id FROM {self.runs_table} ORDER BY start_date DESC, id DESC").result())
        if not include_tasks:
            return [
                WorkflowRun(
                    id=r["id"],
                    flow_name=r["flow_name"],
                    params=_json_out(r["params_json"]) or {},
                    state=r["state"],
                    start_date=r["start_date"],
                    end_date=r["end_date"],
                    tasks={},
                    parent_run_id=r["parent_run_id"],
                    parent_task_id=r["parent_task_id"],
                )
                for r in rows
            ]
        return [self.load(r["id"]) for r in rows]
