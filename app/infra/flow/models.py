# app/infra/flow/models.py
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional, List, Protocol


# ============================================================
#                   TASK / RUN STATES
# ============================================================
class State:
    NONE = "none"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"

    @classmethod
    def is_final(cls, state: str) -> bool:
        """Return True if the state represents a terminal status."""
        return state in {cls.SUCCESS, cls.FAILED, cls.SKIPPED}


# ============================================================
#                   TASK INSTANCE
# ============================================================
class TaskType(enum.StrEnum):
    FLOW = "flow"
    TASK = "task"


@dataclass
class TaskInstance:
    """
    Represents one execution of a task inside a workflow run.
    """
    task_id: str
    type: TaskType = TaskType.TASK
    state: str = State.SCHEDULED
    try_number: int = 0
    input_json: Dict[str, Any] = field(default_factory=dict)
    output_json: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


# ============================================================
#                   WORKFLOW RUN
# ============================================================
@dataclass
class WorkflowRun:
    """
    Represents a full workflow execution (DAG instance).
    """
    id: str
    flow_name: str
    params: Dict[str, Any] = field(default_factory=dict)
    state: str = State.SCHEDULED
    start_date: datetime = field(default_factory=datetime.utcnow)
    end_date: Optional[datetime] = None
    tasks: Dict[str, TaskInstance] = field(default_factory=dict)

    # parent flow links (used for subflows)
    parent_run_id: Optional[str] = None
    parent_task_id: Optional[str] = None

    def get_all_tasks_recursive(self, store: Store) -> Dict[str, TaskInstance]:
        """
        Retourne toutes les tâches incluant celles des subflows de manière récursive.
        Les clés sont préfixées par le chemin: 'subflow_id.task_id' pour les tâches de subflows.
        """
        all_tasks = {}

        # Ajouter les tâches du flow courant
        for task_id, task in self.tasks.items():
            all_tasks[task_id] = task

            # Si c'est un subflow (type FLOW), récupérer ses tâches
            if task.type == TaskType.FLOW and task.output_json:
                child_run_id = task.output_json.get("child_run_id")
                if child_run_id:
                    try:
                        child_run = store.load(child_run_id)
                        # Récursivement obtenir toutes les tâches du subflow
                        child_tasks = child_run.get_all_tasks_recursive(store)
                        for child_task_id, child_task in child_tasks.items():
                            # Préfixer avec le nom du subflow
                            prefixed_key = f"{task_id}.{child_task_id}"
                            all_tasks[prefixed_key] = child_task
                    except Exception:
                        pass  # Le child run n'existe peut-être pas encore

        return all_tasks


# ============================================================
#                   STORE INTERFACE
# ============================================================
class Store(Protocol):
    """
    Abstract storage interface.
    Concrete implementations: SQLiteStore, JsonStore, MemoryStore.
    """

    def open(self) -> None: ...

    def close(self) -> None: ...

    def save(self, run: WorkflowRun) -> None: ...

    def load(self, run_id: str) -> WorkflowRun: ...

    def exists(self, run_id: str) -> bool: ...

    def list_runs(self) -> List[str]: ...

    def list_children(self, parent_run_id: str) -> List[str]: ...
