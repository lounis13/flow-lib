"""
SQLModel models and mappers for workflow persistence.

This module contains SQLModel table definitions and mapper functions
to convert between SQLModel instances and domain dataclasses.
"""
from datetime import datetime
from typing import Optional, Dict, TypeVar
import json

from sqlmodel import Field, SQLModel

from app.infra.flow.models import TaskInstance, WorkflowRun, TaskType, ExecutionState

# Type variable for generic model update
T = TypeVar('T', bound=SQLModel)


# ============================================================
#                   SQLMODEL TABLE DEFINITIONS
# ============================================================

class WorkflowRunModel(SQLModel, table=True):
    """
    SQLModel representation of a workflow run for database persistence.

    Table: workflow_run
    """
    __tablename__ = "workflow_run"

    id: str = Field(primary_key=True)
    flow_name: str = Field(index=True)
    params_json: str = Field(default="{}")
    state: str = Field(index=True)
    start_date: Optional[datetime] = Field(default=None, index=True)
    end_date: Optional[datetime] = Field(default=None)
    parent_run_id: Optional[str] = Field(default=None, index=True)
    parent_task_id: Optional[str] = Field(default=None)


class TaskInstanceModel(SQLModel, table=True):
    """
    SQLModel representation of a task instance for database persistence.

    Table: task_instance
    Primary Key: (run_id, task_id, try_number)
    """
    __tablename__ = "task_instance"

    run_id: str = Field(foreign_key="workflow_run.id", primary_key=True)
    task_id: str = Field(primary_key=True)
    id: str = Field(default="")
    type: str = Field(default="task")
    state: str = Field(default="scheduled")
    try_number: int = Field(default=0, primary_key=True)
    input_json: Optional[str] = Field(default=None)
    output_json: Optional[str] = Field(default=None)
    error: Optional[str] = Field(default=None)
    start_date: Optional[datetime] = Field(default=None)
    end_date: Optional[datetime] = Field(default=None)


# ============================================================
#                   UTILITY FUNCTIONS
# ============================================================

def update_model_from_dict(target: T, source_dict: Dict[str, any], exclude_keys: Optional[set] = None) -> T:
    """
    Update a SQLModel instance with values from a dictionary.

    This is a generic, extensible way to update models without manually listing all fields.
    When you add a new field to a model, it will automatically be included in updates.

    Args:
        target: The SQLModel instance to update
        source_dict: Dictionary containing field names and values
        exclude_keys: Optional set of field names to skip during update

    Returns:
        The updated target model

    Example:
        existing_run = session.get(WorkflowRunModel, run_id)
        new_run = workflow_run_to_model(run)
        update_model_from_dict(existing_run, new_run.model_dump())
    """
    exclude_keys = exclude_keys or set()

    for key, value in source_dict.items():
        if key not in exclude_keys and hasattr(target, key):
            setattr(target, key, value)

    return target


def copy_model_fields(target: T, source: T, exclude_keys: Optional[set] = None) -> T:
    """
    Copy all fields from source model to target model.

    This is more type-safe and extensible than manually copying fields.
    Automatically handles all fields defined in the model.

    Args:
        target: The SQLModel instance to update
        source: The SQLModel instance to copy from
        exclude_keys: Optional set of field names to skip during copy

    Returns:
        The updated target model

    Example:
        existing_run = session.get(WorkflowRunModel, run_id)
        new_run = workflow_run_to_model(run)
        copy_model_fields(existing_run, new_run, exclude_keys={'id'})
    """
    exclude_keys = exclude_keys or set()

    # Use model_dump() to get all fields as a dictionary
    source_dict = source.model_dump(exclude=exclude_keys)

    for key, value in source_dict.items():
        if hasattr(target, key):
            setattr(target, key, value)

    return target


# ============================================================
#                   MAPPER FUNCTIONS
# ============================================================

def workflow_run_to_model(run: WorkflowRun) -> WorkflowRunModel:
    """
    Convert a WorkflowRun dataclass to a WorkflowRunModel SQLModel instance.

    Args:
        run: The WorkflowRun dataclass instance

    Returns:
        WorkflowRunModel instance ready for database persistence
    """
    return WorkflowRunModel(
        id=run.id,
        flow_name=run.flow_name,
        params_json=json.dumps(run.params or {}),
        state=str(run.state) if not isinstance(run.state, str) else run.state,
        start_date=run.start_date,
        end_date=run.end_date,
        parent_run_id=run.parent_run_id,
        parent_task_id=run.parent_task_id,
    )


def model_to_workflow_run(
    model: WorkflowRunModel,
    tasks: Optional[Dict[str, TaskInstance]] = None
) -> WorkflowRun:
    """
    Convert a WorkflowRunModel SQLModel instance to a WorkflowRun dataclass.

    Args:
        model: The WorkflowRunModel instance from the database
        tasks: Optional dictionary of task instances to include

    Returns:
        WorkflowRun dataclass instance
    """
    return WorkflowRun(
        id=model.id,
        flow_name=model.flow_name,
        params=json.loads(model.params_json or "{}"),
        state=model.state,
        start_date=model.start_date,
        end_date=model.end_date,
        tasks=tasks or {},
        parent_run_id=model.parent_run_id,
        parent_task_id=model.parent_task_id,
    )


def task_instance_to_model(task: TaskInstance, run_id: str) -> TaskInstanceModel:
    """
    Convert a TaskInstance dataclass to a TaskInstanceModel SQLModel instance.

    Args:
        task: The TaskInstance dataclass instance
        run_id: The workflow run ID this task belongs to

    Returns:
        TaskInstanceModel instance ready for database persistence
    """
    return TaskInstanceModel(
        run_id=run_id,
        id=task.id,
        task_id=task.task_id,
        type=str(task.type) if not isinstance(task.type, str) else task.type,
        state=str(task.state) if not isinstance(task.state, str) else task.state,
        try_number=int(task.try_number or 0),
        input_json=json.dumps(task.input_json) if task.input_json is not None else None,
        output_json=json.dumps(task.output_json) if task.output_json is not None else None,
        error=task.error,
        start_date=task.start_date,
        end_date=task.end_date,
    )


def model_to_task_instance(model: TaskInstanceModel) -> TaskInstance:
    """
    Convert a TaskInstanceModel SQLModel instance to a TaskInstance dataclass.

    Args:
        model: The TaskInstanceModel instance from the database

    Returns:
        TaskInstance dataclass instance
    """
    return TaskInstance(
        id=model.id,
        task_id=model.task_id,
        type=model.type,
        state=model.state,
        try_number=model.try_number,
        input_json=json.loads(model.input_json) if model.input_json else None,
        output_json=json.loads(model.output_json) if model.output_json else None,
        error=model.error,
        start_date=model.start_date,
        end_date=model.end_date,
    )
