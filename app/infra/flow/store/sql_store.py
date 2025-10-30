"""
SQLStore implementation using SQLModel for workflow persistence.

This module provides a persistent storage backend using SQLModel/SQLAlchemy
for storing workflow runs and task instances.
"""
from datetime import datetime
from typing import List, Optional, cast

from sqlalchemy import Engine, desc as sqlalchemy_desc
from sqlmodel import Session, SQLModel, create_engine, select, col

from app.infra.flow.models import Store, WorkflowRun, TaskInstance
from app.infra.flow.store.sql_models import (
    WorkflowRunModel,
    TaskInstanceModel,
    workflow_run_to_model,
    model_to_workflow_run,
    task_instance_to_model,
    model_to_task_instance,
    copy_model_fields,
)


class SQLStore(Store):

    def __init__(self, connection_string: str = "sqlite:///runs.db", echo: bool = False):
        self.connection_string = connection_string
        self.echo = echo
        self.engine: Optional[Engine] = None

    # ---------- Lifecycle ----------

    def open(self):
        self.engine = create_engine(
            self.connection_string,
            echo=self.echo,
            connect_args={"check_same_thread": False} if "sqlite" in self.connection_string else {}
        )
        SQLModel.metadata.create_all(self.engine)

    def close(self):
        """Close the database engine and release resources."""
        if self.engine:
            self.engine.dispose()
            self.engine = None

    # ---------- Run CRUD ----------

    def save(self, run: WorkflowRun):
        if not self.engine:
            raise RuntimeError("open() must be called before save()")

        with Session(self.engine) as session:
            # Upsert workflow run
            run_model = workflow_run_to_model(run)
            existing_run = session.get(WorkflowRunModel, run.id)

            if existing_run:
                # Update existing run - automatically copies all fields from run_model
                # No need to manually list fields - extensible!
                copy_model_fields(existing_run, run_model, exclude_keys={'id'})
            else:
                # Insert a new run
                session.add(run_model)

            # Replace all tasks for this run
            statement = select(TaskInstanceModel).where(TaskInstanceModel.run_id == run.id)
            existing_tasks = session.exec(statement).all()
            for task in existing_tasks:
                session.delete(task)

            # Insert current tasks
            for task in (run.tasks or {}).values():
                task_model = task_instance_to_model(task, run.id)
                session.add(task_model)

            session.commit()

    def load(self, run_id: str) -> WorkflowRun:
        if not self.engine:
            raise RuntimeError("open() must be called before load()")

        with Session(self.engine) as session:
            # Load workflow run
            run_model = session.get(WorkflowRunModel, run_id)
            if not run_model:
                raise KeyError(f"WorkflowRun not found: {run_id}")

            # Type narrowing: after the check, run_model is guaranteed to be WorkflowRunModel

            # Load all task instances for this run
            statement = (
                select(TaskInstanceModel)
                .where(TaskInstanceModel.run_id == run_id)
                .order_by(col(TaskInstanceModel.task_id), col(TaskInstanceModel.try_number))
            )
            task_models = session.exec(statement).all()

            # Keep only the latest attempt per task_id
            latest_by_task_id = {}
            for task_model in task_models:
                task_instance = model_to_task_instance(task_model)
                latest_by_task_id[task_instance.task_id] = task_instance

            return model_to_workflow_run(run_model, latest_by_task_id)

    def exists(self, run_id: str) -> bool:
        """
        Check if a workflow run exists.

        Args:
            run_id: The workflow run ID to check

        Returns:
            True if the run exists
        """
        if not self.engine:
            raise RuntimeError("open() must be called before exists()")

        with Session(self.engine) as session:
            run_model = session.get(WorkflowRunModel, run_id)
            return run_model is not None

    def mark_run_state(
            self,
            run_id: str,
            state: str,
            end_date: Optional[datetime] = None,
    ):
        """
        Met à jour l'état global du run (et éventuellement end_date).

        Args:
            run_id: The workflow run ID
            state: The new state
            end_date: Optional end date
        """
        if not self.engine:
            raise RuntimeError("open() must be called before mark_run_state()")

        with Session(self.engine) as session:
            run_model = session.get(WorkflowRunModel, run_id)
            if not run_model:
                raise KeyError(f"WorkflowRun not found: {run_id}")

            # Type narrowing: after the check, run_model is guaranteed to be WorkflowRunModel
            run_model = cast(WorkflowRunModel, run_model)

            run_model.state = str(state) if not isinstance(state, str) else state
            if end_date:
                run_model.end_date = end_date

            session.add(run_model)
            session.commit()

    def update_task(self, run_id: str, task: TaskInstance):
        """
        Upsert d'une tentative de tâche.

        Args:
            run_id: The workflow run ID
            task: The TaskInstance to update
        """
        if not self.engine:
            raise RuntimeError("open() must be called before update_task()")

        with Session(self.engine) as session:
            # Check if a task instance already exists
            statement = (
                select(TaskInstanceModel)
                .where(TaskInstanceModel.run_id == run_id)
                .where(TaskInstanceModel.task_id == task.task_id)
                .where(TaskInstanceModel.try_number == task.try_number)
            )
            existing_task = session.exec(statement).first()

            task_model = task_instance_to_model(task, run_id)

            if existing_task:
                # Update existing task - automatically copies all fields from task_model
                # No need to manually list fields - extensible!
                copy_model_fields(existing_task, task_model, exclude_keys={'run_id', 'task_id', 'try_number'})
            else:
                # Insert new task
                session.add(task_model)

            session.commit()

    def get_all_runs(self, include_tasks: bool = True) -> List[WorkflowRun]:
        """
        Get all workflow runs.

        Args:
            include_tasks: Whether to load task instances for each run

        Returns:
            List of WorkflowRun instances
        """
        if not self.engine:
            raise RuntimeError("open() must be called before get_all_runs()")

        with Session(self.engine) as session:
            statement = (
                select(WorkflowRunModel)
                .order_by(sqlalchemy_desc(col(WorkflowRunModel.start_date)), sqlalchemy_desc(col(WorkflowRunModel.id)))
            )
            run_models = session.exec(statement).all()

            runs = []
            for run_model in run_models:
                if include_tasks:
                    runs.append(self.load(run_model.id))
                else:
                    runs.append(model_to_workflow_run(run_model, {}))

            return runs
