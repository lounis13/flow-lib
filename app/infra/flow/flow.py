# app/infra/flow/flow.py
"""
Async workflow orchestration engine.

This module provides a declarative, type-safe workflow engine for building
and executing directed acyclic graphs (DAGs) of tasks with support for:
- Task dependencies and parallel execution
- Nested subflows
- Automatic and manual retries
- State persistence
- Event-driven execution
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from app.infra.flow.event_manager import (
    DependencyResolver,
    EventManager,
    WorkflowEventHandler,
)
from app.infra.flow.executors import StandardTaskExecutor, SubFlowExecutor
from app.infra.flow.models import (
    Context,
    EventType,
    ExecutionState,
    Store,
    SubFlowDefinition,
    TaskDefinition,
    TaskDefinitionSchema,
    TaskExecutionSchema,
    TaskInstance,
    TaskType,
    WorkflowExitEvent,
    WorkflowRun,
    FlowDefinitionSchema,
    FlowExecutionSchema,
)
from app.infra.flow.retry_manager import RetryManager


class AsyncFlow:
    """
    Declarative async workflow orchestration engine.

    Provides a fluent API for defining and executing workflows with:
    - Task registration via decorators
    - Dependency management
    - Retry policies
    - Subflow composition
    - Concurrent execution with configurable limits

    Example:
        ```python
        flow = AsyncFlow("my_workflow", store)

        @flow.task("task_1")
        def task_1(ctx: Context):
            return {"result": "data"}

        @flow.task("task_2", depends_on=["task_1"])
        def task_2(ctx: Context):
            data = ctx.pull_output("task_1")
            return {"processed": data}

        run_id = flow.start_run(params={"input": "value"})
        await flow.run_until_complete(run_id)
        ```
    """

    def __init__(self, flow_name: str, store: Optional[Store] = None):
        """
        Initialize a new workflow.

        Args:
            flow_name: Unique name for this workflow
            store: Storage backend for persisting execution state
        """
        self.flow_name = flow_name
        self._store = store

        # Task and subflow registries
        self._task_definitions: Dict[str, TaskDefinition] = {}
        self._subflow_definitions: Dict[str, SubFlowDefinition] = {}

        # Execution management
        self._event_manager = EventManager()
        self._run_locks: Dict[str, asyncio.Lock] = {}
        self._concurrency_semaphore: Optional[asyncio.Semaphore] = None

        # Dependency and retry managers (lazy init)
        self._dependency_resolver: Optional[DependencyResolver] = None
        self._retry_manager: Optional[RetryManager] = None

    # ================================================================
    #                   TASK REGISTRATION (Public API)
    # ================================================================

    def add_task_definition(
            self,
            task_id: str,
            task_function: Callable[[Context], Any],
            *,
            task_type: TaskType = TaskType.TASK,
            dependencies: Optional[List[str]] = None,
            max_retries: int = 0,
            retry_delay=None,
    ) -> AsyncFlow:
        """
        Register a task definition.

        Unified public method for task registration, used by both
        decorators and workflow builders.

        Args:
            task_id: Unique identifier for the task
            task_function: The callable that executes the task logic
            task_type: Type of task (default: TASK)
            dependencies: List of task IDs that must complete before this task
            max_retries: Maximum number of retry attempts on failure
            retry_delay: Time to wait between retries (timedelta)

        Returns:
            Self for method chaining
        """
        self._task_definitions[task_id] = TaskDefinition(
            task_function=task_function,
            task_type=task_type,
            dependencies=dependencies or [],
            max_retries=max_retries,
            retry_delay=retry_delay,
        )
        return self

    def add_subflow_definition(
            self,
            subflow_id: str,
            child_flow: AsyncFlow,
            *,
            dependencies: Optional[List[str]] = None,
            max_retries: int = 0,
            retry_delay=None,
            params: dict = None,
    ) -> AsyncFlow:
        """
        Register a subflow definition.

        Unified public method for subflow registration, used by both
        decorators and workflow builders.

        Args:
            subflow_id: Unique identifier for the subflow
            child_flow: The AsyncFlow instance to execute as a subflow
            dependencies: List of task IDs that must complete before this subflow
            max_retries: Maximum number of retry attempts on failure
            retry_delay: Time to wait between retries (timedelta)
            params: Input parameters for the subflow

        Returns:
            Self for method chaining
        """
        self._subflow_definitions[subflow_id] = SubFlowDefinition(
            child_flow=child_flow,
            dependencies=dependencies or [],
            max_retries=max_retries,
            retry_delay=retry_delay,
            params=params
        )
        return self

    def task(
            self,
            task_id: str,
            *,
            task_type: TaskType = TaskType.TASK,
            depends_on: Optional[List[str]] = None,
            max_retries: int = 0,
            retry_delay=None,
    ) -> Callable:
        """
        Decorator to register a task in the workflow.

        Args:
            task_id: Unique identifier for the task
            task_type: Type of task (default: TASK)
            depends_on: List of task IDs that must complete before this task
            max_retries: Maximum number of retry attempts on failure
            retry_delay: Time to wait between retries (timedelta)

        Returns:
            Decorator function

        Example:
            ```python
            @flow.task("extract_data", depends_on=["validate"])
            def extract(ctx: Context):
                ctx.log("Extracting data...")
                return {"data": [1, 2, 3]}
            ```
        """

        def decorator(task_function: Callable[[Context], Any]):
            self.add_task_definition(
                task_id=task_id,
                task_function=task_function,
                task_type=task_type,
                dependencies=depends_on,
                max_retries=max_retries,
                retry_delay=retry_delay,
            )
            return task_function

        return decorator

    def subflow(
            self,
            subflow_id: str,
            *,
            depends_on: Optional[List[str]] = None,
            max_retries: int = 0,
            retry_delay=None,
            params: dict = None,
    ) -> Callable:
        """
        Decorator to register a subflow (nested workflow).

        Args:
            subflow_id: Unique identifier for the subflow
            depends_on: List of task IDs that must complete before this subflow
            max_retries: Maximum number of retry attempts on failure
            retry_delay: Time to wait between retries (timedelta)

        Returns:
            Decorator function

        Example:
            ```python
            @flow.subflow("process_batch", depends_on=["extract"])
            def get_batch_flow(ctx: Context):
                return batch_processing_flow
            ```
            :param params:
        """

        def decorator(child_flow_fn: Callable):
            # Call the function to get the actual AsyncFlow instance
            actual_flow = child_flow_fn(None) if callable(child_flow_fn) else child_flow_fn
            self.add_subflow_definition(
                subflow_id=subflow_id,
                child_flow=actual_flow,
                dependencies=depends_on,
                max_retries=max_retries,
                retry_delay=retry_delay,
                params=params,
            )
            return child_flow_fn

        return decorator

    def has_task(self, task_id: str) -> bool:
        """
        Check if a task exists in the workflow.

        Args:
            task_id: ID of the task to check

        Returns:
            True if the task exists
        """
        return task_id in self._task_definitions

    def update_task_retry_policy(
            self,
            task_id: str,
            max_retries: int,
            retry_delay=None,
    ) -> AsyncFlow:
        """
        Update the retry policy for an existing task.

        This is a public method intended for workflow builder patterns.

        Args:
            task_id: ID of the task to update
            max_retries: Maximum number of retry attempts
            retry_delay: Time to wait between retries (timedelta)

        Returns:
            Self for method chaining

        Raises:
            KeyError: If task_id doesn't exist
        """
        if task_id not in self._task_definitions:
            raise KeyError(f"Task '{task_id}' not found in workflow")

        task_def = self._task_definitions[task_id]
        task_def.max_retries = max_retries
        task_def.retry_delay = retry_delay
        return self

    # ================================================================
    #                   WORKFLOW LIFECYCLE
    # ================================================================

    def init_run(
            self,
            params: Optional[Dict[str, Any]] = None,
            parent_run_id: Optional[str] = None,
            parent_task_id: Optional[str] = None,
            run_id: Optional[str] = None,
    ) -> str:
        """
        Initialize a new workflow run.

        Creates task instances for all registered tasks and subflows,
        and recursively initializes child runs for subflows.

        Args:
            params: Input parameters for the workflow
            parent_run_id: ID of parent workflow (if this is a subflow)
            parent_task_id: ID of parent task (if this is a subflow)
            run_id: Optional run ID to reuse (for retries)

        Returns:
            The run ID of the created workflow run
        """
        # Check if run exists and update it
        if run_id and self._store.exists(run_id):
            workflow_run = self._store.load(run_id)
            workflow_run.params = params or {}
            workflow_run.parent_run_id = parent_run_id
            workflow_run.parent_task_id = parent_task_id
        else:
            # Create new run
            run_id = run_id or str(uuid.uuid4())
            workflow_run = WorkflowRun(
                id=run_id,
                flow_name=self.flow_name,
                params=params or {},
                parent_run_id=parent_run_id,
                parent_task_id=parent_task_id,
            )

            # Create task instances for standard tasks
            for task_id in self._task_definitions:
                workflow_run.tasks[task_id] = TaskInstance(
                    task_id=task_id,
                    type=self._task_definitions[task_id].task_type,
                )

            # Create task instances for subflows
            for subflow_id in self._subflow_definitions:
                workflow_run.tasks[subflow_id] = TaskInstance(
                    task_id=subflow_id,
                    type=TaskType.FLOW,
                    input_json=self._subflow_definitions[subflow_id].params or {},
                )

        self._store.save(workflow_run)

        # Recursively initialize subflow runs
        for subflow_id, subflow_def in self._subflow_definitions.items():
            child_flow = subflow_def.child_flow
            child_flow._store = self._store

            # Check if child run already exists
            existing_child_run_id = None
            if subflow_id in workflow_run.tasks and workflow_run.tasks[subflow_id].output_json:
                existing_child_run_id = workflow_run.tasks[subflow_id].output_json.get(
                    "child_run_id"
                )

            # Initialize child run
            subflow_params = subflow_def.params or {}
            child_run_id = child_flow.init_run(
                params={**params, **subflow_params} if params else subflow_params,
                parent_run_id=workflow_run.id,
                parent_task_id=subflow_id,
                run_id=existing_child_run_id,
            )

            # Store child_run_id in subflow task instance
            workflow_run.tasks[subflow_id].output_json = {"child_run_id": child_run_id}

        self._store.save(workflow_run)
        return run_id

    async def run_until_complete(
            self, run_id: str, max_concurrency: int = 0
    ) -> None:
        """
        Execute the workflow until all tasks complete.

        Processes events and launches tasks as their dependencies are satisfied.
        Blocks until the workflow reaches a terminal state.

        Args:
            run_id: ID of the workflow run to execute
            max_concurrency: Maximum number of concurrent tasks (0 = unlimited)
        """
        workflow_run = self._store.load(run_id)

        # Mark workflow as running
        async with self._get_run_lock(run_id):
            workflow_run.state = ExecutionState.RUNNING
            self._store.save(workflow_run)

        # Initialize managers
        self._initialize_managers()

        # Set up concurrency limiting
        if max_concurrency > 0:
            self._concurrency_semaphore = asyncio.Semaphore(max_concurrency)

        # Get event queue
        event_queue = self._event_manager.get_event_queue(run_id)

        # Launch initially ready tasks
        for task_id in self._dependency_resolver.get_ready_tasks(workflow_run):
            asyncio.create_task(self._execute_task(workflow_run, task_id))

        for subflow_id in self._dependency_resolver.get_ready_subflows(workflow_run):
            asyncio.create_task(self._execute_subflow(workflow_run, subflow_id))

        # Event processing loop
        try:
            while True:
                event = await event_queue.get()

                if event.event_type == EventType.TASK_COMPLETED:
                    await self._handle_task_completion(workflow_run, event.task_id)
                elif event.event_type == EventType.TASK_FAILED:
                    await self._handle_task_failure(workflow_run, event.task_id)
                elif event.event_type == EventType.WORKFLOW_EXIT:
                    break

                # Check if workflow is complete
                # Reload to get fresh state of all tasks
                workflow_run_check = self._store.load(run_id)
                event_handler = self._create_event_handler()
                if event_handler.is_workflow_complete(workflow_run_check):
                    async with self._get_run_lock(run_id):
                        # Use the checked instance for final state
                        workflow_run = workflow_run_check
                        workflow_run.state = event_handler.compute_workflow_final_state(
                            workflow_run
                        )
                        workflow_run.end_date = datetime.now()
                        self._store.save(workflow_run)
                    await self._event_manager.emit_event(
                        WorkflowExitEvent(
                            event_type=EventType.WORKFLOW_EXIT, run_id=run_id
                        )
                    )
        finally:
            # Clean up resources
            self._event_manager.cleanup_run_resources(run_id)
            self._run_locks.pop(run_id, None)

    # ================================================================
    #                   TASK EXECUTION
    # ================================================================

    async def _execute_task(self, workflow_run: WorkflowRun, task_id: str) -> None:
        """
        Execute a standard task.

        Args:
            workflow_run: The workflow run containing the task
            task_id: ID of the task to execute
        """
        task_def = self._task_definitions[task_id]
        executor = StandardTaskExecutor(
            task_definition=task_def,
            store=self._store,
            concurrency_semaphore=self._concurrency_semaphore,
        )
        await executor.execute(
            workflow_run=workflow_run,
            task_id=task_id,
            run_lock=self._get_run_lock(workflow_run.id),
            event_emitter=self._event_manager.emit_event,
        )

    async def _execute_subflow(
            self, workflow_run: WorkflowRun, subflow_id: str
    ) -> None:
        """
        Execute a subflow.

        Args:
            workflow_run: The workflow run containing the subflow
            subflow_id: ID of the subflow to execute
        """
        subflow_def = self._subflow_definitions[subflow_id]
        executor = SubFlowExecutor(
            subflow_definition=subflow_def,
            store=self._store,
            concurrency_semaphore=self._concurrency_semaphore,
        )
        await executor.execute(
            workflow_run=workflow_run,
            task_id=subflow_id,
            run_lock=self._get_run_lock(workflow_run.id),
            event_emitter=self._event_manager.emit_event,
        )

    # ================================================================
    #                   EVENT HANDLING
    # ================================================================

    async def _handle_task_completion(
            self, workflow_run: WorkflowRun, task_id: str
    ) -> None:
        """
        Handle task completion event.

        Args:
            workflow_run: The workflow run (shared instance)
            task_id: ID of the completed task
        """
        event_handler = self._create_event_handler()
        await event_handler.handle_task_completion(workflow_run, task_id)

    async def _handle_task_failure(
            self, workflow_run: WorkflowRun, task_id: str
    ) -> None:
        """
        Handle task failure event.

        Args:
            workflow_run: The workflow run (shared instance)
            task_id: ID of the failed task
        """
        event_handler = self._create_event_handler()
        await event_handler.handle_task_failure(
            workflow_run, task_id, self._get_run_lock(workflow_run.id)
        )

    # ================================================================
    #                   RETRY MANAGEMENT
    # ================================================================

    def manual_retry(
            self, run_id: str, task_id: str, reset_downstream: bool = False
    ) -> None:
        """
        Manually retry a failed task.

        Args:
            run_id: ID of the workflow run
            task_id: ID of the task to retry
            reset_downstream: Whether to also reset downstream tasks
        """
        self._initialize_managers()
        self._retry_manager.manual_retry(run_id, task_id, reset_downstream)

    async def retry(
            self,
            run_id: str,
            task_id: str,
            reset_downstream: bool = True,
            max_concurrency: int = 0,
    ) -> None:
        """
        Retry a failed task and resume workflow execution.

        Args:
            run_id: ID of the workflow run
            task_id: ID of the task to retry
            reset_downstream: Whether to also reset downstream tasks
            max_concurrency: Maximum number of concurrent tasks
        """
        self.manual_retry(run_id, task_id, reset_downstream=reset_downstream)
        await self.run_until_complete(run_id, max_concurrency=max_concurrency)

    # ================================================================
    #                   SERIALIZATION
    # ================================================================

    def get_execution_details(self, run_id: str) -> FlowExecutionSchema:
        """
        Get complete execution details with strong typing.

        Returns a serializable representation of the workflow execution
        including all task states and subflow details.

        Args:
            run_id: ID of the workflow run

        Returns:
            Structured execution details
        """
        workflow_run = self._store.load(run_id)
        return self._build_execution_schema(workflow_run, self)

    def _build_execution_schema(
            self, workflow_run: WorkflowRun, flow_def: AsyncFlow
    ) -> FlowExecutionSchema:
        """
        Recursively build execution schema for a workflow run.

        Args:
            workflow_run: The workflow run to serialize
            flow_def: The flow definition

        Returns:
            Complete execution schema
        """
        # Build flow definition schema
        task_def_schemas = {}
        for task_id, task_def in flow_def._task_definitions.items():
            task_def_schemas[task_id] = TaskDefinitionSchema(
                task_id=task_id,
                task_type=task_def.task_type,
                dependencies=task_def.dependencies,
                max_retries=task_def.max_retries,
            )

        for subflow_id, subflow_def in flow_def._subflow_definitions.items():
            task_def_schemas[subflow_id] = TaskDefinitionSchema(
                task_id=subflow_id,
                task_type="flow",
                dependencies=subflow_def.dependencies,
                max_retries=subflow_def.max_retries,
                flow_name=subflow_def.child_flow.flow_name,
            )

        flow_definition = FlowDefinitionSchema(
            flow_name=flow_def.flow_name,
            tasks=task_def_schemas,
        )

        # Build task execution schemas
        task_exec_schemas = {}
        for task_id, task_instance in workflow_run.tasks.items():
            # Get dependencies
            dependencies = []
            if task_id in flow_def._task_definitions:
                dependencies = flow_def._task_definitions[task_id].dependencies
            elif task_id in flow_def._subflow_definitions:
                dependencies = flow_def._subflow_definitions[task_id].dependencies

            task_exec_schema = TaskExecutionSchema(
                id=task_instance.id,
                task_id=task_instance.task_id,
                task_type=task_instance.type,
                state=task_instance.state,
                attempt_number=task_instance.try_number,
                dependencies=dependencies,
                input_data=task_instance.input_json,
                output_data=task_instance.output_json,
                error_message=task_instance.error,
                start_timestamp=task_instance.start_date.isoformat()
                if task_instance.start_date
                else None,
                end_timestamp=task_instance.end_date.isoformat()
                if task_instance.end_date
                else None,
            )

            # Recursively add subflow execution
            if (
                    task_instance.type == "flow"
                    and task_instance.output_json
                    and "child_run_id" in task_instance.output_json
            ):
                child_run_id = task_instance.output_json["child_run_id"]
                try:
                    child_run = self._store.load(child_run_id)
                    child_flow_def = flow_def._subflow_definitions[task_id].child_flow
                    task_exec_schema.subflow_execution = self._build_execution_schema(
                        child_run, child_flow_def
                    )
                except Exception:
                    task_exec_schema.subflow_execution = None

            task_exec_schemas[task_id] = task_exec_schema

        return FlowExecutionSchema(
            run_id=workflow_run.id,
            flow_name=workflow_run.flow_name,
            params=workflow_run.params,
            state=workflow_run.state,
            start_timestamp=workflow_run.start_date.isoformat()
            if workflow_run.start_date
            else None,
            end_timestamp=workflow_run.end_date.isoformat()
            if workflow_run.end_date
            else None,
            parent_run_id=workflow_run.parent_run_id,
            parent_task_id=workflow_run.parent_task_id,
            flow_definition=flow_definition,
            task_executions=task_exec_schemas,
        )

    # ================================================================
    #                   INTERNAL HELPERS
    # ================================================================

    def _get_run_lock(self, run_id: str) -> asyncio.Lock:
        """Get or create a lock for a workflow run."""
        if run_id not in self._run_locks:
            self._run_locks[run_id] = asyncio.Lock()
        return self._run_locks[run_id]

    def _initialize_managers(self) -> None:
        """Initialize dependency resolver and retry manager if not already done."""
        if not self._dependency_resolver:
            self._dependency_resolver = DependencyResolver(
                self._task_definitions, self._subflow_definitions
            )

        if not self._retry_manager:
            self._retry_manager = RetryManager(
                store=self._store,
                task_definitions=self._task_definitions,
                subflow_definitions=self._subflow_definitions,
                downstream_resolver=self._dependency_resolver,
            )

    def _create_event_handler(self) -> WorkflowEventHandler:
        """Create an event handler for workflow coordination."""
        return WorkflowEventHandler(
            dependency_resolver=self._dependency_resolver,
            store=self._store,
            task_launcher=lambda run, tid: asyncio.create_task(
                self._execute_task(run, tid)
                if tid in self._task_definitions
                else self._execute_subflow(run, tid)
            ),
        )
