# app/infra/flow/models.py
"""
Core data models and types for the async workflow engine.

This module contains all dataclasses, enums, protocols, and type definitions
used throughout the flow library.
"""
from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Dict, Generic, List, Optional, Protocol, TypeVar, Union

if TYPE_CHECKING:
    pass


# ============================================================
#                   EXECUTION STATES
# ============================================================
class ExecutionState:
    """
    Enumeration of possible execution states for workflow runs and task instances.
    """
    NONE = "none"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"

    @classmethod
    def is_terminal(cls, state: str) -> bool:
        """
        Check if the given state represents a terminal (final) status.

        Args:
            state: The state to check

        Returns:
            True if the state is terminal (SUCCESS, FAILED, or SKIPPED)
        """
        return state in {cls.SUCCESS, cls.FAILED, cls.SKIPPED}


# Alias for backward compatibility
State = ExecutionState


# ============================================================
#                   TASK TYPES
# ============================================================
class TaskType(enum.StrEnum):
    """Types of executable units in a workflow."""
    FLOW = "flow"
    TASK = "task"


# ============================================================
#                   TASK INSTANCE
# ============================================================
@dataclass
class TaskInstance:
    """
    Represents a single execution attempt of a task within a workflow run.

    Attributes:
        task_id: Unique identifier for the task
        type: Type of the task (TASK or FLOW)
        state: Current execution state
        try_number: Number of execution attempts (starts at 0)
        input_json: Input parameters for the task
        output_json: Output data produced by the task
        error: Error message if the task failed
        start_date: Timestamp when execution started
        end_date: Timestamp when execution completed
    """
    task_id: str
    type: TaskType = TaskType.TASK
    state: str = ExecutionState.SCHEDULED
    try_number: int = 0
    input_json: Dict[str, Any] = field(default_factory=dict)
    output_json: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    id: str = str(uuid.uuid4())

# ============================================================
#                   WORKFLOW RUN
# ============================================================
@dataclass
class WorkflowRun:
    """
    Represents a complete workflow execution (DAG instance).

    A workflow run contains all task instances and tracks the overall
    execution state of the workflow.

    Attributes:
        id: Unique identifier for this workflow run
        flow_name: Name of the flow being executed
        params: Input parameters for the workflow
        state: Current execution state of the workflow
        start_date: Timestamp when the workflow started
        end_date: Timestamp when the workflow completed
        tasks: Map of task_id to TaskInstance
        parent_run_id: ID of parent workflow (if this is a subflow)
        parent_task_id: ID of parent task (if this is a subflow)
    """
    id: str
    flow_name: str
    params: Dict[str, Any] = field(default_factory=dict)
    state: str = ExecutionState.SCHEDULED
    start_date: datetime = field(default_factory=datetime.utcnow)
    end_date: Optional[datetime] = None
    tasks: Dict[str, TaskInstance] = field(default_factory=dict)
    parent_run_id: Optional[str] = None
    parent_task_id: Optional[str] = None

    def get_all_tasks_recursive(self, store: Store) -> Dict[str, TaskInstance]:
        """
        Recursively collect all tasks including those in subflows.

        Subflow tasks are prefixed with their parent path: 'subflow_id.task_id'

        Args:
            store: Storage backend to load child runs

        Returns:
            Dictionary mapping full task paths to TaskInstance objects
        """
        all_tasks = {}

        for task_id, task_instance in self.tasks.items():
            all_tasks[task_id] = task_instance

            if task_instance.type == TaskType.FLOW and task_instance.output_json:
                child_run_id = task_instance.output_json.get("child_run_id")
                if child_run_id:
                    try:
                        child_run = store.load(child_run_id)
                        child_tasks = child_run.get_all_tasks_recursive(store)
                        for child_task_id, child_task in child_tasks.items():
                            prefixed_path = f"{task_id}.{child_task_id}"
                            all_tasks[prefixed_path] = child_task
                    except Exception:
                        pass  # Child run may not exist yet

        return all_tasks


# ============================================================
#                   TASK DEFINITIONS
# ============================================================
TParams = TypeVar('TParams')
TOutput = TypeVar('TOutput')


@dataclass
class TaskDefinition:
    """
    Defines the structure and behavior of a task in the workflow.

    Attributes:
        task_function: The callable that executes the task logic
        task_type: Type of task (TASK or FLOW)
        dependencies: List of task IDs that must complete before this task
        max_retries: Maximum number of retry attempts on failure
        retry_delay: Time to wait between retry attempts
        name: Human-readable name for the task (optional)
        description: Description of what the task does (optional)
    """
    task_function: Callable[[Context], Any]
    task_type: TaskType = TaskType.TASK
    dependencies: List[str] = field(default_factory=list)
    max_retries: int = 0
    retry_delay: Optional[timedelta] = None
    name: Optional[str] = None
    description: Optional[str] = None


@dataclass
class SubFlowDefinition:
    """
    Defines a subflow (nested workflow) within a parent workflow.

    Attributes:
        child_flow: The AsyncFlow instance to execute as a subflow
        dependencies: List of task IDs that must complete before this subflow
        max_retries: Maximum number of retry attempts on failure
        retry_delay: Time to wait between retry attempts
        params: Parameters to pass to the subflow
        name: Human-readable name for the subflow (optional)
        description: Description of what the subflow does (optional)
    """
    child_flow: Any  # AsyncFlow (avoiding circular import)
    dependencies: List[str] = field(default_factory=list)
    max_retries: int = 0
    retry_delay: Optional[timedelta] = None
    params: dict = field(default_factory=dict)
    name: Optional[str] = None
    description: Optional[str] = None


# ============================================================
#                   EVENT SYSTEM
# ============================================================
class EventType(Enum):
    """Types of events that can occur during workflow execution."""
    TASK_COMPLETED = "TASK_COMPLETED"
    TASK_FAILED = "TASK_FAILED"
    WORKFLOW_EXIT = "WORKFLOW_EXIT"


@dataclass(frozen=True)
class TaskEvent:
    """
    Event indicating a task state change.

    Attributes:
        event_type: Type of event
        task_id: ID of the task that triggered the event
        run_id: ID of the workflow run
    """
    event_type: EventType
    task_id: str
    run_id: str


@dataclass(frozen=True)
class WorkflowExitEvent:
    """
    Event indicating workflow completion.

    Attributes:
        event_type: Type of event (always WORKFLOW_EXIT)
        run_id: ID of the workflow run
    """
    event_type: EventType
    run_id: str


WorkflowEvent = Union[TaskEvent, WorkflowExitEvent]


# ============================================================
#                   EXECUTION CONTEXT
# ============================================================
class Context(Generic[TParams]):
    """
    Execution context provided to task functions.

    Provides access to workflow parameters, task metadata, and methods
    for inter-task communication (pull/push).

    Type Parameters:
        TParams: Type of the workflow parameters

    Attributes:
        workflow_run: The current workflow run
        task_instance: The current task instance
    """

    def __init__(self, workflow_run: WorkflowRun, task_instance: TaskInstance):
        """
        Initialize the execution context.

        Args:
            workflow_run: The current workflow run
            task_instance: The current task instance
        """
        self.workflow_run = workflow_run
        self.task_instance = task_instance
        self._pushed_value: Optional[Any] = None

    @property
    def params(self) -> TParams:
        """Get workflow input parameters."""
        return self.workflow_run.params

    @property
    def task_id(self) -> str:
        """Get current task ID."""
        return self.task_instance.task_id

    @property
    def attempt_number(self) -> int:
        """Get current attempt number (0-indexed)."""
        return self.task_instance.try_number

    def pull_output(self, upstream_task_id: str) -> Optional[Any]:
        """
        Retrieve output from an upstream task.

        Args:
            upstream_task_id: ID of the task to pull output from

        Returns:
            The output data if the task succeeded, None otherwise
        """
        upstream_task = self.workflow_run.tasks.get(upstream_task_id)
        if upstream_task and upstream_task.state == ExecutionState.SUCCESS:
            output = upstream_task.output_json
            # For subflow tasks, extract the result from the structure
            if (upstream_task.type == TaskType.FLOW and
                    isinstance(output, dict) and
                    "result" in output):
                return output["result"]
            return output
        return None

    def push_output(self, output_value: Any) -> None:
        """
        Set the output value for the current task.

        Args:
            output_value: The data to push as task output
        """
        self._pushed_value = output_value

    # Backward compatibility aliases
    def pull(self, upstream_task_id: str) -> Optional[Any]:
        """Alias for pull_output() for backward compatibility."""
        return self.pull_output(upstream_task_id)

    def push(self, output_value: Any) -> None:
        """Alias for push_output() for backward compatibility."""
        self.push_output(output_value)

    @property
    def run(self) -> WorkflowRun:
        """Alias for workflow_run for backward compatibility."""
        return self.workflow_run

    def get_pushed_value(self) -> Optional[Any]:
        """
        Internal method to retrieve the pushed value.

        Returns:
            The pushed value, if any
        """
        return self._pushed_value

    def log(self, message: str) -> None:
        """
        Log a message with task context.

        Args:
            message: The message to log
        """
        timestamp = datetime.now().isoformat(timespec="seconds")
        log_prefix = f"[{timestamp}] [{self.workflow_run.id}::{self.task_instance.task_id}#{self.task_instance.try_number}]"
        print(f"{log_prefix} {message}")


# ============================================================
#                   STORAGE INTERFACE
# ============================================================
class Store(Protocol):
    """
    Protocol defining the storage interface for workflow persistence.

    Implementations include SQLStore, JsonStore, and MemoryStore.
    """

    def open(self) -> None:
        """Initialize the storage backend."""
        ...

    def close(self) -> None:
        """Close the storage backend and release resources."""
        ...

    def save(self, workflow_run: WorkflowRun) -> None:
        """
        Persist a workflow run to storage.

        Args:
            workflow_run: The workflow run to save
        """
        ...

    def load(self, run_id: str) -> WorkflowRun:
        """
        Load a workflow run from storage.

        Args:
            run_id: ID of the workflow run to load

        Returns:
            The loaded workflow run

        Raises:
            KeyError: If the run_id does not exist
        """
        ...

    def exists(self, run_id: str) -> bool:
        """
        Check if a workflow run exists in storage.

        Args:
            run_id: ID of the workflow run to check

        Returns:
            True if the run exists
        """
        ...

    def update_task(self, run_id: str, task_instance: TaskInstance) -> None:
        """
        Update a single task instance within a workflow run.

        This is more efficient than saving the entire workflow run
        when only one task has changed.

        Args:
            run_id: ID of the workflow run
            task_instance: The task instance to update

        Raises:
            KeyError: If the run_id or task_id does not exist
        """
        ...



    def get_all_runs(self, include_tasks: bool = True) -> List[WorkflowRun]:
        """
        get all workflow runs in storage.
        :param include_tasks:
        :return:
        """


# ============================================================
#                   SERIALIZATION MODELS
# ============================================================
@dataclass
class TaskDefinitionSchema:
    """
    Schema for serializing task definitions.

    Attributes:
        task_id: Unique identifier for the task
        task_type: Type of task
        dependencies: List of task IDs this task depends on
        max_retries: Maximum retry attempts
        flow_name: Name of the flow (for subflow tasks only)
        name: Human-readable name for the task
        description: Description of what the task does
    """
    task_id: str
    task_type: str
    dependencies: List[str]
    max_retries: int
    flow_name: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None


@dataclass
class FlowDefinitionSchema:
    """
    Schema for serializing flow definitions.

    Attributes:
        flow_name: Name of the flow
        tasks: Map of task_id to task definition
    """
    flow_name: str
    tasks: Dict[str, TaskDefinitionSchema]


@dataclass
class TaskExecutionSchema:
    """
    Schema for serializing task execution state.

    Attributes:
        task_id: Unique identifier for the task
        task_type: Type of task
        state: Current execution state
        attempt_number: Current attempt number
        dependencies: List of task IDs this task depends on
        input_data: Input parameters for the task
        output_data: Output data produced by the task
        error_message: Error message if failed
        start_timestamp: ISO timestamp of start
        end_timestamp: ISO timestamp of completion
        subflow_execution: Nested subflow execution data (if applicable)
    """
    id: str
    task_id: str
    task_type: str
    state: str
    attempt_number: int
    dependencies: List[str]
    input_data: Dict[str, Any]
    output_data: Optional[Dict[str, Any]]
    error_message: Optional[str]
    start_timestamp: Optional[str]
    end_timestamp: Optional[str]
    subflow_execution: Optional['FlowExecutionSchema'] = None


@dataclass
class FlowExecutionSchema:
    """
    Schema for serializing complete flow execution state.

    Attributes:
        run_id: Unique identifier for the workflow run
        flow_name: Name of the flow
        params: Input parameters for the workflow
        state: Current execution state
        start_timestamp: ISO timestamp of start
        end_timestamp: ISO timestamp of completion
        parent_run_id: ID of parent workflow (if subflow)
        parent_task_id: ID of parent task (if subflow)
        flow_definition: Definition of the flow structure
        task_executions: Map of task_id to task execution state
    """
    run_id: str
    flow_name: str
    params: Dict[str, Any]
    state: str
    start_timestamp: Optional[str]
    end_timestamp: Optional[str]
    parent_run_id: Optional[str]
    parent_task_id: Optional[str]
    flow_definition: FlowDefinitionSchema
    task_executions: Dict[str, TaskExecutionSchema]


# ============================================================
#                   FLAT EXECUTION SCHEMAS
# ============================================================

@dataclass
class FlatTaskSchema:
    """
    Flattened task representation for UI consumption.

    All tasks (including those in subflows) are flattened to a single level
    with their full path as task_id.
    """
    id: str
    task_id: str
    task_type: str
    state: str
    attempt_number: int
    name: Optional[str] = None  # Human-readable name
    description: Optional[str] = None  # Description of what the task does
    input_data: Optional[Dict[str, Any]] = None
    output_data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    start_timestamp: Optional[str] = None
    end_timestamp: Optional[str] = None
    duration_ms: Optional[float] = None  # Duration in milliseconds


@dataclass
class TaskDependencySchema:
    """
    Represents a dependency between two tasks.

    Attributes:
        source: Full task_id path of source task (e.g., "ftb_flow.task1")
        target: Full task_id path of target task (e.g., "ftb_flow.task2")
        source_id: Real instance ID (UUID) of the source task
        target_id: Real instance ID (UUID) of the target task
        source_output: Output data of source task
        target_input: Input data of target task
    """
    source: str  # task_id of source task
    target: str  # task_id of target task
    source_id: Optional[str] = None  # Real instance ID of source task
    target_id: Optional[str] = None  # Real instance ID of target task
    source_output: Optional[Any] = None  # Output of source task
    target_input: Optional[Any] = None  # Input of target task


@dataclass
class FlatExecutionSchema:
    """
    Flattened execution schema optimized for UI display.

    All tasks are flattened (subflows included) and sorted by start_timestamp.
    Dependencies are explicitly listed as source->target pairs.
    """
    run_id: str
    flow_name: str
    params: Dict[str, Any]
    state: str
    start_timestamp: Optional[str] = None
    end_timestamp: Optional[str] = None
    duration_ms: Optional[float] = None
    parent_run_id: Optional[str] = None
    parent_task_id: Optional[str] = None

    # Flattened tasks sorted by start_timestamp
    tasks: List['FlatTaskSchema'] = field(default_factory=list)

    # All dependencies as source -> target pairs
    dependencies: List['TaskDependencySchema'] = field(default_factory=list)

    # Statistics
    total_tasks: int = 0
    successful_tasks: int = 0
    failed_tasks: int = 0
    running_tasks: int = 0


# ============================================================
#                   JOB LIST SCHEMAS
# ============================================================

@dataclass
class JobSummarySchema:
    """
    Summary information for a single job in the list view.

    Provides essential information and statistics without loading full task details.
    """
    run_id: str
    flow_name: str
    state: str
    start_timestamp: Optional[str] = None
    end_timestamp: Optional[str] = None
    duration_ms: Optional[float] = None
    parent_run_id: Optional[str] = None

    # Quick stats
    total_tasks: int = 0
    successful_tasks: int = 0
    failed_tasks: int = 0
    running_tasks: int = 0
    pending_tasks: int = 0


@dataclass
class JobListStatsSchema:
    """
    Aggregated statistics across all jobs.
    """
    total_jobs: int = 0
    running_jobs: int = 0
    successful_jobs: int = 0
    failed_jobs: int = 0
    pending_jobs: int = 0

    total_tasks_across_all_jobs: int = 0
    total_duration_ms: Optional[float] = None
    average_duration_ms: Optional[float] = None


@dataclass
class JobListResponseSchema:
    """
    Response schema for listing all jobs with aggregated statistics.
    """
    jobs: List[JobSummarySchema] = field(default_factory=list)
    stats: Optional[JobListStatsSchema] = None
    total_count: int = 0
