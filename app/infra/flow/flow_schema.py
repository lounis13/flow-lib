"""
Strongly-typed dataclasses for flow representation and serialization.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, Optional, List


@dataclass
class TaskDefinition:
    """Represents the definition of a task in the flow."""
    task_id: str
    type: str
    depends_on: List[str]
    retries: int
    flow_name: Optional[str] = None  # For subflows


@dataclass
class FlowDefinition:
    """Represents the flow definition (DAG structure)."""
    name: str
    tasks: Dict[str, TaskDefinition]


@dataclass
class TaskExecution:
    """Represents the execution state of a task."""
    task_id: str
    type: str
    state: str
    try_number: int
    depends_on: List[str]
    input_json: Dict[str, Any]
    output_json: Optional[Dict[str, Any]]
    error: Optional[str]
    start_date: Optional[str]
    end_date: Optional[str]
    subflow: Optional[FlowExecution] = None


@dataclass
class FlowExecution:
    """Represents the complete execution of a flow with all its tasks."""
    id: str
    flow_name: str
    params: Dict[str, Any]
    state: str
    start_date: Optional[str]
    end_date: Optional[str]
    parent_run_id: Optional[str]
    parent_task_id: Optional[str]
    flow: FlowDefinition
    tasks: Dict[str, TaskExecution]
