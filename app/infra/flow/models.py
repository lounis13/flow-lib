from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, Any, Optional, Callable, TypeVar, Generic


class State:
    NONE = "none"; SCHEDULED = "scheduled"; RUNNING = "running"; SUCCESS = "success"; FAILED = "failed"; SKIPPED = "skipped"


@dataclass
class TaskInstance:
    task_id: str
    state: str = State.SCHEDULED
    try_number: int = 0
    input_json: Dict[str, Any] = field(default_factory=dict)
    output_json: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


@dataclass
class WorkflowRun:
    id: str
    flow_name: str
    params: Dict[str, Any] = field(default_factory=dict)
    state: str = State.SCHEDULED
    start_date: datetime = field(default_factory=datetime.utcnow)
    end_date: Optional[datetime] = None
    tasks: Dict[str, TaskInstance] = field(default_factory=dict)


class Store:
    def open(self): ...
    def close(self): ...
    def save(self, run: WorkflowRun): ...
    def load(self, run_id: str) -> WorkflowRun: ...
    def exists(self, run_id: str) -> bool: ...
    def list_runs(self) -> list[str]: ...


# ===== Typed task outputs and references =====
T = TypeVar("T")


@dataclass(frozen=True)
class TaskRef(Generic[T]):
    task_id: str
    _from_dict: Callable[[Dict[str, Any]], T]


# Common output payloads used in demo/flows
@dataclass(frozen=True)
class BuildOut:
    build: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "BuildOut":
        return BuildOut(build=d["build"])


@dataclass(frozen=True)
class PricingOut:
    pricing: bool

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "PricingOut":
        return PricingOut(pricing=bool(d["pricing"]))


@dataclass(frozen=True)
class CollationOut:
    collation: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "CollationOut":
        return CollationOut(collation=float(d["collation"]))


@dataclass(frozen=True)
class DiffOut:
    diff: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "DiffOut":
        return DiffOut(diff=float(d["diff"]))


# Convenience builders to avoid stringly typed access at call sites

def ref_build(task_id: str) -> TaskRef[BuildOut]:
    return TaskRef(task_id, BuildOut.from_dict)


def ref_pricing(task_id: str) -> TaskRef[PricingOut]:
    return TaskRef(task_id, PricingOut.from_dict)


def ref_collation(task_id: str) -> TaskRef[CollationOut]:
    return TaskRef(task_id, CollationOut.from_dict)


def ref_diff(task_id: str) -> TaskRef[DiffOut]:
    return TaskRef(task_id, DiffOut.from_dict)
