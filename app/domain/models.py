import uuid
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Job:
    id: uuid.UUID
    description: str | None

    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class PricingLibrary:
    version: str
    created_at: datetime


@dataclass(frozen=True)
class PricerImage:
    fullname: str
    commit: str
    pricing_library: PricingLibrary
