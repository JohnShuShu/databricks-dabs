"""Tracking-record dataclasses (one per detection surface).

Each has ``to_dict()`` so the writer can build a Spark DataFrame from a list of
records (mirrors the buffered-dataclass pattern in
aif_iceberg-converter/src/iceberg_converter/lineage.py). ``run_id`` / ``run_ts``
are stamped by the writer at write time, so they are NOT fields here.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass
class NotebookFinding:
    path: str
    language: str
    object_id: int
    modified_at: datetime | None
    cell_no: int
    line_no: int
    category: str
    pattern_name: str
    severity: str
    snippet: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AuditEgressEvent:
    event_time: datetime
    email: str
    service_name: str
    action_name: str
    category: str
    source_ip_address: str
    user_agent: str
    request_id: str
    request_summary: str  # small subset of request_params, JSON-encoded

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ApiUsageDaily:
    event_date: datetime  # DATE
    email: str
    service_name: str
    action_name: str
    call_count: int
    baseline_mean: float
    baseline_stddev: float
    zscore: float
    is_spike: bool

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class LineageEgress:
    event_time: datetime
    email: str
    entity_type: str
    source_type: str
    source: str
    target_type: str
    target: str
    direction: str  # "read_external" | "write_external"

    def to_dict(self) -> dict:
        return asdict(self)
