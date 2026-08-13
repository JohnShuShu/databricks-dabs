"""SQL execution abstraction so the same queries run in two modes.

- ``SparkRunner``  : in-workspace serverless/cluster job -> ``spark.sql``.
- ``WarehouseRunner``: standalone (laptop/CI) -> Databricks SQL Statement
  Execution API against a SQL warehouse, via databricks-sdk. Works against ANY
  account/workspace the WorkspaceClient is pointed at.

Both expose ``run(sql) -> list[dict]`` returning one dict per row keyed by
column name. Spark returns typed values; the warehouse API returns every value
as a string (or None). So query mappers must coerce with the ``as_*`` helpers
below rather than assuming a type -- that keeps a single mapping path for both
runners.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Protocol

from .logging_setup import logging_setup

logger = logging_setup(__name__)


class SqlRunner(Protocol):
    """Anything that can run read-only SQL and return rows as dicts."""

    def run(self, sql: str) -> list[dict]:  # pragma: no cover - protocol
        ...


class SparkRunner:
    """Runs SQL via an active SparkSession (in-workspace mode)."""

    def __init__(self, spark):
        self.spark = spark

    def run(self, sql: str) -> list[dict]:
        return [row.asDict() for row in self.spark.sql(sql).collect()]


class WarehouseRunner:
    """Runs SQL via the SQL Statement Execution API (standalone mode).

    Pages through result chunks so large result sets are fully materialized.
    """

    def __init__(self, workspace_client, warehouse_id: str, wait_timeout: str = "50s"):
        if not warehouse_id:
            raise ValueError("WarehouseRunner requires a warehouse_id")
        self.w = workspace_client
        self.warehouse_id = warehouse_id
        self.wait_timeout = wait_timeout

    def run(self, sql: str) -> list[dict]:
        from databricks.sdk.service.sql import StatementState

        resp = self.w.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=sql,
            wait_timeout=self.wait_timeout,
            disposition=None,
        )
        statement_id = resp.statement_id
        # Poll until the statement leaves a pending state.
        while resp.status and resp.status.state in (
            StatementState.PENDING,
            StatementState.RUNNING,
        ):
            resp = self.w.statement_execution.get_statement(statement_id)

        if not resp.status or resp.status.state != StatementState.SUCCEEDED:
            err = resp.status.error if resp.status else None
            raise RuntimeError(f"statement {statement_id} failed: {err}")

        return self._collect_rows(resp, statement_id)

    def _collect_rows(self, resp, statement_id: str) -> list[dict]:
        manifest = resp.manifest
        if not manifest or not manifest.schema or not manifest.schema.columns:
            return []
        columns = [c.name for c in manifest.schema.columns]

        rows: list[dict] = []
        result = resp.result
        while result is not None:
            for data_row in result.data_array or []:
                rows.append(dict(zip(columns, data_row)))
            next_chunk = result.next_chunk_index
            if next_chunk is None:
                break
            result = self.w.statement_execution.get_statement_result_chunk_n(
                statement_id, next_chunk
            )
        return rows


# ── value coercion (warehouse returns strings; Spark returns typed) ──────────
def as_dt(value) -> datetime | None:
    """Coerce a value to a timezone-aware datetime (UTC), or None."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    s = str(value).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        # Some warehouse timestamps come back as epoch millis strings.
        try:
            return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
        except (ValueError, TypeError):
            logger.warning("could not parse datetime %r", value)
            return None


def as_date(value) -> date | None:
    dt = as_dt(value)
    return dt.date() if dt else None


def as_int(value, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return int(float(value))


def as_float(value, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "t", "1", "yes")


def as_str(value, default: str = "") -> str:
    return default if value is None else str(value)
