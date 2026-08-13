"""Where findings are written -- two interchangeable sinks.

- ``SparkSink`` : append to Unity Catalog Delta tables (+ CSV mirror to a UC
  volume). Used in-workspace where a SparkSession is available.
- ``FileSink``  : write CSV + JSON files to a directory (local path or a mounted
  UC volume). Used standalone (laptop/CI) with no Spark.

Both take a list of dataclass records (each with ``.to_dict()``) and stamp every
row with the run's ``run_id`` / ``run_ts``. The column order for each table
comes from ``TABLE_COLUMNS`` so Delta, CSV and JSON stay consistent.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Protocol

from .logging_setup import logging_setup

logger = logging_setup(__name__)

NOTEBOOK_TABLE = "egress_notebook_findings"
AUDIT_TABLE = "egress_audit_events"
API_USAGE_TABLE = "api_usage_daily"
LINEAGE_TABLE = "egress_lineage"

_RUN_COLS = ["run_id", "run_ts"]

# Canonical column order per table (record fields + run-stamp columns).
TABLE_COLUMNS: dict[str, list[str]] = {
    NOTEBOOK_TABLE: [
        "path", "language", "object_id", "modified_at", "cell_no", "line_no",
        "category", "pattern_name", "severity", "snippet", *_RUN_COLS,
    ],
    AUDIT_TABLE: [
        "event_time", "email", "service_name", "action_name", "category",
        "source_ip_address", "user_agent", "request_id", "request_summary", *_RUN_COLS,
    ],
    API_USAGE_TABLE: [
        "event_date", "email", "service_name", "action_name", "call_count",
        "baseline_mean", "baseline_stddev", "zscore", "is_spike", *_RUN_COLS,
    ],
    LINEAGE_TABLE: [
        "event_time", "email", "entity_type", "source_type", "source",
        "target_type", "target", "direction", *_RUN_COLS,
    ],
}


def _stamped_rows(table: str, records: list, run_id: str, run_ts: datetime) -> list[dict]:
    cols = TABLE_COLUMNS[table]
    out = []
    for rec in records:
        d = rec.to_dict()
        d["run_id"] = run_id
        d["run_ts"] = run_ts
        out.append({c: d.get(c) for c in cols})
    return out


class Sink(Protocol):
    def write(self, table: str, records: list, run_id: str, run_ts: datetime) -> int:  # pragma: no cover
        ...


class SparkSink:
    """Append findings to UC Delta tables (+ optional CSV mirror to a volume)."""

    def __init__(self, spark, fq_schema: str, csv_dir: str | None = None):
        self.spark = spark
        self.fq_schema = fq_schema
        self.csv_dir = csv_dir
        from . import spark_schemas  # local import: pyspark only in-workspace

        self._schemas = spark_schemas.SCHEMAS

    def write(self, table: str, records: list, run_id: str, run_ts: datetime) -> int:
        rows = _stamped_rows(table, records, run_id, run_ts)
        order = TABLE_COLUMNS[table]
        tuples = [tuple(r[c] for c in order) for r in rows]
        df = self.spark.createDataFrame(tuples, schema=self._schemas[table])
        fqn = f"{self.fq_schema}.{table}"
        df.write.mode("append").option("mergeSchema", "true").saveAsTable(fqn)
        logger.info("wrote %d rows -> %s", len(tuples), fqn)

        if self.csv_dir and tuples:
            csv_path = f"{self.csv_dir.rstrip('/')}/{table}/{run_id}.csv"
            try:
                df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
                logger.info("csv mirror -> %s", csv_path)
            except Exception as exc:  # noqa: BLE001 - CSV mirror is best-effort
                logger.warning("csv mirror failed for %s: %s", table, exc)
        return len(tuples)


class FileSink:
    """Write findings as CSV + JSON files under a directory (no Spark)."""

    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)

    def write(self, table: str, records: list, run_id: str, run_ts: datetime) -> int:
        rows = _stamped_rows(table, records, run_id, run_ts)
        cols = TABLE_COLUMNS[table]
        target_dir = self.output_dir / table
        target_dir.mkdir(parents=True, exist_ok=True)

        csv_path = target_dir / f"{run_id}.csv"
        with csv_path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
            writer.writeheader()
            for r in rows:
                writer.writerow({c: _scalarize(r[c]) for c in cols})

        json_path = target_dir / f"{run_id}.json"
        with json_path.open("w") as fh:
            json.dump([{c: _scalarize(r[c]) for c in cols} for r in rows], fh, indent=2, default=str)

        logger.info("wrote %d rows -> %s (+ .json)", len(rows), csv_path)
        return len(rows)


def _scalarize(value):
    """Render datetimes/dates as ISO strings for CSV/JSON; pass through else."""
    if isinstance(value, datetime):
        return value.isoformat()
    return value
