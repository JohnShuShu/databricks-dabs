"""Incremental watermark storage -- two interchangeable stores.

Each surface records the max event/modified time it has processed, so the next
run only looks at what changed (the last_check_time pattern from github-monitor).

- ``SparkStateStore`` : one Delta row per surface in ``<fq_schema>.egress_scan_state``.
- ``FileStateStore``  : a small JSON file (``egress_scan_state.json``) in the
  output dir. Used standalone where there is no Spark.

``resolve_since`` returns the watermark unless a full scan is requested, in which
case it returns now - lookback_days.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol

from .logging_setup import logging_setup
from .runners import as_dt

logger = logging_setup(__name__)

STATE_TABLE = "egress_scan_state"


class StateStore(Protocol):
    def get(self, surface: str) -> datetime | None:  # pragma: no cover
        ...

    def set(self, surface: str, last_event_time: datetime, run_id: str, run_ts: datetime) -> None:  # pragma: no cover
        ...


class SparkStateStore:
    def __init__(self, spark, fq_schema: str):
        self.spark = spark
        self.fq_schema = fq_schema
        self.fqn = f"{fq_schema}.{STATE_TABLE}"
        self._ensure()

    def _ensure(self) -> None:
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.fqn} (
                surface         STRING,
                last_event_time TIMESTAMP,
                run_id          STRING,
                run_ts          TIMESTAMP
            ) USING DELTA
            """
        )

    def get(self, surface: str) -> datetime | None:
        try:
            rows = self.spark.sql(
                f"SELECT last_event_time FROM {self.fqn} WHERE surface = '{surface}'"
            ).collect()
        except Exception as exc:  # noqa: BLE001
            logger.info("no watermark for %s (%s)", surface, exc)
            return None
        if not rows or rows[0]["last_event_time"] is None:
            return None
        return rows[0]["last_event_time"]

    def set(self, surface: str, last_event_time: datetime, run_id: str, run_ts: datetime) -> None:
        src = self.spark.createDataFrame(
            [(surface, last_event_time, run_id, run_ts)],
            schema="surface STRING, last_event_time TIMESTAMP, run_id STRING, run_ts TIMESTAMP",
        )
        src.createOrReplaceTempView("_egress_wm_src")
        self.spark.sql(
            f"""
            MERGE INTO {self.fqn} AS t
            USING _egress_wm_src AS s
            ON t.surface = s.surface
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        logger.info("watermark %s -> %s", surface, last_event_time)


class FileStateStore:
    def __init__(self, output_dir: str):
        self.path = Path(output_dir) / f"{STATE_TABLE}.json"

    def _load(self) -> dict:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("could not read state file %s: %s", self.path, exc)
            return {}

    def get(self, surface: str) -> datetime | None:
        entry = self._load().get(surface)
        return as_dt(entry.get("last_event_time")) if entry else None

    def set(self, surface: str, last_event_time: datetime, run_id: str, run_ts: datetime) -> None:
        data = self._load()
        data[surface] = {
            "last_event_time": last_event_time.isoformat(),
            "run_id": run_id,
            "run_ts": run_ts.isoformat(),
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(data, indent=2))
        logger.info("watermark %s -> %s", surface, last_event_time)


def resolve_since(
    store: StateStore,
    surface: str,
    lookback_days: int,
    full_scan: bool,
) -> datetime:
    """Resolve the start timestamp for a surface: watermark unless full_scan."""
    fallback = datetime.now(tz=timezone.utc) - timedelta(days=lookback_days)
    if full_scan:
        return fallback
    return store.get(surface) or fallback
