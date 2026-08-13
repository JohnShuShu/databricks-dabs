"""Table-lineage egress surface (system.access.table_lineage).

Flags read/write operations whose source or target is an external path
(``source_type`` / ``target_type`` = 'PATH'). A PATH target = data written to an
external location (egress out of managed storage); a PATH source = data read
from an external location (potential external staging before egress). Verified
against the live table: PATH lineage rows exist in this account.

Note: an empty ``target_type`` on a PATH source (or vice-versa) is normal --
lineage rows often record only one side. We classify by whichever side is PATH.

Most PATH rows in this account point at Databricks-*internal* managed-table
backing storage (``.../__unitystorage/...``) -- that is normal managed-Delta I/O,
NOT external egress, and reporting it would bury real findings in noise. So we
exclude PATH values whose external side is an internal ``__unitystorage`` path.
An external UC volume path (``/Volumes/...``) or a customer S3 path is kept.

The SQL builder is pure so it can be unit-tested; ``query_lineage_egress``
executes it and maps rows into LineageEgress records.
"""

from __future__ import annotations

from datetime import datetime

from .logging_setup import logging_setup
from .records import LineageEgress
from .runners import SqlRunner, as_dt, as_str

logger = logging_setup(__name__)

# Substring that identifies a Databricks-internal managed-table backing path.
# PATH rows containing this are normal managed-Delta I/O, not external egress.
_INTERNAL_PATH_MARKER = "__unitystorage"


def build_lineage_sql(since_ts: datetime) -> str:
    """SQL for external-path lineage events at or after ``since_ts``.

    Keeps rows where the source or target is a PATH, but drops those whose PATH
    side is an internal ``__unitystorage`` managed-table location.
    """
    since_iso = since_ts.strftime("%Y-%m-%d %H:%M:%S")
    since_date = since_ts.strftime("%Y-%m-%d")
    return f"""
        SELECT
            event_time,
            created_by,
            entity_type,
            source_type,
            COALESCE(source_path, source_table_full_name) AS source,
            target_type,
            COALESCE(target_path, target_table_full_name) AS target
        FROM system.access.table_lineage
        WHERE event_date >= DATE'{since_date}'
          AND event_time >= TIMESTAMP'{since_iso}'
          AND (
                (source_type = 'PATH' AND source_path NOT LIKE '%{_INTERNAL_PATH_MARKER}%')
             OR (target_type = 'PATH' AND target_path NOT LIKE '%{_INTERNAL_PATH_MARKER}%')
          )
    """.strip()


def query_lineage_egress(runner: SqlRunner, since_ts: datetime) -> list[LineageEgress]:
    """Run the lineage query and return external-path egress events."""
    sql = build_lineage_sql(since_ts)
    rows = runner.run(sql)
    events = []
    for r in rows:
        # A PATH target is a write-out; otherwise the PATH is on the source side.
        direction = "write_external" if r["target_type"] == "PATH" else "read_external"
        events.append(
            LineageEgress(
                event_time=as_dt(r["event_time"]),
                email=as_str(r["created_by"], "unknown") or "unknown",
                entity_type=as_str(r["entity_type"]),
                source_type=as_str(r["source_type"]),
                source=as_str(r["source"]),
                target_type=as_str(r["target_type"]),
                target=as_str(r["target"]),
                direction=direction,
            )
        )
    logger.info("lineage egress: %d external-path events since %s", len(events), since_ts)
    return events
