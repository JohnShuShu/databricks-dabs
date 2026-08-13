"""API-usage profiling surface (system.access.audit aggregation).

Aggregates audit events into per-user / per-service / per-action daily call
counts, and flags spikes: for each (email, service_name, action_name) it
computes a baseline mean/stddev over the trailing window and marks any day whose
z-score exceeds the configured threshold. This profiles "who calls what, how
much" and surfaces sudden jumps (a signal of bulk export / scripted API abuse).

The SQL builder is pure so it can be unit-tested; ``query_api_usage`` executes
it and maps rows into ApiUsageDaily records.
"""

from __future__ import annotations

from datetime import datetime

from .logging_setup import logging_setup
from .records import ApiUsageDaily
from .runners import SqlRunner, as_bool, as_date, as_float, as_int, as_str

logger = logging_setup(__name__)


def build_api_usage_sql(since_date: datetime, baseline_days: int, zscore_threshold: float) -> str:
    """SQL for daily API-usage counts with per-key baseline + spike flag.

    ``since_date`` bounds the *reported* days; the baseline window extends
    ``baseline_days`` further back so early reported days still have history.
    """
    since = since_date.strftime("%Y-%m-%d")
    return f"""
        WITH daily AS (
            SELECT
                event_date,
                user_identity.email AS email,
                service_name,
                action_name,
                COUNT(*)            AS call_count
            FROM system.access.audit
            WHERE event_date >= DATE'{since}' - INTERVAL {baseline_days} DAYS
            GROUP BY event_date, user_identity.email, service_name, action_name
        ),
        stats AS (
            SELECT
                email, service_name, action_name,
                AVG(call_count)    AS baseline_mean,
                STDDEV(call_count) AS baseline_stddev
            FROM daily
            GROUP BY email, service_name, action_name
        )
        SELECT
            d.event_date,
            COALESCE(d.email, 'unknown')       AS email,
            COALESCE(d.service_name, '')       AS service_name,
            COALESCE(d.action_name, '')        AS action_name,
            d.call_count,
            COALESCE(s.baseline_mean, 0.0)     AS baseline_mean,
            COALESCE(s.baseline_stddev, 0.0)   AS baseline_stddev,
            CASE
                WHEN COALESCE(s.baseline_stddev, 0.0) > 0
                THEN (d.call_count - s.baseline_mean) / s.baseline_stddev
                ELSE 0.0
            END                                AS zscore,
            CASE
                WHEN COALESCE(s.baseline_stddev, 0.0) > 0
                     AND (d.call_count - s.baseline_mean) / s.baseline_stddev > {zscore_threshold}
                THEN TRUE ELSE FALSE
            END                                AS is_spike
        FROM daily d
        LEFT JOIN stats s
          ON d.email <=> s.email
         AND d.service_name <=> s.service_name
         AND d.action_name <=> s.action_name
        WHERE d.event_date >= DATE'{since}'
    """.strip()


def query_api_usage(
    runner: SqlRunner,
    since_date: datetime,
    baseline_days: int,
    zscore_threshold: float,
) -> list[ApiUsageDaily]:
    """Run the API-usage aggregation and return daily rows (spikes flagged)."""
    sql = build_api_usage_sql(since_date, baseline_days, zscore_threshold)
    rows = runner.run(sql)
    usage = [
        ApiUsageDaily(
            event_date=as_date(r["event_date"]),
            email=as_str(r["email"], "unknown"),
            service_name=as_str(r["service_name"]),
            action_name=as_str(r["action_name"]),
            call_count=as_int(r["call_count"]),
            baseline_mean=as_float(r["baseline_mean"]),
            baseline_stddev=as_float(r["baseline_stddev"]),
            zscore=as_float(r["zscore"]),
            is_spike=as_bool(r["is_spike"]),
        )
        for r in rows
    ]
    spikes = sum(1 for u in usage if u.is_spike)
    logger.info("api usage: %d daily rows, %d spikes since %s", len(usage), spikes, since_date)
    return usage
