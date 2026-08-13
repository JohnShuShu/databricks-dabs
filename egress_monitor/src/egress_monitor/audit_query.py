"""Audit-log egress surface (system.access.audit).

Selects the audit actions that represent data leaving the platform or the
credentials/API access that enables it. The action allow-list was verified
against the live table -- these actions actually occur in this account:

  workspaceExport                      notebook/workspace source exported out
  createDownloadUrl                    presigned download of a file / result
  getResults / downloadQueryResult     SQL/notebook result download
  generateTemporaryTableCredential     external credential vend for a UC table
  generateTemporaryVolumeCredential    external credential vend for a UC volume
  getForeignCredentials                credentials for a foreign/federated source
  tokenLogin / oidcTokenAuthorization  API-token auth (API-key usage signal)

The SQL builder is pure (no Spark) so it is unit-testable; ``query_audit_egress``
runs it via ``spark.sql`` and maps rows into AuditEgressEvent records.
"""

from __future__ import annotations

import json
from datetime import datetime

from .logging_setup import logging_setup
from .records import AuditEgressEvent
from .runners import SqlRunner, as_dt, as_str

logger = logging_setup(__name__)

# action_name -> egress category. Kept as a dict so the SQL IN-list and the
# per-row categorization stay in sync.
EGRESS_ACTIONS: dict[str, str] = {
    "workspaceExport": "workspace_export",
    "createDownloadUrl": "file_download",
    "getResults": "result_download",
    "downloadQueryResult": "result_download",
    "generateTemporaryTableCredential": "credential_vend",
    "generateTemporaryVolumeCredential": "credential_vend",
    "getForeignCredentials": "credential_vend",
    "tokenLogin": "api_token_auth",
    "oidcTokenAuthorization": "api_token_auth",
}

# request_params keys worth keeping in the summary (small, non-sensitive).
_SUMMARY_KEYS = ["path", "name", "full_name_arg", "securable_type", "operation"]


def _sql_str_list(values) -> str:
    """Render a Python iterable as a SQL string IN-list: 'a','b'."""
    return ", ".join("'" + str(v).replace("'", "''") + "'" for v in values)


def build_audit_sql(since_ts: datetime) -> str:
    """SQL for egress-relevant audit events at or after ``since_ts``.

    Filters on event_date first (the table's partition column) so the scan is
    pruned, then on the precise event_time and action allow-list.
    """
    since_iso = since_ts.strftime("%Y-%m-%d %H:%M:%S")
    since_date = since_ts.strftime("%Y-%m-%d")
    actions = _sql_str_list(EGRESS_ACTIONS.keys())
    return f"""
        SELECT
            event_time,
            user_identity.email        AS email,
            service_name,
            action_name,
            source_ip_address,
            user_agent,
            request_id,
            request_params
        FROM system.access.audit
        WHERE event_date >= DATE'{since_date}'
          AND event_time >= TIMESTAMP'{since_iso}'
          AND action_name IN ({actions})
    """.strip()


def _summarize_params(params) -> str:
    """Keep a small, useful subset of request_params as a JSON string.

    Spark returns a dict/Row map; the warehouse API returns request_params as a
    JSON string. Handle both.
    """
    if not params:
        return "{}"
    if isinstance(params, str):
        try:
            params = json.loads(params)
        except json.JSONDecodeError:
            return "{}"
    try:
        as_dict = dict(params)
    except (TypeError, ValueError):
        return "{}"
    kept = {k: as_dict[k] for k in _SUMMARY_KEYS if k in as_dict}
    return json.dumps(kept, default=str)


def query_audit_egress(runner: SqlRunner, since_ts: datetime) -> list[AuditEgressEvent]:
    """Run the audit query and return egress events."""
    sql = build_audit_sql(since_ts)
    rows = runner.run(sql)
    events = [
        AuditEgressEvent(
            event_time=as_dt(r["event_time"]),
            email=as_str(r["email"], "unknown") or "unknown",
            service_name=as_str(r["service_name"]),
            action_name=as_str(r["action_name"]),
            category=EGRESS_ACTIONS.get(r["action_name"], "other"),
            source_ip_address=as_str(r["source_ip_address"]),
            user_agent=as_str(r["user_agent"]),
            request_id=as_str(r["request_id"]),
            request_summary=_summarize_params(r["request_params"]),
        )
        for r in rows
    ]
    logger.info("audit egress: %d events since %s", len(events), since_ts)
    return events
