"""Egress signature catalog for the notebook code scan.

Each pattern is a compiled regex plus a category and severity. A hit means the
notebook *could* move data outside Databricks -- these are heuristics that
capture intent, NOT proof that egress happened. The audit/lineage surfaces
capture actual events; this surface catches egress-in-code before it runs
(critical here because system.access.outbound_network is empty in this account).

Patterns are seeded from real usage found across the team's repos so they flag
true positives:
  - af_aif-loch-ness/src/nessie/box/client.py    -> `import requests`, urllib3
  - DB-Admin-Console-Python-CL/pg_admin_web_py    -> httpx
  - aif_iceberg-converter                         -> boto3 / s3 writes

Severity scale:
  high    -> direct outbound data transfer (HTTP client, external object store,
             external DB write, SMTP/SFTP).
  medium  -> credential/URL material or result exfil that commonly precedes
             egress (presigned URLs, s3:// literals, notebook.exit payloads).
  low     -> weaker signals worth noting (generic urllib import).
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class EgressPattern:
    name: str
    regex: "re.Pattern[str]"
    category: str
    severity: str


def _p(name: str, pattern: str, category: str, severity: str) -> EgressPattern:
    return EgressPattern(name, re.compile(pattern), category, severity)


# NOTE: patterns are intentionally line-oriented and conservative. We favor a
# few false positives (a reviewer can dismiss) over missing real egress.
EGRESS_PATTERNS: list[EgressPattern] = [
    # ── Outbound HTTP clients ───────────────────────────────────────────
    _p("requests_import", r"\b(?:import\s+requests\b|from\s+requests\s+import)", "http_client", "high"),
    _p("requests_call", r"\brequests\.(?:get|post|put|patch|delete|request|Session)\s*\(", "http_client", "high"),
    _p("httpx_use", r"\bhttpx\.(?:get|post|put|patch|delete|Client|AsyncClient|stream)\s*\(", "http_client", "high"),
    _p("aiohttp_use", r"\baiohttp\.(?:ClientSession|request)\b", "http_client", "high"),
    _p("urllib3_use", r"\burllib3\.(?:PoolManager|request)\b", "http_client", "high"),
    _p("urllib_request", r"\burllib\.request\.(?:urlopen|Request)\b", "http_client", "medium"),
    _p("urllib_import", r"\bimport\s+urllib\b", "http_client", "low"),
    _p("http_client_lib", r"\bimport\s+http\.client\b|\bhttp\.client\.HTTP", "http_client", "medium"),

    # ── External object store / cloud SDK writes ────────────────────────
    _p("boto3_use", r"\bboto3\.(?:client|resource|Session)\s*\(", "cloud_sdk", "high"),
    _p("s3_put", r"\.(?:put_object|upload_file|upload_fileobj)\s*\(", "cloud_sdk", "high"),
    _p("s3_uri", r"\bs3[an]?://", "external_uri", "medium"),
    _p("gcs_uri", r"\bgs://", "external_uri", "medium"),
    _p("azure_blob_uri", r"\b(?:wasbs?|abfss?)://", "external_uri", "medium"),
    _p("gcs_sdk", r"\bfrom\s+google\.cloud\s+import\s+storage\b|\bstorage\.Client\s*\(", "cloud_sdk", "high"),
    _p("azure_sdk", r"\bBlobServiceClient\b", "cloud_sdk", "high"),

    # ── External database writes / connectors ───────────────────────────
    _p("jdbc_write", r"\.write\b[\s\S]{0,120}?\.format\(\s*['\"]jdbc['\"]\s*\)", "external_db", "high"),
    _p("jdbc_url", r"\bjdbc:(?:mysql|postgresql|oracle|sqlserver|snowflake|redshift)", "external_db", "high"),
    _p("snowflake_connector", r"\bimport\s+snowflake\.connector\b|\bsnowflake\.connector\.connect\b", "external_db", "high"),
    _p("pg_connector", r"\b(?:import\s+psycopg2|psycopg2\.connect|import\s+pymysql|pymysql\.connect|cx_Oracle\.connect|oracledb\.connect)\b", "external_db", "medium"),

    # ── File transfer / mail ────────────────────────────────────────────
    _p("sftp_paramiko", r"\bparamiko\.(?:SSHClient|Transport)\b|\bpysftp\.Connection\b", "file_transfer", "high"),
    _p("ftp_lib", r"\bftplib\.FTP(?:_TLS)?\s*\(", "file_transfer", "high"),
    _p("smtp_send", r"\bsmtplib\.SMTP(?:_SSL)?\s*\(|\bimport\s+smtplib\b", "mail", "high"),

    # ── Result exfil / presigned material ───────────────────────────────
    _p("notebook_exit", r"\bdbutils\.notebook\.exit\s*\(", "result_exfil", "medium"),
    _p("presigned_url", r"\bgenerate_presigned_url\b|\bcreateDownloadUrl\b", "presigned_url", "medium"),
    _p("external_http_literal", r"https?://(?!(?:[\w.-]*\.)?(?:databricks\.com|cloud\.databricks\.com|azuredatabricks\.net|localhost|127\.0\.0\.1))", "external_uri", "low"),
]


def scan_line(line: str) -> list[EgressPattern]:
    """Return every pattern that matches ``line`` (usually 0 or 1)."""
    return [p for p in EGRESS_PATTERNS if p.regex.search(line)]
