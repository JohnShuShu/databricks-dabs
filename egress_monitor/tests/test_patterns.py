"""Unit tests for the egress pattern catalog (no Spark / no workspace needed)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from egress_monitor.patterns import EGRESS_PATTERNS, scan_line  # noqa: E402


def _hit_names(line: str) -> set[str]:
    return {p.name for p in scan_line(line)}


def test_requests_import_flagged():
    assert "requests_import" in _hit_names("import requests")
    assert "requests_import" in _hit_names("from requests import Session")


def test_requests_call_flagged():
    assert "requests_call" in _hit_names("resp = requests.post(url, json=payload)")
    assert "requests_call" in _hit_names("    r = requests.get('https://api.example.com/data')")


def test_httpx_flagged():
    assert "httpx_use" in _hit_names("client = httpx.Client()")


def test_urllib3_flagged():
    assert "urllib3_use" in _hit_names("http = urllib3.PoolManager()")


def test_boto3_and_s3_put_flagged():
    assert "boto3_use" in _hit_names("s3 = boto3.client('s3')")
    assert "s3_put" in _hit_names("s3.put_object(Bucket=b, Key=k, Body=data)")


def test_s3_uri_flagged():
    assert "s3_uri" in _hit_names("df.write.parquet('s3://external-bucket/out')")


def test_external_jdbc_flagged():
    hits = _hit_names("url = 'jdbc:postgresql://external-host:5432/db'")
    assert "jdbc_url" in hits


def test_snowflake_connector_flagged():
    assert "snowflake_connector" in _hit_names("import snowflake.connector")


def test_smtp_flagged():
    assert "smtp_send" in _hit_names("server = smtplib.SMTP('smtp.gmail.com', 587)")


def test_sftp_flagged():
    assert "sftp_paramiko" in _hit_names("client = paramiko.SSHClient()")


def test_notebook_exit_flagged():
    assert "notebook_exit" in _hit_names("dbutils.notebook.exit(json.dumps(rows))")


def test_external_http_literal_flagged():
    assert "external_http_literal" in _hit_names("URL = 'https://evil.example.com/upload'")


# ── negative cases: benign code must NOT be flagged ──────────────────────
def test_databricks_url_not_flagged():
    # internal Databricks hosts are explicitly excluded from the http literal rule
    assert "external_http_literal" not in _hit_names("host = 'https://dbc-2ff2d226-c0da.cloud.databricks.com'")


def test_plain_spark_read_not_flagged():
    assert _hit_names("df = spark.read.table('catalog.schema.table')") == set()


def test_managed_delta_write_not_flagged():
    assert _hit_names("df.write.mode('overwrite').saveAsTable('cat.sch.tbl')") == set()


def test_comment_mentioning_requests_word_only():
    # the word "requests" in prose without import/call should not match the
    # import/call rules (it may match nothing)
    assert "requests_import" not in _hit_names("# this handles user requests gracefully")
    assert "requests_call" not in _hit_names("# this handles user requests gracefully")


def test_all_patterns_have_valid_severity():
    valid = {"high", "medium", "low"}
    for p in EGRESS_PATTERNS:
        assert p.severity in valid, f"{p.name} has bad severity {p.severity}"
        assert p.category, f"{p.name} missing category"
