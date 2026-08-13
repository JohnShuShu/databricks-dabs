"""Unit tests for the SQL builders + config (pure logic, no Spark)."""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from egress_monitor.api_usage import build_api_usage_sql  # noqa: E402
from egress_monitor.audit_query import EGRESS_ACTIONS, build_audit_sql  # noqa: E402
from egress_monitor.config import Settings  # noqa: E402
from egress_monitor.lineage_query import build_lineage_sql  # noqa: E402

TS = datetime(2026, 8, 10, 14, 30, 0)


# ── audit SQL ────────────────────────────────────────────────────────────
def test_audit_sql_prunes_on_event_date_and_time():
    sql = build_audit_sql(TS)
    assert "event_date >= DATE'2026-08-10'" in sql
    assert "event_time >= TIMESTAMP'2026-08-10 14:30:00'" in sql
    assert "FROM system.access.audit" in sql


def test_audit_sql_includes_every_egress_action():
    sql = build_audit_sql(TS)
    for action in EGRESS_ACTIONS:
        assert f"'{action}'" in sql, f"{action} missing from IN-list"


def test_audit_sql_selects_email_from_struct():
    assert "user_identity.email" in build_audit_sql(TS)


# ── api usage SQL ──────────────────────────────────────────────────────────
def test_api_usage_sql_baseline_window_and_threshold():
    sql = build_api_usage_sql(TS, baseline_days=14, zscore_threshold=3.0)
    assert "INTERVAL 14 DAYS" in sql
    assert "3.0" in sql
    assert "is_spike" in sql
    # reported rows bounded by the since date, baseline extends earlier
    assert "d.event_date >= DATE'2026-08-10'" in sql


def test_api_usage_sql_null_safe_join():
    # null-safe equality so rows with null email/service still join to baseline
    assert "<=>" in build_api_usage_sql(TS, 14, 3.0)


# ── lineage SQL ────────────────────────────────────────────────────────────
def test_lineage_sql_filters_external_paths():
    sql = build_lineage_sql(TS)
    assert "source_type = 'PATH'" in sql
    assert "target_type = 'PATH'" in sql
    assert "FROM system.access.table_lineage" in sql


def test_lineage_sql_excludes_internal_unitystorage():
    # internal managed-table backing paths are not external egress
    sql = build_lineage_sql(TS)
    assert "__unitystorage" in sql
    assert "NOT LIKE" in sql


# ── config ─────────────────────────────────────────────────────────────────
def test_fq_schema_and_csv_dir():
    s = Settings(output_catalog="fabric_dev", output_schema="observability")
    assert s.fq_schema == "fabric_dev.observability"
    assert s.csv_dir == "/Volumes/fabric_dev/observability/egress_reports"


def test_portable_defaults():
    s = Settings()
    assert s.mode == "spark"
    assert s.fq_schema == "main.egress_monitor"


def test_csv_dir_explicit_override():
    s = Settings(csv_volume_path="/Volumes/x/y/z/")
    assert s.csv_dir == "/Volumes/x/y/z"


def test_enabled_surfaces_default_is_all_four():
    assert Settings().enabled_surfaces() == ["notebooks", "audit", "api_usage", "lineage"]


def test_enabled_surfaces_subset():
    assert Settings(surfaces="audit, lineage").enabled_surfaces() == ["audit", "lineage"]


def test_enabled_surfaces_rejects_unknown():
    try:
        Settings(surfaces="bogus").enabled_surfaces()
    except ValueError as e:
        assert "bogus" in str(e)
    else:
        raise AssertionError("expected ValueError for unknown surface")
