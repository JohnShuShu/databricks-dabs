"""Unit tests for the portability layer: environment registry, runner value
coercion, file sink, and file state store. No Spark / no workspace needed."""

import json
import os
import sys
from datetime import date, datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from egress_monitor import runners  # noqa: E402
from egress_monitor.environment import Environment, get_environment, load_registry  # noqa: E402
from egress_monitor.records import ApiUsageDaily, AuditEgressEvent  # noqa: E402
from egress_monitor.sinks import API_USAGE_TABLE, AUDIT_TABLE, FileSink  # noqa: E402
from egress_monitor.state import FileStateStore, resolve_since  # noqa: E402


# ── environment registry ─────────────────────────────────────────────────
def test_load_registry_yaml(tmp_path):
    reg = tmp_path / "envs.yaml"
    reg.write_text(
        "environments:\n"
        "  acme:\n"
        "    profile: acme-prof\n"
        "    warehouse_id: wh1\n"
        "    output_catalog: acme_cat\n"
    )
    envs = load_registry(reg)
    assert "acme" in envs
    assert envs["acme"].profile == "acme-prof"
    assert envs["acme"].warehouse_id == "wh1"
    assert envs["acme"].fq_schema == "acme_cat.egress_monitor"


def test_load_registry_json(tmp_path):
    reg = tmp_path / "envs.json"
    reg.write_text(json.dumps({"environments": {"b": {"host": "https://x", "warehouse_id": "w"}}}))
    envs = load_registry(reg)
    assert envs["b"].host == "https://x"


def test_get_environment_missing_raises(tmp_path):
    reg = tmp_path / "envs.json"
    reg.write_text(json.dumps({"environments": {"a": {}}}))
    try:
        get_environment(reg, "nope")
    except KeyError as e:
        assert "nope" in str(e) and "a" in str(e)
    else:
        raise AssertionError("expected KeyError")


def test_environment_resolve_token_env(monkeypatch):
    monkeypatch.setenv("MY_TOKEN", "dapi-secret")
    env = Environment(name="e", token="env:MY_TOKEN")
    assert env.resolve_token() == "dapi-secret"


def test_environment_resolve_token_literal():
    assert Environment(name="e", token="dapi-literal").resolve_token() == "dapi-literal"


# ── runner value coercion (warehouse returns strings) ──────────────────────
def test_as_dt_parses_iso_and_z():
    dt = runners.as_dt("2026-08-12T13:05:05.354Z")
    assert dt.year == 2026 and dt.tzinfo is not None


def test_as_dt_passthrough_datetime():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert runners.as_dt(now) == now


def test_as_dt_none_and_empty():
    assert runners.as_dt(None) is None
    assert runners.as_dt("") is None


def test_as_date():
    assert runners.as_date("2026-08-12") == date(2026, 8, 12)


def test_numeric_and_bool_coercion():
    assert runners.as_int("42") == 42
    assert runners.as_int(None, default=-1) == -1
    assert runners.as_float("3.5") == 3.5
    assert runners.as_bool("true") is True
    assert runners.as_bool("false") is False
    assert runners.as_bool(True) is True


# ── file sink + state round-trip ───────────────────────────────────────────
def _sample_audit():
    return AuditEgressEvent(
        event_time=datetime(2026, 8, 12, 10, tzinfo=timezone.utc),
        email="a@b.com", service_name="workspace", action_name="workspaceExport",
        category="workspace_export", source_ip_address="1.2.3.4", user_agent="ua",
        request_id="rid", request_summary="{}",
    )


def test_file_sink_writes_csv_and_json(tmp_path):
    sink = FileSink(str(tmp_path))
    n = sink.write(AUDIT_TABLE, [_sample_audit()], run_id="run1",
                   run_ts=datetime(2026, 8, 12, tzinfo=timezone.utc))
    assert n == 1
    csv_file = tmp_path / AUDIT_TABLE / "run1.csv"
    json_file = tmp_path / AUDIT_TABLE / "run1.json"
    assert csv_file.exists() and json_file.exists()
    data = json.loads(json_file.read_text())
    assert data[0]["action_name"] == "workspaceExport"
    assert data[0]["run_id"] == "run1"
    # datetimes rendered as ISO strings
    assert data[0]["event_time"].startswith("2026-08-12")


def test_file_sink_empty_batch_still_writes_header(tmp_path):
    sink = FileSink(str(tmp_path))
    n = sink.write(API_USAGE_TABLE, [], run_id="r0",
                   run_ts=datetime(2026, 8, 12, tzinfo=timezone.utc))
    assert n == 0
    assert (tmp_path / API_USAGE_TABLE / "r0.csv").exists()


def test_file_state_store_round_trip(tmp_path):
    store = FileStateStore(str(tmp_path))
    assert store.get("audit") is None
    wm = datetime(2026, 8, 12, 9, tzinfo=timezone.utc)
    store.set("audit", wm, "run1", datetime(2026, 8, 12, tzinfo=timezone.utc))
    got = store.get("audit")
    assert got == wm


def test_resolve_since_full_scan_ignores_watermark(tmp_path):
    store = FileStateStore(str(tmp_path))
    store.set("audit", datetime(2020, 1, 1, tzinfo=timezone.utc), "r", datetime(2026, 1, 1, tzinfo=timezone.utc))
    # full_scan -> fallback window, not the stored 2020 watermark
    since = resolve_since(store, "audit", lookback_days=2, full_scan=True)
    assert since.year >= 2026


def test_resolve_since_uses_watermark(tmp_path):
    store = FileStateStore(str(tmp_path))
    wm = datetime(2026, 8, 1, tzinfo=timezone.utc)
    store.set("audit", wm, "r", datetime(2026, 8, 12, tzinfo=timezone.utc))
    assert resolve_since(store, "audit", lookback_days=2, full_scan=False) == wm
