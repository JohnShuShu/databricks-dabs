"""Orchestrator entrypoint for the portable egress + API-usage monitor.

Runs the four detection surfaces against ANY Databricks account/workspace, in
either mode:

  --mode spark        In-workspace SparkSession. Queries via spark.sql, findings
                      appended to UC Delta tables. This is what the DAB job runs.
  --mode standalone   From a laptop/CI. Queries a SQL warehouse via databricks-sdk
                      (pointed at any workspace), findings written as CSV/JSON.
                      Needs an environment (profile/host+token) + a warehouse_id.

Environment selection: pass --registry + --environment to pull host/auth/
warehouse/output from a registry file (see environment.py), or pass the pieces
directly via flags / DBEGRESS_* env vars.

Each surface is isolated so one failure doesn't sink the others. Config
precedence: CLI args > environment registry entry > DBEGRESS_* env > defaults.
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import date, datetime, timezone

from . import api_usage, audit_query, lineage_query, notebook_scanner, sinks, state
from .config import Settings, get_settings
from .environment import Environment, build_client, get_environment
from .logging_setup import logging_setup
from .runners import SparkRunner, WarehouseRunner

logger = logging_setup(__name__)


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Portable Databricks egress + API-usage monitor")
    p.add_argument("--mode", choices=["spark", "standalone"], help="run mode")
    p.add_argument("--registry", help="path to environment registry (YAML/JSON)")
    p.add_argument("--environment", help="environment name within the registry")
    p.add_argument("--warehouse-id", help="SQL warehouse id (standalone mode)")
    p.add_argument("--catalog")
    p.add_argument("--schema")
    p.add_argument("--scan-root")
    p.add_argument("--lookback-days", type=int)
    p.add_argument("--csv-volume-path")
    p.add_argument("--output-dir", help="file-output dir (standalone mode)")
    p.add_argument("--surfaces", help="comma list: notebooks,audit,api_usage,lineage")
    p.add_argument("--full", action="store_true", help="ignore watermarks; full lookback")
    return p.parse_args(argv)


def _apply_environment(s: Settings, env: Environment) -> Settings:
    """Overlay a registry Environment onto Settings (env fills unset fields)."""
    return s.model_copy(update={
        "output_catalog": env.output_catalog,
        "output_schema": env.output_schema,
        "csv_volume_path": env.csv_volume_path or s.csv_volume_path,
        "output_dir": env.output_dir or s.output_dir,
        "scan_root": env.scan_root or s.scan_root,
        "warehouse_id": env.warehouse_id or s.warehouse_id,
    })


def build_settings(args: argparse.Namespace) -> tuple[Settings, Environment | None]:
    """Resolve Settings + optional Environment from args/registry/env/defaults."""
    s = get_settings()
    env: Environment | None = None

    if args.mode:
        s = s.model_copy(update={"mode": args.mode})
    registry_path = args.registry or s.registry_path
    env_name = args.environment or s.environment
    if registry_path and env_name:
        env = get_environment(registry_path, env_name)
        s = _apply_environment(s, env)
        # An environment implies standalone unless spark was explicitly chosen.
        if not args.mode and s.mode == "spark":
            s = s.model_copy(update={"mode": "standalone"})

    # CLI overrides win over everything.
    overrides: dict = {}
    if args.catalog:
        overrides["output_catalog"] = args.catalog
    if args.schema:
        overrides["output_schema"] = args.schema
    if args.scan_root:
        overrides["scan_root"] = args.scan_root
    if args.lookback_days is not None:
        overrides["lookback_days"] = args.lookback_days
    if args.csv_volume_path:
        overrides["csv_volume_path"] = args.csv_volume_path
    if args.output_dir:
        overrides["output_dir"] = args.output_dir
    if args.warehouse_id:
        overrides["warehouse_id"] = args.warehouse_id
    if args.surfaces:
        overrides["surfaces"] = args.surfaces
    if args.full:
        overrides["full_scan"] = True
    if overrides:
        s = s.model_copy(update=overrides)
    return s, env


def _build_backends(s: Settings, env: Environment | None):
    """Return (runner, sink, store, workspace_client) for the chosen mode."""
    if s.mode == "standalone":
        client = build_client(env) if env else build_client(Environment(name="cli"))
        if not s.warehouse_id:
            raise ValueError("standalone mode requires --warehouse-id (or env.warehouse_id)")
        runner = WarehouseRunner(client, s.warehouse_id)
        sink = sinks.FileSink(s.output_dir)
        store = state.FileStateStore(s.output_dir)
        return runner, sink, store, client

    # spark mode: ambient session + client.
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s.fq_schema}")
    client = build_client(env) if env else build_client(Environment(name="ambient"))
    runner = SparkRunner(spark)
    sink = sinks.SparkSink(spark, s.fq_schema, s.csv_dir)
    store = state.SparkStateStore(spark, s.fq_schema)
    return runner, sink, store, client


def _max_time(records, attr: str):
    times = [getattr(r, attr) for r in records if getattr(r, attr) is not None]
    return max(times) if times else None


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    s, env = build_settings(args)
    surfaces = s.enabled_surfaces()
    run_id = uuid.uuid4().hex
    run_ts = datetime.now(tz=timezone.utc)
    logger.info(
        "egress_monitor run %s | mode=%s | env=%s | schema=%s | surfaces=%s | full=%s",
        run_id, s.mode, (env.name if env else "-"), s.fq_schema, surfaces, s.full_scan,
    )

    runner, sink, store, client = _build_backends(s, env)
    summary: dict[str, int | str] = {}

    def run_surface(name: str, fn) -> None:
        if name not in surfaces:
            return
        try:
            summary[name] = fn()
        except Exception as exc:  # noqa: BLE001 - isolate surface failures
            logger.exception("surface %s failed: %s", name, exc)
            summary[name] = f"ERROR: {exc}"

    def _advance(surface: str, records, attr: str) -> None:
        newest = _max_time(records, attr)
        if newest:
            if isinstance(newest, date) and not isinstance(newest, datetime):
                newest = datetime(newest.year, newest.month, newest.day, tzinfo=timezone.utc)
            store.set(surface, newest, run_id, run_ts)

    def _notebooks() -> int:
        floor = None if s.full_scan else store.get("notebooks")
        findings = notebook_scanner.scan_notebooks(s, since=floor, ws=client)
        n = sink.write(sinks.NOTEBOOK_TABLE, findings, run_id, run_ts)
        _advance("notebooks", findings, "modified_at")
        return n

    def _audit() -> int:
        since = state.resolve_since(store, "audit", s.lookback_days, s.full_scan)
        events = audit_query.query_audit_egress(runner, since)
        n = sink.write(sinks.AUDIT_TABLE, events, run_id, run_ts)
        _advance("audit", events, "event_time")
        return n

    def _api_usage() -> int:
        since = state.resolve_since(store, "api_usage", s.lookback_days, s.full_scan)
        usage = api_usage.query_api_usage(runner, since, s.spike_baseline_days, s.spike_zscore)
        n = sink.write(sinks.API_USAGE_TABLE, usage, run_id, run_ts)
        _advance("api_usage", usage, "event_date")
        return n

    def _lineage() -> int:
        since = state.resolve_since(store, "lineage", s.lookback_days, s.full_scan)
        events = lineage_query.query_lineage_egress(runner, since)
        n = sink.write(sinks.LINEAGE_TABLE, events, run_id, run_ts)
        _advance("lineage", events, "event_time")
        return n

    run_surface("notebooks", _notebooks)
    run_surface("audit", _audit)
    run_surface("api_usage", _api_usage)
    run_surface("lineage", _lineage)

    logger.info("egress_monitor run %s complete | %s", run_id, summary)
    errored = [k for k, v in summary.items() if isinstance(v, str) and v.startswith("ERROR")]
    return 1 if errored else 0


if __name__ == "__main__":
    sys.exit(main())
