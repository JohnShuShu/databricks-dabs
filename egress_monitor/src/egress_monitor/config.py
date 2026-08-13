"""Settings for the egress monitor.

Env-var backed via pydantic-settings with the ``DBEGRESS_`` prefix (mirrors the
``PGA_``-prefixed pattern in DB-Admin-Console-Python-CL/pg_admin_web_py/config.py).
Every value also has a sane default so the job runs with zero configuration.

CLI parameters passed by the Databricks job (``--catalog``, ``--schema``, ...)
override these via ``settings_from_args`` in run.py. We deliberately do NOT read
custom ``spark.*`` conf keys: serverless / Spark Connect rejects them with
CONFIG_NOT_AVAILABLE (learned from mro_cost_dev_sync/src/sync_mro_cost.py).
"""

from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "DBEGRESS_"}

    # ── Run mode + environment selection (portability) ──────────────────
    # "spark"      -> in-workspace SparkSession + Delta tables (the DAB job).
    # "standalone" -> SQL warehouse via databricks-sdk + file output (any
    #                 account/workspace, from a laptop/CI). Requires a warehouse.
    mode: str = "spark"
    # Path to the environment registry (YAML/JSON) and the environment name to
    # use from it. When set (standalone), env values override the fields below.
    registry_path: str = ""
    environment: str = ""
    # Standalone-only: SQL warehouse to run queries on (may come from the env).
    warehouse_id: str = ""

    # ── Output location (Unity Catalog) ─────────────────────────────────
    # Delta tables land in <catalog>.<schema>.egress_*; the per-run CSV goes to
    # a UC volume derived from these unless csv_volume_path is set explicitly.
    output_catalog: str = "main"
    output_schema: str = "egress_monitor"
    # UC volume path for the per-run CSV mirror. Empty -> derived as
    # /Volumes/<catalog>/<schema>/egress_reports.
    csv_volume_path: str = ""
    # Standalone (file) output directory for CSV/JSON findings + state file.
    output_dir: str = "./egress_output"

    # ── Notebook scan ───────────────────────────────────────────────────
    # Recursive workspace path to scan. "/" = whole workspace.
    scan_root: str = "/"
    # Skip a single exported notebook larger than this (bytes) to bound memory.
    max_notebook_bytes: int = 5_000_000
    # Chars of context captured around a pattern hit in the finding snippet.
    snippet_chars: int = 200

    # ── System-table window ─────────────────────────────────────────────
    # Full (non-incremental) lookback in days for audit/lineage queries. On an
    # incremental run the watermark from egress_scan_state takes precedence.
    lookback_days: int = 2
    # Trailing window (days) used to compute the per-user/action baseline for
    # API-usage spike detection.
    spike_baseline_days: int = 14
    # Flag an API-usage day as a spike when its z-score exceeds this.
    spike_zscore: float = 3.0

    # ── Run behavior ────────────────────────────────────────────────────
    # Force a full (non-incremental) scan, ignoring stored watermarks.
    full_scan: bool = False
    # Comma-separated surfaces to run; empty -> all four.
    # Valid: notebooks, audit, api_usage, lineage
    surfaces: str = ""

    # ── Derived helpers ─────────────────────────────────────────────────
    @property
    def fq_schema(self) -> str:
        """Fully-qualified <catalog>.<schema> prefix for output tables."""
        return f"{self.output_catalog}.{self.output_schema}"

    @property
    def csv_dir(self) -> str:
        """UC volume directory for the per-run CSV mirror."""
        if self.csv_volume_path:
            return self.csv_volume_path.rstrip("/")
        return f"/Volumes/{self.output_catalog}/{self.output_schema}/egress_reports"

    def enabled_surfaces(self) -> list[str]:
        """Return the surfaces to run (all four if unset)."""
        all_surfaces = ["notebooks", "audit", "api_usage", "lineage"]
        if not self.surfaces.strip():
            return all_surfaces
        requested = [s.strip() for s in self.surfaces.split(",") if s.strip()]
        unknown = [s for s in requested if s not in all_surfaces]
        if unknown:
            raise ValueError(
                f"Unknown surface(s) {unknown}; valid: {all_surfaces}"
            )
        return requested


settings = Settings()


def get_settings() -> Settings:
    """Return the module Settings singleton."""
    return settings
