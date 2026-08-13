"""Environment registry: describe every account/workspace the monitor runs in.

The monitor is portable across Databricks accounts and workspaces. An
``Environment`` names one target (a workspace + how to auth + where to write),
and a registry file (YAML or JSON) holds many of them so you can point the same
code at any environment by name.

Auth precedence for ``build_client`` (all optional; first present wins):
  1. ``profile``               -> WorkspaceClient(profile=...) reads ~/.databrickscfg
  2. ``host`` + ``token``      -> WorkspaceClient(host=..., token=...) (token may be
                                  an env-var reference like "env:DBX_TOKEN_ACME")
  3. nothing                   -> WorkspaceClient() ambient (in-workspace / SP)

This mirrors the profile-based auth the team already uses
(datafabric_dabs: `databricks auth login --profile ...`) while also supporting
host+token for CI / laptops without a configured profile.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class Environment:
    """One account/workspace target for the monitor."""

    name: str
    # ── auth (choose one style; see build_client) ───────────────────────
    profile: str | None = None          # ~/.databrickscfg profile
    host: str | None = None             # workspace URL, e.g. https://dbc-xxxx.cloud.databricks.com
    token: str | None = None            # PAT, or "env:VAR_NAME" to read from env
    # ── where to run SQL in standalone mode ─────────────────────────────
    warehouse_id: str | None = None     # SQL warehouse for StatementExecution
    # ── where findings go ───────────────────────────────────────────────
    output_catalog: str = "main"
    output_schema: str = "egress_monitor"
    csv_volume_path: str = ""           # UC volume dir; "" -> derived (spark mode)
    output_dir: str = ""                # local dir for file sink (standalone mode)
    # ── what to scan ────────────────────────────────────────────────────
    scan_root: str = "/"
    # ── free-form notes (surfaced to operators / LLMs) ──────────────────
    labels: dict[str, str] = field(default_factory=dict)

    @property
    def fq_schema(self) -> str:
        return f"{self.output_catalog}.{self.output_schema}"

    def resolve_token(self) -> str | None:
        """Resolve ``token``, expanding an ``env:VAR`` reference."""
        if not self.token:
            return None
        if self.token.startswith("env:"):
            return os.environ.get(self.token[4:])
        return self.token


def load_registry(path: str | Path) -> dict[str, Environment]:
    """Load a registry file (``.yaml``/``.yml``/``.json``) into name->Environment.

    Expected shape::

        environments:
          acme-prod:
            profile: acme-prod
            warehouse_id: abc123
            output_catalog: acme_gov
            output_schema: observability
          beta-dev:
            host: https://dbc-yyyy.cloud.databricks.com
            token: env:DBX_TOKEN_BETA
            warehouse_id: def456
    """
    p = Path(path)
    raw = p.read_text()
    if p.suffix in (".yaml", ".yml"):
        import yaml  # local import: only needed for YAML registries

        data: dict[str, Any] = yaml.safe_load(raw) or {}
    else:
        data = json.loads(raw)

    envs_block = data.get("environments", data)
    registry: dict[str, Environment] = {}
    for name, cfg in envs_block.items():
        registry[name] = Environment(name=name, **(cfg or {}))
    return registry


def get_environment(path: str | Path, name: str) -> Environment:
    """Load ``name`` from the registry file, erroring clearly if absent."""
    registry = load_registry(path)
    if name not in registry:
        available = ", ".join(sorted(registry)) or "(none)"
        raise KeyError(f"environment '{name}' not in {path}; available: {available}")
    return registry[name]


def build_client(env: Environment):
    """Build a databricks-sdk WorkspaceClient for ``env`` (auth precedence above)."""
    from databricks.sdk import WorkspaceClient

    if env.profile:
        return WorkspaceClient(profile=env.profile)
    if env.host and env.token:
        return WorkspaceClient(host=env.host, token=env.resolve_token())
    if env.host:
        # host given without token -> let the SDK resolve a default cred chain
        # (env vars, CLI cache) for that host.
        return WorkspaceClient(host=env.host)
    return WorkspaceClient()
