# egress_monitor

Portable Databricks **data-egress & API-usage monitor**. Detects data leaving a
workspace to outside sources (notebook code scan + `system.access.audit` +
`system.access.table_lineage`) and profiles API usage per user/service.

Runs two ways from one codebase:
- **standalone** — from a laptop/CI against **any account/workspace** via a SQL
  warehouse; findings as CSV/JSON.
- **spark** — in-workspace serverless DAB job; findings as UC Delta tables.

## 📖 Everything is in one file: [`EGRESS_MONITOR.md`](./EGRESS_MONITOR.md)

That single self-contained reference has the architecture, **all SQL verbatim**,
verified system-table schemas, the config/environment reference, per-account
onboarding, grants, operating commands, and an LLM operator guide. It's designed
to be copied between environments and handed to an assistant for full context.

## Quick start (standalone)
```bash
pip install -e '.[dev]'                          # -> `egress-monitor` console script + tests
cp environments.example.yaml environments.yaml   # fill in your workspace + warehouse
egress-monitor --registry environments.yaml --environment <name> --full
pytest                                            # 45 tests (config in pyproject.toml)
```

**Not deployed** — deploy steps are in `EGRESS_MONITOR.md` §8, gated on your go-ahead.
