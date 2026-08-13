# EGRESS_MONITOR — Single-File Reference

> **Purpose of this file.** One self-contained document describing the Databricks
> egress & API-usage monitor: what it does, how it's built, every SQL query it
> runs, the exact system-table schemas it depends on, how to configure it for
> **any account/workspace**, and how to operate it. It is written to be copied
> between environments and handed to an LLM assistant so it has full context in
> one place. If you only read one file, read this one.
>
> **Status:** built and unit-tested (45 tests green); the SQL is verified against
> a live workspace. **Not deployed.** Deploy steps are documented but gated on
> your go-ahead + auth.

---

## 1. What it does & why

Detects and tracks **data leaving a Databricks workspace to outside sources**,
and profiles **API usage** per user/service. It runs four complementary
detection surfaces:

| Surface | Source | Catches |
|---------|--------|---------|
| **Notebook code scan** | Workspace API (export SOURCE) | egress *intent in code*: `requests`/`httpx`/`urllib3`, `boto3`/`s3://`, external JDBC/Snowflake, SFTP/SMTP, `dbutils.notebook.exit`, presigned URLs |
| **Audit egress** | `system.access.audit` | actual events: `workspaceExport`, `createDownloadUrl`, result downloads, temp-credential vending, `getForeignCredentials`, token logins |
| **API-usage profiling** | `system.access.audit` | per-user/service/action daily call counts + z-score **spike** flag |
| **Lineage egress** | `system.access.table_lineage` | external-path (`PATH`) reads/writes (volumes, workspace files, dbfs, external S3) |

### The key design fact (do not skip)

`system.access.outbound_network` — the table that would log a notebook doing
`requests.post("https://external", data=df)` — was **empty** in the workspace
this was built against, because **serverless egress logging was not enabled**.
So system tables **cannot** see network egress from code there. That is why the
**notebook code scan exists and is load-bearing**: it is the only surface that
catches `requests`-style egress until/unless outbound network logging is turned
on. Verify this per-environment (§7) — if `outbound_network` has rows in your
environment, consider adding a fifth surface querying it.

---

## 2. Two run modes (this is the portability story)

The same code runs two ways. Pick per environment.

### Mode A — `spark` (in-workspace, the DAB job)
Runs **inside** a workspace as a serverless Spark job. Queries via `spark.sql`;
writes findings to **Unity Catalog Delta tables**. Auth is **ambient** (the job's
service principal). This is the scheduled/automated path.

### Mode B — `standalone` (from anywhere, any workspace)
Runs from a **laptop or CI** and points at **any account/workspace**. Queries a
**SQL warehouse** via the databricks-sdk Statement Execution API; scans notebooks
via the Workspace API; writes findings as **CSV + JSON files**. Auth via a
`~/.databrickscfg` profile or `host`+`token`. **This is what makes it portable
across accounts** — you never deploy anything into the target workspace; you just
need a warehouse id and read access.

```
                    ┌─────────────── run.py (orchestrator) ───────────────┐
                    │  picks mode → builds runner + sink + state store      │
                    └───────────────────────────────────────────────────────┘
        mode=spark  │                                   │  mode=standalone
                    ▼                                   ▼
     SparkRunner (spark.sql)              WarehouseRunner (SQL Statement Exec API)
     SparkSink   (Delta + CSV vol)        FileSink        (CSV + JSON files)
     SparkStateStore (Delta table)        FileStateStore  (JSON watermark file)
                    │                                   │
                    └──────── same 4 query/scan modules ─────────┘
              audit_query · api_usage · lineage_query · notebook_scanner
```

Every query/scan module takes an abstract **`runner`** and returns dataclass
records. The runner and sink are the only things that differ between modes, so
the detection logic and SQL are identical everywhere.

---

## 3. Layout & module contracts

Repo path: `datafabric_dabs/bundles/egress_monitor/` (Python 3.10+; ~1540 LOC).

```
pyproject.toml                    packaging: src layout, `egress-monitor` console script,
                                  deps, pytest config (pythonpath=src). Install: pip install -e .
databricks.yml                    DAB: portable per-target variables (host, SP, catalog…)
resources/egress_monitor.job.yml  daily serverless job -> run.py --mode spark
environments.example.yaml         registry template for standalone (copy -> environments.yaml)
requirements.txt                  pinned convenience list (mirrors pyproject deps + pytest)
EGRESS_MONITOR.md                 << this file
README.md                         short pointer to this file
src/egress_monitor/
  run.py            orchestrator: parse args → resolve Settings(+Environment) →
                    build backends for the mode → run 4 surfaces (isolated) →
                    write + advance watermarks. Entry: `python -m egress_monitor.run`
  config.py         Settings(BaseSettings, env_prefix="DBEGRESS_") + get_settings().
                    Holds mode, registry_path, environment, warehouse_id, output_*,
                    scan_root, lookback_days, spike_*, surfaces, full_scan. Derived:
                    fq_schema, csv_dir, enabled_surfaces().
  environment.py    Environment dataclass + load_registry()/get_environment() (YAML/JSON)
                    + build_client() (profile | host+token | ambient). token may be
                    "env:VAR". This is the multi-account registry.
  runners.py        SqlRunner protocol; SparkRunner (spark.sql) & WarehouseRunner
                    (Statement Exec API, chunk-paged). Both return list[dict].
                    Coercion helpers as_dt/as_date/as_int/as_float/as_bool/as_str
                    (warehouse returns strings; Spark returns typed → one map path).
  patterns.py       EgressPattern catalog (name, compiled regex, category, severity)
                    + scan_line(). Heuristics: intent, not proof.
  notebook_scanner.py  scan_notebooks(settings, since, ws): workspace.list(recursive)
                    → export SOURCE → decode → cell/line-aware scan_line. Per-notebook
                    error isolation; incremental via `since` (modified_at floor).
  audit_query.py    build_audit_sql(since) [pure] + query_audit_egress(runner, since).
                    EGRESS_ACTIONS maps action_name → category.
  api_usage.py      build_api_usage_sql(since,baseline_days,z) [pure] +
                    query_api_usage(runner,…). Daily counts + baseline + spike flag.
  lineage_query.py  build_lineage_sql(since) [pure] + query_lineage_egress(runner,since).
                    Excludes internal __unitystorage paths (managed-Delta noise).
  records.py        dataclasses NotebookFinding/AuditEgressEvent/ApiUsageDaily/
                    LineageEgress, each with to_dict().
  sinks.py          TABLE_COLUMNS (canonical column order); SparkSink (Delta+CSV),
                    FileSink (CSV+JSON). Stamp run_id/run_ts on every row.
  spark_schemas.py  Explicit Spark StructTypes (imported lazily by SparkSink only,
                    so the standalone path never needs pyspark).
  state.py          SparkStateStore (Delta MERGE) & FileStateStore (JSON) + resolve_since.
  logging_setup.py  logging_setup(name) factory.
tests/              test_patterns.py, test_queries.py, test_portable.py (no Spark)
```

**Config precedence** (lowest → highest): defaults → `DBEGRESS_*` env vars →
environment registry entry → CLI flags.

---

## 4. All SQL, verbatim

These are the exact statements the runners execute (with a real `since` bound).
They were validated against a live workspace via a SQL warehouse. `{since}` =
`YYYY-MM-DD`, `{since_ts}` = `YYYY-MM-DD HH:MM:SS`.

### 4.1 Audit egress (`audit_query.build_audit_sql`)
```sql
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
WHERE event_date >= DATE'{since}'
  AND event_time >= TIMESTAMP'{since_ts}'
  AND action_name IN (
    'workspaceExport', 'createDownloadUrl', 'getResults', 'downloadQueryResult',
    'generateTemporaryTableCredential', 'generateTemporaryVolumeCredential',
    'getForeignCredentials', 'tokenLogin', 'oidcTokenAuthorization'
  );
```
`event_date` (partition col) is filtered first for pruning, then `event_time`.
`action_name → category` map (in code): `workspaceExport`→`workspace_export`;
`createDownloadUrl`→`file_download`; `getResults`/`downloadQueryResult`→
`result_download`; `generateTemporary*Credential`/`getForeignCredentials`→
`credential_vend`; `tokenLogin`/`oidcTokenAuthorization`→`api_token_auth`.

### 4.2 API-usage daily + spike (`api_usage.build_api_usage_sql`)
```sql
WITH daily AS (
    SELECT event_date, user_identity.email AS email, service_name, action_name,
           COUNT(*) AS call_count
    FROM system.access.audit
    WHERE event_date >= DATE'{since}' - INTERVAL {baseline_days} DAYS
    GROUP BY event_date, user_identity.email, service_name, action_name
),
stats AS (
    SELECT email, service_name, action_name,
           AVG(call_count) AS baseline_mean, STDDEV(call_count) AS baseline_stddev
    FROM daily GROUP BY email, service_name, action_name
)
SELECT
    d.event_date,
    COALESCE(d.email, 'unknown')     AS email,
    COALESCE(d.service_name, '')     AS service_name,
    COALESCE(d.action_name, '')      AS action_name,
    d.call_count,
    COALESCE(s.baseline_mean, 0.0)   AS baseline_mean,
    COALESCE(s.baseline_stddev, 0.0) AS baseline_stddev,
    CASE WHEN COALESCE(s.baseline_stddev,0.0) > 0
         THEN (d.call_count - s.baseline_mean) / s.baseline_stddev ELSE 0.0 END AS zscore,
    CASE WHEN COALESCE(s.baseline_stddev,0.0) > 0
         AND (d.call_count - s.baseline_mean) / s.baseline_stddev > {zscore}
         THEN TRUE ELSE FALSE END AS is_spike
FROM daily d
LEFT JOIN stats s
  ON d.email <=> s.email AND d.service_name <=> s.service_name AND d.action_name <=> s.action_name
WHERE d.event_date >= DATE'{since}';
```
The baseline window extends `baseline_days` **before** `{since}` so early reported
days still have history. `<=>` (null-safe equals) keeps null-email rows joined.

### 4.3 Lineage egress (`lineage_query.build_lineage_sql`)
```sql
SELECT
    event_time, created_by, entity_type, source_type,
    COALESCE(source_path, source_table_full_name) AS source,
    target_type,
    COALESCE(target_path, target_table_full_name) AS target
FROM system.access.table_lineage
WHERE event_date >= DATE'{since}'
  AND event_time >= TIMESTAMP'{since_ts}'
  AND (
        (source_type = 'PATH' AND source_path NOT LIKE '%__unitystorage%')
     OR (target_type = 'PATH' AND target_path NOT LIKE '%__unitystorage%')
  );
```
The `__unitystorage` exclusion drops managed-Delta backing-storage I/O, which is
normal internal traffic — without it, real findings drown in noise. Direction is
derived in code: `PATH` target ⇒ `write_external`, else `read_external`.

### 4.4 Environment probe queries (run these first in a new environment — §7)
```sql
-- Are the tables populated? (outbound_network is often empty)
SELECT 'audit_7d'    AS t, count(*) c FROM system.access.audit            WHERE event_date >= current_date() - INTERVAL 7 DAYS
UNION ALL SELECT 'outbound_7d', count(*) FROM system.access.outbound_network WHERE event_time >= current_timestamp() - INTERVAL 7 DAYS
UNION ALL SELECT 'lineage_7d',  count(*) FROM system.access.table_lineage   WHERE event_time >= current_timestamp() - INTERVAL 7 DAYS;

-- Which egress actions actually occur here?
SELECT service_name, action_name, count(*) c FROM system.access.audit
WHERE event_date >= current_date() - INTERVAL 7 DAYS
  AND action_name IN ('workspaceExport','createDownloadUrl','getResults','downloadQueryResult',
        'generateTemporaryTableCredential','generateTemporaryVolumeCredential','getForeignCredentials')
GROUP BY service_name, action_name ORDER BY c DESC;
```

---

## 5. Verified system-table schemas

Confirmed live via `system.information_schema.columns`. Depend on these; if a
column is missing in your environment the query will error clearly.

**`system.access.audit`** — `account_id, workspace_id, version, event_time
TIMESTAMP, event_date DATE, source_ip_address, user_agent, session_id,
user_identity STRUCT (→ .email), service_name, action_name, request_id,
request_params MAP, response STRUCT, audit_level, event_id, identity_metadata
STRUCT`.
- On Spark, `request_params` is a MAP; via the warehouse API it comes back as a
  JSON string. `_summarize_params()` handles both.

**`system.access.table_lineage`** — `account_id, metastore_id, workspace_id,
entity_type, entity_id, entity_run_id, source_table_full_name, source_table_catalog,
source_table_schema, source_table_name, source_path, source_type,
target_table_full_name, target_table_catalog, target_table_schema,
target_table_name, target_path, target_type, created_by, event_time TIMESTAMP,
event_date DATE, record_id, event_id, statement_id, entity_metadata STRUCT,
direct_access BOOLEAN`.
- `source_type`/`target_type` observed values: `TABLE, VIEW, MATERIALIZED_VIEW,
  STREAMING_TABLE, PATH` (and `NULL` when one side isn't recorded). Only `PATH`
  is treated as external.

**`system.access.outbound_network`** (schema present, was **empty**) —
`account_id, workspace_id, destination_type, destination, dns_event STRUCT,
storage_event STRUCT, event_time TIMESTAMP, access_type, event_id,
network_source_type`.

---

## 6. Output schema (what lands where)

Same logical tables in both modes. Every row carries `run_id` (uuid4 hex) +
`run_ts` (UTC). Mode A → Delta tables in `<catalog>.<schema>`; Mode B →
`<output_dir>/<table>/<run_id>.csv` and `.json`.

| Table | Columns |
|-------|---------|
| `egress_notebook_findings` | path, language, object_id, modified_at, cell_no, line_no, category, pattern_name, severity, snippet, run_id, run_ts |
| `egress_audit_events` | event_time, email, service_name, action_name, category, source_ip_address, user_agent, request_id, request_summary, run_id, run_ts |
| `api_usage_daily` | event_date, email, service_name, action_name, call_count, baseline_mean, baseline_stddev, zscore, is_spike, run_id, run_ts |
| `egress_lineage` | event_time, email, entity_type, source_type, source, target_type, target, direction, run_id, run_ts |
| `egress_scan_state` | surface, last_event_time, run_id, run_ts (watermark; Delta table in Mode A, `egress_scan_state.json` in Mode B) |

**Incremental:** each surface resumes from its `egress_scan_state` watermark.
`--full` ignores watermarks and re-scans the whole `lookback_days` window.

---

## 7. Onboarding a NEW account / workspace

### Mode B (standalone) — recommended for working across many accounts
1. **Auth to the target.** Either `databricks auth login --profile <name>`, or
   have a PAT (`export DBX_TOKEN_X=dapi…`).
2. **Get a SQL warehouse id:** `databricks warehouses list -p <profile>`.
3. **Probe the environment** (§4.4) on that warehouse — confirm `audit`/`lineage`
   have rows; note whether `outbound_network` is populated; see which egress
   actions occur. This tells you which surfaces are worthwhile.
4. **Add a registry entry** in `environments.yaml` (copy from
   `environments.example.yaml`):
   ```yaml
   environments:
     newco-prod:
       profile: newco-prof          # OR host: + token: env:DBX_TOKEN_X
       warehouse_id: "<id>"
       output_catalog: <cat>        # only used if you later switch to spark mode
       output_schema: observability
       output_dir: ./egress_output/newco-prod
       scan_root: /                 # narrow to limit scope, e.g. /Workspace/Shared
   ```
5. **Install once, then run:**
   ```bash
   pip install -e .   # from the bundle root; puts `egress-monitor` on PATH
   egress-monitor --registry environments.yaml --environment newco-prod --full
   # findings under ./egress_output/newco-prod/<table>/<run_id>.{csv,json}
   ```
   Access needed: `SELECT` on `system.access.audit` + `table_lineage`, warehouse
   `CAN_USE`, and workspace read/export on `scan_root`.

### Mode A (in-workspace DAB) — for a scheduled job in a workspace you own
Add a `targets:` block in `databricks.yml` (a commented template is included):
```yaml
targets:
  newco:
    workspace: { host: ${var.workspace_host}, root_path: /Workspace/Shared/.bundle/${bundle.name}/${bundle.target} }
    run_as:    { service_principal_name: ${var.service_principal_name} }
    variables:
      workspace_host: https://dbc-XXXX.cloud.databricks.com
      service_principal_name: <sp-application-id>
      output_catalog: <catalog>
      trigger_pause_status: PAUSED
```
Then (see §8) validate/deploy/run. The SP needs, in that workspace: `SELECT` on
the two system tables, `USE CATALOG/SCHEMA` + `CREATE TABLE`/`MODIFY` on the
output schema (+ the UC volume for CSV), and workspace read/export on `scan_root`.

**Grants example** (run as a metastore/catalog admin):
```sql
GRANT SELECT ON TABLE system.access.audit         TO `<principal>`;
GRANT SELECT ON TABLE system.access.table_lineage TO `<principal>`;
GRANT USE CATALOG ON CATALOG <catalog>            TO `<principal>`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA <catalog>.<schema> TO `<principal>`;
```

---

## 8. Operating it

### Install
```bash
# editable install from the bundle root -> `egress-monitor` on PATH, plus the
# `python -m egress_monitor.run` module entry. -e points at src/ so edits are live.
pip install -e '.[dev]'      # or: pip install -r requirements.txt (no install/entry point)
```
`pyproject.toml` uses a `src/` layout, declares the runtime deps (databricks-sdk,
pydantic-settings, pyyaml), a `dev` extra (pytest), a console script
`egress-monitor = egress_monitor.run:main`, and pytest config (`pythonpath=src`,
`testpaths=tests`) so `pytest` works from the bundle root with no shim.
**`pyspark` is intentionally NOT a dependency** — the runtime provides it in
spark mode, and standalone never imports it (SparkSink/spark_schemas import it
lazily).

### Standalone (any workspace)
```bash
# after `pip install -e .` the console script and module entry are equivalent:
egress-monitor --registry environments.yaml --environment <name> --full         # full scan
egress-monitor --registry environments.yaml --environment <name> --surfaces audit,lineage
python -m egress_monitor.run --registry environments.yaml --environment <name>   # equivalent
# ad-hoc without a registry (profile from ~/.databrickscfg via SDK env, or host+token env)
DBEGRESS_MODE=standalone egress-monitor --warehouse-id <id> --output-dir ./out
```

### In-workspace DAB
```bash
cd datafabric_dabs/bundles/egress_monitor
databricks bundle validate -t dev -p <profile>
databricks bundle deploy   -t dev -p <profile>   # deploys PAUSED
databricks bundle run egress_monitor -t dev -p <profile>
# after verifying, set trigger_pause_status: UNPAUSED and redeploy
```

### CLI / env flags
`--mode {spark,standalone}` · `--registry` · `--environment` · `--warehouse-id` ·
`--catalog` · `--schema` · `--scan-root` · `--lookback-days` · `--csv-volume-path` ·
`--output-dir` · `--surfaces` · `--full`. Env equivalents: `DBEGRESS_MODE`,
`DBEGRESS_WAREHOUSE_ID`, `DBEGRESS_OUTPUT_CATALOG`, `DBEGRESS_SCAN_ROOT`,
`DBEGRESS_SPIKE_ZSCORE`, `DBEGRESS_SURFACES`, etc.

### Testing
```bash
pip install -e '.[dev]'
pytest            # 45 tests; pyproject sets pythonpath=src + testpaths=tests, so run from bundle root
```

---

## 9. Egress pattern catalog (notebook scan)

Each is `(name, regex, category, severity)`; a line matching any is a finding.
Severity: **high** = direct outbound transfer; **medium** = credential/URL
material or result exfil; **low** = weak signal.

| category | patterns (names) | sev |
|----------|------------------|-----|
| http_client | requests_import, requests_call, httpx_use, aiohttp_use, urllib3_use | high |
| http_client | urllib_request, http_client_lib | medium |
| http_client | urllib_import | low |
| cloud_sdk | boto3_use, s3_put, gcs_sdk, azure_sdk | high |
| external_uri | s3_uri, gcs_uri, azure_blob_uri | medium |
| external_uri | external_http_literal (excludes *.databricks.com/localhost) | low |
| external_db | jdbc_write, jdbc_url, snowflake_connector | high |
| external_db | pg_connector (psycopg2/pymysql/cx_Oracle/oracledb) | medium |
| file_transfer | sftp_paramiko, ftp_lib | high |
| mail | smtp_send | high |
| result_exfil | notebook_exit | medium |
| presigned_url | presigned_url (generate_presigned_url / createDownloadUrl) | medium |

These are **leads for review, not proof of egress.** Tune `patterns.py` per
environment (e.g. allow an approved internal API host).

---

## 10. LLM operator guide (read this if you're an assistant working here)

- **Ground truth is live, not this doc.** Before proposing changes for a new
  environment, run the §4.4 probes on that environment's warehouse (the user can
  do this via an MCP SQL tool or `databricks sql`). `outbound_network` being
  empty vs populated changes whether the notebook scan is essential or
  supplementary.
- **Don't invent columns.** The schemas in §5 are verified; if you need another
  field, confirm it in `system.information_schema.columns` first.
- **Two modes, one logic.** If you change a query, change only the `build_*_sql`
  function — both runners use it. Keep value handling in the `as_*` coercers
  (warehouse returns strings; Spark returns typed).
- **Secrets:** never write a PAT into `environments.yaml`; use `token: env:VAR`.
  `environments.yaml`, `environments.json`, and `egress_output/` are gitignored.
- **Heuristic honesty:** notebook findings and lineage `PATH` hits are signals,
  not confirmed exfiltration. Say so when reporting.
- **Adding a surface:** add a `build_*_sql` + `query_*` module returning a
  dataclass (give it `to_dict()` in `records.py`), a column list in
  `sinks.TABLE_COLUMNS`, a Spark schema in `spark_schemas.SCHEMAS`, and wire a
  `_surface()` fn + name into `run.py` and `Settings.enabled_surfaces()`.
- **Packaging:** `pyproject.toml` (src layout, setuptools). `pip install -e .`
  gives the `egress-monitor` console script; `pytest` reads config from
  `pyproject.toml`. Don't add `pyspark` as a dep — it's runtime-provided in
  spark mode and lazily imported, so standalone stays Spark-free.
- **This is NOT deployed.** Deploying is gated on the user's explicit go-ahead
  and valid auth. Validation currently fails only on an expired token, not config.

---

## 11. Provenance / assumptions

- Built against GE DataFabric dev workspace `dbc-2ff2d226-c0da.cloud.databricks.com`,
  warehouse `e9781459f2639f5e` (Serverless X-Large). Conventions mirror the
  sibling `mro_cost_dev_sync` bundle (serverless job, SP `run_as`, dev/prod
  targets) and team patterns (pydantic-settings config, `logging_setup` factory).
- Serverless rejects custom `spark.*` conf keys (`CONFIG_NOT_AVAILABLE`), so
  config is passed as CLI argv / env, never via `spark.conf`.
- Live observation that shaped the design: most `table_lineage` `PATH` rows were
  internal `__unitystorage` managed-storage → excluded as noise; the remaining
  external paths are UC volumes / workspace files / dbfs / external S3.
```
