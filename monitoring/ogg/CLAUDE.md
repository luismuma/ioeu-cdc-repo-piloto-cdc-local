# CLAUDE.md — OGG Monitoring Report

Queries every Oracle GoldenGate MA (Microservices Architecture) deployment via REST API
and renders a self-contained HTML report. Key files:

- `ogg_report.py` — main script (API queries + Jinja2 render)
- `db_metadata.py` — Oracle table metadata via python-oracledb (optional)
- `templates/report.html.j2` — up to six-tab dark-blue HTML report
- Inventory lives at `../../cdc-controller/config/inventory_deployments/inventory.yaml`

---

## Auth

### OGGClient (from oggctl `api/client.py`)
- `OGGClient` is imported from `../../ogg-ctl/api/client.py` at runtime
- `_auth_mode = "probing"` if `OGG_ADMIN_PASSWORD` is set → tries Basic first, falls back to Entra ID bearer on 401
- `_auth_mode = "bearer"` if no password → goes straight to Entra ID

### Credentials (env vars)
- `OGG_ADMIN_USER` — Basic auth username (default: `"oggadmin"` if unset or empty)
- `OGG_ADMIN_PASSWORD` — Basic auth password
- Entra ID config lives **per-deployment** in the inventory (`entraid_tenant_id`, `entraid_client_id_dev/pre/pro`, `entraid_client_secret`)
- `entraid_client_secret` holds the **env var name**, not the secret value itself

### Startup log is per-deployment reality
`_has_entra` is true if ANY deployment in the inventory has Entra ID config — the log
reflects the actual auth mode used, not a fixed global config.

---

## Non-obvious API behaviour

### Health endpoint
- URL: `GET /services/v2/config/health`
- Response key is `criticalResources`, not `services` or `items`
- Covers **all deployments** on the Service Manager — filter by `deploymentName == dep_name`

### Stats — STATS via POST
Stats are collected by sending a command to the process, not by reading a pmsrvr metric.
**Never use the direct adminsrvr URL** (`/services/{dep_name}/adminsrvr/v2/...`) — it causes ~60s hangs.

```
POST {admin_url}/services/v2/extracts/{name}/command   {"command":"STATS","arguments":"DAILY"}
POST {admin_url}/services/v2/replicats/{name}/command  {"command":"STATS","arguments":"TOTAL"}
```

Response: `response.replyData.stats[]`
- Skip entries with `"ddl": true`
- `sourceTable` comes as 3-part `MP01.SCHEMA.TABLE` — strip the first segment when there are 2+ dots
- Some entries have no `"operations"` key (zero activity) — handle gracefully
- `DAILY` = since midnight; `TOTAL` = since process start — no reset needed

**Note on REPORTCDR**: the `REPORTCDR` argument adds CDR (Conflict Detection and Resolution)
counters to the STATS response. These are not parsed or displayed by this script. It is not used
because CDR is not configured in this deployment, and for coordinated replicats `REPORTCDR`
may change the response structure in unpredictable ways per thread.

### Process endpoints
```
GET /services/v2/extracts          → list of extracts
GET /services/v2/replicats         → list of replicats
GET /services/v2/sources           → distribution paths
GET /services/v2/targets           → receiver paths
```
Response items are under `response.items[]`.

---

## Oracle DB metadata (`db_metadata.py`)

Optional — only runs for `db_engine: ORACLE` deployments when `db_connections.yaml` is available.

### Flow
1. `get_extract_params(client, name)` — fetches the PRM file via OGG API
2. `parse_prm(lines)` — extracts `useridalias`, `sourcecatalog`, `tables[]`, `table_tags{}`
3. `find_connection(db_conns, useridalias)` — looks up by `conn_name_prim` in db_connections.yaml
4. `get_oracle_password(credential, env, region)` — env var → AWS SM fallback
5. `get_table_metadata(conn_string, ...)` — queries `DBA_SEGMENTS` + `DBA_TAB_STATISTICS`

### db_key
`load_db_connections` stores the YAML top-level key (e.g. `"ACNPDB_PRO"`) as `db_key` in each entry.
This is what the Tablas tab shows in the BBDD column — more specific than just `"ORACLE"`.

### Connection selection
- CDC extracts → `conn_string_prim`
- Initial Load extracts with standby → `conn_string_stby` (reduces load on primary)

### Password resolution order
1. Env var named after `password_credential` (e.g. `ORACLE_ACNPDB_GGADMIN`)
2. `ORACLE_DB_PASSWORD` (global fallback)
3. AWS Secrets Manager `CDC-VAULT-{ENV}` JSON field

### Thick mode
Set `ORACLE_CLIENT_LIB=<path>` to use Oracle Instant Client thick mode (required for old 10g DES verifiers — DPY-3015 error in thin mode).

---

## PRM parsing

### Comments
- Lines starting with `--` or `/*` are skipped entirely (including `--TABLE` commented-out tables)

### TAGS inline comments
PRM TABLE/MAP lines can carry JSON metadata after `-- TAGS`:
```
TABLE SCHEMA.TABLE_NAME;  -- TAGS {"requested_by":"TeamName","cost_center":"CC001","date":"2026-01"}
MAP SCHEMA.TABLE_NAME, TARGET SCHEMA.TABLE_NAME;  -- TAGS {"requested_by":"TeamName","cost_center":"CC001"}
```
Applies to both extract PRM (TABLE lines) and replicat PRM (MAP lines) — REPL_INI processes included.
Parsed per-table into `table_tags[TABLE_UPPER]` dict.
Propagated to stats rows as `team` (from `requested_by`) and `cost_center`.
Shown as colour badges in the Tabla cell in the HTML report.
Used to populate the Consumo tab aggregation by team.

---

## Zero-DML rows for stopped extracts

When an extract is STOPPED (or running but idle), STATS returns empty.
The report still shows all tables by:
1. Always parsing the PRM (Step 1 in `_query_extract_db_meta`) regardless of STATS result
2. Adding zero-DML placeholder rows for every PRM/Oracle table not in STATS output

---

## Data flow

```
collect_deployment()
  ├── query_process_stats()     → rows (may be empty)
  ├── _query_extract_db_meta()  → prm_tables, oracle_meta, table_tags, db_key
  │     ├── get_extract_params() + parse_prm()   [always, Oracle extracts/replicats only]
  │     └── get_table_metadata()                 [optional, requires db_conns]
  ├── zero-DML rows for tables in PRM/meta not in stats
  └── enrichment loop: adds bbdd, db_engine, team, cost_center, proc context

build_report_data()
  ├── hosts        — one per unique host
  ├── deployments  — grouped process counts by tipo
  ├── all_procs    — flat list, all EXTRACTs/REPLICATs/paths
  └── stats_rows   — flat list, sorted by env → bbdd → tipo → estado → tabla
        └── size_mb propagation: REPL_INI/REPL_CDC rows without size_mb get it
            from a lookup dict built from matching EXTR_INI/EXTR_CDC rows

build_ini_load_data(stats_rows, all_procs)          [only when INI processes exist]
  ├── _compute_ini_summary(extracts, replicats, pending) → summary dict
  │     total rows, rates, ETAs, bottleneck, overall pct
  ├── global summary (all envs combined)
  └── summaries_by_env {env → summary}              [per DEV/PRE/PRO]

team_summary(stats_rows)                            [only when TAGS attribution exists]
  ├── by_team   — DML/size/proc aggregates per team (all envs)
  └── by_team_env — same breakdown per team+env combo (for env filter reactivity)

render_html()     — Jinja2 + custom filters (fmt_num, fmt_size, lag_class, tojson)
  ├── Carga Inicial tab: summaries_by_env serialised as _iniSummaries JS object
  └── Consumo tab: by_team_env serialised as _consumoTeamEnvs JS object
```

---

## HTML template

### Tabs
- **Infraestructura** — host/service health grid
- **Deployments** — process count cards per deployment
- **Procesos** — sortable/filterable table: Entorno→Deployment→Tipo→Proceso→Estado→Lag→Last Started→Tablas→Descripción
- **Tablas** — sortable/filterable table: Entorno→Motor→BBDD→Proceso→Tipo→Estado→Tabla→Size→Rows→Analyzed→Last Started→Lag→Inserts→Updates→Deletes→Upserts→Discards→Descripción
- **Carga Inicial** — shown only when EXTR_INI/REPL_INI processes exist
  - Global summary card: overall progress bar, extraction/replication row counts and rates, ETAs
  - Card content is regenerated by `updateIniGlobal(env)` JS when the env filter changes
  - Per-env summaries pre-computed in Python (`summaries_by_env`) and embedded as `_iniSummaries` JS object
  - Extract and replicat tables with per-process progress, rate, ETA, and paired extract reference
  - Column labels: "Filas est/aplicadas" (extract, denominator = Oracle estimate) vs "Filas ext/aplicadas" (replicat, denominator = extract's real count)
- **Consumo** — shown only when TAGS attribution is present
  - Hoy / Total (startup) toggle at the top of the tab — governs both tables and donut charts
  - Three SVG donut charts: DML distribution, Tamaño tablas, Infra (replicat count) — all by team
  - Charts react to the global env filter via `renderConsumoCharts(mode)` called from `setEnvGlobal`
  - Data embedded as `_consumoTeams` (all-env totals) and `_consumoTeamEnvs` (per-env breakdown)
  - KPI cards for top DML consumer, top infra user, top table count, unattributed tables
  - Aggregate and per-env breakdown tables with `<0.1%` display for tiny non-zero percentages

### Jinja2 filters registered in `render_html()`
- `| fmt_num` — integer → `1.2K` / `3.4M` / `5.6B` or `—` for zero
- `| fmt_size` — MB → `GB` if ≥ 1024, `—` if None
- `| lag_class` — CSS class: `lag-red` (≥3d), `lag-amber` (any lag), `text-muted` (unknown/zero)
- `| tojson` — JSON-serialise a Python object for safe embedding in `<script>` blocks

### Stats toggle (Hoy / Total)
`data-h24` / `data-tot` attributes on `<td>` elements, swapped by `setStatsMode()` JS.
Metric column headers (`th-ins` etc.) also update their label suffix.

### applyFilters JS
Generic function using element ID prefix. Filters: `Text`, `Tipo`, `Estado`, `Env`, `Dep`,
`Bbdd`, `Dbengine`, `Team`, `Costcenter`. Each filter is a `data-*` attribute on the `<tr>`.

### Global env filter reactivity
`setEnvGlobal(btn, env)` (called by the DEV/PRE/PRO toggle buttons) adds `env-<X>` class to
`document.body`. CSS hides non-matching `data-env` rows across all tabs. It also calls:
- `updateIniGlobal(env)` — regenerates the Carga Inicial Global card from `_iniSummaries[env]`
- `renderConsumoCharts(_consumoMode)` — re-filters all three donuts to the selected env

### MongoDB deployments
`db_engine: MONGO` → `_query_extract_db_meta` is skipped entirely (no PRM fetch, no Oracle query).
STATS command still works for MongoDB extracts/replicats — they appear in Procesos and Tablas tabs.

---

## Key design decisions

- **Parallel collection** via `ThreadPoolExecutor` (max 8 workers), one thread per deployment
- **Stats not reset** — `DAILY REPORTCDR` resets automatically at midnight; `--no-reset` flag kept for CLI compatibility only
- **Table size** — available from Oracle DBA_SEGMENTS via db_metadata, shown as Size column; propagated from extract to replicat rows so size appears in all views
- **BBDD column** shows `db_key` (e.g. `ACNPDB_PRO`) for Oracle, engine name (e.g. `MONGO`) as fallback
- **Entorno filter** (Tablas tab) sourced from `all_procs` so DEV appears even with no table stats
- **Donut charts** — pure SVG/JS, no CDN; `_drawDonut` renders an SVG path-based donut with tooltips
- **`<0.1%` display** — percentages that are non-zero but round to `0.0%` are shown as `<0.1%` in both the Jinja2 `pct_bar` macro and the donut legend JS
- **Ansible live log sync** — `_poll_async_progress.yml` pipes new log lines incrementally via SSH + `tee -a` each poll cycle when `_async_log_sync_host` is set; `sudo` on the remote is conditional (`_poll_sync_become`)
