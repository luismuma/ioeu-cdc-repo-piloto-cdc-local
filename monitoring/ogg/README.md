# OGG Monitoring — Daily Status Report

Queries every Oracle GoldenGate MA deployment via REST API and produces a
self-contained HTML report. No `adminclient` CLI required.

---

## How it works

### End-to-end architecture

```
  ┌─────────────────────────────────────────┐
  │  Trigger                                │
  │  ┌─────────────┐   ┌──────────────────┐ │
  │  │  AAP Job    │   │  CLI (local run) │ │
  │  │  Template   │   │  python3         │ │
  │  └──────┬──────┘   └────────┬─────────┘ │
  └─────────┼────────────────────┼───────────┘
            │                    │
            ▼                    ▼
  ┌─────────────────────────────────────────┐
  │  ogg_daily_report.yml  (Ansible)        │
  │                                         │
  │  Play 1 ─ localhost (EE)                │
  │    ├─ Parse vaults (AWS SM / env vars)  │
  │    ├─ Launch ogg_report.py (async)      │
  │    ├─ Poll log every N seconds (AAP)   │
  │    ├─ Upload to S3   (optional)         │
  │    ├─ Send email      (optional)        │
  │    └─ Register reporting host           │
  │                                         │
  │  Play 2 ─ reporting host (SSH)          │
  │    └─ Copy HTML + log via SSH           │
  │                                         │
  │  Play 3 ─ localhost (EE)                │
  │    └─ Cleanup EE + re-propagate failure │
  └──────────────┬──────────────────────────┘
                 │
                 ▼
  ┌─────────────────────────────────────────┐
  │  ogg_report.py                          │
  │                                         │
  │  inventory.yaml ──► deployment list     │
  │                                         │
  │  ThreadPoolExecutor (up to 8 workers)   │
  │  ┌─────────────────────────────────┐    │
  │  │  Per deployment (parallel)      │    │
  │  │                                 │    │
  │  │  OGG REST API                   │    │
  │  │  ├─ GET  /v2/config/health      │    │
  │  │  ├─ GET  /v2/extracts           │    │
  │  │  ├─ GET  /v2/replicats          │    │
  │  │  ├─ GET  /v2/sources            │    │
  │  │  ├─ GET  /v2/targets            │    │
  │  │  ├─ POST /v2/extracts/{n}/cmd   │    │
  │  │  │        STATS DAILY/TOTAL     │    │
  │  │  └─ POST /v2/replicats/{n}/cmd  │    │
  │  │           STATS DAILY/TOTAL     │    │
  │  │                                 │    │
  │  │  db_metadata.py [Oracle only]   │    │
  │  │  └─ python-oracledb             │    │
  │  │       ├─ DBA_SEGMENTS (size)    │    │
  │  │       └─ DBA_TAB_STATISTICS     │    │
  │  │            (rows, last analyzed)│    │
  │  └─────────────────────────────────┘    │
  │                                         │
  │  Jinja2 render  ◄─ templates/           │
  │        │             report.html.j2     │
  │        ▼                                │
  │  reports/YYYYMMDD/OGG_Informe_*.html    │
  └──────────────┬──────────────────────────┘
                 │
        ┌────────┼──────────┐
        ▼        ▼          ▼
    S3 bucket  Email    Reporting
   (presigned  (SMTP)   host (SSH)
     URL)
```

### Authentication flow

```
  OGG_ADMIN_PASSWORD set?
  ├─ Yes → Basic Auth  ──► 401/403? → Entra ID bearer token (fallback)
  └─ No  → Entra ID bearer token directly
                └─ tenant_id + client_id (per env: dev/pre/pro) + client_secret (env var)
```

All deployments are queried in parallel (up to 8 threads). The HTML report is
fully self-contained (no external CDN dependencies) and has up to six tabs:
**Infraestructura**, **Deployments**, **Procesos**, **Tablas**,
**Carga Inicial** (shown only when INI processes exist), and
**Consumo** (shown only when TAGS attribution is present).

### Data collection flow (per deployment)

```
inventory.yaml
      │  admin_url, env, db_engine, auth config
      ▼
  Build OGGClient
  (Basic Auth → fallback to Entra ID bearer on 401)
      │
      ▼
  GET /services/v2/config/health
      │  unreachable? → stop, mark deployment as unreachable
      │
      ├──► GET /services/v2/extracts       ┐
      ├──► GET /services/v2/replicats      │  process list
      ├──► GET /services/v2/sources        │  (distribution paths)
      └──► GET /services/v2/targets        ┘  (receiver paths)
               │
               │  for each EXTRACT / REPLICAT:
               ▼
      ┌────────────────────────────────────────────────┐
      │  POST /services/v2/{type}/{name}/command       │
      │        STATS DAILY  → inserts/updates/deletes  │
      │        STATS TOTAL  → totals since start       │
      │        → per-table operation counts            │
      └────────────────┬───────────────────────────────┘
                       │
          db_engine == ORACLE?
                       │
          ┌────────────┴────────────┐
         YES                       NO
          │                    (skip PRM + DB)
          ▼
      GET /services/v2/extracts/{name}  (PRM config lines)
          │
          ▼
      Parse PRM
          ├── tables[]            → list of TABLE lines
          ├── useridalias         → credential alias for DB connection
          ├── sourcecatalog       → PDB name
          └── TAGS {}             → requested_by, cost_center per table
          │
          │  db_connections.yaml present?
          │
          ├─── YES ──────────────────────────────────────┐
          │                                               ▼
          │                             find_connection(useridalias)
          │                                    │
          │                             connect to Oracle DB
          │                             (python-oracledb)
          │                                    │
          │                             ┌──────┴───────────────┐
          │                             │ SELECT DBA_SEGMENTS  │ → size_mb per table
          │                             │ SELECT DBA_TAB_STATS  │ → num_rows, last_analyzed
          │                             └──────────────────────┘
          │                                    │  CDC extract  → primary connection
          │                                    │  Initial Load → standby connection (if set)
          │
          └─── NO ──────────────────────────────┐
                                                 │
                                    (table names from PRM only,
                                     no size/rows metadata)
          │
          ▼
      Merge STATS + PRM + Oracle metadata
          ├── tables in STATS but not in PRM → keep as-is
          ├── tables in PRM but not in STATS → add zero-DML placeholder row
          └── enrich each row:
                  bbdd, db_engine, env, team (TAGS), cost_center,
                  proceso, proc_type, estado, lag, description,
                  size_mb, num_rows, last_analyzed
```

After all deployments are collected:

```
  collect_deployment() × N  (parallel)
          │
          ▼
  build_report_data()
      ├── hosts          — one entry per unique host, with service health
      ├── deployments    — process count cards grouped by deployment
      ├── all_procs      — flat list of all EXTRACTs / REPLICATs / paths
      ├── stats_rows     — flat list sorted by env → bbdd → type → status → table
      │       └── size_mb propagated from extract rows to matching replicat rows
      ├── build_ini_load_data()
      │       ├── extracts    — EXTR_INI entries with progress, ETA, rate
      │       ├── replicats   — REPL_INI entries paired to their extract
      │       ├── summary     — aggregate global progress/ETA
      │       └── summaries_by_env  — per-env summaries (DEV/PRE/PRO) for JS filter reactivity
      └── team_summary
              ├── by_team     — DML, table count, size, infra per team (all envs)
              └── by_team_env — same, broken down by (team, env)
          │
          ▼
  render_html()  (Jinja2)
      ├── Infraestructura tab  — host/service health grid
      ├── Deployments tab      — process count cards per deployment
      ├── Procesos tab         — sortable/filterable process table
      ├── Tablas tab           — sortable/filterable table stats
      ├── Carga Inicial tab    — per-process progress bars, ETAs, paired extract/replicat
      │       └── Global summary card reacts to env filter (JS)
      └── Consumo tab          — team attribution: DML, tamaño, infra donut charts + tables
              └── Charts react to env filter and Hoy/Total toggle (JS)
          │
          ▼
  reports/YYYYMMDD/OGG_Informe_YYYYMMDDHHMMSS.html
```

---

## Prerequisites

```bash
pip install pyyaml requests jinja2

# Optional — Oracle table metadata (size, row count, last analyzed)
pip install oracledb boto3
```

Python 3.11+ required.

---

## Credentials

All credentials are **environment variables only** — never stored in the inventory or on disk.

### Basic Auth (per-deployment local user)

```bash
export OGG_ADMIN_USER=oggadmin          # default if unset
export OGG_ADMIN_PASSWORD=your_password
```

### Entra ID (Azure AD bearer token)

Entra ID config lives in the inventory `defaults` block (see below). The script
picks the right client ID based on each deployment's `env` field. The client
secret is referenced by env var name, not value:

```bash
export OGG_AUTH_PROFILE_CLIENT_SECRET=<actual-secret-value>
```

### Auth fallback per deployment
1. If `OGG_ADMIN_PASSWORD` is set → tries Basic Auth first; if the server returns 401/403, falls back to Entra ID bearer
2. If no password → goes straight to Entra ID bearer

### Oracle metadata passwords (optional)

```bash
# Per-credential (recommended — matches password_credential field in db_connections.yaml)
export ORACLE_ACNPDB_GGADMIN=password

# Global fallback
export ORACLE_DB_PASSWORD=password
```

In production, passwords are fetched from AWS Secrets Manager (`CDC-VAULT-{ENV}`) automatically.

---

## Running locally

```bash
# All deployments
python3 ogg_report.py

# Single deployment (useful for testing)
python3 ogg_report.py -d OGG_CDC_ODB_ORA_E01_AWS_PRE

# Multiple deployments (comma-separated)
python3 ogg_report.py -d "OGG_CDC_ODB_ORA_E01_AWS_PRE,OGG_INI_ODB_ORA_E01_AWS_PRE"

# Custom inventory path
python3 ogg_report.py -i /path/to/ogg_inventory.yaml

# Custom output file
python3 ogg_report.py --out /tmp/my_report.html
```

Output goes to `./reports/YYYYMMDD/OGG_Informe_YYYYMMDDHHMMSS.html` by default.

### Local render test (no OGG servers needed)

```bash
python3 test_local.py --open
```

Builds the full report from synthetic fake data and opens it in the browser.
Useful for verifying template changes without connecting to any OGG server.

---

## Inventory structure

The inventory lives at `cdc-controller/config/inventory_deployments/inventory.yaml`. Key sections:

```yaml
defaults:
  verify_tls:              false
  nginx_proxy:             false       # admin_url IS the nginx gateway
  admin_user:              ""          # blank → uses OGG_ADMIN_USER env var
  entraid_tenant_id:       "5cc6c66d-..."
  entraid_client_id_dev:   "a1fff41a-..."
  entraid_client_id_pre:   "1a6485db-..."
  entraid_client_id_pro:   "ff9388c2-..."
  entraid_client_secret:   "OGG_AUTH_PROFILE_CLIENT_SECRET"  # env var name, not value

OGG_CDC_ODB_ORA_E01_AWS_PRE:
  admin_url:    "https://ogg-cdc-odb-ora-e01-aws-pre.es.mapfre.net"
  deployment:   "OGG_CDC_ODB_ORA_E01_AWS_PRE"
  env:          "PRE"           # DEV | PRE | PRO
  grupo:        "Online"        # used for grouping in the Deployments tab
  role_base:    "Extractor"     # Extractor | Replicator
  db_engine:    "ORACLE"        # ORACLE | MONGO | DB2
```

**Key field notes:**

| Field | Description |
|-------|-------------|
| `admin_url` | HTTPS base of the OGG MA deployment (reverse proxy). Script appends `/services/v2/…` |
| `deployment` | Deployment name as registered in OGG MA. Must match the YAML key. |
| `env` | Badge colour + Entra ID client selection (`entraid_client_id_dev/pre/pro`) |
| `db_engine` | Engine type shown in the Motor column (`ORACLE`, `MONGO`, `DB2`, …) |
| `nginx_proxy` | `false` = `admin_url` is already the nginx gateway (no extra path prefix) |
| `entraid_client_secret` | Name of the env var holding the actual secret, not the secret itself |

---

## Oracle table metadata (optional)

When `db_connections.yaml` is present (at `../../cdc-controller/config/inventory_connections/db_connections.yaml`)
and Oracle credentials are available, the report enriches the Tablas tab with:

- **Size** — table size from `DBA_SEGMENTS`
- **Rows** — row estimate from `DBA_TAB_STATISTICS`
- **Analyzed** — date of last `ANALYZE`
- **BBDD** — the specific database key from `db_connections.yaml` (e.g. `ACNPDB_PRO`)

Connection selection per extract type:
- **CDC extract** → primary connection (`conn_string_prim`)
- **Initial Load extract** → standby connection (`conn_string_stby`) if available, else primary

This reduces load on the primary database during initial loads.

---

## TAGS in PRM files

Both Extract `TABLE` lines and Replicat `MAP` lines can carry JSON metadata in an inline comment:

```
TABLE SCHEMA.TABLE_NAME;                         -- TAGS {"requested_by":"TeamName","cost_center":"CC001"}
MAP SCHEMA.TABLE_NAME, TARGET SCHEMA.TABLE_NAME; -- TAGS {"requested_by":"TeamName","cost_center":"CC001"}
```

The report parses TAGS from both sides and shows them as coloured badges inside the Tabla cell.
Filter dropdowns for **Equipo** (team) and **Centro de Coste** appear automatically
when tagged tables are present. Tags also power the **Consumo** tab — team attribution
for DML, table size, and replicat infrastructure counts.

Commented-out lines (`-- TABLE ...` / `-- MAP ...`) are correctly ignored.

---

## Running via Ansible / AAP

In AAP, create a Job Template pointing to `ansible/ogg_daily_report.yml` and paste
the contents of `ansible/extra_vars_sample.yml` into the **Extra Variables** field.

The playbook runs in three plays:
1. **Play 1 (localhost/EE)** — launches `ogg_report.py` async, polls progress every N seconds
   (live log lines are shown in AAP output), then uploads to S3 and/or sends email.
2. **Play 2 (reporting host, SSH)** — copies the HTML and log to a remote server.
   When `reporting_host` is set, the polling phase also streams new log lines to that
   host incrementally via SSH (so the log is live on the reporting host during the run,
   not just copied at the end).
3. **Play 3 (localhost/EE)** — cleans up temp files and re-propagates any failure from Play 1.

Key variables (set in the AAP job template extra vars):

| Variable | Default | Description |
|----------|---------|-------------|
| `ogg_inventory` | `…/inventory.yaml` | Path to the OGG deployment inventory |
| `filter_deploy` | `` | If set, only collect these deployment keys (comma-separated) |
| `db_connections` | `…/db_connections.yaml` | Oracle connection file (optional) |
| `reporting_host` | `` | SSH hostname for HTML/log delivery; empty = skip |
| `reporting_user` | `ec2-user` | SSH user on the reporting host |
| `reporting_become` | `false` | `true` → sudo to oracle on the reporting host |
| `ogg_poll_interval_sec` | `120` | Seconds between each log poll / progress update in AAP |
| `ogg_poll_max_minutes` | `0` | Max wait in minutes; `0` = no limit (job template timeout applies) |
| `s3_bucket` | `` | S3 bucket for HTML upload; empty = skip |
| `email_to` | `` | Recipient(s) for email delivery; empty = skip |

Credentials are injected via the playbook `environment:` block — nothing is written to disk.

---

## Project structure

```
monitoring/ogg/
├── ogg_report.py               Main script — REST queries + Jinja2 render
├── db_metadata.py              Oracle metadata + PRM parsing (optional dependency)
├── test_local.py               Local render test with synthetic data (no OGG servers needed)
├── templates/
│   └── report.html.j2          Jinja2 HTML template (up to 6-tab dark-blue UI)
├── ansible/
│   ├── ogg_daily_report.yml    AAP/AWX playbook (3-play: generate → copy → cleanup)
│   └── extra_vars_sample.yml   Extra variables template for the AAP job template
└── reports/                    Generated HTML reports (gitignored)
    └── YYYYMMDD/
        └── OGG_Informe_*.html

ansible/common/
└── _poll_async_progress.yml    Shared async polling helper (used by ogg_daily_report.yml
                                and other CDC controller playbooks)
```

---

## Troubleshooting

**Deployment marked unreachable**
The health endpoint (`GET /services/v2/config/health`) returned nothing or timed out.
Check connectivity:
```bash
curl -sk -u "$OGG_ADMIN_USER:$OGG_ADMIN_PASSWORD" \
  https://your-ogg-host/services/v2/config/health | python3 -m json.tool
```

**Tables: 0 for a stopped extract**
The report now always parses the PRM file to discover tables even when the extract is stopped.
If tables still show 0, the PRM fetch may have failed (check logs for `PRM fetch for ...`).

**Auth fails with 401 loop**
If `OGG_ADMIN_PASSWORD` is set to an empty string (e.g. PowerShell `$env:OGG_ADMIN_PASSWORD=""`),
the script treats it as "no password" and uses Entra ID only. Unset the variable rather than
setting it to empty if you want the Basic→Entra fallback.

**Wrong Entra ID client selected**
The client ID is picked by `entraid_client_id_{env}` where `env` is the deployment's `env`
field lowercased. A deployment with `env: "PRO"` uses `entraid_client_id_pro`.

**Oracle metadata not loading**
- Check `db_connections.yaml` path is correct relative to the script
- Verify `ORACLE_<password_credential>` env var is set (e.g. `ORACLE_ACNPDB_GGADMIN`)
- If you see `DPY-3015` in the log, the DB uses an old password verifier — set `ORACLE_CLIENT_LIB` to your Oracle Instant Client directory

**MongoDB extracts — no table metadata**
MongoDB PRM format differs from Oracle. Oracle metadata queries are skipped for
`db_engine: MONGO` deployments. Table stats (inserts/updates/deletes) are still
collected via the STATS command.
goo