# Oracle GoldenGate Microservices CLI

`ogg-ctl` is a Python CLI for automating Oracle GoldenGate Microservices
resources through the REST API. It is the lower-level operations tool used by
the CDC automation project.

It manages:

- Extracts
- Replicats
- Distribution Paths
- Receiver Paths
- OGG configuration files
- Credential aliases
- Named connections
- Supplemental logging (trandata)
- Managed process profiles
- Scheduled tasks (purge trails)
- OGG service users
- Deployment status and metadata

The CLI follows this shape:

```bash
python oggctl.py RESOURCE ACTION [NAME] --deployment DEPLOYMENT [options]
```

`DEPLOYMENT` can be either a key from the shared inventory or a path to a YAML
deployment file.

## Project Layout

```text
ogg-ctl/
|-- oggctl.py
|-- create_vault.py
|-- api/                       # REST API operations (one module per resource)
|   |-- _common.py             # shared helpers (log_ogg_messages, managed-process settings, ...)
|   |-- auth.py                # Entra ID token fetch + inventory field extraction
|   |-- client.py              # OGGClient + auto-detect auth (basic → Bearer fallback)
|   |-- config_file_ops.py
|   |-- connection_ops.py
|   |-- credential_alias_ops.py
|   |-- deployment_ops.py      # Service Manager API
|   |-- distribution_path_ops.py
|   |-- errors.py
|   |-- extract_ops.py
|   |-- managed_process_profile_ops.py
|   |-- prm_builder.py
|   |-- receiver_path_ops.py
|   |-- replicat_ops.py        # includes Big Data .properties helpers
|   |-- tablelist.py           # tables-file / tables-json parsing
|   |-- task_ops.py
|   |-- trail.py
|   |-- trandata_ops.py
|   `-- user_ops.py
|-- cli/                       # argparse handlers (one module per resource)
|   |-- __init__.py            # entry point, inventory loading, client build
|   |-- _io.py                 # load_lines / load_properties helpers
|   |-- parser.py
|   |-- output.py              # format-aware print_status_table / print_info / print_name_list
|   |-- config_file.py
|   |-- connection.py
|   |-- credential_alias.py
|   |-- deployment.py
|   |-- distribution_path.py
|   |-- extract.py
|   |-- managed_process_profile.py
|   |-- receiver_path.py
|   |-- replicat.py
|   |-- task.py
|   |-- trandata.py
|   `-- user.py
|-- playbooks/
|-- replication_requests/
|-- templates/
|   |-- extract/
|   `-- replicat/
`-- vault/
```

The shared deployment inventory and target connection config live outside this folder:

```text
cdc-controller/config/inventory_deployments/inventory.yaml
cdc-controller/config/inventory_connections/target_connections.yaml
```

## Requirements

Install the repository dependencies from the repository root:

```bash
python -m pip install -r requirements.txt
```

Core Python dependencies used by `ogg-ctl` include `requests`, `urllib3`,
`PyYAML`, and `Jinja2`.

## Authentication

`ogg-ctl` automatically selects the auth method per deployment session:

1. **Basic auth** — tried first if `OGG_ADMIN_PASSWORD` is set (or `admin_password_file` is configured).
2. **Entra ID Bearer (OAuth2 client credentials)** — used as a fallback when basic auth returns HTTP 401,
   or exclusively when no password is provided but Entra ID is configured in the inventory.

This means no change is needed to how you call `oggctl.py` during the migration.
Deployments that still accept basic auth continue to work. Deployments where an
authorization profile has been enabled will automatically fall back to Bearer.

### Basic auth setup

```bash
export OGG_ADMIN_PASSWORD=secret
python oggctl.py --deployment OGG_CDC_ODB_ORA_E01_AWS_DEV extract list
```

Or via a file (path set in inventory):

```yaml
admin_password_file: /run/secrets/ogg_admin_password
```

### Entra ID Bearer setup

Add the following fields to the inventory `defaults` (or to individual entries to override):

```yaml
defaults:
  entraid_tenant_id:     "5cc6c66d-ffb2-469f-9385-cda840e57836"
  entraid_client_id_dev: "a1fff41a-e259-4b6c-ab3d-00c393b880a7"
  entraid_client_id_pre: "1a6485db-ec59-424d-a39a-fc0336ddc059"
  entraid_client_id_pro: "ff9388c2-26a2-43e0-986e-9b4397a18248"
  entraid_client_secret: "OGG_AUTH_PROFILE_CLIENT_SECRET"   # env var name, not the value
```

`entraid_client_secret` holds the **name** of the environment variable that
contains the actual client secret. The secret value is never stored in the
inventory. At runtime, the CLI reads:

```bash
export OGG_AUTH_PROFILE_CLIENT_SECRET=<actual_client_secret>
```

The client ID is selected automatically based on the deployment's `env` field
(`DEV` → `entraid_client_id_dev`, `PRE` → `entraid_client_id_pre`, etc.).
The OAuth scope is derived as `<client_id>/.default` (standard for Azure app
registrations).

### AAP credential mapping

| Env var | Injected by | Purpose |
|---------|-------------|---------|
| `OGG_ADMIN_PASSWORD` | AAP Vault credential (`ogg_credential_dev/pre/pro`) | Basic auth password |
| `OGG_AUTH_PROFILE_CLIENT_SECRET` | AWS Secrets Manager credential | Entra ID client secret |

Both credential types can be attached to the same job template simultaneously.
When a deployment migrates to Entra ID, the fallback fires automatically — no
playbook changes required.

Do not store plaintext passwords or secrets in the deployment inventory.

## Deployment Configuration

By default, `oggctl.py` reads:

```text
../cdc-controller/config/inventory_deployments/inventory.yaml
```

Override this with:

```bash
set OGG_INVENTORY=C:\path\to\inventory.yaml
```

PowerShell example:

```powershell
$env:OGG_INVENTORY = "C:\path\to\inventory.yaml"
$env:OGG_ADMIN_PASSWORD = "..."
```

Inventory entries are merged with `defaults`:

```yaml
defaults:
  verify_tls: false
  suppress_insecure_warning: true
  timeout: [30, 180]
  domain: "OracleGoldenGate"
  description: "Created via automation"
  api_version: "v2"
  admin_user: "oggadmin"
  nginx_proxy: true

OGG_GR_ODB_ORA_E01_AWS_DEV:
  admin_url: "https://example-host"
  deployment: "OGG_GR_ODB_ORA_E01_AWS_DEV"
  env: "DEV"
  project: "GOLDENRECORDS"
  db_engine: "ORACLE"
```

Important fields:

| Field | Purpose |
| --- | --- |
| `admin_url` | Base URL for the OGG deployment or reverse proxy |
| `deployment` | OGG deployment name used in nginx proxy URLs |
| `env` | Environment tag: `DEV`, `PRE`, or `PRO` — used for Entra ID client ID selection |
| `api_version` | REST API version, default `v2` |
| `admin_user` | OGG admin username |
| `admin_password_file` | Optional file containing the OGG admin password |
| `verify_tls` | `true`, `false`, or CA bundle path |
| `timeout` | `[connect_timeout, read_timeout]` in seconds |
| `nginx_proxy` | Builds URLs as `/services/<deployment>/<service>/<api_version>` |
| `distribution_url` | Optional explicit Distribution Server URL |
| `receiver_url` | Optional explicit Receiver Server URL |
| `performance_url` | Optional explicit Performance Metrics Server URL |
| `entraid_tenant_id` | Azure AD tenant ID (shared across all envs) |
| `entraid_client_id_dev` | Entra ID app client ID for DEV |
| `entraid_client_id_pre` | Entra ID app client ID for PRE |
| `entraid_client_id_pro` | Entra ID app client ID for PRO |
| `entraid_client_secret` | Name of the env var that holds the client secret (e.g. `OGG_AUTH_PROFILE_CLIENT_SECRET`) |

When `nginx_proxy: true`, only `admin_url` is normally needed. The client builds
service paths for Administration, Distribution, Receiver, Performance, and
Service Manager APIs.

## Global Options

Every action accepts:

| Option | Description |
| --- | --- |
| `--deployment DEPLOYMENT` | Required. Inventory key or path to a YAML config file |
| `--verbose`, `-v` | Enable debug logging to stderr |
| `--format auto|table|json` | Output format. Default is `auto` |

`auto` prints JSON when stdout is not a TTY and table output when it is.

## Table Input

Extract and Replicat create/validate commands accept either a text file or a
JSON string.

Text file:

```text
SOE.ORDERS
SOE.CUSTOMERS
# comments are allowed
```

JSON example for Extract:

```json
[
  {"name": "SOE.ORDERS"},
  {"name": "SOE.CUSTOMERS", "columns": ["ID", "NAME", "EMAIL"]},
  {"name": "SOE.SALES", "filter": "REGION = 'EMEA'", "keycols": ["ID"]}
]
```

Supported JSON keys include:

| Key | Extract | Replicat | Purpose |
| --- | --- | --- | --- |
| `name` | Yes | Yes | Table name, usually `SCHEMA.TABLE` |
| `columns` | Yes | No | Include only these columns |
| `columns_except` | Yes | Yes | Exclude these columns |
| `sqlpredicate` | Yes | No | Extract SQL predicate |
| `filter` | Yes | Yes | Row filter |
| `keycols` | Yes | Yes | Explicit key columns |

## Extracts

```bash
python oggctl.py extract list --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py extract status ETAPI1 --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py extract info ETAPI1 --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py extract history ETAPI1 --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py extract start ETAPI1 --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py extract stop ETAPI1 --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py extract delete ETAPI1 --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
```

`history` returns the run history for an extract (most recent run first). Each entry includes:

| Field | Description |
|-------|-------------|
| `run` | Run index (1 = most recent) |
| `status` | Final status of that run (`STOPPED`, `ABENDED`, etc.) |
| `started` | ISO 8601 start timestamp |
| `finished` | ISO 8601 stop/completion timestamp |
| `pid` | OS process ID for that run |
| `exit_code` | Process exit code |

Pass `--format json` (or pipe to a non-TTY) to get the full JSON array.

Create an integrated Extract:

```bash
python oggctl.py extract create ETAPI1 \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV \
  --extract-type integrated \
  --trail et \
  --useridalias ACNPDB_PRIM_DEV \
  --tables-file tables.txt \
  --pdb ACNPDB
```

Create from JSON table input:

```bash
python oggctl.py extract create ETAPI1 \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV \
  --extract-type integrated \
  --trail et \
  --useridalias ACNPDB_PRIM_DEV \
  --tables-json "[{\"name\":\"SOE.ORDERS\"}]"
```

PowerShell passes arguments to native executables differently from bash. Use
backslash-escaped JSON quotes, or the stop-parsing marker:

```powershell
python oggctl.py extract create ETAPI1 `
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV `
  --extract-type integrated `
  --trail et `
  --useridalias ACNPDB_PRIM_DEV `
  --tables-json '[{\"name\":\"SOE.ORDERS\"}]'
```

```powershell
python --% oggctl.py extract create ETAPI1 --deployment OGG_GR_ODB_ORA_E01_AWS_DEV --extract-type integrated --trail et --useridalias ACNPDB_PRIM_DEV --tables-json "[{\"name\":\"SOE.ORDERS\"}]"
```

Validate without leaving state behind:

```bash
python oggctl.py extract validate ETAPI1 \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV \
  --extract-type integrated \
  --trail et \
  --useridalias ACNPDB_PRIM_DEV \
  --tables-file tables.txt
```

Manage the parameter file (`.prm`) on an existing Extract:

```bash
python oggctl.py extract params get ETAPI1 --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py extract params set ETAPI1 --from-file new.prm --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py extract params append ETAPI1 --lines-json '["TABLE SOE.NEW;"]' --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
```

`get` prints the raw PRM, `set` replaces it entirely (PATCH on the extract resource),
`append` appends new lines. `set` and `append` accept either `--from-file FILE` or
`--lines-json '[...]'`.

Register an existing Extract with the source database:

```bash
python oggctl.py extract register ETAPI1 \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV \
  --optimized \
  --replace
```

Update properties:

```bash
python oggctl.py extract update ETAPI1 \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV \
  --description "Updated via automation" \
  --critical
```

Reports, statistics, and process commands:

```bash
python oggctl.py extract report ETAPI1 --deployment OGG_GR_ODB_ORA_E01_AWS_DEV --tail 50
python oggctl.py extract stats ETAPI1 --deployment OGG_GR_ODB_ORA_E01_AWS_DEV --period hourly
python oggctl.py extract stats ETAPI1 --deployment OGG_GR_ODB_ORA_E01_AWS_DEV --table SOE.ORDERS
python oggctl.py extract cmd ETAPI1 GETLAG --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py extract cmd ETAPI1 STATS --arguments "HOURLY" --is-reported --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
```

Extract create/validate options:

| Option | Required | Description |
| --- | --- | --- |
| `--extract-type integrated|initial_load` | Yes | Extract profile |
| `--trail TRAIL` | Create: yes, validate: optional | Trail name or path, e.g. `et`, `north/et` |
| `--useridalias ALIAS` | Yes | Credential alias |
| `--tables-file FILE` | One of table inputs | Plain text table list |
| `--tables-json JSON` | One of table inputs | JSON table payload |
| `--pdb PDB_NAME` | No | Adds `SOURCECATALOG` |
| `--domain DOMAIN` | No | Credential domain |
| `--description TEXT` | No | Process description |
| `--critical` | No | Set process critical flag |
| `--encryption-profile PROFILE` | No | Encryption profile |

## Replicats

```bash
python oggctl.py replicat list --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py replicat status RPAPI1 --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py replicat info RPAPI1 --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py replicat history RPAPI1 --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py replicat start RPAPI1 --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py replicat stop RPAPI1 --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py replicat delete RPAPI1 --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
```

`history` returns the run history for a replicat (most recent run first). Fields are the same as `extract history`:
`run`, `status`, `started`, `finished`, `pid`, `exit_code`. Pass `--format json` for the full JSON array.

### Creating a Replicat

`replicat create` is **target-aware**. `--target` is required and drives both
the PRM content and, for Big Data targets, an automatic properties-file render
after the replicat is created.

| `--target` | What it does |
|------------|--------------|
| `oracle` | Emits `USERIDALIAS` in the PRM; `--useridalias` is required |
| `kafka_confluent` | Skips USERIDALIAS; for coordinated, adds `TARGETDB LIBFILE libggjava.so`; renders `kafka_confluent.properties.j2` and uploads it as `<replicat>.properties` |
| `s3` | Same as Kafka but renders `s3.properties.j2` instead |

**Oracle target:**
```bash
python oggctl.py replicat create RPORA1 \
  --deployment OGG_GR_ODB_DAA_R01_AWS_DEV \
  --replicat-type classic \
  --target oracle \
  --trail ea \
  --useridalias TARGET_ALIAS \
  --tables-file tables.txt
```

**Kafka Confluent target (with `--target-name` for connection vars):**
```bash
python oggctl.py replicat create RPKAFKA1 \
  --deployment OGG_GR_ODB_DAA_R01_AWS_DEV \
  --replicat-type coordinated \
  --target kafka_confluent \
  --trail ea \
  --tables-json '[{"name":"SOE.ORDERS"}]' \
  --target-name kafka-dev \
  --target-vars-json '{"topic_prefix":"esp.int-sales.bronze.zeus","database":"MP01","schema":"RIC_LD"}' \
  --max-threads 4
```

**S3 target (with `--target-name` for connection vars):**
```bash
python oggctl.py replicat create RPS31 \
  --deployment OGG_GR_ODB_DAA_R01_AWS_PROD \
  --replicat-type coordinated \
  --target s3 \
  --trail ea \
  --tables-json '[{"name":"SOE.ORDERS"}]' \
  --target-name s3-prod \
  --target-vars-json '{"database":"MP01","bucket_path_prefix":"golden_gate/es/mapfreas/MP01"}'
```

`--target-name` looks up a named connection in `target_connections.yaml` and auto-injects
its fields (bootstrap servers, OAuth endpoint, S3 role, etc.) as template variables.
`--target-vars-json` is still accepted for per-replicat variables (topic prefix, database,
schema, ...) and overrides any `--target-name` values when keys overlap.

To see which variables a Big Data target template needs (no `--deployment`
required — `templates` is local-only):
```bash
python oggctl.py templates show s3
python oggctl.py templates show kafka_confluent
```

**Rollback note:** if the replicat is created but the properties upload fails
(bad vars, broken template, etc.) the replicat is **left in place**. Fix the
vars and rerun `replicat properties set --template` to finish the setup — no
need to delete and recreate.

Other replicat actions:

```bash
python oggctl.py replicat validate RPAPI1 --deployment OGG_GR_ODB_DAA_R01_AWS_DEV --replicat-type classic --target oracle --trail ea --useridalias TARGET_ALIAS --tables-file tables.txt
python oggctl.py replicat update RPAPI1 --deployment OGG_GR_ODB_DAA_R01_AWS_DEV --description "Updated" --critical
python oggctl.py replicat report RPAPI1 --deployment OGG_GR_ODB_DAA_R01_AWS_DEV --tail 50
python oggctl.py replicat stats RPAPI1 --deployment OGG_GR_ODB_DAA_R01_AWS_DEV --period daily
python oggctl.py replicat cmd RPAPI1 GETLAG --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
```

Replicat create/validate options:

| Option | Required | Description |
| --- | --- | --- |
| `--replicat-type classic|coordinated` | Yes | Replicat profile |
| `--target oracle|kafka_confluent|s3` | Yes | Target technology — drives PRM content and properties render |
| `--trail TRAIL` | Create: yes, validate: optional | Source trail name or path |
| `--useridalias ALIAS` | Only when `--target=oracle` | Target credential alias |
| `--tables-file FILE` | One of table inputs | Plain text table list |
| `--tables-json JSON` | One of table inputs | JSON table payload |
| `--target-name NAME` | No | Load connection vars from `target_connections.yaml` (Big Data targets) |
| `--target-vars-json JSON` | No | Per-replicat template variables; overrides `--target-name` values |
| `--checkpoint-table SCHEMA.TABLE` | No | Checkpoint table, can come from config |
| `--pdb PDB_NAME` | No | Source catalog |
| `--domain DOMAIN` | No | Credential domain |
| `--description TEXT` | No | Process description |
| `--critical` | No | Set process critical flag |
| `--encryption-profile PROFILE` | No | Encryption profile |
| `--max-threads N` | No | Coordinated Replicat thread limit |

Manage the parameter file (`.prm`) on an existing Replicat — same shape as Extract:

```bash
python oggctl.py replicat params get RPAPI1 --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py replicat params set RPAPI1 --from-file new.prm --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py replicat params append RPAPI1 --lines-json '["MAP SOE.NEW, TARGET SOE.NEW;"]' --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
```

Manage the `.properties` file for Big Data Replicats (Kafka, S3, BigQuery, ...).
The properties file is stored as `<replicat>.properties` under `/config/files/`:

```bash
python oggctl.py replicat properties get RPKAFKA1 --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py replicat properties set RPKAFKA1 --from-file kafka.properties --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py replicat properties merge RPKAFKA1 \
  --properties-json '{"gg.handler.kafka.topic":"orders","gg.handler.kafka.bootstrap.servers":"b1:9092,b2:9092"}' \
  --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
```

- `get` returns the parsed file as a dict (or JSON when piped to a non-TTY)
- `set` replaces the entire file (idempotent upsert: creates if absent)
- `merge` updates specific keys without touching others

`set` and `merge` accept either `--from-file FILE` (Java `.properties` format with
`#`/`!` comments) or `--properties-json '{...}'` (JSON object). Comments are not
preserved on round-trip.

### Set from a built-in template

Two ready-made templates live under `templates/replicat/` for common Big Data
targets — same Jinja-rendering flow as `extract create`'s `.prm` templates:

- `s3` → `s3.properties.j2`
- `kafka_confluent` → `kafka_confluent.properties.j2`

Use `--template` with `replicat properties set` to render the template and
upload the result in one step (idempotent upsert):

```bash
# S3 — connection vars from target_connections.yaml, per-replicat vars in --vars-json
python oggctl.py replicat properties set RPS31 \
  --template s3 \
  --target-name s3-prod \
  --vars-json '{"database":"MP01","bucket_path_prefix":"golden_gate/es/mapfreas/MP01"}' \
  --deployment OGG_GR_ODB_DAA_R01_AWS_PROD

# Kafka Confluent — same pattern
python oggctl.py replicat properties set RPKAFKA1 \
  --template kafka_confluent \
  --target-name kafka-dev \
  --vars-json '{"platform":"sales","product":"zeus","database":"MP01","schema":"RIC_LD"}' \
  --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
```

You can also pass all variables directly without `--target-name`:

```bash
python oggctl.py replicat properties set RPS31 \
  --template s3 \
  --vars-json '{
    "s3_sts_assume_role":"arn:aws:iam::123456789:role/ogg-cdc-prod",
    "s3_sts_session_name":"ogg-rps31",
    "s3_credentials_alias":"s3_prod_creds",
    "database":"MP01",
    "bucket_path_prefix":"golden_gate/es/mapfreas/MP01"
  }' \
  --deployment OGG_GR_ODB_DAA_R01_AWS_PROD
```

Auto-injected variables — you don't need to pass these in `--vars-json`:

| Variable | Source |
|----------|--------|
| `replicat_name` | positional `NAME` argument |
| `deployment_name` | inventory entry's `deployment` field |
| `env` | inventory entry's `env` field |
| `db_engine` | inventory entry's `db_engine` field |

Anything you pass in `--vars-json` overrides auto-injected and `--target-name` values.

**S3 template — credentials are not template variables**: `accessKeyId` and
`secretKey` are rendered as wallet references (`ORACLEWALLETUSERNAME[alias domain]`).
Create the credential once via `credential-alias create` (see below), then pass
just the alias name as `s3_credentials_alias`. The AWS access key + secret only
ever live encrypted inside OGG's wallet.

## Distribution Paths

Distribution Paths move trail data from a source Distribution Server to a target
Receiver Server.

```bash
python oggctl.py distribution-path list --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py distribution-path create dp-test \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV \
  --source-uri "trail://source-host/services/OGG_DEPLOYMENT/distsrvr/v2/sources?trail=et" \
  --target-uri "wss://target-host/services/OGG_DEPLOYMENT/recvsrvr/v2/targets?trail=ea"
python oggctl.py distribution-path start dp-test --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py distribution-path stop dp-test --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py distribution-path status dp-test --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py distribution-path info dp-test --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py distribution-path stats dp-test --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py distribution-path update dp-test --deployment OGG_GR_ODB_ORA_E01_AWS_DEV --description "Updated"
python oggctl.py distribution-path delete dp-test --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
```

Create options:

| Option | Required | Description |
| --- | --- | --- |
| `--source-uri URI` | Yes | Source trail URI |
| `--target-uri URI` | Yes | Target receiver URI |
| `--source-alias ALIAS` | No | Source credential alias |
| `--target-alias ALIAS` | No | Target credential alias |
| `--domain DOMAIN` | No | Credential domain |
| `--begin now|sequence:offset` | No | Start position, default `now` |
| `--description TEXT` | No | Path description |
| `--critical` | No | Set critical flag |
| `--encryption-profile PROFILE` | No | Encryption profile |

## Receiver Paths

Receiver Paths define inbound trail ingestion on the target side.

```bash
python oggctl.py receiver-path list --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py receiver-path create rp-test \
  --deployment OGG_GR_ODB_DAA_R01_AWS_DEV \
  --source-uri "wss://source-host/services/OGG_DEPLOYMENT/distsrvr/v2/sources?trail=et" \
  --target-uri "trail://localhost/services/v2/trail-files/local/ea"
python oggctl.py receiver-path start rp-test --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py receiver-path stop rp-test --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py receiver-path status rp-test --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py receiver-path info rp-test --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py receiver-path stats rp-test --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py receiver-path update rp-test --deployment OGG_GR_ODB_DAA_R01_AWS_DEV --critical
python oggctl.py receiver-path delete rp-test --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
```

Create options are the same shape as Distribution Path create:
`--source-uri`, `--target-uri`, optional aliases, `--domain`, `--begin`,
`--description`, `--critical`, and `--encryption-profile`.

## Managed Process Profiles

Managed process profiles are reusable auto-start and auto-restart settings that
can be attached to Extracts and Replicats.

```bash
python oggctl.py managed-process-profile create default-restart \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV \
  --auto-restart \
  --auto-restart-retries 5 \
  --auto-restart-delay 30 \
  --window 120 \
  --disable-on-failure

python oggctl.py managed-process-profile list --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py managed-process-profile get default-restart --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py managed-process-profile update default-restart --deployment OGG_GR_ODB_ORA_E01_AWS_DEV --auto-start --auto-restart
python oggctl.py managed-process-profile delete default-restart --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
```

Supported managed-process options:

| Option | Description |
| --- | --- |
| `--managed-process-profile PROFILE` | Use a named profile on Extract/Replicat create or update |
| `--auto-start`, `--no-auto-start` | Auto-start when Administration Server starts |
| `--auto-start-delay SEC` | Delay before auto-start |
| `--auto-restart`, `--no-auto-restart` | Auto-restart if the process abends |
| `--auto-restart-retries N` | Restart retry count |
| `--auto-restart-delay SEC` | Delay between restarts |
| `--on-success`, `--no-on-success` | Restart even after clean process exit |
| `--window SEC` | Retry counting window |
| `--disable-on-failure`, `--no-disable-on-failure` | Disable after retries are exhausted |

`--managed-process-profile` is mutually exclusive with inline auto-start and
auto-restart flags.

## Tasks

OGG scheduled tasks are managed under the `task` resource. Currently only the
`purge-trail` type is supported.

### Purge Trail tasks

A purge trail task tells OGG to periodically delete old trail files, keeping
only files newer than a minimum age.

```bash
# List all purge trail tasks
python oggctl.py task purge-trail list --deployment OGG_GR_ODB_ORA_E01_AWS_DEV

# Show a specific task
python oggctl.py task purge-trail get cleanup-primary --deployment OGG_GR_ODB_ORA_E01_AWS_DEV

# Create
python oggctl.py task purge-trail create cleanup-primary \
  --trail primary/ACNPDB/EPPI00D/pr \
  --keep-value 2 --keep-units hours \
  --schedule-value 1 --schedule-units hours \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV

# Update (read-modify-write — only supplied flags are changed)
python oggctl.py task purge-trail update cleanup-primary \
  --keep-value 4 \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV

# Lifecycle
python oggctl.py task purge-trail enable  cleanup-primary --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py task purge-trail disable cleanup-primary --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py task purge-trail start   cleanup-primary --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py task purge-trail stop    cleanup-primary --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py task purge-trail delete  cleanup-primary --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
```

**Trail format** — `--trail` follows the same format as `extract create --trail`:
the last segment (2 characters) is the trail name, everything before the last `/`
is the path prefix.

| Example | Trail name | Path prefix |
|---------|-----------|-------------|
| `et` | `et` | *(root)* |
| `north/et` | `et` | `north` |
| `primary/ACNPDB/EPPI00D/pr` | `pr` | `primary/ACNPDB/EPPI00D` |

The trail must exist on the deployment — the CLI validates against `GET /trails?details`
before creating the task. Multiple trails with the same 2-char name at different
paths are allowed by OGG; the full path is required to identify them uniquely.

`--trail` is repeatable to purge multiple trails in one task:

```bash
python oggctl.py task purge-trail create cleanup-all \
  --trail primary/ACNPDB/EPPI00D/pr \
  --trail secondary/ACNPDB/EPPI00D/pr \
  --keep-value 2 --keep-units hours \
  --schedule-value 1 --schedule-units hours \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
```

`purge-trail create` options:

| Option | Required | Description |
|--------|----------|-------------|
| `TASK_NAME` | Yes | Unique task name |
| `--trail TRAIL` | Yes (repeatable) | Trail to purge: `path/name` or just `name` |
| `--keep-value N` | Yes | Minimum age of trail files to keep |
| `--keep-units UNITS` | Yes | `seconds`, `minutes`, `hours`, or `days` |
| `--schedule-value N` | Yes | How often to run |
| `--schedule-units UNITS` | Yes | `seconds`, `minutes`, `hours`, or `days` |
| `--enabled` | No | Enable immediately after creation (default: false) |
| `--critical` | No | Mark task as critical (default: false) |
| `--use-checkpoints` | No | Honour extract checkpoints when purging (default: false) |

`purge-trail update` accepts the same optional flags; omitted flags keep their
existing values (read-modify-write).

## Config Files

Manage files stored by OGG, such as `.prm`, `.properties`, `.conf`, `.inc`,
`GLOBALS`, and `ENCKEYS`.  This is the low-level resource — for the parameter
file of a specific Extract/Replicat use `extract params` / `replicat params`,
and for a Big Data Replicat's properties use `replicat properties`.

```bash
python oggctl.py config-file list --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py config-file get handler.properties --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py config-file create handler.properties --from-file local-handler.properties --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py config-file replace handler.properties --lines-json '["a=1","b=2"]' --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
python oggctl.py config-file delete handler.properties --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
```

`create` and `replace` keep strict semantics (`create` fails if the file exists,
`replace` fails if it doesn't). Both accept either `--from-file FILE` or
`--lines-json '[...]'` (mutually exclusive, exactly one required).

### Create from a built-in template

For shared, env-specific config files (typically referenced by multiple
replicats), pass `--template` to `config-file create` to render a Jinja
template and **upsert** the result — creates if absent, replaces if present.

Available templates:

- `kafka_producer` → `templates/replicat/kafka_producer.properties.j2` —
  Kafka broker + Schema Registry config referenced by the replicat's
  `gg.handler.kafkaconnect.kafkaProducerConfigFile=...` line.
- `kafka_client_jaas` → `templates/replicat/kafka_client_jaas.conf.j2` —
  JAAS config for the `OAUTHBEARER` login module (`clientId`, `clientSecret`, `scope`).
  Required when the Kafka cluster enforces OAuth. The `clientSecret` is plaintext
  in this file (product limitation) — supply it via `--secret-env` so it is never
  passed on the command line or logged.

With `--target-name` (recommended — loads connection vars from `target_connections.yaml`):

```bash
python oggctl.py config-file create kafka_producer_DEV.properties \
  --template kafka_producer \
  --target-name kafka-dev \
  --vars-json '{"schema_registry_url":"https://schema.azure","identity_pool":"pool-1"}' \
  --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
```

Without `--target-name` (pass all vars manually):

```bash
python oggctl.py config-file create kafka_producer_DEV.properties \
  --template kafka_producer \
  --vars-json '{
    "bootstrap_servers":"kafka1.broker.azure:9092",
    "oauth_endpoint_url":"https://login.microsoftonline.com/tenant-id/oauth2/v2.0/token",
    "api_scope":"kafka-dev/.default",
    "schema_registry_url":"https://schema.azure",
    "cluster_id":"lsrc-xyz",
    "identity_pool":"pool-1",
    "ogg_alias_credential":"kafka_oauth_dev"
  }' \
  --deployment OGG_GR_ODB_DAA_R01_AWS_DEV
```

`deployment_name` and `env` are auto-injected from the inventory entry.
OAuth `client_id` / `client_secret` are **not** template variables — the
rendered file uses wallet references (`ORACLEWALLETUSERNAME[alias domain]`)
that resolve at runtime against the OGG credential store. Create the OAuth
credential once via `credential-alias create` and pass the alias name as
`ogg_alias_credential`.

**JAAS config (`kafka_client_jaas`):**

The JAAS file contains `clientSecret` in plaintext (OGG product limitation).
Use `--secret-env TEMPLATE_VAR=ENV_VAR_NAME` to supply it from an environment
variable — the value is never passed on the command line and never logged.
`--target-name` auto-fills `client_id` and `api_scope` from `target_connections.yaml`.

```bash
# File name: kafka_client_<cluster>_<identity_pool>_<env>_oauth_jaas.conf
# Hyphens in cluster_name / identity_pool are replaced with underscores automatically.
python oggctl.py config-file create kafka_client_clusterA_poolY_DEV_oauth_jaas.conf \
  --template kafka_client_jaas \
  --target-name kafka-dev \
  --secret-env client_secret=OGG_AUTH_PROFILE_CLIENT_SECRET \
  --deployment OGG_CDC_MDB_DAA_R01_AWS_DEV
```

`--secret-env` is repeatable. The env var must be set in the shell (or injected
by the AAP credential) before running the command:

```bash
export OGG_AUTH_PROFILE_CLIENT_SECRET=<actual_secret>
```

## Credential Aliases

Credential aliases store credentials inside the OGG credential store (Oracle
Wallet) so that `.prm` / `.properties` files can reference them as
`ORACLEWALLETUSERNAME[alias domain]` / `ORACLEWALLETPASSWORD[alias domain]`
instead of containing plaintext secrets. Used by database logins, AWS S3
credentials, Kafka OAuth client identities, etc.

Domains are normalized by the CLI — `oraclegoldengate` becomes
`OracleGoldenGate`.

```bash
python oggctl.py credential-alias list   --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py credential-alias list   --deployment OGG_GR_ODB_ORA_E01_AWS_DEV --domain OracleGoldenGate
python oggctl.py credential-alias info     --deployment OGG_GR_ODB_ORA_E01_AWS_DEV --alias ACNPDB_PRIM_DEV
python oggctl.py credential-alias validate --deployment OGG_GR_ODB_ORA_E01_AWS_DEV --alias ACNPDB_PRIM_DEV
python oggctl.py credential-alias delete   --deployment OGG_GR_ODB_ORA_E01_AWS_DEV --alias ACNPDB_PRIM_DEV
```

### Creating an alias — three password modes

The password is never accepted as a command-line argument. `create` accepts
one of these mutually exclusive sources (and prompts interactively if none is
given on a TTY):

| Mode | Flag | Best for |
|------|------|----------|
| File | `--password-file FILE` | Local scripts; the file lives briefly on disk |
| Env var | `--password-env VAR_NAME` | **AAP** — Ansible Vault → env var, never touches disk |
| Interactive | *(no flag)* | Ad-hoc local use; `getpass` prompts you |

**File:**
```bash
python oggctl.py credential-alias create \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV \
  --alias ACNPDB_PRIM_DEV \
  --userid "c##ggadmin@//dbhost:1521/SERVICE" \
  --password-file secrets/db.pass
```

**Env var (AAP-friendly):**
```yaml
- name: "Create credential alias in OGG"
  command:
    argv:
      - "{{ _venv }}/bin/python3"
      - "{{ playbook_dir }}/../oggctl.py"
      - credential-alias
      - create
      - --alias
      - kafka_oauth_dev
      - --userid
      - "{{ oauth_client_id }}"
      - --password-env
      - OAUTH_CLIENT_SECRET
      - --deployment
      - "{{ target_deployment }}"
  environment:
    OAUTH_CLIENT_SECRET: "{{ oauth_client_secret }}"   # decrypted by Ansible Vault, in-memory only
  no_log: true
```

**Interactive (on a TTY, no flag given):**
```
$ python oggctl.py credential-alias create --alias my_creds --userid AKIA... --deployment ...
Password for alias 'my_creds': ********
```

If no flag is provided and stdout is not a TTY (e.g. CI, piped invocation), the
command fails fast rather than hanging.

## Connections

Named connections link a logical connection name to an existing credential alias.

```bash
python oggctl.py connection create DB_ACNPDB \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV \
  --alias ACNPDB_PRIM_DEV

python oggctl.py connection list --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py connection delete DB_ACNPDB --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
```

## Trandata (Supplemental Logging)

`trandata` manages Oracle supplemental logging for tables and schemas through the
OGG Administration Service. Supplemental logging is required before an Extract can
capture DML changes; without it, OGG cannot reconstruct the before-image of an
UPDATE or DELETE.

All mutating operations (`add`, `delete`, `modify`) are **idempotent**: the CLI
checks the current state via `info` before acting, so it is safe to re-run during
provisioning without causing errors.

### Connection name and domain

OGG stores connections with a fully-qualified name: `{domain}.{alias}`. The default
domain is `OracleGoldenGate`, so a connection named `ACNPDB_PRIM_DEV` in the OGG UI
is stored as `OracleGoldenGate.ACNPDB_PRIM_DEV`.

The `--connection` argument accepts either form:

```bash
# Short alias — domain is prepended automatically
--connection ACNPDB_PRIM_DEV

# Fully-qualified — used as-is
--connection OracleGoldenGate.ACNPDB_PRIM_DEV
```

Use `--domain DOMAIN` to override the default (`OracleGoldenGate`) when your
connections live under a different domain.

### Add supplemental logging

Enables supplemental logging for one or more tables (or an entire schema). Skips
any table/schema that is already enabled.

```bash
# Single table
python oggctl.py trandata add \
  --connection ACNPDB_PRIM_DEV \
  --table SOE.ORDERS \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV

# Multiple tables
python oggctl.py trandata add \
  --connection ACNPDB_PRIM_DEV \
  --table SOE.ORDERS \
  --table SOE.CUSTOMERS \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV

# Entire schema
python oggctl.py trandata add \
  --connection ACNPDB_PRIM_DEV \
  --schema SOE \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
```

Enable all-column logging (required for tables without a unique key):

```bash
python oggctl.py trandata add \
  --connection ACNPDB_PRIM_DEV \
  --table SOE.ADDRESSES \
  --all-columns \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
```

> **When to use `--all-columns`:** Tables with no primary key and no suitable unique
> key (`noUniqueKeyDefined` in `check-support` output) require all-column logging.
> Without it, OGG cannot reconstruct the before-image of an UPDATE or DELETE and
> will abort with `OGG-01403`.

Specify only certain columns:

```bash
python oggctl.py trandata add \
  --connection ACNPDB_PRIM_DEV \
  --table SOE.ORDERS \
  --columns "ORDER_ID,CUSTOMER_ID,STATUS" \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
```

### Delete supplemental logging

Removes supplemental logging. Skips any table/schema that is not currently enabled.

The delete always removes both all-column and scheduling-column logging. Use
`--no-scheduling-columns` to only remove all-column logging and leave scheduling
columns in place.

```bash
# Full remove (default — removes everything)
python oggctl.py trandata delete \
  --connection ACNPDB_PRIM_DEV \
  --table SOE.ORDERS \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV

# Remove only all-column logging; keep scheduling columns
python oggctl.py trandata delete \
  --connection ACNPDB_PRIM_DEV \
  --table SOE.ORDERS \
  --no-scheduling-columns \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV

# Schema-level delete
python oggctl.py trandata delete \
  --connection ACNPDB_PRIM_DEV \
  --schema SOE \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
```

### Modify supplemental logging

Force-replaces the logging configuration (delete if enabled, then add with the
new settings). Useful when you need to switch from scheduling-columns-only to
all-column logging without a separate delete/add cycle.

```bash
python oggctl.py trandata modify \
  --connection ACNPDB_PRIM_DEV \
  --table SOE.ADDRESSES \
  --all-columns \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
```

### Check current state (info)

Shows the current supplemental logging state for a table or schema. Accepts SQL
wildcards (`%`).

```bash
# Specific table
python oggctl.py trandata info \
  --connection ACNPDB_PRIM_DEV \
  --table SOE.ORDERS \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV

# All tables in a schema (wildcard)
python oggctl.py trandata info \
  --connection ACNPDB_PRIM_DEV \
  --table "SOE.%" \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV

# Schema-level entry
python oggctl.py trandata info \
  --connection ACNPDB_PRIM_DEV \
  --schema SOE \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
```

Example output:

```
table             : SOE.ORDERS
enabled           : True
primaryKey        : ORDER_ID
schedulingColumns : True
allColumns        : False
columns           : (none specified)
```

Key fields:

| Field | Meaning |
|-------|---------|
| `enabled` | `True` if supplemental logging is active |
| `primaryKey` | Column(s) OGG identified as the key |
| `schedulingColumns` | Scheduling-column supplemental logging is on |
| `allColumns` | All-column supplemental logging is on (superset of scheduling columns) |

When `allColumns` is `True`, the `schedulingColumns` state is irrelevant — all
columns are already logged.

### Check OGG capture support

Reports whether OGG can capture changes from each table and what logging is
recommended.

```bash
python oggctl.py trandata check-support \
  --connection ACNPDB_PRIM_DEV \
  --schema SOE \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV

python oggctl.py trandata check-support \
  --connection ACNPDB_PRIM_DEV \
  --table SOE.ORDERS \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
```

The `details` field in the output is a list of codes that describe the table's
support status:

| Code | Meaning |
|------|---------|
| `tableFullSupport` | Table has a primary or unique key — standard trandata is sufficient |
| `noUniqueKeyDefined` | No primary key or usable unique key; use `--all-columns` when adding trandata |
| `disqualifiedUniqueKey` | A unique index exists but OGG cannot use it (e.g. function-based, partial); treat like `noUniqueKeyDefined` |
| `oggKeycolList` | OGG will use a custom key column list |
| `logicalReplication` | Table is configured for logical replication |

Tables with `noUniqueKeyDefined` or `disqualifiedUniqueKey` must have all-column
logging enabled (`--all-columns`) or you will get `OGG-01403` errors at runtime.

### Bulk input with `--tables-json`

All `add`, `delete`, `modify`, `info`, and `check-support` subcommands accept
`--tables-json` as an alternative to repeating `--table`. Pass either a JSON string
or `@path/to/file.json`.

**Simple array of table names** — CLI flags apply to all:

```json
["SOE.ORDERS", "SOE.CUSTOMERS", "SOE.ADDRESSES"]
```

```bash
python oggctl.py trandata add \
  --connection ACNPDB_PRIM_DEV \
  --tables-json '["SOE.ORDERS", "SOE.CUSTOMERS"]' \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
```

**Array of objects** — per-table overrides (uses OGG API camelCase keys):

```json
[
  {"tableName": "SOE.ORDERS", "schedulingColumns": true},
  {"tableName": "SOE.ADDRESSES", "allColumns": true},
  {"tableName": "SOE.CUSTOMERS"}
]
```

CLI flags serve as defaults; per-entry fields override them.

Supported JSON object keys:

| Key | CLI equivalent | Description |
|-----|----------------|-------------|
| `tableName` | `--table` | Required. `SCHEMA.TABLE` |
| `allColumns` | `--all-columns` | Enable all-column supplemental logging |
| `schedulingColumns` | `--no-scheduling-columns` (inverse) | Control scheduling-column logging |
| `columns` | `--columns` | List of specific columns to log |
| `prepareCsnMode` | `--prepare-csn-mode` | CSN preparation mode |

**From a file:**

```bash
python oggctl.py trandata add \
  --connection ACNPDB_PRIM_DEV \
  --tables-json @tables.json \
  --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
```

### Trandata options reference

| Option | Applies to | Description |
|--------|-----------|-------------|
| `--connection ALIAS` | all | OGG connection name (short alias or `Domain.Alias`) |
| `--domain DOMAIN` | all | Domain prefix when `--connection` has no dot. Default: `OracleGoldenGate` |
| `--table SCHEMA.TABLE` | all | Target table; repeatable |
| `--tables-json JSON\|@FILE` | all | JSON array of table names or objects (mutually exclusive with `--table`) |
| `--schema SCHEMA` | all | Target schema (mutually exclusive with `--table`/`--tables-json`) |
| `--all-columns` | add, modify | Enable all-column supplemental logging |
| `--columns COL,COL,...` | add, modify | Log only these columns |
| `--no-scheduling-columns` | add, modify, delete | Disable scheduling-column logging; on delete, keeps all-column logging in place |
| `--prepare-csn-mode MODE` | add, modify | CSN preparation mode |

## Users

Manage OGG service users by role.

Supported roles:

- `Administrator`
- `Operator`
- `User`
- `Security`

Examples:

```bash
python oggctl.py user list --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py user list --deployment OGG_GR_ODB_ORA_E01_AWS_DEV --role Operator
python oggctl.py user get appuser --deployment OGG_GR_ODB_ORA_E01_AWS_DEV --role Operator
python oggctl.py user create appuser --deployment OGG_GR_ODB_ORA_E01_AWS_DEV --role Operator --password-file secrets/appuser.pass --info "Automation user"
python oggctl.py user update appuser --deployment OGG_GR_ODB_ORA_E01_AWS_DEV --role Operator --password-file secrets/appuser-new.pass
python oggctl.py user delete appuser --deployment OGG_GR_ODB_ORA_E01_AWS_DEV --role Operator
```

## Deployments

Deployment commands query the Service Manager API.

```bash
python oggctl.py deployment list --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py deployment status --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py deployment status OGG_GR_ODB_ORA_E01_AWS_DEV --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py deployment info --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
python oggctl.py deployment info OGG_GR_ODB_ORA_E01_AWS_DEV --deployment OGG_GR_ODB_ORA_E01_AWS_DEV
```

When no deployment name is passed to `status` or `info`, the command uses the
`deployment` value from the selected inventory entry.

### Checking which auth method is active

```bash
python oggctl.py deployment check-auth --deployment OGG_CDC_ODB_ORA_E01_AWS_DEV
```

Fires a single lightweight request and reports which auth mode locked in for that session:

```
deployment:  OGG_CDC_ODB_ORA_E01_AWS_DEV
auth:        basic (OGG_ADMIN_PASSWORD)
http_status: 200
```

or, when the deployment has an authorization profile enabled:

```
deployment:  OGG_CDC_ODB_ORA_E01_AWS_PRO
auth:        entraid_bearer (Entra ID client-credentials)
http_status: 200
```

Useful during the migration to Entra ID to verify each deployment without running a full operation.

## Templates

`templates` is a local introspection command — it doesn't call the OGG API.
Use it to discover what variables each built-in Jinja template needs before
invoking `replicat create`, `replicat properties set --template`, or
`config-file create --template`.

```bash
python oggctl.py templates list
# kafka_confluent   replicat properties
# kafka_producer    config file
# s3                replicat properties

python oggctl.py templates show s3
# template       : s3
# kind           : replicat properties
# file           : replicat/s3.properties.j2
# description    : AWS S3 event handler — writes Parquet files to an S3 bucket via STS-assumed role
# required       : s3_bucket, s3_sts_assume_role, s3_sts_session_name, s3_credentials_alias, database, bucket_path_prefix
# optional       : aws_region=eu-west-1, s3_credentials_domain=OracleGoldenGate, ogg_staging_root=/u01/ogg/data_staging, ogg_home=/u01/ogg/ogg23ai
# auto_injected  : replicat_name
```

`--format json` works here too — useful when an AAP playbook wants to validate
the required vars programmatically before invoking `replicat create`.

Missing a required variable at render time produces a clear error
(`Template 'X' is missing required variable(s): ...`) instead of a cryptic
Jinja undefined-variable trace.

### Where template variables come from

There are three sources, applied in order (later overrides earlier):

| Source | How to supply | Examples |
|--------|--------------|----------|
| Auto-injected from inventory | automatic | `deployment_name`, `env`, `db_engine`, `replicat_name` |
| `target_connections.yaml` | `--target-name NAME` | `bootstrap_servers`, `oauth_endpoint_url`, `api_scope`, `ogg_alias_credential`, `cluster_id`, `s3_sts_assume_role`, ... |
| Per-invocation overrides | `--vars-json` / `--target-vars-json` | `topic_prefix`, `database`, `schema`, `bucket_path_prefix`, ... |

`target_connections.yaml` holds the per-environment connection details that are
shared across multiple replicats (Kafka brokers, OAuth endpoints, S3 role ARNs,
etc.). By referencing a named connection with `--target-name`, AAP playbooks
avoid duplicating these values across every `replicat create` call.

In an AAP playbook the flow is:

```yaml
- name: Create Kafka replicat
  command:
    argv:
      - python3
      - oggctl.py
      - replicat
      - create
      - "{{ replicat_name }}"
      - --target
      - kafka_confluent
      - --target-name
      - "{{ kafka_target_name }}"        # e.g. "kafka-dev" from group_vars
      - --target-vars-json
      - "{{ per_replicat_vars | to_json }}"
      - --deployment
      - "{{ ogg_deployment }}"
```

## Ansible Integration

The `playbooks/` directory contains local Ansible wrappers around `oggctl.py`.
The orchestration playbook provisions a basic CDC pipeline:

1. Create Extract on the source deployment
2. Create Distribution Path from source to target
3. Create Replicat on the target deployment

Example local run:

```bash
ansible-playbook playbooks/orchestration/provision_cdc_pipeline.yml \
  --vault-password-file ~/.vault_pass \
  -e @extra_vars.yml
```

The playbooks expect an Ansible Vault file that provides `ogg_admin_password`,
then expose it to the CLI as `OGG_ADMIN_PASSWORD`.

## Vault Helper

`create_vault.py` creates an Ansible Vault file (AES-256) containing:

```yaml
ogg_admin_password: "..."
```

The script runs on any OS without needing Ansible installed (Ansible itself
doesn't run on Windows). It only needs the `cryptography` Python package.

```bash
python create_vault.py            # interactive: prompts for Vault ID + output path + passwords
python create_vault.py vault/ogg_passwords_prod.yml
python create_vault.py --help     # full usage + AAP credential setup walkthrough
```

Each environment (dev, pre, prod, ...) should have its own:

- vault file (e.g. `vault/ogg_passwords_dev.yml`)
- Vault ID (e.g. `oggadmin-dev-credential`)
- vault password

In AAP, create one Vault credential per environment and set the matching
**Vault Identifier**. Ansible automatically decrypts only the file whose ID
matches the credential.

Commit only the encrypted vault files. Never commit vault password files —
`.vault_pass` is in `.gitignore`.

## Notes and Current Boundaries

- This CLI talks to OGG Microservices REST APIs, not classic GGSCI directly.
- Process creation builds PRM content from the templates in `templates/`.
- Extract and Replicat names are still subject to GoldenGate limits.
- Trail names must follow the validation in `api/trail.py` — exactly 2 characters.
- `task purge-trail create` validates the specified trail(s) against `GET /trails?details`
  before creating the task. The full path must be provided when the trail is in a subdirectory
  (e.g. `primary/ACNPDB/EPPI00D/pr`, not just `pr`).
- Delete operations require the target process or path to be in a deletable
  state according to the OGG API.
- Secrets should come from environment variables, password files, or Ansible
  Vault, not from committed plaintext files.
- Auth is auto-detected per session: basic is tried first; on HTTP 401, Entra ID
  Bearer is used if `entraid_*` fields are configured in the inventory. Use
  `deployment check-auth` to verify which method is active for a deployment.
- `OGG_INVENTORY` overrides the default inventory path.
- `OGG_TARGET_CONNECTIONS` overrides the default `target_connections.yaml` path.
