import os
import sys
import json
import re
import subprocess

from utils import print_log, get_matching_deployments, get_db_connection


# ─────────────────────────────────────────────────────────────────────────────
# Config loader — only the 5 fields required for config_trandata are mandatory.
# ─────────────────────────────────────────────────────────────────────────────

def _load_trandata_config(filepath):
    _UPPERCASE_FIELDS = {"env", "request_type", "bbdd_engine", "bbdd_name"}
    _PREFIX    = "variable => "
    _VAR_ALIGN = 26

    if not os.path.isfile(filepath):
        print_log("error", f"file '{filepath}' not found.")
        sys.exit(1)
    if not filepath.lower().endswith(".json"):
        print_log("error", f"file '{filepath}' is not a JSON file.")
        sys.exit(1)
    with open(filepath, "r", encoding="utf-8") as f:
        try:
            raw = json.load(f)
        except json.JSONDecodeError as e:
            print_log("error", f"file '{filepath}' is not valid JSON. {e}")
            sys.exit(1)
    if not raw:
        print_log("error", f"file '{filepath}' is empty or has no data.")
        sys.exit(1)

    config = {}
    for k, v in raw.items():
        if k.startswith("//"):
            continue
        if isinstance(v, str):
            config[k] = v.strip().upper() if k in _UPPERCASE_FIELDS else v.strip()
        else:
            config[k] = v

    print_log("info", f"{'json config file':<{len(_PREFIX) + _VAR_ALIGN}}: {os.path.basename(filepath)}")
    for k, v in config.items():
        display_val = ",".join(v) if isinstance(v, list) else str(v)
        print_log("info", f"variable => {k:<{_VAR_ALIGN}}: {display_val}")

    print_log("info", "validating variables from json config file ...")

    VALID_ENVS          = {"DEV", "PRE", "PRO"}
    VALID_REQUEST_TYPES = {"CREATE", "DELETE", "STATUS"}

    errors = []

    if not config.get("env"):
        errors.append("'env' is required")
    elif config["env"] not in VALID_ENVS:
        errors.append(f"'env' value '{config['env']}' not valid. Must be one of: {', '.join(sorted(VALID_ENVS))}")

    if not config.get("request_type"):
        errors.append("'request_type' is required")
    elif config["request_type"] not in VALID_REQUEST_TYPES:
        errors.append(f"'request_type' value '{config['request_type']}' not valid. Must be one of: {', '.join(sorted(VALID_REQUEST_TYPES))}")

    if not config.get("bbdd_engine"):
        errors.append("'bbdd_engine' is required")
    elif config["bbdd_engine"] != "ORACLE":
        errors.append(f"'bbdd_engine' value '{config['bbdd_engine']}' not valid for config_trandata. TRANDATA is Oracle-only. Must be: ORACLE")

    if not config.get("bbdd_name"):
        errors.append("'bbdd_name' is required")

    if not config.get("tables"):
        errors.append("'tables' is required and cannot be empty")
    else:
        table_pattern = re.compile(r"^\w+\.\w+(\((\*|\w+(\s*,\s*\w+)*)\))?$")
        for tbl in config["tables"]:
            if not table_pattern.match(tbl):
                errors.append(
                    f"'tables' entry '{tbl}' has invalid format. "
                    "Expected: SCHEMA.TABLE or SCHEMA.TABLE(*) or SCHEMA.TABLE(COL1,COL2)"
                )

    print_log("info", f"{'total tables detected in JSON':<{len(_PREFIX) + _VAR_ALIGN}}: {len(config.get('tables', []))}")

    if errors:
        print_log("info", f"{'json config file validation':<{len(_PREFIX) + _VAR_ALIGN}}: ERROR")
        for err in errors:
            print_log("error", f"data validation error => {err}")
        return None

    print_log("info", f"{'json config file validation':<{len(_PREFIX) + _VAR_ALIGN}}: OK")
    return config


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _strip_columns(table_entry):
    """'SCHEMA.TABLE(*)' → 'SCHEMA.TABLE'  (removes trailing column spec)."""
    return re.sub(r"\(.*\)$", "", table_entry).strip()


def _build_table_arg(pdb, schema_table):
    """Prefix with PDB catalog when the source DB is an Oracle PDB."""
    return f"{pdb}.{schema_table}" if pdb else schema_table


def _run_trandata(oggctl_path, subcommand, deployment, connection, table_arg, extra=None):
    """Run: python oggctl.py trandata <subcommand> --deployment D --connection C --table T [extra...]
    Returns (ok: bool, output: str).
    """
    cmd = [
        sys.executable, oggctl_path,
        "trandata", subcommand,
        "--deployment", deployment,
        "--connection", connection,
        "--table", table_arg,
    ]
    if extra:
        cmd.extend(extra)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        out = ((r.stdout or "") + (r.stderr or "")).strip()
        return r.returncode == 0, out
    except Exception as e:
        return False, str(e)


def _last_line(output):
    """Return the last non-empty line of command output, or 'unknown error'."""
    lines = [l for l in output.splitlines() if l.strip()]
    return lines[-1] if lines else "unknown error"


def _trandata_is_enabled(output):
    """Parse trandata info output and return True if 'enabled : True'."""
    for line in output.splitlines():
        if "enabled" in line.lower():
            return "true" in line.lower()
    return False


def _check_prerequisites(oggctl_path, dep_name, connection, db_conn, _W=38):
    """Ensure the OGG credential alias exists, matches db_connections.yaml, and can connect.

    Delegates to apply._ensure_credential_alias which handles:
      - alias missing   → create
      - userid drift    → delete + recreate
      - already OK      → skip (idempotent)
      - post-create     → validate connectivity
    Returns True if the alias is ready, False if any step fails.
    """
    from apply import _ensure_credential_alias

    print_log("info", "")
    print_log("info", "====== Check Trandata Prerequisites ======")
    print_log("info", "")

    db_user  = db_conn.get("db_user", "")
    conn_str = db_conn.get("conn_string_prim", "")
    userid   = f"{db_user}@{conn_str}" if conn_str else db_user
    password_credential = db_conn.get("password_credential", "")

    ok = _ensure_credential_alias(
        oggctl_path        = oggctl_path,
        alias              = connection,
        userid             = userid,
        password_credential= password_credential,
        dep_name           = dep_name,
        dry_run            = False,
        width              = _W,
        label              = "DB connection (alias)",
    )

    print_log("info", "")
    return ok


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def config_trandata(filepath):

    config = _load_trandata_config(filepath)
    if config is None:
        return 1

    env          = config["env"]
    bbdd_engine  = config["bbdd_engine"]
    bbdd_name    = config["bbdd_name"]
    request_type = config["request_type"]
    tables       = config.get("tables", [])

    # ── Resolve deployment ────────────────────────────────────────────────────
    # Trandata is configured via the online extract deployment (it acts against
    # the source database credential registered there).
    deployments = get_matching_deployments(
        env, bbdd_engine, load_types=["online"], proc_types=["extract"]
    )
    if not deployments:
        print_log("error",
            f"no online extract deployment found in inventory for env={env}, engine={bbdd_engine}")
        return 1
    dep_name = deployments[0][0]
    print_log("info", f"{'deployment':<38}: {dep_name}")

    # ── Resolve connection alias and PDB prefix ───────────────────────────────
    conn_key = f"{bbdd_name}_{env}"
    db_conn  = get_db_connection(conn_key)
    if not db_conn:
        print_log("error", f"connection '{conn_key}' not found in db_connections.yaml")
        return 1
    connection = db_conn.get("conn_name_prim", "")
    pdb        = db_conn.get("pdb", "")
    if not connection:
        print_log("error", f"'conn_name_prim' not set for '{conn_key}' in db_connections.yaml")
        return 1
    print_log("info", f"{'connection alias':<38}: {connection}")
    print_log("info", f"{'bbdd_name':<38}: {bbdd_name}")
    print_log("info", f"{'tables':<38}: {len(tables)}")

    # ── ogg-ctl path ──────────────────────────────────────────────────────────
    oggctl_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl", "oggctl.py"
    )

    # ── Prerequisites check ───────────────────────────────────────────────────
    if not _check_prerequisites(oggctl_path, dep_name, connection, db_conn):
        return 1

    # ── Section header ────────────────────────────────────────────────────────
    print_log("info", f"====== CONFIG TRANDATA (engine={bbdd_engine}, bbdd={bbdd_name}, env={env}) ======")
    print_log("info", "")

    ok_count  = 0
    err_count = 0

    for raw_table in tables:
        schema_table = _strip_columns(raw_table)
        table_arg    = _build_table_arg(pdb, schema_table)

        print_log("info", f"config trandata for table {table_arg}")

        if request_type == "CREATE":
            # 1. Check current state before doing anything
            ok_i, out_i = _run_trandata(oggctl_path, "info", dep_name, connection, table_arg)
            if ok_i and _trandata_is_enabled(out_i):
                print_log("info", f"{'trandata info':<38}: OK, already enabled ({table_arg})")
                ok_count += 1
                continue

            # 2. Not enabled (or info failed) — activate with all-columns
            ok, out = _run_trandata(oggctl_path, "add", dep_name, connection, table_arg, ["--all-columns"])
            if ok:
                print_log("info",  f"{'trandata add':<38}: OK ({table_arg})")
            else:
                print_log("error", f"{'trandata add':<38}: ERROR ({table_arg}: {_last_line(out)})")
                err_count += 1
                continue

            # 3. Verify
            ok_i, out_i = _run_trandata(oggctl_path, "info", dep_name, connection, table_arg)
            if ok_i:
                print_log("info",    f"{'trandata info':<38}: OK ({table_arg})")
            else:
                print_log("warning", f"{'trandata info':<38}: WARNING ({table_arg}: {_last_line(out_i)})")
            ok_count += 1

        elif request_type == "DELETE":
            ok, out = _run_trandata(oggctl_path, "delete", dep_name, connection, table_arg)
            if ok:
                print_log("info",  f"{'trandata delete':<38}: OK ({table_arg})")
            else:
                print_log("error", f"{'trandata delete':<38}: ERROR ({table_arg}: {_last_line(out)})")
                err_count += 1
                continue

            ok_i, out_i = _run_trandata(oggctl_path, "info", dep_name, connection, table_arg)
            if ok_i:
                print_log("info",    f"{'trandata info':<38}: OK ({table_arg})")
            else:
                print_log("warning", f"{'trandata info':<38}: WARNING ({table_arg}: {_last_line(out_i)})")
            ok_count += 1

        elif request_type == "STATUS":
            ok_i, out_i = _run_trandata(oggctl_path, "info", dep_name, connection, table_arg)
            if ok_i:
                print_log("info",  f"{'trandata info':<38}: OK ({table_arg})")
                ok_count += 1
            else:
                print_log("error", f"{'trandata info':<38}: ERROR ({table_arg}: {_last_line(out_i)})")
                err_count += 1

    # ── Summary ───────────────────────────────────────────────────────────────
    print_log("info", "")
    total      = len(tables)
    status_str = "OK" if err_count == 0 else "ERROR"
    lvl        = "info" if err_count == 0 else "error"
    print_log(lvl, f"{'config_trandata result':<38}: {status_str}  ({ok_count}/{total} OK, {err_count} error(s))")

    return 0 if err_count == 0 else 1
