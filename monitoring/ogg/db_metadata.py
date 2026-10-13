#!/usr/bin/env python3
"""
db_metadata.py — Oracle table metadata collection via Extract PRM.

Flow:
  1. Parse Extract PRM (USERIDALIAS, SOURCECATALOG, TABLE list)
  2. Look up db_connections.yaml by conn_name_prim == useridalias
  3. Fetch Oracle password from AWS Secrets Manager (CDC-VAULT-{ENV})
  4. Connect with python-oracledb; run ALTER SESSION SET CONTAINER if SOURCECATALOG is set
  5. Query DBA_SEGMENTS (size_mb) + DBA_TAB_STATISTICS (num_rows, last_analyzed)

Usage (library):
    from db_metadata import collect_extract_metadata

    rows = collect_extract_metadata(
        prm_lines=prm_lines,       # list[str] from get_extract_params or OGG API
        db_connections_path="../../cdc-controller/config/inventory_connections/db_connections.yaml",
    )
    # rows: [{"table": "SCHEMA.TABLE", "size_mb": 12.5, "num_rows": 500000, "last_analyzed": "..."}]
"""
from __future__ import annotations

import json
import logging
import os
from typing import Optional

import boto3
import oracledb
import yaml

# ── Oracle client mode ────────────────────────────────────────────────────────
# Thin mode (default) does not support old 10g DES password verifiers (DPY-3015).
# Set ORACLE_CLIENT_LIB to the Oracle Instant Client directory to use thick mode,
# which supports all password verifier types.
# Example: ORACLE_CLIENT_LIB=C:\oracle\instantclient_21_3
def _try_init_thick_mode() -> bool:
    lib_dir = os.environ.get("ORACLE_CLIENT_LIB", "").strip()
    try:
        if lib_dir:
            oracledb.init_oracle_client(lib_dir=lib_dir)
        else:
            oracledb.init_oracle_client()
        return True
    except Exception:
        return False

_THICK_MODE = _try_init_thick_mode()

log = logging.getLogger(__name__)

_VAULT_BY_ENV = {
    "DEV": "CDC-VAULT-DEV",
    "PRE": "CDC-VAULT-PRE",
    "PRO": "CDC-VAULT-PRO",
}


# ──────────────────────────────────────────────────────────────────────────────
# PRM PARSING
# ──────────────────────────────────────────────────────────────────────────────

def parse_prm(lines: list[str]) -> dict:
    """
    Parse Extract PRM lines.

    Returns:
        useridalias:   str | None        — credential alias (matches conn_name_prim in db_connections)
        sourcecatalog: str | None        — PDB name for ALTER SESSION SET CONTAINER
        tables:        list[str]         — SCHEMA.TABLE entries (uppercase, 2-part), active only
        wildcard:      bool              — True when any TABLE line contains '*'
        table_tags:    dict[str, dict]   — per-table TAGS parsed from inline comments,
                                           e.g. {"SCHEMA.TABLE": {"requested_by": "...", ...}}
    """
    import json as _json
    import re as _re

    result: dict = {
        "useridalias":          None,
        "sourcecatalog":        None,
        "tables":               [],
        "wildcard":             False,
        "wildcard_patterns":    [],   # e.g. ["SOE.*", "HR.EMP*"]
        "table_tags":           {},
        "hash_partition_counts": {},  # TABLE_UPPER -> int (N+1 where N is ORA_HASH modulus)
    }

    for raw_line in lines:
        line  = raw_line.strip()
        upper = line.upper()

        # Skip comment lines (-- or /* style) — covers --TABLE and -- TABLE patterns
        if upper.startswith("--") or upper.startswith("/*"):
            continue

        if upper.startswith("USERIDALIAS "):
            parts = line.split()
            if len(parts) >= 2:
                result["useridalias"] = parts[1].strip(";,").upper()

        elif upper.startswith("SOURCECATALOG "):
            parts = line.split()
            if len(parts) >= 2:
                result["sourcecatalog"] = parts[1].strip(";,").upper()

        elif upper.startswith("TABLE "):
            # Split code from inline comment on first '--'
            code_part    = line.split("--")[0]
            comment_part = line[len(code_part):]
            parts = code_part.split()
            if len(parts) >= 2:
                tbl = parts[1].strip(";, ").upper()
                if "*" in tbl:
                    result["wildcard"] = True
                    # Normalize 3-part catalog prefix (CATALOG.SCHEMA.* → SCHEMA.*)
                    if tbl.count(".") >= 2:
                        tbl = tbl[tbl.index(".") + 1:]
                    if tbl not in result["wildcard_patterns"]:
                        result["wildcard_patterns"].append(tbl)
                else:
                    # Normalize 3-part (OWNER.SCHEMA.TABLE) → 2-part (SCHEMA.TABLE)
                    if tbl.count(".") >= 2:
                        tbl = tbl[tbl.index(".") + 1:]
                    if tbl not in result["tables"]:
                        result["tables"].append(tbl)
                    # Parse TAGS JSON from inline comment if present
                    tags_match = _re.search(r"TAGS\s*(\{[^}]+\})", comment_part, _re.IGNORECASE)
                    if tags_match:
                        try:
                            result["table_tags"][tbl] = _json.loads(tags_match.group(1))
                        except _json.JSONDecodeError:
                            pass
                    # Detect ORA_HASH partitioning: SQLPREDICATE "... ORA_HASH(ROWID,N)=K ..."
                    # N is the modulus → N+1 partitions. Store so caller can adjust num_rows.
                    ora_hash_match = _re.search(
                        r"ORA_HASH\s*\(\s*ROWID\s*,\s*(\d+)\s*\)", code_part, _re.IGNORECASE
                    )
                    if ora_hash_match:
                        modulus = int(ora_hash_match.group(1))
                        result["hash_partition_counts"][tbl] = modulus + 1

    return result


def parse_replicat_prm(lines: list[str]) -> dict:
    """
    Parse Replicat PRM MAP statements to get the source-side table list and TAGS.

    Returns:
        tables:            list[str]         — SCHEMA.TABLE sources (uppercase, 2-part)
        wildcard:          bool              — True when any MAP uses '*'
        wildcard_patterns: list[str]         — source wildcard patterns e.g. ["SOE.*"]
        table_tags:        dict[str, dict]   — per-table TAGS from inline comments,
                                               e.g. {"SCHEMA.TABLE": {"requested_by": "...", ...}}
    """
    import json as _json
    import re as _re

    result: dict = {
        "tables":           [],
        "wildcard":         False,
        "wildcard_patterns": [],
        "table_tags":       {},
    }

    for raw_line in lines:
        line  = raw_line.strip()
        upper = line.upper()

        if upper.startswith("--") or upper.startswith("/*"):
            continue

        if upper.startswith("MAP "):
            code_part    = line.split("--")[0]
            comment_part = line[len(code_part):]
            # MAP SOURCE, TARGET TARGET_TABLE;
            comma_idx = code_part.find(",")
            if comma_idx < 0:
                continue
            src = code_part[4:comma_idx].strip().upper()   # skip "MAP "

            if "*" in src:
                result["wildcard"] = True
                if src.count(".") >= 2:
                    src = src[src.index(".") + 1:]
                if src not in result["wildcard_patterns"]:
                    result["wildcard_patterns"].append(src)
            else:
                if src.count(".") >= 2:
                    src = src[src.index(".") + 1:]
                if src and src not in result["tables"]:
                    result["tables"].append(src)
                tags_match = _re.search(r"TAGS\s*(\{[^}]+\})", comment_part, _re.IGNORECASE)
                if tags_match:
                    try:
                        result["table_tags"][src] = _json.loads(tags_match.group(1))
                    except _json.JSONDecodeError:
                        pass

    return result


# ──────────────────────────────────────────────────────────────────────────────
# DB CONNECTIONS
# ──────────────────────────────────────────────────────────────────────────────

def load_db_connections(path: str) -> dict:
    """
    Load db_connections.yaml, merge defaults.
    Returns dict keyed by conn_name_prim and conn_name_stby (uppercase) for Oracle entries only.
    Both aliases point to the same connection entry so that initial-load PRMs configured with
    the standby alias are still found by find_connection.
    """
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    defaults = raw.pop("defaults", {})
    result: dict[str, dict] = {}

    for _key, entry in raw.items():
        if not isinstance(entry, dict):
            continue
        merged = {**defaults, **entry}
        if merged.get("db_engine", "").upper() != "ORACLE":
            continue
        merged["db_key"] = _key  # e.g. "ACNPDB_PRO"
        conn_name = (merged.get("conn_name_prim") or "").upper()
        if conn_name:
            result[conn_name] = merged
        conn_name_stby = (merged.get("conn_name_stby") or "").upper()
        if conn_name_stby and conn_name_stby not in result:
            result[conn_name_stby] = merged

    return result


def find_connection(db_connections: dict, useridalias: str) -> Optional[dict]:
    """Return the connection entry whose conn_name_prim or conn_name_stby matches useridalias."""
    return db_connections.get(useridalias.upper())


# ──────────────────────────────────────────────────────────────────────────────
# AWS SECRETS MANAGER
# ──────────────────────────────────────────────────────────────────────────────

def get_oracle_password(
    password_credential: str,
    env: str,
    aws_region: Optional[str] = None,
) -> str:
    """
    Fetch the Oracle password from AWS Secrets Manager.

    The secret is a JSON object in CDC-VAULT-{ENV}.
    password_credential is the field name within that JSON.

    Local override (AWS SM skipped):
      1. Env var named after the credential key itself, e.g. ORACLE_ACNPDB_GGADMIN=...
      2. ORACLE_DB_PASSWORD as a global fallback for single-database setups
    """
    # Per-credential env var (matches the password_credential key in db_connections.yaml)
    per_db = os.environ.get(password_credential)
    if per_db:
        log.info("Using %s env var (AWS SM skipped)", password_credential)
        return per_db

    # Global fallback
    override = os.environ.get("ORACLE_DB_PASSWORD")
    if override:
        log.info("Using ORACLE_DB_PASSWORD env var (AWS SM skipped)")
        return override

    vault_name = _VAULT_BY_ENV.get(env.upper(), f"CDC-VAULT-{env.upper()}")
    log.debug("AWS SM: secret=%s field=%s", vault_name, password_credential)

    sm_kwargs: dict = {}
    if aws_region:
        sm_kwargs["region_name"] = aws_region

    sm = boto3.client("secretsmanager", **sm_kwargs)
    response   = sm.get_secret_value(SecretId=vault_name)
    secret_str = response.get("SecretString", "{}")
    secret     = json.loads(secret_str)

    password = secret.get(password_credential)
    if not password:
        raise ValueError(
            f"Field '{password_credential}' not found in secret '{vault_name}'"
        )
    return password


# ──────────────────────────────────────────────────────────────────────────────
# ORACLE QUERIES
# ──────────────────────────────────────────────────────────────────────────────

_SQL_SEGMENTS = """
SELECT ROUND(SUM(s.bytes) / 1024 / 1024, 2)
FROM   dba_segments s
WHERE  s.owner = UPPER(:owner)
  AND  (
         s.segment_name = UPPER(:table_name)
         OR s.segment_name IN (
             SELECT p.partition_name
             FROM   dba_tab_partitions p
             WHERE  p.table_owner = UPPER(:owner)
               AND  p.table_name  = UPPER(:table_name)
         )
       )
"""

_SQL_TAB_STATS = """
SELECT num_rows,
       TO_CHAR(last_analyzed, 'YYYY-MM-DD HH24:MI:SS')
FROM   dba_tab_statistics
WHERE  owner      = UPPER(:owner)
  AND  table_name = UPPER(:table_name)
"""

_SQL_WILDCARD_TABLES = """
SELECT OWNER || '.' || TABLE_NAME
FROM   dba_tables
WHERE  owner      = UPPER(:owner)
  AND  table_name LIKE UPPER(:pattern)
ORDER  BY table_name
"""


def get_table_metadata(
    conn_string: str,
    db_user: str,
    password: str,
    tables: list[str],
    pdb: Optional[str] = None,
    wildcard_patterns: Optional[list[str]] = None,
) -> list[dict]:
    """
    Connect to Oracle and query size + statistics for each table.

    tables:            list of 'SCHEMA.TABLE' strings (uppercase, 2-part)
    pdb:               PDB name — if set, runs ALTER SESSION SET CONTAINER before any queries.
    wildcard_patterns: list of 'SCHEMA.TABLE_GLOB' patterns (e.g. ["SOE.*", "HR.EMP*"]).
                       Resolved against DBA_TABLES inside the same connection before
                       querying metadata, so wildcard-only PRMs are handled correctly.

    Returns list of:
        {table: str, size_mb: float, num_rows: int|None, last_analyzed: str|None}
    """
    rows: list[dict] = []

    with oracledb.connect(user=db_user, password=password, dsn=conn_string) as conn:
        if pdb:
            with conn.cursor() as cur:
                log.info("ALTER SESSION SET CONTAINER = %s", pdb)
                cur.execute(f"ALTER SESSION SET CONTAINER = {pdb}")

        resolved = list(tables)
        if wildcard_patterns:
            with conn.cursor() as cur:
                for pat in wildcard_patterns:
                    if "." not in pat:
                        continue
                    schema, tbl_glob = pat.split(".", 1)
                    sql_pat = tbl_glob.replace("*", "%").replace("?", "_")
                    cur.execute(_SQL_WILDCARD_TABLES, {"owner": schema, "pattern": sql_pat})
                    for (qualified,) in cur:
                        if qualified not in resolved:
                            resolved.append(qualified)
            log.info("  Wildcard resolution: %d patterns → %d tables",
                     len(wildcard_patterns), len(resolved) - len(tables))

        with conn.cursor() as cur:
            for schema_table in resolved:
                if "." not in schema_table:
                    log.warning("Skipping malformed table reference: %s", schema_table)
                    continue

                schema, table = schema_table.split(".", 1)
                size_mb:       float        = 0.0
                num_rows:      Optional[int] = None
                last_analyzed: Optional[str] = None

                try:
                    cur.execute(_SQL_SEGMENTS, {"owner": schema, "table_name": table})
                    seg = cur.fetchone()
                    if seg and seg[0] is not None:
                        size_mb = float(seg[0])
                except Exception as exc:
                    log.warning("DBA_SEGMENTS failed for %s: %s", schema_table, exc)

                try:
                    cur.execute(_SQL_TAB_STATS, {"owner": schema, "table_name": table})
                    stat = cur.fetchone()
                    if stat:
                        num_rows      = stat[0]
                        last_analyzed = stat[1]
                except Exception as exc:
                    log.warning("DBA_TAB_STATISTICS failed for %s: %s", schema_table, exc)

                rows.append({
                    "table":         schema_table,
                    "size_mb":       size_mb,
                    "num_rows":      num_rows,
                    "last_analyzed": last_analyzed,
                })

    return rows


# ──────────────────────────────────────────────────────────────────────────────
# HIGH-LEVEL ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

def collect_extract_metadata(
    prm_lines: list[str],
    db_connections_path: str,
    aws_region: Optional[str] = None,
) -> list[dict]:
    """
    Full pipeline: parse Extract PRM → look up connection → query Oracle metadata.

    Args:
        prm_lines:           PRM content as list of strings.
                             Obtain via get_extract_params() from ogg-ctl or the OGG REST API.
        db_connections_path: Path to cdc-controller's db_connections.yaml.
        aws_region:          AWS region override (default: uses boto3 env / ~/.aws/config).

    Returns:
        list of {table, size_mb, num_rows, last_analyzed}
        Empty list if PRM uses wildcards or has no USERIDALIAS.
    """
    prm = parse_prm(prm_lines)

    if prm["wildcard"]:
        log.warning("Extract uses wildcard TABLE — metadata collection skipped")
        return []

    if not prm["useridalias"]:
        log.warning("No USERIDALIAS in PRM — cannot look up database connection")
        return []

    if not prm["tables"]:
        log.warning("No TABLE entries found in PRM")
        return []

    db_conns  = load_db_connections(db_connections_path)
    conn_info = find_connection(db_conns, prm["useridalias"])
    if not conn_info:
        raise ValueError(
            f"No Oracle connection found for USERIDALIAS='{prm['useridalias']}' "
            f"in {db_connections_path}"
        )

    env      = conn_info.get("env", "DEV")
    password = get_oracle_password(conn_info["password_credential"], env, aws_region)

    # SOURCECATALOG (from PRM) is authoritative; fall back to pdb field in yaml
    pdb = prm["sourcecatalog"] or conn_info.get("pdb") or None

    log.info(
        "Oracle: user=%s dsn=%.60s... pdb=%s tables=%d",
        conn_info["db_user"],
        conn_info["conn_string_prim"],
        pdb or "none",
        len(prm["tables"]),
    )

    return get_table_metadata(
        conn_string=conn_info["conn_string_prim"],
        db_user=conn_info["db_user"],
        password=password,
        tables=prm["tables"],
        pdb=pdb,
    )


# ──────────────────────────────────────────────────────────────────────────────
# CLI — local testing
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import json as _json
    import os
    import sys
    from pathlib import Path

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-5s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    _REPO_ROOT = Path(__file__).resolve().parents[2]
    _DEFAULT_DB_CONN = str(
        _REPO_ROOT / "cdc-controller" / "config" / "inventory_connections" / "db_connections.yaml"
    )

    parser = argparse.ArgumentParser(
        description="Query Oracle table metadata for an Extract's TABLE list.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment variables:
  <password_credential>  Per-database password (key from db_connections.yaml password_credential field).
                         E.g. ORACLE_ACNPDB_GGADMIN=xxx  or  ORACLE_MD01_GGADMIN=yyy
  ORACLE_DB_PASSWORD     Global fallback if no per-database var is set (single-DB setups).
  ORACLE_CLIENT_LIB      Oracle Instant Client directory for thick mode (needed for old 10g verifiers).

Examples:
  # Use a saved PRM file:
  python db_metadata.py --prm-file ECDC01.prm

  # Pipe PRM from stdin:
  cat ECDC01.prm | python db_metadata.py --prm-file -

  # Specify a custom db_connections.yaml:
  python db_metadata.py --prm-file ECDC01.prm --db-connections /path/to/db_connections.yaml
""",
    )
    parser.add_argument(
        "--prm-file", required=True, metavar="FILE",
        help="Path to the Extract PRM file (use '-' to read from stdin).",
    )
    parser.add_argument(
        "--db-connections", default=_DEFAULT_DB_CONN, metavar="PATH",
        help=f"Path to db_connections.yaml (default: {_DEFAULT_DB_CONN})",
    )
    parser.add_argument(
        "--aws-region", default=None, metavar="REGION",
        help="AWS region for Secrets Manager (default: boto3 env/config).",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Output as JSON array instead of a table.",
    )
    args = parser.parse_args()

    # Read PRM lines
    if args.prm_file == "-":
        prm_lines = sys.stdin.read().splitlines()
    else:
        prm_lines = Path(args.prm_file).read_text(encoding="utf-8").splitlines()

    # Show parsed PRM summary before connecting
    prm = parse_prm(prm_lines)
    print(f"USERIDALIAS : {prm['useridalias'] or '(not found)'}")
    print(f"SOURCECATALOG: {prm['sourcecatalog'] or '(not set)'}")
    print(f"Tables      : {len(prm['tables'])} {'(wildcard)' if prm['wildcard'] else ''}")
    for t in prm["tables"]:
        print(f"  {t}")
    print()

    if prm["wildcard"] or not prm["useridalias"] or not prm["tables"]:
        print("Nothing to query — check PRM content above.")
        sys.exit(0)

    rows = collect_extract_metadata(prm_lines, args.db_connections, args.aws_region)

    if args.json:
        print(_json.dumps(rows, indent=2, default=str))
    else:
        if not rows:
            print("No rows returned.")
        else:
            header = f"{'TABLE':<45} {'SIZE_MB':>10} {'NUM_ROWS':>12} {'LAST_ANALYZED':<22}"
            print(header)
            print("-" * len(header))
            for r in rows:
                num_rows = r["num_rows"] if r["num_rows"] is not None else "-"
                last_ana = r["last_analyzed"] or "-"
                print(f"{r['table']:<45} {r['size_mb']:>10.2f} {str(num_rows):>12} {last_ana:<22}")
