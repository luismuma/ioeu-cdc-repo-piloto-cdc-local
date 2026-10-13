#!/usr/bin/env python3
"""
ogg_report.py — OGG daily monitoring report via REST API.

Loads a YAML inventory, queries each OGG MA deployment via REST API v2,
and renders a self-contained HTML report using a Jinja2 template.
Replaces: ogg_report.sh, ogg_api_report.sh, ogg_html_report.py.

Usage:
    python3 ogg_report.py [-i inventory.yaml] [-d DEPLOY_KEY] [--no-reset] [--out path.html]

Credentials — environment variables only, never written to disk:
    OGG_API_USER / OGG_API_PASS         Basic Auth (primary)
    OGG_AUTH_PROFILE_TENANT_ID          Entra ID tenant UUID
    OGG_AUTH_PROFILE_CLIENT_ID          Entra ID client UUID (per-env override in inventory)
    <entraid_client_secret value>       Env-var name stored in inventory field entraid_client_secret
"""
from __future__ import annotations

import json
import logging
import os
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import urllib3
import yaml
from jinja2 import Environment, FileSystemLoader

# ── oggctl API (shared client + functions, no duplication) ────────────────────
_OGGCTL = Path(__file__).resolve().parents[2] / "ogg-ctl"
if str(_OGGCTL) not in sys.path:
    sys.path.insert(0, str(_OGGCTL))

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from api.client import OGGClient                                           # noqa: E402
from api.errors import OGGAPIError                                         # noqa: E402
from api.extract_ops import (                                              # noqa: E402
    list_extracts, get_extract_params,
    get_extract_info      as _get_extract_info,
    get_extract_history   as _get_extract_history,
    get_table_stats       as _get_extract_stats,
    list_extract_reports  as _list_extract_reports,
    get_extract_report    as _get_extract_report,
)
from api.replicat_ops import (                                             # noqa: E402
    list_replicats, get_replicat_params,
    get_replicat_info      as _get_replicat_info,
    get_replicat_history   as _get_replicat_history,
    get_table_stats        as _get_replicat_stats,
    list_replicat_reports  as _list_replicat_reports,
    get_replicat_report    as _get_replicat_report,
)
from api.deployment_ops import get_messages                                # noqa: E402

try:
    from db_metadata import (
        parse_prm, parse_replicat_prm, load_db_connections, find_connection,
        get_oracle_password, get_table_metadata,
    )
    _DB_METADATA_OK = True
except ImportError:
    _DB_METADATA_OK = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-5s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# INVENTORY
# ──────────────────────────────────────────────────────────────────────────────

def load_inventory(path: str) -> list[dict]:
    """Load YAML inventory, merging defaults into every deployment entry."""
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    defaults = raw.pop("defaults", {})
    deps = []
    for key, entry in raw.items():
        if not isinstance(entry, dict):
            continue
        dep = {**defaults, **entry}
        dep.setdefault("deployment", key)
        lt = dep.get("load_type", "online").lower()
        dep["grupo"] = "InitialLoad" if "initial" in lt else "Online"
        m = re.match(r"https?://([^/]+)", dep.get("admin_url", ""))
        dep["host"] = m.group(1) if m else key
        deps.append(dep)
    return deps


# ──────────────────────────────────────────────────────────────────────────────
# CLIENT FACTORY
# ──────────────────────────────────────────────────────────────────────────────

def _build_ogg_client(dep: dict, env: str) -> OGGClient:
    """Build an OGGClient (from oggctl) for a deployment inventory entry.

    Uses nginx_proxy=False so the admin_url (the nginx gateway) is addressed
    directly at /services/v2/..., which correctly routes all endpoints including
    POST /extracts/{name}/command for STATS queries.
    """
    import json as _json
    env_lc     = env.lower()
    tenant_id  = dep.get("entraid_tenant_id", "")
    client_id  = dep.get(f"entraid_client_id_{env_lc}") or dep.get("entraid_client_id", "")
    secret_var = dep.get("entraid_client_secret", "OGG_AUTH_PROFILE_CLIENT_SECRET")

    # Per-env vault JSON (injected by playbook as OGG_VAULT_DEV/PRE/PRO).
    # Falls back to empty dict so ogg-ctl single-env usage is unaffected.
    _vault_raw = os.environ.get(f"OGG_VAULT_{env.upper()}", "") or ""
    _vault     = _json.loads(_vault_raw) if _vault_raw.strip() else {}
    ogg_password   = _vault.get("OGG_OGGADMIN", "")           # empty → OGGClient reads OGG_ADMIN_PASSWORD
    client_secret  = _vault.get("OGG_AUTH_PROFILE_CLIENT_SECRET", "")

    entra_cfg  = (
        {"tenant_id": tenant_id, "client_id": client_id,
         "secret_env_name": secret_var, "secret_value": client_secret}
        if tenant_id and client_id else None
    )
    return OGGClient(
        admin_url       = dep["admin_url"],
        api_version     = dep.get("api_version", "v2"),
        user            = dep.get("admin_user") or os.environ.get("OGG_ADMIN_USER") or "oggadmin",
        password        = ogg_password or None,   # None → OGGClient falls back to OGG_ADMIN_PASSWORD env var
        verify_tls      = dep.get("verify_tls", False),
        nginx_proxy     = False,
        deployment_name = dep.get("deployment", ""),
        timeout         = tuple(dep.get("timeout", [30, 180])),
        entra_cfg       = entra_cfg,
    )


def _get_json(client: OGGClient, path: str) -> dict:
    """GET path and return parsed JSON (empty dict on any error)."""
    try:
        r = client.get(path)
        if r.status_code in (200, 201, 206):
            return r.json()
        log.warning("HTTP %s: %s", r.status_code, path)
    except OGGAPIError as exc:
        log.warning("GET %s: %s", path, exc)
    return {}


# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _norm(s: object) -> str:
    return str(s or "UNKNOWN").upper()


def _safe_int(v: object) -> int:
    try:
        s = str(v).strip()
        return int(float(s)) if s not in ("", "-", "N/A", "null", "NULL") else 0
    except (ValueError, TypeError):
        return 0


def _items(data: dict, *keys) -> list:
    """Extract list from API response, trying multiple key paths."""
    resp = data.get("response", data)
    for k in keys:
        val = resp.get(k)
        if isinstance(val, list):
            return val
    return []


def parse_lag(raw: object) -> str:
    """Normalise OGG lag to 'Xd:HH:MM:SS' or 'HH:MM:SS', empty string if zero.

    Accepts: ISO 8601 duration (PT5M3S), HH:MM:SS, Xd:HH:MM:SS, or integer/float seconds.
    """
    s = str(raw or "").strip()
    if not s or s in ("null", "PT0S", "PT", "P0D", "P"):
        return ""
    secs = 0
    if s.startswith("P"):
        m = re.match(r"P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?", s)
        if m:
            d, h, mi, sec = (float(g or 0) for g in m.groups())
            secs = int(d * 86400 + h * 3600 + mi * 60 + sec)
    elif re.match(r"^\d+d[\s:]", s):
        m = re.match(r"^(\d+)d[\s:](\d{1,2}):(\d{2}):(\d{2})", s)
        if m:
            secs = int(m[1])*86400 + int(m[2])*3600 + int(m[3])*60 + int(m[4])
    elif re.match(r"^\d{1,2}:\d{2}:\d{2}", s):
        m = re.match(r"^(\d{1,2}):(\d{2}):(\d{2})", s)
        if m:
            secs = int(m[1])*3600 + int(m[2])*60 + int(m[3])
    elif re.match(r"^\d+(\.\d+)?$", s):
        # Raw integer or float seconds (e.g. from /info/status lag field)
        secs = int(float(s))
    else:
        return s
    if secs == 0:
        return ""
    days, rem = divmod(secs, 86400)
    hh, rem   = divmod(rem, 3600)
    mm, ss    = divmod(rem, 60)
    return f"{days}d:{hh:02d}:{mm:02d}:{ss:02d}" if days else f"{hh:02d}:{mm:02d}:{ss:02d}"


def calc_elapsed(since_str: str) -> str:
    if not since_str or since_str in ("null", "-", ""):
        return "Sin actividad"
    try:
        dt   = datetime.fromisoformat(since_str.replace("Z", "+00:00"))
        diff = max(int((datetime.now(timezone.utc) - dt).total_seconds()), 0)
        days, rem = divmod(diff, 86400)
        hh, rem   = divmod(rem, 3600)
        mm, ss    = divmod(rem, 60)
        return f"{days}d {hh:02d}:{mm:02d}:{ss:02d}" if days else f"{hh:02d}:{mm:02d}:{ss:02d}"
    except Exception:
        return "Sin actividad"


def classify_proc(proc_type: str, name: str) -> str:
    """Classify EXTRACT/REPLICAT into CDC/INI subtypes by name prefix."""
    n = name.upper()
    if proc_type == "EXTRACT":
        return "EXTR_INI" if n.startswith("EI") else "EXTR_CDC"
    if proc_type == "REPLICAT":
        return "REPL_INI" if n.startswith("RI") else "REPL_CDC"
    return proc_type


# ──────────────────────────────────────────────────────────────────────────────
# API QUERY FUNCTIONS
# ──────────────────────────────────────────────────────────────────────────────

def query_health(client: OGGClient, dep_name: str = "") -> list[dict]:
    data = _get_json(client, "/config/health")
    resp = data.get("response", data)
    # OGG MA returns services under different keys depending on version:
    # - "services" / "items" (direct deployment endpoint)
    # - "criticalResources" (Service Manager health, lists services for all deployments)
    svc_list = (resp.get("services") or resp.get("items") or
                resp.get("criticalResources") or [])
    svcs = []
    for svc in svc_list:
        name   = svc.get("name") or svc.get("serviceName", "")
        status = _norm(svc.get("status") or svc.get("serviceStatus"))
        if not name:
            continue
        # criticalResources mixes services from all deployments — keep only ours
        svc_dep = svc.get("deploymentName", "")
        if dep_name and svc_dep and svc_dep != dep_name:
            continue
        svcs.append({"name": name, "estado": status})
    return svcs


def _query_procs_enriched(client: OGGClient, proc_type: str) -> list[dict]:
    """List EXTRACT or REPLICAT processes, enriched with lag/lastStarted/description."""
    try:
        items = list_extracts(client) if proc_type == "EXTRACT" else list_replicats(client)
    except OGGAPIError as exc:
        log.warning("list_%ss failed: %s", proc_type.lower(), exc)
        return []

    _get_info    = _get_extract_info    if proc_type == "EXTRACT" else _get_replicat_info
    _get_history = _get_extract_history if proc_type == "EXTRACT" else _get_replicat_history
    endpoint     = "extracts"           if proc_type == "EXTRACT" else "replicats"

    procs = []
    for item in items:
        name   = item.get("name", "")
        status = _norm(item.get("status", ""))
        if not name:
            continue

        proc = {"name": name, "status": status, "type": proc_type,
                "lag": "", "last_started": "", "last_stopped": "", "desc": ""}

        # Runtime status: lag + lastStarted
        try:
            info = _get_info(client, name)
            proc["lag"]          = parse_lag(info.get("lag") or "")
            proc["last_started"] = info.get("lastStarted") or ""
        except OGGAPIError:
            pass

        # For STOPPED processes, fetch run history to get actual completion time.
        # history[0].finished = ISO timestamp of when the most recent run stopped.
        if status == "STOPPED":
            history = _get_history(client, name)
            if history:
                proc["last_stopped"] = history[0].get("finished") or ""

        # Description (config endpoint)
        cfg = _get_json(client, f"/{endpoint}/{name}")
        proc["desc"] = cfg.get("response", cfg).get("description") or ""

        procs.append(proc)
    return procs


def query_dist_paths(client: OGGClient) -> list[dict]:
    data = _get_json(client, "/sources")
    paths = []
    for item in _items(data, "items", "sources"):
        name   = item.get("name") or item.get("sourceName") or item.get("pathName", "")
        status = _norm(item.get("status") or item.get("sourceStatus") or item.get("pathStatus"))
        if name:
            paths.append({"name": name, "status": status, "type": "DIST_PATH",
                          "lag": "", "last_started": "", "desc": ""})
    return paths


def query_recv_paths(client: OGGClient) -> list[dict]:
    data = _get_json(client, "/targets")
    paths = []
    for item in _items(data, "items", "targets"):
        name   = item.get("name") or item.get("targetName") or item.get("pathName", "")
        status = _norm(item.get("status") or item.get("targetStatus") or item.get("pathStatus"))
        if name:
            paths.append({"name": name, "status": status, "type": "RCVR_PATH",
                          "lag": "", "last_started": "", "desc": ""})
    return paths


def query_errors_24h(client: OGGClient) -> int:
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
        msgs = get_messages(client, severity="error", since=cutoff)
        return len(msgs) if isinstance(msgs, list) else 0
    except Exception as exc:
        log.debug("errors_24h unavailable: %s", exc)
        return 0


def _stats_rows(daily: list[dict], total: list[dict]) -> list[dict]:
    """Merge oggctl daily + total stats rows into the report's internal format.

    oggctl get_table_stats returns: [{table, since, insert, update, delete, upsert, discard}]
    """
    total_lkp = {r["table"].upper(): r for r in total}
    rows = []
    for dr in daily:
        tbl = dr["table"].upper()
        tr  = total_lkp.get(tbl, {})
        rows.append({
            "tabla":    tbl,
            "since":    dr.get("since", ""),
            "elapsed":  calc_elapsed(dr.get("since", "")),
            "inserts":  dr.get("insert",  0), "updates":  dr.get("update",  0),
            "deletes":  dr.get("delete",  0), "upserts":  dr.get("upsert",  0),
            "discards": dr.get("discard", 0),
            "tot_ins":  tr.get("insert",  dr.get("insert",  0)),
            "tot_upd":  tr.get("update",  dr.get("update",  0)),
            "tot_del":  tr.get("delete",  dr.get("delete",  0)),
            "tot_ups":  tr.get("upsert",  dr.get("upsert",  0)),
            "tot_dis":  tr.get("discard", dr.get("discard", 0)),
        })
    return rows


def query_process_stats(client: OGGClient, proc_type: str, name: str) -> list[dict]:
    """Return per-table DAILY + TOTAL stats via oggctl get_table_stats."""
    _get_stats = _get_extract_stats if proc_type == "EXTRACT" else _get_replicat_stats
    try:
        daily = _get_stats(client, name, "daily")
    except OGGAPIError as exc:
        log.warning("  %s %s — STATS DAILY failed: %s", proc_type, name, exc)
        return []

    if not daily:
        log.info("  %s %s — STATS DAILY empty (no DML today)", proc_type, name)
        return []

    try:
        total = _get_stats(client, name, "total")
    except OGGAPIError:
        total = []

    return _stats_rows(daily, total)


# ──────────────────────────────────────────────────────────────────────────────
# ORACLE DB METADATA (table size + statistics via Extract PRM)
# ──────────────────────────────────────────────────────────────────────────────

def _query_extract_db_meta(
    client: OGGClient,
    extract_name: str,
    db_conns: dict | None,
    aws_region: str | None,
    is_initial_load: bool = False,
) -> tuple[list[str], dict[str, dict], dict[str, dict], str]:
    """Parse the extract's PRM and optionally query Oracle for table metadata.

    Returns (prm_tables, oracle_meta, table_tags, db_key):
      prm_tables  — list of SCHEMA.TABLE names from PRM (populated without Oracle)
      oracle_meta — {TABLE_UPPER: {size_mb, num_rows, last_analyzed}} (empty if Oracle unavailable)
      table_tags  — {TABLE_UPPER: {requested_by, cost_center, ...}} parsed from TAGS inline comments
      db_key      — YAML key from db_connections.yaml (e.g. "ACNPDB_PRO"), "" if not resolved
    """
    prm_tables: list[str] = []
    if not _DB_METADATA_OK:
        return prm_tables, {}, {}, ""

    # ── Step 1: parse PRM to get table names + tags (no Oracle needed) ────────
    try:
        prm_lines = get_extract_params(client, extract_name)
    except Exception as exc:
        log.debug("PRM fetch for %s: %s", extract_name, exc)
        return prm_tables, {}, {}, ""

    if not prm_lines:
        return prm_tables, {}, {}, ""

    prm = parse_prm(prm_lines)
    wildcard_patterns = prm.get("wildcard_patterns", [])
    has_specific      = bool(prm["tables"])
    has_wildcards     = bool(wildcard_patterns)

    if not has_specific and not has_wildcards:
        # PRM has no TABLE entries at all
        return prm_tables, {}, {}, prm.get("sourcecatalog") or ""

    prm_tables = prm["tables"]
    table_tags = {t.upper(): v for t, v in prm.get("table_tags", {}).items()}

    # ── Step 2: Oracle metadata (optional — requires db_conns and password) ───
    if not db_conns or not prm["useridalias"]:
        # No DB connection config — use PDB name from PRM (sourcecatalog) as BBDD fallback.
        # Wildcard tables cannot be resolved without a DB connection, so prm_tables stays
        # empty for wildcard-only PRMs; BBDD column will still show the correct PDB name.
        return prm_tables, {}, table_tags, prm.get("sourcecatalog") or ""

    db_key = ""
    try:
        conn_info = find_connection(db_conns, prm["useridalias"])
        if not conn_info:
            log.info("No DB connection for USERIDALIAS=%s (extract %s) — Oracle row counts unavailable",
                     prm["useridalias"], extract_name)
            return prm_tables, {}, table_tags, prm.get("sourcecatalog") or ""

        db_key   = conn_info.get("db_key", "")
        env      = conn_info.get("env", "DEV")
        password = get_oracle_password(conn_info["password_credential"], env, aws_region)
        pdb      = prm["sourcecatalog"] or conn_info.get("pdb") or None

        # Initial load uses standby when available (reduces load on primary)
        stby = conn_info.get("conn_string_stby", "")
        conn_string = stby if (is_initial_load and stby) else conn_info["conn_string_prim"]
        conn_label  = "stby" if (is_initial_load and stby) else "prim"

        log.info("  DB meta %s — %d tables + %d wildcard patterns, pdb=%s, conn=%s (%s)",
                 extract_name, len(prm_tables), len(wildcard_patterns), pdb or "none", db_key, conn_label)
        rows = get_table_metadata(
            conn_string=conn_string,
            db_user=conn_info["db_user"],
            password=password,
            tables=prm_tables,
            pdb=pdb,
            wildcard_patterns=wildcard_patterns or None,
        )
        # prm_tables may have been empty (wildcard-only PRM); update it with resolved tables
        resolved_tables = [r["table"].upper() for r in rows]
        if has_wildcards and not has_specific:
            prm_tables = resolved_tables

        # Adjust num_rows for hash-partitioned initial loads (SQLPREDICATE ORA_HASH).
        # Oracle's DBA_TAB_STATISTICS gives the full table count; divide by the number
        # of partitions so progress and ETA are calculated per-partition, not per-table.
        hash_counts = prm.get("hash_partition_counts", {})
        oracle_meta = {}
        for r in rows:
            tbl_upper = r["table"].upper()
            entry = dict(r)
            n_parts = hash_counts.get(tbl_upper)
            if n_parts and n_parts > 1 and entry.get("num_rows"):
                entry["num_rows"] = max(1, entry["num_rows"] // n_parts)
            oracle_meta[tbl_upper] = entry

        return prm_tables, oracle_meta, table_tags, db_key
    except Exception as exc:
        msg = str(exc)
        if "DPY-3015" in msg or "password verifier" in msg.lower():
            log.warning(
                "DB metadata for extract %s failed: Oracle uses an old password verifier "
                "not supported in thin mode. Install Oracle Instant Client and set "
                "ORACLE_CLIENT_LIB=<path> to enable thick mode.",
                extract_name,
            )
        else:
            log.warning("DB metadata for extract %s failed: %s", extract_name, exc)
        return prm_tables, {}, table_tags, db_key


def _query_replicat_tables(client, replicat_name: str) -> tuple[list[str], dict]:
    """
    Fetch the replicat PRM and return (table_list, table_tags) from MAP statements.
    Returns ([], {}) on error, wildcard-only PRMs, or when db_metadata is unavailable.
    """
    if not _DB_METADATA_OK:
        return [], {}
    try:
        prm_lines = get_replicat_params(client, replicat_name)
    except Exception as exc:
        log.debug("Replicat PRM fetch for %s: %s", replicat_name, exc)
        return [], {}
    if not prm_lines:
        return [], {}
    prm = parse_replicat_prm(prm_lines)
    return prm["tables"], prm.get("table_tags", {})


# ──────────────────────────────────────────────────────────────────────────────
# PER-DEPLOYMENT COLLECTION
# ──────────────────────────────────────────────────────────────────────────────

def _derive_role(dep: dict) -> str:
    proc_type = dep.get("proc_type", "").lower()
    if proc_type == "extract":
        return "Extractor"
    if proc_type == "replicat":
        return "Replicator"
    u = dep.get("deployment", "").upper()
    if "_R0" in u or "DAA" in u:
        return "Replicator"
    if "_E0" in u or "ORA" in u:
        return "Extractor"
    return "Deployment"


def collect_deployment(dep: dict, db_conns: dict | None = None, aws_region: str | None = None) -> dict:
    """Query all OGG API endpoints for one deployment. Always returns a dict."""
    dep_name = dep["deployment"]
    host     = dep["host"]
    grupo    = dep["grupo"]
    env      = dep.get("env", "").upper()
    role     = _derive_role(dep)
    log.info("━━━ %s  (%s)", dep_name, dep.get("admin_url", ""))

    base = {
        "dep_name": dep_name, "host": host, "grupo": grupo,
        "env": env, "role_base": role, "s3_bucket": dep.get("s3_bucket", ""),
    }

    client = _build_ogg_client(dep, env)

    try:
        services = query_health(client, dep_name)
    except Exception as exc:
        log.error("Health check failed for %s: %s", dep_name, exc)
        services = []

    if not services:
        log.warning("%s — unreachable (health returned no services)", dep_name)
        return {**base, "unreachable": True, "services": [],
                "processes": [], "stats": [], "errors_24h": 0}

    procs: list[dict] = []
    for proc_type in ("EXTRACT", "REPLICAT"):
        procs.extend(_query_procs_enriched(client, proc_type))
    for fn in (query_dist_paths, query_recv_paths):
        try:
            procs.extend(fn(client))
        except Exception as exc:
            log.warning("%s — %s error: %s", dep_name, fn.__name__, exc)

    errors_24h = 0
    try:
        errors_24h = query_errors_24h(client)
    except Exception as exc:
        log.debug("%s — errors_24h unavailable: %s", dep_name, exc)

    all_stats: list[dict] = []
    for proc in procs:
        if proc["type"] not in ("EXTRACT", "REPLICAT"):
            continue

        # ── DML stats (DAILY since midnight) ─────────────────────────────────
        try:
            rows = query_process_stats(client, proc["type"], proc["name"])
        except Exception as exc:
            log.warning("%s/%s stats error: %s", dep_name, proc["name"], exc)
            rows = []

        # ── Oracle DB metadata (size, rows, last_analyzed) + PRM tags ───────────
        # For EXTRACTs: parse PRM for table names + TAGS (always), then optionally
        # query Oracle for size/num_rows/last_analyzed (requires db_conns).
        prm_tables: list[str] = []
        meta: dict[str, dict] = {}
        table_tags: dict[str, dict] = {}
        db_key = ""
        is_oracle = dep.get("db_engine", "").upper() == "ORACLE"
        if proc["type"] == "EXTRACT" and is_oracle:
            proc_tipo      = classify_proc(proc["type"], proc["name"])
            is_initial_load = proc_tipo == "EXTR_INI"
            prm_tables, meta, table_tags, db_key = _query_extract_db_meta(
                client, proc["name"], db_conns, aws_region, is_initial_load=is_initial_load
            )
        elif proc["type"] == "REPLICAT" and is_oracle:
            prm_tables, repl_tags = _query_replicat_tables(client, proc["name"])
            table_tags.update(repl_tags)

        # ── Add zero-DML rows for tables in PRM/Oracle not covered by stats ──
        tbl_in_rows = {r["tabla"].upper() for r in rows}
        all_known   = {t.upper() for t in prm_tables} | set(meta.keys())
        for tbl in sorted(all_known - tbl_in_rows):
            tags = table_tags.get(tbl, {})
            rows.append({
                "tabla":       tbl,
                "since":       "", "elapsed":  "",
                "inserts":     0, "updates":  0, "deletes":  0,
                "upserts":     0, "discards": 0,
                "tot_ins":     0, "tot_upd":  0, "tot_del":  0,
                "tot_ups":     0, "tot_dis":  0,
                "team":        tags.get("requested_by", ""),
                "cost_center": tags.get("cost_center", ""),
            })

        # ── Enrich stats rows with metadata + process context ─────────────────
        for r in rows:
            tbl_upper = r["tabla"].upper()
            m    = meta.get(tbl_upper, {})
            tags = table_tags.get(tbl_upper, {})
            r["size_mb"]       = m.get("size_mb")
            r["num_rows"]      = m.get("num_rows")
            r["last_analyzed"] = m.get("last_analyzed")
            r.setdefault("team",        tags.get("requested_by", ""))
            r.setdefault("cost_center", tags.get("cost_center",  ""))
            r.update({
                "proceso":           proc["name"],
                "proc_type":         proc["type"],
                "proc_status":       proc["status"],
                "bbdd":              db_key or dep.get("db_engine", "-"),
                "db_engine":         dep.get("db_engine", "-"),
                "proc_lag":          proc.get("lag", ""),
                "proc_last_started": proc.get("last_started", ""),
                "proc_last_stopped": proc.get("last_stopped", ""),
                "proc_desc":         proc.get("desc", ""),
            })

        # For STOPPED initial load processes: STATS API returns nothing once stopped;
        # the OGG report file retains the final Run Time Statistics section.
        # Fetch it to recover the insert count and show real progress.
        _proc_tipo = classify_proc(proc["type"], proc["name"])
        if (proc["status"] == "STOPPED"
                and rows
                and not any((r.get("tot_ins") or 0) > 0 for r in rows)):
            if _proc_tipo == "EXTR_INI":
                report_ins = _fetch_ini_report_inserts(client, proc["name"])
            elif _proc_tipo == "REPL_INI":
                report_ins = _fetch_ini_replicat_report_inserts(client, proc["name"])
            else:
                report_ins = None
            if report_ins:
                rows[0]["tot_ins"] = report_ins
                rows[0]["inserts"] = report_ins

        all_stats.extend(rows)
        log.info("  %s %s [%s] — %d tables", proc["type"], proc["name"],
                 proc["status"], len(rows))

    MAIN_SVCS = {"adminsrvr", "distsrvr", "pmsrvr", "recvsrvr"}
    main_down = [s for s in services if s["name"] in MAIN_SVCS and s["estado"] != "RUNNING"]
    overall   = "WARNING" if main_down else "OK"

    return {**base, "unreachable": False, "overall": overall,
            "services": services, "processes": procs,
            "stats": all_stats, "errors_24h": errors_24h}


def _fmt_duration(seconds: float | None) -> str:
    """Format a duration in seconds as 'Xd Yh', 'Yh Zm', 'Zm', or '< 1m'."""
    if seconds is None:
        return "—"
    s = int(seconds)
    days  = s // 86400
    hours = (s % 86400) // 3600
    mins  = (s % 3600)  // 60
    if days  > 0: return f"{days}d {hours}h"
    if hours > 0: return f"{hours}h {mins}m"
    if mins  > 0: return f"{mins}m"
    return "< 1m"


def _parse_report_inserts(lines: list[str]) -> int | None:
    """Return the largest inserts value in the 'Run Time Statistics' section.

    Coordinated replicat reports list per-thread counts alongside the global
    total; taking the max gives the overall total without summing duplicates.
    """
    in_stats = False
    best = 0
    for line in lines:
        if "Run Time Statistics" in line:
            in_stats = True
            continue
        if in_stats:
            m = re.search(r"inserts\s*:\s*([\d,]+)", line, re.IGNORECASE)
            if m:
                val = int(m.group(1).replace(",", ""))
                if val > best:
                    best = val
    return best if best > 0 else None


def _fetch_report_inserts_max(
    list_fn, get_fn, proc_type: str, proc_name: str, client: OGGClient
) -> int | None:
    """Scan all OGG report files for a stopped process and return the highest insert count.

    A process accumulates multiple report files across restarts (OGG rolls the report
    on each start). The highest count across all reports is the relevant completed run.
    STATS API returns empty for stopped processes; the report file is the only source.
    """
    try:
        report_names = list_fn(client, proc_name)
    except Exception as exc:
        log.warning("List reports for %s %s failed: %s", proc_type, proc_name, exc)
        report_names = []

    if not report_names:
        report_names = [f"{proc_name}.rpt"]

    best = 0
    for rname in report_names:
        try:
            lines = get_fn(client, proc_name, rname)
            count = _parse_report_inserts(lines) or 0
            if count > best:
                best = count
        except Exception as exc:
            log.warning("Report fetch for %s %s failed: %s", proc_type, proc_name, exc)

    if best == 0:
        log.warning("%s %s — no 'Run Time Statistics' inserts found in %d report(s)",
                    proc_type, proc_name, len(report_names))
        return None

    log.info("  %s %s — best report inserts: %s (across %d report(s))",
             proc_type, proc_name, fmt_num(best), len(report_names))
    return best


def _fetch_ini_report_inserts(client: OGGClient, extract_name: str) -> int | None:
    return _fetch_report_inserts_max(
        _list_extract_reports, _get_extract_report, "EXTRACT", extract_name, client
    )


def _fetch_ini_replicat_report_inserts(client: OGGClient, replicat_name: str) -> int | None:
    return _fetch_report_inserts_max(
        _list_replicat_reports, _get_replicat_report, "REPLICAT", replicat_name, client
    )


def _build_ini_entry(proc_name: str, rows: list[dict], now) -> dict:
    """Aggregate per-table stats rows for one EXTR_INI/REPL_INI process."""
    sample = rows[0]

    started_str = sample.get("last_started", "")
    stopped_str = sample.get("last_stopped", "")
    estado      = sample.get("estado", "").upper()

    dt_start: datetime | None = None
    if started_str:
        try:
            dt_start = datetime.fromisoformat(started_str.replace("Z", "+00:00"))
            if dt_start.tzinfo is None:
                dt_start = dt_start.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            pass

    # For STOPPED processes use stop-time so elapsed doesn't grow after completion.
    # If lastStopped is not available from the API, elapsed is left as None (shown as —).
    # For RUNNING processes use now so elapsed reflects current wall-clock runtime.
    elapsed_sec: float | None = None
    completed_str: str | None = None
    if dt_start:
        if estado == "STOPPED" and stopped_str:
            try:
                dt_stop = datetime.fromisoformat(stopped_str.replace("Z", "+00:00"))
                if dt_stop.tzinfo is None:
                    dt_stop = dt_stop.replace(tzinfo=timezone.utc)
                elapsed_sec   = max((dt_stop - dt_start).total_seconds(), 1)
                completed_str = dt_stop.strftime("%Y-%m-%d %H:%M")
            except (ValueError, TypeError):
                pass  # leave elapsed_sec = None
        elif estado != "STOPPED":
            elapsed_sec = max((now - dt_start).total_seconds(), 1)
        # STOPPED without lastStopped: elapsed stays None

    total_copied = sum(r.get("tot_ins", 0) or 0 for r in rows)
    valid_rows   = [r for r in rows if (r.get("num_rows") or 0) > 0]
    total_rows   = sum(r["num_rows"] for r in valid_rows) if valid_rows else None

    rate_per_sec: float | None = None
    if elapsed_sec and total_copied > 0:
        rate_per_sec = total_copied / elapsed_sec

    progress_pct: float | None = None
    eta_sec:      float | None = None
    if total_rows:
        progress_pct = min(total_copied / total_rows * 100, 100.0)
        if rate_per_sec and rate_per_sec > 0:
            eta_sec = max(total_rows - total_copied, 0) / rate_per_sec

    tables = []
    for r in sorted(rows, key=lambda x: x["tabla"]):
        ti  = r.get("tot_ins", 0) or 0
        nr  = r.get("num_rows") or None
        pct = min(ti / nr * 100, 100.0) if nr else None
        tables.append({"tabla": r["tabla"], "tot_ins": ti,
                       "num_rows": nr, "progress_pct": pct})

    return {
        "proc_name":    proc_name,
        "dep_name":     sample.get("dep", ""),
        "env":          sample.get("env", ""),
        "grupo":        sample.get("grupo", ""),
        "estado":       sample.get("estado", ""),
        "started_str":  started_str,
        "completed_str": completed_str,
        "elapsed_sec":  elapsed_sec,
        "total_copied": total_copied,
        "in_trail":     0,
        "total_rows":   total_rows,
        "rate_per_sec": rate_per_sec,
        "progress_pct": progress_pct,
        "eta_sec":      eta_sec,
        "tables":       tables,
        "desc":         sample.get("desc", "") or "",
    }


def _proc_pair_suffix(name: str) -> str:
    """Strip the 2-char EI/RI type prefix to get the pairing key (e.g. EI_FOO → _FOO)."""
    return name[2:] if name[:2].upper() in ("EI", "RI") else name


def _avg_rate_by_start_group(procs: list[dict]) -> float | None:
    """Compute avg-of-group-averages for display rate.

    Groups RUNNING+STOPPED processes (not PENDING) by start minute, averages
    rate within each group, then returns the average of those group averages.
    This gives equal weight to each deployment wave regardless of batch size.
    """
    groups: dict[str, list[float]] = {}
    for p in procs:
        if p.get("estado") == "PENDING":
            continue
        rate = p.get("rate_per_sec") or 0
        if rate <= 0:
            continue
        key = (p.get("started_str") or "")[:16] or "__notime__"
        groups.setdefault(key, []).append(rate)
    if not groups:
        return None
    group_avgs = [sum(rates) / len(rates) for rates in groups.values()]
    return sum(group_avgs) / len(group_avgs)


def _compute_ini_summary(extracts: list[dict], replicats: list[dict], pending: list[dict]) -> dict:
    """Aggregate extract/replicat/pending entries into a summary dict."""
    all_r = replicats + pending
    total_oracle_rows = sum(e.get("total_rows") or 0 for e in extracts) or None
    total_extracted   = sum(e["total_copied"] for e in extracts)
    total_replicated  = sum(r["total_copied"] for r in replicats)
    in_trail          = max(total_extracted - total_replicated, 0)
    # SUM of running rates — used for ETA (parallel throughput = rows processed per second total)
    agg_extr_rate = sum(e["rate_per_sec"] for e in extracts
                        if e["estado"] == "RUNNING" and (e.get("rate_per_sec") or 0) > 0) or None
    agg_repl_rate = sum(r["rate_per_sec"] for r in replicats
                        if r["estado"] == "RUNNING" and (r.get("rate_per_sec") or 0) > 0) or None
    # Avg-of-group-averages (group by start minute) — used for display rate
    display_extr_rate = _avg_rate_by_start_group(extracts)
    display_repl_rate = _avg_rate_by_start_group(replicats)
    extr_rem = max((total_oracle_rows or 0) - total_extracted, 0)
    repl_rem = max((total_oracle_rows or 0) - total_replicated, 0)
    extract_eta  = (extr_rem / agg_extr_rate)  if (agg_extr_rate  and extr_rem > 0) else (0.0 if extr_rem == 0  else None)
    replicat_eta = (repl_rem / agg_repl_rate)  if (agg_repl_rate  and repl_rem > 0) else (0.0 if repl_rem == 0  else None)
    n_extr_running = sum(1 for e in extracts  if e["estado"] == "RUNNING")
    n_extr_done    = sum(1 for e in extracts  if e["estado"] == "STOPPED")
    n_repl_running = sum(1 for r in replicats if r["estado"] == "RUNNING")
    n_repl_done    = sum(1 for r in replicats if r["estado"] == "STOPPED")
    if replicat_eta is not None:
        total_eta  = replicat_eta + (extract_eta or 0)
        bottleneck = ("replicación" if (extract_eta or 0) < replicat_eta else "extracción") if extract_eta else "replicación"
    elif extract_eta is not None and extract_eta > 0:
        total_eta, bottleneck = None, "extracción"
    else:
        total_eta, bottleneck = None, None
    overall_pct = min(total_replicated / total_oracle_rows * 100, 100.0) if total_oracle_rows else None
    extract_pct = min(total_extracted  / total_oracle_rows * 100, 100.0) if total_oracle_rows else None
    return {
        "total_oracle_rows":    total_oracle_rows,
        "total_extracted":      total_extracted,
        "total_replicated":     total_replicated,
        "in_trail":             in_trail,
        "agg_extract_rate":     agg_extr_rate,
        "agg_replicat_rate":    agg_repl_rate,
        "display_extract_rate": display_extr_rate,
        "display_repl_rate":    display_repl_rate,
        "extract_eta_sec":      extract_eta,
        "replicat_eta_sec":     replicat_eta,
        "total_eta_sec":        total_eta,
        "bottleneck":           bottleneck,
        "overall_progress_pct": overall_pct,
        "overall_extract_pct":  extract_pct,
        "n_extr_running":  n_extr_running,  "n_extr_done": n_extr_done,  "n_extr_total": len(extracts),
        "n_repl_running":  n_repl_running,  "n_repl_done": n_repl_done,  "n_repl_pending": len(pending),
        "n_repl_total":    len(all_r),
    }


def build_ini_load_data(stats_rows: list[dict]) -> dict:
    """Aggregate EXTR_INI and REPL_INI stats rows into per-process progress/ETA entries.

    Returns {"extracts": [...], "replicats": [...], "summary": {...}, "summaries_by_env": {...}}
    """
    _ENV_ORD = {"DEV": 0, "PRE": 1, "PRO": 2}
    by_extr: dict[str, list[dict]] = defaultdict(list)
    by_repl: dict[str, list[dict]] = defaultdict(list)
    for row in stats_rows:
        tipo = row.get("tipo", "")
        if tipo == "EXTR_INI":
            by_extr[row["proceso"]].append(row)
        elif tipo == "REPL_INI":
            by_repl[row["proceso"]].append(row)

    if not by_extr and not by_repl:
        return {"extracts": [], "replicats": []}

    now = datetime.now(timezone.utc)

    extracts = [_build_ini_entry(p, rows, now) for p, rows in by_extr.items()]
    replicats = [_build_ini_entry(p, rows, now) for p, rows in by_repl.items()]

    # Pair each replicat to the corresponding extract by (env, name_suffix).
    # dep_name differs between extract and replicat deployments (ORA vs DAA), so env is used.
    extr_idx: dict[tuple, dict] = {
        (e["env"], _proc_pair_suffix(e["proc_name"])): e for e in extracts
    }
    for r in replicats:
        key = (r["env"], _proc_pair_suffix(r["proc_name"]))
        pe  = extr_idx.get(key)
        r["paired_extr"] = pe
        # Borrow total_rows from the paired extract when the replicat has no Oracle data.
        # Priority: extract's Oracle count (DBA_TAB_STATISTICS) > extract's copied count (when done).
        if pe and r["total_rows"] is None:
            extr_total = pe.get("total_rows") or (
                pe["total_copied"]
                if pe.get("estado", "").upper() in ("STOPPED", "ABENDED") and pe.get("total_copied", 0) > 0
                else None
            )
            if extr_total:
                r["total_rows"] = extr_total
                r["progress_pct"] = min(r["total_copied"] / extr_total * 100, 100.0)
                if r.get("rate_per_sec") and r["rate_per_sec"] > 0:
                    r["eta_sec"] = max(extr_total - r["total_copied"], 0) / r["rate_per_sec"]

    # ── Pending replicats: extracts with no paired replicat ───────────────────
    paired_extr_names = {r["paired_extr"]["proc_name"] for r in replicats if r.get("paired_extr")}

    running_repl = [r for r in replicats if r["estado"] == "RUNNING" and (r.get("rate_per_sec") or 0) > 0]
    avg_repl_rate = (sum(r["rate_per_sec"] for r in running_repl) / len(running_repl)) if running_repl else None

    pending_replicats: list[dict] = []
    for e in extracts:
        if e["proc_name"] in paired_extr_names:
            continue
        extr_name = e["proc_name"]
        repl_name = ("RI" + extr_name[2:]) if extr_name[:2].upper() == "EI" else ("RI_" + extr_name)
        total_rows = e.get("total_rows") or (e["total_copied"] if e["total_copied"] > 0 else None)
        in_trail   = e["total_copied"]
        eta_sec    = (in_trail / avg_repl_rate) if (avg_repl_rate and in_trail > 0) else None
        pending_replicats.append({
            "proc_name":    repl_name,
            "dep_name":     e["dep_name"],
            "env":          e["env"],
            "grupo":        e["grupo"],
            "estado":       "PENDING",
            "started_str":  None,
            "elapsed_sec":  None,
            "total_copied": 0,
            "in_trail":     in_trail,
            "total_rows":   total_rows,
            "rate_per_sec": avg_repl_rate,
            "progress_pct": 0.0 if total_rows else None,
            "eta_sec":      eta_sec,
            "tables":       e["tables"],
            "desc":         e.get("desc", ""),
            "paired_extr":  e,
        })

    all_replicats = replicats + pending_replicats

    # ── Overall summary ────────────────────────────────────────────────────────
    summary = _compute_ini_summary(extracts, replicats, pending_replicats)

    # ── Per-env summaries (for JS filter reactivity) ───────────────────────────
    envs_seen = sorted(
        {e["env"] for e in extracts} | {r["env"] for r in replicats},
        key=lambda e: _ENV_ORD.get(e, 9),
    )
    summaries_by_env: dict[str, dict] = {"": summary}
    for _env in envs_seen:
        summaries_by_env[_env] = _compute_ini_summary(
            [e for e in extracts         if e["env"] == _env],
            [r for r in replicats        if r["env"] == _env],
            [p for p in pending_replicats if p["env"] == _env],
        )

    sort_key = lambda x: (_ENV_ORD.get(x["env"], 9), x["proc_name"])
    extracts.sort(key=sort_key)
    all_replicats.sort(key=sort_key)
    return {"extracts": extracts, "replicats": all_replicats,
            "summary": summary, "summaries_by_env": summaries_by_env}


# ──────────────────────────────────────────────────────────────────────────────
# AGGREGATE REPORT DATA
# ──────────────────────────────────────────────────────────────────────────────

def build_report_data(results: list[dict]) -> dict:
    """Transform per-deployment results into the data structures used by the template."""

    # ── hosts ─────────────────────────────────────────────────────────────────
    host_map: dict[str, dict] = {}
    for r in results:
        h = r["host"]
        if h not in host_map:
            host_map[h] = {"host": h, "grupo": r["grupo"],
                           "estado": "UNREACHABLE" if r.get("unreachable") else "RUNNING",
                           "deps": []}
        if r.get("unreachable"):
            host_map[h]["estado"] = "UNREACHABLE"
        if r["dep_name"] not in host_map[h]["deps"]:
            host_map[h]["deps"].append(r["dep_name"])

    # ── deployments ───────────────────────────────────────────────────────────
    deployments: dict[str, dict] = {}
    for r in results:
        if r.get("unreachable"):
            continue
        dep_name = r["dep_name"]
        groups: dict[str, dict] = defaultdict(
            lambda: {"running": 0, "stopped": 0, "abended": 0, "killed": 0, "names": []})
        for proc in r["processes"]:
            t = classify_proc(proc["type"], proc["name"]) if proc["type"] in ("EXTRACT", "REPLICAT") else proc["type"]
            g = groups[t]
            e = proc["status"]
            if   e == "RUNNING": g["running"] += 1
            elif e == "STOPPED": g["stopped"] += 1
            elif e == "ABENDED": g["abended"] += 1
            elif e == "KILLED":  g["killed"]  += 1
            g["names"].append({"name": proc["name"], "estado": e})
        deployments[dep_name] = {
            "host": r["host"], "grupo": r["grupo"], "env": r["env"],
            "role_base": r["role_base"], "overall": r.get("overall", "OK"),
            "services": r["services"],
            "groups": {k: dict(v) for k, v in groups.items()},
            "errors_24h": r["errors_24h"],
        }

    # Enrich host entries with env list and error count
    # Use all results (including unreachable) so env is populated even for down hosts.
    dep_env_map = {r["dep_name"]: r.get("env", "") for r in results}
    hosts = list(host_map.values())
    for h in hosts:
        h["envs"]       = sorted({dep_env_map.get(d, "") for d in h["deps"]} - {""})
        h["errors_24h"] = sum(deployments[d].get("errors_24h", 0)
                              for d in h["deps"] if d in deployments)

    # ── all_procs ──────────────────────────────────────────────────────────────
    all_procs: list[dict] = []
    for r in results:
        if r.get("unreachable"):
            continue
        for proc in r["processes"]:
            if proc["type"] in ("DIST_PATH", "RCVR_PATH"):
                tipo = proc["type"]
                lag  = "—"
            else:
                tipo    = classify_proc(proc["type"], proc["name"])
                raw_lag = proc.get("lag", "")
                lag     = raw_lag if (raw_lag and not re.match(r"^0+[d:]?0*:0*:0*$", raw_lag)) else "unknown"
            all_procs.append({
                "nombre":       proc["name"],
                "tipo":         tipo,
                "env":          r["env"],
                "grupo":        r["grupo"],
                "dep":          r["dep_name"],
                "estado":       proc["status"],
                "lag":          lag,
                "desc":         proc.get("desc") or "—",
                "last_started": proc.get("last_started", ""),
                "n_tablas":     0,
            })

    # ── stats_rows ─────────────────────────────────────────────────────────────
    stats_rows: list[dict] = []
    for r in results:
        if r.get("unreachable"):
            continue
        for s in r["stats"]:
            tipo = classify_proc(s["proc_type"], s["proceso"])
            stats_rows.append({
                "tabla":        s["tabla"],
                "bbdd":         s.get("bbdd", "-"),
                "proceso":      s.get("proceso", ""),
                "dep":          r["dep_name"],
                "tipo":         tipo,
                "env":          r["env"],
                "grupo":        r["grupo"],
                "estado":       s.get("proc_status", "UNKNOWN"),
                "lag":          s.get("proc_lag", ""),
                "last_started": s.get("proc_last_started", ""),
                "last_stopped": s.get("proc_last_stopped", ""),
                "desc":         s.get("proc_desc", ""),
                "inserts":  s.get("inserts",  0), "updates":  s.get("updates",  0),
                "deletes":  s.get("deletes",  0), "upserts":  s.get("upserts",  0),
                "discards": s.get("discards", 0),
                "tot_ins":  s.get("tot_ins",  s.get("inserts",  0)),
                "tot_upd":  s.get("tot_upd",  s.get("updates",  0)),
                "tot_del":  s.get("tot_del",  s.get("deletes",  0)),
                "tot_ups":  s.get("tot_ups",  s.get("upserts",  0)),
                "tot_dis":  s.get("tot_dis",  s.get("discards", 0)),
                "size_mb":       s.get("size_mb"),
                "num_rows":      s.get("num_rows"),
                "last_analyzed": s.get("last_analyzed"),
                "team":          s.get("team", ""),
                "cost_center":   s.get("cost_center", ""),
                "db_engine":     s.get("db_engine", "-"),
            })

    stats_rows.sort(key=lambda x: (
        x["env"],
        x.get("bbdd", ""),
        x.get("tipo", ""),
        x.get("estado", ""),
        x["tabla"],
    ))

    # Propagate Oracle size_mb from extract rows to matching replicat rows
    _extr_size_lut: dict[str, float] = {
        row["tabla"].upper(): row["size_mb"]
        for row in stats_rows
        if row["tipo"] in {"EXTR_INI", "EXTR_CDC"} and row.get("size_mb") is not None
    }
    for row in stats_rows:
        if row["tipo"] in {"REPL_INI", "REPL_CDC"} and row.get("size_mb") is None:
            row["size_mb"] = _extr_size_lut.get(row["tabla"].upper())

    # Fill n_tablas + last_started for all_procs from stats
    tables_count: dict[str, int] = defaultdict(int)
    ls_lookup:    dict[str, str] = {}
    for s in stats_rows:
        tables_count[s["proceso"]] += 1
        if s["proceso"] not in ls_lookup and s["last_started"]:
            ls_lookup[s["proceso"]] = s["last_started"]
    for p in all_procs:
        p["n_tablas"]     = tables_count.get(p["nombre"], 0)
        if not p["last_started"]:
            p["last_started"] = ls_lookup.get(p["nombre"], "")

    # ── kpis ───────────────────────────────────────────────────────────────────
    extract_tipos  = {"EXTR_CDC", "EXTR_INI", "DIST_PATH"}
    replicat_tipos = {"REPL_CDC", "REPL_INI", "RCVR_PATH"}
    unreachable    = [h["host"] for h in hosts if h["estado"] == "UNREACHABLE"]
    kpis = {
        "total_hosts":       len(hosts),
        "unreachable":       len(unreachable),
        "ext_running":       sum(1 for p in all_procs if p["estado"] == "RUNNING" and p["tipo"] in extract_tipos),
        "repl_running":      sum(1 for p in all_procs if p["estado"] == "RUNNING" and p["tipo"] in replicat_tipos),
        "ext_error":         sum(1 for p in all_procs if p["estado"] in ("ABENDED", "KILLED", "STOPPED") and p["tipo"] in extract_tipos),
        "repl_error":        sum(1 for p in all_procs if p["estado"] in ("ABENDED", "KILLED", "STOPPED") and p["tipo"] in replicat_tipos),
        "unreachable_hosts": unreachable,
    }

    # Tab counter matrix for JS
    def _cnt(items, grupo, env):
        return sum(1 for it in items
                   if (not grupo or it.get("grupo") == grupo)
                   and (not env   or it.get("env")   == env))
    GS = ["", "Online", "InitialLoad"]
    ES = ["", "DEV", "PRE", "PRO"]
    counts_by_filter = {
        "procs":  {f"{g}|{e}": _cnt(all_procs,  g, e) for g in GS for e in ES},
        "tables": {f"{g}|{e}": _cnt(stats_rows, g, e) for g in GS for e in ES},
    }

    # Deployment display order
    def _dep_key(name):
        d = deployments[name]
        return (
            {"Online": 0, "InitialLoad": 1}.get(d["grupo"], 2),
            {"DEV": 0, "PRE": 1, "PRO": 2}.get(d["env"], 3),
            {"Extractor": 0, "Replicator": 1}.get(d["role_base"], 2),
            name,
        )
    dep_order     = sorted(deployments.keys(), key=_dep_key)
    ini_load_data = build_ini_load_data(stats_rows)

    # ── team_summary (replicat-side attribution only) ─────────────────────────
    _REPL_TIPOS = {"REPL_CDC", "REPL_INI"}

    def _new_team_bucket() -> dict:
        return {
            "tables": set(), "processes": set(), "envs": set(), "deps": set(),
            "repl_ini_procs": set(), "repl_cdc_procs": set(),
            "inserts": 0, "updates": 0, "deletes": 0,
            "tot_ins": 0, "tot_upd": 0, "tot_del": 0,
            "size_mb": None, "num_rows": None,
        }

    def _accum_team_bucket(b: dict, row: dict) -> None:
        b["tables"].add(row["tabla"])
        b["processes"].add(row["proceso"])
        b["envs"].add(row["env"])
        b["deps"].add(row["dep"])
        if row.get("tipo") == "REPL_INI":
            b["repl_ini_procs"].add(row["proceso"])
        elif row.get("tipo") == "REPL_CDC":
            b["repl_cdc_procs"].add(row["proceso"])
        b["inserts"]  += row.get("inserts",  0) or 0
        b["updates"]  += row.get("updates",  0) or 0
        b["deletes"]  += row.get("deletes",  0) or 0
        b["tot_ins"]  += row.get("tot_ins",  0) or 0
        b["tot_upd"]  += row.get("tot_upd",  0) or 0
        b["tot_del"]  += row.get("tot_del",  0) or 0
        if row.get("size_mb") is not None:
            b["size_mb"] = (b["size_mb"] or 0) + row["size_mb"]
        if row.get("num_rows") is not None:
            b["num_rows"] = (b["num_rows"] or 0) + row["num_rows"]

    team_agg:     dict[tuple, dict] = {}
    team_env_agg: dict[tuple, dict] = {}

    for row in stats_rows:
        if row["tipo"] not in _REPL_TIPOS:
            continue
        team = row.get("team") or ""
        cc   = row.get("cost_center") or ""
        env  = row["env"]
        key_t  = (team, cc)
        key_te = (team, cc, env)
        if key_t  not in team_agg:     team_agg[key_t]     = _new_team_bucket()
        if key_te not in team_env_agg: team_env_agg[key_te] = _new_team_bucket()
        _accum_team_bucket(team_agg[key_t],      row)
        _accum_team_bucket(team_env_agg[key_te], row)

    _tot_tables  = sum(len(b["tables"])    for b in team_agg.values())
    _tot_dml     = sum(b["inserts"] + b["updates"] + b["deletes"] for b in team_agg.values())
    _tot_tot_dml = sum(b["tot_ins"] + b["tot_upd"] + b["tot_del"] for b in team_agg.values())
    _tot_procs   = sum(len(b["processes"]) for b in team_agg.values())
    _tot_size    = sum(b["size_mb"] for b in team_agg.values() if b["size_mb"] is not None) or None

    def _pct(num: float, den: float):
        return round(100 * num / den, 1) if den else None

    def _finalize_team(b: dict, team: str, cc: str) -> dict:
        dml     = b["inserts"] + b["updates"] + b["deletes"]
        tot_dml = b["tot_ins"] + b["tot_upd"] + b["tot_del"]
        return {
            "team":         team or "Sin equipo",
            "cost_center":  cc or "—",
            "unattributed": not team,
            "tables":       sorted(b["tables"]),
            "table_count":  len(b["tables"]),
            "processes":    sorted(b["processes"]),
            "proc_count":   len(b["processes"]),
            "repl_ini_count": len(b["repl_ini_procs"]),
            "repl_cdc_count": len(b["repl_cdc_procs"]),
            "envs":         sorted(b["envs"]),
            "deps":         sorted(b["deps"]),
            "inserts":  b["inserts"], "updates": b["updates"], "deletes": b["deletes"],
            "dml":      dml,
            "tot_ins":  b["tot_ins"], "tot_upd": b["tot_upd"], "tot_del": b["tot_del"],
            "tot_dml":  tot_dml,
            "size_mb":  b["size_mb"],
            "num_rows": b["num_rows"],
            "pct_tables": _pct(len(b["tables"]),    _tot_tables),
            "pct_dml":    _pct(dml,                  _tot_dml),
            "pct_tot_dml":_pct(tot_dml,              _tot_tot_dml),
            "pct_procs":  _pct(len(b["processes"]), _tot_procs),
            "pct_size":   _pct(b["size_mb"] or 0,   _tot_size or 1) if _tot_size else None,
        }

    by_team = sorted(
        [_finalize_team(b, k[0], k[1]) for k, b in team_agg.items()],
        key=lambda x: (x["unattributed"], -(x["dml"] or 0)),
    )
    by_team_env = sorted(
        [dict(_finalize_team(b, k[0], k[1]), env=k[2]) for k, b in team_env_agg.items()],
        key=lambda x: (x["unattributed"], x["team"], x["env"]),
    )
    team_summary = {
        "by_team":     by_team,
        "by_team_env": by_team_env,
        "totals": {
            "tables":  _tot_tables,
            "dml":     _tot_dml,
            "tot_dml": _tot_tot_dml,
            "procs":   _tot_procs,
            "size_mb": _tot_size,
        },
    }

    return {
        "kpis": kpis, "hosts": hosts, "deployments": deployments,
        "dep_order": dep_order, "all_procs": all_procs, "stats_rows": stats_rows,
        "counts_by_filter": counts_by_filter, "ini_load_data": ini_load_data,
        "team_summary": team_summary,
    }


# ──────────────────────────────────────────────────────────────────────────────
# JINJA2 FILTERS
# ──────────────────────────────────────────────────────────────────────────────

def fmt_num(n) -> str:
    if n is None or n == 0:  return "—"
    if n >= 1_000_000_000:   return f"{n/1e9:.1f}B"
    if n >= 1_000_000:       return f"{n/1e6:.1f}M"
    if n >= 1_000:           return f"{n/1e3:.1f}K"
    return str(n)


def fmt_size(mb) -> str:
    if mb is None or mb == 0: return "—"
    if mb >= 1024: return f"{mb/1024:.1f} GB"
    return f"{round(mb)} MB"


def lag_class(lag: str) -> str:
    """Return CSS class for lag display: lag-red (≥3d), lag-amber (any lag), text-muted (unknown), '' (none)."""
    if lag == "unknown":
        return "text-muted"
    if not lag or lag == "—":
        return ""
    m = re.match(r"^(\d+)d:", lag)
    if m and int(m.group(1)) >= 3:
        return "lag-red"
    return "lag-amber"


# ──────────────────────────────────────────────────────────────────────────────
# RENDER
# ──────────────────────────────────────────────────────────────────────────────

def render_html(ctx: dict, templates_dir: str) -> str:
    env = Environment(loader=FileSystemLoader(templates_dir), autoescape=False)
    env.filters["fmt_num"]      = fmt_num
    env.filters["fmt_size"]     = fmt_size
    env.filters["lag_class"]    = lag_class
    env.filters["fmt_duration"] = _fmt_duration
    env.filters["tojson"]       = lambda obj: json.dumps(obj, ensure_ascii=False)
    return env.get_template("report.html.j2").render(**ctx)


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="OGG daily monitoring report via REST API")
    parser.add_argument("-i", "--inventory",     default="ogg_inventory.yaml")
    parser.add_argument("-d", "--deployment",    default="", help="Filter: process only this deployment key")
    parser.add_argument("-e", "--env",           default="", metavar="ENV",
                        help="Filter by environment (DEV, PRE, PRO). Case-insensitive.")
    parser.add_argument("--no-reset",            action="store_true", help="Skip stats reset after reading")
    parser.add_argument("--out",                 default=None, help="Output HTML path")
    parser.add_argument("--templates-dir",       default=None)
    parser.add_argument(
        "--db-connections", default=None, metavar="PATH",
        help="Path to db_connections.yaml (cdc-controller). "
             "When provided, each Extract's PRM is fetched and Oracle is queried "
             "for table size and row count. Set ORACLE_DB_PASSWORD to skip AWS SM.",
    )
    parser.add_argument("--aws-region", default=None, metavar="REGION",
                        help="AWS region for Secrets Manager (default: boto3 env/config)")
    args = parser.parse_args()

    script_dir    = Path(__file__).parent
    templates_dir = args.templates_dir or str(script_dir / "templates")

    inv_path = args.inventory
    if not Path(inv_path).is_file():
        log.error("Inventory not found: %s", inv_path)
        sys.exit(1)

    deps = load_inventory(inv_path)
    if args.deployment:
        keys = {k.strip() for k in args.deployment.split(",") if k.strip()}
        deps = [d for d in deps if d["deployment"] in keys]
    if args.env:
        deps = [d for d in deps if d.get("env", "").upper() == args.env.upper()]
    if not deps:
        log.error("No deployments to process")
        sys.exit(1)

    log.info("Inventory: %s  (%d deployments)", inv_path, len(deps))
    _user    = os.environ.get("OGG_ADMIN_USER", "")
    _pwd     = os.environ.get("OGG_ADMIN_PASSWORD", "")
    _entra_tid_env = os.environ.get("OGG_AUTH_PROFILE_TENANT_ID", "")
    _has_vault = any(os.environ.get(f"OGG_VAULT_{e}") for e in ("DEV", "PRE", "PRO"))
    # Entra may also be configured per-deployment in inventory (entraid_tenant_id field)
    _entra_inv = any(d.get("entraid_tenant_id") and
                     (d.get("entraid_client_id") or d.get(f"entraid_client_id_{d.get('env','').lower()}"))
                     for d in deps)
    _has_basic = bool(_has_vault or (_user and _pwd))
    _has_entra = bool(_has_vault or _entra_tid_env or _entra_inv)
    if _has_vault:
        log.info("Auth: per-env vault (OGG_VAULT_DEV/PRE/PRO) — Basic + Entra ID per deployment")
    elif _has_basic and _has_entra:
        log.info("Auth: Basic (%s) with Entra ID fallback", _user)
    elif _has_basic:
        log.info("Auth: Basic (%s) only", _user)
    elif _has_entra:
        log.info("Auth: Entra ID only%s", f" ({_user})" if _user else "")
    else:
        log.error("OGG credentials not set. Provide OGG_ADMIN_USER + OGG_ADMIN_PASSWORD "
                  "or configure entraid_* fields in inventory for Entra ID.")
        sys.exit(1)

    # Load Oracle DB connections if requested
    db_conns: dict | None = None
    if args.db_connections:
        _db_path = Path(args.db_connections)
        if not _DB_METADATA_OK:
            log.warning("--db-connections ignored: db_metadata.py not importable "
                        "(run: pip install oracledb boto3 pyyaml)")
        elif not _db_path.is_file():
            log.warning("--db-connections path not found: %s", _db_path)
        else:
            db_conns = load_db_connections(str(_db_path))
            log.info("DB connections: %d Oracle entries from %s", len(db_conns), _db_path)
            # Password is resolved per-credential at collection time (ORACLE_<key>, ORACLE_DB_PASSWORD, or AWS SM)

    # Parallel collection
    from functools import partial
    import time as _time

    def _prog(msg: str) -> None:
        """Print a timestamped progress line to stdout immediately."""
        ts = _time.strftime("%H:%M:%S")
        print(f"[{ts}] {msg}", flush=True)

    collect_fn = partial(collect_deployment, db_conns=db_conns, aws_region=args.aws_region)

    n       = len(deps)
    workers = min(n, 8)
    _prog(f"Starting collection — {n} deployment{'s' if n != 1 else ''} (up to {workers} parallel)")
    by_env: dict[str, list[str]] = {}
    for d in deps:
        by_env.setdefault(d.get("env", "?"), []).append(d["deployment"])
    for env_name, names in sorted(by_env.items()):
        _prog(f"  {env_name}: {', '.join(names)}")

    results: list[dict] = []
    done = unreachable = 0
    start_times: dict[str, float] = {}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures: dict = {}
        for d in deps:
            start_times[d["deployment"]] = _time.monotonic()
            futures[pool.submit(collect_fn, d)] = d

        for fut in as_completed(futures):
            done += 1
            dep     = futures[fut]
            dep_name = dep["deployment"]
            elapsed  = _time.monotonic() - start_times[dep_name]
            try:
                r = fut.result()
                if r.get("unreachable"):
                    unreachable += 1
                    _prog(f"  [{done:>{len(str(n))}}/{n}] ✗ {dep_name} — UNREACHABLE ({elapsed:.1f}s)")
                else:
                    n_procs = len(r.get("processes", []))
                    _prog(f"  [{done:>{len(str(n))}}/{n}] ✓ {dep_name} — {n_procs} proc{'s' if n_procs != 1 else ''} ({elapsed:.1f}s)")
            except Exception as exc:
                unreachable += 1
                log.error("Unhandled error for %s: %s", dep_name, exc)
                _prog(f"  [{done:>{len(str(n))}}/{n}] ✗ {dep_name} — ERROR: {exc} ({elapsed:.1f}s)")
                r = {
                    "dep_name": dep_name, "host": dep["host"],
                    "grupo": dep["grupo"], "env": dep.get("env", ""),
                    "role_base": "Deployment", "unreachable": True,
                    "services": [], "processes": [], "stats": [], "errors_24h": 0,
                    "s3_bucket": dep.get("s3_bucket", ""),
                }
            results.append(r)

    ok = n - unreachable
    _prog(f"Collection done — {ok}/{n} OK{f', {unreachable} unreachable' if unreachable else ''}")
    _prog("Building report data...")
    data = build_report_data(results)

    total_procs  = len(data["all_procs"])
    total_tables = len(data["stats_rows"])
    log.info("Hosts: %d | Deployments: %d | Processes: %d | Tables: %d",
             len(data["hosts"]), len(data["deployments"]), total_procs, total_tables)

    now      = datetime.now()
    date_str = now.strftime("%d/%m/%Y %H:%M")
    day_str  = now.strftime("%Y%m%d")
    ts_str   = now.strftime("%Y%m%d_%H%M%S")

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        out_dir  = script_dir / "reports" / day_str
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"OGG_Informe_{ts_str}.html"

    ctx = {
        **data,
        "date_str":     date_str,
        "total_procs":  total_procs,
        "total_tables": total_tables,
        "counts_js":    json.dumps(data["counts_by_filter"]),
    }
    _prog(f"Rendering HTML → {out_path}")
    html = render_html(ctx, templates_dir)
    out_path.write_text(html, encoding="utf-8")
    _prog(f"Done — {len(html) // 1024} KB written to {out_path}")
    log.info("✓ Report: %s  (%d KB)", out_path, len(html) // 1024)


if __name__ == "__main__":
    main()
