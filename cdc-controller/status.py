import re

from utils import (
    print_log,
    get_matching_deployments,
    get_extracts_for_bbdd,
    get_extract_info_now,
    get_replicat_info_now,
    get_tables_in_use_by_replicats,
    get_tables_in_use_by_extracts,
    find_processes_for_table,
    find_extract_last_started,
)


# Note: this module no longer exposes a CLI entry point — the '-status' command has been
# removed from cdc-controller.py. The status flow is now driven by the JSON field
# 'request_type': STATUS, which apply.py / validate.py / plan dispatch to show_status(config).


def show_status(config):
    """
    Renders the current status of every OGG process (primary extract, primary replicat,
    initial_load extract) that is handling the replication described in 'config'.

    The output is a per-deployment tabular report with columns:
        PROCESS  TYPE  STATE  STATUS  LAST STARTUP TIME

    STATE is the cdc-controller observation:
        - 'in-use'   : the process exists in OGG and handles at least one of the tables
                       requested in the JSON.
        - 'missing'  : no process was found for the role on the deployment.

    A process is shown ONLY when its PRM actually maps at least one of the tables
    requested in the JSON (OGG wildcards honored, TABLEEXCLUDE/MAPEXCLUDE respected);
    processes that do not cover any requested table are silently omitted — no row, no
    CONFIGURATION block, no deployment header. The TABLES section under each shown
    process lists exactly the JSON tables that are confirmed in that process's PRM.

    Returns 0 (status is read-only; never blocks).
    """
    bbdd_engine = (config.get("bbdd_engine") or "").upper()
    env         = (config.get("env")         or "").upper()
    bbdd_name   = (config.get("bbdd_name")   or "").upper()
    project     = (config.get("project")     or "")
    rep_type    = (config.get("replication_type") or "")
    target_eng  = (config.get("target_engine")    or "")

    if not (bbdd_engine and env and bbdd_name):
        print_log("error", "status: missing required JSON fields (env, bbdd_engine, bbdd_name)")
        return 1

    # Tables requested in this JSON (without column lists, uppercased)
    requested_tables = [t.split("(")[0].strip().upper() for t in config.get("tables", [])]

    # Discover ALL deployments for this env+engine. We deliberately do NOT filter by
    # 'load_type' here: for STATUS we want to see every relevant process regardless of
    # whether the deployment serves online / initial_load / both.
    all_deps      = get_matching_deployments(env, bbdd_engine, load_types=None)
    extract_deps  = [name for name, _ in all_deps if re.search(r"_E\d+_", name)]
    replicat_deps = [name for name, _ in all_deps if re.search(r"_R\d+_", name)]

    # Build the list of rows. Each row belongs to one (deployment, process).
    rows = []

    # ──────────────────────────────────────────────────────────────────────────
    # Primary (integrated) extract — identified by USERIDALIAS = <bbdd>_PRIM_<env>
    # ──────────────────────────────────────────────────────────────────────────
    primary_extracts = get_extracts_for_bbdd(extract_deps, bbdd_name, env) if extract_deps else {}
    for dep_name in extract_deps:
        ext_list = primary_extracts.get(dep_name, [])
        if not ext_list:
            rows.append({
                "dep":          dep_name,
                "process":      "(none)",
                "type":         "Extract (integrated)",
                "state":        "missing",
                "status":       "-",
                "last_started": "-",
            })
            continue
        for ext_name in ext_list:
            ok_info, info = get_extract_info_now(dep_name, ext_name)
            status_str   = str(info.get("status", "unknown")) if ok_info else "unknown"
            last_started = (find_extract_last_started(info) if ok_info else None) or "-"
            rows.append({
                "dep":          dep_name,
                "process":      ext_name,
                "type":         "Extract (integrated)",
                "state":        "in-use",
                "status":       status_str,
                "last_started": str(last_started),
            })

    # ──────────────────────────────────────────────────────────────────────────
    # Fetch PRM include/exclude patterns for every extract / replicat on the
    # matching deployments. We always fetch (independently of requested_tables)
    # because the CONFIGURATION block below needs to verify which of the JSON
    # tables are actually configured in each process's PRM.
    # ──────────────────────────────────────────────────────────────────────────
    extract_patterns = get_tables_in_use_by_extracts(extract_deps) if extract_deps else []
    rep_patterns     = get_tables_in_use_by_replicats(replicat_deps) if replicat_deps else []

    # ──────────────────────────────────────────────────────────────────────────
    # Initial_load extract(s) — identified by USERIDALIAS = <bbdd>_STBY_<env>
    # (or <bbdd>_PRIM_<env> if the STBY entry has no connection string).
    # We additionally filter by tables in the PRM (they're per-table by design).
    # ──────────────────────────────────────────────────────────────────────────
    if extract_deps and requested_tables:
        # Find extracts that map at least one requested table, EXCLUDING the primary
        # extracts already listed above (we don't want to double-count integrated ones).
        primary_pairs = {(d, e) for d, exts in primary_extracts.items() for e in exts}
        il_pairs = set()
        for tbl in requested_tables:
            for dep, ext, _pat in find_processes_for_table(tbl, extract_patterns):
                if (dep, ext) not in primary_pairs:
                    il_pairs.add((dep, ext))
        for dep_name, ext_name in sorted(il_pairs):
            ok_info, info = get_extract_info_now(dep_name, ext_name)
            status_str   = str(info.get("status", "unknown")) if ok_info else "unknown"
            last_started = (find_extract_last_started(info) if ok_info else None) or "-"
            rows.append({
                "dep":          dep_name,
                "process":      ext_name,
                "type":         "Extract (initial_load)",
                "state":        "in-use",
                "status":       status_str,
                "last_started": str(last_started),
            })

    # ──────────────────────────────────────────────────────────────────────────
    # Primary replicat — any replicat on a _R<digits>_ deployment that MAPs at
    # least one of the requested tables (OGG wildcards honored).
    # ──────────────────────────────────────────────────────────────────────────
    _rep_mode_label = {"nonintegrated": "classic", "coordinated": "coordinated"}
    if replicat_deps:
        replicat_pairs = set()
        for tbl in requested_tables:
            for dep, rep, _pat in find_processes_for_table(tbl, rep_patterns):
                replicat_pairs.add((dep, rep))
        # For deployments without any matching replicat -> add a 'missing' placeholder
        deps_with_matches = {d for d, _ in replicat_pairs}
        for dep_name in replicat_deps:
            if dep_name not in deps_with_matches:
                rows.append({
                    "dep":          dep_name,
                    "process":      "(none)",
                    "type":         "Replicat",
                    "state":        "missing",
                    "status":       "-",
                    "last_started": "-",
                })
        for dep_name, rep_name in sorted(replicat_pairs):
            ok_info, info = get_replicat_info_now(dep_name, rep_name)
            if ok_info:
                status_str   = str(info.get("status", "unknown"))
                last_started = (find_extract_last_started(info) or "-")
                mode      = info.get("mode")
                mode_type = mode.get("type") if isinstance(mode, dict) else None
                type_lbl  = _rep_mode_label.get(mode_type, mode_type or "unknown")
            else:
                status_str   = "unknown"
                last_started = "-"
                type_lbl     = "unknown"
            rows.append({
                "dep":          dep_name,
                "process":      rep_name,
                "type":         f"Replicat ({type_lbl})",
                "state":        "in-use",
                "status":       status_str,
                "last_started": str(last_started),
            })

    # ──────────────────────────────────────────────────────────────────────────
    # Print per-process: one single-row table + one configuration block
    # Order: extract deployments first ('_E<digits>_'), then replicat deployments ('_R<digits>_'),
    # then anything else. Within a deployment we keep the natural order built by the row
    # collection above (primary extract, initial_load extracts, then replicats).
    # ──────────────────────────────────────────────────────────────────────────
    def _dep_sort_key(dep_name):
        if re.search(r"_E\d+_", dep_name):
            return (0, dep_name)
        if re.search(r"_R\d+_", dep_name):
            return (1, dep_name)
        return (2, dep_name)

    print_log("info", "")
    print_log("info", "====== Replication Status ======")

    # Keep only 'in-use' rows that actually map at least one of the JSON tables in their
    # PRM. The status report is meant to describe processes that handle the requested
    # replication; processes whose PRM does not cover ANY of the requested tables are
    # irrelevant for this status view and are intentionally omitted (no row, no
    # CONFIGURATION block, not even the deployment header).
    def _matched_tables(r):
        is_extract   = r["type"].lower().startswith("extract")
        src_patterns = extract_patterns if is_extract else rep_patterns
        proc_records = [
            (d, p, inc, exc)
            for d, p, inc, exc in src_patterns
            if d == r["dep"] and p == r["process"]
        ]
        return [tbl for tbl in requested_tables if find_processes_for_table(tbl, proc_records)]

    in_use_rows = []
    for r in rows:
        if r["state"] != "in-use":
            continue
        matched = _matched_tables(r)
        if not matched:
            continue
        r["matched_tables"] = matched
        in_use_rows.append(r)

    deps_in_rows = sorted({r["dep"] for r in in_use_rows}, key=_dep_sort_key)
    if not deps_in_rows:
        print_log("warning", f"No OGG processes found for the requested replication on any matching deployment (env={env}, engine={bbdd_engine}).")
        return 0

    for dep in deps_in_rows:
        dep_rows = [r for r in in_use_rows if r["dep"] == dep]
        print_log("info", "")
        print_log("info", f"OGG Deployment : {dep}")
        for r in dep_rows:
            # ── single-row process table ────────────────────────────────────
            _P = max(len("PROCESS"),           len(r["process"]))
            _T = max(len("TYPE"),              len(r["type"]))
            _A = max(len("STATE"),             len(r["state"]))
            _S = max(len("STATUS"),            len(r["status"]))
            _L = max(len("LAST STARTUP TIME"), len(r["last_started"]))
            sep_line = f"{'-'*_P}  {'-'*_T}  {'-'*_A}  {'-'*_S}  {'-'*_L}"
            print_log("info", sep_line)
            print_log("info", f"{'PROCESS':<{_P}}  {'TYPE':<{_T}}  {'STATE':<{_A}}  {'STATUS':<{_S}}  {'LAST STARTUP TIME':<{_L}}")
            print_log("info", sep_line)
            print_log("info", f"{r['process']:<{_P}}  {r['type']:<{_T}}  {r['state']:<{_A}}  {r['status']:<{_S}}  {r['last_started']:<{_L}}")
            print_log("info", sep_line)

            # ── per-process CONFIGURATION block ────────────────────────────
            # Only emitted for rows in state 'in-use' (a real process exists). For 'missing'
            # rows the table itself already conveys "no process found", and rendering the
            # CONFIGURATION block would be misleading (the fields would describe a process
            # that does NOT exist in that deployment).
            # Type-dependent fields:
            #   Extract  -> BBDD ENGINE | ALIAS | BBDD | TABLES
            #   Replicat -> TARGET ENGINE | BBDD | TABLES
            if r["state"] != "in-use":
                print_log("info", "")
                continue

            is_extract = r["type"].lower().startswith("extract")
            indent     = "         "   # 9 spaces -> visually offset from the table edge
            print_log("info", f"{indent}------------------")
            print_log("info", f"{indent}CONFIGURATION")
            print_log("info", f"{indent}------------------")
            if is_extract:
                # Convention used by cdc-controller when creating extracts:
                #   primary (integrated) -> USERIDALIAS = <bbdd>_PRIM_<env>
                #   initial_load         -> USERIDALIAS = <bbdd>_STBY_<env>  (or _PRIM_ if no STBY)
                role = "PRIM" if "integrated" in r["type"].lower() else "STBY"
                alias = f"{bbdd_name}_{role}_{env}"
                print_log("info", f"{indent}BBDD ENGINE   :   {bbdd_engine}")
                print_log("info", f"{indent}ALIAS         :   {alias}")
                print_log("info", f"{indent}BBDD          :   {bbdd_name}")
            else:
                print_log("info", f"{indent}TARGET ENGINE :   {target_eng}")
                print_log("info", f"{indent}BBDD          :   {bbdd_name}")
            # TABLES section — only the JSON tables that are ACTUALLY covered by this
            # process's PRM (OGG wildcards honored, TABLEEXCLUDE/MAPEXCLUDE respected).
            # Already computed during row filtering above; guaranteed non-empty here
            # because rows with no PRM match are excluded from in_use_rows.
            print_log("info", f"{indent}TABLES        :")
            for tbl in r["matched_tables"]:
                print_log("info", f"{indent}  - {tbl}")
            print_log("info", "")   # blank line to visually separate this block from the next

    return 0
