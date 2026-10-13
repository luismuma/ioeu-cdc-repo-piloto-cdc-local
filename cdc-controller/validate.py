import sys
import os
import re
import json
import socket
import subprocess
from urllib.parse import urlparse
try: import oracledb
except ImportError: oracledb = None
try: import pymongo
except ImportError: pymongo = None
try: import ibm_db
except ImportError: ibm_db = None
from utils import *


def _parse_oracle_lag_to_seconds(lag_str):
    # Parse the v$dataguard_stats VALUE column (apply lag / transport lag) and return
    # total elapsed time in seconds. Known formats coming from Oracle Data Guard:
    #   '+DD HH:MM:SS'           e.g. '+00 00:01:23'
    #   '+DD HH:MM:SS.fff'       with fractional seconds
    #   'HH:MM:SS' / 'HH:MM:SS.fff'  (no leading day component)
    # Returns int seconds, or None if the format cannot be parsed (caller treats as error).
    try:
        s = (lag_str or "").strip()
        if s.startswith("+"):
            s = s[1:].strip()
        parts = s.split()
        days  = 0
        if len(parts) == 2:
            days, time_part = int(parts[0]), parts[1]
        else:
            time_part = parts[0]
        if "." in time_part:
            time_part = time_part.split(".", 1)[0]
        h, m, sec = (int(x) for x in time_part.split(":"))
        return days * 86400 + h * 3600 + m * 60 + sec
    except Exception:
        return None


def _validate_delete_request(config):
    # Validations specific to the DELETE flow. Three checks run, each independent of the
    # others (a failure of one does not skip the others):
    #
    #   1) Deployment reachability — for every deployment that the DELETE flow may touch
    #      (proc_type x load_type combinations), confirm the OGG admin server answers.
    #   2) For each requested table, confirm it appears in AT LEAST ONE OGG process across
    #      all relevant deployments. Tables not found anywhere will be silently skipped at
    #      -apply time, so we surface them as WARNING here.
    #   3) Managed-process-profile 'Critical' check is intentionally SKIPPED — DELETE only
    #      stops/removes processes, the profile is irrelevant.
    #
    # Returns 0 on success (warnings still allowed), 1 on a blocking error.
    _W           = 38
    bbdd_engine  = (config.get("bbdd_engine") or "").upper()
    env          = (config.get("env")         or "").upper()
    bbdd_name    = (config.get("bbdd_name")   or "").upper()

    print_log("info", "")
    print_log("info", "====== Validate DELETE flow ======")
    print_log("info", f"requested tables  : {', '.join(config.get('tables', []))}")
    print_log("info", f"bbdd_name         : {bbdd_name}")
    print_log("info", f"env / project     : {env} / {config.get('project', '')}")

    config_tables = [t.split("(")[0].strip().upper() for t in config.get("tables", [])]
    if not config_tables:
        print_log("error", "no tables defined in JSON 'tables' field; DELETE would be a no-op")
        return 1

    errors      = []
    warning_log = []

    # ─── 1) Deployment discovery ──────────────────────────────────────────────────────
    # Reachability is implicitly checked in step 2 below — get_tables_in_use_by_* logs an
    # ERROR per deployment that does not respond. We do NOT call a dedicated ping helper
    # here because a separate round-trip per deployment would just duplicate that signal.
    il_rep_deps  = [n for n, _ in get_matching_deployments(env, bbdd_engine, load_types=["initial_load"], proc_types=["replicat"])]
    il_ext_deps  = [n for n, _ in get_matching_deployments(env, bbdd_engine, load_types=["initial_load"], proc_types=["extract"])]
    pri_rep_deps = [n for n, _ in get_matching_deployments(env, bbdd_engine, load_types=["online"],       proc_types=["replicat"])]
    pri_ext_deps = [n for n, _ in get_matching_deployments(env, bbdd_engine, load_types=["online"],       proc_types=["extract"])]
    all_deps = sorted(set(il_rep_deps + il_ext_deps + pri_rep_deps + pri_ext_deps))
    if not all_deps:
        print_log("warning", f"no deployments matched env={env}, engine={bbdd_engine} — DELETE would have nothing to do")
        warning_log.append(f"[DELETE] no deployments matched env={env}, engine={bbdd_engine}")
    else:
        print_log("info", f"deployments in scope for DELETE : {len(all_deps)}  ({', '.join(all_deps)})")

    # ─── 2) Each requested table must appear in at least ONE OGG process ──────────────
    tables_in_use = set()
    if il_rep_deps or pri_rep_deps:
        rep_patterns = get_tables_in_use_by_replicats(sorted(set(il_rep_deps + pri_rep_deps)))
        for tbl in config_tables:
            if find_processes_for_table(tbl, rep_patterns):
                tables_in_use.add(tbl)
    if il_ext_deps or pri_ext_deps:
        ext_patterns = get_tables_in_use_by_extracts(sorted(set(il_ext_deps + pri_ext_deps)))
        for tbl in config_tables:
            if find_processes_for_table(tbl, ext_patterns):
                tables_in_use.add(tbl)

    not_in_use = [t for t in config_tables if t not in tables_in_use]
    if not_in_use:
        for t in not_in_use:
            print_log("warning", f"{'table in any process':<{_W - 2}}: WARNING  ({t}: not found in any extract/replicat — will be skipped at apply)")
            warning_log.append(f"[DELETE] table '{t}' is not in any active OGG process; nothing will be deleted for it")
        if not tables_in_use:
            print_log("error", "none of the requested tables exists in any OGG process; DELETE would be a no-op")
            errors.append("[DELETE] none of the requested tables exists in any OGG process")
    if tables_in_use:
        print_log("info", f"{'tables found in OGG processes':<{_W}}: OK     ({len(tables_in_use)}/{len(config_tables)} matched)")

    # ─── End of checks ────────────────────────────────────────────────────────────────
    print_log("info", "")
    if errors:
        print_log("error", f"{'DELETE validation':<{_W - 1}}: FAILED  {len(errors)} error(s) reported:")
        for e in errors:
            print_log("error", f"  - {e}")
        if warning_log:
            print_log("warning", f"plus {len(warning_log)} warning(s):")
            for w in warning_log:
                print_log("warning", f"  - {w}")
        print_log("info", "end validate")
        return 1
    if warning_log:
        print_log("warning", f"{'DELETE validation':<{_W - 2}}: WARNING  {len(warning_log)} warning(s) reported:")
        for w in warning_log:
            print_log("warning", f"  - {w}")
    else:
        print_log("info", f"{'DELETE validation':<{_W}}: OK")
    print_log("info", "end validate")
    return 0


def validate(filepath):
    # Checks connectivity prerequisites for the source database engine (Oracle, MongoDB, DB2).
    # Verifies that the required driver is installed and that a connection can be established.

    # print begin
    print_log("info", "starting validate")

    # get variables from JSON config file 
    config = load_json_file(filepath)
    if config is None:
        return 1
    
    # get bbdd_engine from config file
    bbdd_engine = config["bbdd_engine"]

    # global alignment width for all ':' separators in output
    _W = 38

    # global lists of findings across all checks (each entry is a descriptive string with context)
    error_log   = []   # blocks the validation (returned as ERROR)
    warning_log = []   # informational only (validation still passes)

    # ── Dispatch by request_type ─────────────────────────────────────────────────────────
    # CREATE -> falls through to the existing validation flow below.
    # UPDATE -> same validation as CREATE (source DB, target connections, table checks all
    #           apply equally). Falls through — no separate dispatch needed.
    # DELETE -> handled by _validate_delete_request() (placeholder for now); returns early.
    # STATUS -> there is nothing to validate; we just delegate to status.show_status so
    #           the user sees the current state of every OGG process for this replication.
    request_type = config.get("request_type", "CREATE")
    print_log("info", "")
    print_log("info", "====== REQUEST VALIDATE ======")
    print_log("info", f"request_type is {request_type}...")
    if request_type == "DELETE":
        return _validate_delete_request(config)
    if request_type == "STATUS":
        from status import show_status
        return show_status(config)

    ### CHECK 1 : Source database validation (ORACLE | MONGO | DB2)
    print_log("info", "")
    print_log("info", "====== Check Source Database ======")
    print_log("info", "Starting database validation for database engine : " + bbdd_engine.upper())

    # INFO: show configured extract initial_load_parallel (FULL / INITIAL_LOAD only)
    _rep_type_src = (config.get("replication_type") or "").upper()
    if _rep_type_src in ("FULL", "INITIAL_LOAD"):
        _src_bbdd_conn = get_db_connection(f"{config.get('bbdd_name', '')}_{config.get('env', '')}")
        _il_par_raw    = (_src_bbdd_conn or {}).get("initial_load_parallel")
        _il_par_val    = int(_il_par_raw) if _il_par_raw is not None else None
        _il_par_str    = str(_il_par_val) if _il_par_val else "not defined (will default to number of tables at apply time)"
        _il_par_src    = "db_connections.yaml" if _il_par_val else "—"
        print_log("info", f"{'initial_load extract parallel':<{_W}}: {_il_par_str}  (source: {_il_par_src})")

    ############## Oracle
    if bbdd_engine == "ORACLE":
        # Prepare Oracle connection info (init client + read credentials/DSNs from db_connections.yaml + password from env)
        ora = prepare_oracle_connection(config["bbdd_name"], config["env"])
        if ora is None:
            db_password, db_pdb, dsns_to_check, db_user_prim, dsn_prim_string = "", "", [], "", ""
            errors = ["could not prepare source database connection (check db_connections.yaml and env vars) — source database checks and table checks skipped"]
        else:
            db_password     = ora["db_password"]
            db_pdb          = ora["db_pdb"]
            dsns_to_check   = ora["dsns_to_check"]
            # primary user/DSN are the first tuple — needed later for the table checks block
            _, db_user_prim, dsn_prim_string = dsns_to_check[0]
            errors = []

        # loop for checks in dsns_to_check (prim and stby)
        for db_label, db_user, dsn in dsns_to_check:
            print_log("info", f"{db_label}")
            try:
                conn = oracledb.connect(user=db_user, password=db_password, dsn=dsn)
                print_log("info", f"{'connected to Oracle Database':<{_W}}: OK")
                cursor = conn.cursor()

                # print basic database info as key-value lines
                cursor.execute("select db.NAME, db.DB_UNIQUE_NAME, db.DATABASE_ROLE from v$database db")
                rows = cursor.fetchall()
                col_names = [d[0] for d in cursor.description]
                for col, val in zip(col_names, rows[0]):
                    print_log("info", f"{col:<{_W}}: {val}")

                # Check oradb 0: DATABASE_ROLE must match the intended connection type
                db_role = rows[0][2]
                _label_lower = db_label.lower()
                if "primary" in _label_lower:
                    _expected_role = "PRIMARY"
                elif "standby" in _label_lower:
                    _expected_role = "PHYSICAL STANDBY"
                else:
                    _expected_role = None
                if _expected_role is not None:
                    ok = db_role == _expected_role
                    print_log("info" if ok else "error", f"{'DATABASE_ROLE check':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  (expected={_expected_role}, actual={db_role})")
                    if not ok:
                        errors.append(f"[{db_label}] DATABASE_ROLE mismatch: expected '{_expected_role}' but connected database reports role '{db_role}' — check DSN configuration")

                # Check oradb 1: Oracle version >= 19
                cursor.execute("SELECT version FROM v$instance")
                version_str   = cursor.fetchone()[0]
                major_version = int(version_str.split(".")[0])
                ok = major_version >= 19
                print_log("info" if ok else "error", f"{'Oracle version':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({version_str})")
                if not ok:
                    errors.append(f"[{db_label}] Oracle version {version_str} is not 19c or greater")

                # Check oradb 2: ARCHIVELOG mode
                cursor.execute("SELECT log_mode FROM v$database")
                log_mode = cursor.fetchone()[0]
                ok = log_mode == "ARCHIVELOG"
                print_log("info" if ok else "error", f"{'ARCHIVELOG mode':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({log_mode})")
                if not ok:
                    errors.append(f"[{db_label}] database is not in ARCHIVELOG mode (current: {log_mode})")

                # Check oradb 3: FORCE LOGGING
                cursor.execute("SELECT force_logging FROM v$database")
                force_logging = cursor.fetchone()[0]
                ok = force_logging == "YES"
                print_log("info" if ok else "error", f"{'FORCE LOGGING':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({force_logging})")
                if not ok:
                    errors.append(f"[{db_label}] FORCE LOGGING is not enabled (current: {force_logging})")

                # Check oradb 4: Supplemental Log MIN
                cursor.execute("SELECT supplemental_log_data_min FROM v$database")
                suplog_min = cursor.fetchone()[0]
                ok = suplog_min == "YES"
                print_log("info" if ok else "error", f"{'Supplemental Log (MIN)':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({suplog_min})")
                if not ok:
                    errors.append(f"[{db_label}] Supplemental Log (MIN) is not enabled (current: {suplog_min})")

                # Check oradb 5: enable_goldengate_replication = TRUE
                cursor.execute("SELECT value FROM v$parameter WHERE name = 'enable_goldengate_replication'")
                row     = cursor.fetchone()
                ogg_val = row[0] if row and row[0] else "NOT SET"
                ok = ogg_val.upper() == "TRUE"
                print_log("info" if ok else "error", f"{'enable_goldengate_replication':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({ogg_val})")
                if not ok:
                    errors.append(f"[{db_label}] enable_goldengate_replication is not TRUE (current: {ogg_val})")

                # Check oradb 6: streams_pool_size > 1000 MB (warning only)
                cursor.execute("SELECT value FROM v$parameter WHERE name = 'streams_pool_size'")
                row        = cursor.fetchone()
                streams_mb = int(row[0]) // (1024 * 1024) if row and row[0] else 0
                ok = streams_mb > 1000
                print_log("info" if ok else "warning", f"{'Streams Pool Size > 1000 MB':<{_W - (0 if ok else 2)}}: {'OK' if ok else 'WARNING':<1}  ({streams_mb} MB)")
                if not ok:
                    warning_log.append(f"[{db_label}] streams_pool_size is {streams_mb} MB (recommended > 1000)")

                # Check oradb 7 (STANDBY only): the configured PDB must be in OPEN_MODE = 'READ ONLY'.
                # Severity depends on replication_type:
                #   - INITIAL_LOAD / FULL  -> ERROR. The initial_load extracts connect to the STANDBY
                #                             to offload reads from the primary; if the PDB is MOUNTED
                #                             they cannot fetch any row.
                #   - ONLINE               -> WARNING only. The integrated extract reads from the
                #                             PRIMARY; the standby is just a fallback, so a closed PDB
                #                             does not block the apply but should be flagged.
                if db_label.startswith("Standby") and db_pdb:
                    cursor.execute(
                        "SELECT open_mode FROM v$pdbs WHERE UPPER(name) = UPPER(:pdb)",
                        {"pdb": db_pdb},
                    )
                    row_pdb       = cursor.fetchone()
                    rep_type_here = (config.get("replication_type") or "").upper()
                    is_blocking   = rep_type_here in ("INITIAL_LOAD", "FULL")
                    sev_level     = "error" if is_blocking else "warning"
                    sev_label     = "ERROR" if is_blocking else "WARNING"

                    if row_pdb is None:
                        msg = f"PDB '{db_pdb}' not found in v$pdbs"
                        print_log(sev_level, f"{'PDB open_mode (standby)':<{_W - (1 if is_blocking else 2)}}: {sev_label}  ({msg})")
                        if is_blocking:
                            errors.append(f"[{db_label}] {msg} (replication_type={rep_type_here})")
                        else:
                            warning_log.append(f"[ORACLE PREREQ] [{db_label}] {msg} (replication_type={rep_type_here})")
                    else:
                        open_mode = (row_pdb[0] or "").upper()
                        ok = open_mode == "READ ONLY"
                        if ok:
                            print_log("info", f"{'PDB open_mode (standby)':<{_W}}: OK  ({db_pdb}: {open_mode})")
                        else:
                            print_log(sev_level, f"{'PDB open_mode (standby)':<{_W - (1 if is_blocking else 2)}}: {sev_label}  ({db_pdb}: {open_mode or '<empty>'})")
                            detail = f"PDB '{db_pdb}' open_mode is '{open_mode or '<empty>'}', expected 'READ ONLY'"
                            if is_blocking:
                                errors.append(f"[{db_label}] {detail} (replication_type={rep_type_here})")
                            else:
                                warning_log.append(f"[ORACLE PREREQ] [{db_label}] {detail} (replication_type=ONLINE)")

                # Check oradb 8 (STANDBY only): Data Guard apply lag.
                # Threshold: > 60 seconds -> ERROR for FULL/INITIAL_LOAD (stale standby would
                # break the consistency window between bulk load and CDC stream).
                # For ONLINE -> WARNING only (standby is a fallback, not the capture source).
                if db_label.startswith("Standby"):
                    rep_type_lag  = (config.get("replication_type") or "").upper()
                    is_lag_block  = rep_type_lag in ("INITIAL_LOAD", "FULL")
                    lag_sev_level = "error" if is_lag_block else "warning"
                    lag_sev_label = "ERROR" if is_lag_block else "WARNING"
                    cursor.execute("SELECT VALUE FROM v$dataguard_stats WHERE NAME = 'apply lag'")
                    row_lag = cursor.fetchone()
                    if not row_lag or row_lag[0] is None:
                        msg = "v$dataguard_stats has no 'apply lag' row (not a physical standby?)"
                        print_log(lag_sev_level, f"{'Data Guard apply lag':<{_W - (1 if is_lag_block else 2)}}: {lag_sev_label}  ({msg})")
                        if is_lag_block:
                            errors.append(f"[{db_label}] {msg}")
                        else:
                            warning_log.append(f"[ORACLE PREREQ] [{db_label}] {msg} (replication_type=ONLINE)")
                    else:
                        lag_str     = str(row_lag[0]).strip()
                        lag_seconds = _parse_oracle_lag_to_seconds(lag_str)
                        if lag_seconds is None:
                            msg = f"could not parse apply lag value '{lag_str}'"
                            print_log(lag_sev_level, f"{'Data Guard apply lag':<{_W - (1 if is_lag_block else 2)}}: {lag_sev_label}  ({msg})")
                            if is_lag_block:
                                errors.append(f"[{db_label}] {msg}")
                            else:
                                warning_log.append(f"[ORACLE PREREQ] [{db_label}] {msg} (replication_type=ONLINE)")
                        elif lag_seconds > 60:
                            print_log(lag_sev_level, f"{'Data Guard apply lag':<{_W - (1 if is_lag_block else 2)}}: {lag_sev_label}  ({lag_str} = {lag_seconds}s, exceeds 60s threshold)")
                            if is_lag_block:
                                errors.append(f"[{db_label}] Data Guard apply lag {lag_str} ({lag_seconds}s) exceeds 1 minute")
                            else:
                                warning_log.append(f"[ORACLE PREREQ] [{db_label}] Data Guard apply lag {lag_str} ({lag_seconds}s) exceeds 1 minute (replication_type=ONLINE)")
                        else:
                            print_log("info", f"{'Data Guard apply lag':<{_W}}: OK  ({lag_str} = {lag_seconds}s)")

                cursor.close()
                conn.close()
            except Exception as e:
                is_stby_online = (
                    db_label.startswith("Standby") and
                    (config.get("replication_type") or "").upper() == "ONLINE"
                )
                if is_stby_online:
                    print_log("warning", f"{'Oracle Standby connection':<{_W - 2}}: WARNING  (skipped — {e})")
                    warning_log.append(f"[ORACLE PREREQ] [STANDBY] connection failed: {e} (replication_type=ONLINE, standby not required)")
                else:
                    print_log("error", f"Oracle {db_label} connection failed: {e}")
                    errors.append(f"[{db_label}] connection failed: {e}")

        if errors:
            print_log("error", f"{'Oracle prerequisites checks':<{_W - 1}}: FAILED  {len(errors)} check(s) not met:")
            for err in errors:
                print_log("error", f"  - {err}")
            error_log.extend(f"[ORACLE PREREQ] {e}" for e in errors)
        else:
            print_log("info", f"{'Oracle prerequisites checks':<{_W}}: OK")


        # Check database tables block — only runs when DB connection info was available
        if dsn_prim_string:
            print_log("info", "")
            print_log("info", "====== Check Database Tables and Consistency ======")
            print_log("info", f"checking tables in primary database ...")
        # The whole Check Tables block is wrapped in try/except so a connection / query failure
        # is reported as a check ERROR and added to error_log — validate must NEVER raise.
        # Cleanup happens in 'finally' even if the connect itself failed (conn may be None).
        conn   = None
        cursor = None
        try:
            if not dsn_prim_string:
                raise ValueError("table checks skipped — DB connection info unavailable (see above)")
            conn   = oracledb.connect(user=db_user_prim, password=db_password, dsn=dsn_prim_string)
            cursor = conn.cursor()

            # detect multitenant: if CDB=YES and db_pdb is set, switch session to the PDB
            cursor.execute("SELECT cdb FROM v$database")
            is_cdb = cursor.fetchone()[0] == "YES"
            if is_cdb and db_pdb:
                print_log("info", f"multitenant database detected, switching session to PDB : {db_pdb}")
                cursor.execute(f"ALTER SESSION SET CONTAINER = {db_pdb}")
            elif is_cdb and not db_pdb:
                print_log("warning", "multitenant database detected but db_pdb is not set, table check runs on root container")
                warning_log.append("[TABLES] multitenant database detected but db_pdb is not set in db_connections.yaml (table checks ran on root container)")

            # Check tables: all configured tables exist (warning only, single query)
            config_tables = [t.split("(")[0].strip().upper() for t in config.get("tables", [])]
            if config_tables:
                binds        = {f"t{i}": tn for i, tn in enumerate(config_tables)}
                placeholders = ",".join(f":t{i}" for i in range(len(config_tables)))
                cursor.execute(
                    f"SELECT OWNER || '.' || TABLE_NAME FROM all_tables WHERE OWNER || '.' || TABLE_NAME IN ({placeholders})",
                    binds
                )
                found   = {row[0] for row in cursor.fetchall()}
                missing = [t for t in config_tables if t not in found]
                ok = not missing
                print_log("info" if ok else "warning", f"{'tables exist in database':<{_W - (0 if ok else 2)}}: {'OK' if ok else 'WARNING':<1}  ({len(config_tables) - len(missing)}/{len(config_tables)} found)")
                for t in missing:
                    print_log("warning", f"  - table not found: {t}")
                if missing:
                    warning_log.extend(f"[TABLES] table not found in database: {t}" for t in missing)


            # Check tables: all configured columns exist and no duplicates (single query)
            if config_tables:
                # parse columns per table from raw config: "SCHEMA.TABLE(COL1,COL2)" or "SCHEMA.TABLE(*)"
                cols_per_table = {}
                for raw in config.get("tables", []):
                    full     = raw.split("(")[0].strip().upper()
                    cols_str = raw.split("(", 1)[1].rsplit(")", 1)[0].strip()
                    cols_per_table[full] = None if cols_str == "*" else [c.strip().upper() for c in cols_str.split(",")]

                cursor.execute(
                    f"SELECT OWNER || '.' || TABLE_NAME, COLUMN_NAME FROM all_tab_columns WHERE OWNER || '.' || TABLE_NAME IN ({placeholders})",
                    binds
                )
                db_cols = {}
                for tab, col in cursor.fetchall():
                    db_cols.setdefault(tab, set()).add(col)

                issues = []
                for full, cols in cols_per_table.items():
                    if cols is None:
                        continue  # (*) means all columns -> automatic OK
                    # detect duplicates within the JSON entry
                    seen, dups = set(), []
                    for c in cols:
                        if c in seen and c not in dups: dups.append(c)
                        seen.add(c)
                    if dups:
                        issues.append((full, f"duplicate columns in config: {', '.join(dups)}"))
                        continue
                    # check each column exists in the database
                    missing_cols = [c for c in cols if c not in db_cols.get(full, set())]
                    if missing_cols:
                        issues.append((full, f"columns not found in table: {', '.join(missing_cols)}"))

                ok = not issues
                print_log("info" if ok else "error", f"{'tables columns valid':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({len(config_tables) - len(issues)}/{len(config_tables)} found)")
                for tbl, problem in issues:
                    print_log("error", f"  - {tbl}: {problem}")
                if issues:
                    error_log.extend(f"[TABLES COLUMNS] {tbl}: {problem}" for tbl, problem in issues)

            # Check tables: every PK column of a table with an explicit COLMAP must appear
            # in the JSON column list. If a PK column is missing the replicat will ABEND at
            # runtime with 'OGG-00918 Key column <X> is missing from map' because OGG cannot
            # identify the affected row when applying UPDATE/DELETE. Targets in this
            # application are always Kafka or S3, but the source-side PK is still the key
            # OGG uses to identify rows in the trail, so the check applies regardless of target.
            if config_tables:
                pk_check_tables = [t for t, cols in cols_per_table.items() if cols is not None and cols]
                if pk_check_tables:
                    pk_binds        = {f"pk{i}": tn for i, tn in enumerate(pk_check_tables)}
                    pk_placeholders = ",".join(f":pk{i}" for i in range(len(pk_check_tables)))
                    cursor.execute(
                        "SELECT acc.OWNER || '.' || acc.TABLE_NAME, acc.COLUMN_NAME "
                        "FROM   all_cons_columns acc "
                        "JOIN   all_constraints  ac "
                        "       ON acc.OWNER = ac.OWNER "
                        "      AND acc.CONSTRAINT_NAME = ac.CONSTRAINT_NAME "
                        "WHERE  ac.CONSTRAINT_TYPE = 'P' "
                        f" AND  acc.OWNER || '.' || acc.TABLE_NAME IN ({pk_placeholders}) "
                        "ORDER  BY acc.OWNER, acc.TABLE_NAME, acc.POSITION",
                        pk_binds,
                    )
                    pk_cols_by_table = {}
                    for tab, col in cursor.fetchall():
                        pk_cols_by_table.setdefault(tab, []).append(col)

                    pk_issues = []
                    for t in pk_check_tables:
                        allow   = set(cols_per_table[t])
                        pk_cols = pk_cols_by_table.get(t, [])
                        if not pk_cols:
                            # No PK declared on the table — OGG will pick a substitute
                            # identification key at runtime (smallest unique index, or all
                            # columns as last resort). We cannot statically verify what OGG
                            # will choose, so we flag it as a WARNING — the operator should
                            # double-check that the JSON column list covers an acceptable key.
                            print_log("warning", f"  {t}: no PRIMARY KEY declared on the table; COLMAP coverage cannot be statically verified (OGG will pick a substitute identification key at runtime)")
                            warning_log.append(f"[PK COLMAP COVERAGE] {t}: no PK declared, runtime key resolution may surprise — verify the JSON column list covers an acceptable key")
                            continue
                        missing = [c for c in pk_cols if c not in allow]
                        if missing:
                            pk_issues.append((t, pk_cols, missing))

                    ok = not pk_issues
                    print_log("info" if ok else "error",
                              f"{'PK columns covered by COLMAP':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({len(pk_check_tables) - len(pk_issues)}/{len(pk_check_tables)} tables OK)")
                    for tbl, pk_cols, missing in pk_issues:
                        print_log("error", f"  - {tbl}: PK={','.join(pk_cols)}; missing in COLMAP: {','.join(missing)} (replicat would ABEND with OGG-00918)")
                    if pk_issues:
                        error_log.extend(
                            f"[PK COLMAP COVERAGE] {tbl}: PK column(s) {','.join(missing)} not in JSON column list (PK={','.join(pk_cols)})"
                            for tbl, pk_cols, missing in pk_issues
                        )

            # Check tables: all tables in dba_goldengate_support_mode are supported for replication (single query)
            if config_tables:
                cursor.execute(
                    f"SELECT OWNER || '.' || OBJECT_NAME FROM dba_goldengate_support_mode WHERE OWNER || '.' || OBJECT_NAME IN ({placeholders})",
                    binds
                )
                found_ogg   = {row[0] for row in cursor.fetchall()}
                missing_ogg = [t for t in config_tables if t not in found_ogg]
                ok = not missing_ogg
                print_log("info" if ok else "warning", f"{'tables in OGG support mode':<{_W - (0 if ok else 2)}}: {'OK' if ok else 'WARNING':<1}  ({len(config_tables) - len(missing_ogg)}/{len(config_tables)} found)")
                for t in missing_ogg:
                    print_log("warning", f"  - table not in dba_goldengate_support_mode: {t}")
                if missing_ogg:
                    warning_log.extend(f"[OGG support mode] table not in dba_goldengate_support_mode: {t}" for t in missing_ogg)

            # Check tables: all tables have TRANDATA enabled (single query against dba_log_groups)
            if config_tables:
                cursor.execute(
                    f"SELECT DISTINCT OWNER ||'.'|| TABLE_NAME FROM dba_log_groups WHERE OWNER ||'.'|| TABLE_NAME IN ({placeholders})",
                    binds
                )

                found_trd   = {row[0] for row in cursor.fetchall()}
                missing_trd = [t for t in config_tables if t not in found_trd]
                ok = not missing_trd
                print_log("info" if ok else "error", f"{'tables with TRANDATA enabled':<{_W - (0 if ok else 2)}}: {'OK' if ok else 'Error':<1}  ({len(config_tables) - len(missing_trd)}/{len(config_tables)} found)")
                for t in missing_trd:
                    print_log("error", f"+ TRANDATA not enabled on table: {t}")
                if missing_trd:
                    error_log.extend(f"[TRANDATA] not enabled on table: {t}" for t in missing_trd)

            # Check tables: no configured table contains a LONG or LONG RAW column.
            # OGG cannot replicate LONG/LONG RAW columns — the extract will ABEND with
            # OGG-01154 if such a column is present, even when not included in a COLMAP.
            if config_tables:
                cursor.execute(
                    f"SELECT OWNER ||'.'|| TABLE_NAME, COLUMN_NAME, DATA_TYPE "
                    f"FROM all_tab_columns "
                    f"WHERE OWNER ||'.'|| TABLE_NAME IN ({placeholders}) "
                    f"  AND DATA_TYPE IN ('LONG', 'LONG RAW') "
                    f"ORDER BY 1, 2",
                    binds
                )
                long_cols = cursor.fetchall()
                ok = not long_cols
                print_log("info" if ok else "error", f"{'tables free of LONG types':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({len(config_tables) - len({r[0] for r in long_cols})}/{len(config_tables)} clean)")
                for tab, col, dtype in long_cols:
                    print_log("error", f"  - {tab}.{col} ({dtype}) — OGG cannot replicate LONG/LONG RAW columns (OGG-01154)")
                if long_cols:
                    error_log.extend(
                        f"[LONG types] {tab}.{col} has unsupported type {dtype} (OGG-01154)"
                        for tab, col, dtype in long_cols
                    )

            # Check tables: none of the configured tables appear in dba_nested_tables
            # (Oracle GoldenGate does not support replication of nested-table types,
            #  neither the parent table containing the nested column nor the nested storage table).
            if config_tables:
                cursor.execute(
                    f"SELECT DISTINCT OWNER ||'.'|| PARENT_TABLE_NAME FROM dba_nested_tables "
                    f" WHERE OWNER ||'.'|| PARENT_TABLE_NAME IN ({placeholders}) "
                    f"UNION "
                    f"SELECT DISTINCT OWNER ||'.'|| TABLE_NAME        FROM dba_nested_tables "
                    f" WHERE OWNER ||'.'|| TABLE_NAME        IN ({placeholders})",
                    binds
                )
                found_nested = {row[0] for row in cursor.fetchall()}
                ok = not found_nested
                print_log("info" if ok else "error", f"{'tables free of nested tables':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({len(config_tables) - len(found_nested)}/{len(config_tables)} clean)")
                for t in sorted(found_nested):
                    print_log("error", f"+ table found in dba_nested_tables (not supported by OGG): {t}")
                if found_nested:
                    error_log.extend(f"[NESTED TABLES] table found in dba_nested_tables (not supported by OGG): {t}" for t in sorted(found_nested))

            # Check open transactions age (PRIMARY) — FULL / INITIAL_LOAD only.
            # A long-lived open transaction forces the consistency SCN used by the initial-load
            # extracts to be anchored far in the past, causing apply to replicate a stale snapshot.
            #   > 24 h → ERROR  (apply would use a dangerously old SCN)
            #   1 h – 24 h → WARNING
            #   < 1 h / no open transactions → OK
            _rep_type_scn = (config.get("replication_type") or "").upper()
            if _rep_type_scn in ("FULL", "INITIAL_LOAD"):
                try:
                    cursor.execute(
                        "SELECT MIN(START_DATE) AS MIN_START_DATE, MIN(START_SCN) AS MIN_START_SCN "
                        "FROM gv$transaction "
                        "WHERE START_SCN > 0"
                    )
                    _row = cursor.fetchone()
                    _min_start_date, _min_start_scn = (_row[0], _row[1]) if _row else (None, None)
                    _scn_label = "open transactions age (primary)"
                    if _min_start_date is None:
                        print_log("info", f"{_scn_label:<{_W}}: OK  (no open transactions found)")
                    else:
                        from datetime import datetime as _dt
                        _age_hours = ((_dt.now() - _min_start_date).total_seconds()) / 3600.0
                        _age_str   = f"MIN_START_DATE={_min_start_date.strftime('%d-%m-%Y %H:%M:%S')}  SCN={_min_start_scn}  age={_age_hours:.1f}h"
                        if _age_hours > 24:
                            print_log("error",   f"{_scn_label:<{_W - 1}}: ERROR    ({_age_str} — MIN_SCN older than 24 h detected, this may impact initial_load consistency)")
                            error_log.append(f"[SCN AGE] {_age_str} — MIN_SCN older than 24 h detected, this may impact initial_load consistency")
                        elif _age_hours > 1:
                            print_log("warning", f"{_scn_label:<{_W - 3}}: WARNING  ({_age_str} — MIN_SCN of {_age_hours:.1f} h detected, this may impact initial_load consistency)")
                            warning_log.append(f"[SCN AGE] {_age_str} — MIN_SCN of {_age_hours:.1f} h detected, this may impact initial_load consistency")
                        else:
                            print_log("info",    f"{_scn_label:<{_W}}: OK  ({_age_str})")
                except Exception as _e_scn:
                    print_log("warning", f"{'open transactions age (primary)':<{_W - 3}}: WARNING  (could not query gv$transaction: {_e_scn})")
                    warning_log.append(f"[SCN AGE] could not query gv$transaction on primary: {_e_scn}")

        except ValueError:
            pass  # DB connection info unavailable — table checks intentionally skipped
        except oracledb.DatabaseError as e:
            # ORA-12514 / 12541 / 01017 / etc. — surface the ORA-code as a check ERROR and
            # add it to error_log so the run terminates with a non-zero status, but validate
            # itself does NOT raise so the rest of the checks still report their results.
            err_str = str(e).strip().splitlines()[0] if str(e).strip() else repr(e)
            print_log("error", f"{'Check Tables (primary DB)':<{_W - 1}}: ERROR  ({err_str})")
            error_log.append(f"[TABLES] could not run table checks on primary DB: {err_str}")
        except Exception as e:
            print_log("error", f"{'Check Tables (primary DB)':<{_W - 1}}: ERROR  (unexpected: {e})")
            error_log.append(f"[TABLES] unexpected error during table checks: {e}")
        finally:
            # Close oracle database connection (safe even if connect() never succeeded —
            # both can be None if the failure happened before they were assigned).
            if cursor is not None:
                try: cursor.close()
                except Exception: pass
            if conn is not None:
                try: conn.close()
                except Exception: pass

    ############## Mongo
    elif bbdd_engine == "MONGO":
        if pymongo is None:
            print_log("error", "pymongo module not installed. Run: pip install pymongo")
            error_log.append("[MONGODB] pymongo module not installed")
        else:
            mongo = prepare_mongo_connection(config["bbdd_name"], config["env"])
            if mongo is None:
                error_log.append("[MONGODB] could not prepare connection — check db_connections.yaml and env vars")
            else:
                print_log("info", f"connecting to MongoDB host : {mongo['host']}")
                client = None
                try:
                    client = pymongo.MongoClient(mongo["uri"], serverSelectionTimeoutMS=5000)
                    srv_info = client.server_info()
                    print_log("info", f"{'MongoDB connection':<{_W}}: OK  (host: {mongo['host']})")

                    # Check mdb 1: server version >= 4.0 (change streams — required by OGG CDC for MongoDB)
                    version_str = srv_info.get("version", "")
                    try:
                        parts = [int(x) for x in version_str.split(".") if x.isdigit()]
                        ok = parts[0] >= 4 if parts else False
                    except Exception:
                        ok = False
                    print_log("info" if ok else "error",
                              f"{'MongoDB version >= 4.0':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({version_str})")
                    if not ok:
                        error_log.append(f"[MONGODB] server version '{version_str}' is below 4.0 — change streams (required by OGG CDC) need MongoDB 4.0+")

                    # Check mdb 2: topology supports change streams (replica set or sharded cluster; NOT standalone)
                    try:
                        topo = client.topology_description.topology_type_name
                        ok_topo = topo not in ("Single", "Unknown")
                        print_log("info" if ok_topo else "error",
                                  f"{'MongoDB topology (change streams)':<{_W - (0 if ok_topo else 1)}}: {'OK' if ok_topo else 'ERROR':<1}  ({topo})")
                        if not ok_topo:
                            error_log.append(f"[MONGODB] topology '{topo}' does not support change streams — requires replica set or sharded cluster")
                    except Exception as e:
                        print_log("warning", f"{'MongoDB topology check':<{_W - 2}}: WARNING  (could not determine: {e})")
                        warning_log.append(f"[MONGODB] topology check failed: {e}")

                    # Check mdb 3: collections exist in the target database (WARNING only — same as Oracle table check)
                    print_log("info", "")
                    print_log("info", "====== Check Collections ======")
                    db_name = mongo["db_name"]
                    config_collections = []
                    for entry in config.get("tables", []):
                        raw = entry.split("(")[0].strip()
                        coll = raw.split(".", 1)[1].strip() if "." in raw else raw
                        if coll:
                            config_collections.append(coll)

                    if config_collections:
                        try:
                            db = client[db_name]
                            existing     = db.list_collection_names()
                            existing_lc  = {e.lower(): e for e in existing}  # lowercase → actual name
                            found, missing = [], []
                            for coll in config_collections:
                                actual = existing_lc.get(coll.lower())  # case-insensitive lookup
                                if actual:
                                    found.append((coll, actual))
                                else:
                                    missing.append(coll)
                            ok = not missing
                            print_log("info" if ok else "warning",
                                      f"{'collections exist':<{_W - (0 if ok else 2)}}: {'OK' if ok else 'WARNING':<1}  ({len(found)}/{len(config_collections)} found in '{db_name}')")
                            for coll, actual in found:
                                if coll != actual:
                                    print_log("info", f"  + {coll} → actual name in MongoDB: '{actual}'")
                            for coll in missing:
                                print_log("warning", f"  + collection not found in database '{db_name}': {coll}")
                                warning_log.append(f"[MONGODB COLLECTIONS] '{coll}' not found in database '{db_name}'")
                        except Exception as e:
                            print_log("error", f"{'collections check':<{_W - 1}}: ERROR  ({e})")
                            error_log.append(f"[MONGODB] collection check failed: {e}")

                except pymongo.errors.ServerSelectionTimeoutError as e:
                    print_log("error", f"{'MongoDB connection':<{_W - 1}}: ERROR  ({e})")
                    error_log.append(f"[MONGODB] connection failed: {e}")
                except Exception as e:
                    print_log("error", f"{'MongoDB connection':<{_W - 1}}: ERROR  (unexpected: {e})")
                    error_log.append(f"[MONGODB] unexpected error: {e}")
                finally:
                    if client is not None:
                        try: client.close()
                        except Exception: pass

    ############## DB2
    elif bbdd_engine == "DB2":
        if ibm_db is None:
            print_log("error", "ibm_db module not installed. Run: pip install ibm_db")
            error_log.append("[DB2] ibm_db module not installed")
        else:
            db2 = prepare_db2_connection(config["bbdd_name"], config["env"])
            if db2 is None:
                error_log.append("[DB2] could not prepare connection — check db_connections.yaml and env vars")
            else:
                ok_tcp, msg_tcp = check_tcp_reachable(db2["host"], db2["port"], timeout=5)
                print_log("info" if ok_tcp else "error",
                          f"{'DB2 TCP reachable':<{_W - (0 if ok_tcp else 1)}}: {'OK' if ok_tcp else 'ERROR':<1}  ({db2['host']}:{db2['port']}{' - ' + msg_tcp if not ok_tcp else ''})")
                if not ok_tcp:
                    error_log.append(f"[DB2] TCP connection to {db2['host']}:{db2['port']} failed: {msg_tcp}")

    ### CHECK 2 : Target for replication validation (KAFKA | AWS-S3 ...)
    # Reads target_connections.yaml to locate the target (cluster / bucket) and runs
    # network-level reachability checks from the cdc-controller host. Note: these checks
    # run FROM the cdc-controller machine, not from the OGG deployment that will actually
    # publish — so they're a sanity check, not a guarantee. Deeper auth/handshake errors
    # are caught later when the replicat starts (via report-error verification).
    print_log("info", "")
    print_log("info", "====== Check Target System ======")
    target_engine = config.get("target_engine", "")
    print_log("info", f"Starting target validation for target engine : {target_engine}")

    # locate the target_connections.yaml entry
    # AWS-S3 : look up by s3_bucket_name (identifies which entry to use in target_connections.yaml)
    # KAFKA   : look up by kafka_cluster_name + kafka_identity_pool + env (disambiguates entries sharing the same cluster)
    # Other   : derive the key from project/env as fallback
    target_name = config.get("target_name") or derive_target_name(target_engine, config.get("project", ""), config.get("env", ""))
    target_conn = None
    if target_engine == "AWS-S3" and config.get("s3_bucket_name"):
        target_conn = get_target_connection_by_bucket(config["s3_bucket_name"])
        if target_conn is None:
            print_log("error", f"{'target_connections.yaml entry':<{_W - 1}}: ERROR  (no entry with s3_bucket_name='{config['s3_bucket_name']}')")
            error_log.append(f"[TARGET] no target_connections.yaml entry found with s3_bucket_name='{config['s3_bucket_name']}'")
        else:
            print_log("info", f"{'target_connections.yaml entry':<{_W}}: OK  (matched by s3_bucket_name='{config['s3_bucket_name']}')")
    elif target_engine == "KAFKA" and config.get("kafka_cluster_name"):
        _kname = config["kafka_cluster_name"]
        _kenv  = config.get("env", "")
        _kpool = config.get("kafka_identity_pool", "")
        target_conn = get_target_connection_by_cluster_name(_kname, _kenv, identity_pool=_kpool)
        if target_conn is None:
            print_log("error", f"{'target_connections.yaml entry':<{_W - 1}}: ERROR  (no entry with cluster_name='{_kname}' identity_pool='{_kpool}' env='{_kenv}')")
            error_log.append(f"[TARGET] no target_connections.yaml entry found with cluster_name='{_kname}', identity_pool='{_kpool}' and env='{_kenv}'")
        else:
            print_log("info", f"{'target_connections.yaml entry':<{_W}}: OK  (matched by cluster_name='{_kname}', identity_pool='{_kpool}')")
    else:
        print_log("info", f"{'target_name (resolved)':<{_W}}: {target_name}")
        target_conn = get_target_connection(target_name)
        if target_conn is None:
            print_log("error", f"{'target_connections.yaml entry':<{_W - 1}}: ERROR  ('{target_name}' not found)")
            error_log.append(f"[TARGET] target '{target_name}' not found in target_connections.yaml")

    # INFO: show configured replicat initial_load_parallel (FULL / INITIAL_LOAD only)
    _rep_type_tgt = (config.get("replication_type") or "").upper()
    if _rep_type_tgt in ("FULL", "INITIAL_LOAD"):
        _tgt_il_par_raw = (target_conn or {}).get("initial_load_parallel") if target_conn else None
        _tgt_il_par_val = int(_tgt_il_par_raw) if _tgt_il_par_raw is not None else None
        _tgt_il_par_str = str(_tgt_il_par_val) if _tgt_il_par_val else "not defined (will default to number of replicats at apply time)"
        _tgt_il_par_src = "target_connections.yaml" if _tgt_il_par_val else "—"
        print_log("info", f"{'initial_load replicat parallel':<{_W}}: {_tgt_il_par_str}  (source: {_tgt_il_par_src})")

    ########## KAFKA
    if target_engine == "KAFKA":
        print_log("info", "Validating Kafka replication target ...")
        if target_conn is None:
            print_log("warning", f"{'Kafka checks':<{_W - 2}}: WARNING  (skipped — no target_connections.yaml entry)")
        else:
            # Check k1: TCP reachability of every bootstrap server (comma-separated host:port[,host:port,...])
            bootstrap = (target_conn.get("bootstrap_servers") or "").strip()
            if not bootstrap:
                print_log("error", f"{'Kafka bootstrap_servers':<{_W - 1}}: ERROR  (empty in target_connections.yaml entry '{target_name}')")
                error_log.append(f"[KAFKA] bootstrap_servers not defined for target '{target_name}'")
            else:
                tcp_errors = []
                for host_port in [hp.strip() for hp in bootstrap.split(",") if hp.strip()]:
                    if ":" not in host_port:
                        tcp_errors.append((host_port, "missing :port"))
                        continue
                    host, port_s = host_port.rsplit(":", 1)
                    try:
                        port = int(port_s)
                    except ValueError:
                        tcp_errors.append((host_port, f"invalid port '{port_s}'"))
                        continue
                    ok_tcp, msg_tcp = check_tcp_reachable(host, port, timeout=5)
                    print_log("info" if ok_tcp else "error", f"{'Kafka broker TCP reachable':<{_W - (0 if ok_tcp else 1)}}: {'OK' if ok_tcp else 'ERROR':<1}  ({host}:{port}{' - ' + msg_tcp if not ok_tcp else ''})")
                    if not ok_tcp:
                        tcp_errors.append((host_port, msg_tcp))
                ok = not tcp_errors
                print_log("info" if ok else "error", f"{'Kafka bootstrap reachable':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({len(tcp_errors)} broker(s) unreachable)")
                if not ok:
                    error_log.extend(f"[KAFKA] broker '{hp}' unreachable: {err}" for hp, err in tcp_errors)

            # Check k2: Schema Registry TCP/HTTP reachability (connectivity only)
            sr_url = (target_conn.get("schema_registry_url") or "").strip()
            if not sr_url:
                print_log("warning", f"{'Kafka Schema-Registry':<{_W - 2}}: WARNING  (schema_registry_url empty)")
            else:
                ok_sr, msg_sr = check_http_reachable(sr_url, timeout=5)
                print_log("info" if ok_sr else "error", f"{'Kafka Schema-Registry reachable':<{_W - (0 if ok_sr else 1)}}: {'OK' if ok_sr else 'ERROR':<1}  ({sr_url}{'' if ok_sr else ' - ' + msg_sr})")
                if not ok_sr:
                    error_log.append(f"[KAFKA] Schema-Registry '{sr_url}' unreachable: {msg_sr}")

            # Check k3: OAuth authentication (Entra ID client_credentials)
            # Obtain the token once and reuse it for both the Schema Registry auth check (k4)
            # and the Kafka broker handshake (k5). Failing here means credentials are wrong
            # BEFORE we even try to connect to the broker — avoids misleading _TRANSPORT errors.
            _oauth_token = None
            ok_oauth, msg_oauth = False, ""
            _oauth_token, _oauth_err = get_kafka_oauth_token(target_conn, timeout=10)
            ok_oauth = _oauth_token is not None
            msg_oauth = "token obtained" if ok_oauth else _oauth_err
            print_log("info" if ok_oauth else "error", f"{'Kafka OAuth authentication':<{_W - (0 if ok_oauth else 1)}}: {'OK' if ok_oauth else 'ERROR':<1}  ({msg_oauth})")
            if not ok_oauth:
                error_log.append(f"[KAFKA] OAuth authentication failed: {msg_oauth}")

            # Check k4: Schema Registry authentication (Bearer token + identity pool + cluster headers)
            if sr_url and ok_oauth:
                _sr_identity_pool = target_conn.get("identity_pool")
                _sr_cluster_id    = target_conn.get("schema_registry_id")
                ok_sr_auth, msg_sr_auth = check_schema_registry_auth(
                    sr_url, _oauth_token,
                    identity_pool=_sr_identity_pool,
                    sr_cluster_id=_sr_cluster_id,
                    timeout=5,
                )
                print_log("info" if ok_sr_auth else "error", f"{'Kafka Schema-Registry auth':<{_W - (0 if ok_sr_auth else 1)}}: {'OK' if ok_sr_auth else 'ERROR':<1}  ({sr_url} - {msg_sr_auth})")
                if not ok_sr_auth:
                    error_log.append(f"[KAFKA] Schema-Registry authentication failed: {msg_sr_auth}")
            elif sr_url and not ok_oauth:
                print_log("warning", f"{'Kafka Schema-Registry auth':<{_W - 2}}: WARNING  (skipped — OAuth token not available)")

            # Check k5: broker handshake + SASL OAUTHBEARER + verification that the per-table
            # Kafka topics exist in the target cluster. The OGG kafka_confluent template builds
            # topic names as '<kafka_topic_prefix>.<table_lowercase>', so we compute that list here and
            # cross-check it against the cluster.
            topic_prefix        = (config.get("kafka_topic_prefix") or "").strip()
            config_tables_kafka = [t.split("(")[0].strip().upper() for t in config.get("tables", [])]
            expected_topics     = [kafka_topic_name(topic_prefix, t) for t in config_tables_kafka]

            ok_kt, existence, msg_kt = check_kafka_topics_exist(target_conn, expected_topics, timeout=10)
            if not ok_kt:
                # _TRANSPORT after a successful TCP check (k1) indicates that the execution
                # environment's firewall blocks Kafka protocol data on port 9092 (L7 inspection).
                # OGG runs on its own server with a separate network path — this is a false positive
                # from the AAP EE. Log as WARNING so validate does not block apply.
                # To fix: open egress from AAP execution nodes to the Kafka bootstrap on port 9092.
                _is_transport_err = "_TRANSPORT" in msg_kt or "Broker transport failure" in msg_kt
                if _is_transport_err:
                    print_log("warning", f"{'Kafka topics check':<{_W - 2}}: WARNING  (broker unreachable from this host — {msg_kt})")
                    print_log("warning", f"  Topics could not be verified from the AAP EE. Ensure the OGG server has network access to {bootstrap} (SASL_SSL).")
                else:
                    print_log("error", f"{'Kafka topics check':<{_W - 1}}: ERROR  (could not query cluster: {msg_kt})")
                    error_log.append(f"[KAFKA TOPICS] could not query cluster: {msg_kt}")
            else:
                missing_topics = [t for t in expected_topics if not existence.get(t, False)]
                found_count    = len(expected_topics) - len(missing_topics)
                if missing_topics:
                    print_log("error", f"{'Kafka topics in cluster':<{_W - 1}}: ERROR  ({found_count}/{len(expected_topics)} found, prefix '{topic_prefix}')")
                    for top in missing_topics:
                        print_log("error", f"  - missing topic: {top}")
                    error_log.extend(f"[KAFKA TOPICS] missing topic: {top}" for top in missing_topics)
                else:
                    print_log("info", f"{'Kafka topics in cluster':<{_W}}: OK  ({found_count}/{len(expected_topics)} found, prefix '{topic_prefix}')")

    ########## AWS-S3
    elif target_engine == "AWS-S3":
        print_log("info", "Validating AWS-S3 replication target ...")
        if target_conn is None:
            print_log("warning", f"{'AWS-S3 checks':<{_W - 2}}: WARNING  (skipped — no target_connections.yaml entry)")
        else:
            # Check s1: region S3 endpoint HTTP reachability (region-level health)
            region = (target_conn.get("aws_region") or "").strip()
            if not region:
                print_log("error", f"{'AWS-S3 aws_region':<{_W - 1}}: ERROR  (empty)")
                error_log.append(f"[AWS-S3] aws_region not defined for target '{target_name}'")
            else:
                s3_endpoint = f"https://s3.{region}.amazonaws.com"
                ok_s3, msg_s3 = check_http_reachable(s3_endpoint, timeout=5)
                print_log("info" if ok_s3 else "error", f"{'AWS-S3 region endpoint reachable':<{_W - (0 if ok_s3 else 1)}}: {'OK' if ok_s3 else 'ERROR':<1}  ({s3_endpoint} - {msg_s3})")
                if not ok_s3:
                    error_log.append(f"[AWS-S3] region endpoint '{s3_endpoint}' unreachable: {msg_s3}")

            # Check s2: STS::AssumeRole + HeadBucket + (optional) ListObjectsV2 on the prefix.
            # s3_bucket_name and s3_bucket_path come from the JSON input (user-provided).
            # bucket_path_prefix = s3_bucket_path + "/" + bbdd_name  (used by the OGG S3 template)
            s3_bucket  = (target_conn.get("s3_bucket_name") or "").strip()
            s3_path    = (config.get("s3_bucket_path") or "").strip()
            _bbdd_name = (config.get("bbdd_name") or "").upper()
            s3_prefix  = f"{s3_path}/{_bbdd_name}" if s3_path and _bbdd_name else (s3_path or _bbdd_name)
            if not s3_bucket:
                print_log("error", f"{'AWS-S3 s3_bucket_name':<{_W - 1}}: ERROR  (missing in target_connections.yaml entry '{target_name}')")
                error_log.append(f"[AWS-S3] 's3_bucket_name' not defined for target '{target_name}' in target_connections.yaml")
            if not s3_path:
                print_log("error", f"{'AWS-S3 s3_bucket_path':<{_W - 1}}: ERROR  (missing in JSON input — 's3_bucket_path' is required)")
                error_log.append(f"[AWS-S3] 's3_bucket_path' not defined in JSON input")
            if s3_bucket:
                ok_s3b, msg_s3b = check_s3_bucket_access(target_conn, s3_bucket, s3_bucket_path=s3_prefix, timeout=10)
                print_log("info" if ok_s3b else "error", f"{'AWS-S3 bucket access (STS+HeadBucket)':<{_W - (0 if ok_s3b else 1)}}: {'OK' if ok_s3b else 'ERROR':<1}  {msg_s3b}")
                if not ok_s3b:
                    error_log.append(f"[AWS-S3] bucket access check failed: {msg_s3b}")

    else:
        print_log("error", f"Unknown replication target: {target_engine}")
        error_log.append(f"[TARGET] unknown replication target: {target_engine}")

    ### CHECK 3 : Check deployment connection is correct
    print_log("info", "")
    print_log("info", "====== Check CDC Deployments ======")

    # OGG credential wallet alias names are limited to 30 characters.
    if target_conn is not None:
        _alias = (target_conn.get("ogg_alias_credential") or "").strip()
        if _alias and len(_alias) > 30:
            print_log("error", f"{'OGG alias credential length':<{_W - 1}}: ERROR  ('{_alias}' is {len(_alias)} chars — OGG wallet limit is 30)")
            error_log.append(f"[TARGET] ogg_alias_credential '{_alias}' exceeds 30-character OGG wallet limit ({len(_alias)} chars)")
        elif _alias:
            print_log("info", f"{'OGG alias credential length':<{_W}}: OK  ('{_alias}' — {len(_alias)}/30 chars)")

    ########## Oracle GoldenGate (for ORACLE | MONGO)
    if bbdd_engine in ("ORACLE","MONGO"):
        print_log("info", f"Validating Oracle GoldenGate deployment for source : {bbdd_engine}")

        # get OGG deployment info from inventory.yaml matching env / db_engine / load_type
        # (load_type pivot derived from replication_type — see load_types_for_replication()).
        deployments = get_matching_deployments(config["env"], bbdd_engine, load_types=load_types_for_replication(config.get("replication_type", "")))
        if not deployments:
            print_log("error", f"{'no matching deployments found':<{_W - 1}}: env={config['env']}, db_engine={bbdd_engine}")
            error_log.append(f"[CHECK DEPLOYMENTS] no matching deployments found for env={config['env']}, db_engine={bbdd_engine}")
        else:
            print_log("info", f"{'matching deployments found':<{_W}}: {len(deployments)}")
            for name, dep in deployments:
                print_log("info", f"+ {name}  ({dep.get('admin_url')})")

            for name, dep in deployments:
                print_log("info", f"OGG deployment: {name} ")

                # Check ogg_dep 1: TCP reachability on port 443
                admin_url = dep.get("admin_url", "")
                host      = urlparse(admin_url).hostname
                port      = urlparse(admin_url).port or 443
                ok        = False
                err_msg   = ""
                try:
                    with socket.create_connection((host, port), timeout=5):
                        ok = True
                except Exception as e:
                    err_msg = str(e)
                print_log("info" if ok else "error", f"{'+ OGG deployment reachable port 443':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({host}:{port}{'' if ok else ' - ' + err_msg})")
                if not ok:
                    error_log.append(f"[DEPLOYMENT {name}] not reachable on {host}:{port} - {err_msg}")

                # Check ogg_dep 2: deployment status via oggctl.py
                oggctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl", "oggctl.py")
                try:
                    result = subprocess.run(
                        [sys.executable, oggctl_path, "deployment", "status", "--deployment", name],
                        capture_output=True, text=True, timeout=30
                    )
                    ok        = result.returncode == 0
                    last_line = (result.stdout or result.stderr).strip().splitlines()[-1] if (result.stdout or result.stderr) else f"return code {result.returncode}"
                    # strip log prefix: "<ts> [LEVEL] cli.deployment: Deployment <name>: <status>" -> "<name>: <status>"
                    status    = last_line.split("Deployment ", 1)[1] if "Deployment " in last_line else last_line
                except Exception as e:
                    ok      = False
                    status  = str(e)
                print_log("info" if ok else "error", f"{'+ OGG deployment status':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({status})")
                if not ok:
                    error_log.append(f"[DEPLOYMENT {name}] status check failed: {status}")

                # Check ogg_dep 3: managed-process profile 'Critical' must exist on the deployment
                # (cdc-controller's apply will reference it when creating extracts/replicats; missing -> apply fails).
                # NOTE: OGG enforces an EXACT-CASE match on the profile name at process-create time
                # so this check must also be case-sensitive. Convention in this repo: 'Critical'.
                _MPP_NAME = "Critical"
                mp_ok, mp_exists, mp_msg = managed_process_profile_exists(name, _MPP_NAME)
                if not mp_ok:
                    print_log("error", f"{'+ managed-process profile':<{_W - 1}}: ERROR  (could not query: {mp_msg})")
                    error_log.append(f"[DEPLOYMENT {name}] managed-process profile check failed: {mp_msg}")
                elif not mp_exists:
                    print_log("error", f"{'+ managed-process profile':<{_W - 1}}: ERROR  (profile '{_MPP_NAME}' not found)")
                    error_log.append(f"[DEPLOYMENT {name}] managed-process profile '{_MPP_NAME}' not found")
                else:
                    print_log("info", f"{'+ managed-process profile':<{_W}}: OK  ('{_MPP_NAME}' exists)")

                # Check ogg_dep 4: deployment can connect to source databases (validate credential aliases)
                # Only applies to Extract deployments (naming convention _E<digits>_, e.g. _E1_, _E01_, _E91_); skip Replicats.
                if not re.search(r"_E\d+_", name):
                    continue

                # OGG credential alias names are stored in the unified db_connections.yaml entry
                # under conn_name_prim / conn_name_stby (STBY only validated when a STBY DSN exists).
                bbdd_conn = get_db_connection(f"{config['bbdd_name']}_{config['env']}")
                aliases_to_validate = [bbdd_conn.get("conn_name_prim") if bbdd_conn else f"{config['bbdd_name']}_PRIM_{config['env']}"]
                if bbdd_conn and bbdd_conn.get("conn_name_stby"):
                    aliases_to_validate.append(bbdd_conn["conn_name_stby"])

                # Build expected userid map: alias_name -> "db_user@conn_string" from db_connections.yaml.
                # Used below to detect conn_string drift between db_connections.yaml and OGG.
                _expected_userid = {}
                if bbdd_conn:
                    _db_user = bbdd_conn.get("db_user", "")
                    if bbdd_conn.get("conn_name_prim") and bbdd_conn.get("conn_string_prim"):
                        _expected_userid[bbdd_conn["conn_name_prim"]] = f"{_db_user}@{bbdd_conn['conn_string_prim']}"
                    if bbdd_conn.get("conn_name_stby") and bbdd_conn.get("conn_string_stby"):
                        _expected_userid[bbdd_conn["conn_name_stby"]] = f"{_db_user}@{bbdd_conn['conn_string_stby']}"

                for alias_name in aliases_to_validate:
                    try:
                        result = subprocess.run(
                            [sys.executable, oggctl_path, "credential-alias", "validate",
                             "--deployment", name, "--alias", alias_name],
                            capture_output=True, text=True, timeout=30
                        )
                        ok        = result.returncode == 0
                        last_line = (result.stdout or result.stderr).strip().splitlines()[-1] if (result.stdout or result.stderr) else f"return code {result.returncode}"
                        status    = last_line if ok else last_line[-200:]
                    except Exception as e:
                        ok      = False
                        status  = str(e)
                    label = "+ DB connection from OGG Deployment"
                    alias_not_found = not ok and ("404" in status or "does not exist" in status)
                    if ok:
                        print_log("info",    f"{label:<{_W}}: OK  ({alias_name})")
                        # Idempotency check: compare conn_string in OGG with db_connections.yaml
                        if alias_name in _expected_userid:
                            _info_r = subprocess.run(
                                [sys.executable, oggctl_path, "credential-alias", "info",
                                 "--alias", alias_name, "--deployment", name,
                                 "--format", "json"],
                                capture_output=True, text=True, timeout=30,
                            )
                            _current_userid = None
                            if _info_r.returncode == 0:
                                try:
                                    _current_userid = json.loads(_info_r.stdout).get("userid")
                                except Exception:
                                    pass
                            _conn_label = "+ DB conn_string check"
                            if _current_userid is None:
                                _err_detail = (_info_r.stderr or "").strip().splitlines()[-1][:120] if (_info_r.stderr or "").strip() else f"rc={_info_r.returncode}"
                                print_log("warning", f"{_conn_label:<{_W - 3}}: WARNING  ({alias_name}@{name}: could not retrieve userid from OGG — {_err_detail})")
                            elif _current_userid != _expected_userid[alias_name]:
                                print_log("warning", f"{_conn_label:<{_W - 3}}: WARNING  ({alias_name}@{name}: conn_string in OGG differs from db_connections.yaml — will be updated at apply time)")
                                warning_log.append(f"[DEPLOYMENT {name}] alias '{alias_name}': conn_string differs from db_connections.yaml — apply will delete and recreate it")
                            else:
                                print_log("info", f"{_conn_label:<{_W}}: OK  ({alias_name}: conn_string matches db_connections.yaml)")
                    elif alias_not_found:
                        print_log("warning", f"{label:<{_W - 3}}: WARNING  ({alias_name}: alias not found — will be created at apply time if needed)")
                        warning_log.append(f"[DEPLOYMENT {name}] credential alias '{alias_name}' not found — will be created automatically during apply")
                    else:
                        print_log("error",   f"{label:<{_W - 1}}: ERROR  ({alias_name}: {status})")
                        error_log.append(f"[DEPLOYMENT {name}] DB connection failed with alias '{alias_name}': {status}")

    ########## IBM InfoSphere (for DB2 )
    elif bbdd_engine == "DB2":
        print_log("info", "Validating IBM InfoSphere deployment for source : DB2")
        print_log("warning", f"{'InfoSphere deployment reachable':<{_W - 3}}: WIP")
        print_log("warning", f"{'InfoSphere admin authentication':<{_W - 3}}: WIP")
        print_log("warning", f"{'InfoSphere deployment status':<{_W - 3}}: WIP")

    else:
        print_log("error", f"Unknown bbdd_engine for deployment validation: {bbdd_engine}")
        error_log.append(f"[CHECK DEPLOYMENTS] unknown bbdd_engine: {bbdd_engine}")


    ### CHECK 4 : Check that requested tables are not part of another active replication process
    print_log("info", "")    
    print_log("info", "====== Check Existing CDC processes  ======")

    deployments = get_matching_deployments(config["env"], bbdd_engine, load_types=load_types_for_replication(config.get("replication_type", "")))
    if not deployments:
        print_log("error", f"no matching deployments found: env={config['env']}, db_engine={bbdd_engine}")
        error_log.append(f"[CHECK EXISTING PROCESSES] no matching deployments found for env={config['env']}, db_engine={bbdd_engine}")
    else:
        dep_names     = [name for name, _ in deployments]
        config_tables = [t.split("(")[0].strip().upper() for t in config.get("tables", [])]

        # === Extracts ===
        # 1) verify the configured tables are not already part of another Extract process (_E<digits>_ deployments)
        #    Conflict severity depends on the live status of the owning extract:
        #      - RUNNING        -> real conflict, ERROR (blocks validation)
        #      - any other state -> soft conflict, WARNING (informational, does not block)
        extract_deps = [n for n in dep_names if re.search(r"_E\d+_", n)]
        print_log("info", f"checking tables are not already in another extract on {len(extract_deps)} deployment(s): {', '.join(extract_deps) or '(none)'}")
        if extract_deps:
            extract_patterns = get_tables_in_use_by_extracts(extract_deps)
            real_conflicts = []   # (tbl, dep, ext, status, pattern) -> ERROR  (extract is RUNNING)
            soft_conflicts = []   # (tbl, dep, ext, status, pattern) -> WARNING (extract not running)
            # cache statuses to avoid re-querying for repeated (dep, ext) pairs across tables
            status_cache = {}
            for t in config_tables:
                # find_processes_for_table honors OGG wildcards (e.g. "SOE.*" matches "SOE.LOGON")
                for dep, ext, pat in find_processes_for_table(t, extract_patterns):
                    key = (dep, ext)
                    if key not in status_cache:
                        ok, st = get_extract_status_now(dep, ext)
                        status_cache[key] = st if ok else f"unknown ({st})"
                    st = status_cache[key]
                    if str(st).upper() == "RUNNING":
                        real_conflicts.append((t, dep, ext, st, pat))
                    else:
                        soft_conflicts.append((t, dep, ext, st, pat))

            def _pat_suffix(tbl, pat):
                # only show the pattern when it differs from the exact table (wildcard matched)
                return "" if pat == tbl else f" via TABLE '{pat}'"

            if real_conflicts:
                if request_type == "UPDATE":
                    # For UPDATE the table must already be in a RUNNING extract — this is expected.
                    print_log("info", f"{'tables already captured (expected for UPDATE)':<{_W}}: OK  ({len(real_conflicts)} table(s) in RUNNING extract)")
                    for tbl, dep, ext, st, pat in real_conflicts:
                        print_log("info", f"  - {tbl}  (extract '{ext}' on deployment '{dep}', status: {st}{_pat_suffix(tbl, pat)})")
                else:
                    print_log("warning", f"{'tables already captured by RUNNING extract':<{_W - 2}}: WARNING  ({len(real_conflicts)} conflict(s))")
                    for tbl, dep, ext, st, pat in real_conflicts:
                        print_log("warning", f"  - {tbl}  (extract '{ext}' on deployment '{dep}', status: {st}{_pat_suffix(tbl, pat)})")
                    warning_log.extend(
                        f"[EXTRACT CONFLICT] table already captured by RUNNING extract '{ext}' on deployment '{dep}' (status: {st}{_pat_suffix(tbl, pat)}) [table: {tbl}]"
                        for tbl, dep, ext, st, pat in real_conflicts
                    )

            if soft_conflicts:
                if request_type == "UPDATE":
                    print_log("info", f"{'tables in non-running extract (expected for UPDATE)':<{_W}}: OK  ({len(soft_conflicts)} found)")
                    for tbl, dep, ext, st, pat in soft_conflicts:
                        print_log("info", f"  - {tbl}  (extract '{ext}' on deployment '{dep}', status: {st}{_pat_suffix(tbl, pat)})")
                else:
                    print_log("warning", f"{'tables already defined in a non-running extract':<{_W - 2}}: WARNING  ({len(soft_conflicts)} found)")
                    for tbl, dep, ext, st, pat in soft_conflicts:
                        print_log("warning", f"  - {tbl}  (extract '{ext}' on deployment '{dep}', status: {st}{_pat_suffix(tbl, pat)})")
                    warning_log.extend(
                        f"[EXTRACT SOFT CONFLICT] table already defined in non-running extract '{ext}' on deployment '{dep}' (status: {st}{_pat_suffix(tbl, pat)}) [table: {tbl}]"
                        for tbl, dep, ext, st, pat in soft_conflicts
                    )

            if not real_conflicts and not soft_conflicts:
                if request_type == "UPDATE":
                    print_log("warning", f"{'tables not found in any extract':<{_W - 2}}: WARNING  (tables not yet captured — run CREATE first)")
                else:
                    print_log("info", f"{'tables free for new extract':<{_W}}: OK  ({len(config_tables)} table(s) not in any extract)")

        # === Replicats ===
        # Verifies whether the configured tables are already mapped by another Replicat process
        # (_R<digits>_ deployments). Severity mirrors the Extract check:
        #   - RUNNING        -> real conflict, ERROR (blocks validation)
        #   - any other state -> soft conflict, WARNING (informational, does not block)
        # When the match comes from an OGG wildcard (e.g. "MAP SOE.*" or "MAP *.*") the matched
        # pattern is shown in the message so the user can judge whether it's a real conflict or
        # a catch-all that the apply step will treat as REUSE.
        replicat_deps = [n for n in dep_names if re.search(r"_R\d+_", n)]
        print_log("info", f"checking tables are not already in another replicat on {len(replicat_deps)} deployment(s): {', '.join(replicat_deps) or '(none)'}")
        if replicat_deps:
            replicat_patterns = get_tables_in_use_by_replicats(replicat_deps)
            real_conflicts = []   # (tbl, dep, rep, status, pattern) -> ERROR  (replicat is RUNNING)
            soft_conflicts = []   # (tbl, dep, rep, status, pattern) -> WARNING (replicat not running)
            # cache statuses to avoid re-querying for repeated (dep, rep) pairs across tables
            status_cache = {}
            for t in config_tables:
                # find_processes_for_table honors OGG wildcards (e.g. "MAP *.*" matches any table)
                for dep, rep, pat in find_processes_for_table(t, replicat_patterns):
                    key = (dep, rep)
                    if key not in status_cache:
                        ok, st = get_replicat_status_now(dep, rep)
                        status_cache[key] = st if ok else f"unknown ({st})"
                    st = status_cache[key]
                    if str(st).upper() == "RUNNING":
                        real_conflicts.append((t, dep, rep, st, pat))
                    else:
                        soft_conflicts.append((t, dep, rep, st, pat))

            def _pat_suffix_rep(tbl, pat):
                # only show the pattern when it differs from the exact table (wildcard matched)
                return "" if pat == tbl else f" via MAP '{pat}'"

            if real_conflicts:
                if request_type == "UPDATE":
                    # For UPDATE the table must already be in a RUNNING replicat — this is expected.
                    print_log("info", f"{'tables already mapped (expected for UPDATE)':<{_W}}: OK  ({len(real_conflicts)} table(s) in RUNNING replicat)")
                    for tbl, dep, rep, st, pat in real_conflicts:
                        print_log("info", f"  - {tbl}  (replicat '{rep}' on deployment '{dep}', status: {st}{_pat_suffix_rep(tbl, pat)})")
                else:
                    # Map requested target_engine to the internal type returned by get_replicat_target_type
                    _te_to_internal = {"AWS-S3": "s3", "KAFKA": "kafka_confluent", "ORACLE": "oracle"}
                    _requested_target = _te_to_internal.get((target_engine or "").upper())

                    # Split by whether the existing replicat shares the same target type
                    _same_target = []
                    _diff_target = []
                    for item in real_conflicts:
                        tbl, dep, rep, st, pat = item
                        existing_target = get_replicat_target_type(dep, rep)
                        if existing_target is not None and _requested_target is not None and existing_target != _requested_target:
                            _diff_target.append(item)
                        else:
                            _same_target.append(item)

                    if _same_target:
                        print_log("error", f"{'tables already mapped by RUNNING replicat (same target)':<{_W - 1}}: ERROR  ({len(_same_target)} conflict(s))")
                        for tbl, dep, rep, st, pat in _same_target:
                            print_log("error", f"  - {tbl}  (replicat '{rep}' on deployment '{dep}', status: {st}{_pat_suffix_rep(tbl, pat)})")
                        error_log.extend(
                            f"[REPLICAT CONFLICT] table already mapped by RUNNING replicat '{rep}' on deployment '{dep}' (status: {st}{_pat_suffix_rep(tbl, pat)}) [table: {tbl}]"
                            for tbl, dep, rep, st, pat in _same_target
                        )

                    if _diff_target:
                        print_log("warning", f"{'tables mapped by RUNNING replicat with different target':<{_W - 2}}: WARNING  ({len(_diff_target)} found)")
                        for tbl, dep, rep, st, pat in _diff_target:
                            print_log("warning", f"  - {tbl}  (replicat '{rep}' on deployment '{dep}', status: {st}{_pat_suffix_rep(tbl, pat)})")
                        warning_log.extend(
                            f"[REPLICAT CONFLICT] table mapped by RUNNING replicat '{rep}' with different target type on deployment '{dep}' (status: {st}{_pat_suffix_rep(tbl, pat)}) [table: {tbl}]"
                            for tbl, dep, rep, st, pat in _diff_target
                        )

            if soft_conflicts:
                if request_type == "UPDATE":
                    print_log("info", f"{'tables in non-running replicat (expected for UPDATE)':<{_W}}: OK  ({len(soft_conflicts)} found)")
                    for tbl, dep, rep, st, pat in soft_conflicts:
                        print_log("info", f"  - {tbl}  (replicat '{rep}' on deployment '{dep}', status: {st}{_pat_suffix_rep(tbl, pat)})")
                else:
                    print_log("warning", f"{'tables already defined in a non-running replicat':<{_W - 2}}: WARNING  ({len(soft_conflicts)} found)")
                    for tbl, dep, rep, st, pat in soft_conflicts:
                        print_log("warning", f"  - {tbl}  (replicat '{rep}' on deployment '{dep}', status: {st}{_pat_suffix_rep(tbl, pat)})")
                    warning_log.extend(
                        f"[REPLICAT SOFT CONFLICT] table already defined in non-running replicat '{rep}' on deployment '{dep}' (status: {st}{_pat_suffix_rep(tbl, pat)}) [table: {tbl}]"
                        for tbl, dep, rep, st, pat in soft_conflicts
                    )

            if not real_conflicts and not soft_conflicts:
                if request_type == "UPDATE":
                    print_log("warning", f"{'tables not found in any replicat':<{_W - 2}}: WARNING  (tables not yet mapped — run CREATE first)")
                else:
                    print_log("info", f"{'tables free for new replicat':<{_W}}: OK  ({len(config_tables)} table(s) not in any replicat)")


    ### FINAL SUMMARY
    print_log("info", f"")
    print_log("info", f"====== Final Summary ======")

    # always list warnings first (informational, do NOT change the result)
    if warning_log:
        print_log("warning", f"{'warnings found':<{_W - 2}}: WARNING  ({len(warning_log)} warning(s) found)")
        for i, w in enumerate(warning_log, start=1):
            print_log("warning", f"  {i:>2}. {w}")

    # errors block the validation
    if error_log:
        print_log("error", f"{'validate result':<{_W - 1}}: ERROR  ({len(error_log)} error(s) found)")
        for i, err in enumerate(error_log, start=1):
            print_log("error", f"  {i:>2}. {err}")
        print_log("info", "end validate")
        return 1

    if warning_log:
        print_log("info", f"{'validate result':<{_W}}: OK  (all checks passed, {len(warning_log)} warning(s) — review above)")
    else:
        print_log("info", f"{'validate result':<{_W}}: OK  (all checks passed)")
    print_log("info", "end validate")
    return 0
