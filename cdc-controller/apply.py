import os
import re
import sys
import json
import time
import subprocess
from datetime import datetime, timedelta
try: import oracledb
except ImportError: oracledb = None
from utils import *
from utils import _nn_to_b36, _b36_to_nn


def plan(filepath):
    # Thin wrapper: runs apply() in dry-run mode. Reports exactly what would be created /
    # modified / started, without executing any change against the OGG deployments or
    # processes. Returns the same code apply() would have returned (0 = clean plan).
    return apply(filepath, dry_run=True)


# ────────────────────────────────────────────────────────────────────────────────────────
# Shared helpers used by the FULL flow (_apply_create_full). The ONLINE flow keeps its
# inline implementation for now to minimize the blast radius of this change.
# ────────────────────────────────────────────────────────────────────────────────────────

def _run_oggctl(cmd, label, name, dep, dry_run, width):
    # Run an oggctl subcommand uniformly: log a single OK/ERROR line and return
    # (ok: bool, last_line: str). In dry-run mode, log the planned action and return OK
    # without executing.
    if dry_run:
        print_log("info", f"(plan) would {label} '{name}' @ '{dep}'  (cmd={' '.join(cmd[2:6])})")
        return True, "(plan) skipped"
    try:
        result    = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        ok        = result.returncode == 0
        output    = (result.stdout or "") + (result.stderr or "")
        last_line = output.strip().splitlines()[-1] if output.strip() else f"return code {result.returncode}"
    except Exception as e:
        ok        = False
        last_line = str(e)
    print_log("info" if ok else "error", f"{label:<{width - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({name}@{dep}: {last_line})")
    return ok, last_line


def _credential_alias_exists(oggctl_path, alias, dep_name):
    # Returns True if the alias appears in 'credential-alias list' output for this deployment.
    try:
        r = subprocess.run(
            [sys.executable, oggctl_path, "credential-alias", "list", "--deployment", dep_name],
            capture_output=True, text=True, timeout=30,
        )
        if r.returncode != 0:
            return False
        # Output: domain name on its own line, aliases indented below it
        for line in (r.stdout or "").splitlines():
            if line.strip().upper() == alias.upper():
                return True
    except Exception:
        pass
    return False


def _get_credential_alias_userid(oggctl_path, alias, dep_name):
    # Returns (ok, userid_str) by calling 'credential-alias info --format json'.
    # userid_str is the value OGG stores for the alias (e.g. "C##GGADMIN@(DESCRIPTION=...)").
    try:
        r = subprocess.run(
            [sys.executable, oggctl_path, "credential-alias", "info",
             "--alias", alias, "--deployment", dep_name, "--format", "json"],
            capture_output=True, text=True, timeout=30,
        )
        if r.returncode != 0:
            return False, None
        data = json.loads(r.stdout)
        userid = data.get("userid")
        if userid is not None:
            return True, str(userid)
    except Exception:
        pass
    return False, None


def _replicat_target_matches(target_type, details, rep_target, config):
    # Returns True when the target-type AND target-specific identifiers from an existing
    # replicat's .properties file match what the user is requesting in their JSON config.
    # This prevents accidentally reusing a replicat that goes to the wrong bucket / cluster
    # when multiple replicats of the same broad type (s3 / kafka_confluent) exist on a trail.
    if target_type != rep_target:
        return False
    if rep_target == "s3":
        requested = (config.get("s3_bucket_name") or "").strip()
        existing  = details.get("s3_bucket_name", "").strip()
        return existing == requested
    if rep_target == "kafka_confluent":
        cluster_norm = (config.get("kafka_cluster_name") or "").replace("-", "_")
        pool_norm    = (config.get("kafka_identity_pool") or "").replace("-", "_")
        expected_stem = f"{cluster_norm}_{pool_norm}"
        existing_stem = details.get("producer_config_stem", "")
        return existing_stem == expected_stem
    # oracle / None: type match alone is sufficient (at most one oracle replicat per trail)
    return True


def _config_file_exists(oggctl_path, filename, dep_name):
    # Returns True if the config file exists in dep_name.
    # Uses 'config-file get <filename>': exit 0 = exists, non-zero = not found.
    # More reliable than parsing 'list' output (avoids format/case-sensitivity issues).
    try:
        r = subprocess.run(
            [sys.executable, oggctl_path, "config-file", "get", filename, "--deployment", dep_name],
            capture_output=True, text=True, timeout=30,
        )
        return r.returncode == 0
    except Exception:
        return False


def _purge_task_exists(oggctl_path, task_name, dep_name):
    # Returns True if a purge-trail task with 'task_name' already exists in dep_name.
    # Uses 'task purge-trail list' output (one task name per line).
    try:
        r = subprocess.run(
            [sys.executable, oggctl_path, "task", "purge-trail", "list", "--deployment", dep_name],
            capture_output=True, text=True, timeout=30,
        )
        if r.returncode != 0:
            return False
        for line in (r.stdout or "").splitlines():
            if line.strip().upper() == task_name.upper():
                return True
    except Exception:
        pass
    return False


def _ensure_credential_alias(oggctl_path, alias, userid, password_credential, dep_name, dry_run, width, label="DB connection (alias)"):
    # Ensure a credential alias exists in an OGG deployment.
    # 1. credential-alias list  → if alias found, log OK and return True.
    # 2. Not found              → WARNING + credential-alias create.
    # 3. Re-validate after creation to confirm connectivity (Oracle only; skipped for others).
    # Returns True when the alias is ready, False when creation fails.
    if dry_run:
        print_log("info", f"(plan) would check/create credential alias '{alias}' @ '{dep_name}'")
        return True

    _alias_deleted = False
    if _credential_alias_exists(oggctl_path, alias, dep_name):
        # Alias exists — verify the userid matches db_connections.yaml (idempotency check).
        ok_info, current_userid = _get_credential_alias_userid(oggctl_path, alias, dep_name)
        if ok_info and current_userid != userid:
            print_log("warning", f"{label:<{width - 2}}: WARNING  ({alias}@{dep_name}: conn_string differs from db_connections.yaml — deleting and recreating)")
            del_cmd = [sys.executable, oggctl_path, "credential-alias", "delete",
                       "--alias", alias, "--deployment", dep_name]
            del_ok, _ = _run_oggctl(del_cmd, "credential-alias delete", alias, dep_name, dry_run=False, width=width)
            if not del_ok:
                return False
            _alias_deleted = True
            # fall through to create below
        else:
            print_log("info", f"{label:<{width}}: OK  ({alias}@{dep_name})")
            return True

    # --- alias not found or was deleted due to conn_string drift: create it ---
    if not _alias_deleted:
        print_log("warning", f"{label:<{width - 2}}: WARNING  ({alias} not found@{dep_name} — creating)")
    create_cmd = [sys.executable, oggctl_path,
                  "credential-alias", "create",
                  "--alias",        alias,
                  "--userid",       userid,
                  "--password-env", password_credential,
                  "--deployment",   dep_name]
    ok, _ = _run_oggctl(create_cmd, "credential-alias create", alias, dep_name, dry_run=False, width=width)
    if not ok:
        return False

    # --- re-validate after creation (connectivity check, best-effort) ---
    validate_cmd = [sys.executable, oggctl_path, "credential-alias", "validate",
                    "--alias", alias, "--deployment", dep_name]
    try:
        r = subprocess.run(validate_cmd, capture_output=True, text=True, timeout=30)
        ok_v = r.returncode == 0
    except Exception:
        ok_v = False
    print_log("info" if ok_v else "warning",
              f"{label + ' validate':<{width - (0 if ok_v else 2)}}: {'OK' if ok_v else 'WARNING (connectivity check skipped)':<1}  ({alias}@{dep_name})")
    return True


def _ensure_kafka_target_prereqs(oggctl_path, target_name, tc, replicat_deps, dry_run, width):
    # For each replicat deployment: ensure the Kafka OAuth credential alias exists,
    # the kafka producer properties file exists, and the kafka client JAAS config exists.
    # Uses list-based existence checks — only creates resources that are genuinely missing.
    # Returns True if all checks/creates succeed, False on first failure.
    ogg_alias     = tc.get("ogg_alias_credential", "")
    client_id     = tc.get("client_id", "")
    client_secret = tc.get("client_secret_credential", "")
    cluster_name  = tc.get("cluster_name", "").replace("-", "_")
    identity_pool = tc.get("identity_pool", "").replace("-", "_")
    env           = tc.get("env", "")
    producer_file = f"kafka_producer_{cluster_name}_{identity_pool}_{env}_oauth.properties"
    jaas_file     = f"kafka_client_{cluster_name}_{identity_pool}_{env}_oauth_jaas.conf"
    for dep in replicat_deps:
        # 1. Credential alias
        ok = _ensure_credential_alias(oggctl_path, ogg_alias, client_id, client_secret, dep, dry_run, width, label="Credential alias")
        if not ok:
            return False
        # 2. Kafka producer properties file
        if not dry_run and _config_file_exists(oggctl_path, producer_file, dep):
            print_log("info", f"{'kafka producer config-file':<{width}}: OK  ({producer_file}@{dep}: already exists)")
        else:
            ok, _ = _run_oggctl(
                [sys.executable, oggctl_path, "config-file", "create", producer_file,
                 "--template", "kafka_producer", "--target-name", target_name, "--deployment", dep],
                "kafka producer config-file", producer_file, dep, dry_run, width,
            )
            if not ok:
                return False
        # 3. Kafka client JAAS config file
        if not dry_run and _config_file_exists(oggctl_path, jaas_file, dep):
            print_log("info", f"{'kafka client jaas config-file':<{width}}: OK  ({jaas_file}@{dep}: already exists)")
        else:
            ok, _ = _run_oggctl(
                [sys.executable, oggctl_path, "config-file", "create", jaas_file,
                 "--template", "kafka_client_jaas", "--target-name", target_name,
                 "--secret-env", f"client_secret={client_secret}", "--deployment", dep],
                "kafka client jaas config-file", jaas_file, dep, dry_run, width,
            )
            if not ok:
                return False
    return True


def _ensure_s3_target_prereqs(oggctl_path, tc, replicat_deps, dry_run, width):
    # For each replicat deployment: ensure the AWS S3 credential alias exists.
    # The alias stores the long-term IAM access key used by the OGG S3 event handler
    # (userid = aws_access_key_id, password = aws_secret_access_key).
    # Returns True if all checks/creates succeed, False on first failure.
    ogg_alias  = tc.get("ogg_alias_credential", "")
    userid     = tc.get("aws_access_key_id", "")
    secret_env = tc.get("aws_secret_access_key_credential", "")
    if not ogg_alias or not userid or not secret_env:
        print_log("error", f"{'S3 credential alias prereq':<{width - 1}}: ERROR  (ogg_alias_credential / aws_access_key_id / aws_secret_access_key_credential missing in target_connections.yaml)")
        return False
    for dep in replicat_deps:
        ok = _ensure_credential_alias(oggctl_path, ogg_alias, userid, secret_env, dep, dry_run, width, label="S3 credential alias")
        if not ok:
            return False
    return True


def _verify_extract_report(dep_name, ext_name, after_label, dry_run):
    # Single error scan on the extract's report file 30 seconds after start/restart.
    # Returns True if no errors are detected. A report we cannot read is treated as
    # inconclusive (warning, returns True) so transient API hiccups don't block the apply.
    #
    # Robustness against OGG auto-restart: we anchor the scan window to a wall-clock
    # timestamp captured RIGHT BEFORE the verify sleep. If the 'Critical' profile restarts
    # the extract after an ABEND, the new 'Starting at' line goes into the report AFTER
    # our error, but the timestamp filter still picks up the error. We subtract 10 s as a
    # safety margin (clock drift between the controller host and the OGG admin server).
    if dry_run:
        print_log("info", f"(plan) would verify '{ext_name}' for errors (skipped in dry-run)")
        return True
    label    = f"verifying '{ext_name}' for errors"
    _W       = 38
    since_dt = datetime.now() - timedelta(seconds=10)
    for wait_s, round_label in [(10, "after 10s")]:
        time.sleep(wait_s)
        rpt_ok, has_err, err_lines, err_msg = check_extract_report_for_errors(dep_name, ext_name, since_dt=since_dt)
        if not rpt_ok:
            print_log("warning", f"{label:<{_W}}: WARNING  ({round_label}, report unreadable: {err_msg})")
            continue
        if has_err:
            first_err = err_lines[0].strip()[:140] if err_lines else "unknown"
            more      = f", +{len(err_lines) - 1} more" if len(err_lines) > 1 else ""
            print_log("error", f"{label:<{_W - 1}}: ERROR  ({round_label}, {len(err_lines)} error(s) found: {first_err}{more})")
            # Detail dump for the audit log (kept multi-line so the operator sees every OGG-NNNNN).
            for line in err_lines[:10]:
                print_log("error", f"  - {line}")
            if len(err_lines) > 10:
                print_log("error", f"  ... and {len(err_lines) - 10} more error line(s) (truncated)")
            return False
        print_log("info", f"{label:<{_W}}: OK  ({round_label}, no errors found)")
    return True


def _verify_replicat_report(dep_name, rep_name, after_label, dry_run):
    # Same as _verify_extract_report but for replicats (uses the replicat report endpoint).
    # See the extract version for the rationale of the timestamp-anchored scan window.
    if dry_run:
        print_log("info", f"(plan) would verify '{rep_name}' for errors (skipped in dry-run)")
        return True
    label    = f"verifying '{rep_name}' for errors"
    _W       = 38
    since_dt = datetime.now() - timedelta(seconds=10)
    for wait_s, round_label in [(10, "after 10s")]:
        time.sleep(wait_s)
        rpt_ok, has_err, err_lines, err_msg = check_replicat_report_for_errors(dep_name, rep_name, since_dt=since_dt)
        if not rpt_ok:
            print_log("warning", f"{label:<{_W}}: WARNING  ({round_label}, report unreadable: {err_msg})")
            continue
        if has_err:
            first_err = err_lines[0].strip()[:140] if err_lines else "unknown"
            more      = f", +{len(err_lines) - 1} more" if len(err_lines) > 1 else ""
            print_log("error", f"{label:<{_W - 1}}: ERROR  ({round_label}, {len(err_lines)} error(s) found: {first_err}{more})")
            for line in err_lines[:10]:
                print_log("error", f"  - {line}")
            if len(err_lines) > 10:
                print_log("error", f"  ... and {len(err_lines) - 10} more error line(s) (truncated)")
            return False
        print_log("info", f"{label:<{_W}}: OK  ({round_label}, no errors found)")
    return True


def _parse_added_items_from_msg(msg):
    # Parse the message returned by add_tables_to_{extract,replicat}() to extract the
    # list of items (tables / MAPs) the helper actually appended to the PRM.
    # Known formats:
    #   "added N new table(s): T1, T2, T3"
    #   "added N new MAP(s): T1, T2, T3"
    #   "no new tables to add (all N already covered)"
    if not msg or msg.startswith("no new"):
        return []
    if ":" not in msg:
        return []
    _, rest = msg.split(":", 1)
    return [t.strip().upper() for t in rest.split(",") if t.strip()]


def _parse_tables_from_config(config):
    # Parse the 'tables' field of the JSON input and split each entry into the OWNER.TABLE
    # name and an optional column allow-list.
    #
    # Accepted forms per entry:
    #   "SCHEMA.TABLE"              -> no column restriction (all columns replicated)
    #   "SCHEMA.TABLE(*)"           -> no column restriction (explicit form)
    #   "SCHEMA.TABLE(COL1,COL2)"   -> replicat-side column allow-list. Extracts capture
    #                                   every column regardless; only the replicat's MAP
    #                                   honours the list via a COLMAP clause.
    #
    # Returns (config_tables, column_map):
    #   config_tables : list[str]   uppercased 'OWNER.TABLE' names, preserving input order
    #   column_map    : dict[str, list[str]|None]   per-table allow-list, or None when the
    #                                                table is requested without restriction.
    names = []
    cols  = {}
    for raw in config.get("tables", []):
        if "(" in raw:
            name  = raw.split("(", 1)[0].strip().upper()
            inner = raw.split("(", 1)[1].rsplit(")", 1)[0].strip()
            cols[name] = (
                [c.strip().upper() for c in inner.split(",") if c.strip()]
                if inner and inner != "*"
                else None
            )
        else:
            name = raw.strip().upper()
            cols[name] = None
        names.append(name)
    return names, cols


def _build_replicat_table_entry(name, comment, column_map, **extras):
    # Helper to build one dict for the --tables-json payload of a replicat. The 'columns'
    # field is added only when the JSON input requested a column allow-list for THIS table;
    # extras (e.g. 'threadrange' for IL replicats) are merged in. Extracts must NOT use this
    # — they always capture every column regardless of the JSON input.
    entry = {"name": name, "comment": comment}
    cols  = (column_map or {}).get(name)
    if cols:
        entry["columns"] = cols
    entry.update(extras)
    return entry


def _apply_delete_request(config, dry_run=False):
    # DELETE flow. Removes the requested tables from the replication set, in 4 steps:
    #
    #   Step 1 — IL Replicats : deployments with proc_type=replicat + load_type=initial_load.
    #                            Any IL replicat mapping a requested table is fully STOPPED
    #                            and DELETED (initial_load is a one-shot bulk apply; the
    #                            replicat is single-table by design).
    #   Step 2 — IL Extracts  : deployments with proc_type=extract  + load_type=initial_load.
    #                            Same as above, full delete.
    #   Step 3 — Primary Replicat : proc_type=replicat + load_type=online. The replicat's
    #                                PRM is patched to remove the MAP lines for the requested
    #                                tables (partial delete). If the PRM ends up with zero
    #                                MAP lines, the replicat is fully deleted (stop + delete).
    #   Step 4 — Primary Extract  : proc_type=extract + load_type=online. The extract's PRM
    #                                is patched to remove the TABLE lines for the requested
    #                                tables (partial delete). If empty afterwards, the extract
    #                                is fully deleted (stop + UNREGISTER + delete).
    #
    # Wildcard PRM entries are NOT touched (we don't add TABLEEXCLUDE / MAPEXCLUDE).
    # If a requested table is only covered by a wildcard, the helper emits a WARNING and the
    # process is left untouched for that table.
    #
    # In dry_run mode every action is logged with the '(plan)' prefix and no OGG resource is
    # mutated. The final summary tables are still printed showing what WOULD be done.
    mode_label   = "plan" if dry_run else "apply"
    _W           = 38
    bbdd_engine  = (config.get("bbdd_engine") or "").upper()
    env          = config["env"]
    bbdd_name    = (config.get("bbdd_name") or "").upper()
    # DELETE only needs table names; any column allow-list in the JSON is ignored — we
    # remove the whole TABLE/MAP entry regardless of column scope.
    config_tables, _ = _parse_tables_from_config(config)

    print_log("info", f"====== DELETE flow (engine={bbdd_engine}, bbdd={bbdd_name}, env={env}) ======")
    if not config_tables:
        print_log("error", "no tables defined in JSON 'tables' field; nothing to delete")
        return 1

    # Result accumulators (used by the Final Summary at the end)
    il_rep_delete_results  = []   # [{dep, ext_name, table, action, status}]
    il_ext_delete_results  = []   # [{dep, ext_name, table, action, status}]
    pri_rep_delete_results = []   # [{dep, ext_name, action, tables_removed, status}]
    pri_ext_delete_results = []   # [{dep, ext_name, action, tables_removed, status}]

    # ─── Deployments split by proc_type + load_type ───────────────────────────────────────
    il_rep_deps  = [n for n, _ in get_matching_deployments(env, bbdd_engine, load_types=["initial_load"], proc_types=["replicat"])]
    il_ext_deps  = [n for n, _ in get_matching_deployments(env, bbdd_engine, load_types=["initial_load"], proc_types=["extract"])]
    pri_rep_deps = [n for n, _ in get_matching_deployments(env, bbdd_engine, load_types=["online"],       proc_types=["replicat"])]
    pri_ext_deps = [n for n, _ in get_matching_deployments(env, bbdd_engine, load_types=["online"],       proc_types=["extract"])]

    # Track which requested tables we found in any process — anything left at the end
    # is a no-op that we warn about.
    tables_seen = set()

    # ═══ Step 1/4: IL Replicats — full STOP+DELETE ═══════════════════════════════════════
    print_log("info", "")
    print_log("info", "------ Step 1/4: initial_load Replicats — full delete ------")
    if not il_rep_deps:
        print_log("info", "no initial_load Replicat deployment found; skipping")
    else:
        rep_patterns = get_tables_in_use_by_replicats(il_rep_deps)
        # Build set of (dep, name, matched_tables[]) — a replicat is picked if it maps at
        # least one of the requested tables.
        affected = {}   # (dep, name) -> set(matched_tables)
        for tbl in config_tables:
            for dep, rep, _pat in find_processes_for_table(tbl, rep_patterns):
                affected.setdefault((dep, rep), set()).add(tbl)
                tables_seen.add(tbl)
        if not affected:
            print_log("info", "no initial_load Replicat maps any of the requested tables; nothing to delete")
        for (dep, rep_name), matched in sorted(affected.items()):
            tbl_str = ", ".join(sorted(matched))
            if dry_run:
                print_log("info", f"(plan) would full-delete IL replicat '{rep_name}' on '{dep}' (covers {tbl_str})")
                il_rep_delete_results.append({"dep": dep, "ext_name": rep_name, "table": tbl_str, "action": "(plan) would full-delete", "status": "(plan)"})
                continue
            print_log("info", f"deleting IL replicat '{rep_name}' on '{dep}' (covers {tbl_str}) ...")
            ok, msg = delete_replicat_full(dep, rep_name)
            print_log("info" if ok else "error", f"{'initial_load replicat delete':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({rep_name}@{dep}: {msg})")
            il_rep_delete_results.append({"dep": dep, "ext_name": rep_name, "table": tbl_str, "action": "full-delete", "status": "deleted" if ok else f"ERROR ({msg})"})

    # ═══ Step 2/4: IL Extracts — full STOP+DELETE ════════════════════════════════════════
    print_log("info", "")
    print_log("info", "------ Step 2/4: initial_load Extracts — full delete ------")
    if not il_ext_deps:
        print_log("info", "no initial_load Extract deployment found; skipping")
    else:
        ext_patterns = get_tables_in_use_by_extracts(il_ext_deps)
        affected = {}
        for tbl in config_tables:
            for dep, ext, _pat in find_processes_for_table(tbl, ext_patterns):
                affected.setdefault((dep, ext), set()).add(tbl)
                tables_seen.add(tbl)
        if not affected:
            print_log("info", "no initial_load Extract captures any of the requested tables; nothing to delete")
        for (dep, ext_name), matched in sorted(affected.items()):
            tbl_str = ", ".join(sorted(matched))
            if dry_run:
                print_log("info", f"(plan) would full-delete IL extract '{ext_name}' on '{dep}' (covers {tbl_str})")
                il_ext_delete_results.append({"dep": dep, "ext_name": ext_name, "table": tbl_str, "action": "(plan) would full-delete", "status": "(plan)"})
                continue
            print_log("info", f"deleting IL extract '{ext_name}' on '{dep}' (covers {tbl_str}) ...")
            ok, msg = delete_extract_full(dep, ext_name, unregister=False)
            print_log("info" if ok else "error", f"{'initial_load extract delete':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({ext_name}@{dep}: {msg})")
            il_ext_delete_results.append({"dep": dep, "ext_name": ext_name, "table": tbl_str, "action": "full-delete", "status": "deleted" if ok else f"ERROR ({msg})"})

    # ═══ Step 3/4: Primary Replicat — partial PRM patch, full delete if empty ════════════
    print_log("info", "")
    print_log("info", "------ Step 3/4: Primary Replicat — partial PRM delete (full delete if empty) ------")
    if not pri_rep_deps:
        print_log("info", "no online Replicat deployment found; skipping")
    else:
        rep_patterns = get_tables_in_use_by_replicats(pri_rep_deps)
        affected = {}
        for tbl in config_tables:
            for dep, rep, _pat in find_processes_for_table(tbl, rep_patterns):
                affected.setdefault((dep, rep), set()).add(tbl)
                tables_seen.add(tbl)
        if not affected:
            print_log("info", "no online Replicat maps any of the requested tables; nothing to do")
        for (dep, rep_name), matched in sorted(affected.items()):
            tbl_str = ", ".join(sorted(matched))
            if dry_run:
                print_log("info", f"(plan) would remove {len(matched)} MAP(s) from primary replicat '{rep_name}' on '{dep}' ({tbl_str}); full-delete if PRM ends up empty")
                pri_rep_delete_results.append({"dep": dep, "ext_name": rep_name, "action": "(plan) partial-delete (?empty)", "tables_removed": tbl_str, "status": "(plan)"})
                continue
            print_log("info", f"removing MAP(s) {tbl_str} from primary replicat '{rep_name}' on '{dep}' ...")
            ok, removed, remaining, msg = remove_tables_from_replicat(dep, rep_name, sorted(matched))
            removed_str = ", ".join(removed) if removed else "(none)"
            print_log("info" if ok else "error", f"{'replicat partial-delete':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({rep_name}@{dep}: {msg})")
            if not ok:
                pri_rep_delete_results.append({"dep": dep, "ext_name": rep_name, "action": "partial-delete (failed)", "tables_removed": removed_str, "status": f"ERROR ({msg})"})
                continue
            if remaining == 0:
                print_log("info", f"primary replicat '{rep_name}' has no remaining MAP lines — full-deleting it")
                ok2, msg2 = delete_replicat_full(dep, rep_name)
                if ok2:
                    pri_rep_delete_results.append({"dep": dep, "ext_name": rep_name, "action": "full-delete (PRM empty)", "tables_removed": removed_str, "status": "deleted"})
                else:
                    pri_rep_delete_results.append({"dep": dep, "ext_name": rep_name, "action": "full-delete attempted", "tables_removed": removed_str, "status": f"ERROR ({msg2})"})
            else:
                pri_rep_delete_results.append({"dep": dep, "ext_name": rep_name, "action": f"partial-delete ({len(removed)} of {len(removed) + remaining})", "tables_removed": removed_str, "status": "running" if removed else "unchanged"})

    # ═══ Step 4/4: Primary Extract — partial PRM patch, full delete if empty (unreg) ═════
    print_log("info", "")
    print_log("info", "------ Step 4/4: Primary Extract — partial PRM delete (full delete if empty, with UNREGISTER) ------")
    if not pri_ext_deps:
        print_log("info", "no online Extract deployment found; skipping")
    else:
        ext_patterns = get_tables_in_use_by_extracts(pri_ext_deps)
        affected = {}
        for tbl in config_tables:
            for dep, ext, _pat in find_processes_for_table(tbl, ext_patterns):
                affected.setdefault((dep, ext), set()).add(tbl)
                tables_seen.add(tbl)
        if not affected:
            print_log("info", "no online Extract captures any of the requested tables; nothing to do")
        for (dep, ext_name), matched in sorted(affected.items()):
            tbl_str = ", ".join(sorted(matched))
            if dry_run:
                print_log("info", f"(plan) would remove {len(matched)} TABLE(s) from primary extract '{ext_name}' on '{dep}' ({tbl_str}); full-delete + UNREGISTER if PRM ends up empty")
                pri_ext_delete_results.append({"dep": dep, "ext_name": ext_name, "action": "(plan) partial-delete (?empty)", "tables_removed": tbl_str, "status": "(plan)"})
                continue
            print_log("info", f"removing TABLE(s) {tbl_str} from primary extract '{ext_name}' on '{dep}' ...")
            ok, removed, remaining, msg = remove_tables_from_extract(dep, ext_name, sorted(matched))
            removed_str = ", ".join(removed) if removed else "(none)"
            print_log("info" if ok else "error", f"{'extract partial-delete':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({ext_name}@{dep}: {msg})")
            if not ok:
                pri_ext_delete_results.append({"dep": dep, "ext_name": ext_name, "action": "partial-delete (failed)", "tables_removed": removed_str, "status": f"ERROR ({msg})"})
                continue
            if remaining == 0:
                print_log("info", f"primary extract '{ext_name}' has no remaining TABLE lines — full-deleting it (with UNREGISTER)")
                ok2, msg2 = delete_extract_full(dep, ext_name, unregister=True)
                if ok2:
                    pri_ext_delete_results.append({"dep": dep, "ext_name": ext_name, "action": "full-delete (PRM empty)", "tables_removed": removed_str, "status": "deleted"})
                else:
                    pri_ext_delete_results.append({"dep": dep, "ext_name": ext_name, "action": "full-delete attempted", "tables_removed": removed_str, "status": f"ERROR ({msg2})"})
            else:
                pri_ext_delete_results.append({"dep": dep, "ext_name": ext_name, "action": f"partial-delete ({len(removed)} of {len(removed) + remaining})", "tables_removed": removed_str, "status": "running" if removed else "unchanged"})

    # ─── Tables that were never found in ANY process ──────────────────────────────────────
    not_found = [t for t in config_tables if t not in tables_seen]
    if not_found:
        print_log("warning", f"the following requested tables were NOT found in any OGG process — nothing to delete: {', '.join(not_found)}")

    # ─── Final Summary ────────────────────────────────────────────────────────────────────
    _print_delete_summary(il_rep_delete_results, il_ext_delete_results, pri_rep_delete_results, pri_ext_delete_results, _W)
    print_log("info", f"end {mode_label}")
    return 0


def _print_delete_summary(il_rep_results, il_ext_results, pri_rep_results, pri_ext_results, _W):
    # Prints the four Final Summary tables produced by _apply_delete_request:
    #   1) Primary Extract delete
    #   2) Primary Replicat delete
    #   3) IL Extracts delete
    #   4) IL Replicats delete
    # Each table is only emitted if it has at least one row. Column widths are computed
    # globally across the 4 tables for the shared columns (PROCESS, STATUS) so the blocks
    # line up vertically when printed one after another.
    _MIN_PROCESS = 8
    _MIN_TYPE    = 24
    _MIN_TABLE   = 18
    _MIN_ACTION  = 30
    _MIN_STATUS  = 24

    all_rows = (
        [(r["ext_name"], r.get("status", "")) for r in pri_ext_results] +
        [(r["ext_name"], r.get("status", "")) for r in pri_rep_results] +
        [(r["ext_name"], r.get("status", "")) for r in il_ext_results] +
        [(r["ext_name"], r.get("status", "")) for r in il_rep_results]
    )
    _P_glob = max(_MIN_PROCESS, len("PROCESS"), max((len(p) for p, _ in all_rows), default=0))
    _S_glob = max(_MIN_STATUS,  len("STATUS"),  max((len(s) for _, s in all_rows), default=0))

    def _render(title, rows, type_label, tables_key):
        if not rows:
            return
        _T = max(_MIN_TYPE,   len("TYPE"),           len(type_label))
        _B = max(_MIN_TABLE,  len("TABLES_REMOVED"), max((len(str(r.get(tables_key, "-"))) for r in rows), default=0))
        _A = max(_MIN_ACTION, len("ACTION"),         max((len(r.get("action", "-"))         for r in rows), default=0))
        deps = sorted({r["dep"] for r in rows if r.get("dep")})
        print_log("info", "")
        print_log("info", f"====== Final Summary: DELETE — {title} ======")
        print_log("info", f"OGG Deployment : {', '.join(deps) if deps else '(unknown)'}")
        sep = f"{'-'*_P_glob}  {'-'*_T}  {'-'*_A}  {'-'*_B}  {'-'*_S_glob}"
        print_log("info", sep)
        print_log("info", f"{'PROCESS':<{_P_glob}}  {'TYPE':<{_T}}  {'ACTION':<{_A}}  {'TABLES_REMOVED':<{_B}}  {'STATUS':<{_S_glob}}")
        print_log("info", sep)
        for r in rows:
            print_log("info", f"{r['ext_name']:<{_P_glob}}  {type_label:<{_T}}  {r.get('action', '-'):<{_A}}  {str(r.get(tables_key, '-')):<{_B}}  {r.get('status', '-'):<{_S_glob}}")
        print_log("info", sep)

    _render("Primary Extract",  pri_ext_results, "Extract (integrated)",   "tables_removed")
    _render("Primary Replicat", pri_rep_results, "Replicat (classic)",     "tables_removed")
    _render("initial_load Extracts",  il_ext_results, "Extract (initial_load)",  "table")
    _render("initial_load Replicats", il_rep_results, "Replicat (initial_load)", "table")


def _get_ogg_rate_delta(oggctl_path, deployment, process_name, process_type="replicat"):
    # Returns (ok: bool, rate: int|None, delta: int|None) by reading the last OGG-08549
    # line from the process report via oggctl CLI subprocess.
    # process_type: "replicat" or "extract"
    try:
        r = subprocess.run(
            [sys.executable, oggctl_path, process_type, "report", process_name,
             "--deployment", deployment],
            capture_output=True, text=True, timeout=30,
        )
        if r.returncode != 0:
            return False, None, None
        output = r.stdout or ""
    except Exception:
        return False, None, None
    _pat = re.compile(r'OGG-08549.*\(rate ([\d,]+),\s*delta ([\d,]+)\)', re.IGNORECASE)
    last = None
    for line in output.splitlines():
        m = _pat.search(line)
        if m:
            last = m
    if not last:
        return False, None, None
    try:
        return True, int(last.group(1).replace(",", "")), int(last.group(2).replace(",", ""))
    except (ValueError, TypeError):
        return False, None, None


def _run_il_replicats_step(
    *,
    tables_for_il, tables_size_all, replicat_deps_il,
    il_results, il_replicat_results,
    bbdd_name, env, cdc_seq, rep_target, source_pdb,
    project, kafka_topic_prefix, audit_comment, config_table_columns,
    oggctl_path, dry_run, _W,
    plan_replicat_namer=None,
    s3_target_conn=None,
    kafka_target_name=None,
    s3_bucket_path="",
):
    # IL Replicats step shared by FULL (Step 5/5) and INITIAL_LOAD-only legacy (Step 2/2).
    # Creates and starts one coordinated IL replicat per IL extract trail, then waits for
    # all replicats to consume their trail (STATS-driven auto-stop at 100%, post-finish
    # bookkeeping). Caller must print the 'Step N/M' header BEFORE calling.
    #
    # In dry_run the function synthesises the plan only and populates 'il_replicat_results';
    # the caller continues to its Final Summary normally.
    #
    # 'plan_replicat_namer' is an optional callable(dep, prefer_nn=...) -> name used in
    # plan mode to simulate NN allocation across iterations (the FULL flow has one; the
    # legacy flow passes None and a single 'generate_replicat_name' call is used).

    # ── Plan-mode synthesis ───────────────────────────────────────────────────────────────
    if dry_run:
        if not tables_for_il:
            print_log("info", "(plan) no IL extracts would be created; IL replicats step would be skipped")
            return
        if not replicat_deps_il:
            print_log("warning", "(plan) no initial_load Replicat deployment (_R<digits>_) found; would skip IL replicats")
            return
        planned = []
        for rep_dep in replicat_deps_il:
            for full, _gb, parallel_count in tables_size_all:
                if full not in tables_for_il or "." not in full:
                    continue
                schema, table  = full.split(".", 1)
                parallel_count = max(1, int(parallel_count) if str(parallel_count).isdigit() else 1)
                for part_idx in range(parallel_count):
                    if parallel_count == 1:
                        trail      = f"initial/{bbdd_name}/{schema}/{table}/il"
                        part_label = ""
                    else:
                        trail      = f"initial/{bbdd_name}/{schema}/{table}/P{part_idx + 1}/il"
                        part_label = f" part {part_idx + 1}/{parallel_count}"
                    planned.append((rep_dep, f"{full}{part_label}", trail))
        print_log("info", f"(plan) would launch {len(planned)} IL replicat(s) on deployment(s) {sorted({p[0] for p in planned})}")
        for rep_dep, full_part, trail in planned:
            # Best-effort NN sync: find the IL extract synthesised on this trail.
            prefer_nn = None
            for r in il_results:
                if r.get("trail") == trail and r.get("process") and len(r["process"]) >= 4:
                    try:
                        prefer_nn = _b36_to_nn(r["process"][2:4])
                    except (ValueError, TypeError):
                        prefer_nn = None
                    break
            if plan_replicat_namer is not None:
                planned_name = plan_replicat_namer(rep_dep, prefer_nn=prefer_nn)
            else:
                name, _msg   = generate_replicat_name(rep_dep, env, cdc_seq, role="initial_load", prefer_nn=prefer_nn)
                planned_name = name or "(plan)"
            print_log("info", f"(plan)   + {planned_name} for {full_part}@{rep_dep}  [trail: {trail}]")
            il_replicat_results.append({
                "dep": rep_dep, "ext_name": planned_name, "table": full_part,
                "action": "(plan) would create + start", "trail": trail,
                "elapsed": "-", "rows": "-",
            })
        return

    # ── Real-run path ─────────────────────────────────────────────────────────────────────
    # If ANY partition of a table failed its extract, we must skip ALL replicats for that
    # table — replicating an incomplete dataset is worse than replicating nothing.
    _failed_base_tables = {
        r["table"].split(" part ")[0]
        for r in il_results
        if not r.get("is_ok")
    }

    all_ok_extract_jobs = [r for r in il_results if r.get("is_ok") and r.get("trail")]

    # Warn for partially-failed tables (some parts OK, some failed)
    _warned_skip = set()
    for j in all_ok_extract_jobs:
        base = j["table"].split(" part ")[0]
        if base in _failed_base_tables and base not in _warned_skip:
            print_log("warning", f"{'initial_load replicat skip':<{_W}}: WARNING  '{base}' — one or more initial_load extract processes failed; replicat skipped")
            _warned_skip.add(base)

    # Warn for entirely-failed tables (no OK parts at all — not in all_ok_extract_jobs)
    _ok_base_tables = {j["table"].split(" part ")[0] for j in all_ok_extract_jobs}
    for base in sorted(_failed_base_tables - _ok_base_tables):
        print_log("warning", f"{'initial_load replicat skip':<{_W}}: WARNING  '{base}' — one or more initial_load extract processes failed; replicat skipped")

    # Record skipped entries in il_replicat_results so they appear in the Final Summary
    for r in il_results:
        base = r["table"].split(" part ")[0]
        if base in _failed_base_tables:
            for rep_dep in (replicat_deps_il if replicat_deps_il else ["-"]):
                il_replicat_results.append({
                    "dep": rep_dep, "ext_name": "-", "table": r["table"],
                    "action": "skipped (extract failed)", "elapsed": "-", "rows": "-",
                })

    # Only proceed with tables where ALL parts extracted successfully
    ok_il_extract_jobs = [j for j in all_ok_extract_jobs if j["table"].split(" part ")[0] not in _failed_base_tables]

    if not ok_il_extract_jobs:
        print_log("info", "no IL extract completed successfully for any table; IL replicat step is skipped")
        return
    if not replicat_deps_il:
        print_log("warning", "no initial_load Replicat deployment (_R<digits>_) found; skipping IL replicats")
        return

    # Cross-product: one IL replicat per (replicat deployment × IL extract trail).
    il_rep_pending = []
    for rep_dep in replicat_deps_il:
        for j in ok_il_extract_jobs:
            il_rep_pending.append({
                "dep":         rep_dep,
                "table":       j["table"],
                "trail":       j["trail"],
                "source_full": j["table"].split(" part ")[0],
            })

    # ── Pool configuration: read initial_load_parallel from target connection ──────────
    if rep_target == "s3":
        _raw_max = (s3_target_conn or {}).get("initial_load_parallel")
    elif rep_target == "kafka_confluent" and kafka_target_name:
        _raw_max = (get_target_connection(kafka_target_name) or {}).get("initial_load_parallel")
    else:
        _raw_max = None
    try:
        il_max_parallel = int(_raw_max) if _raw_max is not None else 0
    except (ValueError, TypeError):
        il_max_parallel = 0
    total_rep_jobs = len(il_rep_pending)
    if il_max_parallel < 1:
        il_max_parallel = total_rep_jobs or 1
        print_log("warning", f"{'initial_load_parallel':<{_W}}: not defined/invalid in target_connections.yaml — defaulting to {il_max_parallel} (no limit)")
    else:
        print_log("info", f"{'initial_load_parallel':<{_W}}: {il_max_parallel}  (total_processes={total_rep_jobs})")
    _ilr_concurrent = min(il_max_parallel, total_rep_jobs)
    print_log("info", f"{'initial_load replicat parallel':<{_W}}: {total_rep_jobs} job(s) to launch — up to {_ilr_concurrent} will run concurrently (max_parallel={il_max_parallel})")

    trail_to_total_rows = {}
    for r in il_results:
        if r.get("is_ok") and r.get("trail") and isinstance(r.get("rows"), str) and r["rows"].isdigit():
            trail_to_total_rows[r["trail"]] = int(r["rows"])

    # ── Unified pool: fill slots on-demand → monitor STATS → drain ───────────────────────
    # Progress cadence: first 30 min → every 2 min; after 30 min → every 10 min
    pending           = list(il_rep_pending)
    running           = []   # jobs currently started (have ext_name, start_time)
    _prev_running_len = 0    # used to detect when new slots are filled
    wait_start_at   = time.time()
    next_check_at   = wait_start_at + 30
    last_log_at     = wait_start_at
    _CHECK_INTERVAL = 30

    while pending or running:
        # Fill pool up to max_parallel
        while pending and len(running) < il_max_parallel:
            j = pending.pop(0)
            owner_table = j["source_full"]
            if "." not in owner_table:
                print_log("error", f"unexpected table format '{owner_table}'; skipping IL replicat")
                continue
            schema, _table = owner_table.split(".", 1)

            # Best-effort NN sync with the producing IL extract.
            prefer_nn = None
            for r in il_results:
                if r.get("trail") == j.get("trail") and r.get("process") and len(r["process"]) >= 4:
                    try:
                        prefer_nn = _b36_to_nn(r["process"][2:4])
                    except (ValueError, TypeError):
                        prefer_nn = None
                    break

            il_rep_name, msg = generate_replicat_name(j["dep"], env, cdc_seq, role="initial_load", prefer_nn=prefer_nn)
            if not il_rep_name:
                print_log("error", f"could not generate IL replicat name for '{j['dep']}': {msg}")
                il_replicat_results.append({"dep": j["dep"], "ext_name": "-", "table": j["table"], "action": "Error (NAME_FAILED)", "trail": j["trail"], "rows": "-"})
                continue
            if prefer_nn is not None and il_rep_name[2:4] != _nn_to_b36(prefer_nn):
                print_log("warning", f"IL extract↔replicat NN correspondence broken for trail '{j['trail']}': preferred NN={_nn_to_b36(prefer_nn)} but generated '{il_rep_name}' (preferred NN already in use on '{j['dep']}')")

            _tc_conn = s3_target_conn or {}
            _raw_maxthreads = _tc_conn.get("initial_load_maxthreads")
            try:
                _il_maxthreads = int(_raw_maxthreads) if _raw_maxthreads is not None else None
            except (ValueError, TypeError):
                _il_maxthreads = None
            _threadrange = f"1-{_il_maxthreads}" if _il_maxthreads else "1-4"

            il_rep_description = f"Replicat-Initial_load-{bbdd_name}-{il_rep_name[2:4]}-{project}-{env} for table {j['table']} (created by cdc-controller date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"
            il_rep_tables_payload = json.dumps([
                _build_replicat_table_entry(owner_table, audit_comment, config_table_columns, threadrange=_threadrange)
            ])

            cmd_create = [
                sys.executable, oggctl_path,
                "replicat", "create", il_rep_name,
                "--replicat-type",           "coordinated",
                "--target",                  rep_target,
                "--trail",                   j["trail"],
                "--tables-json",             il_rep_tables_payload,
                "--description",             il_rep_description,
                "--managed-process-profile", "Default",
                "--deployment",              j["dep"],
            ]
            if _il_maxthreads:
                cmd_create.extend(["--max-threads", str(_il_maxthreads)])
            if source_pdb:
                cmd_create.extend(["--pdb", source_pdb])
            if rep_target == "kafka_confluent":
                if kafka_target_name:
                    cmd_create.extend(["--target-name", kafka_target_name])
                cmd_create.extend(["--target-vars-json", json.dumps({
                    "database":           bbdd_name,
                    "schema":             schema,
                    "kafka_topic_prefix": kafka_topic_prefix or "",
                })])
            elif rep_target == "s3":
                _tc      = s3_target_conn or {}
                _s3_path = (s3_bucket_path or "").strip()
                cmd_create.extend(["--target-vars-json", json.dumps({
                    "database":             bbdd_name,
                    "s3_bucket_name":          (_tc.get("s3_bucket_name") or "").strip(),
                    "bucket_path_prefix":   f"{_s3_path}/{bbdd_name}" if _s3_path else bbdd_name,
                    "s3_sts_assume_role":   (_tc.get("sts_assume_role") or ""),
                    "s3_sts_session_name":  (_tc.get("sts_session_name") or ""),
                    "s3_credentials_alias": (_tc.get("ogg_alias_credential") or ""),
                    "aws_region":           (_tc.get("aws_region") or "eu-west-1"),
                })])

            _il_rep_label_name = f"{il_rep_name} table {j['table']}"

            ok, _ = _run_oggctl(cmd_create, "initial_load replicat create", _il_rep_label_name, j["dep"], dry_run, _W)
            if not ok:
                il_replicat_results.append({"dep": j["dep"], "ext_name": il_rep_name, "table": j["table"], "action": "Error (CREATE_FAILED)", "trail": j["trail"], "rows": "-"})
                continue

            ok, _ = _run_oggctl(
                [sys.executable, oggctl_path, "replicat", "start", il_rep_name, "--deployment", j["dep"]],
                "initial_load replicat start", _il_rep_label_name, j["dep"], dry_run, _W,
            )
            if not ok:
                il_replicat_results.append({"dep": j["dep"], "ext_name": il_rep_name, "table": j["table"], "action": "Error (START_FAILED)", "trail": j["trail"], "rows": "-"})
                continue

            j["ext_name"]   = il_rep_name
            j["start_time"] = time.time()
            il_replicat_results.append({"dep": j["dep"], "ext_name": il_rep_name, "table": j["table"], "action": "create (started)", "trail": j["trail"], "rows": "?"})
            running.append(j)

        if not running:
            break

        # Print pool status only when new slots were just filled (state changed)
        if len(running) != _prev_running_len:
            print_log("info", f"{'initial_load replicat pool':<{_W}}: {len(running)} running, {len(pending)} pending of {total_rep_jobs} total (initial_load_parallel={il_max_parallel}) — waiting for completion ...")
            _prev_running_len = len(running)

        time.sleep(5)
        finished_reps = []
        for j in running:
            ok_st, info = get_replicat_info_now(j["dep"], j["ext_name"])
            s = str(info.get("status", "")).upper() if ok_st else ""
            if (not ok_st) or s not in ("RUNNING", "STARTING"):
                finished_reps.append((j, s if ok_st else "unknown"))

        now = time.time()
        if now >= next_check_at:
            _rep_elapsed      = now - wait_start_at
            _rep_log_interval = 120 if _rep_elapsed <= 1800 else 600
            should_log        = (now - last_log_at) >= _rep_log_interval
            next_check_at     = now + _CHECK_INTERVAL
            finished_set      = {id(fj) for fj, _ in finished_reps}
            for j in running:
                if id(j) in finished_set:
                    continue
                ok_p, applied, _pmsg = get_il_replicat_rows_applied(j["dep"], j["ext_name"], j["source_full"])
                total = trail_to_total_rows.get(j["trail"])
                if ok_p and applied is not None and total:
                    pct    = (applied * 100.0 / total) if total > 0 else 0.0
                    detail = f"{applied}/{total} replicated, {pct:.1f}% completed (status=RUNNING)"
                elif ok_p and applied is not None:
                    detail = f"{applied} rows replicated (total unknown) (status=RUNNING)"
                else:
                    detail = "STATS not available yet (status=RUNNING)"
                if should_log:
                    _ok_rd, _rate, _delta = _get_ogg_rate_delta(oggctl_path, j["dep"], j["ext_name"], "replicat")
                    _rate_detail = f", rate {_rate}, delta {_delta}" if _ok_rd else ""
                    if ok_p and applied is not None and total:
                        detail = f"{applied}/{total} replicated{_rate_detail}, {pct:.1f}% completed (status=RUNNING)"
                    elif ok_p and applied is not None:
                        detail = f"{applied} rows replicated{_rate_detail} (total unknown) (status=RUNNING)"
                    print_log("info", f"{'initial_load replicat progress':<{_W}}: {j['ext_name']} for '{j['table']}', {detail}")

                # Auto-stop at 100% — IL replicats are coordinated, OGG never stops them
                # on its own; we issue STOP when STATS confirms applied >= total AND the
                # report is clean. The 'critical' flag was already cleared at create time.
                if ok_p and applied is not None and total and applied >= total and not j.get("stop_issued"):
                    rpt_ok, has_err, _err_lines, _ = check_replicat_report_for_errors(j["dep"], j["ext_name"])
                    if rpt_ok and not has_err:
                        j["final_rows"] = applied
                        _pct_100 = (applied * 100.0 / total) if total > 0 else 100.0
                        print_log("info", f"{'initial_load replicat progress':<{_W}}: {j['ext_name']} for '{j['table']}', {applied}/{total} replicated, {_pct_100:.1f}% completed (status=RUNNING)")
                        _run_oggctl(
                            [sys.executable, oggctl_path, "replicat", "stop", j["ext_name"], "--deployment", j["dep"]],
                            "initial_load replicat stop", f"{j['ext_name']} table {j['source_full']}", j["dep"], False, _W,
                        )
                        j["stop_issued"] = True
            if should_log:
                print_log("info", f"{'initial_load replicat pool':<{_W}}: {len(running)} running, {len(pending)} pending of {total_rep_jobs} total (initial_load_parallel={il_max_parallel}) — waiting for completion ...")
                last_log_at = now

        if finished_reps:
            for j, final_status in finished_reps:
                running.remove(j)
                _prev_running_len = len(running)
                rpt_ok, has_err, err_lines, _ = check_replicat_report_for_errors(j["dep"], j["ext_name"])
                elapsed_s = time.time() - j.get("start_time", time.time())
                h, rem = divmod(int(elapsed_s), 3600)
                m, sec = divmod(rem, 60)
                elapsed = f"{h}:{m:02d}:{sec:02d}"
                # Priority 1: final_rows captured before STOP. Priority 2: post-mortem STATS.
                if j.get("final_rows") is not None:
                    rows_field = str(j["final_rows"])
                else:
                    ok_pf, applied_final, _ = get_il_replicat_rows_applied(j["dep"], j["ext_name"], j["source_full"])
                    rows_field = str(applied_final) if (ok_pf and applied_final is not None) else "?"
                _ilr_label = "initial_load replicat finished"
                total_for_log = trail_to_total_rows.get(j.get("trail"))
                if rows_field not in ("?", "-") and total_for_log:
                    try:
                        _pct = (int(rows_field) * 100.0 / int(total_for_log)) if int(total_for_log) > 0 else 0.0
                    except (ValueError, TypeError):
                        _pct = 0.0
                    rows_label = f", {rows_field}/{total_for_log} replicated, {_pct:.1f}% completed"
                elif rows_field not in ("?", "-"):
                    rows_label = f", {rows_field} replicated"
                else:
                    rows_label = ""
                _is_abended = str(final_status).upper() == "ABENDED"
                if has_err or _is_abended:
                    print_log("error", f"{_ilr_label:<{_W - 1}}: ERROR  {j['ext_name']} for '{j['table']}' finished with errors{rows_label} (status={final_status}) [elapsed: {elapsed}]")
                    for line in err_lines[:5]:
                        print_log("error", f"  - {line}")
                    for r in il_replicat_results:
                        if r["ext_name"] == j["ext_name"] and r["dep"] == j["dep"]:
                            r["action"]     = f"Error ({final_status})"
                            r["elapsed"]    = elapsed
                            r["rows"]       = rows_field
                            r["total_rows"] = total_for_log
                else:
                    print_log("info", f"{_ilr_label:<{_W}}: OK  {j['ext_name']} for '{j['table']}'{rows_label} (status={final_status}) [elapsed: {elapsed}]")
                    for r in il_replicat_results:
                        if r["ext_name"] == j["ext_name"] and r["dep"] == j["dep"]:
                            r["action"]     = f"OK ({final_status}, no errors)"
                            r["elapsed"]    = elapsed
                            r["rows"]       = rows_field
                            r["total_rows"] = total_for_log


def _apply_create_full(config, dry_run=False):
    # CREATE flow specific to replication_type=FULL.
    # The order of steps differs from the ONLINE flow because FULL has to instantiate the
    # initial_load extracts at the SCN at which the integrated extract started processing
    # redo — otherwise data loss / duplication between the bulk load and the CDC stream is
    # possible. That SCN is read by parsing the LAST OGG-02845 line in the extract's report.
    #
    # Order:
    #   1) PRIMARY REPLICAT  : reuse + add MAPs / create + start (restart on PRM update)
    #   2) PRIMARY EXTRACT   : reuse + add TABLEs / create + register + start
    #   3) CONSISTENCY SCN   : parse last OGG-02845 line in <extract>.rpt to extract SCN
    #   4) IL EXTRACT(S)     : one per NEWLY-ADDED table, instantiated at that SCN; wait
    #   5) IL REPLICAT(S)    : one classic replicat per IL extract trail; wait until they finish
    #
    # "Newly-added tables" = JSON tables that the primary extract did NOT already capture.
    # For brand-new extracts that's the full JSON list. For reused extracts it's only the
    # subset that add_tables_to_extract actually appended (parsed from its return message).
    mode_label = "plan" if dry_run else "apply"
    _W = 38

    _inv_defaults       = get_inventory_defaults()
    _purge_days_primary = str(_inv_defaults.get("purge_retention_primary", "30"))
    _purge_days_initial = str(_inv_defaults.get("purge_retention_initial", "10"))

    bbdd_engine = (config.get("bbdd_engine") or "").upper()
    rep_type    = config.get("replication_type", "FULL")
    env         = config["env"]
    bbdd_name   = config["bbdd_name"]

    print_log("info", f"====== CREATE flow for replication_type=FULL (engine={bbdd_engine}, bbdd={bbdd_name}, env={env}) ======")

    # ─── Deployments split by load_type ──────────────────────────────────────────────────
    online_deps   = get_matching_deployments(env, bbdd_engine, load_types=["online"])
    il_deps_all   = get_matching_deployments(env, bbdd_engine, load_types=["initial_load"])
    extract_deps_online  = [n for n, _ in online_deps if re.search(r"_E\d+_", n)]
    replicat_deps_online = [n for n, _ in online_deps if re.search(r"_R\d+_", n)]
    extract_deps_il      = [n for n, _ in il_deps_all if re.search(r"_E\d+_", n)]
    replicat_deps_il     = [n for n, _ in il_deps_all if re.search(r"_R\d+_", n)]

    if not extract_deps_online:
        print_log("error", f"no Extract deployment (_E<digits>_) found for env={env}, engine={bbdd_engine} with load_type=online")
        return 1
    if not replicat_deps_online:
        print_log("error", f"no Replicat deployment (_R<digits>_) found for env={env}, engine={bbdd_engine} with load_type=online")
        return 1

    # ─── Minimum Requirements pre-flight ────────────────────────────────────────────────
    print_log("info", "")
    print_log("info", "====== MINIMUM REQUIREMENTS ======")

    # ─── Managed-process profile pre-flight ──────────────────────────────────────────────
    _MPP_NAME = "Critical"
    deps_for_mpp = sorted(set(extract_deps_online + replicat_deps_online + extract_deps_il + replicat_deps_il))
    if deps_for_mpp:
        print_log("info", f"verifying managed-process-profile '{_MPP_NAME}' exists on {len(deps_for_mpp)} deployment(s) ...")
        for _dep in deps_for_mpp:
            mp_ok, mp_exists, mp_msg = managed_process_profile_exists(_dep, _MPP_NAME)
            if (not mp_ok) or (not mp_exists):
                detail = mp_msg if not mp_ok else f"profile '{_MPP_NAME}' not found"
                print_log("error", f"{'managed-process profile check':<{_W - 1}}: ERROR  ({_dep}: {detail})")
                print_log("error", f"aborting {mode_label}: managed-process-profile '{_MPP_NAME}' missing on '{_dep}'")
                return 1
            print_log("info", f"{'managed-process profile check':<{_W}}: OK  ({_dep}: profile '{_MPP_NAME}' exists)")

    # ─── Common BBDD info ────────────────────────────────────────────────────────────────
    bbdd_conn = get_db_connection(f"{bbdd_name}_{env}")
    if not bbdd_conn:
        print_log("error", f"connection '{bbdd_name}_{env}' not found in db_connections.yaml")
        return 1
    cdc_seq    = bbdd_conn.get("cdc_seq", "")
    source_pdb = bbdd_conn.get("pdb", "")
    if not cdc_seq:
        print_log("error", f"cdc_seq not defined in db_connections.yaml for '{bbdd_name}_{env}'")
        return 1
    useridalias_prim = bbdd_conn.get("conn_name_prim") or f"{bbdd_name}_PRIM_{env}"
    useridalias_stby = bbdd_conn.get("conn_name_stby") or f"{bbdd_name}_STBY_{env}"
    il_useridalias   = useridalias_stby if bbdd_conn.get("conn_string_stby") else useridalias_prim

    oggctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl", "oggctl.py")

    # ─── Credential alias pre-flight ─────────────────────────────────────────────────────
    _all_extract_deps = sorted(set(extract_deps_online + extract_deps_il))
    _userid_prim_pf   = f"{bbdd_conn.get('db_user', '')}@{bbdd_conn.get('conn_string_prim', '')}"
    _userid_stby_pf   = f"{bbdd_conn.get('db_user', '')}@{bbdd_conn.get('conn_string_stby', '')}"
    for _alias, _userid_pf, _required in [
        (useridalias_prim, _userid_prim_pf, bool(bbdd_conn.get("conn_string_prim"))),
        (useridalias_stby, _userid_stby_pf, bool(bbdd_conn.get("conn_string_stby"))),
    ]:
        if not _required:
            continue
        print_log("info", f"verifying credential alias '{_alias}' on {len(_all_extract_deps)} deployment(s) ...")
        for _dep in _all_extract_deps:
            _ok = _ensure_credential_alias(oggctl_path, _alias, _userid_pf,
                                           bbdd_conn.get("password_credential", ""), _dep, dry_run, _W)
            if not _ok:
                print_log("error", f"aborting {mode_label}: credential alias '{_alias}' failed on '{_dep}'")
                return 1

    target_engine_map = {"KAFKA": "kafka_confluent", "AWS-S3": "s3", "ORACLE": "oracle"}
    target_engine_in  = (config.get("target_engine") or "").upper()
    rep_target        = target_engine_map.get(target_engine_in)
    if not rep_target:
        print_log("error", f"unknown target_engine '{config.get('target_engine')}' (valid: KAFKA, AWS-S3, ORACLE)")
        return 1

    # ─── Kafka / S3 target pre-flight ────────────────────────────────────────────────────────
    _kafka_target_name = None
    if rep_target == "kafka_confluent":
        _kafka_tc = get_target_connection_by_cluster_name(config.get("kafka_cluster_name", ""), env, identity_pool=config.get("kafka_identity_pool", ""))
        if not _kafka_tc:
            print_log("error", f"Kafka target not found for cluster_name='{config.get('kafka_cluster_name')}', identity_pool='{config.get('kafka_identity_pool')}', env='{env}' in target_connections.yaml")
            return 1
        _kafka_target_name = _kafka_tc.get("_key", "")
        _kafka_rep_deps = sorted(set(replicat_deps_online + replicat_deps_il))
        print_log("info", f"verifying Kafka target prerequisites for '{_kafka_target_name}' on {len(_kafka_rep_deps)} deployment(s) ...")
        if not _ensure_kafka_target_prereqs(oggctl_path, _kafka_target_name, _kafka_tc, _kafka_rep_deps, dry_run, _W):
            print_log("error", f"aborting {mode_label}: Kafka target prerequisites failed for '{_kafka_target_name}'")
            return 1
    elif rep_target == "s3":
        _s3_tc_pre = get_target_connection_by_bucket(config.get("s3_bucket_name") or "") or {}
        _s3_rep_deps = sorted(set(replicat_deps_online + replicat_deps_il))
        print_log("info", f"verifying S3 target prerequisites on {len(_s3_rep_deps)} deployment(s) ...")
        if not _ensure_s3_target_prereqs(oggctl_path, _s3_tc_pre, _s3_rep_deps, dry_run, _W):
            print_log("error", f"aborting {mode_label}: S3 target prerequisites failed")
            return 1

    config_tables, config_table_columns = _parse_tables_from_config(config)
    if not config_tables:
        print_log("error", "no tables defined in JSON 'tables' field")
        return 1

    # Single audit comment (consistent timestamp + requester for every PRM line added by this run)
    audit_comment = "TAGS " + json.dumps({
        "requested_by":  config.get("user_name", ""),
        "cost_center":   config.get("cost_center", ""),
        "change_number": config.get("change_number", ""),
        "date":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }, separators=(",", ":"))

    # Result accumulators (used by the Final Summary at the end)
    primary_replicat_results = []
    primary_results          = []
    il_results               = []
    il_replicat_results      = []

    # ═══ Pre-step: initial_load table-size analysis ══════════════════════════════════════
    # Runs BEFORE any process is created/modified so the operator sees disk usage and the
    # derived parallel-bucket up-front and can abort with Ctrl-C if something looks wrong.
    # Also runs in dry-run (plan) — the query is read-only.
    # The results (tables_size_all) feed Step 4 (initial_load extracts) — filtered there to
    # the subset of tables actually newly added to the primary extract.
    print_log("info", "")
    print_log("info", "------ Pre-step: initial_load table-size analysis ------")
    ora = prepare_oracle_connection(bbdd_name, env)
    if ora is None:
        return 1
    _, _db_user_prim, _dsn_prim = ora["dsns_to_check"][0]
    _db_password = ora["db_password"]
    _db_pdb      = ora["db_pdb"]
    try:
        _conn = oracledb.connect(user=_db_user_prim, password=_db_password, dsn=_dsn_prim)
    except Exception as e:
        print_log("error", f"Oracle Database (primary) connection failed: {e}")
        return 1
    _cursor = _conn.cursor()
    try:
        _cursor.execute("SELECT NAME, CDB FROM v$database")
        _name, _cdb_flag = _cursor.fetchone()
        if _cdb_flag == "YES" and _db_pdb:
            print_log("info", f"multitenant database detected, switching session to PDB : {_db_pdb}")
            _cursor.execute(f"ALTER SESSION SET CONTAINER = {_db_pdb}")

        _binds, _placeholders = {}, []
        for i, t in enumerate(config_tables):
            if "." not in t:
                print_log("error", f"invalid table entry (expected SCHEMA.TABLE): {t}")
                return 1
            _own, _tab = t.split(".", 1)
            _binds[f"o{i}"] = _own
            _binds[f"n{i}"] = _tab
            _placeholders.append(f"(:o{i},:n{i})")
        # NUM_ROWS from dba_tables is the optimizer's row-count estimate (refreshed by
        # DBMS_STATS). It is NOT exact — tables that have never been analysed return 0 —
        # but it is essentially free to read and useful as a coarse comparison vs the
        # 'count(1) AS OF SCN' executed right before each IL extract starts.
        _sql = (
            "SELECT OWNER, TABLE_NAME, NVL(TABLE_SEGS_GB,0), NVL(LOB_SEGS_GB,0), "
            "       (NVL(TABLE_SEGS_GB,0)+NVL(LOB_SEGS_GB,0)) AS TOTAL_GB, "
            "       NVL(EST_ROWS,0) AS ESTIMATED_ROWS FROM ("
            "  SELECT t.OWNER, t.TABLE_NAME,"
            "    (SELECT ROUND(SUM(BYTES)/1024/1024/1024,1) FROM dba_segments s WHERE s.OWNER=t.OWNER AND s.SEGMENT_NAME=t.TABLE_NAME) AS TABLE_SEGS_GB,"
            "    (SELECT ROUND(SUM(sl.BYTES)/1024/1024/1024,1) FROM dba_segments sl, dba_lobs L WHERE L.OWNER=t.OWNER AND L.TABLE_NAME=t.TABLE_NAME AND sl.SEGMENT_NAME=L.SEGMENT_NAME) AS LOB_SEGS_GB,"
            "    t.NUM_ROWS AS EST_ROWS"
            "  FROM dba_tables t"
            f"  WHERE (t.OWNER, t.TABLE_NAME) IN ({','.join(_placeholders)})"
            "  GROUP BY t.OWNER, t.TABLE_NAME, t.NUM_ROWS"
            ") ORDER BY TOTAL_GB DESC"
        )
        _sizes = {}
        try:
            _cursor.execute(_sql, _binds)
            for _owner, _table, _table_gb, _lob_gb, _total, _est_rows in _cursor.fetchall():
                _sizes[f"{_owner}.{_table}"] = (float(_table_gb), float(_lob_gb), int(_est_rows or 0))
        except Exception as e:
            print_log("error", f"failed to query table sizes: {e}")
            return 1
    finally:
        try: _cursor.close()
        except Exception: pass
        try: _conn.close()
        except Exception: pass

    def _parallel_for(total_gb):
        if total_gb <=  25: return 1
        if total_gb <=  50: return 2
        if total_gb <= 150: return 4
        if total_gb <= 250: return 6
        if total_gb <= 650: return 10
        return 14

    # display order: tables found in DB (largest first per SQL ORDER BY), then any config tables missing in DB
    _display_order = list(_sizes.keys()) + [t for t in config_tables if t not in _sizes]
    tables_size_all = []   # list of (OWNER.TABLE, total_gb, parallel_count) for ALL config_tables
    _C1, _C2, _C3, _C4, _C6, _C5 = _W + 10, 13, 11, 9, 14, 11
    _sep_line = f"{'-'*_C1}  {'-'*_C2}  {'-'*_C3}  {'-'*_C4}  {'-'*_C6}  {'-'*_C5}"
    print_log("info", _sep_line)
    print_log("info", f"{'OWNER.TABLE_NAME':<{_C1}}  {'TABLE_SEGS_GB':>{_C2}}  {'LOB_SEGS_GB':>{_C3}}  {'TOTAL_GB':>{_C4}}  {'ESTIMATED_ROWS':>{_C6}}  {'IL_EXTRACTS':>{_C5}}")
    print_log("info", _sep_line)
    _total_tab_gb = 0.0
    _total_lob_gb = 0.0
    _total_rows   = 0
    for t in _display_order:
        _tab_gb, _lob_gb, _est_rows = _sizes.get(t, (0.0, 0.0, 0))
        _total_gb        = _tab_gb + _lob_gb
        _parallel        = _parallel_for(_total_gb)
        tables_size_all.append((t, _total_gb, _parallel))
        _total_tab_gb   += _tab_gb
        _total_lob_gb   += _lob_gb
        _total_rows     += _est_rows
        _level = "info" if t in _sizes else "warning"
        _suffix = "" if t in _sizes else "  (table not found)"
        print_log(_level, f"{t:<{_C1}}  {_tab_gb:>{_C2}.1f}  {_lob_gb:>{_C3}.1f}  {_total_gb:>{_C4}.1f}  {_est_rows:>{_C6}}  {_parallel:>{_C5}}{_suffix}")
    print_log("info", _sep_line)
    print_log("info", f"{'TOTAL':<{_C1}}  {_total_tab_gb:>{_C2}.1f}  {_total_lob_gb:>{_C3}.1f}  {_total_tab_gb + _total_lob_gb:>{_C4}.1f}  {_total_rows:>{_C6}}")
    print_log("info", "")

    # Drop tables that don't exist in the source DB from the replication set. Even though
    # validate.py already flags them, an operator can still run -apply on a JSON that
    # references a missing table — and continuing here would break Step 2 (the primary
    # extract would fail to add a non-existent table) and Step 4 (the IL extract would
    # abend trying to read it). We skip them and keep the rest of the set working.
    missing_tables = [t for t in config_tables if t not in _sizes]
    if missing_tables:
        print_log("warning", f"the following tables do NOT exist in the source DB and will be SKIPPED from the replication set: {', '.join(missing_tables)}")
        config_tables   = [t for t in config_tables if t in _sizes]
        tables_size_all = [(t, gb, pc) for (t, gb, pc) in tables_size_all if t in _sizes]
        if not config_tables:
            print_log("error", "no valid tables remaining after dropping missing tables; aborting apply")
            return 1

    # ─── Pre-step: identify primary extract per online deployment ────────────────────────
    # We need the extract name BEFORE creating the replicat (Step 1) because the replicat
    # trail path embeds the extract name. For deployments where no extract exists yet, we
    # pre-generate the name now and reuse it later in Step 2 (so name and trail stay in sync).
    existing_extracts = get_extracts_for_bbdd(extract_deps_online, bbdd_name, env)
    primary_plan      = []   # list of {dep, name, mode: 'reuse'|'create'}
    for dep_name in extract_deps_online:
        found = existing_extracts.get(dep_name, [])
        if found:
            primary_plan.append({"dep": dep_name, "name": found[0], "mode": "reuse"})
        else:
            new_name, msg = generate_extract_name(dep_name, env, cdc_seq, role="primary")
            if not new_name:
                print_log("error", f"could not generate extract name for '{dep_name}': {msg}")
                return 1
            primary_plan.append({"dep": dep_name, "name": new_name, "mode": "create"})

    # All online extracts on the same BBDD share the same trail name from the replicat side
    # (replicat doesn't care which deployment hosts the extract, only the trail path). We pick
    # the first plan entry's name to build the trail. If multiple extracts exist for the same
    # BBDD, this is ambiguous — warn the user; in practice there is exactly one per BBDD.
    trail_ext_name = primary_plan[0]["name"]
    rep_trail_path = f"primary/{bbdd_name}/{trail_ext_name}/pr"
    if len(primary_plan) > 1:
        print_log("warning", f"multiple primary extract deployments; using trail from '{trail_ext_name}' for replicat trail matching")

    # ═══ Step 1/5: PRIMARY REPLICAT ══════════════════════════════════════════════════════
    print_log("info", "")
    print_log("info", f"------ Step 1/5: PRIMARY REPLICAT (trail={rep_trail_path}) ------")
    rep_existing = get_replicats_for_trail(replicat_deps_online, rep_trail_path)

    # target-vars-json (kafka / s3 need it; oracle target uses --useridalias instead)
    schemas_in_config = sorted({t.split(".")[0].upper() for t in config_tables if "." in t})
    if len(schemas_in_config) > 1:
        print_log("warning", f"multiple schemas in configured tables ({', '.join(schemas_in_config)}); using '{schemas_in_config[0]}' for replicat target-vars")
    rep_schema = schemas_in_config[0] if schemas_in_config else ""
    rep_target_vars_json = None
    if rep_target == "kafka_confluent":
        rep_target_vars_json = json.dumps({
            "database":           bbdd_name,
            "schema":             rep_schema,
            "kafka_topic_prefix": config.get("kafka_topic_prefix", ""),
        })
    elif rep_target == "s3":
        _s3_tc   = get_target_connection_by_bucket(config.get("s3_bucket_name") or "") or {}
        _s3_path = (config.get("s3_bucket_path") or "").strip()
        rep_target_vars_json = json.dumps({
            "database":             bbdd_name,
            "s3_bucket_name":          (config.get("s3_bucket_name") or "").strip(),
            "bucket_path_prefix":   f"{_s3_path}/{bbdd_name}" if _s3_path else bbdd_name,
            "s3_sts_assume_role":   (_s3_tc.get("sts_assume_role") or ""),
            "s3_sts_session_name":  (_s3_tc.get("sts_session_name") or ""),
            "s3_credentials_alias": (_s3_tc.get("ogg_alias_credential") or ""),
            "aws_region":           (_s3_tc.get("aws_region") or "eu-west-1"),
        })
    # Primary replicat MAP payload: include a 'columns' allow-list per table when the JSON
    # input asked for one (-> COLMAP clause in the PRM). Extracts always capture every
    # column regardless of the JSON column restriction.
    rep_tables_payload = json.dumps([_build_replicat_table_entry(t, audit_comment, config_table_columns) for t in config_tables])

    for rep_dep in replicat_deps_online:
        matched = rep_existing.get(rep_dep, [])
        # Only reuse a replicat that matches the requested target type. A trail can have multiple
        # replicats reading from it (e.g. one Kafka and one S3); reusing one with the wrong target
        # would silently add tables to the wrong destination.
        same_target = [r for r in matched
                       if _replicat_target_matches(*get_replicat_target_info(rep_dep, r), rep_target, config)]
        if same_target:
            chosen = same_target[0]
            if len(same_target) > 1:
                print_log("warning", f"multiple replicats with target '{rep_target}' consume trail '{rep_trail_path}' on '{rep_dep}': {same_target} — picking '{chosen}'")
            other_target = [r for r in matched if r not in same_target]
            if other_target:
                print_log("info", f"skipping {len(other_target)} replicat(s) with different target type on trail '{rep_trail_path}': {other_target}")
            print_log("info", f"Reusing existing Replicat '{chosen}' on '{rep_dep}' (matched by source trail + target '{rep_target}')")
            if dry_run:
                print_log("info", f"(plan) would check / add missing MAPs on existing replicat '{chosen}' @ '{rep_dep}'")
                primary_replicat_results.append({"dep": rep_dep, "ext_name": chosen, "action": "reuse"})
                continue
            ok_add, msg_add = add_tables_to_replicat(rep_dep, chosen, config_tables, comment=audit_comment, column_map=config_table_columns)
            print_log("info" if ok_add else "error", f"{'replicat add-tables':<{_W - (0 if ok_add else 1)}}: {'OK' if ok_add else 'ERROR':<1}  ({chosen}@{rep_dep}: {msg_add})")
            if not ok_add:
                primary_replicat_results.append({"dep": rep_dep, "ext_name": chosen, "action": "reuse (add-tables failed)"})
                continue
            if "no new tables" in msg_add:
                primary_replicat_results.append({"dep": rep_dep, "ext_name": chosen, "action": "reuse"})
            else:
                report_ok = _verify_replicat_report(rep_dep, chosen, "add-tables (stop+restart)", dry_run)
                action    = "reuse (add tables)" if report_ok else "reuse (add tables, errors in report)"
                if not dry_run:
                    ok_st, status_st = wait_for_replicat_running(rep_dep, chosen, max_wait=60, poll_interval=5)
                    print_log("info" if ok_st else "error", f"{'replicat status':<{_W - (0 if ok_st else 1)}}: {'RUNNING' if ok_st else status_st:<1}  ({chosen}@{rep_dep})")
                primary_replicat_results.append({"dep": rep_dep, "ext_name": chosen, "action": action})
        else:
            # No replicat matches the requested target type on this trail.
            # This covers two cases:
            #   (a) no replicats on the trail at all
            #   (b) replicats exist but all have a different target type → create a new one
            if matched:
                print_log("info",
                          f"found {len(matched)} replicat(s) on trail '{rep_trail_path}' on '{rep_dep}' "
                          f"but none match target '{rep_target}' — creating new replicat")
            new_rep_name, msg = generate_replicat_name(rep_dep, env, cdc_seq, role="primary")
            if not new_rep_name:
                print_log("error", f"could not generate replicat name for '{rep_dep}': {msg}")
                continue
            print_log("info", f"creating Replicat '{new_rep_name}' on '{rep_dep}' ...")
            _rep_target_label = "KAFKA" if rep_target == "kafka_confluent" else rep_target.upper()
            description = f"Replicat-Primary-{bbdd_name}-{new_rep_name[4:6]}-{_rep_target_label}-{env} (created by cdc-controller date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"
            cmd_create = [
                sys.executable, oggctl_path,
                "replicat", "create", new_rep_name,
                "--replicat-type",           "classic",
                "--target",                  rep_target,
                "--trail",                   rep_trail_path,
                "--tables-json",             rep_tables_payload,
                "--description",             description,
                "--managed-process-profile", "Critical",
                "--deployment",              rep_dep,
            ]
            if source_pdb:
                cmd_create.extend(["--pdb", source_pdb])
            if _kafka_target_name and rep_target == "kafka_confluent":
                cmd_create.extend(["--target-name", _kafka_target_name])
            if rep_target_vars_json:
                cmd_create.extend(["--target-vars-json", rep_target_vars_json])

            ok, _ = _run_oggctl(cmd_create, "replicat create", new_rep_name, rep_dep, dry_run, _W)
            if not ok:
                primary_replicat_results.append({"dep": rep_dep, "ext_name": new_rep_name, "action": "create failed"})
                continue
            ok, _ = _run_oggctl(
                [sys.executable, oggctl_path, "replicat", "start", new_rep_name, "--deployment", rep_dep],
                "replicat start", new_rep_name, rep_dep, dry_run, _W,
            )
            if not ok:
                primary_replicat_results.append({"dep": rep_dep, "ext_name": new_rep_name, "action": "start failed"})
                continue
            report_ok = _verify_replicat_report(rep_dep, new_rep_name, "replicat start", dry_run)
            action    = "create" if report_ok else "create (errors in report)"
            if not dry_run:
                ok_st, status_st = wait_for_replicat_running(rep_dep, new_rep_name, max_wait=60, poll_interval=5)
                print_log("info" if ok_st else "error", f"{'replicat status':<{_W - (0 if ok_st else 1)}}: {'RUNNING' if ok_st else status_st:<1}  ({new_rep_name}@{rep_dep})")
            primary_replicat_results.append({"dep": rep_dep, "ext_name": new_rep_name, "action": action})

    # ═══ Step 2/5: PRIMARY EXTRACT ═══════════════════════════════════════════════════════
    print_log("info", "")
    print_log("info", "------ Step 2/5: PRIMARY EXTRACT ------")

    newly_added_per_dep = {}   # dep_name -> [OWNER.TABLE that this run added to that extract]
    extract_started     = []   # list of (dep_name, ext_name) successfully started or already running

    # Primary extract TABLE payload: NEVER carries a 'columns' allow-list — extracts must
    # capture every column so the trail has the full row image. Column restriction belongs
    # to the replicat side (built via _build_replicat_table_entry above).
    extract_tables_payload = json.dumps([{"name": t, "comment": audit_comment} for t in config_tables])

    for plan in primary_plan:
        dep_name = plan["dep"]
        ext_name = plan["name"]

        # Pre-check: ensure the DB credential alias exists in this deployment.
        # If missing → WARNING + create via credential-alias create; block if creation fails.
        _userid_prim = f"{bbdd_conn.get('db_user', '')}@{bbdd_conn.get('conn_string_prim', '')}"
        ok_alias = _ensure_credential_alias(
            oggctl_path,
            alias=useridalias_prim,
            userid=_userid_prim,
            password_credential=bbdd_conn.get("password_credential", ""),
            dep_name=dep_name,
            dry_run=dry_run,
            width=_W,
        )
        if not ok_alias:
            primary_results.append({"dep": dep_name, "ext_name": ext_name, "action": "credential-alias failed"})
            continue

        if plan["mode"] == "reuse":
            print_log("info", f"Reusing existing Extract '{ext_name}' on '{dep_name}'")
            if dry_run:
                print_log("info", f"(plan) would add tables to '{ext_name}' @ '{dep_name}': {', '.join(config_tables)}")
                newly_added_per_dep[dep_name] = list(config_tables)   # worst-case: assume all are new
                primary_results.append({"dep": dep_name, "ext_name": ext_name, "action": "reuse (add tables)"})
                extract_started.append((dep_name, ext_name))
                continue
            ok_add, msg_add = add_tables_to_extract(dep_name, ext_name, config_tables, comment=audit_comment)
            print_log("info" if ok_add else "error", f"{'extract add-tables':<{_W - (0 if ok_add else 1)}}: {'OK' if ok_add else 'ERROR':<1}  ({ext_name}@{dep_name}: {msg_add})")
            if not ok_add:
                primary_results.append({"dep": dep_name, "ext_name": ext_name, "action": "reuse (add-tables failed)"})
                continue
            added_now = _parse_added_items_from_msg(msg_add)
            newly_added_per_dep[dep_name] = added_now
            if not added_now:
                primary_results.append({"dep": dep_name, "ext_name": ext_name, "action": "reuse"})
            else:
                report_ok = _verify_extract_report(dep_name, ext_name, "add-tables (stop+restart)", dry_run)
                action    = "reuse (add tables)" if report_ok else "reuse (add tables, errors in report)"
                primary_results.append({"dep": dep_name, "ext_name": ext_name, "action": action})
            extract_started.append((dep_name, ext_name))
        else:
            print_log("info", f"Creating new Extract '{ext_name}' on '{dep_name}' ...")
            description = f"Extract-Primary-{bbdd_name}-{ext_name[4:6]}-{env} (created by cdc-controller date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"
            trail_path  = f"primary/{bbdd_name}/{ext_name}/pr"
            ok, _ = _run_oggctl(
                [sys.executable, oggctl_path,
                 "extract", "create", ext_name,
                 "--extract-type",            "integrated",
                 "--trail",                   trail_path,
                 "--tables-json",             extract_tables_payload,
                 "--useridalias",             useridalias_prim,
                 "--pdb",                     source_pdb,
                 "--description",             description,
                 "--managed-process-profile", "Critical",
                 "--deployment",              dep_name],
                "extract create", ext_name, dep_name, dry_run, _W,
            )
            if not ok:
                primary_results.append({"dep": dep_name, "ext_name": ext_name, "action": "create failed"})
                continue
            ok, _ = _run_oggctl(
                [sys.executable, oggctl_path, "extract", "register", ext_name, "--deployment", dep_name],
                "extract register", ext_name, dep_name, dry_run, _W,
            )
            if not ok:
                primary_results.append({"dep": dep_name, "ext_name": ext_name, "action": "register failed"})
                continue
            ok, _ = _run_oggctl(
                [sys.executable, oggctl_path, "extract", "start", ext_name, "--deployment", dep_name],
                "extract start", ext_name, dep_name, dry_run, _W,
            )
            if not ok:
                primary_results.append({"dep": dep_name, "ext_name": ext_name, "action": "start failed"})
                continue
            report_ok = _verify_extract_report(dep_name, ext_name, "extract start", dry_run)
            action    = "create" if report_ok else "create (errors in report)"
            primary_results.append({"dep": dep_name, "ext_name": ext_name, "action": action})
            newly_added_per_dep[dep_name] = list(config_tables)   # all tables are new on a brand-new extract
            extract_started.append((dep_name, ext_name))
            _purge_task_name = f"Purge-trail-{ext_name}"
            if not dry_run and _purge_task_exists(oggctl_path, _purge_task_name, dep_name):
                print_log("warning", f"{'purge task check':<{_W}}: WARNING  ({_purge_task_name}@{dep_name}: already exists — check retention)")
            else:
                _run_oggctl(
                    [sys.executable, oggctl_path, "task", "purge-trail", "create", _purge_task_name,
                     "--trail", trail_path,
                     "--keep-value", _purge_days_primary, "--keep-units", "days",
                     "--schedule-value", "1", "--schedule-units", "days",
                     "--critical", "--use-checkpoints", "--enabled",
                     "--deployment", dep_name],
                    "purge task create", _purge_task_name, dep_name, dry_run, _W,
                )

    if not extract_started:
        print_log("error", f"no primary extract is running; aborting {mode_label}")
        _print_full_summary(primary_replicat_results, primary_results, il_results, il_replicat_results, _W)
        return 1

    # Gate: wait for the primary extracts to reach RUNNING before reading the consistency SCN.
    # The status check itself emits one OK/ERROR line per extract — no extra banner needed.
    if not dry_run:
        for dep_name, ext_name in extract_started:
            ok, status = wait_for_extract_running(dep_name, ext_name, max_wait=60, poll_interval=5)
            print_log("info" if ok else "error", f"{'extract status':<{_W - (0 if ok else 1)}}: {'RUNNING' if ok else status:<1}  ({ext_name}@{dep_name})")
            if not ok:
                print_log("error", f"integrated extract '{ext_name}' on '{dep_name}' is not RUNNING ({status}); aborting initial_load")
                _print_full_summary(primary_replicat_results, primary_results, il_results, il_replicat_results, _W)
                return 1

    # ═══ Step 3/5: consistency SCN from primary extract ══════════════════════════════════
    print_log("info", "")
    print_log("info", "------ Step 3/5: consistency SCN from primary extract ------")
    scn_dep, scn_ext = extract_started[0]
    if dry_run:
        print_log("info", f"(plan) would parse last OGG-02845 line in '{scn_ext}.rpt' on '{scn_dep}' to extract the consistency SCN")
        consistency_scn = None
    else:
        ok_scn, consistency_scn, scn_msg = get_consistency_scn_from_extract_report(scn_dep, scn_ext)
        print_log("info" if ok_scn else "error", f"{'consistency SCN primary extract':<{_W - (0 if ok_scn else 1)}}: {'OK' if ok_scn else 'ERROR':<1}  ({scn_msg})")
        if not ok_scn:
            print_log("error", f"could not obtain consistency SCN from primary extract; aborting initial_load")
            _print_full_summary(primary_replicat_results, primary_results, il_results, il_replicat_results, _W)
            return 1

    # ── Plan-only helpers: pre-generate the IL extract / IL replicat names that this run
    # WOULD use. generate_extract_name / generate_replicat_name return the FIRST free NN
    # for the deployment but don't reserve it; calling them several times in a row would
    # return the same name. To simulate the post-create state we cache the starting NN per
    # (deployment, role) and increment locally for every subsequent planned process.
    # In a real apply these counters are unused — names are generated at create-time and
    # may differ if other runs allocated NNs in between.
    _il_extract_nn  = {}   # dep -> [prefix, next_nn, suffix]
    _il_replicat_nn = {}   # dep -> [prefix, next_nn, suffix]

    # IL names are 5 chars: 'EI<NN><env>' / 'RI<NN><env>'. Prefix is 2 chars, NN at pos 2-3,
    # env letter at pos 4.
    def _next_planned_il_extract_name(dep):
        if dep not in _il_extract_nn:
            name, msg = generate_extract_name(dep, env, cdc_seq, role="initial_load")
            if not name:
                return f"(plan: {msg})"
            _il_extract_nn[dep] = [name[:2], _b36_to_nn(name[2:4]), name[4:]]
        prefix, nn, suffix = _il_extract_nn[dep]
        out = f"{prefix}{_nn_to_b36(nn)}{suffix}"
        _il_extract_nn[dep][1] += 1
        return out

    def _next_planned_il_replicat_name(dep, prefer_nn=None):
        # In plan mode try to mirror the IL extract's NN ('best-effort sync'). If the
        # preferred NN is past the counter for this deployment, advance the counter to
        # match — that's a synthetic visualisation, not a real reservation.
        if dep not in _il_replicat_nn:
            name, msg = generate_replicat_name(dep, env, cdc_seq, role="initial_load", prefer_nn=prefer_nn)
            if not name:
                return f"(plan: {msg})"
            _il_replicat_nn[dep] = [name[:2], _b36_to_nn(name[2:4]), name[4:]]
        prefix, nn, suffix = _il_replicat_nn[dep]
        if prefer_nn is not None and prefer_nn >= nn:
            nn = prefer_nn
        out = f"{prefix}{_nn_to_b36(nn)}{suffix}"
        _il_replicat_nn[dep][1] = nn + 1
        return out

    # ═══ Step 4/5: INITIAL_LOAD EXTRACTS (only for newly-added tables) ═══════════════════
    # Build the table set from what was actually appended to the primary extract this run.
    tables_for_il = sorted({t for added in newly_added_per_dep.values() for t in added})
    print_log("info", "")
    print_log("info", f"------ Step 4/5: INITIAL_LOAD EXTRACTS ({len(tables_for_il)} table(s)) ------")
    if not tables_for_il:
        print_log("info", "no new tables were added to the primary extract; IL extracts step is skipped")
    elif not extract_deps_il:
        print_log("warning", "no initial_load Extract deployment (_E<digits>_) found; skipping IL extracts")
    else:
        # ── Re-use the table-size analysis already computed at the start of the flow ────
        # tables_size_all contains (OWNER.TABLE, total_gb, parallel_count) for EVERY table
        # in the JSON. Here we filter it to just the tables newly added to the primary
        # extract this run (tables_for_il). No re-connect to Oracle is needed.
        tables_size = [(t, gb, pc) for (t, gb, pc) in tables_size_all if t in tables_for_il]

        # ── Build pending pool (one job per partition per table per IL deployment) ──────
        pending = []
        for dep_name in extract_deps_il:
            for full, total_gb, parallel_count in tables_size:
                if "." not in full:
                    print_log("error", f"invalid table entry (expected SCHEMA.TABLE): {full}")
                    continue
                schema, table  = full.split(".", 1)
                parallel_count = max(1, int(parallel_count) if str(parallel_count).isdigit() else 1)
                for part_idx in range(parallel_count):
                    if parallel_count == 1:
                        sqlpred = f"AS OF SCN {consistency_scn}"
                    else:
                        sqlpred = f"AS OF SCN {consistency_scn} WHERE ORA_HASH(ROWID,{parallel_count - 1})={part_idx}"
                    pending.append({
                        "dep":         dep_name,
                        "full":        full,
                        "schema":      schema,
                        "table":       table,
                        "part_idx":    part_idx,
                        "total_parts": parallel_count,
                        "sqlpred":     sqlpred,
                    })
        total_jobs = len(pending)

        # initial_load_parallel from db_connections.yaml (with sane fallbacks)
        max_parallel_raw = bbdd_conn.get("initial_load_parallel")
        try:
            max_parallel = int(max_parallel_raw) if max_parallel_raw is not None else 0
        except (ValueError, TypeError):
            max_parallel = 0
        if max_parallel < 1:
            max_parallel = max(1, total_jobs)
        _il_concurrent = min(max_parallel, total_jobs)
        print_log("info", f"{'initial_load_parallel':<{_W}}: {max_parallel}  (total_jobs={total_jobs}) — up to {_il_concurrent} will run concurrently")

        # Pre-check: ensure DB credential alias exists on each IL extract deployment.
        # il_useridalias points to standby when available, otherwise primary.
        _il_conn_string = bbdd_conn.get("conn_string_stby") or bbdd_conn.get("conn_string_prim", "")
        _il_userid      = f"{bbdd_conn.get('db_user', '')}@{_il_conn_string}"
        _failed_il_deps = set()
        for _dep_il in extract_deps_il:
            _ok_il = _ensure_credential_alias(
                oggctl_path,
                alias=il_useridalias,
                userid=_il_userid,
                password_credential=bbdd_conn.get("password_credential", ""),
                dep_name=_dep_il,
                dry_run=dry_run,
                width=_W,
            )
            if not _ok_il:
                _failed_il_deps.add(_dep_il)
                print_log("error", f"IL deployment '{_dep_il}' skipped (credential alias failed)")
        if _failed_il_deps:
            pending = [j for j in pending if j["dep"] not in _failed_il_deps]

        if dry_run:
            # In plan mode the consistency SCN was NOT queried (Step 3 was skipped), so
            # 'consistency_scn' is None and the sqlpredicate placeholder is meaningless.
            # Surface this cleanly: report SCN as "<read at apply time>" and label the
            # sqlpredicate without the literal "None".
            # We also synthesize one row per planned job in 'il_results' so the Final
            # Summary table includes every IL extract that WOULD be created — without it
            # the plan output only shows Primary Extract / Primary Replicat and the IL
            # plan stays invisible.
            scn_label = "<read at apply time>"
            print_log("info", f"(plan) would launch {len(pending)} IL extract(s) on deployment(s) {sorted({j['dep'] for j in pending})} with max_parallel={max_parallel} (SCN={scn_label})")
            for j in pending:
                part_label = "" if j["total_parts"] == 1 else f" part {j['part_idx'] + 1}/{j['total_parts']}"
                if j["total_parts"] == 1:
                    trail   = f"initial/{bbdd_name}/{j['schema']}/{j['table']}/il"
                    pred    = f"AS OF SCN {scn_label}"
                else:
                    trail   = f"initial/{bbdd_name}/{j['schema']}/{j['table']}/P{j['part_idx'] + 1}/il"
                    pred    = f"AS OF SCN {scn_label} WHERE ORA_HASH(ROWID,{j['total_parts'] - 1})={j['part_idx']}"
                pred_short = pred if len(pred) <= 100 else pred[:97] + "..."
                planned_name = _next_planned_il_extract_name(j["dep"])
                print_log("info", f"(plan)   + {planned_name} for {j['full']}{part_label}@{j['dep']}  [trail: {trail}, sqlpredicate: {pred_short}]")
                il_results.append({
                    "dep":     j["dep"],
                    "table":   f"{j['full']}{part_label}",
                    "process": planned_name,
                    "status":  "(plan) would create + start",
                    "elapsed": "-",
                    "rows":    "-",
                    "is_ok":   True,
                    "trail":   trail,
                })
            pending = []   # short-circuit pool below

        running = []
        poll_interval = 5
        while pending or running:
            while pending and len(running) < max_parallel:
                job  = pending.pop(0)
                full = job["full"]
                part_suffix = "" if job["total_parts"] == 1 else f" part {job['part_idx'] + 1}/{job['total_parts']}"
                # generate name with retry on collision
                il_extract_name = None
                ok = False
                last_line = ""
                for attempt in range(1, 6):
                    il_extract_name, msg = generate_extract_name(job["dep"], env, cdc_seq, role="initial_load")
                    if il_extract_name is None:
                        last_line = msg or "generate_extract_name returned None"
                        break
                    if job["total_parts"] == 1:
                        il_trail_path = f"initial/{bbdd_name}/{job['schema']}/{job['table']}/il"
                    else:
                        il_trail_path = f"initial/{bbdd_name}/{job['schema']}/{job['table']}/P{job['part_idx'] + 1}/il"
                    # IL extract names are 5 chars ('EI<NN><env>') — NN sits at positions 2-3.
                    il_description = f"Extract-Initial_load-{bbdd_name}-{il_extract_name[2:4]}-{config['project']}-{env} for table {full}{part_suffix} (created by cdc-controller date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"
                    single_table_payload = json.dumps([{"name": full, "sqlpredicate": job["sqlpred"]}])
                    cmd = [
                        sys.executable, oggctl_path,
                        "extract", "create", il_extract_name,
                        "--extract-type",            "initial_load",
                        "--trail",                   il_trail_path,
                        "--tables-json",             single_table_payload,
                        "--useridalias",             il_useridalias,
                        "--pdb",                     source_pdb,
                        "--description",             il_description,
                        "--managed-process-profile", "Default",
                        "--deployment",              job["dep"],
                    ]
                    try:
                        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                        ok        = result.returncode == 0
                        output    = (result.stdout or "") + (result.stderr or "")
                        last_line = output.strip().splitlines()[-1] if output.strip() else f"return code {result.returncode}"
                    except Exception as e:
                        ok, last_line, output = False, str(e), str(e)
                    if ok:
                        break
                    if "already exists" in output.lower():
                        print_log("warning", f"extract name '{il_extract_name}' collision, regenerating ...")
                        continue
                    break
                print_log("info" if ok else "error", f"{'initial_load extract create':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({il_extract_name} table {full}{part_suffix}@{job['dep']}: {last_line})")
                if not ok:
                    il_results.append({"dep": job["dep"], "table": f"{full}{part_suffix}", "process": il_extract_name or "-", "status": "Error (CREATE_FAILED)", "elapsed": "-", "rows": "-", "is_ok": False, "trail": None})
                    continue

                ok, last_line = _run_oggctl(
                    [sys.executable, oggctl_path, "extract", "start", il_extract_name, "--deployment", job["dep"]],
                    "initial_load extract start", il_extract_name, job["dep"], False, _W,
                )
                if ok:
                    job["ext_name"]   = il_extract_name
                    job["trail"]      = il_trail_path
                    # Remember the NN so Step 5 can ask the replicat side to use the same
                    # NN (best-effort sync — falls back to the next free NN if taken).
                    # The NN lives in positions 2-3 of the 5-char IL name (e.g. 'EI00D').
                    try:
                        job["il_nn"] = _b36_to_nn(il_extract_name[2:4])
                    except (ValueError, TypeError, IndexError):
                        job["il_nn"] = None
                    _il_purge_task_name = f"Purge-trail-{il_extract_name}"
                    if not dry_run:
                        _del_ok = subprocess.run(
                            [sys.executable, oggctl_path, "task", "purge-trail", "delete", _il_purge_task_name,
                             "--deployment", job["dep"]],
                            capture_output=True, text=True, timeout=30,
                        )
                        if _del_ok.returncode == 0:
                            print_log("warning", f"{'purge task check':<{_W}}: WARNING  ({_il_purge_task_name}@{job['dep']}: already existed — deleted before recreating)")
                    _run_oggctl(
                        [sys.executable, oggctl_path, "task", "purge-trail", "create", _il_purge_task_name,
                         "--trail", il_trail_path,
                         "--keep-value", _purge_days_initial, "--keep-units", "days",
                         "--schedule-value", "1", "--schedule-units", "days",
                         "--critical", "--use-checkpoints", "--enabled",
                         "--skip-trail-check",
                         "--deployment", job["dep"]],
                        "purge task create", _il_purge_task_name, job["dep"], dry_run, _W,
                    )
                    job["start_time"] = time.time()
                    job["total_rows"] = None   # fetched below, after fill, to avoid blocking new slots
                    running.append(job)
                else:
                    il_results.append({"dep": job["dep"], "table": f"{full}{part_suffix}", "process": il_extract_name, "status": "Error (START_FAILED)", "elapsed": "-", "rows": "-", "is_ok": False, "trail": il_trail_path})

            if not running:
                break

            print_log("info", f"{'initial_load extract pool':<{_W}}: {len(running)} running, {len(pending)} pending of {total_jobs} total (max_parallel={max_parallel}) — waiting for completion ...")

            # Fetch row counts for newly started jobs.  Moved outside the fill loop because
            # count_rows_as_of_scn runs a SQL COUNT AS OF SCN which can take several minutes
            # on large tables — keeping it inside the fill loop delayed every subsequent slot
            # launch by that duration.  Counts are display-only (used for progress % lines).
            for job in running:
                if job.get("_count_done"):
                    continue
                _part_label  = "" if job["total_parts"] == 1 else f" part {job['part_idx']+1}/{job['total_parts']}"
                _sql_preview = f"SELECT /*+ PARALLEL(4) */ count(1) FROM {job['full']} {job['sqlpred']}"
                print_log("info", f"{'initial_load row count':<{_W}}: counting rows to be extracted for '{job['full']}{_part_label}' ({_sql_preview}) ...")
                ok_cnt, total_rows, count_msg = count_rows_as_of_scn(bbdd_name, env, job["full"], job["sqlpred"])
                job["total_rows"] = total_rows if (ok_cnt and total_rows is not None) else None
                job["_count_done"] = True
                if ok_cnt and total_rows is not None:
                    print_log("info", f"{'initial_load row count':<{_W}}: '{job['full']}{_part_label}' → {total_rows} rows")
                else:
                    print_log("warning", f"could not get row count for '{job['full']}{_part_label}' ({count_msg}); progress will be shown without a percentage")
            # Progress cadence:
            #   - STATS check every 30 s while the extract is RUNNING.
            #   - Progress line written to the log every 15 min while the extract is RUNNING.
            #   - When a process FINISHES, the 'finished' branch below emits its own line
            #     immediately, so no wait is needed in that case.
            ext_wait_start_at  = time.time()
            ext_next_check_at  = ext_wait_start_at + 30
            ext_last_log_at    = ext_wait_start_at
            _EXT_CHECK_INTERVAL = 30
            # Progress cadence: first 30 min → every 2 min; after 30 min → every 10 min
            while True:
                time.sleep(poll_interval)
                finished = []
                for job in running:
                    ok_st, status = get_extract_status_now(job["dep"], job["ext_name"])
                    s = str(status).upper()
                    if (not ok_st) or s not in ("RUNNING", "STARTING"):
                        job["end_time"] = time.time()
                        finished.append((job, status))

                # Periodic STATS query every 30 s. The STATS call always runs so we keep
                # per-process bookkeeping warm and so that finished processes get caught
                # quickly (the 'finished' branch above emits the line immediately).
                # 'should_log' gates whether the progress line is written: only every
                # 15 min while the job is still RUNNING.
                now = time.time()
                if now >= ext_next_check_at:
                    _ext_elapsed        = now - ext_wait_start_at
                    _ext_log_interval   = 120 if _ext_elapsed <= 1800 else 600
                    should_log          = (now - ext_last_log_at) >= _ext_log_interval
                    ext_next_check_at   = now + _EXT_CHECK_INTERVAL
                    finished_ids = {id(fj) for fj, _ in finished}
                    for job in running:
                        if id(job) in finished_ids:
                            continue
                        ok_p, applied, pmsg = get_il_extract_rows_applied(job["dep"], job["ext_name"], job["full"])
                        total = job.get("total_rows")
                        if ok_p and applied is not None and total:
                            pct    = (applied * 100.0 / total) if total > 0 else 0.0
                            detail = f"{applied}/{total} extracted, {pct:.1f}% completed (status=RUNNING)"
                        elif ok_p and applied is not None:
                            detail = f"{applied} rows extracted (total unknown) (status=RUNNING)"
                        else:
                            detail = f"STATS not available yet ({pmsg}) (status=RUNNING)"
                        if should_log:
                            _ok_rd, _rate, _delta = _get_ogg_rate_delta(oggctl_path, job["dep"], job["ext_name"], "extract")
                            _rate_detail = f", rate {_rate}, delta {_delta}" if _ok_rd else ""
                            if ok_p and applied is not None and total:
                                detail = f"{applied}/{total} extracted{_rate_detail}, {pct:.1f}% completed (status=RUNNING)"
                            elif ok_p and applied is not None:
                                detail = f"{applied} rows extracted{_rate_detail} (total unknown) (status=RUNNING)"
                            print_log("info", f"{'initial_load extract progress':<{_W}}: {job['ext_name']} for '{job['full']}', {detail}")
                    if should_log:
                        ext_last_log_at = now

                if finished:
                    for job, status in finished:
                        running.remove(job)
                        part_label = "" if job["total_parts"] == 1 else f" part {job['part_idx'] + 1}/{job['total_parts']}"
                        elapsed_s  = job["end_time"] - job.get("start_time", job["end_time"])
                        h, rem = divmod(int(elapsed_s), 3600)
                        m, sec = divmod(rem, 60)
                        elapsed = f"{h}:{m:02d}:{sec:02d}"
                        s = str(status).upper()
                        row_table = f"{job['full']}{part_label}"
                        # Always log a final 100% progress line before the finished message
                        _fin_ok_p, _fin_applied, _ = get_il_extract_rows_applied(job["dep"], job["ext_name"], job["full"])
                        _fin_total = job.get("total_rows")
                        if _fin_ok_p and _fin_applied is not None and _fin_total:
                            try:
                                _fin_pct = (_fin_applied * 100.0 / _fin_total) if int(_fin_total) > 0 else 0.0
                            except (ValueError, TypeError):
                                _fin_pct = 0.0
                            _fin_detail = f"{_fin_applied}/{_fin_total} extracted, {_fin_pct:.1f}% completed (status={status})"
                        elif _fin_ok_p and _fin_applied is not None:
                            _fin_detail = f"{_fin_applied} rows extracted (total unknown) (status={status})"
                        else:
                            _fin_detail = f"(status={status})"
                        print_log("info", f"{'initial_load extract progress':<{_W}}: {job['ext_name']} for '{job['full']}{part_label}', {_fin_detail}")
                        rpt_err_ok, has_errors, err_lines, _ = check_extract_report_for_errors(job["dep"], job["ext_name"])
                        # Aligned label used by every termination outcome (OK / ERROR), padded
                        # so the colon lines up with the rest of the apply log.
                        _il_label = "initial_load extract finished"
                        if has_errors:
                            first_err = err_lines[0].strip()[:120] if err_lines else "unknown"
                            print_log("error", f"{_il_label:<{_W - 1}}: ERROR  '{job['ext_name']}' for '{job['full']}'{part_label} with {len(err_lines)} error(s) [elapsed: {elapsed}]")
                            il_results.append({"dep": job["dep"], "table": row_table, "process": job["ext_name"], "status": f"Error ({s}; report: {first_err})", "elapsed": elapsed, "rows": "-", "is_ok": False, "trail": job.get("trail")})
                        elif s == "STOPPED":
                            user_stopped, _, _ = extract_was_user_stopped(job["dep"], job["ext_name"])
                            if user_stopped:
                                print_log("error", f"{_il_label:<{_W - 1}}: ERROR  '{job['ext_name']}' for '{job['full']}'{part_label} was stopped manually [elapsed: {elapsed}]")
                                il_results.append({"dep": job["dep"], "table": row_table, "process": job["ext_name"], "status": "Error (USER_STOPPED)", "elapsed": elapsed, "rows": "-", "is_ok": False, "trail": job.get("trail")})
                            else:
                                # Parse Run Time Statistics from the report to surface the
                                # number of rows the IL extract wrote to its trail file.
                                # The rows count is treated as mandatory: if it can't be
                                # obtained we still consider the extract OK (the report says
                                # it finished cleanly) but emit a WARNING so the operator
                                # notices the telemetry gap.
                                ok_rows, rows_count, rows_msg = get_il_extract_rows_from_report(job["dep"], job["ext_name"])
                                # Show '<extracted>/<total> extracted, X% completed' when
                                # we have both numbers; otherwise just '<extracted> extracted'.
                                # 'total_rows' came from the count(1) AS OF SCN executed
                                # right after the extract start.
                                total_for_log = job.get("total_rows")
                                if ok_rows and rows_count is not None:
                                    if total_for_log:
                                        try:
                                            _pct = (int(rows_count) * 100.0 / int(total_for_log)) if int(total_for_log) > 0 else 0.0
                                        except (ValueError, TypeError):
                                            _pct = 0.0
                                        rows_label_ext = f", {rows_count}/{total_for_log} extracted, {_pct:.1f}% completed"
                                    else:
                                        rows_label_ext = f", {rows_count} extracted"
                                    print_log("info", f"{_il_label:<{_W}}: OK  '{job['ext_name']}' for '{job['full']}'{part_label}{rows_label_ext} [elapsed: {elapsed}]")
                                    rows_field = str(rows_count)
                                else:
                                    print_log("info",    f"{_il_label:<{_W}}: OK  '{job['ext_name']}' for '{job['full']}'{part_label} [elapsed: {elapsed}]")
                                    print_log("warning", f"could not obtain row count from '{job['ext_name']}.rpt': {rows_msg}")
                                    rows_field = "?"
                                il_results.append({"dep": job["dep"], "table": row_table, "process": job["ext_name"], "status": "OK (STOPPED, no errors)", "elapsed": elapsed, "rows": rows_field, "total_rows": job.get("total_rows"), "is_ok": True, "trail": job.get("trail")})
                        else:
                            print_log("error", f"{_il_label:<{_W - 1}}: ERROR  '{job['ext_name']}' for '{job['full']}'{part_label} with status: {status} [elapsed: {elapsed}]")
                            il_results.append({"dep": job["dep"], "table": row_table, "process": job["ext_name"], "status": f"Error ({s})", "elapsed": elapsed, "rows": "-", "is_ok": False, "trail": job.get("trail")})
                    break

        # In plan mode the pool above is short-circuited, so this summary line would be
        # misleading (nothing actually ran). Only emit it after a real apply.
        if not dry_run:
            print_log("info", f"all IL extracts completed for BBDD '{bbdd_name}'")

    # IL Extract intermediate summary — printed here so the operator can review extract
    # results before the IL Replicats step begins. Printed again in the Final Summary.
    _print_il_extract_summary(il_results, _W)

    # ═══ Step 5/5: INITIAL_LOAD REPLICATS (coordinated, one per IL trail) ════════════════
    print_log("info", "")
    print_log("info", "------ Step 5/5: INITIAL_LOAD REPLICATS ------")
    _run_il_replicats_step(
        tables_for_il        = tables_for_il,
        tables_size_all      = tables_size_all,
        replicat_deps_il     = replicat_deps_il,
        il_results           = il_results,
        il_replicat_results  = il_replicat_results,
        bbdd_name            = bbdd_name,
        env                  = env,
        cdc_seq              = cdc_seq,
        rep_target           = rep_target,
        source_pdb           = source_pdb,
        project              = config["project"],
        kafka_topic_prefix   = config.get("kafka_topic_prefix", ""),
        audit_comment        = audit_comment,
        config_table_columns = config_table_columns,
        oggctl_path          = oggctl_path,
        dry_run              = dry_run,
        _W                   = _W,
        plan_replicat_namer  = _next_planned_il_replicat_name,
        s3_target_conn       = _s3_tc if rep_target == "s3" else None,
        kafka_target_name    = _kafka_target_name if rep_target == "kafka_confluent" else None,
        s3_bucket_path       = config.get("s3_bucket_path", ""),
    )

    # ═══ Final Summary ═══════════════════════════════════════════════════════════════════
    _print_full_summary(primary_replicat_results, primary_results, il_results, il_replicat_results, _W)
    print_log("info", f"end {mode_label}")
    return 0


# ── End of _apply_create_full — the Step 5/5 body now lives in _run_il_replicats_step
# (defined above). The dead inline block that used to sit here has been deleted.


def _print_il_extract_summary(il_results, _W):
    # Prints the "Final Summary: initial_load Extracts" table standalone.
    # Called just before the IL Replicats step (intermediate checkpoint) and again
    # at the very end via _print_full_summary.  Both calls use the same logic.
    _MIN_PROCESS = 8
    _MIN_TYPE    = 24
    _MIN_STATUS  = 30
    _MIN_TABLE   = 18
    _MIN_ELAPSED = 10

    def _il_row(process, ttype, table, status, elapsed, rows, total):
        rows_disp   = rows
        status_disp = status
        try:
            rows_n  = int(rows)  if rows  not in ("?", "-") else None
            total_n = int(total) if total not in (None, "?", "-") else None
        except (ValueError, TypeError):
            rows_n, total_n = None, None
        pct_disp = "-"
        if rows_n is not None and total_n is not None:
            rows_disp = f"{rows_n}/{total_n}"
            pct_disp  = f"{rows_n * 100.0 / total_n:.1f}%" if total_n > 0 else "0.0%"
            if rows_n < total_n:
                status_disp = f"WARNING (partial — {rows_n}/{total_n} rows; original: {status})"
        elif rows == "?" and str(status).startswith("OK (") and "no errors" in str(status):
            status_disp = status.replace("OK (", "WARNING (", 1).replace(", no errors", ", row count unavailable", 1)
        return {"process": process, "type": ttype, "table": table, "status": status_disp,
                "elapsed": elapsed, "rows": rows_disp, "pct": pct_disp}

    ile_rows = [
        _il_row(r["process"], "Extract (initial_load)", r["table"], r["status"],
                r["elapsed"], r.get("rows", "-"), r.get("total_rows"))
        for r in il_results
    ]
    if not ile_rows:
        return

    _P = max(_MIN_PROCESS, len("PROCESS"), max((len(r["process"]) for r in ile_rows), default=0))
    _T = max(_MIN_TYPE,    len("TYPE"),    max((len(r["type"])    for r in ile_rows), default=0))
    _S = max(_MIN_STATUS,  len("STATUS"),  max((len(r["status"])  for r in ile_rows), default=0))
    _B = max(_MIN_TABLE,   len("TABLE"),   max((len(r["table"])   for r in ile_rows), default=0))
    _R = max(              len("ROWS"),    max((len(str(r["rows"])) for r in ile_rows), default=0))
    _PCT = max(            len("PCT%"),    max((len(r["pct"])      for r in ile_rows), default=0))
    _E = max(_MIN_ELAPSED, len("ELAPSED"), max((len(r["elapsed"]) for r in ile_rows), default=0))

    print_log("info", "")
    print_log("info", "====== Final Summary: initial_load Extracts ======")
    deps = sorted({r["dep"] for r in il_results if r.get("dep")})
    print_log("info", f"OGG Deployment : {', '.join(deps) if deps else '(unknown)'}")
    sep = f"{'-'*_P}  {'-'*_T}  {'-'*_B}  {'-'*_S}  {'-'*_R}  {'-'*_PCT}  {'-'*_E}"
    print_log("info", sep)
    print_log("info", f"{'PROCESS':<{_P}}  {'TYPE':<{_T}}  {'TABLE':<{_B}}  {'STATUS':<{_S}}  {'ROWS':<{_R}}  {'PCT%':<{_PCT}}  {'ELAPSED':<{_E}}")
    print_log("info", sep)
    for r in sorted(ile_rows, key=lambda x: x["process"]):
        print_log("info", f"{r['process']:<{_P}}  {r['type']:<{_T}}  {r['table']:<{_B}}  {r['status']:<{_S}}  {str(r['rows']):<{_R}}  {r['pct']:<{_PCT}}  {r['elapsed']:<{_E}}")
    print_log("info", sep)
    _ile_ok  = sum(1 for r in il_results if r["is_ok"])
    _ile_err = len(il_results) - _ile_ok
    print_log("info" if _ile_err == 0 else "error",
              f"initial_load extracts result: {'OK' if _ile_err == 0 else 'ERROR'}  (total={len(il_results)}, OK={_ile_ok}, ERROR={_ile_err})")


def _print_full_summary(primary_replicat_results, primary_results, il_results, il_replicat_results, _W):
    # Prints the four Final Summary tables produced by _apply_create_full:
    #   1) Primary Replicat   2) Primary Extract   3) IL Extracts   4) IL Replicats
    # Each table is only emitted if it has at least one row.
    #
    # Column-width policy: shared columns (PROCESS, TYPE, STATUS) use a SINGLE width
    # computed across ALL FOUR tables, so the headers and values line up vertically when
    # the four blocks are printed one after another. Per-table columns (ACTION + LAST
    # STARTUP TIME for Primary; TABLE + ELAPSED for IL) keep their own widths since
    # there is nothing to align them against. Minimums are floors, never caps.
    _rep_mode_label = {"nonintegrated": "classic", "coordinated": "coordinated"}

    _MIN_PROCESS    = 8
    _MIN_TYPE       = 24
    _MIN_ACTION     = 22
    _MIN_STATUS     = 30
    _MIN_TABLE      = 18
    _MIN_LAST_START = 26
    _MIN_ELAPSED    = 10

    # ── Phase 1: build all four 'rows' lists up-front so we can compute global widths.
    prep_rows = []   # Primary Replicat rows
    for pr in primary_replicat_results:
        ok_info, info = get_replicat_info_now(pr["dep"], pr["ext_name"])
        if ok_info:
            status     = str(info.get("status", "unknown"))
            last_start = find_extract_last_started(info) or "-"
            mode       = info.get("mode")
            mode_type  = mode.get("type") if isinstance(mode, dict) else None
            type_lbl   = _rep_mode_label.get(mode_type, mode_type or "unknown")
        else:
            status, last_start, type_lbl = f"unknown ({info.get('error', '')})", "-", "unknown"
        prep_rows.append({"process": pr["ext_name"], "type": f"Replicat ({type_lbl})", "action": pr["action"], "status": status, "last_start": str(last_start)})

    pri_rows = []    # Primary Extract rows
    for pr in primary_results:
        ok_info, info = get_extract_info_now(pr["dep"], pr["ext_name"])
        if ok_info:
            status     = str(info.get("status", "unknown"))
            last_start = find_extract_last_started(info) or "-"
        else:
            status, last_start = f"unknown ({info.get('error', '')})", "-"
        pri_rows.append({"process": pr["ext_name"], "type": "Extract (integrated)", "action": pr["action"], "status": status, "last_start": str(last_start)})

    def _il_row_for_summary(process, ttype, table, status, elapsed, rows, total):
        # Render the ROWS cell as '<rows>/<total>' when we have both numbers; otherwise
        # just '<rows>'. When the process finished but applied/extracted FEWER rows than
        # expected (rows < total), bump the STATUS to WARNING — this catches partial
        # finishes such as a user STOP issued before the bulk load completed.
        # When rows='?' (STATS unretrievable) and the process reports finished OK, also
        # bump to WARNING because we cannot confirm all rows were processed.
        rows_disp   = rows
        status_disp = status
        try:
            rows_n  = int(rows) if rows not in ("?", "-") else None
            total_n = int(total) if total not in (None, "?", "-") else None
        except (ValueError, TypeError):
            rows_n, total_n = None, None
        pct_disp = "-"
        if rows_n is not None and total_n is not None:
            rows_disp = f"{rows_n}/{total_n}"
            pct_disp  = f"{rows_n * 100.0 / total_n:.1f}%" if total_n > 0 else "0.0%"
            if rows_n < total_n:
                status_disp = f"WARNING (partial — {rows_n}/{total_n} rows; original: {status})"
        elif rows == "?" and str(status).startswith("OK (") and "no errors" in str(status):
            status_disp = status.replace("OK (", "WARNING (", 1).replace(", no errors", ", row count unavailable", 1)
        return {"process": process, "type": ttype, "table": table, "status": status_disp, "elapsed": elapsed, "rows": rows_disp, "pct": pct_disp}

    ile_rows = [
        _il_row_for_summary(r["process"], "Extract (initial_load)", r["table"], r["status"],          r["elapsed"],          r.get("rows", "-"), r.get("total_rows"))
        for r in il_results
    ]
    ilr_rows = [
        _il_row_for_summary(r["ext_name"], "Replicat (initial_load)", r["table"], r.get("action", "-"), r.get("elapsed", "-"), r.get("rows", "-"), r.get("total_rows"))
        for r in il_replicat_results
    ]

    # ── Phase 2: compute GLOBAL widths for shared columns (PROCESS / TYPE / STATUS).
    all_rows = prep_rows + pri_rows + ile_rows + ilr_rows
    _P_glob = max(_MIN_PROCESS, len("PROCESS"), max((len(r["process"]) for r in all_rows), default=0))
    _T_glob = max(_MIN_TYPE,    len("TYPE"),    max((len(r["type"])    for r in all_rows), default=0))
    _S_glob = max(_MIN_STATUS,  len("STATUS"),  max((len(r["status"])  for r in all_rows), default=0))

    # ---- Primary Replicat ----
    if prep_rows:
        print_log("info", "")
        print_log("info", "====== Final Summary: Primary Replicat ======")
        deps = sorted({pr["dep"] for pr in primary_replicat_results})
        print_log("info", f"OGG Deployment : {', '.join(deps) if deps else '(unknown)'}")
        _A = max(_MIN_ACTION,     len("ACTION"),            max((len(r["action"])     for r in prep_rows), default=0))
        _L = max(_MIN_LAST_START, len("LAST STARTUP TIME"), max((len(r["last_start"]) for r in prep_rows), default=0))
        sep = f"{'-'*_P_glob}  {'-'*_T_glob}  {'-'*_A}  {'-'*_S_glob}  {'-'*_L}"
        print_log("info", sep); print_log("info", f"{'PROCESS':<{_P_glob}}  {'TYPE':<{_T_glob}}  {'ACTION':<{_A}}  {'STATUS':<{_S_glob}}  {'LAST STARTUP TIME':<{_L}}"); print_log("info", sep)
        for r in sorted(prep_rows, key=lambda x: x["process"]):
            print_log("info", f"{r['process']:<{_P_glob}}  {r['type']:<{_T_glob}}  {r['action']:<{_A}}  {r['status']:<{_S_glob}}  {r['last_start']:<{_L}}")
        print_log("info", sep)

    # ---- Primary Extract ----
    if pri_rows:
        print_log("info", "")
        print_log("info", "====== Final Summary: Primary Extract ======")
        deps = sorted({pr["dep"] for pr in primary_results})
        print_log("info", f"OGG Deployment : {', '.join(deps) if deps else '(unknown)'}")
        _A = max(_MIN_ACTION,     len("ACTION"),            max((len(r["action"])     for r in pri_rows), default=0))
        _L = max(_MIN_LAST_START, len("LAST STARTUP TIME"), max((len(r["last_start"]) for r in pri_rows), default=0))
        sep = f"{'-'*_P_glob}  {'-'*_T_glob}  {'-'*_A}  {'-'*_S_glob}  {'-'*_L}"
        print_log("info", sep); print_log("info", f"{'PROCESS':<{_P_glob}}  {'TYPE':<{_T_glob}}  {'ACTION':<{_A}}  {'STATUS':<{_S_glob}}  {'LAST STARTUP TIME':<{_L}}"); print_log("info", sep)
        for r in sorted(pri_rows, key=lambda x: x["process"]):
            print_log("info", f"{r['process']:<{_P_glob}}  {r['type']:<{_T_glob}}  {r['action']:<{_A}}  {r['status']:<{_S_glob}}  {r['last_start']:<{_L}}")
        print_log("info", sep)

    # ---- IL Extracts ----
    # Includes a ROWS column that surfaces the total rows extracted to the trail file
    # (parsed from the '# inserts:' lines of the report). '-' means the count is N/A
    # (e.g. the job errored before producing trail data); '?' means the report was read
    # but the inserts line was missing — see the WARNING emitted earlier in the run.
    if ile_rows:
        print_log("info", "")
        print_log("info", "====== Final Summary: initial_load Extracts ======")
        deps = sorted({r["dep"] for r in il_results if r.get("dep")})
        print_log("info", f"OGG Deployment : {', '.join(deps) if deps else '(unknown)'}")
        _B   = max(_MIN_TABLE,   len("TABLE"),   max((len(r["table"])    for r in ile_rows), default=0))
        _R   = max(              len("ROWS"),    max((len(str(r["rows"])) for r in ile_rows), default=0))
        _PCT = max(              len("PCT%"),    max((len(r["pct"])       for r in ile_rows), default=0))
        _E   = max(_MIN_ELAPSED, len("ELAPSED"), max((len(r["elapsed"])  for r in ile_rows), default=0))
        sep = f"{'-'*_P_glob}  {'-'*_T_glob}  {'-'*_B}  {'-'*_S_glob}  {'-'*_R}  {'-'*_PCT}  {'-'*_E}"
        print_log("info", sep); print_log("info", f"{'PROCESS':<{_P_glob}}  {'TYPE':<{_T_glob}}  {'TABLE':<{_B}}  {'STATUS':<{_S_glob}}  {'ROWS':<{_R}}  {'PCT%':<{_PCT}}  {'ELAPSED':<{_E}}"); print_log("info", sep)
        for r in sorted(ile_rows, key=lambda x: x["process"]):
            print_log("info", f"{r['process']:<{_P_glob}}  {r['type']:<{_T_glob}}  {r['table']:<{_B}}  {r['status']:<{_S_glob}}  {str(r['rows']):<{_R}}  {r['pct']:<{_PCT}}  {r['elapsed']:<{_E}}")
        print_log("info", sep)
        _ile_ok   = sum(1 for r in ile_rows if str(r.get("status", "")).startswith("OK ("))
        _ile_warn = sum(1 for r in ile_rows if str(r.get("status", "")).startswith("WARNING"))
        _ile_err  = sum(1 for r in ile_rows if str(r.get("status", "")).startswith("ERROR") or str(r.get("status", "")).startswith("skipped"))
        _ile_total = len(il_results)
        if _ile_err > 0:
            _ile_res_str, _ile_res_lvl = "ERROR",   "error"
        elif _ile_warn > 0:
            _ile_res_str, _ile_res_lvl = "WARNING", "warning"
        else:
            _ile_res_str, _ile_res_lvl = "OK",      "info"
        print_log(_ile_res_lvl,
                  f"initial_load extracts apply result: {_ile_res_str}  (total={_ile_total}, OK={_ile_ok}, WARNING={_ile_warn}, ERROR={_ile_err})")

    # ---- IL Replicats ----
    # Note: the internal accumulator key is still 'action' (e.g. "create (started)",
    # "OK (STOPPED, no errors)", ...), but for the final summary we expose it under the
    # STATUS column to keep the column headings uniform across the 4 tables.
    if ilr_rows:
        print_log("info", "")
        print_log("info", "====== Final Summary: initial_load Replicats ======")
        deps = sorted({r["dep"] for r in il_replicat_results if r.get("dep")})
        print_log("info", f"OGG Deployment : {', '.join(deps) if deps else '(unknown)'}")
        _B   = max(_MIN_TABLE,   len("TABLE"),   max((len(r["table"])    for r in ilr_rows), default=0))
        _R   = max(              len("ROWS"),    max((len(str(r["rows"])) for r in ilr_rows), default=0))
        _PCT = max(              len("PCT%"),    max((len(r["pct"])       for r in ilr_rows), default=0))
        _E   = max(_MIN_ELAPSED, len("ELAPSED"), max((len(r["elapsed"])  for r in ilr_rows), default=0))
        sep = f"{'-'*_P_glob}  {'-'*_T_glob}  {'-'*_B}  {'-'*_S_glob}  {'-'*_R}  {'-'*_PCT}  {'-'*_E}"
        print_log("info", sep); print_log("info", f"{'PROCESS':<{_P_glob}}  {'TYPE':<{_T_glob}}  {'TABLE':<{_B}}  {'STATUS':<{_S_glob}}  {'ROWS':<{_R}}  {'PCT%':<{_PCT}}  {'ELAPSED':<{_E}}"); print_log("info", sep)
        for r in sorted(ilr_rows, key=lambda x: x["process"]):
            print_log("info", f"{r['process']:<{_P_glob}}  {r['type']:<{_T_glob}}  {r['table']:<{_B}}  {r['status']:<{_S_glob}}  {str(r['rows']):<{_R}}  {r['pct']:<{_PCT}}  {r['elapsed']:<{_E}}")
        print_log("info", sep)
        _ilr_ok   = sum(1 for r in ilr_rows if str(r.get("status", "")).startswith("OK ("))
        _ilr_warn = sum(1 for r in ilr_rows if str(r.get("status", "")).startswith("WARNING"))
        _ilr_err  = sum(1 for r in ilr_rows if str(r.get("status", "")).startswith("ERROR") or str(r.get("status", "")).startswith("skipped"))
        _total    = len(il_replicat_results)
        if _ilr_err > 0:
            _res_str, _res_lvl = "ERROR",   "error"
        elif _ilr_warn > 0:
            _res_str, _res_lvl = "WARNING", "warning"
        else:
            _res_str, _res_lvl = "OK",      "info"
        print_log(_res_lvl,
                  f"initial_load replicats apply result: {_res_str}  (total={_total}, OK={_ilr_ok}, WARNING={_ilr_warn}, ERROR={_ilr_err})")


def _apply_update_request(config, dry_run=False):
    # UPDATE flow: updates the MAP entries (column lists) in existing primary replicat(s).
    #
    # The extract is intentionally NOT touched — extracts capture every column by design;
    # column restrictions live exclusively on the replicat MAP via the COLMAP clause.
    # Tables that are missing from the replicat MAP are appended; tables already mapped
    # but with a different COLMAP are replaced in-place.
    #
    # Scope: ORACLE + replication_type=ONLINE.
    bbdd_engine = config["bbdd_engine"]
    _W = 38

    if bbdd_engine != "ORACLE":
        print_log("error", f"request_type=UPDATE is only supported for ORACLE (got {bbdd_engine})")
        return 1

    rep_type = (config.get("replication_type") or "").upper()
    if rep_type != "ONLINE":
        print_log("error", f"request_type=UPDATE is only supported for replication_type=ONLINE (got {rep_type})")
        return 1

    bbdd_name = config["bbdd_name"]
    env       = config["env"]

    print_log("info", "")
    print_log("info", f"====== UPDATE flow (engine={bbdd_engine}, bbdd={bbdd_name}, env={env}) ======")
    print_log("info", "")

    # 1) Parse tables + per-table column allow-list from the JSON input.
    config_tables, config_table_columns = _parse_tables_from_config(config)
    if not config_tables:
        print_log("error", "no tables defined in the JSON input")
        return 1

    # 2) Find extract and replicat deployments.
    deployments   = get_matching_deployments(env, bbdd_engine, load_types=["online"])
    extract_deps  = [n for n, _ in deployments if re.search(r"_E\d+_", n)]
    replicat_deps = [n for n, _ in deployments if re.search(r"_R\d+_", n)]

    if not extract_deps:
        print_log("error", f"no Extract deployment (_E<digits>_) found for env={env}, engine={bbdd_engine}")
        return 1
    if not replicat_deps:
        print_log("error", f"no Replicat deployment (_R<digits>_) found for env={env}, engine={bbdd_engine}")
        return 1

    # 3) Find existing primary extracts to derive the source trail path.
    print_log("info", "looking for existing primary extract associated with this BBDD ...")
    existing_extracts = get_extracts_for_bbdd(extract_deps, bbdd_name, env)
    found_extracts = [(dep, ext) for dep, exts in existing_extracts.items() for ext in exts]
    if not found_extracts:
        print_log("error", f"no primary extract found for BBDD '{bbdd_name}' (env={env}) — cannot determine trail; run CREATE first")
        return 1

    trail_ext_dep, trail_ext_name = found_extracts[0]
    trail_path = f"primary/{bbdd_name}/{trail_ext_name}/pr"
    print_log("info", f"{'source trail (from extract)':<{_W}}: {trail_path}")
    if len(found_extracts) > 1:
        print_log("warning", f"multiple primary extracts found for '{bbdd_name}'; using trail from '{trail_ext_name}'")

    # 4) Find replicats consuming that trail.
    print_log("info", f"looking for existing replicats consuming trail '{trail_path}' ...")
    existing_rep    = get_replicats_for_trail(replicat_deps, trail_path)
    found_replicats = [(dep, rep) for dep, reps in existing_rep.items() for rep in reps]
    if not found_replicats:
        print_log("error", f"no replicat found consuming trail '{trail_path}' — cannot perform UPDATE; run CREATE first")
        return 1

    print_log("info", f"{'replicats found':<{_W}}: {len(found_replicats)}")

    # 5) Update MAP entries in each replicat.
    _audit_comment = "TAGS " + json.dumps({
        "requested_by":  config.get("user_name", ""),
        "cost_center":   config.get("cost_center", ""),
        "change_number": config.get("change_number", ""),
        "date":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }, separators=(",", ":"))

    results    = []
    overall_ok = True
    for dep_name, rep_name in found_replicats:
        if dry_run:
            tables_desc = ", ".join(
                f"{t}({','.join(cols)})" if cols else t
                for t, cols in config_table_columns.items()
            )
            print_log("info", f"(plan) would update MAP entries in '{rep_name}' @ '{dep_name}' — tables: {tables_desc}")
            results.append({"dep": dep_name, "rep_name": rep_name, "action": "update (plan)"})
        else:
            print_log("info", f"updating MAP entries in replicat '{rep_name}' @ '{dep_name}' ...")
            ok, msg = update_tables_in_replicat(dep_name, rep_name, config_table_columns, comment=_audit_comment)
            print_log("info" if ok else "error",
                      f"{'replicat update-maps':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({rep_name}@{dep_name}: {msg})")
            results.append({"dep": dep_name, "rep_name": rep_name, "action": "update" if ok else f"FAILED: {msg}"})
            if not ok:
                overall_ok = False

    # 6) Final summary.
    print_log("info", "")
    print_log("info", "====== UPDATE Summary ======")
    for r in results:
        print_log("info", f"  {r['rep_name']}@{r['dep']}: {r['action']}")

    return 0 if overall_ok else 1


def apply(filepath, dry_run=False):
    # Configure all files and processes required by the replication processes based on the source database engine (Oracle, MongoDB, DB2).
    # Oracle and MongoDB: CDC engine is Oracle Goldengate
    # DB2: CDC engine is IBM Infopshere Data replicator
    #
    # When dry_run=True (entry point: plan()):
    #   - all detection / lookups still run (deployments, existing extracts, table sizes, etc.)
    #   - NO mutating action is executed: no oggctl 'create', 'register', 'start', no PRM patching,
    #     no waiting for RUNNING state, no pool polling.
    #   - every action that WOULD be taken is logged with the prefix '(plan)' so the user can
    #     review the planned changes before running '-apply'.

    # print begin
    mode_label = "plan" if dry_run else "apply"
    print_log("info", f"starting {mode_label}{' (dry-run, no changes will be applied)' if dry_run else ''}")

    # get JSON config file
    config = load_json_file(filepath)
    if config is None:
        return 1

    _inv_defaults       = get_inventory_defaults()
    _purge_days_primary = str(_inv_defaults.get("purge_retention_primary", "30"))
    _purge_days_initial = str(_inv_defaults.get("purge_retention_initial", "10"))

    bbdd_engine = config["bbdd_engine"]

    # global alignment width for all ':' separators in output
    _W = 38

    # ── Dispatch by request_type ─────────────────────────────────────────────────────────
    # CREATE        -> falls through to the existing creation flow below, EXCEPT for
    #                  replication_type=FULL which uses its own orchestrator (different step
    #                  order: replicat-first, consistency SCN from extract report, then IL extracts +
    #                  IL replicats — see _apply_create_full for details).
    # DELETE        -> handled by _apply_delete_request() (placeholder for now); returns early.
    # STATUS        -> read-only: shows the current state of every OGG process handling the
    #                  requested replication (delegates to status.show_status). No CREATE /
    #                  DELETE side-effects regardless of dry_run.
    # UPDATE        -> handled by _apply_update_request(); updates the COLMAP clause in the
    #                  primary replicat MAP lines (new/expanded column lists). The extract is
    #                  never modified — it always captures all columns. Returns early.
    request_type = config.get("request_type", "CREATE")
    print_log("info", "")
    print_log("info", f"====== REQUEST APPLY ======")
    print_log("info", f"request_type is {request_type}...")
    if request_type == "DELETE":
        return _apply_delete_request(config, dry_run=dry_run)
    if request_type == "STATUS":
        from status import show_status
        return show_status(config)
    if request_type == "UPDATE":
        return _apply_update_request(config, dry_run=dry_run)
    if request_type == "CREATE" and bbdd_engine == "ORACLE" and (config.get("replication_type") or "").upper() == "FULL":
        return _apply_create_full(config, dry_run=dry_run)

    ############## Oracle
    if bbdd_engine == "ORACLE":
        print_log("info", "database engine is ORACLE, preparing replication configuration for Oracle Goldengate ...")

        # get replication type from config 
        rep_type = config["replication_type"]

        # check replication type
        if rep_type not in ("ONLINE", "FULL", "INITIAL_LOAD"):
            print_log("error", f"unknown replication_type: {rep_type}")
            return 1

        # map replication_type -> extract types to configure
        #   FULL         -> integrated + initial_load
        #   ONLINE       -> integrated
        #   INITIAL_LOAD -> initial_load
        needs_integrated   = rep_type in ("FULL", "ONLINE")
        needs_initial_load = rep_type in ("FULL", "INITIAL_LOAD")
        extract_types      = [t for t, needed in [("integrated", needs_integrated), ("initial_load", needs_initial_load)] if needed]
        print_log("info", f"replication_type is {rep_type}, configuring extract type(s): {', '.join(extract_types)}")

        # Accumulators for the unified Final Summary printed at the very end of the Oracle block.
        # Declared up-front so they're available regardless of which sub-block (integrated / initial_load) executed.
        primary_results          = []   # primary (integrated) extracts created/modified: {dep, ext_name, action}
        primary_replicat_results = []   # primary replicats created/reused: {dep, ext_name, action}
        il_results               = []   # initial_load extracts outcome: {table, process, status, elapsed, is_ok}
        il_replicat_results      = []   # initial_load replicats outcome (Step 3/3 of legacy INITIAL_LOAD)

        # ─── Minimum Requirements pre-flight ────────────────────────────────────────────────
        print_log("info", "")
        print_log("info", "====== MINIMUM REQUIREMENTS ======")

        # ----- Pre-flight: the managed-process profile referenced when creating extracts/replicats -----
        # must already exist on every deployment we will touch. We fail-fast here so that we don't
        # discover the missing profile half-way through (after some extracts have been created).
        # Extract deployments (_E<digits>_) host primary + initial_load extracts; Replicat deployments
        # (_R<digits>_) host the primary replicat — both need the profile.
        _MPP_NAME = "Critical"
        # for the pre-flight we need all deployments that match ANY of the load_types involved
        # in this replication (online, initial_load, or both for FULL).
        all_oracle_deps = get_matching_deployments(config["env"], bbdd_engine, load_types=load_types_for_replication(rep_type))
        deps_for_mpp_check = []
        if needs_integrated or needs_initial_load:
            deps_for_mpp_check += [n for n, _ in all_oracle_deps if re.search(r"_E\d+_", n)]
        if needs_integrated:
            deps_for_mpp_check += [n for n, _ in all_oracle_deps if re.search(r"_R\d+_", n)]
        deps_for_mpp_check = sorted(set(deps_for_mpp_check))

        if deps_for_mpp_check:
            print_log("info", f"verifying managed-process-profile '{_MPP_NAME}' exists on {len(deps_for_mpp_check)} deployment(s) ...")
            mpp_missing = []
            for _dep in deps_for_mpp_check:
                mp_ok, mp_exists, mp_msg = managed_process_profile_exists(_dep, _MPP_NAME)
                if not mp_ok:
                    print_log("error", f"{'managed-process profile check':<{_W - 1}}: ERROR  ({_dep}: {mp_msg})")
                    mpp_missing.append((_dep, f"could not query profiles ({mp_msg})"))
                elif not mp_exists:
                    print_log("error", f"{'managed-process profile check':<{_W - 1}}: ERROR  ({_dep}: profile '{_MPP_NAME}' not found)")
                    mpp_missing.append((_dep, f"profile '{_MPP_NAME}' not found"))
                else:
                    print_log("info", f"{'managed-process profile check':<{_W}}: OK  ({_dep}: profile '{_MPP_NAME}' exists)")
            if mpp_missing:
                print_log("error", f"aborting {mode_label}: managed-process-profile '{_MPP_NAME}' missing on {len(mpp_missing)} deployment(s); create it first with 'oggctl managed-process-profile create {_MPP_NAME} --deployment <name> ...'")
                return 1

        # ─── Credential alias pre-flight ─────────────────────────────────────────────────────
        _bbdd_name_pf   = config["bbdd_name"]
        _env_pf         = config["env"]
        _bbdd_conn_pf   = get_db_connection(f"{_bbdd_name_pf}_{_env_pf}")
        if not _bbdd_conn_pf:
            print_log("error", f"connection '{_bbdd_name_pf}_{_env_pf}' not found in db_connections.yaml")
            return 1
        _extract_deps_pf  = sorted(set(n for n, _ in all_oracle_deps if re.search(r"_E\d+_", n)))
        _oggctl_path_pf   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl", "oggctl.py")
        _userid_prim_pf_o = f"{_bbdd_conn_pf.get('db_user', '')}@{_bbdd_conn_pf.get('conn_string_prim', '')}"
        _userid_stby_pf_o = f"{_bbdd_conn_pf.get('db_user', '')}@{_bbdd_conn_pf.get('conn_string_stby', '')}"
        _alias_prim_pf    = _bbdd_conn_pf.get("conn_name_prim") or f"{_bbdd_name_pf}_PRIM_{_env_pf}"
        _alias_stby_pf    = _bbdd_conn_pf.get("conn_name_stby") or f"{_bbdd_name_pf}_STBY_{_env_pf}"
        for _alias_pf, _userid_pf_o, _req_pf in [
            (_alias_prim_pf, _userid_prim_pf_o, bool(_bbdd_conn_pf.get("conn_string_prim"))),
            (_alias_stby_pf, _userid_stby_pf_o, bool(_bbdd_conn_pf.get("conn_string_stby"))),
        ]:
            if not _req_pf or not _extract_deps_pf:
                continue
            print_log("info", f"verifying credential alias '{_alias_pf}' on {len(_extract_deps_pf)} deployment(s) ...")
            for _dep in _extract_deps_pf:
                _ok = _ensure_credential_alias(_oggctl_path_pf, _alias_pf, _userid_pf_o,
                                               _bbdd_conn_pf.get("password_credential", ""), _dep, dry_run, _W)
                if not _ok:
                    print_log("error", f"aborting {mode_label}: credential alias '{_alias_pf}' failed on '{_dep}'")
                    return 1

        # ─── Kafka / S3 target pre-flight ────────────────────────────────────────────────────
        _kafka_target_name = None
        if (config.get("target_engine") or "").upper() == "KAFKA":
            _kafka_tc_pf     = get_target_connection_by_cluster_name(config.get("kafka_cluster_name", ""), config["env"], identity_pool=config.get("kafka_identity_pool", ""))
            if not _kafka_tc_pf:
                print_log("error", f"Kafka target not found for cluster_name='{config.get('kafka_cluster_name')}', identity_pool='{config.get('kafka_identity_pool')}', env='{config['env']}' in target_connections.yaml")
                return 1
            _kafka_target_name = _kafka_tc_pf.get("_key", "")
            _kafka_rep_deps_pf = sorted(set(n for n, _ in all_oracle_deps if re.search(r"_R\d+_", n)))
            print_log("info", f"verifying Kafka target prerequisites for '{_kafka_target_name}' on {len(_kafka_rep_deps_pf)} deployment(s) ...")
            if not _ensure_kafka_target_prereqs(_oggctl_path_pf, _kafka_target_name, _kafka_tc_pf, _kafka_rep_deps_pf, dry_run, _W):
                print_log("error", f"aborting {mode_label}: Kafka target prerequisites failed for '{_kafka_target_name}'")
                return 1
        elif (config.get("target_engine") or "").upper() == "AWS-S3":
            _s3_tc_pf      = get_target_connection_by_bucket(config.get("s3_bucket_name") or "") or {}
            _s3_rep_deps_pf = sorted(set(n for n, _ in all_oracle_deps if re.search(r"_R\d+_", n)))
            print_log("info", f"verifying S3 target prerequisites on {len(_s3_rep_deps_pf)} deployment(s) ...")
            if not _ensure_s3_target_prereqs(_oggctl_path_pf, _s3_tc_pf, _s3_rep_deps_pf, dry_run, _W):
                print_log("error", f"aborting {mode_label}: S3 target prerequisites failed")
                return 1

        ########## INTEGRATED extract (CDC capture for ONLINE / FULL)
        if needs_integrated:
            # FULL is dispatched to _apply_create_full earlier, so this 'integrated' block
            # only ever runs for ONLINE in the legacy flow. Header + step labels mirror the
            # FULL flow's wording so the operator sees a uniform layout across replication
            # types ('Step N/M' progression with a per-flow header at the top).
            print_log("info", "")
            print_log("info", f"====== CREATE flow for replication_type=ONLINE (engine={bbdd_engine}, bbdd={config['bbdd_name']}, env={config['env']}) ======")
            print_log("info", "")
            print_log("info", "------ Step 1/2: PRIMARY EXTRACT ------")

            # Helper: after (re)starting a primary extract, perform two report-error checks
            # (single check 30s after start) to catch any OGG error emitted shortly after startup
            # (e.g. PRM syntax errors, DB-side failures). Returns True if BOTH checks pass.
            # If the report cannot be read, the check is treated as inconclusive (returns True
            # with a warning) so transient API hiccups don't block the apply.
            def _verify_primary_no_errors(dep_name, ext_name, after_label):
                if dry_run:
                    print_log("info", f"(plan) would verify '{ext_name}' for errors (skipped in dry-run)")
                    return True
                label    = f"verifying '{ext_name}' for errors"
                since_dt = datetime.now() - timedelta(seconds=10)
                for wait_s, round_label in [(10, "after 10s")]:
                    time.sleep(wait_s)
                    rpt_ok, has_err, err_lines, err_msg = check_extract_report_for_errors(dep_name, ext_name, since_dt=since_dt)
                    if not rpt_ok:
                        print_log("warning", f"{label:<{_W}}: WARNING  ({round_label}, report unreadable: {err_msg})")
                        continue
                    if has_err:
                        first_err = err_lines[0].strip()[:140] if err_lines else "unknown"
                        more      = f", +{len(err_lines) - 1} more" if len(err_lines) > 1 else ""
                        print_log("error", f"{label:<{_W - 1}}: ERROR  ({round_label}, {len(err_lines)} error(s) found: {first_err}{more})")
                        for line in err_lines[:10]:
                            print_log("error", f"  - {line}")
                        if len(err_lines) > 10:
                            print_log("error", f"  ... and {len(err_lines) - 10} more error line(s) (truncated)")
                        return False
                    print_log("info", f"{label:<{_W}}: OK  ({round_label}, no errors found)")
                return True

            # 1) check whether an Extract already exists for this source BBDD on each Extract deployment.
            #    Detection matches the Extract's USERIDALIAS to the credential alias of this BBDD
            #    (convention: <BBDD_NAME>_PRIM_<ENV>). Per deployment one of two decisions:
            #       - REUSE  -> add the new tables to the existing Extract's TABLE list
            #       - CREATE -> generate a new Extract from template/parameter file
            print_log("info", "looking for existing Extract associated with this BBDD ...")

            # primary (integrated) extract deployments -> load_type=online
            deployments  = get_matching_deployments(config["env"], bbdd_engine, load_types=["online"])
            extract_deps = [name for name, _ in deployments if re.search(r"_E\d+_", name)]
            if not extract_deps:
                print_log("error", f"no Extract deployment (_E<digits>_) found for env={config['env']}, engine={bbdd_engine}")
                return 1

            existing = get_extracts_for_bbdd(extract_deps, config["bbdd_name"], config["env"])

            extracts_to_reuse   = []   # list of (deployment_name, extract_name)
            deps_needing_create = []   # list of deployment_name
            integrated_started  = []   # (dep, ext) pairs known to be started (used later as a gate for initial_load)

            for dep_name in extract_deps:
                found = existing.get(dep_name, [])
                if found:
                    for ext_name in found:
                        extracts_to_reuse.append((dep_name, ext_name))
                        print_log("info", f"Updating existing Extract : '{ext_name}' on deployment '{dep_name}'")
                else:
                    deps_needing_create.append(dep_name)
                    print_log("info", f"Creating new Extract on deployment '{dep_name}' ...")

            print_log("info", f"{'extracts to reuse':<{_W}}: {len(extracts_to_reuse)}")
            print_log("info", f"{'deployments needing new extract':<{_W}}: {len(deps_needing_create)}")

            # --- Reuse existing extracts: add new tables to their TABLE list ---
            if extracts_to_reuse:
                # Names only — extracts capture every column regardless of any column
                # allow-list in the JSON input (the COLMAP clause lives on the replicat).
                config_table_names, _config_table_columns_unused_for_extract = _parse_tables_from_config(config)
                # Audit TAGS comment appended to every newly-added TABLE line (mirrors the comment
                # used when creating an extract from scratch). Computed once per apply.
                _reuse_audit_comment = "TAGS " + json.dumps({
                    "requested_by":  config.get("user_name", ""),
                    "cost_center":   config.get("cost_center", ""),
                    "change_number": config.get("change_number", ""),
                    "date":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }, separators=(",", ":"))
                for dep_name, ext_name in extracts_to_reuse:
                    if dry_run:
                        print_log("info", f"(plan) would add tables to existing extract '{ext_name}' @ '{dep_name}' with audit TAGS: {', '.join(config_table_names)}")
                        ok, msg = True, "(plan) skipped"
                    else:
                        print_log("info", f"adding tables to existing extract '{ext_name}' @ '{dep_name}' ...")
                        ok, msg = add_tables_to_extract(dep_name, ext_name, config_table_names, comment=_reuse_audit_comment)
                    print_log("info" if ok else "error", f"{'extract add-tables':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({ext_name}@{dep_name}: {msg})")
                    if ok:
                        # verify the report has no new errors after the restart triggered by add-tables
                        report_ok = _verify_primary_no_errors(dep_name, ext_name, "add-tables (stop+restart)")
                        if report_ok:
                            integrated_started.append((dep_name, ext_name))
                            primary_results.append({"dep": dep_name, "ext_name": ext_name, "action": "reuse (add tables)"})
                        else:
                            # still record in primary_results so the final summary shows the failure,
                            # but do NOT add to integrated_started (gate must not wait on a broken extract).
                            primary_results.append({"dep": dep_name, "ext_name": ext_name, "action": "reuse (add tables, errors in report)"})

            # --- Create new extracts where missing ---
            if deps_needing_create:
                # naming/connection conventions
                bbdd_name      = config["bbdd_name"]
                env            = config["env"]
                trail_name     = "pr"                          # integrated/primary trail file name (per convention)
                bbdd_conn      = get_db_connection(f"{bbdd_name}_{env}")
                if not bbdd_conn:
                    print_log("error", f"connection '{bbdd_name}_{env}' not found in db_connections.yaml")
                    return 1
                useridalias    = bbdd_conn.get("conn_name_prim") or f"{bbdd_name}_PRIM_{env}"
                source_pdb     = bbdd_conn.get("pdb", "")
                cdc_seq        = bbdd_conn.get("cdc_seq", "")

                # cdc_seq is mandatory for the extract naming convention
                if not cdc_seq:
                    print_log("error", f"cdc_seq not defined in db_connections.yaml for '{bbdd_name}_{env}' (required for extract naming)")
                    return 1

                # build tables JSON payload — each entry carries an audit 'comment' that oggctl
                # appends to its TABLE line: 'TABLE SOE.X;  -- TAGS {"requested_by":"...","cost_center":"...","date":"..."}'
                # The payload after the 'TAGS ' prefix is a valid JSON object so it can be machine-parsed later.
                _audit_comment = "TAGS " + json.dumps({
                    "requested_by":  config.get("user_name", ""),
                    "cost_center":   config.get("cost_center", ""),
                    "change_number": config.get("change_number", ""),
                    "date":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }, separators=(",", ":"))
                # Primary extract TABLE payload (legacy ONLINE flow): NEVER carries a
                # 'columns' allow-list — extracts capture every column. Column restriction
                # is done in the replicat's MAP (see rep_tables_payload below).
                tables_payload = json.dumps(
                    [{"name": t.split("(")[0].strip().upper(), "comment": _audit_comment} for t in config.get("tables", [])]
                )

                # oggctl.py path (sibling repo)
                oggctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl", "oggctl.py")

                for dep_name in deps_needing_create:
                    # generate extract name following convention: EP<cdc_seq><NN><env_letter>
                    extract_name, msg = generate_extract_name(dep_name, env, cdc_seq, role="primary")
                    if extract_name is None:
                        print_log("error", f"could not generate extract name for deployment '{dep_name}': {msg}")
                        continue
                    print_log("info", f"creating Extract '{extract_name}' on deployment '{dep_name}' ...")

                    nn          = extract_name[4:6]  # NN sequential portion of the extract name (e.g. 'EP0100D' -> '00')
                    description = f"Extract-Primary-{bbdd_name}-{nn}-{env} (created by cdc-controller date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"
                    trail_path  = f"primary/{bbdd_name}/{extract_name}/{trail_name}"
                    cmd_create = [
                        sys.executable, oggctl_path,
                        "extract", "create", extract_name,
                        "--extract-type",            "integrated",
                        "--trail",                   trail_path,
                        "--tables-json",             tables_payload,
                        "--useridalias",             useridalias,
                        "--pdb",                     source_pdb,
                        "--description",             description,
                        "--managed-process-profile", "Critical",
                        "--deployment",              dep_name,
                    ]
                    if dry_run:
                        print_log("info", f"(plan) would CREATE extract '{extract_name}' @ '{dep_name}'  [trail={trail_path}, useridalias={useridalias}, pdb={source_pdb or '<none>'}, tables={len(config.get('tables', []))}]")
                        ok, last_line = True, "(plan) skipped"
                    else:
                        try:
                            result = subprocess.run(cmd_create, capture_output=True, text=True, timeout=120)
                            ok        = result.returncode == 0
                            output    = (result.stdout or "") + (result.stderr or "")
                            last_line = output.strip().splitlines()[-1] if output.strip() else f"return code {result.returncode}"
                        except Exception as e:
                            ok        = False
                            last_line = str(e)
                        print_log("info" if ok else "error", f"{'extract create':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({extract_name}@{dep_name}: {last_line})")
                    if not ok:
                        continue

                    # register the new integrated extract with the database
                    cmd_register = [
                        sys.executable, oggctl_path,
                        "extract", "register", extract_name,
                        "--deployment", dep_name,
                    ]
                    if dry_run:
                        print_log("info", f"(plan) would REGISTER extract '{extract_name}' @ '{dep_name}'")
                        ok, last_line = True, "(plan) skipped"
                    else:
                        try:
                            result = subprocess.run(cmd_register, capture_output=True, text=True, timeout=120)
                            ok        = result.returncode == 0
                            output    = (result.stdout or "") + (result.stderr or "")
                            last_line = output.strip().splitlines()[-1] if output.strip() else f"return code {result.returncode}"
                        except Exception as e:
                            ok        = False
                            last_line = str(e)
                        print_log("info" if ok else "error", f"{'extract register':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({extract_name}@{dep_name}: {last_line})")
                    if not ok:
                        continue

                    # start the newly created and registered extract
                    cmd_start = [
                        sys.executable, oggctl_path,
                        "extract", "start", extract_name,
                        "--deployment", dep_name,
                    ]
                    if dry_run:
                        print_log("info", f"(plan) would START extract '{extract_name}' @ '{dep_name}'")
                        ok, last_line = True, "(plan) skipped"
                    else:
                        try:
                            result = subprocess.run(cmd_start, capture_output=True, text=True, timeout=120)
                            ok        = result.returncode == 0
                            output    = (result.stdout or "") + (result.stderr or "")
                            last_line = output.strip().splitlines()[-1] if output.strip() else f"return code {result.returncode}"
                        except Exception as e:
                            ok        = False
                            last_line = str(e)
                        print_log("info" if ok else "error", f"{'extract start':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({extract_name}@{dep_name}: {last_line})")
                    if ok:
                        # verify the report has no errors shortly after the initial start
                        report_ok = _verify_primary_no_errors(dep_name, extract_name, "extract start")
                        if report_ok:
                            integrated_started.append((dep_name, extract_name))
                            primary_results.append({"dep": dep_name, "ext_name": extract_name, "action": "create"})
                        else:
                            # still record in primary_results so the final summary shows the failure,
                            # but do NOT add to integrated_started (gate must not wait on a broken extract).
                            primary_results.append({"dep": dep_name, "ext_name": extract_name, "action": "create (errors in report)"})
                        _purge_task_name = f"Purge-trail-{extract_name}"
                        if not dry_run and _purge_task_exists(oggctl_path, _purge_task_name, dep_name):
                            print_log("warning", f"{'purge task check':<{_W}}: WARNING  ({_purge_task_name}@{dep_name}: already exists — check retention)")
                        else:
                            _run_oggctl(
                                [sys.executable, oggctl_path, "task", "purge-trail", "create", _purge_task_name,
                                 "--trail", trail_path,
                                 "--keep-value", _purge_days_primary, "--keep-units", "days",
                                 "--schedule-value", "1", "--schedule-units", "days",
                                 "--critical", "--use-checkpoints", "--enabled",
                                 "--deployment", dep_name],
                                "purge task create", _purge_task_name, dep_name, dry_run, _W,
                            )


        ########## PRIMARY REPLICAT (consumer of the primary extract trail; only for ONLINE / FULL)
        # Per replicat deployment (matching pattern '_R<digits>_'):
        #   - REUSE  : an existing replicat already MAPs at least one of the configured tables
        #              (OGG wildcards in its MAP entries are honored — 'MAP *.*' counts as REUSE).
        #              The existing replicat is left untouched (no add-tables step for now).
        #   - CREATE : no existing replicat covers any of the configured tables; a fresh
        #              primary replicat is generated (RP<cdc_seq><NN><env_letter>), created
        #              via oggctl with --replicat-type classic, started, and its report file
        #              is verified for errors (10s + 10s — same pattern as primary extract).
        if needs_integrated:
            print_log("info", "")
            print_log("info", "------ Step 2/2: PRIMARY REPLICAT ------")

            # local helper: after starting a replicat, verify its report has no errors
            # (two rounds at 10s + 10s — mirror of _verify_primary_no_errors but reading the replicat report).
            def _verify_replicat_no_errors(dep_name, rep_name, after_label):
                if dry_run:
                    print_log("info", f"(plan) would verify '{rep_name}' for errors (skipped in dry-run)")
                    return True
                label    = f"verifying '{rep_name}' for errors"
                since_dt = datetime.now() - timedelta(seconds=10)
                for wait_s, round_label in [(10, "after 10s")]:
                    time.sleep(wait_s)
                    rpt_ok, has_err, err_lines, err_msg = check_replicat_report_for_errors(dep_name, rep_name, since_dt=since_dt)
                    if not rpt_ok:
                        print_log("warning", f"{label:<{_W}}: WARNING  ({round_label}, report unreadable: {err_msg})")
                        continue
                    if has_err:
                        first_err = err_lines[0].strip()[:140] if err_lines else "unknown"
                        more      = f", +{len(err_lines) - 1} more" if len(err_lines) > 1 else ""
                        print_log("error", f"{label:<{_W - 1}}: ERROR  ({round_label}, {len(err_lines)} error(s) found: {first_err}{more})")
                        for line in err_lines[:10]:
                            print_log("error", f"  - {line}")
                        if len(err_lines) > 10:
                            print_log("error", f"  ... and {len(err_lines) - 10} more error line(s) (truncated)")
                        return False
                    print_log("info", f"{label:<{_W}}: OK  ({round_label}, no errors found)")
                return True

            # 1) Load BBDD connection (cdc_seq + pdb)
            rep_bbdd_name = config["bbdd_name"]
            rep_env       = config["env"]
            rep_bbdd_conn = get_db_connection(f"{rep_bbdd_name}_{rep_env}")
            if not rep_bbdd_conn:
                print_log("error", f"connection '{rep_bbdd_name}_{rep_env}' not found in db_connections.yaml (required for replicat naming)")
                return 1
            rep_cdc_seq    = rep_bbdd_conn.get("cdc_seq", "")
            rep_source_pdb = rep_bbdd_conn.get("pdb", "")
            if not rep_cdc_seq:
                print_log("error", f"cdc_seq not defined in db_connections.yaml for '{rep_bbdd_name}_{rep_env}' (required for replicat naming)")
                return 1

            # 2) Map target_engine -> oggctl --target value
            target_engine_map = {
                "KAFKA":  "kafka_confluent",
                "AWS-S3": "s3",
                "ORACLE": "oracle",
            }
            target_engine_in  = (config.get("target_engine") or "").upper()
            rep_target        = target_engine_map.get(target_engine_in)

            # primary replicat deployments -> load_type=online (consume the primary extract trail)
            rep_deployments_all = get_matching_deployments(config["env"], bbdd_engine, load_types=["online"])
            replicat_deps       = [name for name, _ in rep_deployments_all if re.search(r"_R\d+_", name)]
            if not replicat_deps:
                print_log("warning", f"no Replicat deployment (_R<digits>_) found for env={config['env']}, engine={bbdd_engine}; skipping primary replicat setup")
            elif not rep_target:
                print_log("error", f"unknown target_engine '{config.get('target_engine')}' — cannot map to oggctl --target (valid: KAFKA, AWS-S3, ORACLE)")
            else:
                rep_config_tables, rep_config_table_columns = _parse_tables_from_config(config)

                # ── Compute the EXPECTED SOURCE TRAIL ahead of REUSE detection ────────────
                # REUSE is decided by whether an existing replicat consumes from THIS source
                # trail (not by whether it happens to map any of the requested tables). The
                # primary extract may be one that was just created OR an existing one we are
                # re-using; in either case its name is in 'integrated_started'.
                if not integrated_started:
                    print_log("warning", "no primary extract is started/reused; cannot determine the source trail for replicat decisions — skipping primary replicat section")
                    rep_trail_path = None
                    existing_rep   = {}
                else:
                    _, trail_ext_name = integrated_started[0]
                    rep_trail_path = f"primary/{rep_bbdd_name}/{trail_ext_name}/pr"
                    if len(integrated_started) > 1:
                        print_log("warning", f"multiple primary extracts started; using trail from '{trail_ext_name}' for replicat REUSE matching")
                    print_log("info", f"looking for existing Replicats consuming source trail '{rep_trail_path}' on {len(replicat_deps)} deployment(s) ...")
                    existing_rep = get_replicats_for_trail(replicat_deps, rep_trail_path)

                replicats_to_reuse  = []   # (dep, replicat_name)
                replicats_to_create = []   # (dep, generated_replicat_name)

                # Audit comment for new MAP lines added to an existing replicat (matches the
                # comment used at CREATE time so the PRM audit trail is consistent).
                _rep_reuse_audit_comment = "TAGS " + json.dumps({
                    "requested_by":  config.get("user_name", ""),
                    "cost_center":   config.get("cost_center", ""),
                    "change_number": config.get("change_number", ""),
                    "date":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }, separators=(",", ":"))

                for dep_name in replicat_deps:
                    if rep_trail_path is None:
                        # No primary extract trail available -> skip REUSE/CREATE entirely.
                        continue
                    matched = existing_rep.get(dep_name, [])
                    # Only reuse a replicat that matches the requested target type. A trail can have
                    # multiple replicats reading from it (e.g. one Kafka and one S3); reusing one
                    # with the wrong target would silently add tables to the wrong destination.
                    same_target = [r for r in matched
                                   if _replicat_target_matches(*get_replicat_target_info(dep_name, r), rep_target, config)]
                    if same_target:
                        chosen = same_target[0]
                        replicats_to_reuse.append((dep_name, chosen))
                        if len(same_target) > 1:
                            print_log("warning", f"multiple replicats with target '{rep_target}' consume trail '{rep_trail_path}' on '{dep_name}': {same_target} — picking '{chosen}' (first match)")
                        other_target = [r for r in matched if r not in same_target]
                        if other_target:
                            print_log("info", f"skipping {len(other_target)} replicat(s) with different target type on trail '{rep_trail_path}': {other_target}")
                        print_log("info", f"Reusing existing Replicat : '{chosen}' on deployment '{dep_name}' (matched by source trail + target '{rep_target}')")

                        # REUSE detected by source trail + target type. The PRM of the existing
                        # replicat may NOT yet contain all the requested tables — pass the full
                        # list to add_tables_to_replicat which honors wildcards and MAPEXCLUDE and
                        # only adds what's truly missing.
                        if dry_run:
                            print_log("info", f"(plan) would check / add missing MAP entries to existing replicat '{chosen}' @ '{dep_name}'")
                            ok_add, msg_add = True, "(plan) skipped"
                        else:
                            print_log("info", f"checking + adding missing tables to existing replicat '{chosen}' @ '{dep_name}' ...")
                            ok_add, msg_add = add_tables_to_replicat(dep_name, chosen, rep_config_tables, comment=_rep_reuse_audit_comment, column_map=rep_config_table_columns)
                        print_log("info" if ok_add else "error", f"{'replicat add-tables':<{_W - (0 if ok_add else 1)}}: {'OK' if ok_add else 'ERROR':<1}  ({chosen}@{dep_name}: {msg_add})")

                        if not ok_add:
                            primary_replicat_results.append({"dep": dep_name, "ext_name": chosen, "action": "reuse (add-tables failed)"})
                            continue

                        # Decide on the final action label:
                        #   - 'no new tables to add' -> pure REUSE (PRM untouched, replicat kept running)
                        #   - else -> we DID modify PRM and restart; verify report
                        # In dry-run we do NOT call add_tables_to_replicat, so msg_add is always
                        # '(plan) skipped' and we cannot know yet whether tables would actually be
                        # added. We report 'reuse (add tables)' to mirror the extract side's plan
                        # label — both processes will end up with the same intent annotation in
                        # the final summary. The real apply re-evaluates and picks the truthful
                        # label ('reuse' if nothing was missing, 'reuse (add tables)' otherwise).
                        if "no new tables to add" in msg_add:
                            primary_replicat_results.append({"dep": dep_name, "ext_name": chosen, "action": "reuse"})
                        else:
                            report_ok = _verify_replicat_no_errors(dep_name, chosen, "add-tables (stop+restart)")
                            if report_ok:
                                primary_replicat_results.append({"dep": dep_name, "ext_name": chosen, "action": "reuse (add tables)"})
                            else:
                                primary_replicat_results.append({"dep": dep_name, "ext_name": chosen, "action": "reuse (add tables, errors in report)"})
                    else:
                        # No replicat matches the requested target type on this trail.
                        # This covers two cases:
                        #   (a) no replicats on the trail at all
                        #   (b) replicats exist but all have a different target type → create a new one
                        if matched:
                            print_log("info",
                                      f"found {len(matched)} replicat(s) on trail '{rep_trail_path}' on '{dep_name}' "
                                      f"but none match target '{rep_target}' — creating new replicat")
                        new_rep_name, msg = generate_replicat_name(dep_name, rep_env, rep_cdc_seq, role="primary")
                        if new_rep_name is None:
                            print_log("error", f"could not generate replicat name for deployment '{dep_name}': {msg}")
                            continue
                        replicats_to_create.append((dep_name, new_rep_name))

                print_log("info", f"{'replicats to reuse':<{_W}}: {len(replicats_to_reuse)}")
                print_log("info", f"{'deployments needing new replicat':<{_W}}: {len(replicats_to_create)}")

                # 3) CREATE flow — uses the rep_trail_path computed at the top of this section
                if replicats_to_create:
                    if rep_trail_path is None:
                        # Already warned above; record as skipped in the summary.
                        for dep_name, new_rep_name in replicats_to_create:
                            primary_replicat_results.append({"dep": dep_name, "ext_name": new_rep_name, "action": "skipped (no primary extract trail)"})
                    else:
                        # tables payload — each entry carries the same audit 'comment' as the
                        # primary extract so 'MAP src, TARGET tgt;  -- TAGS {...}' shows the
                        # requester on every MAP line of the replicat PRM (compact JSON).
                        _rep_audit_comment = "TAGS " + json.dumps({
                            "requested_by":  config.get("user_name", ""),
                            "cost_center":   config.get("cost_center", ""),
                            "change_number": config.get("change_number", ""),
                            "date":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }, separators=(",", ":"))
                        # Primary replicat MAP payload (legacy ONLINE flow): include the
                        # 'columns' allow-list per table when the JSON input requested one
                        # (-> COLMAP clause in the PRM).
                        rep_tables_payload = json.dumps([
                            _build_replicat_table_entry(t, _rep_audit_comment, rep_config_table_columns)
                            for t in rep_config_tables
                        ])

                        # target-vars-json — only for Big Data targets (kafka / s3). Oracle target uses --useridalias instead.
                        rep_target_vars_json = None
                        _online_s3_tc        = None
                        if rep_target == "kafka_confluent":
                            schemas_in_config = sorted({t.split(".")[0].upper() for t in rep_config_tables if "." in t})
                            if len(schemas_in_config) > 1:
                                print_log("warning", f"multiple schemas in configured tables ({', '.join(schemas_in_config)}); using '{schemas_in_config[0]}' for replicat target-vars (single-schema templates)")
                            rep_schema = schemas_in_config[0] if schemas_in_config else ""
                            rep_target_vars_json = json.dumps({
                                "database":           rep_bbdd_name,
                                "schema":             rep_schema,
                                "kafka_topic_prefix": config.get("kafka_topic_prefix", ""),
                            })
                        elif rep_target == "s3":
                            _online_s3_tc = get_target_connection_by_bucket(config.get("s3_bucket_name") or "") or {}
                            _s3_path      = (config.get("s3_bucket_path") or "").strip()
                            rep_target_vars_json = json.dumps({
                                "database":             rep_bbdd_name,
                                "s3_bucket_name":          (config.get("s3_bucket_name") or "").strip(),
                                "bucket_path_prefix":   f"{_s3_path}/{rep_bbdd_name}" if _s3_path else rep_bbdd_name,
                                "s3_sts_assume_role":   (_online_s3_tc.get("sts_assume_role") or ""),
                                "s3_sts_session_name":  (_online_s3_tc.get("sts_session_name") or ""),
                                "s3_credentials_alias": (_online_s3_tc.get("ogg_alias_credential") or ""),
                                "aws_region":           (_online_s3_tc.get("aws_region") or "eu-west-1"),
                            })
                        # NOTE: --target=oracle is reachable via the engine map but requires --useridalias
                        # (alias to the TARGET DB). Not implemented in this iteration — would need an
                        # inventory entry for the target database.

                        oggctl_path_rep = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl", "oggctl.py")

                        for dep_name, new_rep_name in replicats_to_create:
                            nn              = new_rep_name[4:6]
                            _rep_target_label = "KAFKA" if rep_target == "kafka_confluent" else rep_target.upper()
                            rep_description = f"Replicat-Primary-{rep_bbdd_name}-{nn}-{_rep_target_label}-{rep_env} (created by cdc-controller date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"

                            cmd_rep_create = [
                                sys.executable, oggctl_path_rep,
                                "replicat", "create", new_rep_name,
                                "--replicat-type",           "classic",
                                "--target",                  rep_target,
                                "--trail",                   rep_trail_path,
                                "--tables-json",             rep_tables_payload,
                                "--description",             rep_description,
                                "--managed-process-profile", "Critical",
                                "--deployment",              dep_name,
                            ]
                            if rep_source_pdb:
                                cmd_rep_create.extend(["--pdb", rep_source_pdb])
                            if _kafka_target_name and rep_target == "kafka_confluent":
                                cmd_rep_create.extend(["--target-name", _kafka_target_name])
                            if rep_target_vars_json:
                                cmd_rep_create.extend(["--target-vars-json", rep_target_vars_json])

                            if dry_run:
                                print_log("info", f"(plan) would CREATE replicat '{new_rep_name}' @ '{dep_name}'  [target={rep_target}, trail={rep_trail_path}, pdb={rep_source_pdb or '<none>'}, tables={len(config.get('tables', []))}]")
                                ok, last_line = True, "(plan) skipped"
                            else:
                                print_log("info", f"creating Replicat '{new_rep_name}' on deployment '{dep_name}' ...")
                                try:
                                    result = subprocess.run(cmd_rep_create, capture_output=True, text=True, timeout=120)
                                    ok        = result.returncode == 0
                                    output    = (result.stdout or "") + (result.stderr or "")
                                    last_line = output.strip().splitlines()[-1] if output.strip() else f"return code {result.returncode}"
                                except Exception as e:
                                    ok        = False
                                    last_line = str(e)
                                print_log("info" if ok else "error", f"{'replicat create':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({new_rep_name}@{dep_name}: {last_line})")
                            if not ok:
                                primary_replicat_results.append({"dep": dep_name, "ext_name": new_rep_name, "action": "create failed"})
                                continue

                            # START
                            cmd_rep_start = [
                                sys.executable, oggctl_path_rep,
                                "replicat", "start", new_rep_name,
                                "--deployment", dep_name,
                            ]
                            if dry_run:
                                print_log("info", f"(plan) would START replicat '{new_rep_name}' @ '{dep_name}'")
                                ok, last_line = True, "(plan) skipped"
                            else:
                                try:
                                    result = subprocess.run(cmd_rep_start, capture_output=True, text=True, timeout=120)
                                    ok        = result.returncode == 0
                                    output    = (result.stdout or "") + (result.stderr or "")
                                    last_line = output.strip().splitlines()[-1] if output.strip() else f"return code {result.returncode}"
                                except Exception as e:
                                    ok        = False
                                    last_line = str(e)
                                print_log("info" if ok else "error", f"{'replicat start':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({new_rep_name}@{dep_name}: {last_line})")
                            if not ok:
                                primary_replicat_results.append({"dep": dep_name, "ext_name": new_rep_name, "action": "start failed"})
                                continue

                            # VERIFY report has no errors after startup
                            report_ok = _verify_replicat_no_errors(dep_name, new_rep_name, "replicat start")
                            if report_ok:
                                primary_replicat_results.append({"dep": dep_name, "ext_name": new_rep_name, "action": "create"})
                            else:
                                primary_replicat_results.append({"dep": dep_name, "ext_name": new_rep_name, "action": "create (errors in report)"})


        ########## Gate: wait for integrated extracts to be RUNNING before launching initial_load
        if needs_integrated and needs_initial_load and integrated_started:
            if dry_run:
                print_log("info", f"(plan) would wait for {len(integrated_started)} integrated extract(s) to reach RUNNING state before initial_load (skipped in dry-run)")
            else:
                for dep_name, ext_name in integrated_started:
                    ok, status = wait_for_extract_running(dep_name, ext_name, max_wait=60, poll_interval=5)
                    print_log("info" if ok else "error", f"{'extract status':<{_W - (0 if ok else 1)}}: {'RUNNING' if ok else status:<1}  ({ext_name}@{dep_name})")
                    if not ok:
                        print_log("error", f"integrated extract '{ext_name}' on '{dep_name}' is not RUNNING ({status}); aborting initial_load")
                        return 1


        ########## INITIAL_LOAD extract (table size analysis + create initial_load extract for INITIAL_LOAD)
        # FULL is dispatched to _apply_create_full earlier, so this 'initial_load' block
        # only ever runs for INITIAL_LOAD-only in the legacy flow. Header + step labels
        # mirror the FULL flow's wording so the operator sees a uniform layout across
        # replication types ('Step N/M' progression with a per-flow header at the top).
        if needs_initial_load:
            print_log("info", "")
            print_log("info", f"====== CREATE flow for replication_type=INITIAL_LOAD (engine={bbdd_engine}, bbdd={config['bbdd_name']}, env={config['env']}) ======")
            print_log("info", "")
            print_log("info", "------ Pre-step: initial_load table-size analysis ------")
            # Prepare Oracle connection info (init client + read credentials/DSNs from db_connections.yaml + password from env)
            ora = prepare_oracle_connection(config["bbdd_name"], config["env"])
            if ora is None:
                return 1
            db_password = ora["db_password"]
            db_pdb      = ora["db_pdb"]
            _, db_user_prim, dsn_prim_string = ora["dsns_to_check"][0]

            # Connect to PRIMARY for table size analysis (covered by the 'Pre-step' header above)
            print_log("info", f"connecting to Oracle Database (primary) ...")
            try:
                conn = oracledb.connect(user=db_user_prim, password=db_password, dsn=dsn_prim_string)
            except Exception as e:
                print_log("error", f"Oracle Database (primary) connection failed: {e}")
                return 1
            cursor = conn.cursor()

            # switch to PDB if multitenant
            cursor.execute("SELECT NAME, CDB FROM v$database")
            db_name, cdb_flag = cursor.fetchone()
            is_cdb = cdb_flag == "YES"
            if is_cdb and db_pdb:
                print_log("info", f"multitenant database detected, switching session to PDB : {db_pdb}")
                cursor.execute(f"ALTER SESSION SET CONTAINER = {db_pdb}")
            else:
                print_log("info", f"non-cdb database detected, connected to  : {db_name}")

            # analyze tables size
            print_log("info", f"analyzing tables size ...")

            # parse configured tables from config (drop column part: "SCHEMA.TABLE(...)" -> "SCHEMA.TABLE")
            config_tables = [t.split("(")[0].strip().upper() for t in config.get("tables", [])]
            if config_tables:
                # build (owner, table) bind pairs
                binds        = {}
                placeholders = []
                for i, t in enumerate(config_tables):
                    own, tab = t.split(".", 1)
                    binds[f"o{i}"] = own
                    binds[f"n{i}"] = tab
                    placeholders.append(f"(:o{i},:n{i})")

                # query table + LOB segment sizes (GB) + estimated row count per configured table
                sql = (
                    "SELECT OWNER, TABLE_NAME, NVL(TABLE_SEGS_GB, 0) AS TABLE_SEGS_GB, NVL(LOB_SEGS_GB, 0) AS LOB_SEGS_GB, "
                    "       (NVL(TABLE_SEGS_GB, 0) + NVL(LOB_SEGS_GB, 0)) AS TOTAL_GB, NVL(EST_ROWS, 0) AS ESTIMATED_ROWS "
                    "FROM ( "
                    "    SELECT t.OWNER, "
                    "           t.TABLE_NAME, "
                    "           (SELECT ROUND(SUM(tab_seg.BYTES)/1024/1024/1024, 1) "
                    "              FROM dba_segments tab_seg "
                    "             WHERE tab_seg.OWNER = t.OWNER AND tab_seg.SEGMENT_NAME = t.TABLE_NAME) AS TABLE_SEGS_GB, "
                    "           (SELECT ROUND(SUM(sl.BYTES)/1024/1024/1024, 1) "
                    "              FROM dba_segments sl, dba_lobs L "
                    "             WHERE L.OWNER = t.OWNER AND L.TABLE_NAME = t.TABLE_NAME AND sl.SEGMENT_NAME = L.SEGMENT_NAME) AS LOB_SEGS_GB, "
                    "           t.NUM_ROWS AS EST_ROWS "
                    "      FROM dba_tables t "
                    f"     WHERE (t.OWNER, t.TABLE_NAME) IN ({','.join(placeholders)}) "
                    "     GROUP BY t.OWNER, t.TABLE_NAME, t.NUM_ROWS "
                    ") "
                    "ORDER BY TOTAL_GB DESC"
                )
                try:
                    cursor.execute(sql, binds)
                    sizes = {}
                    for owner, table, table_gb, lob_gb, _total_gb, est_rows in cursor.fetchall():
                        sizes[f"{owner}.{table}"] = (float(table_gb), float(lob_gb), int(est_rows or 0))
                except Exception as e:
                    print_log("error", f"failed to query table sizes: {e}")
                    cursor.close()
                    conn.close()
                    return 1

                # display order: tables found in DB (largest first per SQL ORDER BY), then any config tables missing in DB
                display_order = list(sizes.keys()) + [t for t in config_tables if t not in sizes]

                # parallel buckets by table total size (GB) — mirrors _apply_create_full
                def parallel_for(total_gb):
                    if total_gb <=  25: return 1
                    if total_gb <=  50: return 2
                    if total_gb <= 150: return 4
                    if total_gb <= 250: return 6
                    if total_gb <= 650: return 10
                    return 14

                # tabular output — same 6-column format as the FULL flow
                _C1, _C2, _C3, _C4, _C6, _C5 = _W + 10, 13, 11, 9, 14, 11
                sep = f"{'-'*_C1}  {'-'*_C2}  {'-'*_C3}  {'-'*_C4}  {'-'*_C6}  {'-'*_C5}"
                print_log("info", sep)
                print_log("info", f"{'OWNER.TABLE_NAME':<{_C1}}  {'TABLE_SEGS_GB':>{_C2}}  {'LOB_SEGS_GB':>{_C3}}  {'TOTAL_GB':>{_C4}}  {'ESTIMATED_ROWS':>{_C6}}  {'IL_EXTRACTS':>{_C5}}")
                print_log("info", sep)

                tables_size  = []
                total_tab_gb = 0.0
                total_lob_gb = 0.0
                total_rows   = 0
                for t in display_order:
                    tab_gb, lob_gb, est_rows = sizes.get(t, (0.0, 0.0, 0))
                    total_gb      = tab_gb + lob_gb
                    parallel      = parallel_for(total_gb)
                    total_tab_gb += tab_gb
                    total_lob_gb += lob_gb
                    total_rows   += est_rows
                    tables_size.append((t, total_gb, parallel))
                    level  = "info" if t in sizes else "warning"
                    suffix = "" if t in sizes else "  (table not found)"
                    print_log(level, f"{t:<{_C1}}  {tab_gb:>{_C2}.1f}  {lob_gb:>{_C3}.1f}  {total_gb:>{_C4}.1f}  {est_rows:>{_C6}}  {parallel:>{_C5}}{suffix}")
                print_log("info", sep)
                print_log("info", f"{'TOTAL':<{_C1}}  {total_tab_gb:>{_C2}.1f}  {total_lob_gb:>{_C3}.1f}  {total_tab_gb + total_lob_gb:>{_C4}.1f}  {total_rows:>{_C6}}")
                print_log("info", "")

            # ═══ Step 1/3: CONSISTENCY SCN ═══════════════════════════════════════════════════
            print_log("info", "")
            print_log("info", "------ Step 1/3: CONSISTENCY SCN ------")
            try:
                cursor.execute(
                    "SELECT MIN(SCN) AS INSTANTIATION_SCN "
                    "FROM ( "
                    "    SELECT MIN(START_SCN) AS SCN FROM gv$transaction where START_SCN > 0"
                    "    UNION ALL "
                    "    SELECT CURRENT_SCN FROM gv$database "
                    ")"
                )
                consistency_scn = cursor.fetchone()[0]
                print_log("info", f"{'consistency SCN (instantiation)':<{_W}}: {consistency_scn}")
            except Exception as e:
                print_log("error", f"failed to get consistency SCN: {e}")
                consistency_scn = None

            cursor.close()
            conn.close()

            # --- Create one initial_load extract PER TABLE on each matching Extract deployment ---
            #     Trail path convention: initial/<bbdd>/<schema>/<table>/<trail_name>
            #     Initial_load extracts prefer STBY (offload reads from primary); fall back to PRIM if STBY is not defined.
            il_bbdd_name      = config["bbdd_name"]
            il_env            = config["env"]
            il_trail_name     = "il"
            il_entry_key      = f"{il_bbdd_name}_{il_env}"
            il_source_conn    = get_db_connection(il_entry_key)
            if not il_source_conn:
                print_log("error", f"connection '{il_entry_key}' not found in db_connections.yaml (required for initial_load)")
                return 1

            il_stby_conn_string = il_source_conn.get("conn_string_stby", "")
            il_prim_alias       = il_source_conn.get("conn_name_prim") or f"{il_bbdd_name}_PRIM_{il_env}"
            il_stby_alias       = il_source_conn.get("conn_name_stby") or f"{il_bbdd_name}_STBY_{il_env}"
            if il_stby_conn_string:
                il_useridalias = il_stby_alias
            else:
                print_log("warning", f"STBY connection not defined in db_connections.yaml entry '{il_entry_key}', falling back to PRIM '{il_prim_alias}' for initial_load")
                il_useridalias = il_prim_alias

            il_source_pdb = il_source_conn.get("pdb", "")
            il_cdc_seq    = il_source_conn.get("cdc_seq", "")

            if not il_cdc_seq:
                print_log("error", f"cdc_seq not defined in db_connections.yaml for '{il_entry_key}' (required for initial_load extract naming)")
                return 1

            # consistency SCN is mandatory for initial_load (ensures point-in-time consistency with integrated capture)
            if not consistency_scn:
                print_log("error", "consistency SCN is not available; cannot create initial_load extract without it")
                return 1

            # discover Extract deployments and build path to oggctl
            # initial_load extract deployments -> load_type=initial_load
            il_deployments  = get_matching_deployments(config["env"], bbdd_engine, load_types=["initial_load"])
            il_extract_deps = [name for name, _ in il_deployments if re.search(r"_E\d+_", name)]
            if not il_extract_deps:
                print_log("error", f"no Extract deployment (_E<digits>_) found for env={config['env']}, engine={bbdd_engine}")
                return 1
            il_oggctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl", "oggctl.py")

            print_log("info", "")
            print_log("info", "------ Step 2/3: INITIAL_LOAD EXTRACTS ------")

            # ----- Build the job list (only metadata, no extracts created yet) -----
            # Iterate tables_size (ordered by total_gb DESC) so the biggest tables are processed FIRST.
            # For parallel > 1, queue N jobs per table (one per partition).
            pending = []   # list of job dicts: {dep, full, schema, table, part_idx, total_parts, sqlpred}
            for dep_name in il_extract_deps:
                for full, total_gb, parallel_count in tables_size:
                    if "." not in full:
                        print_log("error", f"invalid table entry (expected SCHEMA.TABLE): {full}")
                        continue
                    schema, table = full.split(".", 1)
                    try:
                        parallel_count = int(parallel_count)
                    except (ValueError, TypeError):
                        parallel_count = 1
                    if parallel_count < 1:
                        parallel_count = 1
                    for part_idx in range(parallel_count):
                        if parallel_count == 1:
                            sqlpred = f"AS OF SCN {consistency_scn}"
                        else:
                            sqlpred = f"AS OF SCN {consistency_scn} WHERE ORA_HASH(ROWID,{parallel_count - 1})={part_idx}"
                        pending.append({
                            "dep":         dep_name,
                            "full":        full,
                            "schema":      schema,
                            "table":       table,
                            "part_idx":    part_idx,
                            "total_parts": parallel_count,
                            "sqlpred":     sqlpred,
                        })
            total_jobs = len(pending)

            # ----- Pool configuration: initial_load_parallel per BBDD -----
            max_parallel_raw = il_source_conn.get("initial_load_parallel")
            from_default     = "initial_load_parallel" in il_source_conn.get("_defaults_applied", set())
            try:
                max_parallel = int(max_parallel_raw) if max_parallel_raw is not None else 0
            except (ValueError, TypeError):
                max_parallel = 0
            if max_parallel < 1:
                max_parallel = max(1, total_jobs)
                print_log("warning", f"initial_load_parallel not defined or invalid in db_connections.yaml for '{il_entry_key}', defaulting to {max_parallel} (no effective limit)")
            elif from_default:
                print_log("warning", f"{'initial_load_parallel':<{_W - 3}}: {max_parallel}  (BBDD={il_bbdd_name}, total_processes={total_jobs}) — using default value from db_connections.yaml 'defaults' section (not set on entry '{il_entry_key}')")
            else:
                print_log("info", f"{'initial_load_parallel':<{_W}}: {max_parallel}  (BBDD={il_bbdd_name}, total_processes={total_jobs})")
            _il_concurrent = min(max_parallel, total_jobs)
            print_log("info", f"{'initial_load extract parallel':<{_W}}: {total_jobs} job(s) to launch — up to {_il_concurrent} will run concurrently (max_parallel={max_parallel})")

            # In dry-run mode, short-circuit the pool: simulate the work without creating/starting
            # any extract and without polling. We still have the full 'pending' list with table
            # names, partition counts and sqlpredicates — enough to print what WOULD be done.
            if dry_run:
                print_log("info", f"(plan) would launch {len(pending)} initial_load extract(s) on deployment(s) {sorted({j['dep'] for j in pending})} with max_parallel={max_parallel}")
                for j in pending:
                    part_label = "" if j["total_parts"] == 1 else f" part {j['part_idx'] + 1}/{j['total_parts']}"
                    pred       = j["sqlpred"]
                    pred_short = pred if len(pred) <= 80 else pred[:77] + "..."
                    print_log("info", f"(plan)   + {j['full']}{part_label}@{j['dep']}  [sqlpredicate: {pred_short}]")
                # consume 'pending' so the while-loop below sees an empty queue and is skipped;
                # 'il_results' intentionally remains empty in dry-run.
                pending = []

            # ----- Unified pool loop: CREATE + START on-demand respecting max_parallel -----
            # Each iteration:
            #  - fills capacity by creating + starting one extract per free slot
            #  - waits for at least one running job to finish before refilling
            # This avoids pre-creating hundreds of extracts upfront.
            poll_interval = 5  # seconds between status polls
            running       = []   # currently running jobs (each with 'ext_name' set)

            while pending or running:
                # fill capacity: create + start ONE extract per free slot
                while pending and len(running) < max_parallel:
                    job  = pending.pop(0)
                    full = job["full"]
                    part_suffix = "" if job["total_parts"] == 1 else f" part {job['part_idx'] + 1}/{job['total_parts']}"

                    # 1+2) generate name + CREATE with retry on collision
                    # The name is calculated at the moment of the call (not pre-calculated for all tables) so that
                    # concurrent runs (two users on the same BBDD) don't reserve the same NN. If two of them still
                    # race on the same NN, the loser gets "already exists" and we regenerate transparently.
                    max_name_attempts = 5
                    il_extract_name   = None
                    ok                = False
                    last_line         = ""
                    output            = ""
                    for attempt in range(1, max_name_attempts + 1):
                        il_extract_name, msg = generate_extract_name(job["dep"], il_env, il_cdc_seq, role="initial_load")
                        if il_extract_name is None:
                            print_log("error", f"could not generate initial_load extract name for deployment '{job['dep']}' (table {full}{part_suffix}): {msg}")
                            last_line = msg or "generate_extract_name returned None"
                            break

                        # IL extract names are 5 chars ('EI<NN><env>'); NN sits at 2-3.
                        il_nn = il_extract_name[2:4]
                        if job["total_parts"] == 1:
                            il_trail_path = f"initial/{il_bbdd_name}/{job['schema']}/{job['table']}/il"
                        else:
                            il_trail_path = f"initial/{il_bbdd_name}/{job['schema']}/{job['table']}/P{job['part_idx'] + 1}/il"
                        il_description = f"Extract-Initial_load-{il_bbdd_name}-{il_nn}-{config['project']}-{il_env} for table {full}{part_suffix} (created by cdc-controller date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"

                        attempt_label = "" if attempt == 1 else f" (attempt {attempt}/{max_name_attempts})"
                        print_log("info", f"creating initial_load Extract '{il_extract_name}' for table '{full}'{part_suffix} on deployment '{job['dep']}'{attempt_label} ...")
                        single_table_payload = json.dumps([{"name": full, "sqlpredicate": job["sqlpred"]}])
                        il_cmd_create = [
                            sys.executable, il_oggctl_path,
                            "extract", "create", il_extract_name,
                            "--extract-type",            "initial_load",
                            "--trail",                   il_trail_path,
                            "--tables-json",             single_table_payload,
                            "--useridalias",             il_useridalias,
                            "--pdb",                     il_source_pdb,
                            "--description",             il_description,
                            "--managed-process-profile", "Default",
                            "--deployment",              job["dep"],
                        ]
                        try:
                            result = subprocess.run(il_cmd_create, capture_output=True, text=True, timeout=120)
                            ok        = result.returncode == 0
                            output    = (result.stdout or "") + (result.stderr or "")
                            last_line = output.strip().splitlines()[-1] if output.strip() else f"return code {result.returncode}"
                        except Exception as e:
                            ok        = False
                            last_line = str(e)
                            output    = last_line

                        if ok:
                            break

                        # name collision (e.g. concurrent user grabbed the same NN) -> retry with a fresh name
                        if "already exists" in output.lower():
                            print_log("warning", f"extract name '{il_extract_name}' collision (likely concurrent run), regenerating ... [{last_line}]")
                            continue

                        # any other error -> stop retrying
                        break

                    print_log("info" if ok else "error", f"{'initial_load extract create':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({il_extract_name} table {full}{part_suffix}@{job['dep']}: {last_line})")
                    if not ok:
                        il_results.append({
                            "dep":     job["dep"],
                            "table":   f"{full}{part_suffix}",
                            "process": il_extract_name or "-",
                            "status":  f"ERROR (CREATE_FAILED: {last_line})",
                            "elapsed": "-",
                            "rows":    "-",
                            "is_ok":   False,
                            "trail":   None,
                        })
                        continue

                    # 3) START
                    il_cmd_start = [
                        sys.executable, il_oggctl_path,
                        "extract", "start", il_extract_name,
                        "--deployment", job["dep"],
                    ]
                    try:
                        result = subprocess.run(il_cmd_start, capture_output=True, text=True, timeout=120)
                        ok        = result.returncode == 0
                        output    = (result.stdout or "") + (result.stderr or "")
                        last_line = output.strip().splitlines()[-1] if output.strip() else f"return code {result.returncode}"
                    except Exception as e:
                        ok        = False
                        last_line = str(e)
                    print_log("info" if ok else "error", f"{'initial_load extract start':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({il_extract_name} table {full}{part_suffix}@{job['dep']}: {last_line})")
                    if ok:
                        job["ext_name"]   = il_extract_name
                        job["trail"]      = il_trail_path
                        _il_purge_task_name = f"Purge-trail-{il_extract_name}"
                        if not dry_run:
                            _del_ok = subprocess.run(
                                [sys.executable, il_oggctl_path, "task", "purge-trail", "delete", _il_purge_task_name,
                                 "--deployment", job["dep"]],
                                capture_output=True, text=True, timeout=30,
                            )
                            if _del_ok.returncode == 0:
                                print_log("warning", f"{'purge task check':<{_W}}: WARNING  ({_il_purge_task_name}@{job['dep']}: already existed — deleted before recreating)")
                        _run_oggctl(
                            [sys.executable, il_oggctl_path, "task", "purge-trail", "create", _il_purge_task_name,
                             "--trail", il_trail_path,
                             "--keep-value", _purge_days_initial, "--keep-units", "days",
                             "--schedule-value", "1", "--schedule-units", "days",
                             "--critical", "--use-checkpoints", "--enabled",
                             "--skip-trail-check",
                             "--deployment", job["dep"]],
                            "purge task create", _il_purge_task_name, job["dep"], dry_run, _W,
                        )
                        job["start_time"] = time.time()
                        running.append(job)
                    else:
                        il_results.append({
                            "dep":     job["dep"],
                            "table":   f"{full}{part_suffix}",
                            "process": il_extract_name,
                            "status":  f"ERROR (START_FAILED: {last_line})",
                            "elapsed": "-",
                            "rows":    "-",
                            "is_ok":   False,
                            "trail":   il_trail_path,
                        })

                if not running:
                    break   # nothing else to wait for

                # wait until at least one running job finishes (status leaves RUNNING/STARTING)
                print_log("info", f"{'initial_load extract pool':<{_W}}: {len(running)} running, {len(pending)} pending of {total_jobs} total (max_parallel={max_parallel}) — waiting for completion ...")

                # Fetch row counts for newly started jobs (after fill to avoid blocking new slots).
                # Counts are display-only (used for progress % lines).
                for job in running:
                    if job.get("_count_done"):
                        continue
                    _part_label  = "" if job["total_parts"] == 1 else f" part {job['part_idx']+1}/{job['total_parts']}"
                    _sql_preview = f"SELECT /*+ PARALLEL(4) */ count(1) FROM {job['full']} {job['sqlpred']}"
                    print_log("info", f"{'initial_load row count':<{_W}}: counting rows to be extracted for '{job['full']}{_part_label}' ({_sql_preview}) ...")
                    ok_cnt, total_rows, count_msg = count_rows_as_of_scn(il_bbdd_name, il_env, job["full"], job["sqlpred"])
                    job["total_rows"] = total_rows if (ok_cnt and total_rows is not None) else None
                    job["_count_done"] = True
                    if ok_cnt and total_rows is not None:
                        print_log("info", f"{'initial_load row count':<{_W}}: '{job['full']}{_part_label}' → {total_rows} rows")
                    else:
                        print_log("warning", f"could not get row count for '{job['full']}{_part_label}' ({count_msg}); progress will be shown without a percentage")
                ext_wait_start_at   = time.time()
                ext_next_check_at   = ext_wait_start_at + 30
                ext_last_log_at     = ext_wait_start_at
                _EXT_CHECK_INTERVAL = 30
                while True:
                    time.sleep(poll_interval)
                    finished = []
                    for job in running:
                        ok, status = get_extract_status_now(job["dep"], job["ext_name"])
                        s = str(status).upper()
                        if (not ok) or s not in ("RUNNING", "STARTING"):
                            job["end_time"] = time.time()
                            finished.append((job, status))

                    # Progress cadence: first 30 min → every 2 min; after 30 min → every 10 min
                    now = time.time()
                    if now >= ext_next_check_at:
                        _ext_elapsed      = now - ext_wait_start_at
                        _ext_log_interval = 120 if _ext_elapsed <= 1800 else 600
                        should_log        = (now - ext_last_log_at) >= _ext_log_interval
                        ext_next_check_at = now + _EXT_CHECK_INTERVAL
                        finished_ids      = {id(fj) for fj, _ in finished}
                        for job in running:
                            if id(job) in finished_ids:
                                continue
                            ok_p, applied, pmsg = get_il_extract_rows_applied(job["dep"], job["ext_name"], job["full"])
                            total = job.get("total_rows")
                            if ok_p and applied is not None and total:
                                pct    = (applied * 100.0 / total) if total > 0 else 0.0
                                detail = f"{applied}/{total} extracted, {pct:.1f}% completed (status=RUNNING)"
                            elif ok_p and applied is not None:
                                detail = f"{applied} rows extracted (total unknown) (status=RUNNING)"
                            else:
                                detail = f"STATS not available yet ({pmsg}) (status=RUNNING)"
                            if should_log:
                                _pl = "" if job["total_parts"] == 1 else f" part {job['part_idx']+1}/{job['total_parts']}"
                                _ok_rd, _rate, _delta = _get_ogg_rate_delta(il_oggctl_path, job["dep"], job["ext_name"], "extract")
                                _rate_detail = f", rate {_rate}, delta {_delta}" if _ok_rd else ""
                                if ok_p and applied is not None and total:
                                    detail = f"{applied}/{total} extracted{_rate_detail}, {pct:.1f}% completed (status=RUNNING)"
                                elif ok_p and applied is not None:
                                    detail = f"{applied} rows extracted{_rate_detail} (total unknown) (status=RUNNING)"
                                print_log("info", f"{'initial_load extract progress':<{_W}}: {job['ext_name']} for '{job['full']}{_pl}', {detail}")
                        if should_log:
                            ext_last_log_at = now

                    if finished:
                        for job, status in finished:
                            running.remove(job)
                            part_label = "" if job["total_parts"] == 1 else f" part {job['part_idx'] + 1}/{job['total_parts']}"
                            elapsed_s  = job["end_time"] - job.get("start_time", job["end_time"])
                            h, rem     = divmod(int(elapsed_s), 3600)
                            m, sec     = divmod(rem, 60)
                            elapsed    = f"{h}:{m:02d}:{sec:02d}"
                            job["elapsed_str"] = elapsed
                            s = str(status).upper()
                            row_table = f"{job['full']}{part_label}"
                            # Always log a final progress line before the finished message
                            _fin_ok_p, _fin_applied, _ = get_il_extract_rows_applied(job["dep"], job["ext_name"], job["full"])
                            _fin_total = job.get("total_rows")
                            if _fin_ok_p and _fin_applied is not None and _fin_total:
                                try:
                                    _fin_pct = (_fin_applied * 100.0 / _fin_total) if int(_fin_total) > 0 else 0.0
                                except (ValueError, TypeError):
                                    _fin_pct = 0.0
                                _fin_detail = f"{_fin_applied}/{_fin_total} extracted, {_fin_pct:.1f}% completed (status={status})"
                            elif _fin_ok_p and _fin_applied is not None:
                                _fin_detail = f"{_fin_applied} rows extracted (total unknown) (status={status})"
                            else:
                                _fin_detail = f"(status={status})"
                            print_log("info", f"{'initial_load extract progress':<{_W}}: {job['ext_name']} for '{job['full']}{part_label}', {_fin_detail}")
                            # STOPPED is ambiguous: it can mean natural completion, an OGG ABEND, or a user STOP.
                            # We disambiguate by inspecting the report (priority order):
                            #   1) report contains ABEND or "ERROR + OGG-NNNNN" lines  -> FAILED  (highest priority — overrides status)
                            #   2) report contains OGG-01021 (admin STOP)               -> USER_STOPPED
                            #   3) STOPPED with clean report                            -> OK (natural completion)
                            # Any non-STOPPED final state (ABENDED, FAILED, ...) is reported as ERROR; we still scan
                            # the report so the user sees the actual OGG error messages, not just the status string.
                            rpt_err_ok, has_errors, err_lines, rpt_err_msg = check_extract_report_for_errors(job["dep"], job["ext_name"])

                            if has_errors:
                                # priority 1: errors detected in report — failure regardless of the OGG status string
                                first_err = err_lines[0].strip()[:120] if err_lines else "unknown"
                                print_log("error", f"initial_load extract '{job['ext_name']}' for table '{job['full']}'{part_label}@{job['dep']} finished with {len(err_lines)} error(s) in its report (OGG status: {status}) [elapsed: {elapsed}]:")
                                for line in err_lines[:10]:
                                    print_log("error", f"  - {line.strip()}")
                                if len(err_lines) > 10:
                                    print_log("error", f"  ... and {len(err_lines) - 10} more error line(s) (truncated)")
                                il_results.append({"dep": job["dep"], "table": row_table, "process": job["ext_name"], "status": f"Error ({s}; report: {first_err})", "elapsed": elapsed, "rows": "-", "total_rows": job.get("total_rows"), "is_ok": False, "trail": job.get("trail")})
                            elif s == "STOPPED":
                                # no errors in report -> check user-stop marker, else natural completion
                                user_stopped, rpt_us_ok, _rpt_us_msg = extract_was_user_stopped(job["dep"], job["ext_name"])
                                if not rpt_err_ok and not rpt_us_ok:
                                    # report unreadable in BOTH checks — be conservative and mark as OK (with note)
                                    print_log("warning", f"initial_load extract '{job['ext_name']}' for table '{job['full']}'{part_label}@{job['dep']} stopped but report could not be read ({rpt_err_msg}); treating as completed [elapsed: {elapsed}]")
                                    il_results.append({"dep": job["dep"], "table": row_table, "process": job["ext_name"], "status": "OK (STOPPED, no errors)", "elapsed": elapsed, "rows": "-", "total_rows": job.get("total_rows"), "is_ok": True, "trail": job.get("trail")})
                                elif user_stopped:
                                    print_log("error", f"initial_load extract '{job['ext_name']}' for table '{job['full']}'{part_label}@{job['dep']} was stopped manually (OGG-01021 marker found in report) — NOT a successful completion [elapsed: {elapsed}]")
                                    il_results.append({"dep": job["dep"], "table": row_table, "process": job["ext_name"], "status": "Error (USER_STOPPED)", "elapsed": elapsed, "rows": "-", "total_rows": job.get("total_rows"), "is_ok": False, "trail": job.get("trail")})
                                else:
                                    _il_label = "initial_load extract finished"
                                    ok_rows, rows_count, rows_msg = get_il_extract_rows_from_report(job["dep"], job["ext_name"])
                                    if ok_rows:
                                        rows_label_ext = f", {rows_count} extracted"
                                        print_log("info", f"{_il_label:<{_W}}: OK  '{job['ext_name']}' for '{job['full']}'{part_label}@{job['dep']}{rows_label_ext} [elapsed: {elapsed}]")
                                        rows_field = str(rows_count)
                                    else:
                                        print_log("info",    f"{_il_label:<{_W}}: OK  '{job['ext_name']}' for '{job['full']}'{part_label}@{job['dep']} [elapsed: {elapsed}]")
                                        print_log("warning", f"could not obtain row count from '{job['ext_name']}.rpt': {rows_msg}")
                                        rows_field = "?"
                                    il_results.append({"dep": job["dep"], "table": row_table, "process": job["ext_name"], "status": "OK (STOPPED, no errors)", "elapsed": elapsed, "rows": rows_field, "total_rows": job.get("total_rows"), "is_ok": True, "trail": job.get("trail")})
                            else:
                                # non-STOPPED final state without parseable errors in the report (rare but possible)
                                print_log("error", f"initial_load extract '{job['ext_name']}' for table '{job['full']}'{part_label}@{job['dep']} finished with error status: {status} [elapsed: {elapsed}]")
                                il_results.append({"dep": job["dep"], "table": row_table, "process": job["ext_name"], "status": f"Error ({s})", "elapsed": elapsed, "rows": "-", "total_rows": job.get("total_rows"), "is_ok": False, "trail": job.get("trail")})
                        break   # exit inner loop to refill capacity

            print_log("info", f"all initial_load extracts completed for BBDD '{il_bbdd_name}'")

            # IL Extract intermediate summary — printed here so the operator can review extract
            # results before the IL Replicats step begins. Printed again in the Final Summary.
            _print_il_extract_summary(il_results, _W)

            # ═══ Step 3/3: INITIAL_LOAD REPLICATS ═══════════════════════════════════════════
            # Create + start one coordinated IL replicat per IL extract trail; wait for them
            # to consume the trail (auto-stop at 100%) before the Final Summary.
            print_log("info", "")
            print_log("info", "------ Step 3/3: INITIAL_LOAD REPLICATS ------")

            _il_rep_target_map = {"KAFKA": "kafka_confluent", "AWS-S3": "s3", "ORACLE": "oracle"}
            _il_rep_target     = _il_rep_target_map.get((config.get("target_engine") or "").upper())
            _il_rep_deps       = [n for n, _ in get_matching_deployments(config["env"], bbdd_engine, load_types=["initial_load"], proc_types=["replicat"])]
            _il_config_tables, _il_column_map = _parse_tables_from_config(config)
            _il_audit_comment = "TAGS " + json.dumps({
                "requested_by":  config.get("user_name", ""),
                "cost_center":   config.get("cost_center", ""),
                "change_number": config.get("change_number", ""),
                "date":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }, separators=(",", ":"))

            if not _il_rep_target:
                print_log("error", f"unknown target_engine '{config.get('target_engine')}' (valid: KAFKA, AWS-S3, ORACLE); cannot create initial_load replicats")
            else:
                _il_s3_tc = None
                if _il_rep_target == "s3":
                    _il_s3_tc = get_target_connection_by_bucket(config.get("s3_bucket_name") or "")
                _run_il_replicats_step(
                    tables_for_il        = _il_config_tables,
                    tables_size_all      = tables_size,
                    replicat_deps_il     = _il_rep_deps,
                    il_results           = il_results,
                    il_replicat_results  = il_replicat_results,
                    bbdd_name            = il_bbdd_name,
                    env                  = il_env,
                    cdc_seq              = il_cdc_seq,
                    rep_target           = _il_rep_target,
                    source_pdb           = il_source_pdb,
                    project              = config["project"],
                    kafka_topic_prefix   = config.get("kafka_topic_prefix", ""),
                    audit_comment        = _il_audit_comment,
                    config_table_columns = _il_column_map,
                    oggctl_path          = il_oggctl_path,
                    dry_run              = dry_run,
                    _W                   = _W,
                    plan_replicat_namer  = None,
                    s3_target_conn       = _il_s3_tc,
                    kafka_target_name    = _kafka_target_name if _il_rep_target == "kafka_confluent" else None,
                    s3_bucket_path       = config.get("s3_bucket_path", ""),
                )


        ########## Final Summary (Primary + initial_load)
        # Printed at the very end of the Oracle block so both tables appear together AFTER all
        # processing (creation, gate, initial_load pool) is done.

        # ---- Primary table (created/modified integrated extracts) ----
        if primary_results:
            pri_rows = []
            for pr in primary_results:
                ok_info, info = get_extract_info_now(pr["dep"], pr["ext_name"])
                if ok_info:
                    status     = str(info.get("status", "unknown"))
                    last_start = find_extract_last_started(info) or "-"
                    if last_start == "-" and isinstance(info, dict):
                        # diagnostic: helps adjust the lookup if OGG uses an unexpected field name
                        print_log("warning", f"[Primary summary] no startup-time field found for '{pr['ext_name']}'")
                else:
                    status     = f"unknown ({info.get('error', '')})"
                    last_start = "-"
                pri_rows.append({
                    "process":    pr["ext_name"],
                    "type":       "Extract (integrated)",
                    "action":     pr["action"],
                    "status":     status,
                    "last_start": str(last_start),
                })

            print_log("info", "")
            print_log("info", "====== Final Summary: Primary Extract ======")
            pri_deps = sorted({pr["dep"] for pr in primary_results})
            print_log("info", f"OGG Deployment : {', '.join(pri_deps) if pri_deps else '(unknown)'}")
            _P = max(len("PROCESS"),           max((len(r["process"])    for r in pri_rows), default=0))
            _T = max(len("TYPE"),              max((len(r["type"])       for r in pri_rows), default=0))
            _A = max(len("ACTION"),            max((len(r["action"])     for r in pri_rows), default=0))
            _S = max(len("STATUS"),            max((len(r["status"])     for r in pri_rows), default=0))
            _L = max(len("LAST STARTUP TIME"), max((len(r["last_start"]) for r in pri_rows), default=0))
            sep_line = f"{'-'*_P}  {'-'*_T}  {'-'*_A}  {'-'*_S}  {'-'*_L}"
            print_log("info", sep_line)
            print_log("info", f"{'PROCESS':<{_P}}  {'TYPE':<{_T}}  {'ACTION':<{_A}}  {'STATUS':<{_S}}  {'LAST STARTUP TIME':<{_L}}")
            print_log("info", sep_line)
            for r in pri_rows:
                print_log("info", f"{r['process']:<{_P}}  {r['type']:<{_T}}  {r['action']:<{_A}}  {r['status']:<{_S}}  {r['last_start']:<{_L}}")
            print_log("info", sep_line)

        # ---- Primary Replicat table (created/reused) ----
        if primary_replicat_results:
            # OGG REST stores the replicat mode under 'mode.type' on the resource endpoint.
            # 'classic' is internally called 'nonintegrated'; 'coordinated' stays as-is.
            _rep_mode_label = {"nonintegrated": "classic", "coordinated": "coordinated"}

            prep_rows = []
            for pr in primary_replicat_results:
                ok_info, info = get_replicat_info_now(pr["dep"], pr["ext_name"])
                if ok_info:
                    status     = str(info.get("status", "unknown"))
                    last_start = find_extract_last_started(info) or "-"
                    if last_start == "-" and isinstance(info, dict):
                        print_log("warning", f"[Primary Replicat summary] no startup-time field found for '{pr['ext_name']}'")
                    # derive the human-readable replicat type from the API response (mode.type)
                    mode      = info.get("mode")
                    mode_type = mode.get("type") if isinstance(mode, dict) else None
                    type_lbl  = _rep_mode_label.get(mode_type, mode_type or "unknown")
                else:
                    status     = f"unknown ({info.get('error', '')})"
                    last_start = "-"
                    type_lbl   = "unknown"
                prep_rows.append({
                    "process":    pr["ext_name"],
                    "type":       f"Replicat ({type_lbl})",
                    "action":     pr["action"],
                    "status":     status,
                    "last_start": str(last_start),
                })

            print_log("info", "")
            print_log("info", "====== Final Summary: Primary Replicat ======")
            prep_deps = sorted({pr["dep"] for pr in primary_replicat_results})
            print_log("info", f"OGG Deployment : {', '.join(prep_deps) if prep_deps else '(unknown)'}")
            _P = max(len("PROCESS"),           max((len(r["process"])    for r in prep_rows), default=0))
            _T = max(len("TYPE"),              max((len(r["type"])       for r in prep_rows), default=0))
            _A = max(len("ACTION"),            max((len(r["action"])     for r in prep_rows), default=0))
            _S = max(len("STATUS"),            max((len(r["status"])     for r in prep_rows), default=0))
            _L = max(len("LAST STARTUP TIME"), max((len(r["last_start"]) for r in prep_rows), default=0))
            sep_line = f"{'-'*_P}  {'-'*_T}  {'-'*_A}  {'-'*_S}  {'-'*_L}"
            print_log("info", sep_line)
            print_log("info", f"{'PROCESS':<{_P}}  {'TYPE':<{_T}}  {'ACTION':<{_A}}  {'STATUS':<{_S}}  {'LAST STARTUP TIME':<{_L}}")
            print_log("info", sep_line)
            for r in prep_rows:
                print_log("info", f"{r['process']:<{_P}}  {r['type']:<{_T}}  {r['action']:<{_A}}  {r['status']:<{_S}}  {r['last_start']:<{_L}}")
            print_log("info", sep_line)

        # ---- initial_load tables (extracts + replicats) ----
        # Reuse _print_full_summary so the IL Extract + IL Replicat tables look identical
        # to the FULL flow output (shared column widths, same headers). The two Primary
        # accumulators are empty in INITIAL_LOAD-only and the helper skips empty tables.
        if il_results or il_replicat_results:
            _print_full_summary([], [], il_results, il_replicat_results, _W)

    ############## Mongo
    elif bbdd_engine == "MONGO":
        print_log("info", "database engine is MONGO, preparing replication configuration for Oracle Goldengate ...")

        # MongoDB CDC with OGG uses change streams — only ONLINE replication is supported.
        rep_type = config["replication_type"]
        if rep_type != "ONLINE":
            print_log("error", f"replication_type '{rep_type}' is not supported for MONGO engine (only ONLINE is supported via change streams)")
            return 1

        print_log("info", f"replication_type is {rep_type}, configuring extract type(s): online")

        # Accumulators — identical shape to the Oracle block so the Final Summary code is the same.
        primary_results          = []   # primary extracts created/modified
        primary_replicat_results = []   # primary replicats created/reused

        # ─── Minimum Requirements pre-flight ────────────────────────────────────────────────
        print_log("info", "")
        print_log("info", "====== MINIMUM REQUIREMENTS ======")

        # ----- Pre-flight: managed-process profile must exist on every deployment -----
        _MPP_NAME        = "Critical"
        all_mongo_deps   = get_matching_deployments(config["env"], bbdd_engine, load_types=["online"])
        deps_for_mpp_check = sorted(set(
            [n for n, _ in all_mongo_deps if re.search(r"_E\d+_", n)] +
            [n for n, _ in all_mongo_deps if re.search(r"_R\d+_", n)]
        ))
        if deps_for_mpp_check:
            print_log("info", f"verifying managed-process-profile '{_MPP_NAME}' exists on {len(deps_for_mpp_check)} deployment(s) ...")
            mpp_missing = []
            for _dep in deps_for_mpp_check:
                mp_ok, mp_exists, mp_msg = managed_process_profile_exists(_dep, _MPP_NAME)
                if not mp_ok:
                    print_log("error", f"{'managed-process profile check':<{_W - 1}}: ERROR  ({_dep}: {mp_msg})")
                    mpp_missing.append((_dep, f"could not query profiles ({mp_msg})"))
                elif not mp_exists:
                    print_log("error", f"{'managed-process profile check':<{_W - 1}}: ERROR  ({_dep}: profile '{_MPP_NAME}' not found)")
                    mpp_missing.append((_dep, f"profile '{_MPP_NAME}' not found"))
                else:
                    print_log("info", f"{'managed-process profile check':<{_W}}: OK  ({_dep}: profile '{_MPP_NAME}' exists)")
            if mpp_missing:
                print_log("error", f"aborting {mode_label}: managed-process-profile '{_MPP_NAME}' missing on {len(mpp_missing)} deployment(s); create it first with 'oggctl managed-process-profile create {_MPP_NAME} --deployment <name> ...'")
                return 1

        # ─── Credential alias pre-flight ─────────────────────────────────────────────────────
        _bbdd_name_pf   = config["bbdd_name"]
        _env_pf         = config["env"]
        _bbdd_conn_pf   = get_db_connection(f"{_bbdd_name_pf}_{_env_pf}")
        if not _bbdd_conn_pf:
            print_log("error", f"connection '{_bbdd_name_pf}_{_env_pf}' not found in db_connections.yaml")
            return 1
        _extract_deps_pf = sorted(set(n for n, _ in all_mongo_deps if re.search(r"_E\d+_", n)))
        _oggctl_path_pf = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl", "oggctl.py")
        if _extract_deps_pf:
            _alias_pf     = _bbdd_conn_pf.get("conn_name_prim") or f"{_bbdd_name_pf}_PRIM_{_env_pf}"
            _mongo_cs     = _bbdd_conn_pf.get("conn_string_prim", "")
            if not (_mongo_cs.startswith("mongodb://") or _mongo_cs.startswith("mongodb+srv://")):
                _mongo_cs = f"mongodb+srv://{_mongo_cs}"
            _userid_pf = f"{_bbdd_conn_pf.get('db_user', '')}@{_mongo_cs}"
            print_log("info", f"verifying credential alias '{_alias_pf}' on {len(_extract_deps_pf)} deployment(s) ...")
            for _dep in _extract_deps_pf:
                _ok = _ensure_credential_alias(_oggctl_path_pf, _alias_pf, _userid_pf,
                                               _bbdd_conn_pf.get("password_credential", ""), _dep, dry_run, _W)
                if not _ok:
                    print_log("error", f"aborting {mode_label}: credential alias '{_alias_pf}' failed on '{_dep}'")
                    return 1

        # ─── Kafka / S3 target pre-flight ────────────────────────────────────────────────────
        _kafka_target_name = None
        if (config.get("target_engine") or "").upper() == "KAFKA":
            _kafka_tc_pf     = get_target_connection_by_cluster_name(config.get("kafka_cluster_name", ""), config["env"], identity_pool=config.get("kafka_identity_pool", ""))
            if not _kafka_tc_pf:
                print_log("error", f"Kafka target not found for cluster_name='{config.get('kafka_cluster_name')}', identity_pool='{config.get('kafka_identity_pool')}', env='{config['env']}' in target_connections.yaml")
                return 1
            _kafka_target_name = _kafka_tc_pf.get("_key", "")
            _kafka_rep_deps_pf = sorted(set(n for n, _ in all_mongo_deps if re.search(r"_R\d+_", n)))
            print_log("info", f"verifying Kafka target prerequisites for '{_kafka_target_name}' on {len(_kafka_rep_deps_pf)} deployment(s) ...")
            if not _ensure_kafka_target_prereqs(_oggctl_path_pf, _kafka_target_name, _kafka_tc_pf, _kafka_rep_deps_pf, dry_run, _W):
                print_log("error", f"aborting {mode_label}: Kafka target prerequisites failed for '{_kafka_target_name}'")
                return 1
        elif (config.get("target_engine") or "").upper() == "AWS-S3":
            _s3_tc_pf      = get_target_connection_by_bucket(config.get("s3_bucket_name") or "") or {}
            _s3_rep_deps_pf = sorted(set(n for n, _ in all_mongo_deps if re.search(r"_R\d+_", n)))
            print_log("info", f"verifying S3 target prerequisites on {len(_s3_rep_deps_pf)} deployment(s) ...")
            if not _ensure_s3_target_prereqs(_oggctl_path_pf, _s3_tc_pf, _s3_rep_deps_pf, dry_run, _W):
                print_log("error", f"aborting {mode_label}: S3 target prerequisites failed")
                return 1

        ########## Step 1/2: PRIMARY EXTRACT
        print_log("info", "")
        print_log("info", f"====== CREATE flow for replication_type=ONLINE (engine={bbdd_engine}, bbdd={config['bbdd_name']}, env={config['env']}) ======")
        print_log("info", "")
        print_log("info", "------ Step 1/2: PRIMARY EXTRACT ------")

        def _verify_mongo_extract_no_errors(dep_name, ext_name, after_label):
            if dry_run:
                print_log("info", f"(plan) would verify '{ext_name}' for errors (skipped in dry-run)")
                return True
            label    = f"verifying '{ext_name}' for errors"
            since_dt = datetime.now() - timedelta(seconds=10)
            for wait_s, round_label in [(10, "after 10s")]:
                time.sleep(wait_s)
                rpt_ok, has_err, err_lines, err_msg = check_extract_report_for_errors(dep_name, ext_name, since_dt=since_dt)
                if not rpt_ok:
                    print_log("warning", f"{label:<{_W}}: WARNING  ({round_label}, report unreadable: {err_msg})")
                    continue
                if has_err:
                    first_err = err_lines[0].strip()[:140] if err_lines else "unknown"
                    more      = f", +{len(err_lines) - 1} more" if len(err_lines) > 1 else ""
                    print_log("error", f"{label:<{_W - 1}}: ERROR  ({round_label}, {len(err_lines)} error(s) found: {first_err}{more})")
                    for line in err_lines[:10]:
                        print_log("error", f"  - {line}")
                    if len(err_lines) > 10:
                        print_log("error", f"  ... and {len(err_lines) - 10} more error line(s) (truncated)")
                    return False
                print_log("info", f"{label:<{_W}}: OK  ({round_label}, no errors found)")
            return True

        print_log("info", "looking for existing Extract associated with this BBDD ...")
        deployments  = get_matching_deployments(config["env"], bbdd_engine, load_types=["online"])
        extract_deps = [name for name, _ in deployments if re.search(r"_E\d+_", name)]
        if not extract_deps:
            print_log("error", f"no Extract deployment (_E<digits>_) found for env={config['env']}, engine={bbdd_engine}")
            return 1

        existing = get_extracts_for_bbdd(extract_deps, config["bbdd_name"], config["env"])

        extracts_to_reuse   = []
        deps_needing_create = []
        integrated_started  = []

        for dep_name in extract_deps:
            found = existing.get(dep_name, [])
            if found:
                for ext_name in found:
                    extracts_to_reuse.append((dep_name, ext_name))
                    print_log("info", f"Updating existing Extract : '{ext_name}' on deployment '{dep_name}'")
            else:
                deps_needing_create.append(dep_name)
                print_log("info", f"Creating new Extract on deployment '{dep_name}' ...")

        print_log("info", f"{'extracts to reuse':<{_W}}: {len(extracts_to_reuse)}")
        print_log("info", f"{'deployments needing new extract':<{_W}}: {len(deps_needing_create)}")

        # --- Reuse existing extracts ---
        if extracts_to_reuse:
            config_table_names, _ = _parse_tables_from_config(config)
            _reuse_audit_comment = "TAGS " + json.dumps({
                "requested_by":  config.get("user_name", ""),
                "cost_center":   config.get("cost_center", ""),
                "change_number": config.get("change_number", ""),
                "date":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }, separators=(",", ":"))
            for dep_name, ext_name in extracts_to_reuse:
                if dry_run:
                    print_log("info", f"(plan) would add collections to existing extract '{ext_name}' @ '{dep_name}' with audit TAGS: {', '.join(config_table_names)}")
                    ok, msg = True, "(plan) skipped"
                else:
                    print_log("info", f"adding collections to existing extract '{ext_name}' @ '{dep_name}' ...")
                    ok, msg = add_tables_to_extract(dep_name, ext_name, config_table_names, comment=_reuse_audit_comment)
                print_log("info" if ok else "error", f"{'extract add-tables':<{_W - (0 if ok else 1)}}: {'OK' if ok else 'ERROR':<1}  ({ext_name}@{dep_name}: {msg})")
                if ok:
                    report_ok = _verify_mongo_extract_no_errors(dep_name, ext_name, "add-tables (stop+restart)")
                    if report_ok:
                        integrated_started.append((dep_name, ext_name))
                        primary_results.append({"dep": dep_name, "ext_name": ext_name, "action": "reuse (add tables)"})
                    else:
                        primary_results.append({"dep": dep_name, "ext_name": ext_name, "action": "reuse (add tables, errors in report)"})

        # --- Create new extracts ---
        if deps_needing_create:
            bbdd_name   = config["bbdd_name"]
            env         = config["env"]
            trail_name  = "pr"
            bbdd_conn   = get_db_connection(f"{bbdd_name}_{env}")
            if not bbdd_conn:
                print_log("error", f"connection '{bbdd_name}_{env}' not found in db_connections.yaml")
                return 1
            useridalias = bbdd_conn.get("conn_name_prim") or f"{bbdd_name}_PRIM_{env}"
            cdc_seq     = bbdd_conn.get("cdc_seq", "")
            if not cdc_seq:
                print_log("error", f"cdc_seq not defined in db_connections.yaml for '{bbdd_name}_{env}' (required for extract naming)")
                return 1

            _audit_comment = "TAGS " + json.dumps({
                "requested_by":  config.get("user_name", ""),
                "cost_center":   config.get("cost_center", ""),
                "change_number": config.get("change_number", ""),
                "date":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }, separators=(",", ":"))
            # MongoDB collection names are lowercase; normalize to lower (unlike Oracle which uppercases).
            tables_payload = json.dumps(
                [{"name": t.split("(")[0].strip().lower(), "comment": _audit_comment} for t in config.get("tables", [])]
            )

            oggctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl", "oggctl.py")

            for dep_name in deps_needing_create:
                extract_name, msg = generate_extract_name(dep_name, env, cdc_seq, role="primary")
                if extract_name is None:
                    print_log("error", f"could not generate extract name for deployment '{dep_name}': {msg}")
                    continue
                print_log("info", f"creating Extract '{extract_name}' on deployment '{dep_name}' ...")

                nn          = extract_name[4:6]
                description = f"Extract-Primary-{bbdd_name}-{nn}-{env} (created by cdc-controller date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"
                trail_path  = f"primary/{bbdd_name}/{extract_name}/{trail_name}"

                cmd_create = [
                    sys.executable, oggctl_path,
                    "extract", "create", extract_name,
                    "--extract-type",            "mongo_classic",
                    "--trail",                   trail_path,
                    "--tables-json",             tables_payload,
                    "--useridalias",             useridalias,
                    "--description",             description,
                    "--managed-process-profile", "Critical",
                    "--deployment",              dep_name,
                    # NOTE: no --pdb — MongoDB has no PDB/multitenant concept
                ]
                ok, _ = _run_oggctl(cmd_create, "extract create", extract_name, dep_name, dry_run, _W)
                if not ok:
                    primary_results.append({"dep": dep_name, "ext_name": extract_name, "action": "create failed"})
                    continue

                # NOTE: MongoDB extracts do NOT need a register step (no LogMiner integration)

                ok, _ = _run_oggctl(
                    [sys.executable, oggctl_path, "extract", "start", extract_name, "--deployment", dep_name],
                    "extract start", extract_name, dep_name, dry_run, _W,
                )
                if ok:
                    report_ok = _verify_mongo_extract_no_errors(dep_name, extract_name, "extract start")
                    if report_ok:
                        integrated_started.append((dep_name, extract_name))
                        primary_results.append({"dep": dep_name, "ext_name": extract_name, "action": "create"})
                    else:
                        primary_results.append({"dep": dep_name, "ext_name": extract_name, "action": "create (errors in report)"})
                    _purge_task_name = f"Purge-trail-{extract_name}"
                    if not dry_run and _purge_task_exists(oggctl_path, _purge_task_name, dep_name):
                        print_log("warning", f"{'purge task check':<{_W}}: WARNING  ({_purge_task_name}@{dep_name}: already exists — check retention)")
                    else:
                        _run_oggctl(
                            [sys.executable, oggctl_path, "task", "purge-trail", "create", _purge_task_name,
                             "--trail", trail_path,
                             "--keep-value", _purge_days_primary, "--keep-units", "days",
                             "--schedule-value", "1", "--schedule-units", "days",
                             "--critical", "--use-checkpoints", "--enabled",
                             "--deployment", dep_name],
                            "purge task create", _purge_task_name, dep_name, dry_run, _W,
                        )

        ########## Step 2/2: PRIMARY REPLICAT
        print_log("info", "")
        print_log("info", "------ Step 2/2: PRIMARY REPLICAT ------")

        def _verify_mongo_replicat_no_errors(dep_name, rep_name, after_label):
            if dry_run:
                print_log("info", f"(plan) would verify '{rep_name}' for errors (skipped in dry-run)")
                return True
            label    = f"verifying '{rep_name}' for errors"
            since_dt = datetime.now() - timedelta(seconds=10)
            for wait_s, round_label in [(10, "after 10s")]:
                time.sleep(wait_s)
                rpt_ok, has_err, err_lines, err_msg = check_replicat_report_for_errors(dep_name, rep_name, since_dt=since_dt)
                if not rpt_ok:
                    print_log("warning", f"{label:<{_W}}: WARNING  ({round_label}, report unreadable: {err_msg})")
                    continue
                if has_err:
                    first_err = err_lines[0].strip()[:140] if err_lines else "unknown"
                    more      = f", +{len(err_lines) - 1} more" if len(err_lines) > 1 else ""
                    print_log("error", f"{label:<{_W - 1}}: ERROR  ({round_label}, {len(err_lines)} error(s) found: {first_err}{more})")
                    for line in err_lines[:10]:
                        print_log("error", f"  - {line}")
                    if len(err_lines) > 10:
                        print_log("error", f"  ... and {len(err_lines) - 10} more error line(s) (truncated)")
                    return False
                print_log("info", f"{label:<{_W}}: OK  ({round_label}, no errors found)")
            return True

        rep_bbdd_name = config["bbdd_name"]
        rep_env       = config["env"]
        rep_bbdd_conn = get_db_connection(f"{rep_bbdd_name}_{rep_env}")
        if not rep_bbdd_conn:
            print_log("error", f"connection '{rep_bbdd_name}_{rep_env}' not found in db_connections.yaml (required for replicat naming)")
            return 1
        rep_cdc_seq = rep_bbdd_conn.get("cdc_seq", "")
        if not rep_cdc_seq:
            print_log("error", f"cdc_seq not defined in db_connections.yaml for '{rep_bbdd_name}_{rep_env}' (required for replicat naming)")
            return 1

        target_engine_map = {"KAFKA": "kafka_confluent", "AWS-S3": "s3", "ORACLE": "oracle"}
        target_engine_in  = (config.get("target_engine") or "").upper()
        rep_target        = target_engine_map.get(target_engine_in)

        rep_deployments_all = get_matching_deployments(config["env"], bbdd_engine, load_types=["online"])
        replicat_deps       = [name for name, _ in rep_deployments_all if re.search(r"_R\d+_", name)]
        if not replicat_deps:
            print_log("warning", f"no Replicat deployment (_R<digits>_) found for env={config['env']}, engine={bbdd_engine}; skipping primary replicat setup")
        elif not rep_target:
            print_log("error", f"unknown target_engine '{config.get('target_engine')}' — cannot map to oggctl --target (valid: KAFKA, AWS-S3, ORACLE)")
        else:
            rep_config_tables, rep_config_table_columns = _parse_tables_from_config(config)

            if not integrated_started:
                print_log("warning", "no primary extract is started/reused; cannot determine the source trail for replicat decisions — skipping primary replicat section")
                rep_trail_path = None
                existing_rep   = {}
            else:
                _, trail_ext_name = integrated_started[0]
                rep_trail_path = f"primary/{rep_bbdd_name}/{trail_ext_name}/pr"
                if len(integrated_started) > 1:
                    print_log("warning", f"multiple primary extracts started; using trail from '{trail_ext_name}' for replicat REUSE matching")
                print_log("info", f"looking for existing Replicats consuming source trail '{rep_trail_path}' on {len(replicat_deps)} deployment(s) ...")
                existing_rep = get_replicats_for_trail(replicat_deps, rep_trail_path)

            replicats_to_reuse  = []
            replicats_to_create = []

            _rep_reuse_audit_comment = "TAGS " + json.dumps({
                "requested_by":  config.get("user_name", ""),
                "cost_center":   config.get("cost_center", ""),
                "change_number": config.get("change_number", ""),
                "date":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }, separators=(",", ":"))

            for dep_name in replicat_deps:
                if rep_trail_path is None:
                    continue
                matched = existing_rep.get(dep_name, [])
                if matched:
                    chosen = matched[0]
                    replicats_to_reuse.append((dep_name, chosen))
                    if len(matched) > 1:
                        print_log("warning", f"multiple replicats consume trail '{rep_trail_path}' on '{dep_name}': {matched} — picking '{chosen}' (first match)")
                    else:
                        print_log("info", f"Reusing existing Replicat : '{chosen}' on deployment '{dep_name}' (matched by source trail)")

                    if dry_run:
                        print_log("info", f"(plan) would check / add missing MAP entries to existing replicat '{chosen}' @ '{dep_name}'")
                        ok_add, msg_add = True, "(plan) skipped"
                    else:
                        print_log("info", f"checking + adding missing collections to existing replicat '{chosen}' @ '{dep_name}' ...")
                        ok_add, msg_add = add_tables_to_replicat(dep_name, chosen, rep_config_tables, comment=_rep_reuse_audit_comment, column_map=rep_config_table_columns)
                    print_log("info" if ok_add else "error", f"{'replicat add-tables':<{_W - (0 if ok_add else 1)}}: {'OK' if ok_add else 'ERROR':<1}  ({chosen}@{dep_name}: {msg_add})")

                    if not ok_add:
                        primary_replicat_results.append({"dep": dep_name, "ext_name": chosen, "action": "reuse (add-tables failed)"})
                        continue
                    if "no new tables to add" in msg_add:
                        primary_replicat_results.append({"dep": dep_name, "ext_name": chosen, "action": "reuse"})
                    else:
                        report_ok = _verify_mongo_replicat_no_errors(dep_name, chosen, "add-tables (stop+restart)")
                        if report_ok:
                            primary_replicat_results.append({"dep": dep_name, "ext_name": chosen, "action": "reuse (add tables)"})
                        else:
                            primary_replicat_results.append({"dep": dep_name, "ext_name": chosen, "action": "reuse (add tables, errors in report)"})
                else:
                    new_rep_name, msg = generate_replicat_name(dep_name, rep_env, rep_cdc_seq, role="primary")
                    if new_rep_name is None:
                        print_log("error", f"could not generate replicat name for deployment '{dep_name}': {msg}")
                        continue
                    replicats_to_create.append((dep_name, new_rep_name))

            print_log("info", f"{'replicats to reuse':<{_W}}: {len(replicats_to_reuse)}")
            print_log("info", f"{'deployments needing new replicat':<{_W}}: {len(replicats_to_create)}")

            if replicats_to_create:
                if rep_trail_path is None:
                    for dep_name, new_rep_name in replicats_to_create:
                        primary_replicat_results.append({"dep": dep_name, "ext_name": new_rep_name, "action": "skipped (no primary extract trail)"})
                else:
                    _rep_audit_comment = "TAGS " + json.dumps({
                        "requested_by":  config.get("user_name", ""),
                        "cost_center":   config.get("cost_center", ""),
                        "change_number": config.get("change_number", ""),
                        "date":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }, separators=(",", ":"))
                    rep_tables_payload = json.dumps([
                        _build_replicat_table_entry(t, _rep_audit_comment, rep_config_table_columns)
                        for t in rep_config_tables
                    ])

                    # target-vars-json — same logic as Oracle (lowercase db/schema for MongoDB)
                    rep_target_vars_json = None
                    if rep_target == "kafka_confluent":
                        schemas_in_config = sorted({t.split(".")[0].lower() for t in rep_config_tables if "." in t})
                        if len(schemas_in_config) > 1:
                            print_log("warning", f"multiple schemas in configured tables ({', '.join(schemas_in_config)}); using '{schemas_in_config[0]}' for replicat target-vars (single-schema templates)")
                        rep_schema = schemas_in_config[0] if schemas_in_config else ""
                        rep_target_vars_json = json.dumps({
                            "database":           rep_bbdd_name.lower(),
                            "schema":             rep_schema,
                            "kafka_topic_prefix": config.get("kafka_topic_prefix", ""),
                        })
                    elif rep_target == "s3":
                        _online_s3_tc = get_target_connection_by_bucket(config.get("s3_bucket_name") or "") or {}
                        _s3_path      = (config.get("s3_bucket_path") or "").strip()
                        rep_target_vars_json = json.dumps({
                            "database":             rep_bbdd_name.lower(),
                            "s3_bucket_name":          (config.get("s3_bucket_name") or "").strip(),
                            "bucket_path_prefix":   f"{_s3_path}/{rep_bbdd_name}" if _s3_path else rep_bbdd_name,
                            "s3_sts_assume_role":   (_online_s3_tc.get("sts_assume_role") or ""),
                            "s3_sts_session_name":  (_online_s3_tc.get("sts_session_name") or ""),
                            "s3_credentials_alias": (_online_s3_tc.get("ogg_alias_credential") or ""),
                            "aws_region":           (_online_s3_tc.get("aws_region") or "eu-west-1"),
                        })

                    oggctl_path_rep = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl", "oggctl.py")

                    for dep_name, new_rep_name in replicats_to_create:
                        nn              = new_rep_name[4:6]
                        _rep_target_label = "KAFKA" if rep_target == "kafka_confluent" else rep_target.upper()
                        rep_description = f"Replicat-Primary-{rep_bbdd_name}-{nn}-{_rep_target_label}-{rep_env} (created by cdc-controller date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"

                        cmd_rep_create = [
                            sys.executable, oggctl_path_rep,
                            "replicat", "create", new_rep_name,
                            "--replicat-type",           "classic",
                            "--target",                  rep_target,
                            "--trail",                   rep_trail_path,
                            "--tables-json",             rep_tables_payload,
                            "--description",             rep_description,
                            "--managed-process-profile", "Critical",
                            "--deployment",              dep_name,
                            # NOTE: no --pdb — MongoDB has no PDB/multitenant concept
                        ]
                        if _kafka_target_name and rep_target == "kafka_confluent":
                            cmd_rep_create.extend(["--target-name", _kafka_target_name])
                        if rep_target_vars_json:
                            cmd_rep_create.extend(["--target-vars-json", rep_target_vars_json])

                        ok, _ = _run_oggctl(cmd_rep_create, "replicat create", new_rep_name, dep_name, dry_run, _W)
                        if not ok:
                            primary_replicat_results.append({"dep": dep_name, "ext_name": new_rep_name, "action": "create failed"})
                            continue

                        ok, _ = _run_oggctl(
                            [sys.executable, oggctl_path_rep, "replicat", "start", new_rep_name, "--deployment", dep_name],
                            "replicat start", new_rep_name, dep_name, dry_run, _W,
                        )
                        if not ok:
                            primary_replicat_results.append({"dep": dep_name, "ext_name": new_rep_name, "action": "start failed"})
                            continue

                        report_ok = _verify_mongo_replicat_no_errors(dep_name, new_rep_name, "replicat start")
                        if report_ok:
                            primary_replicat_results.append({"dep": dep_name, "ext_name": new_rep_name, "action": "create"})
                        else:
                            primary_replicat_results.append({"dep": dep_name, "ext_name": new_rep_name, "action": "create (errors in report)"})

        ########## Final Summary — identical layout to Oracle block
        if primary_results:
            pri_rows = []
            for pr in primary_results:
                ok_info, info = get_extract_info_now(pr["dep"], pr["ext_name"])
                if ok_info:
                    status     = str(info.get("status", "unknown"))
                    last_start = find_extract_last_started(info) or "-"
                    if last_start == "-" and isinstance(info, dict):
                        print_log("warning", f"[Primary summary] no startup-time field found for '{pr['ext_name']}'")
                else:
                    status     = f"unknown ({info.get('error', '')})"
                    last_start = "-"
                pri_rows.append({
                    "process":    pr["ext_name"],
                    "type":       "Extract (online)",
                    "action":     pr["action"],
                    "status":     status,
                    "last_start": str(last_start),
                })
            print_log("info", "")
            print_log("info", "====== Final Summary: Primary Extract ======")
            pri_deps = sorted({pr["dep"] for pr in primary_results})
            print_log("info", f"OGG Deployment : {', '.join(pri_deps) if pri_deps else '(unknown)'}")
            _P = max(len("PROCESS"),           max((len(r["process"])    for r in pri_rows), default=0))
            _T = max(len("TYPE"),              max((len(r["type"])       for r in pri_rows), default=0))
            _A = max(len("ACTION"),            max((len(r["action"])     for r in pri_rows), default=0))
            _S = max(len("STATUS"),            max((len(r["status"])     for r in pri_rows), default=0))
            _L = max(len("LAST STARTUP TIME"), max((len(r["last_start"]) for r in pri_rows), default=0))
            sep_line = f"{'-'*_P}  {'-'*_T}  {'-'*_A}  {'-'*_S}  {'-'*_L}"
            print_log("info", sep_line)
            print_log("info", f"{'PROCESS':<{_P}}  {'TYPE':<{_T}}  {'ACTION':<{_A}}  {'STATUS':<{_S}}  {'LAST STARTUP TIME':<{_L}}")
            print_log("info", sep_line)
            for r in pri_rows:
                print_log("info", f"{r['process']:<{_P}}  {r['type']:<{_T}}  {r['action']:<{_A}}  {r['status']:<{_S}}  {r['last_start']:<{_L}}")
            print_log("info", sep_line)

        if primary_replicat_results:
            _rep_mode_label = {"nonintegrated": "classic", "coordinated": "coordinated"}
            prep_rows = []
            for pr in primary_replicat_results:
                ok_info, info = get_replicat_info_now(pr["dep"], pr["ext_name"])
                if ok_info:
                    status     = str(info.get("status", "unknown"))
                    last_start = find_extract_last_started(info) or "-"
                    if last_start == "-" and isinstance(info, dict):
                        print_log("warning", f"[Primary Replicat summary] no startup-time field found for '{pr['ext_name']}'")
                    mode      = info.get("mode")
                    mode_type = mode.get("type") if isinstance(mode, dict) else None
                    type_lbl  = _rep_mode_label.get(mode_type, mode_type or "unknown")
                else:
                    status     = f"unknown ({info.get('error', '')})"
                    last_start = "-"
                    type_lbl   = "unknown"
                prep_rows.append({
                    "process":    pr["ext_name"],
                    "type":       f"Replicat ({type_lbl})",
                    "action":     pr["action"],
                    "status":     status,
                    "last_start": str(last_start),
                })
            print_log("info", "")
            print_log("info", "====== Final Summary: Primary Replicat ======")
            prep_deps = sorted({pr["dep"] for pr in primary_replicat_results})
            print_log("info", f"OGG Deployment : {', '.join(prep_deps) if prep_deps else '(unknown)'}")
            _P = max(len("PROCESS"),           max((len(r["process"])    for r in prep_rows), default=0))
            _T = max(len("TYPE"),              max((len(r["type"])       for r in prep_rows), default=0))
            _A = max(len("ACTION"),            max((len(r["action"])     for r in prep_rows), default=0))
            _S = max(len("STATUS"),            max((len(r["status"])     for r in prep_rows), default=0))
            _L = max(len("LAST STARTUP TIME"), max((len(r["last_start"]) for r in prep_rows), default=0))
            sep_line = f"{'-'*_P}  {'-'*_T}  {'-'*_A}  {'-'*_S}  {'-'*_L}"
            print_log("info", sep_line)
            print_log("info", f"{'PROCESS':<{_P}}  {'TYPE':<{_T}}  {'ACTION':<{_A}}  {'STATUS':<{_S}}  {'LAST STARTUP TIME':<{_L}}")
            print_log("info", sep_line)
            for r in prep_rows:
                print_log("info", f"{r['process']:<{_P}}  {r['type']:<{_T}}  {r['action']:<{_A}}  {r['status']:<{_S}}  {r['last_start']:<{_L}}")
            print_log("info", sep_line)

    ############## DB2
    elif bbdd_engine == "DB2":
        pass

    print_log("info", f"end {mode_label}")
    return 0
