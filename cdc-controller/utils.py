import sys
import os
import json
import re
import time
import fnmatch
from datetime import datetime

_log_file = None


def setup_log_file(banner=""):
    # Creates the logs directory (if it does not exist) and opens a new log file
    # named cdc-controller_exec_YYYYMMDD_HHMMSS.log. Must be called once at startup.
    # Writes the optional banner as the first line of the log file.
    global _log_file
    logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(logs_dir, exist_ok=True)
    filename = datetime.now().strftime("cdc-controller_exec_%Y%m%d_%H%M%S.log")
    log_path = os.path.join(logs_dir, filename)
    _log_file = open(log_path, "w", encoding="utf-8")
    if banner:
        _log_file.write(f"\n{banner}\n\n")
        _log_file.flush()
    print_log("info", f"log file created : {log_path}")


def clean_api_error(err_text, max_len=200):
    # Compacts OGG REST API error messages for logging. Use cases:
    #   - 5xx responses from the OGG admin server / nginx proxy include a full HTML body
    #     (<html>...<title>502 Bad Gateway</title>...). We only want a one-liner.
    #   - Stack traces or multi-line errors mess up the log alignment.
    # Strategy:
    #   - if the message contains HTML, extract the <title> (plus the "(HTTP NNN)" prefix
    #     that oggctl tends to prepend) and discard the body
    #   - otherwise collapse whitespace and truncate to max_len
    if not err_text:
        return ""
    s = str(err_text)
    if "<html" in s.lower() or "<title>" in s.lower():
        m_title  = re.search(r"<title[^>]*>(.*?)</title>", s, re.IGNORECASE | re.DOTALL)
        m_status = re.search(r"\(HTTP\s+\d{3}\)", s)
        title    = m_title.group(1).strip()  if m_title  else ""
        prefix   = m_status.group(0).strip() if m_status else ""
        compact  = f"{prefix} {title}".strip()
        return compact or (s[:max_len].rstrip() + "...")
    s = " ".join(s.split())
    if len(s) > max_len:
        s = s[:max_len].rstrip() + "..."
    return s


def print_log(level, message):
    # Prints a timestamped log message with the specified level (INFO, WARNING, ERROR).
    # Also writes to the log file if setup_log_file() has been called.
    # Defaults to INFO if an unrecognised level is provided.
    ts = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    level_upper = level.upper()
    if level_upper not in ("INFO", "ERROR", "WARNING"):
        level_upper = "INFO"
    line = f"{ts} - {level_upper:<7}: {message}"
    print(line)
    if _log_file:
        _log_file.write(line + "\n")
        _log_file.flush()



def load_json_file(filepath):
    # Validates the file, parses all fields dynamically, prints each variable,
    # and validates mandatory CDC fields (bbdd_engine, tables, etc..).
    # String values are stripped; fields in _LOWERCASE_FIELDS are also lowercased.
    # Skips comment-like keys (starting with "//") and never prints the password field.
    # Returns the config dict, or None if CDC validation fails.

    # local variables
    _UPPERCASE_FIELDS = {"env", "request_type", "bbdd_engine", "bbdd_name", "replication_type", "project", "target_engine"}
    _VAR_ALIGN        = 26

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
    _PREFIX = "variable => "
    print_log("info", f"{'json config file':<{len(_PREFIX) + _VAR_ALIGN}}: {os.path.basename(filepath)}")

    # load variables from json file into config array
    config = {}
    for k, v in raw.items():
        if k.startswith("//"):
            continue
        if isinstance(v, str):
            config[k] = v.strip().upper() if k in _UPPERCASE_FIELDS else v.strip()
        else:
            config[k] = v

    # print all variables defined from JSON file
    for k, v in config.items():
        if k == "password":
            continue
        display_val = ",".join(v) if isinstance(v, list) else str(v)
        print_log("info", f"variable => {k:<{_VAR_ALIGN}}: {display_val}")

    # print validation begin
    print_log("info", f"validating variables from json config file ...")

    # validate required variables and data validation (all values are uppercase)
    VALID_ENVS             = {"DEV", "PRE", "PRO"}
    VALID_REQUEST_TYPES    = {"CREATE", "DELETE", "STATUS", "UPDATE"}
    VALID_DB_ENGINES       = {"ORACLE", "MONGO", "DB2"}
    VALID_PROJECTS         = {"DATAIA", "GOLDENRECORDS"}
    VALID_REPLICATION_TYPE = {"FULL", "ONLINE", "INITIAL_LOAD"}
    VALID_TARGET_ENGINES   = {"AWS-S3", "KAFKA"}

    # initialize errors array
    # config already holds all JSON fields dynamically — new fields added to the JSON
    # are automatically available via config.get() without any code change here.
    errors = []

    # validate required variables
    if not config.get("env"):
        errors.append("'env' is required")
    elif config["env"] not in VALID_ENVS:
        errors.append(f"'env' value '{config['env']}' not valid. Must be one of: {', '.join(sorted(VALID_ENVS))}")

    if not config.get("request_type"):
        errors.append("'request_type' is required")
    elif config["request_type"] not in VALID_REQUEST_TYPES:
        errors.append(f"'request_type' value '{config['request_type']}' not valid. Must be one of: {', '.join(sorted(VALID_REQUEST_TYPES))}")

    if not config.get("project"):
        errors.append("'project' is required")
    elif config["project"] not in VALID_PROJECTS:
        errors.append(f"'project' value '{config['project']}' not valid. Must be one of: {', '.join(sorted(VALID_PROJECTS))}")

    if not config.get("bbdd_engine"):
        errors.append("'bbdd_engine' is required")
    elif config["bbdd_engine"] not in VALID_DB_ENGINES:
        errors.append(f"'bbdd_engine' value '{config['bbdd_engine']}' not valid. Must be one of: {', '.join(sorted(VALID_DB_ENGINES))}")

    if not config.get("bbdd_name"):
        errors.append("'bbdd_name' is required")

    if not config.get("tables"):
        errors.append("'tables' is required and cannot be empty")
    else:
        # validate format: schema.table_name(* | col1,col2,...)
        table_pattern = re.compile(r"^\w+\.\w+\((\*|\w+(\s*,\s*\w+)*)\)$")
        for table in config["tables"]:
            if not table_pattern.match(table):
                errors.append(f"'tables' entry '{table}' has invalid format. Expected: schema.table_name(* | col1,col2,...)")

    if not config.get("replication_type"):
        errors.append("'replication_type' is required")
    elif config["replication_type"] not in VALID_REPLICATION_TYPE:
        errors.append(f"'replication_type' value '{config['replication_type']}' not valid. Must be one of: {', '.join(sorted(VALID_REPLICATION_TYPE))}")

    if not config.get("target_engine"):
        errors.append("'target_engine' is required")
    elif config["target_engine"] not in VALID_TARGET_ENGINES:
        errors.append(f"'target_engine' value '{config['target_engine']}' not valid. Must be one of: {', '.join(sorted(VALID_TARGET_ENGINES))}")

    if not config.get("target_info"):
        errors.append("'target_info' is required")

    # audit / traceability fields used to stamp '-- Requested by: ...; Cost center: ...; Date: ...'
    # on every TABLE/MAP line of the primary extract and primary replicat PRMs.
    if not config.get("user_name"):
        errors.append("'user_name' is required (used in the audit comment of every TABLE/MAP PRM line)")
    if not config.get("cost_center"):
        errors.append("'cost_center' is required (used in the audit comment of every TABLE/MAP PRM line)")
    if not config.get("change_number"):
        errors.append("'change_number' is required (used in the audit comment of every TABLE/MAP PRM line)")

    # conditional requirements based on target_engine
    #   KAFKA   -> 'kafka_topic_prefix'    (prefix used to build per-table Kafka topic names)
    #             'kafka_cluster_name'     (matches cluster_name in target_connections.yaml to locate the right Kafka entry)
    #             'kafka_identity_pool'    (matches identity_pool; disambiguates entries sharing the same cluster)
    #   AWS-S3  -> 's3_bucket_name'         (used to locate the matching entry in target_connections.yaml;
    #                                       s3_bucket_path and credentials come from that entry, not from here)
    if config.get("target_engine") == "KAFKA" and not config.get("kafka_topic_prefix"):
        errors.append("'kafka_topic_prefix' is required when target_engine is KAFKA")
    if config.get("target_engine") == "KAFKA" and not config.get("kafka_cluster_name"):
        errors.append("'kafka_cluster_name' is required when target_engine is KAFKA")
    if config.get("target_engine") == "KAFKA" and not config.get("kafka_identity_pool"):
        errors.append("'kafka_identity_pool' is required when target_engine is KAFKA")
    if config.get("target_engine") == "AWS-S3" and not config.get("s3_bucket_name"):
        errors.append("'s3_bucket_name' is required when target_engine is AWS-S3")
    if config.get("target_engine") == "AWS-S3" and not config.get("s3_bucket_path"):
        errors.append("'s3_bucket_path' is required when target_engine is AWS-S3")

    # print total number of tables detected in JSON
    print_log("info", f"{'total tables detected in JSON':<{len(_PREFIX) + _VAR_ALIGN}}: {len(config.get('tables', []))}")

    # print errors detected in variables
    if errors:
        # print validation ERROR
        print_log("info", f"{'json config file validation':<{len(_PREFIX) + _VAR_ALIGN}}: ERROR")
        for err in errors:
            print_log("error", f"data validation error => {err}")
        return None

    # if errors is empty all validations passed     
    if not errors:
        # print validation OK
        print_log("info", f"{'json config file validation':<{len(_PREFIX) + _VAR_ALIGN}}: OK")

    return config


def _get_tables_in_use_by_processes(deployment_names, resource):
    # Internal helper. resource is "extracts" or "replicats".
    # For each deployment, list processes of that type and parse their PRM directives:
    #   - extracts:  TABLE <pat>;   include  /  TABLEEXCLUDE <pat>;  exclude
    #   - replicats: MAP <pat>, target <pat>;   include  /  MAPEXCLUDE <pat>;   exclude
    # Returns a list of records, one per process that has at least one TABLE/MAP entry:
    #   [(deployment_name, process_name, [include_patterns], [exclude_patterns]), ...]
    # Patterns may contain OGG wildcards '*' and '?' (e.g. "SOE.*", "SOE.A*"). Matching a
    # concrete table is done by find_processes_for_table(), which honors excludes correctly.
    # Requires OGG_ADMIN_PASSWORD env var to be set (OGGClient uses it as auth fallback).
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.extract_ops  import list_extracts
        from api.replicat_ops import list_replicats
    except ImportError as e:
        print_log("error", f"failed to import oggctl modules: {e}")
        return []

    list_fn = list_extracts if resource == "extracts" else list_replicats
    # In Extract PRMs the directives are "TABLE <pat>;" / "TABLEEXCLUDE <pat>;".
    # In Replicat PRMs they are "MAP source.tab, TARGET target.tab;" / "MAPEXCLUDE <pat>;".
    # The startswith checks use a trailing space so e.g. "TABLEEXCLUDE foo;" does NOT match "TABLE ".
    include_directive = "TABLE "        if resource == "extracts" else "MAP "
    exclude_directive = "TABLEEXCLUDE " if resource == "extracts" else "MAPEXCLUDE "

    def _extract_pattern(rest_text):
        # captures the leading SCHEMA.TABLE expression (allowing * / ? wildcards) and
        # normalizes 3-part PDB.SCHEMA.TABLE -> SCHEMA.TABLE.
        m = re.match(r"([A-Za-z0-9_$.*?]+)", rest_text)
        if not m:
            return None
        pat   = m.group(1).upper()
        parts = pat.split(".")
        if len(parts) == 3:
            pat = f"{parts[1]}.{parts[2]}"
        return pat

    processes_in_use = []
    for dep_name in deployment_names:
        try:
            cfg = load_cfg(dep_name)
            maybe_suppress_tls_warning(cfg)
            client = build_client(cfg)
            processes = list_fn(client)
        except Exception as e:
            print_log("error", f"failed to list {resource} on deployment '{dep_name}': {clean_api_error(e)}")
            continue

        for proc in processes:
            proc_name = proc["name"]
            try:
                r = client.get(f"/{resource}/{proc_name}")
                if r.status_code != 200:
                    continue
                prm_lines = r.json().get("response", {}).get("config", []) or []
            except Exception as e:
                print_log("warning", f"could not read PRM for {resource[:-1]} '{proc_name}' on '{dep_name}': {clean_api_error(e)}")
                continue

            includes = []
            excludes = []
            for line in prm_lines:
                s = line.strip()
                if not s or s.startswith("--"):
                    continue
                u = s.upper()
                # exclude FIRST because both directives share the 'TABLE'/'MAP' prefix
                # but exclude has no embedded space and so startswith differentiates them.
                if u.startswith(exclude_directive):
                    rest = s[len(exclude_directive):].strip().rstrip(";").strip()
                    pat  = _extract_pattern(rest)
                    if pat:
                        excludes.append(pat)
                elif u.startswith(include_directive):
                    rest = s[len(include_directive):].strip().rstrip(";").strip()
                    pat  = _extract_pattern(rest)
                    if pat:
                        includes.append(pat)
            if includes or excludes:
                processes_in_use.append((dep_name, proc_name, includes, excludes))

    return processes_in_use


def find_processes_for_table(config_table, processes):
    # Given a concrete OWNER.TABLE and the list of process records returned by
    # get_tables_in_use_by_{extracts,replicats}() (each entry is
    # (dep, proc, [includes], [excludes])), returns the subset of processes that cover
    # the table, respecting OGG include/exclude semantics:
    #   - at least one INCLUDE pattern matches the table   AND
    #   - no EXCLUDE  pattern matches the table
    # Returns list of (dep, proc, matched_include_pattern) — same shape as before so
    # callers (validate.py, get_replicats_for_tables) don't need to change.
    table_up = (config_table or "").upper()
    matches  = []
    for dep, proc, includes, excludes in processes:
        # excluded by any exclude pattern -> skip this process for this table
        if any(fnmatch.fnmatchcase(table_up, ex) for ex in excludes):
            continue
        # find the first include that matches (and stop)
        for inc in includes:
            if fnmatch.fnmatchcase(table_up, inc):
                matches.append((dep, proc, inc))
                break
    return matches


def get_tables_in_use_by_extracts(deployment_names):
    # Returns list of (deployment, extract_name, [includes], [excludes]) — one entry per
    # Extract that has at least one TABLE / TABLEEXCLUDE directive in its PRM. 'includes'
    # / 'excludes' contain OWNER.TABLE patterns (may include OGG wildcards '*' / '?').
    # Use find_processes_for_table() to match a concrete table against these records.
    return _get_tables_in_use_by_processes(deployment_names, "extracts")


def extract_was_user_stopped(deployment, extract_name):
    # Inspects the extract report file (<extract_name>.rpt) on the deployment and detects
    # whether the process was stopped intentionally by a user (via OGG admin server).
    # OGG writes this marker line to the report when a STOP command is received:
    #   "OGG-01021  Command received from application adminsrvr: STOP."
    # Returns (user_stopped: bool, ok: bool, msg: str):
    #   - user_stopped : True if the OGG-01021 STOP marker was found in the report
    #   - ok           : True if the report could be read; False on transport/import errors
    #   - msg          : detail when ok=False; empty otherwise
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.extract_ops  import get_extract_report
    except ImportError as e:
        return False, False, f"failed to import oggctl modules: {e}"
    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
        lines  = get_extract_report(client, extract_name, f"{extract_name}.rpt")
    except Exception as e:
        return False, False, str(e)

    user_stopped = any(
        "OGG-01021" in line and "STOP" in line.upper()
        for line in (lines or [])
    )
    return user_stopped, True, ""


def get_replicat_status_now(deployment, replicat_name):
    # Single status query for a replicat. Returns (ok: bool, status: str).
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli               import load_cfg, build_client, maybe_suppress_tls_warning
        from api.replicat_ops  import get_replicat_status
    except ImportError as e:
        return False, f"failed to import oggctl modules: {e}"
    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
        status = get_replicat_status(client, replicat_name)
        return True, str(status)
    except Exception as e:
        return False, str(e)


def _scan_report_for_errors(lines, since_dt=None):
    # Shared error-scan logic for extract/replicat reports.
    # Two modes for choosing the scan window:
    #   - since_dt provided  : keep lines whose leading 'YYYY-MM-DD HH:MM:SS' timestamp is
    #                          >= since_dt. Wrap/continuation lines (no leading timestamp)
    #                          inherit the include flag of the previous timestamped line —
    #                          this is the recommended mode after a start/restart because
    #                          it is immune to OGG auto-restart cycles that would otherwise
    #                          push the 'last startup marker' past the error we want to see.
    #   - since_dt None      : fall back to 'scan from the last startup marker' (the original
    #                          behaviour, kept for callers that don't have a wall-clock
    #                          reference of when the process was (re)started).
    # Error detection inside the chosen window:
    #   - any line containing the word 'ABEND' (always an error)
    #   - any line containing the word 'ERROR' AND an OGG-NNNNN code (avoids matching prose)
    ogg_code_re = re.compile(r"OGG-\d{5}")
    abend_re    = re.compile(r"\bABEND",   re.IGNORECASE)
    error_kw_re = re.compile(r"\bERROR\b", re.IGNORECASE)

    if since_dt is not None:
        ts_re   = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
        scoped  = []
        include = False
        for line in lines:
            m = ts_re.match(line)
            if m:
                try:
                    line_dt = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                    include = line_dt >= since_dt
                except ValueError:
                    pass
            if include:
                scoped.append(line)
    else:
        startup_markers  = (
            "OGG-00993", "EXTRACT STARTED", "REPLICAT STARTED",
            "STARTING CAPTURE", "STARTING REPLICAT", "STARTING AT",
        )
        last_startup_idx = -1
        for idx, line in enumerate(lines):
            u = line.upper()
            if any(m in u for m in startup_markers):
                last_startup_idx = idx
        scoped = lines[max(0, last_startup_idx + 1):]

    error_lines = []
    for line in scoped:
        if abend_re.search(line):
            error_lines.append(line.rstrip())
            continue
        if error_kw_re.search(line) and ogg_code_re.search(line):
            error_lines.append(line.rstrip())
    return bool(error_lines), error_lines


def check_extract_report_for_errors(deployment, extract_name, since_dt=None):
    # Reads <extract_name>.rpt and looks for ABEND / 'ERROR + OGG-NNNNN' lines.
    # See _scan_report_for_errors for the scan-window logic. When 'since_dt' is provided
    # the scan ignores everything before that wall-clock time, which is robust against
    # critical-profile auto-restart loops that leave a fresh 'Starting at' AFTER the
    # error we care about.
    # Returns (ok, has_errors, error_lines, msg):
    #   - ok          : True if the report could be read (False on transport/import failure)
    #   - has_errors  : True if at least one error line was found within the scan window
    #   - error_lines : list of matched error lines (stripped); empty if none
    #   - msg         : detail when ok=False; empty otherwise
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.extract_ops  import get_extract_report
    except ImportError as e:
        return False, False, [], f"failed to import oggctl modules: {e}"
    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
        lines  = get_extract_report(client, extract_name, f"{extract_name}.rpt") or []
    except Exception as e:
        return False, False, [], str(e)

    has_err, err_lines = _scan_report_for_errors(lines, since_dt=since_dt)
    return True, has_err, err_lines, ""


def check_replicat_report_for_errors(deployment, replicat_name, since_dt=None):
    # Mirror of check_extract_report_for_errors for replicat reports.
    # Returns (ok, has_errors, error_lines, msg) — same shape as the extract version.
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.replicat_ops import get_replicat_report
    except ImportError as e:
        return False, False, [], f"failed to import oggctl modules: {e}"
    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
        lines  = get_replicat_report(client, replicat_name, f"{replicat_name}.rpt") or []
    except Exception as e:
        return False, False, [], str(e)

    has_err, err_lines = _scan_report_for_errors(lines, since_dt=since_dt)
    return True, has_err, err_lines, ""


def get_extract_info_now(deployment, extract_name):
    # Returns (ok: bool, info: dict). The dict is a merge of TWO OGG REST endpoints so
    # the caller doesn't have to know which one exposes a given field (the layout varies
    # across OGG versions):
    #   - GET /extracts/{name}/info/status   -> runtime data (status, position, lag, processId, ...)
    #   - GET /extracts/{name}               -> resource data (config, source/type, lastStarted, ...)
    # On the resource endpoint we don't overwrite keys already set by /info/status; the
    # runtime endpoint wins for live fields like 'status'.
    # On failure returns (False, {"error": "<reason>"}).
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
    except ImportError as e:
        return False, {"error": f"failed to import oggctl modules: {e}"}
    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
    except Exception as e:
        return False, {"error": str(e)}

    merged = {}
    any_ok = False

    # 1) runtime status endpoint
    try:
        r = client.get(f"/extracts/{extract_name}/info/status")
        if r.status_code == 200:
            body = r.json() or {}
            data = body.get("response") if isinstance(body.get("response"), dict) else body
            if isinstance(data, dict):
                merged.update(data)
                any_ok = True
    except Exception:
        pass

    # 2) resource endpoint (fallback for fields not present on /info/status)
    try:
        r = client.get(f"/extracts/{extract_name}")
        if r.status_code == 200:
            body = r.json() or {}
            data = body.get("response") if isinstance(body.get("response"), dict) else body
            if isinstance(data, dict):
                for k, v in data.items():
                    merged.setdefault(k, v)
                any_ok = True
    except Exception:
        pass

    if not any_ok:
        return False, {"error": f"could not fetch info for extract '{extract_name}'"}
    return True, merged


def get_replicat_info_now(deployment, replicat_name):
    # Mirror of get_extract_info_now but for replicats. Merges responses from:
    #   - GET /replicats/{name}/info/status (runtime: status, position, lag, processId, ...)
    #   - GET /replicats/{name}             (resource: config, lastStarted, ...)
    # Returns (ok, info_dict) — info merged across both endpoints, runtime fields win.
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli import load_cfg, build_client, maybe_suppress_tls_warning
    except ImportError as e:
        return False, {"error": f"failed to import oggctl modules: {e}"}
    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
    except Exception as e:
        return False, {"error": str(e)}

    merged = {}
    any_ok = False
    try:
        r = client.get(f"/replicats/{replicat_name}/info/status")
        if r.status_code == 200:
            body = r.json() or {}
            data = body.get("response") if isinstance(body.get("response"), dict) else body
            if isinstance(data, dict):
                merged.update(data)
                any_ok = True
    except Exception:
        pass
    try:
        r = client.get(f"/replicats/{replicat_name}")
        if r.status_code == 200:
            body = r.json() or {}
            data = body.get("response") if isinstance(body.get("response"), dict) else body
            if isinstance(data, dict):
                for k, v in data.items():
                    merged.setdefault(k, v)
                any_ok = True
    except Exception:
        pass

    if not any_ok:
        return False, {"error": f"could not fetch info for replicat '{replicat_name}'"}
    return True, merged


def find_extract_last_started(info):
    # Searches the merged extract-info dict for the most likely "last startup time" field.
    # Different OGG versions place this under different keys (and sometimes nested), so this
    # helper probes a list of known candidates at the top level and under a 'data' wrapper.
    # Returns the string value if found, or None.
    if not isinstance(info, dict):
        return None
    candidates = ("lastStarted", "lastStartedAt", "lastStartTime", "startTime", "started", "startedAt", "lastStart")
    for key in candidates:
        v = info.get(key)
        if v:
            return v
    nested = info.get("data") if isinstance(info.get("data"), dict) else None
    if nested:
        for key in candidates:
            v = nested.get(key)
            if v:
                return v
    return None


def get_extract_status_now(deployment, extract_name):
    # Single status query for an extract. Returns (ok: bool, status: str).
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.extract_ops  import get_extract_status
    except ImportError as e:
        return False, f"failed to import oggctl modules: {e}"
    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
        status = get_extract_status(client, extract_name)
        return True, str(status)
    except Exception as e:
        return False, str(e)


def wait_for_extract_running(deployment, extract_name, max_wait=60, poll_interval=5):
    # Polls the OGG REST API until the extract status is RUNNING or max_wait seconds elapse.
    # Returns (ok: bool, last_status: str).
    import time
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.extract_ops  import get_extract_status
    except ImportError as e:
        return False, f"failed to import oggctl modules: {e}"
    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
    except Exception as e:
        return False, f"failed to build OGG client for '{deployment}': {e}"

    elapsed     = 0
    last_status = "UNKNOWN"
    while True:
        try:
            last_status = get_extract_status(client, extract_name)
        except Exception as e:
            last_status = f"error: {e}"
        if str(last_status).upper() == "RUNNING":
            return True, last_status
        if elapsed >= max_wait:
            return False, last_status
        time.sleep(poll_interval)
        elapsed += poll_interval


def wait_for_replicat_running(deployment, replicat_name, max_wait=60, poll_interval=5):
    # Mirror of wait_for_extract_running for replicats. Polls the OGG REST API until the
    # replicat status is RUNNING or max_wait seconds elapse. Returns (ok: bool, last_status: str).
    import time
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.replicat_ops import get_replicat_status
    except ImportError as e:
        return False, f"failed to import oggctl modules: {e}"
    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
    except Exception as e:
        return False, f"failed to build OGG client for '{deployment}': {e}"

    elapsed     = 0
    last_status = "UNKNOWN"
    while True:
        try:
            last_status = get_replicat_status(client, replicat_name)
        except Exception as e:
            last_status = f"error: {e}"
        if str(last_status).upper() == "RUNNING":
            return True, last_status
        if elapsed >= max_wait:
            return False, last_status
        time.sleep(poll_interval)
        elapsed += poll_interval


_B36 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

def _nn_to_b36(nn: int) -> str:
    return _B36[nn // 36] + _B36[nn % 36]

def _b36_to_nn(s: str) -> int:
    return _B36.index(s[0].upper()) * 36 + _B36.index(s[1].upper())


def generate_extract_name(deployment, env, cdc_seq, role="primary", prefer_nn=None):
    # Builds a unique Extract process name. Two formats coexist:
    #   - role='primary'      -> E<role><cdc_seq><NN><env>     (8 chars) e.g. 'EPPI00D'
    #   - role='initial_load' -> E I <NN> <env>                (5 chars) e.g. 'EI00D'
    # The short form for initial_load mirrors the 5-char limit OGG imposes on coordinated
    # replicat names so that IL extract↔IL replicat pairs can share the same NN visually.
    # 'cdc_seq' is ignored for initial_load (the short name has no room for it).
    #
    # 'prefer_nn' (initial_load only): if the requested NN is free on the deployment, the
    # generator returns it (used by the caller to try to keep extract↔replicat NN in sync).
    # If it's already taken, the first free NN is returned instead — correspondence falls
    # back to the trail path, which always uniquely identifies the pair.
    #
    # Returns (name: str|None, msg: str).

    # 1) role -> letter
    role_map = {"primary": "P", "initial_load": "I"}
    role_letter = role_map.get(role.lower())
    if not role_letter:
        return None, f"unsupported role '{role}' (valid: primary, initial_load)"

    # 2) env -> letter
    env_map = {"DEV": "D", "PRE": "P", "PRO": "R"}
    env_letter = env_map.get((env or "").upper())
    if not env_letter:
        return None, f"unknown env '{env}' (valid: DEV, PRE, PRO)"

    is_initial_load = (role.lower() == "initial_load")

    # 3) cdc_seq validation only applies to the 8-char (primary) form
    cdc_seq_s = ""
    if not is_initial_load:
        cdc_seq_s = str(cdc_seq or "").strip()
        if len(cdc_seq_s) != 2 or not cdc_seq_s.isalnum():
            return None, f"cdc_seq must be exactly 2 alphanumeric characters (letters and/or digits), got: {cdc_seq!r}"

    # 4) list extracts on the deployment to find which NNs are already used
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.extract_ops  import list_extracts
    except ImportError as e:
        return None, f"failed to import oggctl modules: {e}"
    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
        extracts = list_extracts(client)
    except Exception as e:
        return None, f"failed to list extracts on '{deployment}': {clean_api_error(e)}"

    # 5) Pattern depends on the form (5-char IL vs 8-char primary).
    if is_initial_load:
        pattern  = re.compile(rf"^EI([0-9A-Z]{{2}}){env_letter}$", re.IGNORECASE)
        prefix   = f"EI"
        suffix   = env_letter
        slot_lbl = f"EI<NN>{env_letter}"
    else:
        pattern  = re.compile(rf"^E{role_letter}{cdc_seq_s}(\d{{2}}){env_letter}$")
        prefix   = f"E{role_letter}{cdc_seq_s}"
        suffix   = env_letter
        slot_lbl = f"E{role_letter}{cdc_seq_s}NN{env_letter}"
    if is_initial_load:
        used = {_b36_to_nn(m.group(1)) for m in (pattern.match(e["name"]) for e in extracts) if m}
    else:
        used = {int(m.group(1)) for m in (pattern.match(e["name"]) for e in extracts) if m}

    # 6) Pick the NN. 'prefer_nn' is honored only if free; otherwise we fall back to the
    # lowest free NN (so the IL replicat that asked for that same NN already used it).
    if is_initial_load and prefer_nn is not None and 0 <= int(prefer_nn) <= 1295 and int(prefer_nn) not in used:
        chosen_nn = int(prefer_nn)
    else:
        chosen_nn = next((nn for nn in range(1296) if nn not in used), None)
        if chosen_nn is None:
            return None, f"sequential exhausted (max 1295) for pattern {slot_lbl}"

    return f"{prefix}{_nn_to_b36(chosen_nn)}{suffix}", "OK"


def generate_replicat_name(deployment, env, cdc_seq, role="primary", prefer_nn=None):
    # Builds a unique Replicat process name. Two formats coexist:
    #   - role='primary'      -> R<role><cdc_seq><NN><env>     (8 chars) e.g. 'RPPI00D'
    #   - role='initial_load' -> R I <NN> <env>                (5 chars) e.g. 'RI00D'
    # The 5-char short form is mandatory for coordinated replicats (OGG limit) and is
    # what we use for initial_load. 'cdc_seq' is ignored for initial_load.
    #
    # 'prefer_nn' (initial_load only): if the requested NN is free on the deployment, the
    # generator returns it. Used by the IL pairing flow so that 'EI<NN>D' on the extract
    # side and 'RI<NN>D' on the replicat side share the same NN whenever possible. When
    # the preferred NN is already taken, the first free NN is returned and the visual
    # extract↔replicat correspondence falls back to the trail path (which always
    # uniquely identifies the pair).
    #
    # Returns (name: str|None, msg: str).

    # 1) role -> letter
    role_map = {"primary": "P", "initial_load": "I"}
    role_letter = role_map.get(role.lower())
    if not role_letter:
        return None, f"unsupported role '{role}' (valid: primary, initial_load)"

    # 2) env -> letter
    env_map = {"DEV": "D", "PRE": "P", "PRO": "R"}
    env_letter = env_map.get((env or "").upper())
    if not env_letter:
        return None, f"unknown env '{env}' (valid: DEV, PRE, PRO)"

    is_initial_load = (role.lower() == "initial_load")

    # 3) cdc_seq validation only applies to the 8-char (primary) form
    cdc_seq_s = ""
    if not is_initial_load:
        cdc_seq_s = str(cdc_seq or "").strip()
        if len(cdc_seq_s) != 2 or not cdc_seq_s.isalnum():
            return None, f"cdc_seq must be exactly 2 alphanumeric characters (letters and/or digits), got: {cdc_seq!r}"

    # 4) list replicats on the deployment to find which NNs are already used
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.replicat_ops import list_replicats
    except ImportError as e:
        return None, f"failed to import oggctl modules: {e}"
    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
        replicats = list_replicats(client)
    except Exception as e:
        return None, f"failed to list replicats on '{deployment}': {e}"

    if is_initial_load:
        pattern  = re.compile(rf"^RI([0-9A-Z]{{2}}){env_letter}$", re.IGNORECASE)
        prefix   = f"RI"
        suffix   = env_letter
        slot_lbl = f"RI<NN>{env_letter}"
    else:
        pattern  = re.compile(rf"^R{role_letter}{cdc_seq_s}(\d{{2}}){env_letter}$")
        prefix   = f"R{role_letter}{cdc_seq_s}"
        suffix   = env_letter
        slot_lbl = f"R{role_letter}{cdc_seq_s}NN{env_letter}"
    if is_initial_load:
        used = {_b36_to_nn(m.group(1)) for m in (pattern.match(r["name"]) for r in replicats) if m}
    else:
        used = {int(m.group(1)) for m in (pattern.match(r["name"]) for r in replicats) if m}

    if is_initial_load and prefer_nn is not None and 0 <= int(prefer_nn) <= 1295 and int(prefer_nn) not in used:
        chosen_nn = int(prefer_nn)
    else:
        chosen_nn = next((nn for nn in range(1296) if nn not in used), None)
        if chosen_nn is None:
            return None, f"sequential exhausted (max 1295) for pattern {slot_lbl}"

    return f"{prefix}{_nn_to_b36(chosen_nn)}{suffix}", "OK"


def get_replicats_for_tables(deployment_names, tables):
    # For each replicat deployment, finds which replicats already MAP at least one of the
    # configured tables (OGG wildcards in MAP entries are honored).
    #
    # NOTE: REUSE detection now uses get_replicats_for_trail() because matching by tables
    # is unreliable — a replicat created previously for table A is "missed" when the new
    # request asks for table B even though it consumes from the same primary extract trail.
    # This function is kept for callers that still need table-based lookups (e.g. validate's
    # conflict check).
    #
    # Returns dict {deployment_name: [replicat_name, ...]}; each list contains the names of
    # replicats in that deployment that cover at least one configured table (empty list if none).
    patterns = get_tables_in_use_by_replicats(deployment_names)
    result   = {dep: [] for dep in deployment_names}
    for dep in deployment_names:
        seen = set()
        for t in tables:
            for d, rep, _pat in find_processes_for_table(t, patterns):
                if d == dep and rep not in seen:
                    result[dep].append(rep)
                    seen.add(rep)
    return result


def get_replicats_for_trail(deployment_names, expected_trail):
    # REUSE detection for primary replicats based on the SOURCE TRAIL they consume from.
    #
    # Why trail-based and not table-based:
    #   In cdc-controller a primary replicat is uniquely tied to a primary extract (one per
    #   source BBDD per Extract deployment). Two cdc-controller runs against the same BBDD
    #   share the same primary extract and hence the same trail. The replicat reading that
    #   trail is the one to REUSE, regardless of whether the new request happens to include
    #   any of the tables that the existing replicat already maps.
    #
    # 'expected_trail' is the full logical trail path as written when creating the extract,
    # e.g. 'primary/ACNPDB/EPPI00D/pr'. Comparison is done against the replicat's source
    # ('source.path' + '/' + 'source.name') from the OGG REST resource.
    #
    # Returns dict {deployment_name: [replicat_name, ...]}; only replicats whose source trail
    # matches exactly are included.
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.replicat_ops import list_replicats
    except ImportError as e:
        print_log("error", f"failed to import oggctl modules: {e}")
        return {dep: [] for dep in deployment_names}

    expected = (expected_trail or "").strip().strip("/").upper()
    result   = {dep: [] for dep in deployment_names}
    if not expected:
        return result

    for dep_name in deployment_names:
        try:
            cfg = load_cfg(dep_name)
            maybe_suppress_tls_warning(cfg)
            client = build_client(cfg)
            replicats = list_replicats(client)
        except Exception as e:
            print_log("error", f"failed to list replicats on deployment '{dep_name}': {clean_api_error(e)}")
            continue

        for rep in replicats:
            rep_name = rep.get("name", "")
            if not rep_name:
                continue
            try:
                r = client.get(f"/replicats/{rep_name}")
                if r.status_code != 200:
                    continue
                source   = (r.json().get("response", {}) or {}).get("source", {}) or {}
                src_name = (source.get("name") or "").strip().strip("/")
                src_path = (source.get("path") or "").strip().strip("/")
                full = f"{src_path}/{src_name}" if src_path else src_name
                full = full.upper()
            except Exception as e:
                print_log("warning", f"could not read source trail of replicat '{rep_name}' on '{dep_name}': {clean_api_error(e)}")
                continue

            if full == expected:
                result[dep_name].append(rep_name)
    return result


def get_replicat_target_info(dep_name, replicat_name):
    # Determine the target type AND target-specific details of an existing replicat by
    # inspecting its .properties file (Big Data) or PRM (Oracle).
    #
    # Returns: (target_type, details_dict) where:
    #   target_type : 'kafka_confluent' | 's3' | 'oracle' | None
    #   details_dict: target-specific identifiers for unambiguous matching when multiple
    #                 replicats share the same target type on a trail:
    #     s3             → {"s3_bucket_name": "<bucket>", "s3_bucket_path": "<path>"}
    #     kafka_confluent→ {"cluster_name_norm": "<name_with_dashes_as_underscores>",
    #                       "identity_pool_norm": "<pool_with_dashes_as_underscores>",
    #                       "producer_config_file": "<full_path>"}
    #     oracle / None  → {}
    #
    #   None is returned as the type when detection fails; callers treat None as "no match".
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.replicat_ops import get_replicat_properties
        from api.errors       import OGGAPIError
    except ImportError as e:
        print_log("warning", f"could not import oggctl for target-type detection on '{dep_name}': {e}")
        return None, {}

    try:
        cfg = load_cfg(dep_name)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
    except Exception as e:
        print_log("warning", f"could not build OGG client for target-type detection on '{dep_name}': {clean_api_error(e)}")
        return None, {}

    # 1) Try Big Data .properties file
    try:
        props = get_replicat_properties(client, replicat_name)

        if props.get("gg.target", "").lower() == "s3":
            return "s3", {
                "s3_bucket_name": props.get("gg.eventhandler.s3.bucketMappingTemplate", ""),
                "s3_bucket_path": props.get("gg.eventhandler.s3.pathMappingTemplate", ""),
            }

        if "kafkaconnect" in props.get("gg.handlerlist", "").lower():
            producer_file = props.get("gg.handler.kafkaconnect.kafkaProducerConfigFile", "")
            # Extract normalised cluster_name and identity_pool from the producer config file
            # path (template: kafka_producer_{cluster_norm}_{pool_norm}_{env}_oauth.properties).
            import os as _os
            basename = _os.path.basename(producer_file)          # kafka_producer_X_Y_dev_oauth.properties
            stem     = basename.replace("kafka_producer_", "", 1).replace("_oauth.properties", "")
            # stem = "{cluster_norm}_{pool_norm}_{env}" — split off the env suffix
            parts = stem.rsplit("_", 1)                          # ["cluster_norm_pool_norm", env]
            stem_no_env = parts[0] if len(parts) == 2 else stem
            return "kafka_confluent", {
                "producer_config_file": producer_file,
                "producer_config_stem": stem_no_env,             # cluster+pool normalised, for substring match
            }

        return None, {}

    except OGGAPIError:
        # 404 → no .properties file → likely Oracle replicat; fall through to PRM check
        pass
    except Exception as e:
        print_log("warning", f"could not read .properties for replicat '{replicat_name}' on '{dep_name}': {clean_api_error(e)}")
        return None, {}

    # 2) No .properties file — check PRM for USERIDALIAS (Oracle target)
    try:
        r = client.get(f"/replicats/{replicat_name}")
        if r.status_code == 200:
            prm = r.json().get("response", {}).get("config", []) or []
            if any("USERIDALIAS" in line.upper() for line in prm):
                return "oracle", {}
    except Exception as e:
        print_log("warning", f"could not read PRM for replicat '{replicat_name}' on '{dep_name}': {clean_api_error(e)}")

    return None, {}


def get_replicat_target_type(dep_name, replicat_name):
    # Compatibility wrapper — returns only the target_type string.
    ttype, _ = get_replicat_target_info(dep_name, replicat_name)
    return ttype


def add_tables_to_extract(deployment, extract_name, new_table_names, comment=None):
    # Adds TABLE entries to the PRM of an existing Extract, skipping tables that are already present.
    # If 'comment' is provided, each newly-added line is suffixed with '  -- <comment>' so the
    # audit trail (TAGS JSON, etc.) sticks to every line, mirroring what is done at extract create.
    # If the extract is RUNNING, it will be stopped, patched, and restarted.
    # Returns (ok: bool, msg: str).
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.extract_ops  import get_extract_status, start_extract, stop_extract
        from api.errors       import OGGAPIError
    except ImportError as e:
        return False, f"failed to import oggctl modules: {e}"

    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
    except Exception as e:
        return False, f"failed to build OGG client for '{deployment}': {e}"

    # 1) fetch current PRM
    try:
        r = client.get(f"/extracts/{extract_name}")
        if r.status_code != 200:
            return False, f"failed to fetch extract: HTTP {r.status_code}"
        prm_lines = r.json().get("response", {}).get("config", []) or []
    except Exception as e:
        return False, f"failed to read PRM: {e}"

    # 2) collect TABLE entries already in PRM (normalize 3-part PDB.SCHEMA.TABLE -> SCHEMA.TABLE)
    existing = set()
    for line in prm_lines:
        s = line.strip()
        if not s or s.startswith("--") or not s.upper().startswith("TABLE "):
            continue
        rest = s[6:].strip().rstrip(";").strip()
        m = re.match(r"([A-Za-z0-9_$.]+)", rest)
        if not m:
            continue
        name  = m.group(1).upper()
        parts = name.split(".")
        if len(parts) == 3:
            name = f"{parts[1]}.{parts[2]}"
        existing.add(name)

    # 3) classify each requested table: skip if already in PRM (with warning), keep new ones
    new_tables     = []
    skipped_tables = []
    for t in new_table_names:
        tu = t.upper()
        if tu in existing:
            skipped_tables.append(tu)
            print_log("warning", f"  skipping table '{tu}' : already exists in extract '{extract_name}' PRM")
        else:
            new_tables.append(tu)

    if not new_tables:
        return True, f"no new tables to add (all {len(new_table_names)} already in extract)"

    # 4) build updated PRM by appending new TABLE lines (each suffixed with the audit comment if any)
    suffix = f"  -- {comment}" if comment else ""
    updated_prm = list(prm_lines) + [f"TABLE {t};{suffix}" for t in new_tables]

    # alignment width for the ':' separators (auto-scales to extract name length)
    _W = max(38, len(f"  extract '{extract_name}' current status"))

    # 5) stop extract if currently running
    was_running = False
    try:
        status = get_extract_status(client, extract_name)
        print_log("info", f"{f'  extract {extract_name!r} current status':<{_W}}: {status}")
        was_running = status.upper() == "RUNNING"
        if was_running:
            print_log("info", f"{f'  stopping extract {extract_name!r}':<{_W}}: before PRM update ...")
            stop_extract(client, extract_name)
            print_log("info", f"{f'  extract {extract_name!r} stopped':<{_W}}: OK")
    except OGGAPIError as e:
        return False, f"failed to stop extract: {e}"

    # 6) PATCH the config
    try:
        r = client.patch(f"/extracts/{extract_name}", {"config": updated_prm})
        if r.status_code not in (200, 202, 204):
            return False, f"failed to update PRM: HTTP {r.status_code}"
    except Exception as e:
        return False, f"failed to update PRM: {e}"

    # 7) restart extract if we stopped it
    if was_running:
        try:
            print_log("info", f"{f'  starting extract {extract_name!r}':<{_W}}: after PRM update ...")
            start_extract(client, extract_name)
            print_log("info", f"{f'  extract {extract_name!r} started':<{_W}}: OK")
        except OGGAPIError as e:
            return False, f"PRM updated ({len(new_tables)} new table(s)) but failed to restart extract: {e}"

    return True, f"added {len(new_tables)} new table(s): {', '.join(new_tables)}"


def add_tables_to_replicat(deployment, replicat_name, new_table_names, comment=None, column_map=None):
    # Mirror of add_tables_to_extract but for Replicats. Adds MAP entries to an existing
    # replicat's PRM. Tables that are ALREADY covered by an existing MAP entry (OGG wildcards
    # like 'MAP *.*' or 'MAP SOE.*' are honored, MAPEXCLUDE is honored too) are skipped.
    # If 'comment' is provided each newly added line is suffixed with '  -- <comment>' so the
    # audit trail (TAGS JSON, etc.) sticks to every line, mirroring the CREATE convention.
    # The added MAPs use 1-to-1 source/target naming: 'map SCHEMA.TABLE, target SCHEMA.TABLE;'.
    # If the replicat is RUNNING, it will be stopped, patched, and restarted.
    # Returns (ok: bool, msg: str).
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.replicat_ops import get_replicat_status, start_replicat, stop_replicat
        from api.errors       import OGGAPIError
    except ImportError as e:
        return False, f"failed to import oggctl modules: {e}"

    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
    except Exception as e:
        return False, f"failed to build OGG client for '{deployment}': {e}"

    # 1) fetch current PRM
    try:
        r = client.get(f"/replicats/{replicat_name}")
        if r.status_code != 200:
            return False, f"failed to fetch replicat: HTTP {r.status_code}"
        prm_lines = r.json().get("response", {}).get("config", []) or []
    except Exception as e:
        return False, f"failed to read PRM: {e}"

    # 2) Parse existing MAP / MAPEXCLUDE patterns. Both directives may contain OGG wildcards
    #    (* / ?) which we must honor when deciding if a requested table is already covered.
    def _extract_first_pattern(rest_text):
        m = re.match(r"([A-Za-z0-9_$.*?]+)", rest_text)
        if not m:
            return None
        pat = m.group(1).upper()
        parts = pat.split(".")
        if len(parts) == 3:
            pat = f"{parts[1]}.{parts[2]}"
        return pat

    includes = []
    excludes = []
    for line in prm_lines:
        s = line.strip()
        if not s or s.startswith("--"):
            continue
        u = s.upper()
        if u.startswith("MAPEXCLUDE "):
            rest = s[len("MAPEXCLUDE "):].strip().rstrip(";").strip()
            pat  = _extract_first_pattern(rest)
            if pat:
                excludes.append(pat)
        elif u.startswith("MAP "):
            rest = s[len("MAP "):].strip().rstrip(";").strip()
            pat  = _extract_first_pattern(rest)
            if pat:
                includes.append(pat)

    # 3) Classify each requested table — skip if covered (include match AND not excluded), keep others.
    new_tables     = []
    skipped_tables = []
    for t in new_table_names:
        tu = t.upper()
        excluded = any(fnmatch.fnmatchcase(tu, ex) for ex in excludes)
        covered  = (not excluded) and any(fnmatch.fnmatchcase(tu, inc) for inc in includes)
        if covered:
            skipped_tables.append(tu)
            print_log("warning", f"  skipping table '{tu}' : already covered by existing MAP in replicat '{replicat_name}' PRM")
        else:
            new_tables.append(tu)

    if not new_tables:
        return True, f"no new tables to add (all {len(new_table_names)} already covered)"

    # 4) Build updated PRM by appending new MAP lines (source = target convention).
    # The replicat j2 template leaves trailing blank lines at the end of the PRM, so
    # strip blank/whitespace-only trailing entries before concatenating — otherwise the
    # new MAPs would be appended AFTER those blanks and end up visually separated from
    # the existing ones by several empty lines.
    suffix = f"  -- {comment}" if comment else ""
    new_lines = []
    for t in new_tables:
        if "." in t:
            schema, table = t.split(".", 1)
        else:
            schema, table = "", t
        cols = (column_map or {}).get(t)
        colmap_clause = f", COLMAP({','.join(f'{c}={c}' for c in cols)})" if cols else ""
        new_lines.append(f"map {schema}.{table}, target {schema}.{table}{colmap_clause};{suffix}")
    base_prm = list(prm_lines)
    while base_prm and not base_prm[-1].strip():
        base_prm.pop()
    updated_prm = base_prm + new_lines

    # alignment width for the ':' separators
    _W = max(38, len(f"  replicat '{replicat_name}' current status"))

    # 5) stop replicat if currently running
    was_running = False
    try:
        status = get_replicat_status(client, replicat_name)
        print_log("info", f"{f'  replicat {replicat_name!r} current status':<{_W}}: {status}")
        was_running = str(status).upper() == "RUNNING"
        if was_running:
            print_log("info", f"{f'  stopping replicat {replicat_name!r}':<{_W}}: before PRM update ...")
            stop_replicat(client, replicat_name)
            print_log("info", f"{f'  replicat {replicat_name!r} stopped':<{_W}}: OK")
    except OGGAPIError as e:
        return False, f"failed to stop replicat: {e}"

    # 6) PATCH the config
    try:
        r = client.patch(f"/replicats/{replicat_name}", {"config": updated_prm})
        if r.status_code not in (200, 202, 204):
            return False, f"failed to update PRM: HTTP {r.status_code}"
    except Exception as e:
        return False, f"failed to update PRM: {e}"

    # 7) restart replicat if we stopped it
    if was_running:
        try:
            print_log("info", f"{f'  starting replicat {replicat_name!r}':<{_W}}: after PRM update ...")
            start_replicat(client, replicat_name)
            print_log("info", f"{f'  replicat {replicat_name!r} started':<{_W}}: OK")
        except OGGAPIError as e:
            return False, f"PRM updated ({len(new_tables)} new MAP(s)) but failed to restart replicat: {e}"

    return True, f"added {len(new_tables)} new MAP(s): {', '.join(new_tables)}"


def update_tables_in_replicat(deployment, replicat_name, table_column_map, comment=None):
    # Updates (replaces) existing MAP entries in a replicat PRM and appends new ones.
    #
    # table_column_map: dict {SCHEMA.TABLE (uppercase) -> list[str] | None}
    #   - list[str]  : new explicit column allow-list → generates COLMAP(COL=COL,...)
    #   - None       : no column restriction (all columns, no COLMAP clause)
    #
    # For each table in table_column_map:
    #   - MAP line already in PRM  → the line is REPLACED with the updated version
    #   - MAP line missing from PRM → a new MAP line is appended
    # Tables NOT in table_column_map are left untouched.
    # If 'comment' is provided it is appended as '  -- <comment>' on every modified/added line.
    # If the replicat is RUNNING it is stopped, patched, then restarted.
    # Returns (ok: bool, msg: str).
    if not table_column_map:
        return True, "no tables in update map"

    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.replicat_ops import get_replicat_status, start_replicat, stop_replicat
        from api.errors       import OGGAPIError
    except ImportError as e:
        return False, f"failed to import oggctl modules: {e}"

    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
    except Exception as e:
        return False, f"failed to build OGG client for '{deployment}': {e}"

    # 1) fetch current PRM
    try:
        r = client.get(f"/replicats/{replicat_name}")
        if r.status_code != 200:
            return False, f"failed to fetch replicat: HTTP {r.status_code}"
        prm_lines = r.json().get("response", {}).get("config", []) or []
    except Exception as e:
        return False, f"failed to read PRM: {e}"

    # Build one MAP line (mirrors format_replicat_map_line, compact COLMAP format).
    def _fmt_map_line(schema, table, columns, suffix=""):
        base = f"map {schema}.{table}, target {schema}.{table}"
        if columns:
            colmap = ",".join(f"{c}={c}" for c in columns)
            return f"{base}, COLMAP({colmap});{suffix}"
        return f"{base};{suffix}"

    # Extract source SCHEMA.TABLE from a MAP PRM line.
    _MAP_PAT = re.compile(r"^map\s+([A-Za-z0-9_$.*?]+)\s*,", re.IGNORECASE)

    def _parse_map_source(line):
        m = _MAP_PAT.match(line.strip())
        if not m:
            return None
        src = m.group(1).upper()
        parts = src.split(".")
        # Strip catalog prefix from 3-part names (catalog.schema.table → schema.table)
        if len(parts) == 3:
            src = f"{parts[1]}.{parts[2]}"
        return src

    # 2) Index of each MAP line by source table.
    map_line_idx = {}
    for i, line in enumerate(prm_lines):
        src = _parse_map_source(line)
        if src:
            map_line_idx[src] = i

    suffix     = f"  -- {comment}" if comment else ""
    updated_prm = list(prm_lines)
    replaced   = []
    to_append  = []

    # 3) Classify each requested table: replace existing or append new.
    for raw_name, columns in table_column_map.items():
        tu = raw_name.strip().upper()
        if "." not in tu:
            continue
        schema, table = tu.split(".", 1)
        new_line = _fmt_map_line(schema, table, columns, suffix)
        if tu in map_line_idx:
            if updated_prm[map_line_idx[tu]] != new_line:
                updated_prm[map_line_idx[tu]] = new_line
                replaced.append(tu)
        else:
            to_append.append((tu, new_line))

    if not replaced and not to_append:
        return True, "no changes needed (all MAP lines already match)"

    # 4) Append new MAP lines (strip trailing blanks first, mirrors add_tables_to_replicat).
    while updated_prm and not updated_prm[-1].strip():
        updated_prm.pop()
    for _, new_line in to_append:
        updated_prm.append(new_line)

    added = [t for t, _ in to_append]

    _W = max(38, len(f"  replicat '{replicat_name}' current status"))

    # 5) Stop if running.
    was_running = False
    try:
        status = get_replicat_status(client, replicat_name)
        print_log("info", f"{f'  replicat {replicat_name!r} current status':<{_W}}: {status}")
        was_running = str(status).upper() == "RUNNING"
        if was_running:
            print_log("info", f"{f'  stopping replicat {replicat_name!r}':<{_W}}: before PRM update ...")
            stop_replicat(client, replicat_name)
            print_log("info", f"{f'  replicat {replicat_name!r} stopped':<{_W}}: OK")
    except OGGAPIError as e:
        return False, f"failed to stop replicat: {e}"

    # 6) PATCH the config.
    try:
        r = client.patch(f"/replicats/{replicat_name}", {"config": updated_prm})
        if r.status_code not in (200, 202, 204):
            return False, f"failed to update PRM: HTTP {r.status_code}"
    except Exception as e:
        return False, f"failed to update PRM: {e}"

    # 7) Restart if it was running.
    if was_running:
        try:
            print_log("info", f"{f'  starting replicat {replicat_name!r}':<{_W}}: after PRM update ...")
            start_replicat(client, replicat_name)
            print_log("info", f"{f'  replicat {replicat_name!r} started':<{_W}}: OK")
        except OGGAPIError as e:
            parts_fail = []
            if replaced:
                parts_fail.append(f"replaced {len(replaced)}")
            if added:
                parts_fail.append(f"added {len(added)}")
            return False, f"PRM updated ({', '.join(parts_fail)} MAP(s)) but failed to restart replicat: {e}"

    parts_msg = []
    if replaced:
        parts_msg.append(f"replaced {len(replaced)}: {', '.join(replaced)}")
    if added:
        parts_msg.append(f"added {len(added)}: {', '.join(added)}")
    return True, "; ".join(parts_msg)


def remove_tables_from_extract(deployment, extract_name, tables_to_remove):
    # Removes 'TABLE <name>;' lines from an existing Extract's PRM for the requested
    # exact-name tables. Wildcards are NOT honored on the OGG side here: a PRM line such
    # as 'TABLE SOE.*;' is intentionally left untouched even if the caller passed
    # 'SOE.LOGON' — the operator is warned that the requested table is covered by a
    # wildcard and that the process is NOT modified for it.
    #
    # If the extract is RUNNING it is stopped, patched and restarted (mirrors the
    # add_tables_to_extract flow).
    #
    # Returns (ok: bool, removed: list[str], remaining_tables: int, msg: str):
    #   - removed         : tables actually deleted from the PRM (uppercased OWNER.TABLE)
    #   - remaining_tables: count of TABLE lines still present in the PRM after the patch
    #                        (used by the caller to decide whether the extract should be
    #                         fully deleted because it ended up empty).
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.extract_ops  import get_extract_status, start_extract, stop_extract
        from api.errors       import OGGAPIError
    except ImportError as e:
        return False, [], 0, f"failed to import oggctl modules: {e}"

    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
    except Exception as e:
        return False, [], 0, f"failed to build OGG client for '{deployment}': {e}"

    # 1) fetch current PRM
    try:
        r = client.get(f"/extracts/{extract_name}")
        if r.status_code != 200:
            return False, [], 0, f"failed to fetch extract: HTTP {r.status_code}"
        prm_lines = r.json().get("response", {}).get("config", []) or []
    except Exception as e:
        return False, [], 0, f"failed to read PRM: {e}"

    # 2) walk the PRM identifying which TABLE lines belong to which name (normalising
    # 3-part 'PDB.SCHEMA.TABLE' -> 'SCHEMA.TABLE') and which lines contain wildcards.
    targets = {t.upper() for t in (tables_to_remove or [])}
    new_prm           = []
    removed           = []
    wildcard_covered  = []   # requested tables that are only covered by a wildcard line
    remaining_tables  = 0
    for line in prm_lines:
        s = line.strip()
        if not s or s.startswith("--") or not s.upper().startswith("TABLE "):
            new_prm.append(line)
            continue
        rest = s[6:].strip().rstrip(";").strip()
        m = re.match(r"([A-Za-z0-9_$.*?]+)", rest)
        if not m:
            new_prm.append(line)
            continue
        pat = m.group(1).upper()
        parts = pat.split(".")
        if len(parts) == 3:
            pat = f"{parts[1]}.{parts[2]}"
        is_wildcard = ("*" in pat or "?" in pat)
        if is_wildcard:
            new_prm.append(line)
            remaining_tables += 1
            for t in targets:
                if fnmatch.fnmatchcase(t, pat) and t not in wildcard_covered:
                    wildcard_covered.append(t)
            continue
        if pat in targets:
            removed.append(pat)
            continue   # drop the line
        new_prm.append(line)
        remaining_tables += 1

    # Warn about wildcard-covered requested tables (we do NOT add TABLEEXCLUDE — that
    # would be a behavioural change to a process that the operator may not expect).
    for t in wildcard_covered:
        print_log("warning", f"  table '{t}' is covered by a wildcard TABLE entry on extract '{extract_name}' — extract NOT modified for this table")

    not_found = [t for t in targets if t not in removed and t not in wildcard_covered]
    for t in not_found:
        print_log("warning", f"  table '{t}' not found in extract '{extract_name}' PRM — nothing to remove for this table")

    if not removed:
        return True, [], remaining_tables, "no TABLE lines removed from PRM"

    # alignment width for the ':' separators (auto-scales to extract name length)
    _W = max(38, len(f"  extract '{extract_name}' current status"))

    # 3) stop extract if currently running
    was_running = False
    try:
        status = get_extract_status(client, extract_name)
        print_log("info", f"{f'  extract {extract_name!r} current status':<{_W}}: {status}")
        was_running = str(status).upper() == "RUNNING"
        if was_running:
            print_log("info", f"{f'  stopping extract {extract_name!r}':<{_W}}: before PRM update ...")
            stop_extract(client, extract_name)
            print_log("info", f"{f'  extract {extract_name!r} stopped':<{_W}}: OK")
    except OGGAPIError as e:
        return False, [], remaining_tables, f"failed to stop extract: {e}"

    # 4) PATCH the config
    try:
        r = client.patch(f"/extracts/{extract_name}", {"config": new_prm})
        if r.status_code not in (200, 202, 204):
            return False, [], remaining_tables, f"failed to update PRM: HTTP {r.status_code}"
    except Exception as e:
        return False, [], remaining_tables, f"failed to update PRM: {e}"

    # 5) restart extract if we stopped it AND there is still something to capture.
    # If the extract ended up empty we don't restart it — the caller will full-delete it.
    if was_running and remaining_tables > 0:
        try:
            print_log("info", f"{f'  starting extract {extract_name!r}':<{_W}}: after PRM update ...")
            start_extract(client, extract_name)
            print_log("info", f"{f'  extract {extract_name!r} started':<{_W}}: OK")
        except OGGAPIError as e:
            return False, removed, remaining_tables, f"PRM updated ({len(removed)} TABLE(s) removed) but failed to restart extract: {e}"

    return True, removed, remaining_tables, f"removed {len(removed)} TABLE(s): {', '.join(removed)}"


def remove_tables_from_replicat(deployment, replicat_name, tables_to_remove):
    # Mirror of remove_tables_from_extract for Replicats. Removes 'MAP <name>, target ...'
    # lines whose source matches the requested table names (exact match — wildcards left
    # alone with a warning). Honors RUNNING state: stop, patch, restart. Returns
    # (ok, removed: list[str], remaining_maps: int, msg).
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.replicat_ops import get_replicat_status, start_replicat, stop_replicat
        from api.errors       import OGGAPIError
    except ImportError as e:
        return False, [], 0, f"failed to import oggctl modules: {e}"

    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
    except Exception as e:
        return False, [], 0, f"failed to build OGG client for '{deployment}': {e}"

    try:
        r = client.get(f"/replicats/{replicat_name}")
        if r.status_code != 200:
            return False, [], 0, f"failed to fetch replicat: HTTP {r.status_code}"
        prm_lines = r.json().get("response", {}).get("config", []) or []
    except Exception as e:
        return False, [], 0, f"failed to read PRM: {e}"

    targets = {t.upper() for t in (tables_to_remove or [])}
    new_prm          = []
    removed          = []
    wildcard_covered = []
    remaining_maps   = 0
    for line in prm_lines:
        s = line.strip()
        if not s or s.startswith("--") or not s.upper().startswith("MAP "):
            new_prm.append(line)
            continue
        rest = s[len("MAP "):].strip().rstrip(";").strip()
        m = re.match(r"([A-Za-z0-9_$.*?]+)", rest)
        if not m:
            new_prm.append(line)
            continue
        pat = m.group(1).upper()
        parts = pat.split(".")
        if len(parts) == 3:
            pat = f"{parts[1]}.{parts[2]}"
        is_wildcard = ("*" in pat or "?" in pat)
        if is_wildcard:
            new_prm.append(line)
            remaining_maps += 1
            for t in targets:
                if fnmatch.fnmatchcase(t, pat) and t not in wildcard_covered:
                    wildcard_covered.append(t)
            continue
        if pat in targets:
            removed.append(pat)
            continue
        new_prm.append(line)
        remaining_maps += 1

    for t in wildcard_covered:
        print_log("warning", f"  table '{t}' is covered by a wildcard MAP entry on replicat '{replicat_name}' — replicat NOT modified for this table")
    not_found = [t for t in targets if t not in removed and t not in wildcard_covered]
    for t in not_found:
        print_log("warning", f"  table '{t}' not found in replicat '{replicat_name}' PRM — nothing to remove for this table")

    if not removed:
        return True, [], remaining_maps, "no MAP lines removed from PRM"

    _W = max(38, len(f"  replicat '{replicat_name}' current status"))

    was_running = False
    try:
        status = get_replicat_status(client, replicat_name)
        print_log("info", f"{f'  replicat {replicat_name!r} current status':<{_W}}: {status}")
        was_running = str(status).upper() == "RUNNING"
        if was_running:
            print_log("info", f"{f'  stopping replicat {replicat_name!r}':<{_W}}: before PRM update ...")
            stop_replicat(client, replicat_name)
            print_log("info", f"{f'  replicat {replicat_name!r} stopped':<{_W}}: OK")
    except OGGAPIError as e:
        return False, [], remaining_maps, f"failed to stop replicat: {e}"

    try:
        r = client.patch(f"/replicats/{replicat_name}", {"config": new_prm})
        if r.status_code not in (200, 202, 204):
            return False, [], remaining_maps, f"failed to update PRM: HTTP {r.status_code}"
    except Exception as e:
        return False, [], remaining_maps, f"failed to update PRM: {e}"

    if was_running and remaining_maps > 0:
        try:
            print_log("info", f"{f'  starting replicat {replicat_name!r}':<{_W}}: after PRM update ...")
            start_replicat(client, replicat_name)
            print_log("info", f"{f'  replicat {replicat_name!r} started':<{_W}}: OK")
        except OGGAPIError as e:
            return False, removed, remaining_maps, f"PRM updated ({len(removed)} MAP(s) removed) but failed to restart replicat: {e}"

    return True, removed, remaining_maps, f"removed {len(removed)} MAP(s): {', '.join(removed)}"


def delete_extract_full(deployment, extract_name, unregister=False):
    # Stop (if running) + optionally unregister + delete the extract. Used by DELETE to
    # remove an extract that should no longer exist:
    #   - initial_load extracts: 'unregister' is False (they are not registered as Oracle
    #     LogMiner capture). Just stop + delete.
    #   - primary integrated extracts: 'unregister' is True. We invoke 'oggctl extract
    #     unregister' before delete so the OGG$CAP_<name> capture process is cleaned up
    #     on the source DB; otherwise leftover capture metadata stays around.
    # Returns (ok: bool, msg: str). Errors are surfaced; this function does NOT raise.
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.extract_ops  import get_extract_status, stop_extract, delete_extract
        from api.errors       import OGGAPIError
    except ImportError as e:
        return False, f"failed to import oggctl modules: {e}"

    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
    except Exception as e:
        return False, f"failed to build OGG client for '{deployment}': {e}"

    # 1) stop if running
    try:
        try:
            status = get_extract_status(client, extract_name)
        except OGGAPIError:
            status = "UNKNOWN"
        if str(status).upper() == "RUNNING":
            stop_extract(client, extract_name)
    except OGGAPIError as e:
        return False, f"failed to stop extract: {e}"

    # 2) unregister (integrated only) — best effort, do not abort if it fails
    if unregister:
        try:
            oggctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl", "oggctl.py")
            import sys as _sys, subprocess as _sub
            result = _sub.run(
                [_sys.executable, oggctl_path, "extract", "cmd", extract_name, "UNREGISTER", "--deployment", deployment],
                capture_output=True, text=True, timeout=120,
            )
            if result.returncode != 0:
                err = (result.stdout or "") + (result.stderr or "")
                print_log("warning", f"  extract '{extract_name}' UNREGISTER returned non-zero (continuing with delete): {err.strip().splitlines()[-1] if err.strip() else 'unknown'}")
        except Exception as e:
            print_log("warning", f"  could not invoke UNREGISTER for '{extract_name}': {e} (continuing with delete)")

    # 3) delete
    try:
        delete_extract(client, extract_name)
    except OGGAPIError as e:
        return False, f"failed to delete extract: {e}"
    return True, "OK"


def delete_replicat_full(deployment, replicat_name):
    # Stop (if running) + delete the replicat. Used by DELETE for initial_load replicats
    # (full delete) and for primary replicats that ended up empty after a partial delete.
    # If the replicat inherits 'critical=true' from a managed-process-profile, OGG would
    # auto-restart it on stop — we clear the 'critical' flag first as a per-process
    # override (the profile itself is untouched). Returns (ok: bool, msg: str).
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.replicat_ops import get_replicat_status, stop_replicat, delete_replicat, update_replicat
        from api.errors       import OGGAPIError
    except ImportError as e:
        return False, f"failed to import oggctl modules: {e}"

    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
    except Exception as e:
        return False, f"failed to build OGG client for '{deployment}': {e}"

    # 1) clear the critical flag so OGG doesn't auto-restart the replicat after stop
    try:
        update_replicat(client, replicat_name, critical=False)
    except OGGAPIError as e:
        print_log("warning", f"  could not clear 'critical' flag on replicat '{replicat_name}': {e} (continuing)")
    except Exception as e:
        print_log("warning", f"  could not clear 'critical' flag on replicat '{replicat_name}': {e} (continuing)")

    # 2) stop if running
    try:
        try:
            status = get_replicat_status(client, replicat_name)
        except OGGAPIError:
            status = "UNKNOWN"
        if str(status).upper() == "RUNNING":
            stop_replicat(client, replicat_name)
    except OGGAPIError as e:
        return False, f"failed to stop replicat: {e}"

    # 3) delete
    try:
        delete_replicat(client, replicat_name)
    except OGGAPIError as e:
        return False, f"failed to delete replicat: {e}"
    return True, "OK"


def get_extracts_for_bbdd(deployment_names, bbdd_name, env):
    # Inspects every Extract on the given deployments and returns those whose USERIDALIAS
    # matches the source-DB credential alias for this bbdd_name/env (convention: <BBDD>_PRIM_<ENV>).
    # Returns dict {deployment_name: [extract_name, ...]}.
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.extract_ops  import list_extracts
        from api.prm_builder  import get_useridalias_from_prm
    except ImportError as e:
        print_log("error", f"failed to import oggctl modules: {e}")
        return {dep: [] for dep in deployment_names}

    expected_alias = f"{bbdd_name}_PRIM_{env}".upper()
    result = {dep: [] for dep in deployment_names}

    for dep_name in deployment_names:
        try:
            cfg = load_cfg(dep_name)
            maybe_suppress_tls_warning(cfg)
            client = build_client(cfg)
            extracts = list_extracts(client)
        except Exception as e:
            print_log("error", f"failed to list extracts on deployment '{dep_name}': {clean_api_error(e)}")
            continue

        for ext in extracts:
            ext_name = ext["name"]
            try:
                r = client.get(f"/extracts/{ext_name}")
                if r.status_code != 200:
                    continue
                prm_lines = r.json().get("response", {}).get("config", []) or []
            except Exception as e:
                print_log("warning", f"could not read PRM for extract '{ext_name}' on '{dep_name}': {clean_api_error(e)}")
                continue

            alias = get_useridalias_from_prm(prm_lines)
            if alias and alias.upper() == expected_alias:
                result[dep_name].append(ext_name)
    return result


def get_tables_in_use_by_replicats(deployment_names):
    # Returns list of (deployment, replicat_name, [includes], [excludes]) — one entry per
    # Replicat that has at least one MAP / MAPEXCLUDE directive in its PRM. 'includes' /
    # 'excludes' contain OWNER.TABLE patterns from the source side of MAP entries (may
    # include OGG wildcards '*' / '?'). Use find_processes_for_table() to match a concrete
    # table against these records honoring excludes.
    return _get_tables_in_use_by_processes(deployment_names, "replicats")


def prepare_oracle_connection(bbdd_name, env):
    # Prepares Oracle connection info for the given bbdd_name/env.
    # - Initializes Oracle Instant Client (thick mode)
    # - Looks up the unified entry '<bbdd_name>_<env>' in db_connections.yaml
    #   (entry holds conn_string_prim / conn_string_stby, db_user, and password_credential)
    # - Reads password from the env var named by conn['password_credential'] (from AWS SM vault)
    # Returns a dict with: db_password, db_pdb, dsns_to_check (list of (label, user, dsn) tuples).
    # Returns None on failure (errors are logged).
    try:
        import oracledb
    except ImportError:
        print_log("error", "oracledb module not installed. Run: pip install oracledb")
        return None

    try:
        oracledb.init_oracle_client()
    except Exception as e:
        print_log("error", f"Oracle Instant Client not found, thick mode unavailable: {e}")
        return None

    entry_key = f"{bbdd_name}_{env}"
    conn = get_db_connection(entry_key)
    if not conn:
        print_log("error", f"connection '{entry_key}' not found in db_connections.yaml")
        return None

    password_credential = conn.get("password_credential")
    if not password_credential:
        print_log("error", f"'password_credential' not defined in db_connections.yaml for '{entry_key}'")
        return None
    db_password = os.environ.get(password_credential)
    if not db_password:
        print_log("error", f"environment variable '{password_credential}' is not set")
        return None

    db_user         = conn.get("db_user", "")
    dsn_prim_string = conn.get("conn_string_prim", "")
    dsn_stby_string = conn.get("conn_string_stby", "")
    db_pdb          = conn.get("pdb", "")

    dsns_to_check = [("Primary:", db_user, dsn_prim_string)]
    if dsn_stby_string:
        dsns_to_check.append(("Standby:", db_user, dsn_stby_string))
    else:
        print_log("warning", "Standby Oracle connection string not detected, skipping standby checks")

    return {
        "db_password":   db_password,
        "db_pdb":        db_pdb,
        "dsns_to_check": dsns_to_check,
    }


def prepare_mongo_connection(bbdd_name, env):
    # Prepares MongoDB connection info for the given bbdd_name/env.
    # - Looks up '<bbdd_name>_<env>' in db_connections.yaml
    # - Reads password from the env var named by conn['password_credential'] (from AWS SM vault)
    # - Builds a MongoDB URI:
    #     * if conn_string_prim already starts with 'mongodb': inject credentials after the scheme
    #     * otherwise: assumed Atlas SRV format → mongodb+srv://user:password@conn_string_prim
    # Returns a dict with: uri, host, db_user. Returns None on failure (errors are logged).
    entry_key = f"{bbdd_name}_{env}"
    conn = get_db_connection(entry_key)
    if not conn:
        print_log("error", f"connection '{entry_key}' not found in db_connections.yaml")
        return None

    password_credential = conn.get("password_credential")
    if not password_credential:
        print_log("error", f"'password_credential' not defined in db_connections.yaml for '{entry_key}'")
        return None
    db_password = os.environ.get(password_credential)
    if not db_password:
        print_log("error", f"environment variable '{password_credential}' is not set")
        return None

    import urllib.parse
    db_user       = conn.get("db_user", "")
    conn_str_prim = conn.get("conn_string_prim", "")
    user_esc      = urllib.parse.quote_plus(db_user)
    pass_esc      = urllib.parse.quote_plus(db_password)

    if conn_str_prim.startswith("mongodb://") or conn_str_prim.startswith("mongodb+srv://"):
        scheme, rest = conn_str_prim.split("://", 1)
        uri = f"{scheme}://{user_esc}:{pass_esc}@{rest}"
    else:
        uri = f"mongodb+srv://{user_esc}:{pass_esc}@{conn_str_prim}"

    host = conn_str_prim.split("/")[0] if "/" in conn_str_prim else conn_str_prim
    # Extract db_name from the path component of conn_string_prim (e.g. "host.mongodb.net/mydb" → "mydb")
    db_name_from_conn = conn_str_prim.split("/", 1)[1].split("?")[0].strip() if "/" in conn_str_prim else ""
    db_name = db_name_from_conn or bbdd_name
    return {
        "uri":     uri,
        "host":    host,
        "db_user": db_user,
        "db_name": db_name,
    }


def prepare_db2_connection(bbdd_name, env):
    # Prepares DB2 connection info for the given bbdd_name/env.
    # - Looks up '<bbdd_name>_<env>' in db_connections.yaml
    # - Reads password from the env var named by conn['password_credential'] (from AWS SM vault)
    # - Builds a full DSN by appending UID= and PWD= to conn_string_prim
    # Returns a dict with: full_dsn, host, db_user. Returns None on failure (errors are logged).
    entry_key = f"{bbdd_name}_{env}"
    conn = get_db_connection(entry_key)
    if not conn:
        print_log("error", f"connection '{entry_key}' not found in db_connections.yaml")
        return None

    password_credential = conn.get("password_credential")
    if not password_credential:
        print_log("error", f"'password_credential' not defined in db_connections.yaml for '{entry_key}'")
        return None
    db_password = os.environ.get(password_credential)
    if not db_password:
        print_log("error", f"environment variable '{password_credential}' is not set")
        return None

    db_user       = conn.get("db_user", "")
    conn_str_prim = conn.get("conn_string_prim", "")
    full_dsn      = f"{conn_str_prim}UID={db_user};PWD={db_password};"

    host = ""
    port = 50000
    for part in conn_str_prim.split(";"):
        key = part.strip().upper()
        if key.startswith("HOSTNAME="):
            host = part.split("=", 1)[1].strip()
        elif key.startswith("PORT="):
            try: port = int(part.split("=", 1)[1].strip())
            except ValueError: pass

    return {
        "full_dsn": full_dsn,
        "host":     host,
        "port":     port,
        "db_user":  db_user,
    }


def count_rows_as_of_scn(bbdd_name, env, owner_table, sqlpredicate):
    # Counts the rows in <owner>.<table> applying the given OGG sqlpredicate exactly as it
    # will be used by the initial_load extract — so the count reflects what THAT extract
    # will actually read. The predicate must already include 'AS OF SCN <n>' (we delegate
    # building it to the caller because the same string is also fed to oggctl). Used to
    # render IL extract progress as '<rows_extracted>/<total_rows> (X% completed)'.
    # Returns (ok: bool, total: int|None, msg: str).
    try:
        import oracledb
    except ImportError:
        return False, None, "oracledb module not installed"
    ora = prepare_oracle_connection(bbdd_name, env)
    if ora is None:
        return False, None, f"could not prepare Oracle connection for '{bbdd_name}_{env}'"
    # Prefer standby for the count query (offloads reads from primary); fall back to primary.
    dsns = ora["dsns_to_check"]
    _, db_user, dsn = dsns[-1] if len(dsns) > 1 else dsns[0]
    db_password = ora["db_password"]
    db_pdb      = ora["db_pdb"]
    if "." not in (owner_table or ""):
        return False, None, f"invalid table format '{owner_table}' (expected OWNER.TABLE)"
    owner, table = owner_table.split(".", 1)
    sql        = f"SELECT /*+ PARALLEL(4) */ count(1) FROM {owner}.{table} {sqlpredicate}"
    last_error = "unknown error"
    for attempt in range(1, 4):   # up to 3 attempts
        conn   = None
        cursor = None
        try:
            conn   = oracledb.connect(user=db_user, password=db_password, dsn=dsn)
            cursor = conn.cursor()
            cursor.execute("SELECT cdb FROM v$database")
            if (cursor.fetchone()[0] or "").upper() == "YES" and db_pdb:
                cursor.execute(f"ALTER SESSION SET CONTAINER = {db_pdb}")
            cursor.execute(sql)
            total = int(cursor.fetchone()[0])
            return True, total, f"{owner_table} {sqlpredicate} = {total} rows"
        except Exception as e:
            last_error = str(e).strip().splitlines()[0] if str(e).strip() else repr(e)
            if attempt < 3:
                print_log("warning", f"count query attempt {attempt}/3 failed for '{owner_table}': {last_error} — retrying ...")
        finally:
            if cursor is not None:
                try: cursor.close()
                except Exception: pass
            if conn is not None:
                try: conn.close()
                except Exception: pass
    return False, None, f"count query failed after 3 attempts: {last_error}"


def get_il_extract_rows_applied(deployment, extract_name, table_name=None):
    # Returns how many rows the initial_load extract has written to its trail so far.
    # Two sources are tried in order:
    #   1) Live STATS command via the OGG REST API (POST /extracts/<name>/command with
    #      {"command": "STATS", "arguments": "TOTAL"}). We DO NOT add 'TABLE <name>'
    #      anymore — the live STATS reply has been observed to come back empty when the
    #      TABLE filter is set, while it does include 'Total inserts/operations <N>' when
    #      called with just 'TOTAL'. Each IL extract captures a single table so the
    #      filter is unnecessary anyway.
    #   2) Fallback: parse the extract's report file ('# inserts: <N>' under 'Run Time
    #      Statistics'). The report is refreshed every 'reportcount' interval (5 min by
    #      default per template), so it lags vs STATS, but it works even when STATS says
    #      'No active statistics for member ...'.
    # 'table_name' is accepted for API symmetry with the replicat variant but is unused.
    # Returns (ok, total: int|None, msg).
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.extract_ops  import send_command
    except ImportError as e:
        return False, None, f"failed to import oggctl modules: {e}"
    reply = ""
    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
        reply, _ = send_command(client, extract_name, "STATS", arguments="TOTAL")
        reply = reply or ""
    except Exception as e:
        # STATS transport failed but we can still try the report fallback below.
        reply = ""

    def _sum_pattern(pat):
        total = 0
        found = False
        for line in (reply or "").splitlines():
            m = pat.search(line)
            if not m:
                continue
            try:
                total += int(float(m.group(1).replace(",", "")))
                found  = True
            except ValueError:
                continue
        return found, total

    inserts_re = re.compile(r"Total\s+inserts\s+([\d,]+(?:\.\d+)?)",    re.IGNORECASE)
    ops_re     = re.compile(r"Total\s+operations\s+([\d,]+(?:\.\d+)?)", re.IGNORECASE)

    found, total = _sum_pattern(inserts_re)
    if found:
        return True, total, "OK"
    found, total = _sum_pattern(ops_re)
    if found:
        return True, total, "OK (via Total operations)"

    # Fallback: pull '# inserts: N' from the report file. The report is updated every
    # 'reportcount' interval so it may lag, but it's the most reliable source while the
    # extract is still in the early phase of the bulk read.
    ok_rpt, total_rpt, msg_rpt = get_il_extract_rows_from_report(deployment, extract_name)
    if ok_rpt and total_rpt is not None and total_rpt > 0:
        return True, total_rpt, f"OK (via report)"

    first_line = next((l.strip() for l in (reply or "").splitlines() if l.strip()), "(empty)")
    return True, None, f"no counters in STATS reply nor in report (STATS first line: {first_line[:120]}; report: {msg_rpt})"


def get_il_replicat_rows_applied(deployment, replicat_name, table_name=None):
    # Sends the OGG 'STATS' command to a replicat and returns the total number of INSERTS
    # applied so far. For initial_load replicats this is the only operation type (the bulk
    # load only produces inserts in the trail), so it doubles as a progress indicator vs
    # the corresponding IL extract's source row count.
    #
    # Optionally narrowed to a single table via the 'TABLE <owner.table>' argument; if
    # table_name is None the totals across all tables mapped by the replicat are summed.
    #
    # Returns (ok: bool, total_inserts: int|None, msg: str):
    #   - (True, N,    "OK")                : 'Total inserts' line(s) found
    #   - (True, None, "<reason>")          : reply read OK but no inserts line found
    #   - (False, None, "<reason>")         : transport / command failure
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.replicat_ops import send_command
    except ImportError as e:
        return False, None, f"failed to import oggctl modules: {e}"
    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client    = build_client(cfg)
        arguments = f"TOTAL, TABLE {table_name}" if table_name else "TOTAL"
        reply, _ = send_command(client, replicat_name, "STATS", arguments=arguments)
    except Exception as e:
        return False, None, f"failed to send STATS command: {e}"
    if not reply:
        return True, None, "STATS reply was empty"
    # The OGG STATS reply is plain text; 'Total inserts' lines look like:
    #   Total inserts                                3300.00
    # numbers may have decimal '.00' suffix and thousands separators (',').
    pattern = re.compile(r"Total\s+inserts\s+([\d,]+(?:\.\d+)?)", re.IGNORECASE)
    total   = 0
    found   = False
    for line in reply.splitlines():
        m = pattern.search(line)
        if not m:
            continue
        try:
            total += int(float(m.group(1).replace(",", "")))
            found  = True
        except ValueError:
            continue
    if not found:
        return True, None, "no 'Total inserts' line found in STATS reply"
    return True, total, "OK"


def get_il_extract_rows_from_report(deployment, extract_name):
    # Reads the extract report (<extract_name>.rpt) and returns the number of rows the
    # initial_load extract wrote to its trail file. The OGG report's 'Run Time Statistics'
    # block emits one line per operation type:
    #     #                   inserts:      3300
    #     #                   updates:         0
    #     #                   deletes:         0
    #     #                   upserts:         0
    #     #                  discards:         0
    # For initial_load extracts only INSERTS are meaningful — the load is a one-shot bulk
    # copy that produces inserts in the trail (updates/deletes/upserts come from integrated
    # capture, not from initial_load). We therefore sum ONLY the 'inserts' lines (across
    # all 'From table' blocks if the extract is partitioned).
    #
    # Returns (ok: bool, total: int|None, msg: str):
    #   - (True,  N,    "OK")               : inserts line(s) found, total = N (including 0)
    #   - (True,  None, "<reason>")         : report read OK but no 'inserts' line found
    #                                         -> caller should treat as a WARNING
    #   - (False, None, "<reason>")         : transport / read failure (network, perms, etc.)
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.extract_ops  import get_extract_report
    except ImportError as e:
        return False, None, f"failed to import oggctl modules: {e}"
    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client = build_client(cfg)
        lines  = get_extract_report(client, extract_name, f"{extract_name}.rpt") or []
    except Exception as e:
        return False, None, f"failed to read report: {e}"

    # OGG Microservices reports contain multiple statistics sections: one per
    # reportcount interval (delta since last checkpoint) plus a final cumulative
    # block when the process stops. Each section has its own '# inserts: N' line.
    # Summing all of them would inflate the total (delta1 + delta2 + ... + cumulative).
    # We take the LAST value, which is the final cumulative count.
    pattern = re.compile(r"^\s*#\s*inserts\s*:\s*(\d+)\s*$", re.IGNORECASE)
    last    = None
    for line in lines:
        m = pattern.match(line)
        if m:
            last = int(m.group(1))
    if last is None:
        return True, None, "no '# inserts:' line found in report"
    return True, last, "OK"


def get_consistency_scn_from_extract_report(deployment, extract_name, max_wait=300, poll_interval=30):
    # Returns the consistency SCN extracted from the LAST OGG-02845 line in the extract's
    # report file (<extract_name>.rpt). Example line:
    #   2026-06-17 15:09:32  INFO    OGG-02845  Position of first record processed
    #                              Sequence 5686, RBA 47372948, SCN 47297694134659, Time ...
    # The number after 'SCN ' is the SCN at which the integrated extract started processing
    # redo — it is the consistent point at which initial_load extracts must be instantiated
    # so no row changes are lost or duplicated between the bulk load and the CDC stream.
    #
    # Right after 'extract start' the report may not yet contain OGG-02845 (the line is
    # emitted only once the capture has processed the first record). On real workloads the
    # capture often needs minutes to scan / mine redo before the line appears, so we poll
    # up to 'max_wait' seconds (re-fetching the report every 'poll_interval' seconds)
    # before giving up. Defaults: max_wait=300s (5 min), poll_interval=15s — these are the
    # operational values the team uses to balance latency vs noise in the apply log. The
    # OGG REST client is rebuilt on each poll (cheap; mirrors how the rest of the codebase
    # reads reports — see check_extract_report_for_errors).
    #
    # Returns (ok: bool, scn: int|None, msg: str).
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.extract_ops  import get_extract_report
    except ImportError as e:
        return False, None, f"failed to import oggctl modules: {e}"

    ogg2845_re = re.compile(r"OGG-02845")
    scn_re     = re.compile(r"\bSCN\s+(\d+)")
    deadline = time.time() + max(0, max_wait)
    attempt  = 0
    last_err = None
    while True:
        attempt += 1
        try:
            cfg = load_cfg(deployment)
            maybe_suppress_tls_warning(cfg)
            client = build_client(cfg)
            lines  = get_extract_report(client, extract_name, f"{extract_name}.rpt") or []
        except Exception as e:
            last_err = str(e)
            lines    = []

        # walk lines in order, keep the LAST OGG-02845 occurrence with an SCN
        last_scn = None
        for line in lines:
            if ogg2845_re.search(line):
                m = scn_re.search(line)
                if m:
                    last_scn = int(m.group(1))

        if last_scn is not None and last_scn > 0:
            extra = f" (after {attempt} attempt{'s' if attempt > 1 else ''})" if attempt > 1 else ""
            return True, last_scn, f"{extract_name}.rpt OGG-02845 SCN={last_scn}{extra}"

        if time.time() >= deadline:
            if last_err and not lines:
                return False, None, f"could not read report for '{extract_name}' on '{deployment}' after {attempt} attempt(s): {last_err}"
            return False, None, f"no OGG-02845 line with a valid SCN found in '{extract_name}.rpt' after {max_wait}s / {attempt} attempt(s); capture may not have processed any record yet"

        print_log("info", f"  waiting for SCN from report '{extract_name}.rpt' — retrying in {poll_interval}s ...")
        time.sleep(poll_interval)


def get_db_connection(name):
    # Reads db_connections.yaml and returns the connection dict for the given name,
    # with the top-level 'defaults' section merged in (per-entry values override defaults).
    # The returned dict carries a special key '_defaults_applied' (a set) listing which
    # keys came from the defaults section (i.e. NOT defined in the entry itself). Callers
    # can use this to emit a warning when relying on a default.
    # Returns None if not found or file missing.
    if name == "defaults":
        return None
    connections_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "inventory_connections", "db_connections.yaml")
    if not os.path.isfile(connections_path):
        print_log("error", f"connections file not found: {connections_path}")
        return None
    try:
        import yaml
    except ImportError:
        print_log("error", "PyYAML not installed. Run: pip install pyyaml")
        return None
    with open(connections_path, "r", encoding="utf-8") as f:
        connections = yaml.safe_load(f) or {}
    entry = connections.get(name)
    if entry is None:
        return None
    defaults = connections.get("defaults", {}) or {}
    if not isinstance(defaults, dict):
        defaults = {}
    merged = {**defaults, **entry}
    merged["_defaults_applied"] = {k for k in defaults if k not in entry}
    return merged


def get_target_connection(name):
    # Reads target_connections.yaml and returns the target dict for the given name,
    # with the top-level 'defaults' section merged in (per-entry values override defaults).
    # Same shape as get_db_connection (carries '_defaults_applied' set).
    # Returns None if not found or file missing.
    if name == "defaults":
        return None
    targets_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "inventory_connections", "target_connections.yaml")
    if not os.path.isfile(targets_path):
        print_log("error", f"target connections file not found: {targets_path}")
        return None
    try:
        import yaml
    except ImportError:
        print_log("error", "PyYAML not installed. Run: pip install pyyaml")
        return None
    with open(targets_path, "r", encoding="utf-8") as f:
        connections = yaml.safe_load(f) or {}
    entry = connections.get(name)
    if entry is None:
        return None
    defaults = connections.get("defaults", {}) or {}
    if not isinstance(defaults, dict):
        defaults = {}
    merged = {**defaults, **entry}
    merged["_defaults_applied"] = {k for k in defaults if k not in entry}
    return merged


def get_target_connection_by_bucket(s3_bucket_name):
    # Scans target_connections.yaml and returns the first entry whose s3_bucket_name field
    # matches the given value. Merges top-level 'defaults' into the result (same as
    # get_target_connection). Returns None if not found or file missing.
    if not s3_bucket_name:
        return None
    targets_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "inventory_connections", "target_connections.yaml")
    if not os.path.isfile(targets_path):
        print_log("error", f"target connections file not found: {targets_path}")
        return None
    try:
        import yaml
    except ImportError:
        print_log("error", "PyYAML not installed. Run: pip install pyyaml")
        return None
    with open(targets_path, "r", encoding="utf-8") as f:
        connections = yaml.safe_load(f) or {}
    defaults = connections.get("defaults", {}) or {}
    if not isinstance(defaults, dict):
        defaults = {}
    for key, entry in connections.items():
        if key == "defaults" or not isinstance(entry, dict):
            continue
        merged = {**defaults, **entry}
        if (merged.get("s3_bucket_name") or "").strip() == s3_bucket_name.strip():
            merged["_defaults_applied"] = {k for k in defaults if k not in entry}
            return merged
    return None


def get_target_connection_by_cluster_name(cluster_name, env, identity_pool=""):
    # Scans target_connections.yaml and returns the first KAFKA entry whose cluster_name
    # and identity_pool fields match the given values and whose env matches. Merges
    # top-level 'defaults' into the result (same as get_target_connection).
    # 'identity_pool' is optional; when provided it disambiguates entries sharing the same cluster.
    # Returns None if not found or file missing.
    if not cluster_name:
        return None
    targets_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "inventory_connections", "target_connections.yaml")
    if not os.path.isfile(targets_path):
        print_log("error", f"target connections file not found: {targets_path}")
        return None
    try:
        import yaml
    except ImportError:
        print_log("error", "PyYAML not installed. Run: pip install pyyaml")
        return None
    with open(targets_path, "r", encoding="utf-8") as f:
        connections = yaml.safe_load(f) or {}
    defaults = connections.get("defaults", {}) or {}
    if not isinstance(defaults, dict):
        defaults = {}
    for key, entry in connections.items():
        if key == "defaults" or not isinstance(entry, dict):
            continue
        merged = {**defaults, **entry}
        if (merged.get("cluster_name") == cluster_name and
                merged.get("env", "").upper() == (env or "").upper() and
                merged.get("target_engine", "").upper() == "KAFKA" and
                (not identity_pool or merged.get("identity_pool") == identity_pool)):
            merged["_defaults_applied"] = {k for k in defaults if k not in entry}
            merged["_key"] = key
            return merged
    return None


def derive_target_name(target_engine, project, env):
    # Builds a default target_name key for target_connections.yaml when the JSON input
    # does not provide one explicitly. Convention: <ENGINE_PREFIX>_<PROJECT_SHORT>_<ENV>.
    #   target_engine "KAFKA"   -> prefix "KAFKA"
    #   target_engine "AWS-S3"  -> prefix "S3"
    # project_short uses the same 2-letter abbreviations as the OGG inventory (GR / DA / ...).
    engine_prefix_map  = {"KAFKA": "KAFKA", "AWS-S3": "S3"}
    project_short_map  = {"GOLDENRECORDS": "GR", "DATAIA": "DA"}
    prefix       = engine_prefix_map.get((target_engine or "").upper(), (target_engine or "").upper())
    project_abbr = project_short_map.get((project or "").upper(), (project or "").upper())
    return f"{prefix}_{project_abbr}_{(env or '').upper()}"


def managed_process_profile_exists(deployment, profile_name):
    # Checks whether a managed-process-profile with the given name exists on the OGG
    # deployment's config store. Used by validate.py / apply.py to fail-fast when
    # 'oggctl extract|replicat create --managed-process-profile <name>' would 404.
    # The match is CASE-SENSITIVE: OGG itself enforces an exact-case match when a process
    # is created against the profile, so validating with a different casing would let an
    # apply through that then fails at create-time. Convention in this repo: the profile
    # is registered as 'Critical' on every deployment (capital C).
    # Returns (ok, exists, msg):
    #   - ok      : True if the list-profiles API call succeeded
    #   - exists  : True if profile_name appears in the listed profiles (exact case)
    #   - msg     : detail when ok=False; empty otherwise
    ogg_ctl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ogg-ctl")
    if ogg_ctl_path not in sys.path:
        sys.path.insert(0, ogg_ctl_path)
    try:
        from cli                              import load_cfg, build_client, maybe_suppress_tls_warning
        from api.managed_process_profile_ops  import list_profiles
    except ImportError as e:
        return False, False, f"failed to import oggctl modules: {e}"
    try:
        cfg = load_cfg(deployment)
        maybe_suppress_tls_warning(cfg)
        client   = build_client(cfg)
        profiles = list_profiles(client) or []
    except Exception as e:
        return False, False, clean_api_error(e)
    return True, profile_name in profiles, ""


def check_tcp_reachable(host, port, timeout=5):
    # Plain TCP socket-level reachability check. Does NOT perform any protocol handshake
    # (no TLS, no Kafka, no HTTP). Useful as a first-line check before more expensive
    # protocol-level validations. Returns (ok: bool, msg: str).
    import socket
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, "reachable"
    except Exception as e:
        return False, str(e)


def check_http_reachable(url, timeout=5):
    # Plain HTTP reachability check. Returns (ok: bool, status_or_msg: str).
    # 'ok' is True for any HTTP status (the endpoint responded). On transport failure
    # (DNS, TCP, TLS, timeout) returns False with the error message.
    # Common case for Schema Registry: 200 (anonymous index) or 401 (auth required) both count as reachable.
    try:
        import ssl
        from urllib.request  import Request, urlopen
        from urllib.error    import HTTPError, URLError
    except ImportError as e:
        return False, f"stdlib import failed: {e}"
    try:
        req = Request(url, method="GET")
        ctx = ssl.create_default_context()
        # Don't verify cert here — many internal SRs use self-signed certs and we just want
        # to confirm the host responds. Tighten later if needed.
        ctx.check_hostname = False
        ctx.verify_mode    = ssl.CERT_NONE
        with urlopen(req, timeout=timeout, context=ctx) as r:
            return True, f"HTTP {r.status}"
    except HTTPError as e:
        # any HTTP status (including 401/403/404) means the endpoint IS reachable
        return True, f"HTTP {e.code}"
    except URLError as e:
        return False, f"URLError: {e.reason}"
    except Exception as e:
        return False, str(e)


def kafka_topic_name(topic_prefix, table_full):
    # Derives the Kafka topic name from a configured table, mirroring OGG's
    # kafka_confluent.properties.j2 template:
    #   gg.handler.kafkaconnect.topicMappingTemplate={{ kafka_topic_prefix }}.${toLowerCase[${tableName}]}
    # i.e. <kafka_topic_prefix>.<table_lowercase> (the schema portion is dropped).
    #
    # Examples:
    #   kafka_topic_name("esp.int.cac.bronze.grcdcpiloto", "SOE.CARD_DETAILS")
    #     -> "esp.int.cac.bronze.grcdcpiloto.card_details"
    #   kafka_topic_name("foo.bar", "SCHEMA.MY_TBL")
    #     -> "foo.bar.my_tbl"
    if "." in (table_full or ""):
        _, table_part = table_full.split(".", 1)
    else:
        table_part = table_full or ""
    return f"{topic_prefix}.{table_part.lower()}"


def _oauth_get_access_token(oauth_url, client_id, client_secret, scope, timeout=10):
    # Microsoft Entra ID client_credentials flow: POST grant to /oauth2/v2.0/token.
    # Returns (access_token: str, expires_at_epoch_seconds: float).
    # Raises RuntimeError on any failure (caller should wrap with try/except).
    try:
        import requests
    except ImportError as e:
        raise RuntimeError(f"requests not installed: {e}")
    import time as _time
    resp = requests.post(
        oauth_url,
        data={
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
            "scope":         scope,
        },
        timeout=timeout,
    )
    if resp.status_code != 200:
        # Truncate the body so we never log the full secret-revealing error from Entra.
        body_short = (resp.text or "")[:200].replace("\n", " ")
        raise RuntimeError(f"OAuth token request failed: HTTP {resp.status_code} {body_short}")
    body = resp.json() if resp.content else {}
    token = body.get("access_token")
    if not token:
        raise RuntimeError(f"OAuth response missing 'access_token' (keys: {sorted(body.keys())})")
    expires_in = int(body.get("expires_in", 3600))
    return token, _time.time() + expires_in


def get_kafka_oauth_token(target_conn, timeout=10):
    # Obtains an OAuth access token for a Kafka target using the credentials in target_conn.
    # Returns (token: str, None) on success, or (None, error_msg: str) on failure.
    required = ("oauth_endpoint_url", "client_id", "client_secret_credential", "api_scope")
    missing = [f for f in required if not target_conn.get(f)]
    if missing:
        return None, f"target connection missing required field(s): {', '.join(missing)}"
    secret_env = target_conn["client_secret_credential"]
    client_secret = os.environ.get(secret_env, "")
    if not client_secret:
        return None, f"environment variable '{secret_env}' is not set or empty"
    try:
        token, _ = _oauth_get_access_token(
            target_conn["oauth_endpoint_url"],
            target_conn["client_id"],
            client_secret,
            target_conn["api_scope"],
            timeout=timeout,
        )
        return token, None
    except RuntimeError as e:
        return None, str(e)


def check_schema_registry_auth(sr_url, token, identity_pool=None, sr_cluster_id=None, timeout=5):
    # Verifies authentication against a Confluent Cloud Schema Registry by calling
    # GET /subjects with a Bearer token.
    #
    # Confluent Cloud SR with Entra ID identity pools requires THREE headers:
    #   Authorization: Bearer <token>
    #   confluent-identity-pool-id: <pool_id>    ← mandatory when using identity pools
    #   Target-Sr-Cluster: <lsrc-xxxx>           ← mandatory for dedicated SR clusters;
    #                                               maps to "Logical Cluster ID" in the plugin
    # Without the Target-Sr-Cluster header the SR returns HTTP 401 even with a valid token.
    #
    # Returns (ok: bool, msg: str):
    #   ok=True  : HTTP 200 (authenticated + authorized) or HTTP 403 (authenticated,
    #              insufficient permissions to list subjects — auth itself succeeded)
    #   ok=False : HTTP 401 (authentication failed), transport error, or unexpected failure
    try:
        import ssl
        from urllib.request import Request, urlopen
        from urllib.error   import HTTPError, URLError
    except ImportError as e:
        return False, f"stdlib import failed: {e}"
    url = f"{sr_url.rstrip('/')}/subjects"
    req = Request(url, method="GET")
    req.add_header("Authorization", f"Bearer {token}")
    if identity_pool:
        req.add_header("confluent-identity-pool-id", identity_pool)
    if sr_cluster_id:
        req.add_header("Target-Sr-Cluster", sr_cluster_id)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode    = ssl.CERT_NONE
    try:
        with urlopen(req, timeout=timeout, context=ctx) as r:
            return True, f"HTTP {r.status}"
    except HTTPError as e:
        if e.code == 401:
            return False, f"HTTP 401 - authentication failed (check identity_pool and schema_registry_id in target_connections.yaml)"
        if e.code == 403:
            return True, f"HTTP 403 - authenticated (insufficient list permissions, auth OK)"
        return True, f"HTTP {e.code}"
    except URLError as e:
        return False, f"URLError: {e.reason}"
    except Exception as e:
        return False, str(e)


def check_kafka_topics_exist(target_conn, expected_topics, timeout=10):
    # Connects to a Confluent Cloud Kafka cluster using SASL_SSL + OAUTHBEARER (Microsoft
    # Entra ID identity pool) and checks whether each expected topic exists.
    #
    # Required fields in 'target_conn' (from target_connections.yaml):
    #   bootstrap_servers, security_protocol, sasl_mechanism, oauth_endpoint_url,
    #   identity_pool, api_scope, client_id, client_secret_credential
    #
    # The client secret is NOT stored in the YAML. The 'client_secret_credential' field
    # holds the NAME of the environment variable that carries the actual secret value
    # (e.g. 'KAFKA_CLIENT_SECRET_DEV').
    #
    # Returns (ok: bool, existence: dict[str, bool], msg: str):
    #   - ok        : True if the cluster was queried successfully
    #   - existence : {topic_name: True (exists) / False (missing)}, only meaningful when ok
    #   - msg       : transport/auth error detail; empty string on success
    try:
        from confluent_kafka.admin import AdminClient
    except ImportError as e:
        return False, {}, f"confluent-kafka not installed in this environment: {e}"

    # 1) Required fields from target_conn — cluster_id is needed because Confluent Cloud
    #    expects a 'logicalCluster' SASL OAUTHBEARER extension in addition to 'identityPoolId'.
    required_fields = ("bootstrap_servers", "oauth_endpoint_url", "client_id",
                       "api_scope", "identity_pool", "cluster_id",
                       "client_secret_credential")
    missing = [f for f in required_fields if not target_conn.get(f)]
    if missing:
        return False, {}, f"target connection missing required field(s): {', '.join(missing)}"

    identity_pool = target_conn["identity_pool"]
    cluster_id    = target_conn["cluster_id"]
    bootstrap     = target_conn["bootstrap_servers"]
    security_proto= target_conn.get("security_protocol", "SASL_SSL")
    sasl_mech     = target_conn.get("sasl_mechanism",    "OAUTHBEARER")

    # 2) Pre-validate OAuth credentials before handing them to librdkafka.
    #    When _oauth_cb raises an exception during the SASL handshake, librdkafka silently
    #    swallows it and surfaces a generic _TRANSPORT error — hiding the real cause
    #    (wrong secret, unreachable OAuth endpoint, bad scope, etc.).
    token, oauth_err = get_kafka_oauth_token(target_conn, timeout=timeout)
    if oauth_err:
        return False, {}, f"OAuth token request failed (check kafka_client_secret / client_id / oauth_endpoint_url): {oauth_err}"

    # 3) Build the OAuth callback used by librdkafka via confluent-kafka.
    #    For Confluent Cloud with Entra ID, the callback MUST return the 4-tuple form
    #    (token, expiry_epoch, principal, extensions) so the SASL extensions reach the
    #    broker. The 2-tuple form (token, expiry_epoch) does NOT carry extensions and
    #    Confluent rejects auth with 'CLUSTER_ID_MISSING_OR_EMPTY' even if the JWT is valid.
    #    The 'sasl.oauthbearer.config' admin-client setting is IGNORED when oauth_cb is set,
    #    that is why we have to put extensions here.
    client_id  = target_conn["client_id"]
    oauth_url  = target_conn["oauth_endpoint_url"]
    api_scope  = target_conn["api_scope"]
    secret_env = target_conn["client_secret_credential"]
    client_secret = os.environ.get(secret_env, "")

    def _oauth_cb(_oauthbearer_config):
        try:
            tok, expiry = _oauth_get_access_token(oauth_url, client_id, client_secret, api_scope, timeout=timeout)
            extensions = {
                "logicalCluster": cluster_id,
                "identityPoolId": identity_pool,
            }
            return tok, expiry, client_id, extensions
        except Exception as _e:
            import sys
            print(f"[OAUTH_CB ERROR] {_e}", file=sys.stderr, flush=True)
            raise

    # 5) Configure AdminClient. The 'sasl.oauthbearer.config' is left in only as a hint
    #    for librdkafka diagnostics; the actual extensions are sent by _oauth_cb above.
    # 'log_level: 3'  -> librdkafka only emits ERROR-level lines (default 6 = INFO is too noisy:
    #                    telemetry, GETSUBSCRIPTIONS, etc.). Auth/transport errors stay visible.
    # 'enable.metrics.push: false' -> turns off the telemetry subsystem entirely.
    # 'ssl.ca.location' -> librdkafka has its own SSL stack and does not use Python's CA bundle.
    #   'probe' tries common system paths (works on most Linux distros).
    #   Override via 'ssl_ca_location' in target_connections.yaml when a corporate TLS inspection
    #   proxy replaces the broker certificate with one signed by a corporate CA that is in the
    #   system store but not in librdkafka's probed locations.
    # 'enable.ssl.certificate.verification' -> set ssl_verify_certificate: false in
    #   target_connections.yaml ONLY for debugging _TRANSPORT failures in corporate proxy
    #   environments. Never disable in production.
    ssl_ca      = target_conn.get("ssl_ca_location", "probe")
    ssl_verify  = target_conn.get("ssl_verify_certificate", True)
    # kafka_debug: comma-separated librdkafka debug contexts (e.g. "broker,security,tls").
    # Set in target_connections.yaml ONLY for diagnosing _TRANSPORT failures — logs are verbose.
    # Common values: "broker" (connection), "security" (SASL/TLS handshake), "tls" (TLS detail).
    kafka_debug = target_conn.get("kafka_debug", "")
    admin_conf = {
        "bootstrap.servers":                    bootstrap,
        "security.protocol":                    security_proto,
        "sasl.mechanisms":                      sasl_mech,
        "sasl.oauthbearer.config":              f"logicalCluster={cluster_id},identityPoolId={identity_pool}",
        "oauth_cb":                             _oauth_cb,
        "ssl.ca.location":                      ssl_ca,
        "enable.ssl.certificate.verification":  bool(ssl_verify),
        "log_level":                            6 if kafka_debug else 3,
        "enable.metrics.push":                  False,
    }
    if kafka_debug:
        admin_conf["debug"] = kafka_debug
    try:
        admin = AdminClient(admin_conf)
        meta  = admin.list_topics(timeout=timeout)
    except Exception as e:
        return False, {}, clean_api_error(e)

    # 5) Cross-reference expected vs existing topics.
    existing = set((meta.topics or {}).keys())
    existence = {t: (t in existing) for t in expected_topics}
    return True, existence, ""


def check_s3_bucket_access(target_conn, s3_bucket_name, s3_bucket_path="", timeout=10):
    # Validates access to an AWS S3 bucket by:
    #   1) Using the long-term IAM user credentials (access_key + secret) to call STS::AssumeRole
    #      on the role declared in target_conn["sts_assume_role"].
    #   2) Using the temporary credentials returned by STS to call HeadBucket on s3_bucket_name.
    #   3) (optional) ListObjectsV2 with Prefix=s3_bucket_path/ to verify the prefix is accessible.
    #
    # Mirrors what OGG does at runtime when writing to S3 (gg.eventhandler.s3.*) so the validate
    # surfaces auth / permission problems before apply.
    #
    # Required fields in target_conn:
    #   aws_region, sts_assume_role, aws_access_key_id, aws_secret_access_key_credential
    #
    # The secret access key is NOT stored in the YAML. The 'aws_secret_access_key_credential'
    # field holds the NAME of the env var that carries the actual value (e.g.
    # 'AWS_SECRET_ACCESS_KEY_PRE').
    #
    # Returns (ok: bool, msg: str). On failure msg explains why; on success msg is a short summary.
    try:
        import boto3
        from botocore.config     import Config as BotoConfig
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as e:
        return False, f"boto3 not installed in this environment: {e}"

    # 1) Required fields
    required = ("aws_region", "sts_assume_role", "aws_access_key_id",
                "aws_secret_access_key_credential")
    missing = [f for f in required if not target_conn.get(f)]
    if missing:
        return False, f"target connection missing required field(s): {', '.join(missing)}"

    # 2) Resolve the long-term secret access key from the named env var
    secret_env = target_conn["aws_secret_access_key_credential"]
    secret_key = os.environ.get(secret_env, "")
    if not secret_key:
        return False, f"environment variable '{secret_env}' is not set or empty"

    region       = target_conn["aws_region"]
    role_arn     = target_conn["sts_assume_role"]
    session_name = target_conn.get("sts_session_name") or "cdc-controller-validate"
    access_key   = target_conn["aws_access_key_id"]

    boto_cfg = BotoConfig(
        connect_timeout=timeout,
        read_timeout=timeout,
        retries={"max_attempts": 1, "mode": "standard"},
    )

    # 3) Assume the role with the long-term IAM user credentials
    try:
        sts = boto3.client(
            "sts",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=boto_cfg,
        )
        assumed = sts.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
        tmp = assumed["Credentials"]
    except (ClientError, BotoCoreError) as e:
        return False, f"STS::AssumeRole failed: {clean_api_error(e)}"

    # 4) Use the temporary credentials to call HeadBucket
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=tmp["AccessKeyId"],
            aws_secret_access_key=tmp["SecretAccessKey"],
            aws_session_token=tmp["SessionToken"],
            region_name=region,
            config=boto_cfg,
        )
        s3.head_bucket(Bucket=s3_bucket_name)
    except (ClientError, BotoCoreError) as e:
        return False, f"S3 HeadBucket on '{s3_bucket_name}' failed: {clean_api_error(e)}"

    # 5) Optional: verify we can list under the s3_bucket_path prefix
    if s3_bucket_path:
        try:
            prefix = s3_bucket_path if s3_bucket_path.endswith("/") else s3_bucket_path + "/"
            s3.list_objects_v2(Bucket=s3_bucket_name, Prefix=prefix, MaxKeys=1)
        except (ClientError, BotoCoreError) as e:
            return False, f"S3 ListObjectsV2 on '{s3_bucket_name}/{s3_bucket_path}' failed: {clean_api_error(e)}"

    suffix = f", prefix={s3_bucket_path}" if s3_bucket_path else ""
    return True, f"(bucket={s3_bucket_name}, region={region}{suffix})"


def load_types_for_replication(replication_type):
    # Maps the replication_type from the JSON input to the list of OGG deployment
    # 'load_type' values that should be considered as candidates:
    #   ONLINE       -> ['online']
    #   INITIAL_LOAD -> ['initial_load']
    #   FULL         -> ['online', 'initial_load']   (both kinds of deployments needed)
    rt = (replication_type or "").upper()
    if rt == "ONLINE":
        return ["online"]
    if rt == "INITIAL_LOAD":
        return ["initial_load"]
    if rt == "FULL":
        return ["online", "initial_load"]
    return []


def get_matching_deployments(env, db_engine, load_types=None, proc_types=None):
    # Reads inventory.yaml and returns deployments matching env + db_engine + load_type + proc_type.
    # Pivots:
    #   - env, db_engine  : always applied
    #   - load_types      : list/set of acceptable 'load_type' values; if provided, the
    #                       deployment's 'load_type' (string or list in inventory.yaml) must
    #                       intersect with the requested list. Pass None to disable this filter.
    #   - proc_types      : list/set of acceptable 'proc_type' values (e.g. 'extract', 'replicat').
    #                       Same semantics as load_types: pass None to disable the filter.
    # 'project' is intentionally NOT a pivot — a single OGG deployment can serve multiple
    # projects in the same environment.
    # Returns a list of (name, info_dict) tuples, or [] if none match / file missing.
    inventory_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "inventory_deployments", "inventory.yaml")
    if not os.path.isfile(inventory_path):
        print_log("error", f"inventory file not found: {inventory_path}")
        return []
    try:
        import yaml
    except ImportError:
        print_log("error", "PyYAML not installed. Run: pip install pyyaml")
        return []
    with open(inventory_path, "r", encoding="utf-8") as f:
        inventory = yaml.safe_load(f) or {}

    wanted_lt = set(load_types) if load_types else None
    wanted_pt = set(proc_types) if proc_types else None

    def _to_set(raw):
        # inventory fields can be a single string or a list; normalize to a set of strings
        if raw is None:
            return set()
        if isinstance(raw, str):
            return {raw}
        if isinstance(raw, (list, tuple, set)):
            return {str(x) for x in raw}
        return set()

    matches = []
    for name, dep in inventory.items():
        if name == "defaults" or not isinstance(dep, dict):
            continue
        if dep.get("env")       != env:
            continue
        if dep.get("db_engine") != db_engine:
            continue
        if wanted_lt is not None and not (wanted_lt & _to_set(dep.get("load_type"))):
            continue
        if wanted_pt is not None and not (wanted_pt & _to_set(dep.get("proc_type"))):
            continue
        matches.append((name, dep))
    return matches


def get_inventory_defaults():
    # Returns the 'defaults' section of inventory.yaml as a dict (empty dict on any error).
    inventory_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "inventory_deployments", "inventory.yaml")
    if not os.path.isfile(inventory_path):
        return {}
    try:
        import yaml
    except ImportError:
        return {}
    with open(inventory_path, "r", encoding="utf-8") as f:
        inventory = yaml.safe_load(f) or {}
    return inventory.get("defaults", {})


def ora_log_query_result(cursor, rows):
    col_names  = [d[0] for d in cursor.description]
    col_widths = [max(len(name), max((len(str(row[i])) for row in rows), default=0)) for i, name in enumerate(col_names)]
    header    = "  ".join(f"{name:<{col_widths[i]}}"                    for i, name in enumerate(col_names))
    separator = "  ".join("-" * col_widths[i]                           for i in range(len(col_names)))
    print_log("info", header)
    print_log("info", separator)
    for row in rows:
        print_log("info", "  ".join(f"{str(row[i]):<{col_widths[i]}}" for i in range(len(col_names))))
