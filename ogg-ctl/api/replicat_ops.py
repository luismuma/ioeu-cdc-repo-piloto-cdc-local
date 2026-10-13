import logging

from api.errors import OGGAPIError, format_api_error
from api._common import log_ogg_messages

log = logging.getLogger(__name__)


def replicat_exists(client, replicat_name):
    return client.get(f"/replicats/{replicat_name}").status_code == 200


def create_replicat_payload(
    prm_lines,
    description,
    mode,
    source,
    checkpoint_table=None,
    critical=False,
    encryption_profile=None,
    managed_process_settings=None,
):
    payload = {
        "description": description,
        "config": prm_lines,
        "status": "stopped",
        "mode": mode,
        "source": source,
    }

    if checkpoint_table:
        payload["checkpoint"] = {"table": checkpoint_table}

    if critical is not None:
        payload["critical"] = bool(critical)

    if encryption_profile:
        payload["encryptionProfile"] = encryption_profile

    if managed_process_settings:
        payload["managedProcessSettings"] = managed_process_settings

    return payload


def create_replicat(client, replicat_name, payload, fail_if_exists=True):
    if fail_if_exists and replicat_exists(client, replicat_name):
        raise OGGAPIError(f"Replicat {replicat_name} already exists")

    r = client.post(f"/replicats/{replicat_name}", payload)
    if r.status_code not in (200, 201):
        raise OGGAPIError(f"Create failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r


def validate_only_create(client, replicat_name, payload):
    r = create_replicat(client, replicat_name, payload, fail_if_exists=True)

    d = client.delete(f"/replicats/{replicat_name}")
    if d.status_code not in (200, 202, 204):
        raise OGGAPIError(f"Validate-only: delete failed {format_api_error(d)}")

    return r


def list_replicats(client):
    r = client.get("/replicats")
    if r.status_code != 200:
        raise OGGAPIError(f"List replicats failed {format_api_error(r)}")

    log_ogg_messages(r)
    items = r.json().get("response", {}).get("items", [])
    return [
        {"name": item.get("name", ""), "status": item.get("status", "")}
        for item in items
    ]


def get_replicat_status(client, replicat_name):
    r = client.get(f"/replicats/{replicat_name}")
    if r.status_code == 404:
        raise OGGAPIError(f"Replicat {replicat_name} does not exist")
    if r.status_code != 200:
        raise OGGAPIError(f"Get status failed {format_api_error(r)}")
    return r.json()["response"]["status"]


def get_replicat_info(client, replicat_name):
    r = client.get(f"/replicats/{replicat_name}/info/status")
    if r.status_code == 404:
        raise OGGAPIError(f"Replicat {replicat_name} does not exist")
    if r.status_code != 200:
        raise OGGAPIError(f"Get info failed {format_api_error(r)}")
    return r.json()["response"]


def get_replicat_history(client, replicat_name):
    """Return the run-history list for a replicat (most recent run first).

    Each entry has 'status', 'started', 'finished', 'processId', 'exitCode'.
    Returns an empty list if the history endpoint is unavailable or empty.
    """
    r = client.get(f"/replicats/{replicat_name}/info/history")
    if r.status_code != 200:
        return []
    return r.json().get("response", {}).get("history", [])


def list_replicat_reports(client, replicat_name):
    """Return list of report names available for this replicat (most recent first)."""
    r = client.get(f"/replicats/{replicat_name}/info/reports")
    if r.status_code == 404:
        return []
    if r.status_code != 200:
        raise OGGAPIError(f"List reports failed {format_api_error(r)}")
    items = r.json().get("response", {}).get("items", [])
    return [item.get("name", "") for item in items if item.get("name")]


def get_replicat_report(client, replicat_name, report_name):
    r = client.get(f"/replicats/{replicat_name}/info/reports/{report_name}")
    if r.status_code == 404:
        raise OGGAPIError(f"Report '{report_name}' for replicat {replicat_name} not found")
    if r.status_code != 200:
        raise OGGAPIError(f"Get report failed {format_api_error(r)}")
    return r.json()["response"]["lines"]


def get_replicat_params(client, replicat_name):
    r = client.get(f"/replicats/{replicat_name}")
    if r.status_code == 404:
        raise OGGAPIError(f"Replicat {replicat_name} does not exist")
    if r.status_code != 200:
        raise OGGAPIError(f"Get params failed {format_api_error(r)}")
    return r.json()["response"]["config"]


def set_replicat_params(client, replicat_name, lines):
    if not replicat_exists(client, replicat_name):
        raise OGGAPIError(f"Replicat {replicat_name} does not exist")
    r = client.patch(f"/replicats/{replicat_name}", {"config": list(lines)})
    if r.status_code not in (200, 202, 204):
        raise OGGAPIError(f"Set params failed {format_api_error(r)}")
    log_ogg_messages(r)
    return r


def append_replicat_params(client, replicat_name, lines):
    existing = get_replicat_params(client, replicat_name)
    return set_replicat_params(client, replicat_name, list(existing) + list(lines))


# ---------------------------------------------------------------------------
# Properties (Java .properties file for Big Data replicats — Kafka, S3, etc.)
# Stored as /config/files/<replicat_name>.properties
# ---------------------------------------------------------------------------

def parse_properties_lines(lines):
    """Parse Java .properties lines into a flat dict. Comments (#, !) and blank lines are skipped.
    Splits on the first '=' or ':' on each line."""
    result = {}
    for raw in lines:
        line = raw.strip()
        if not line or line[0] in "#!":
            continue
        idx = -1
        for sep in ("=", ":"):
            pos = line.find(sep)
            if pos != -1 and (idx == -1 or pos < idx):
                idx = pos
        if idx == -1:
            result[line] = ""
        else:
            result[line[:idx].strip()] = line[idx + 1:].strip()
    return result


def format_properties_dict(props):
    """Render a dict as Java .properties lines (key=value)."""
    return [f"{k}={v}" for k, v in props.items()]


def _properties_filename(replicat_name):
    return f"{replicat_name}.properties"


def get_replicat_properties(client, replicat_name):
    """Return the replicat .properties file as a dict.  Raises if file doesn't exist."""
    fn = _properties_filename(replicat_name)
    r = client.get(f"/config/files/{fn}")
    if r.status_code == 404:
        raise OGGAPIError(f"Properties file '{fn}' not found for replicat {replicat_name}")
    if r.status_code != 200:
        raise OGGAPIError(f"Get properties failed {format_api_error(r)}")
    return parse_properties_lines(r.json()["response"]["lines"])


def _upsert_properties_lines(client, replicat_name, lines):
    """Create or replace the replicat .properties file with the given raw lines."""
    fn = _properties_filename(replicat_name)
    r = client.get(f"/config/files/{fn}")
    if r.status_code == 200:
        r = client.put(f"/config/files/{fn}", {"lines": lines})
        if r.status_code != 200:
            raise OGGAPIError(f"Replace properties failed {format_api_error(r)}")
    elif r.status_code == 404:
        r = client.post(f"/config/files/{fn}", {"lines": lines})
        if r.status_code not in (200, 201):
            raise OGGAPIError(f"Create properties failed {format_api_error(r)}")
    else:
        raise OGGAPIError(f"Properties existence check failed {format_api_error(r)}")
    log_ogg_messages(r)
    return r


def set_replicat_properties(client, replicat_name, props):
    """Upsert the replicat .properties file from a dict (create if absent, replace if present)."""
    return _upsert_properties_lines(client, replicat_name, format_properties_dict(props))


def render_replicat_properties(client, replicat_name, template_name, context):
    """Render a properties template (e.g. 'kafka_confluent', 's3') with the given context
    and upload the result as the replicat's .properties file.  Preserves comments and ordering."""
    from api.prm_builder import build_properties_lines
    lines = build_properties_lines(template_name, context)
    return _upsert_properties_lines(client, replicat_name, lines)


def merge_replicat_properties(client, replicat_name, updates):
    """Update specific keys, leaving unchanged keys intact.  Creates the file if it doesn't exist."""
    fn = _properties_filename(replicat_name)
    r = client.get(f"/config/files/{fn}")
    if r.status_code == 200:
        existing = parse_properties_lines(r.json()["response"]["lines"])
    elif r.status_code == 404:
        existing = {}
    else:
        raise OGGAPIError(f"Properties existence check failed {format_api_error(r)}")
    merged = {**existing, **updates}
    return set_replicat_properties(client, replicat_name, merged)


def send_command(client, replicat_name, command, arguments=None, is_reported=False):
    payload = {"command": command}
    if arguments:
        payload["arguments"] = arguments
    if is_reported:
        payload["isReported"] = True

    r = client.post(f"/replicats/{replicat_name}/command", payload)
    if r.status_code not in (200, 201):
        raise OGGAPIError(f"Command '{command}' failed {format_api_error(r)}")

    data = r.json().get("response", r.json())
    reply = data.get("reply", "")
    if isinstance(reply, list):
        reply = "\n".join(reply)
    return reply, data.get("replyData", {})


def _parse_stats_response(data: dict) -> list[dict]:
    stats = data.get("response", {}).get("replyData", {}).get("stats", [])
    rows = []
    for entry in stats:
        if entry.get("ddl"):
            continue
        tbl = entry.get("sourceTable") or entry.get("targetTable") or ""
        if tbl.count(".") >= 2:
            tbl = tbl[tbl.index(".") + 1:]
        if not tbl:
            continue
        ops = {op["operation"]: int(op.get("count", 0)) for op in (entry.get("operations") or [])}
        rows.append({
            "table":   tbl,
            "since":   entry.get("since", ""),
            "insert":  ops.get("insert",  0),
            "update":  ops.get("update",  0),
            "delete":  ops.get("delete",  0),
            "upsert":  ops.get("upsert",  0),
            "discard": ops.get("discard", 0),
        })
    return rows


def get_table_stats(client, replicat_name: str, period: str = "daily") -> list[dict]:
    """Return per-table stats for the given period.

    period: 'daily' — ops since midnight; 'total' — ops since process start.
    Returns list of dicts: {table, since, insert, update, delete, upsert, discard}
    """
    r = client.post(
        f"/replicats/{replicat_name}/command",
        {"command": "STATS", "arguments": f"{period.upper()} REPORTCDR"},
    )
    if r.status_code not in (200, 201):
        raise OGGAPIError(f"STATS {period.upper()} failed {format_api_error(r)}")
    return _parse_stats_response(r.json())


def start_replicat(client, replicat_name):
    if not replicat_exists(client, replicat_name):
        raise OGGAPIError(f"Replicat {replicat_name} does not exist, cannot start")

    r = client.patch(f"/replicats/{replicat_name}", {"status": "running"})
    if r.status_code not in (200, 204):
        raise OGGAPIError(f"Start failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r


def stop_replicat(client, replicat_name):
    if not replicat_exists(client, replicat_name):
        raise OGGAPIError(f"Replicat {replicat_name} does not exist, cannot stop")

    r = client.patch(f"/replicats/{replicat_name}", {"status": "stopped"})
    if r.status_code not in (200, 204):
        raise OGGAPIError(f"Stop failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r


def update_replicat(client, replicat_name, *, description=None, critical=None, encryption_profile=None,
                    managed_process_settings=None):
    payload = {}
    if description is not None:
        payload["description"] = description
    if critical is not None:
        payload["critical"] = bool(critical)
    if encryption_profile is not None:
        payload["encryptionProfile"] = encryption_profile
    if managed_process_settings is not None:
        payload["managedProcessSettings"] = managed_process_settings
    if not payload:
        raise ValueError(
            "No fields to update — supply at least one of "
            "--description, --critical/--no-critical, --encryption-profile, "
            "--auto-start/--no-auto-start, --auto-restart/--no-auto-restart"
        )
    r = client.patch(f"/replicats/{replicat_name}", payload)
    if r.status_code not in (200, 202):
        raise OGGAPIError(f"Update failed {format_api_error(r)}")
    log_ogg_messages(r)
    return r


def delete_replicat(client, replicat_name):
    if not replicat_exists(client, replicat_name):
        raise OGGAPIError(f"Replicat {replicat_name} does not exist, cannot delete")

    status = get_replicat_status(client, replicat_name)
    if status.upper() not in ("STOPPED", "ABENDED", "KILLED"):
        raise OGGAPIError(
            f"Replicat {replicat_name} is '{status}', must be stopped, abended, or killed before deleting"
        )

    r = client.delete(f"/replicats/{replicat_name}")
    if r.status_code not in (200, 202, 204):
        raise OGGAPIError(f"Delete failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r
