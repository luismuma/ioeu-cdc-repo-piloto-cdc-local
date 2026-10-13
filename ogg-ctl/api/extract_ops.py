import logging

from api.errors import OGGAPIError, format_api_error
from api._common import log_ogg_messages
from api.prm_builder import get_useridalias_from_prm, get_sourcecatalog_from_prm
from api.trail import parse_trail

log = logging.getLogger(__name__)


def extract_exists(client, extract_name):
    return client.get(f"/extracts/{extract_name}").status_code == 200


def create_extract_payload(
    prm_lines,
    description,
    source_type,          # e.g. "integrated" or None
    critical,
    encryption_profile,
    managed_process_settings=None,
):
    payload = {
        "description": description,
        "config": prm_lines,
        "status": "stopped",
    }

    if source_type:
        payload["source"] = source_type

    if critical is not None:
        payload["critical"] = bool(critical)

    if encryption_profile:
        payload["encryptionProfile"] = encryption_profile

    if managed_process_settings:
        payload["managedProcessSettings"] = managed_process_settings

    return payload


def create_extract(client, extract_name, payload, fail_if_exists=True):

    if fail_if_exists and extract_exists(client, extract_name):
        raise OGGAPIError(f"Extract {extract_name} already exists")

    r = client.post(f"/extracts/{extract_name}", payload)
    if r.status_code not in (200, 201):
        raise OGGAPIError(f"Create failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r


def validate_only_create(client, extract_name, payload):
    r = create_extract(client, extract_name, payload, fail_if_exists=True)

    d = client.delete(f"/extracts/{extract_name}")
    if d.status_code not in (200, 202, 204):
        raise OGGAPIError(f"Validate-only: delete failed {format_api_error(d)}")

    return r


def attach_trail(client, extract_name, trail):
    if not extract_exists(client, extract_name):
        raise OGGAPIError(f"Extract {extract_name} does not exist, cannot attach trail")

    payload = {
        "targets": [parse_trail(trail)]
    }

    r = client.patch(f"/extracts/{extract_name}", payload)
    if r.status_code not in (200, 204):
        raise OGGAPIError(f"Attach trail failed {format_api_error(r)}")

    return r


def register_extract(
    client,
    extract_name,
    optimized=False,
    replace=False,
):
    r = client.get(f"/extracts/{extract_name}")
    if r.status_code == 404:
        raise OGGAPIError(f"Extract {extract_name} does not exist, cannot register")
    if r.status_code != 200:
        raise OGGAPIError(f"Failed to fetch extract {extract_name} {format_api_error(r)}")

    data = r.json()["response"]
    source_type = data.get("type")
    if source_type != "Integrated":
        raise OGGAPIError(f"Extract {extract_name} is not an integrated extract "
              f"(source type: {source_type!r}), skipping registration")

    config = data["config"]

    alias = get_useridalias_from_prm(config)
    if not alias:
        raise OGGAPIError("USERIDALIAS not found in extract PRM, cannot register")

    registration = {
        "optimized": bool(optimized),
        "replace": bool(replace),
    }

    sourcecatalog = get_sourcecatalog_from_prm(config)
    if sourcecatalog:
        registration["containers"] = [sourcecatalog]

    payload = {
        "credentials": {"alias": alias},
        "registration": registration,
    }

    r = client.patch(f"/extracts/{extract_name}", payload)
    if r.status_code not in (200, 204):
        raise OGGAPIError(f"Registration failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r


def list_extracts(client):
    r = client.get("/extracts")
    if r.status_code != 200:
        raise OGGAPIError(f"List extracts failed {format_api_error(r)}")

    log_ogg_messages(r)
    items = r.json().get("response", {}).get("items", [])
    return [
        {"name": item.get("name", ""), "status": item.get("status", "")}
        for item in items
    ]


def get_extract_status(client, extract_name):
    r = client.get(f"/extracts/{extract_name}")
    if r.status_code == 404:
        raise OGGAPIError(f"Extract {extract_name} does not exist")
    if r.status_code != 200:
        raise OGGAPIError(f"Get status failed {format_api_error(r)}")
    return r.json()["response"]["status"]


def get_extract_params(client, extract_name):
    r = client.get(f"/extracts/{extract_name}")
    if r.status_code == 404:
        raise OGGAPIError(f"Extract {extract_name} does not exist")
    if r.status_code != 200:
        raise OGGAPIError(f"Get params failed {format_api_error(r)}")
    return r.json()["response"]["config"]


def set_extract_params(client, extract_name, lines):
    if not extract_exists(client, extract_name):
        raise OGGAPIError(f"Extract {extract_name} does not exist")
    r = client.patch(f"/extracts/{extract_name}", {"config": list(lines)})
    if r.status_code not in (200, 202, 204):
        raise OGGAPIError(f"Set params failed {format_api_error(r)}")
    log_ogg_messages(r)
    return r


def append_extract_params(client, extract_name, lines):
    existing = get_extract_params(client, extract_name)
    return set_extract_params(client, extract_name, list(existing) + list(lines))


def get_extract_info(client, extract_name):
    r = client.get(f"/extracts/{extract_name}/info/status")
    if r.status_code == 404:
        raise OGGAPIError(f"Extract {extract_name} does not exist")
    if r.status_code != 200:
        raise OGGAPIError(f"Get info failed {format_api_error(r)}")
    return r.json()["response"]


def get_extract_history(client, extract_name):
    """Return the run-history list for an extract (most recent run first).

    Each entry has 'status', 'started', 'finished', 'processId', 'exitCode'.
    Returns an empty list if the history endpoint is unavailable or empty.
    """
    r = client.get(f"/extracts/{extract_name}/info/history")
    if r.status_code != 200:
        return []
    return r.json().get("response", {}).get("history", [])


def list_extract_reports(client, extract_name):
    """Return list of report names available for this extract (most recent first)."""
    r = client.get(f"/extracts/{extract_name}/info/reports")
    if r.status_code == 404:
        return []
    if r.status_code != 200:
        raise OGGAPIError(f"List reports failed {format_api_error(r)}")
    items = r.json().get("response", {}).get("items", [])
    return [item.get("name", "") for item in items if item.get("name")]


def get_extract_report(client, extract_name, report_name):
    r = client.get(f"/extracts/{extract_name}/info/reports/{report_name}")
    if r.status_code == 404:
        raise OGGAPIError(f"Report '{report_name}' for extract {extract_name} not found")
    if r.status_code != 200:
        raise OGGAPIError(f"Get report failed {format_api_error(r)}")
    return r.json()["response"]["lines"]


def send_command(client, extract_name, command, arguments=None, is_reported=False):
    payload = {"command": command}
    if arguments:
        payload["arguments"] = arguments
    if is_reported:
        payload["isReported"] = True

    r = client.post(f"/extracts/{extract_name}/command", payload)
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


def get_table_stats(client, extract_name: str, period: str = "daily") -> list[dict]:
    """Return per-table stats for the given period.

    period: 'daily' — ops since midnight; 'total' — ops since process start.
    Returns list of dicts: {table, since, insert, update, delete, upsert, discard}
    """
    r = client.post(
        f"/extracts/{extract_name}/command",
        {"command": "STATS", "arguments": f"{period.upper()} REPORTCDR"},
    )
    if r.status_code not in (200, 201):
        raise OGGAPIError(f"STATS {period.upper()} failed {format_api_error(r)}")
    return _parse_stats_response(r.json())


def start_extract(client, extract_name):
    if not extract_exists(client, extract_name):
        raise OGGAPIError(f"Extract {extract_name} does not exist, cannot start")

    r = client.patch(f"/extracts/{extract_name}", {"status": "running"})
    if r.status_code not in (200, 204):
        raise OGGAPIError(f"Start failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r


def stop_extract(client, extract_name):
    if not extract_exists(client, extract_name):
        raise OGGAPIError(f"Extract {extract_name} does not exist, cannot stop")

    r = client.patch(f"/extracts/{extract_name}", {"status": "stopped"})
    if r.status_code not in (200, 204):
        raise OGGAPIError(f"Stop failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r


def update_extract(client, extract_name, *, description=None, critical=None, encryption_profile=None,
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
    r = client.patch(f"/extracts/{extract_name}", payload)
    if r.status_code not in (200, 202):
        raise OGGAPIError(f"Update failed {format_api_error(r)}")
    log_ogg_messages(r)
    return r


def delete_extract(client, extract_name):
    if not extract_exists(client, extract_name):
        raise OGGAPIError(f"Extract {extract_name} does not exist, cannot delete")

    status = get_extract_status(client, extract_name)
    if status.upper() not in ("STOPPED", "ABENDED", "KILLED"):
        raise OGGAPIError(
            f"Extract {extract_name} is '{status}', must be stopped, abended, or killed before deleting"
        )

    r = client.delete(f"/extracts/{extract_name}")
    if r.status_code not in (200, 202, 204):
        raise OGGAPIError(f"Delete failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r

