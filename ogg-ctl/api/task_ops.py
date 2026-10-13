import logging

from api.errors import OGGAPIError, UserInputError, format_api_error

log = logging.getLogger(__name__)

_TIME_UNITS = ("seconds", "minutes", "hours", "days")


def _path(name=None):
    base = "/tasks"
    return f"{base}/{name}" if name else base


def build_purge_trail_payload(
    trails,
    keep_value,
    keep_units,
    schedule_value,
    schedule_units,
    enabled=False,
    critical=False,
    use_checkpoints=False,
):
    return {
        "$schema": "ogg:task",
        "enabled": enabled,
        "critical": critical,
        "command": {
            "name": "purge",
            "purgeType": "trails",
            "useCheckpoints": use_checkpoints,
            "trails": trails,
            "keep": [{"type": "min", "value": keep_value, "units": keep_units}],
        },
        "schedule": {
            "every": {"value": schedule_value, "units": schedule_units}
        },
        "restart": {"enabled": False},
    }


def list_tasks(client):
    r = client.get(_path())
    if r.status_code != 200:
        raise OGGAPIError(f"List tasks failed {format_api_error(r)}")
    items = r.json().get("response", {}).get("items", [])
    return items


def list_purge_trail_tasks(client):
    items = list_tasks(client)
    result = []
    for item in items:
        name = item.get("name")
        if not name:
            continue
        try:
            task = get_task(client, name)
            cmd = task.get("command", {})
            if cmd.get("name") == "purge" and cmd.get("purgeType") == "trails":
                task.setdefault("name", name)
                result.append(task)
        except OGGAPIError:
            log.debug("Could not fetch task '%s', skipping", name)
    return result


def _known_trails(client):
    """Returns set of (name, path_prefix) tuples from GET /trails?details, or None if unavailable."""
    r = client.get("/trails?details")
    if r.status_code != 200:
        return None
    items = r.json().get("response", {}).get("items", [])
    result = set()
    for item in items:
        details = item.get("details", {})
        name = details.get("name") or item.get("name")
        if not name:
            continue
        path = details.get("path", "")
        # path may be "dir/name" (full) or "dir" (prefix only) — normalise to prefix
        if path.endswith(f"/{name}"):
            path_prefix = path[:-(len(name) + 1)]
        elif path == name:
            path_prefix = ""
        else:
            path_prefix = path
        result.add((name, path_prefix))
    log.debug("Known trails from API: %s", result)
    return result


def validate_trails_exist(client, trails):
    """Validates each (name, path) pair exists as a trail on this deployment."""
    known = _known_trails(client)
    if known is None:
        log.warning("Could not retrieve trail list — skipping trail existence check")
        return
    not_found = []
    for trail in trails:
        key = (trail["name"], trail.get("path", ""))
        if key not in known:
            spec = f"{trail['path']}/{trail['name']}" if trail.get("path") else trail["name"]
            not_found.append(spec)
    if not_found:
        known_specs = sorted(
            f"{p}/{n}" if p else n for n, p in known
        )
        raise UserInputError(
            f"Trail(s) not found on this deployment: {', '.join(not_found)}. "
            f"Known trails: {', '.join(known_specs) or '(none)'}"
        )


def get_task(client, name):
    r = client.get(_path(name))
    if r.status_code == 404:
        raise OGGAPIError(f"Task '{name}' not found")
    if r.status_code != 200:
        raise OGGAPIError(f"Get task failed {format_api_error(r)}")
    return r.json()["response"]


def create_task(client, name, payload):
    r = client.post(_path(name), payload)
    if r.status_code not in (200, 201):
        raise OGGAPIError(f"Create task failed {format_api_error(r)}")
    return r


def update_task(client, name, payload):
    r = client.put(_path(name), payload)
    if r.status_code not in (200, 202):
        raise OGGAPIError(f"Update task failed {format_api_error(r)}")
    return r


def delete_task(client, name):
    r = client.delete(_path(name))
    if r.status_code not in (200, 202, 204):
        raise OGGAPIError(f"Delete task failed {format_api_error(r)}")
    return r


def send_task_cmd(client, name, cmd):
    r = client.post(f"{_path(name)}/cmd", {"name": cmd})
    if r.status_code not in (200, 202):
        raise OGGAPIError(f"Task command '{cmd}' failed {format_api_error(r)}")
    return r
