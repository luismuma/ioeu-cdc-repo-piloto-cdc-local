import logging

from api.errors import OGGAPIError, format_api_error

log = logging.getLogger(__name__)

_PROFILE_TYPE = "ogg:managedProcessSettings"


def _value_name(name):
    """Return the full config-store value name (e.g. 'ogg:managedProcessSettings:test')."""
    if name.startswith(f"{_PROFILE_TYPE}:"):
        return name
    return f"{_PROFILE_TYPE}:{name}"


def _short_name(value_name):
    """Strip the type prefix to get the user-visible name."""
    prefix = f"{_PROFILE_TYPE}:"
    if value_name.startswith(prefix):
        return value_name[len(prefix):]
    return value_name


def profile_ref(name):
    """Return the managedProcessSettings reference string for use in extract/replicat payloads."""
    return _value_name(name)


def _path(name):
    return f"/config/types/{_PROFILE_TYPE}/values/{_value_name(name)}"


def build_profile_payload(
    auto_start=None,
    auto_start_delay=None,
    auto_restart=None,
    auto_restart_retries=None,
    auto_restart_delay=None,
    on_success=None,
    window=None,
    disable_on_failure=None,
):
    """Build a managedProcessSettings body for profile create/update."""
    payload = {"$schema": _PROFILE_TYPE}
    if auto_start is not None:
        entry = {"enabled": bool(auto_start)}
        if auto_start_delay is not None:
            entry["delay"] = auto_start_delay
        payload["autoStart"] = entry
    if auto_restart is not None:
        entry = {"enabled": bool(auto_restart)}
        if auto_restart_retries is not None:
            entry["retries"] = auto_restart_retries
        if auto_restart_delay is not None:
            entry["delay"] = auto_restart_delay
        if on_success is not None:
            entry["onSuccess"] = bool(on_success)
        if window is not None:
            entry["window"] = window
        if disable_on_failure is not None:
            entry["disableOnFailure"] = bool(disable_on_failure)
        payload["autoRestart"] = entry
    return payload


def create_profile(client, name, payload):
    r = client.post(_path(name), payload)
    if r.status_code not in (200, 201):
        raise OGGAPIError(f"Create profile failed {format_api_error(r)}")
    return r


def update_profile(client, name, payload):
    r = client.put(_path(name), payload)
    if r.status_code not in (200, 202):
        raise OGGAPIError(f"Update profile failed {format_api_error(r)}")
    return r


def list_profiles(client):
    r = client.get(f"/config/types/{_PROFILE_TYPE}/values")
    if r.status_code != 200:
        raise OGGAPIError(f"List profiles failed {format_api_error(r)}")
    items = r.json().get("response", {}).get("items", [])
    return [_short_name(item.get("name", "")) for item in items]


def get_profile(client, name):
    r = client.get(_path(name))
    if r.status_code == 404:
        raise OGGAPIError(f"Profile '{name}' not found")
    if r.status_code != 200:
        raise OGGAPIError(f"Get profile failed {format_api_error(r)}")
    return r.json()["response"]


def delete_profile(client, name):
    r = client.delete(_path(name))
    if r.status_code not in (200, 202, 204):
        raise OGGAPIError(f"Delete profile failed {format_api_error(r)}")
    return r
