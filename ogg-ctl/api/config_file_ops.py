import logging

from api.errors import OGGAPIError, format_api_error
from api._common import log_ogg_messages

log = logging.getLogger(__name__)


def list_config_files(client):
    r = client.get("/config/files")
    if r.status_code != 200:
        raise OGGAPIError(f"List config files failed {format_api_error(r)}")

    log_ogg_messages(r)
    items = r.json().get("response", {}).get("items", [])
    return [item.get("name", "") for item in items]


def get_config_file(client, filename):
    r = client.get(f"/config/files/{filename}")
    if r.status_code == 404:
        raise OGGAPIError(f"Config file '{filename}' not found")
    if r.status_code != 200:
        raise OGGAPIError(f"Get config file failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r.json()["response"]["lines"]


def create_config_file(client, filename, lines):
    r = client.post(f"/config/files/{filename}", {"lines": lines})
    if r.status_code not in (200, 201):
        raise OGGAPIError(f"Create config file '{filename}' failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r


def replace_config_file(client, filename, lines):
    r = client.put(f"/config/files/{filename}", {"lines": lines})
    if r.status_code != 200:
        raise OGGAPIError(f"Replace config file '{filename}' failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r


def upsert_config_file(client, filename, lines):
    """Create the file if absent, replace if it exists.  Idempotent — used by `render`."""
    r = client.get(f"/config/files/{filename}")
    if r.status_code == 200:
        return replace_config_file(client, filename, lines)
    if r.status_code == 404:
        return create_config_file(client, filename, lines)
    raise OGGAPIError(f"Existence check failed for '{filename}' {format_api_error(r)}")


def delete_config_file(client, filename):
    r = client.delete(f"/config/files/{filename}")
    if r.status_code not in (200, 204):
        raise OGGAPIError(f"Delete config file '{filename}' failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r
