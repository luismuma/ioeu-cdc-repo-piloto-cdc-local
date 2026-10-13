import logging
from pathlib import Path

from api.errors import OGGAPIError, format_api_error
from api._common import log_ogg_messages

log = logging.getLogger(__name__)

VALID_ROLES = ("Administrator", "Operator", "User", "Security")


def log_ogg_messages(r):
    try:
        for msg in r.json().get("messages", []):
            severity = msg.get("severity", "INFO").upper()
            code = msg.get("code", "")
            title = msg.get("title", "")
            log.debug("OGG [%s] %s: %s", severity, code, title)
    except Exception:
        pass


def _name_from_item(item):
    """Extract a resource name from an API item.

    OGG list responses sometimes omit flat name fields and only include
    a 'links' array whose canonical/self href ends with the resource name.
    """
    name = item.get("name") or item.get("role") or item.get("user")
    if name:
        return name
    for link in item.get("links", []):
        if link.get("rel") in ("canonical", "self"):
            href = link.get("href", "").rstrip("/")
            part = href.split("/")[-1]
            if part:
                return part
    return ""


def user_exists(client, role, username):
    return client.get(f"/authorizations/{role}/{username}").status_code == 200


def list_roles(client):
    r = client.get("/authorizations")
    if r.status_code != 200:
        raise OGGAPIError(f"List roles failed {format_api_error(r)}")
    body = r.json()
    log.debug("authorizations response: %s", body)
    items = body.get("response", {}).get("items", [])
    # "All" is a pseudo-role that aggregates every user; exclude it to avoid duplicates
    return [n for n in (_name_from_item(item) for item in items) if n and n != "All"]


def list_users(client, role):
    r = client.get(f"/authorizations/{role}")
    if r.status_code == 404:
        raise OGGAPIError(f"Role '{role}' not found")
    if r.status_code != 200:
        raise OGGAPIError(f"List users failed {format_api_error(r)}")
    log_ogg_messages(r)
    body = r.json()
    log.debug("authorizations/%s response: %s", role, body)
    items = body.get("response", {}).get("items", [])
    return [
        {"user": _name_from_item(item), "role": role}
        for item in items
        if _name_from_item(item)
    ]


def get_user(client, role, username):
    r = client.get(f"/authorizations/{role}/{username}")
    if r.status_code == 404:
        raise OGGAPIError(f"User '{username}' with role '{role}' not found")
    if r.status_code != 200:
        raise OGGAPIError(f"Get user failed {format_api_error(r)}")
    return r.json().get("response", {})


def create_user(client, role, username, password, info=None, fail_if_exists=True):
    if fail_if_exists and user_exists(client, role, username):
        raise OGGAPIError(f"User '{username}' already exists with role '{role}'")

    payload = {
        "$schema": "ogg:userRole",
        "role": role,
        "type": "Basic",
        "user": username,
        "credential": password,
    }
    if info:
        payload["info"] = info

    r = client.post(f"/authorizations/{role}/{username}", payload)
    if r.status_code not in (200, 201):
        raise OGGAPIError(f"Create user failed {format_api_error(r)}")
    log_ogg_messages(r)
    return r


def update_user(client, role, username, password=None, info=None):
    payload = {}
    if password is not None:
        payload["credential"] = password
    if info is not None:
        payload["info"] = info
    if not payload:
        raise ValueError("No fields to update — supply at least one of --password-file, --info")
    r = client.patch(f"/authorizations/{role}/{username}", payload)
    if r.status_code not in (200, 202, 204):
        raise OGGAPIError(f"Update user failed {format_api_error(r)}")
    log_ogg_messages(r)
    return r


def delete_user(client, role, username):
    if not user_exists(client, role, username):
        raise OGGAPIError(f"User '{username}' with role '{role}' does not exist")
    r = client.delete(f"/authorizations/{role}/{username}")
    if r.status_code not in (200, 202, 204):
        raise OGGAPIError(f"Delete user failed {format_api_error(r)}")
    log_ogg_messages(r)
    return r
