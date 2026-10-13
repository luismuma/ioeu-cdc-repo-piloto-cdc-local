import logging

from api.errors import OGGAPIError, format_api_error

log = logging.getLogger(__name__)


def list_deployments(client):
    """Return a list of {name, status} dicts from the Service Manager."""
    r = client.get("/deployments", service="service_manager")
    if r.status_code != 200:
        raise OGGAPIError(f"List deployments failed {format_api_error(r)}")
    items = r.json().get("response", {}).get("items", [])
    return [{"name": item["name"], "status": item.get("status", "unknown")} for item in items]


def get_deployment_status(client, name):
    """Return the status string for a specific deployment."""
    r = client.get(f"/deployments/{name}", service="service_manager")
    if r.status_code == 404:
        raise OGGAPIError(f"Deployment '{name}' not found")
    if r.status_code != 200:
        raise OGGAPIError(f"Get deployment status failed {format_api_error(r)}")
    return r.json()["response"]["status"]


def stop_deployment(client, name):
    """Request a graceful stop of a deployment via the Service Manager API."""
    r = client.patch(f"/deployments/{name}", {"status": "stopped"}, service="service_manager")
    if r.status_code not in (200, 202, 204):
        raise OGGAPIError(f"Stop deployment failed {format_api_error(r)}")
    return r


def get_deployment_info(client, name):
    """Return a summary dict for a specific deployment (status, enabled, cluster role)."""
    r = client.get(f"/deployments/{name}", service="service_manager")
    if r.status_code == 404:
        raise OGGAPIError(f"Deployment '{name}' not found")
    if r.status_code != 200:
        raise OGGAPIError(f"Get deployment info failed {format_api_error(r)}")
    d = r.json()["response"]
    cluster = d.get("cluster", [])
    role = cluster[0].get("role") if cluster else None
    return {
        "name":    name,
        "status":  d.get("status", "unknown"),
        "enabled": d.get("enabled"),
        "role":    role,
    }


def get_messages(client, severity=None, since=None, limit=None):
    """Return OGG system messages from the admin service.

    The response uses a top-level 'messages[]' key (no 'response' wrapper).
    severity: 'ERROR' | 'WARNING' | 'INFO' (case-insensitive)
    since:    ISO 8601 string, e.g. '2026-09-15T00:00:00Z'
    limit:    max number of messages to return
    """
    params = {}
    if severity:
        params["severity"] = severity.upper()
    if since:
        params["since"] = since
    if limit:
        params["limit"] = limit
    r = client.get("/messages", params=params)
    if r.status_code != 200:
        raise OGGAPIError(f"Get messages failed {format_api_error(r)}")
    return r.json().get("messages", [])
