import logging

from api.credential_alias_ops import normalize_domain
from api.errors import OGGAPIError, format_api_error
from api._common import log_ogg_messages

log = logging.getLogger(__name__)


def log_ogg_messages(r):
    try:
        for msg in r.json().get("messages", []):
            severity = msg.get("severity", "INFO").upper()
            code = msg.get("code", "")
            title = msg.get("title", "")
            log.debug("OGG [%s] %s: %s", severity, code, title)
    except Exception:
        pass


def create_connection(client, connection_name, alias, domain="OracleGoldenGate"):
    domain = normalize_domain(domain)
    payload = {
        "credentials": {
            "domain": domain,
            "alias": alias,
        }
    }

    r = client.post(f"/connections/{connection_name}", payload)
    if r.status_code not in (200, 201):
        raise OGGAPIError(f"Create connection '{connection_name}' failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r


def list_connections(client):
    r = client.get("/connections")
    if r.status_code != 200:
        raise OGGAPIError(f"List connections failed {format_api_error(r)}")

    log_ogg_messages(r)
    items = r.json().get("response", {}).get("items", [])
    return [
        {"name": item.get("name", ""), "status": item.get("status", "")}
        for item in items
    ]


def delete_connection(client, connection_name):
    r = client.delete(f"/connections/{connection_name}")
    if r.status_code not in (200, 204):
        raise OGGAPIError(f"Delete connection '{connection_name}' failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r
