import logging

from api.errors import OGGAPIError, UserInputError, format_api_error
from api._common import log_ogg_messages

log = logging.getLogger(__name__)

_DOMAIN_MAP = {
    "oraclegoldengate": "OracleGoldenGate",
    "network":          "Network",
}


def normalize_domain(domain: str) -> str:
    normalized = _DOMAIN_MAP.get(domain.lower())
    if normalized is None:
        valid = ", ".join(_DOMAIN_MAP.values())
        raise UserInputError(f"Unknown domain '{domain}'. Valid values: {valid}")
    return normalized


def create_alias(client, domain, alias, userid, password):
    domain = normalize_domain(domain)
    payload = {"userid": userid, "password": password}

    r = client.post(f"/credentials/{domain}/{alias}", payload)
    if r.status_code not in (200, 201):
        raise OGGAPIError(f"Create alias '{domain}/{alias}' failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r


def get_alias(client, domain, alias):
    domain = normalize_domain(domain)
    r = client.get(f"/credentials/{domain}/{alias}")
    if r.status_code != 200:
        raise OGGAPIError(f"Get alias '{domain}/{alias}' failed {format_api_error(r)}")

    log_ogg_messages(r)
    response = r.json().get("response", {})
    return {
        "domain": domain,
        "alias": alias,
        "userid": response.get("userid", ""),
    }


def list_domains(client):
    r = client.get("/credentials")
    if r.status_code != 200:
        raise OGGAPIError(f"List credential domains failed {format_api_error(r)}")

    log_ogg_messages(r)
    items = r.json().get("response", {}).get("items", [])
    return [item["name"] for item in items]


def list_aliases(client, domain):
    domain = normalize_domain(domain)
    r = client.get(f"/credentials/{domain}")
    if r.status_code != 200:
        raise OGGAPIError(f"List aliases for domain '{domain}' failed {format_api_error(r)}")

    log_ogg_messages(r)
    items = r.json().get("response", {}).get("items", [])
    return [{"domain": domain, "name": item["name"]} for item in items]


def validate_alias(client, domain, alias):
    domain = normalize_domain(domain)
    r = client.get(f"/credentials/{domain}/{alias}/valid")
    if r.status_code != 200:
        raise OGGAPIError(f"Validate alias '{domain}/{alias}' failed {format_api_error(r)}")

    log_ogg_messages(r)
    body = r.json().get("response", {})
    return {
        "domain": body.get("domain", domain),
        "alias": body.get("alias", alias),
        "serverName": body.get("serverName"),
        "catalogName": body.get("catalogName"),
    }


def delete_alias(client, domain, alias):
    domain = normalize_domain(domain)
    r = client.delete(f"/credentials/{domain}/{alias}")
    if r.status_code not in (200, 204):
        raise OGGAPIError(f"Delete alias '{domain}/{alias}' failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r
