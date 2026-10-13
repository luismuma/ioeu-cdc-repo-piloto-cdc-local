import logging

from api.errors import OGGAPIError, format_api_error
from api._common import log_ogg_messages, build_path_endpoint

log = logging.getLogger(__name__)


def distpath_exists(client, path_name):
    return client.get(f"/sources/{path_name}", service="distribution").status_code == 200


def build_path_endpoint(uri, alias=None, domain="OracleGoldenGate"):
    endpoint = {
        "$schema": "ogg:distPathEndpoint",
        "uri": uri,
    }
    if alias:
        endpoint["authenticationMethod"] = {
            "$schema": "ogg:credentialsRef",
            "domain": domain,
            "alias": alias,
        }
    return endpoint


def create_distribution_path_payload(
    name,
    source_uri,
    target_uri,
    description=None,
    source_alias=None,
    target_alias=None,
    domain="OracleGoldenGate",
    begin="now",
    critical=False,
    encryption_profile=None,
):
    payload = {
        "$schema": "ogg:distPath",
        "name": name,
        "source": build_path_endpoint(source_uri, alias=source_alias, domain=domain),
        "target": build_path_endpoint(target_uri, alias=target_alias, domain=domain),
        "begin": begin,
        "status": "stopped",
    }

    if description:
        payload["description"] = description

    if critical or encryption_profile:
        options = {}
        if critical is not None:
            options["critical"] = bool(critical)
        if encryption_profile:
            payload["encryptionProfile"] = encryption_profile
        if options:
            payload["options"] = options

    return payload


def create_distribution_path(client, path_name, payload, fail_if_exists=True):
    if fail_if_exists and distpath_exists(client, path_name):
        raise OGGAPIError(f"Distribution path '{path_name}' already exists")

    r = client.post(f"/sources/{path_name}", payload, service="distribution")
    if r.status_code not in (200, 201):
        raise OGGAPIError(f"Create failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r


def list_distribution_paths(client):
    r = client.get("/sources", service="distribution")
    if r.status_code != 200:
        raise OGGAPIError(f"List distribution paths failed {format_api_error(r)}")

    log_ogg_messages(r)
    items = r.json().get("response", {}).get("items", [])
    return [
        {"name": item.get("name", ""), "status": item.get("status", "")}
        for item in items
    ]


def get_distribution_path_status(client, path_name):
    r = client.get(f"/sources/{path_name}", service="distribution")
    if r.status_code == 404:
        raise OGGAPIError(f"Distribution path '{path_name}' does not exist")
    if r.status_code != 200:
        raise OGGAPIError(f"Get status failed {format_api_error(r)}")
    return r.json()["response"]["status"]


def get_distribution_path_info(client, path_name):
    r = client.get(f"/sources/{path_name}/info", service="distribution")
    if r.status_code == 404:
        raise OGGAPIError(f"Distribution path '{path_name}' does not exist")
    if r.status_code != 200:
        raise OGGAPIError(f"Get info failed {format_api_error(r)}")
    return r.json()["response"]


def get_distribution_path_stats(client, path_name):
    r = client.get(f"/sources/{path_name}/stats", service="distribution")
    if r.status_code == 404:
        raise OGGAPIError(f"Distribution path '{path_name}' does not exist")
    if r.status_code != 200:
        raise OGGAPIError(f"Get stats failed {format_api_error(r)}")
    return r.json()["response"]


def start_distribution_path(client, path_name):
    if not distpath_exists(client, path_name):
        raise OGGAPIError(f"Distribution path '{path_name}' does not exist, cannot start")

    r = client.patch(f"/sources/{path_name}", {"status": "running"}, service="distribution")
    if r.status_code not in (200, 204):
        raise OGGAPIError(f"Start failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r


def stop_distribution_path(client, path_name):
    if not distpath_exists(client, path_name):
        raise OGGAPIError(f"Distribution path '{path_name}' does not exist, cannot stop")

    r = client.patch(f"/sources/{path_name}", {"status": "stopped"}, service="distribution")
    if r.status_code not in (200, 204):
        raise OGGAPIError(f"Stop failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r


def update_distribution_path(client, path_name, *, description=None, critical=None, encryption_profile=None):
    payload = {}
    if description is not None:
        payload["description"] = description
    if encryption_profile is not None:
        payload["encryptionProfile"] = encryption_profile
    if critical is not None:
        payload.setdefault("options", {})["critical"] = bool(critical)
    if not payload:
        raise ValueError("No fields to update — supply at least one of --description, --critical/--no-critical, --encryption-profile")
    r = client.patch(f"/sources/{path_name}", payload, service="distribution")
    if r.status_code not in (200, 202):
        raise OGGAPIError(f"Update failed {format_api_error(r)}")
    log_ogg_messages(r)
    return r


def delete_distribution_path(client, path_name):
    if not distpath_exists(client, path_name):
        raise OGGAPIError(f"Distribution path '{path_name}' does not exist, cannot delete")

    status = get_distribution_path_status(client, path_name)
    if status.upper() not in ("STOPPED", "ABENDED", "KILLED"):
        raise OGGAPIError(
            f"Distribution path '{path_name}' is '{status}', must be stopped, abended, or killed before deleting"
        )

    r = client.delete(f"/sources/{path_name}", service="distribution")
    if r.status_code not in (200, 202, 204):
        raise OGGAPIError(f"Delete failed {format_api_error(r)}")

    log_ogg_messages(r)
    return r
