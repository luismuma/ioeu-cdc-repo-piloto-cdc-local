import logging

from cli.output import print_status_table, print_info, print_rows
from api.deployment_ops import list_deployments, get_deployment_status, get_deployment_info, stop_deployment, get_messages
from api.errors import OGGAPIError

log = logging.getLogger(__name__)


def handle_deployment(args, client, cfg):
    if args.action == "list":
        deployments = list_deployments(client)
        if not deployments:
            log.info("No deployments found")
            return
        print_status_table([(d["name"], d["status"]) for d in deployments])

    elif args.action == "status":
        name = args.name or cfg.get("deployment")
        status = get_deployment_status(client, name)
        log.info("Deployment %s: %s", name, status)

    elif args.action == "info":
        name = args.name or cfg.get("deployment")
        info = get_deployment_info(client, name)
        print_info(info)

    elif args.action == "stop":
        name = args.name or cfg.get("deployment")
        stop_deployment(client, name)
        log.info("Deployment '%s' stop requested via Service Manager", name)

    elif args.action == "check-auth":
        _check_auth(client, cfg)

    elif args.action == "messages":
        msgs = get_messages(
            client,
            severity=getattr(args, "severity", None),
            since=getattr(args, "since", None),
            limit=getattr(args, "limit", None),
        )
        if not msgs:
            log.info("No messages found")
            return
        rows = [
            {
                "code":     m.get("code", ""),
                "severity": m.get("severity", ""),
                "issued":   m.get("issued", ""),
                "title":    m.get("title", ""),
            }
            for m in msgs
        ]
        print_rows(rows)


def _check_auth(client, cfg):
    """Fire a cheap request to determine which auth method is active."""
    deployment = cfg.get("deployment", "unknown")
    try:
        r = client.get("/deployments", service="service_manager")
    except OGGAPIError as e:
        log.error("check-auth: connection failed — %s", e)
        raise

    mode = client.active_auth_mode
    if r.status_code == 401:
        log.error(
            "check-auth: authentication failed for deployment '%s' "
            "(tried: %s, HTTP 401) — check credentials", deployment, mode
        )
        raise OGGAPIError(
            f"Authentication failed for '{deployment}': "
            "basic auth rejected and Entra ID Bearer also rejected (or not configured). "
            "Verify OGG_ADMIN_PASSWORD / OGG_AUTH_PROFILE_CLIENT_SECRET."
        )

    mode_label = {
        "basic":   "basic (OGG_ADMIN_PASSWORD)",
        "bearer":  "entraid_bearer (Entra ID client-credentials)",
        "probing": "basic (OGG_ADMIN_PASSWORD)",
    }.get(mode, mode)

    print_info({
        "deployment":  deployment,
        "auth":        mode_label,
        "http_status": r.status_code,
    })
    log.info("check-auth: OK — using %s", mode_label)
