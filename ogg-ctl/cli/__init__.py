import logging
import os
import sys
import urllib3
from pathlib import Path

import yaml

from api.client import OGGClient, DEFAULT_TIMEOUT
from api.auth import entra_cfg_from_inventory
from api.errors import die, OGGAPIError, UserInputError

from cli.parser import build_parser
from cli import output as _output
from cli.extract import handle_extract
from cli.replicat import handle_replicat
from cli.distribution_path import handle_distribution_path
from cli.receiver_path import handle_receiver_path
from cli.config_file import handle_config_file
from cli.credential_alias import handle_credential_alias
from cli.connection import handle_connection
from cli.managed_process_profile import handle_managed_process_profile
from cli.task import handle_task
from cli.user import handle_user
from cli.deployment import handle_deployment
from cli.trandata import handle_trandata
from cli.templates import handle_templates

log = logging.getLogger("ogg.cli")

_INVENTORY = Path(
    os.environ.get("OGG_INVENTORY") or
    Path(__file__).parent.parent.parent / "cdc-controller" / "config" / "inventory_deployments" / "inventory.yaml"
)


def load_cfg(deployment_arg):
    """Return a flat merged config for the given deployment key or file path."""
    p = Path(deployment_arg)
    if p.exists():
        return yaml.safe_load(p.read_text())
    if not _INVENTORY.exists():
        die(f"Inventory not found at {_INVENTORY}. Set OGG_INVENTORY env var to override.")
    inventory = yaml.safe_load(_INVENTORY.read_text())
    if deployment_arg not in inventory:
        available = ", ".join(k for k in inventory if k != "defaults")
        die(f"Unknown deployment '{deployment_arg}'. Available: {available}")
    defaults = inventory.get("defaults", {})
    return {**defaults, **inventory[deployment_arg]}


def maybe_suppress_tls_warning(cfg):
    if cfg.get("suppress_insecure_warning", False) and cfg.get("verify_tls", True) is False:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def build_client(cfg):
    verify_tls = cfg.get("verify_tls", True)
    timeout_raw = cfg.get("timeout", DEFAULT_TIMEOUT)
    timeout = tuple(timeout_raw) if isinstance(timeout_raw, list) else timeout_raw
    return OGGClient(
        admin_url=cfg["admin_url"],
        api_version=cfg.get("api_version", "v2"),
        user=cfg["admin_user"],
        password_file=cfg.get("admin_password_file"),
        entra_cfg=entra_cfg_from_inventory(cfg),
        verify_tls=verify_tls,
        distribution_url=cfg.get("distribution_url"),
        receiver_url=cfg.get("receiver_url"),
        performance_url=cfg.get("performance_url"),
        deployment_name=cfg.get("deployment"),
        nginx_proxy=cfg.get("nginx_proxy", False),
        timeout=timeout,
    )


def main():
    p = build_parser()
    args = p.parse_args()

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    _output.set_format(args.format)

    # Local-only resources don't need a deployment / OGG client.
    if args.resource == "templates":
        try:
            handle_templates(args, client=None, cfg=None)
        except UserInputError as e:
            die(str(e))
        return

    cfg = load_cfg(args.deployment)
    maybe_suppress_tls_warning(cfg)
    client = build_client(cfg)

    try:
        if args.resource == "extract":
            handle_extract(args, client, cfg)
        elif args.resource == "replicat":
            handle_replicat(args, client, cfg)
        elif args.resource == "distribution-path":
            handle_distribution_path(args, client, cfg)
        elif args.resource == "receiver-path":
            handle_receiver_path(args, client, cfg)
        elif args.resource == "config-file":
            handle_config_file(args, client, cfg)
        elif args.resource == "credential-alias":
            handle_credential_alias(args, client)
        elif args.resource == "connection":
            handle_connection(args, client)
        elif args.resource == "managed-process-profile":
            handle_managed_process_profile(args, client)
        elif args.resource == "task":
            handle_task(args, client)
        elif args.resource == "user":
            handle_user(args, client)
        elif args.resource == "deployment":
            handle_deployment(args, client, cfg)
        elif args.resource == "trandata":
            handle_trandata(args, client, cfg)
        else:
            die(f"Unknown resource '{args.resource}'")
    except UserInputError as e:
        die(str(e))
    except OGGAPIError as e:
        die(str(e))
