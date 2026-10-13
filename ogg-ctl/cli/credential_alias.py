import getpass
import logging
import os
import sys
from pathlib import Path

from cli.output import print_info
from api.errors import die
from api.credential_alias_ops import (
    create_alias,
    get_alias,
    list_domains,
    list_aliases,
    validate_alias,
    delete_alias,
)

log = logging.getLogger(__name__)


def _resolve_password(args):
    """Return the password from --password-file, --password-env, or an interactive prompt."""
    if args.password_file:
        return Path(args.password_file).read_text().strip()
    if args.password_env:
        value = os.environ.get(args.password_env)
        if value is None:
            die(f"Environment variable '{args.password_env}' (referenced by --password-env) is not set")
        return value
    if not sys.stdin.isatty():
        die("No password source provided. Use --password-file or --password-env, or run interactively to be prompted.")
    return getpass.getpass(f"Password for alias '{args.alias}': ")


def handle_credential_alias(args, client):
    if args.action == "create":
        password = _resolve_password(args)
        create_alias(
            client,
            domain=args.domain,
            alias=args.alias,
            userid=args.userid,
            password=password,
        )
        log.info("Alias '%s/%s' created", args.domain, args.alias)

    elif args.action == "info":
        info = get_alias(client, domain=args.domain, alias=args.alias)
        print_info({
            "domain": info["domain"],
            "alias":  info["alias"],
            "userid": info["userid"],
        })

    elif args.action == "list":
        domain = getattr(args, "domain", None)
        if domain:
            aliases = list_aliases(client, domain=domain)
            if not aliases:
                log.info("No aliases found in domain '%s'", domain)
                return
            for a in aliases:
                print(f"{a['domain']:<25} {a['name']}")
        else:
            domains = list_domains(client)
            if not domains:
                log.info("No credential domains found")
                return
            for d in domains:
                aliases = list_aliases(client, domain=d)
                print(d)
                if aliases:
                    for a in aliases:
                        print(f"  {a['name']}")
                else:
                    print("  (empty)")

    elif args.action == "validate":
        info = validate_alias(client, domain=args.domain, alias=args.alias)
        print_info({
            "domain":  info["domain"],
            "alias":   info["alias"],
            "server":  info["serverName"],
            "catalog": info["catalogName"],
        })
        log.info("Alias '%s/%s' is valid", args.domain, args.alias)

    elif args.action == "delete":
        delete_alias(client, domain=args.domain, alias=args.alias)
        log.info("Alias '%s/%s' deleted", args.domain, args.alias)
