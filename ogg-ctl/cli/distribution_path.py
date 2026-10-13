import logging

from cli.output import print_status_table, print_info
from api.distribution_path_ops import (
    create_distribution_path_payload,
    create_distribution_path,
    list_distribution_paths,
    get_distribution_path_status,
    get_distribution_path_info,
    get_distribution_path_stats,
    start_distribution_path,
    stop_distribution_path,
    update_distribution_path,
    delete_distribution_path,
)

log = logging.getLogger(__name__)


def handle_distribution_path(args, client, cfg):
    if args.action == "create":
        domain = args.domain or cfg.get("domain", "OracleGoldenGate")
        description = args.description or cfg.get("description", "Created via automation")

        payload = create_distribution_path_payload(
            name=args.name,
            source_uri=args.source_uri,
            target_uri=args.target_uri,
            description=description,
            source_alias=args.source_alias,
            target_alias=args.target_alias,
            domain=domain,
            begin=args.begin,
            critical=args.critical or cfg.get("critical", False),
            encryption_profile=args.encryption_profile or cfg.get("encryption_profile"),
        )
        create_distribution_path(client, args.name, payload)
        log.info("Distribution path '%s' created", args.name)

    elif args.action == "list":
        paths = list_distribution_paths(client)
        if not paths:
            log.info("No distribution paths found")
            return
        print_status_table([(p["name"], p["status"]) for p in paths])

    elif args.action == "start":
        start_distribution_path(client, args.name)
        log.info("Distribution path '%s' started", args.name)

    elif args.action == "stop":
        stop_distribution_path(client, args.name)
        log.info("Distribution path '%s' stopped", args.name)

    elif args.action == "status":
        status = get_distribution_path_status(client, args.name)
        log.info("Distribution path '%s': %s", args.name, status)

    elif args.action == "info":
        info = get_distribution_path_info(client, args.name)
        lag_s = info.get("lag")
        since_s = info.get("sinceLagReported")
        print_info({
            "name":             args.name,
            "status":           info.get("status", "unknown"),
            "pid":              info.get("processId"),
            "started":          info.get("lastStarted"),
            "position":         info.get("position"),
            "lag_seconds":      lag_s,
            "lag_since_seconds": since_s,
        })

    elif args.action == "stats":
        stats = get_distribution_path_stats(client, args.name)
        print_info(stats)

    elif args.action == "update":
        update_distribution_path(
            client, args.name,
            description=args.description,
            critical=args.critical,
            encryption_profile=args.encryption_profile,
        )
        log.info("Distribution path '%s' updated", args.name)

    elif args.action == "delete":
        delete_distribution_path(client, args.name)
        log.info("Distribution path '%s' deleted", args.name)
