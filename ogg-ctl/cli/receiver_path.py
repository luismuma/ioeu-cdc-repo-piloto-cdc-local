import logging

from cli.output import print_status_table, print_info
from api.receiver_path_ops import (
    create_receiver_path_payload,
    create_receiver_path,
    list_receiver_paths,
    get_receiver_path_status,
    get_receiver_path_info,
    get_receiver_path_stats,
    start_receiver_path,
    stop_receiver_path,
    update_receiver_path,
    delete_receiver_path,
)

log = logging.getLogger(__name__)


def handle_receiver_path(args, client, cfg):
    if args.action == "create":
        domain = args.domain or cfg.get("domain", "OracleGoldenGate")
        description = args.description or cfg.get("description", "Created via automation")

        payload = create_receiver_path_payload(
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
        create_receiver_path(client, args.name, payload)
        log.info("Receiver path '%s' created", args.name)

    elif args.action == "list":
        paths = list_receiver_paths(client)
        if not paths:
            log.info("No receiver paths found")
            return
        print_status_table([(p["name"], p["status"]) for p in paths])

    elif args.action == "start":
        start_receiver_path(client, args.name)
        log.info("Receiver path '%s' started", args.name)

    elif args.action == "stop":
        stop_receiver_path(client, args.name)
        log.info("Receiver path '%s' stopped", args.name)

    elif args.action == "status":
        status = get_receiver_path_status(client, args.name)
        log.info("Receiver path '%s': %s", args.name, status)

    elif args.action == "info":
        info = get_receiver_path_info(client, args.name)
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
        stats = get_receiver_path_stats(client, args.name)
        print_info(stats)

    elif args.action == "update":
        update_receiver_path(
            client, args.name,
            description=args.description,
            critical=args.critical,
            encryption_profile=args.encryption_profile,
        )
        log.info("Receiver path '%s' updated", args.name)

    elif args.action == "delete":
        delete_receiver_path(client, args.name)
        log.info("Receiver path '%s' deleted", args.name)
