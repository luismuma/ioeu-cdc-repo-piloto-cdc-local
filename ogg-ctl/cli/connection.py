import logging

from cli.output import print_name_list
from api.connection_ops import (
    create_connection,
    list_connections,
    delete_connection,
)

log = logging.getLogger(__name__)


def handle_connection(args, client):
    if args.action == "create":
        create_connection(
            client,
            connection_name=args.name,
            alias=args.alias,
            domain=args.domain,
        )
        log.info("Connection '%s' created (alias: %s/%s)", args.name, args.domain, args.alias)

    elif args.action == "list":
        connections = list_connections(client)
        if not connections:
            log.info("No connections found")
            return
        print_name_list([c["name"] for c in connections])

    elif args.action == "delete":
        delete_connection(client, connection_name=args.name)
        log.info("Connection '%s' deleted", args.name)
