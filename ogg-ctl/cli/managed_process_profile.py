import json
import logging

from cli.output import print_name_list
from api.managed_process_profile_ops import (
    build_profile_payload,
    create_profile,
    update_profile,
    list_profiles,
    get_profile,
    delete_profile,
)

log = logging.getLogger(__name__)


def handle_managed_process_profile(args, client):
    if args.action == "create":
        payload = build_profile_payload(
            auto_start=args.auto_start,
            auto_start_delay=args.auto_start_delay,
            auto_restart=args.auto_restart,
            auto_restart_retries=args.auto_restart_retries,
            auto_restart_delay=args.auto_restart_delay,
            on_success=args.on_success,
            window=args.window,
            disable_on_failure=args.disable_on_failure,
        )
        create_profile(client, args.name, payload)
        log.info("Managed process profile '%s' created", args.name)

    elif args.action == "update":
        payload = build_profile_payload(
            auto_start=args.auto_start,
            auto_start_delay=args.auto_start_delay,
            auto_restart=args.auto_restart,
            auto_restart_retries=args.auto_restart_retries,
            auto_restart_delay=args.auto_restart_delay,
            on_success=args.on_success,
            window=args.window,
            disable_on_failure=args.disable_on_failure,
        )
        update_profile(client, args.name, payload)
        log.info("Managed process profile '%s' updated", args.name)

    elif args.action == "list":
        profiles = list_profiles(client)
        if not profiles:
            log.info("No managed process profiles found")
            return
        print_name_list(profiles)

    elif args.action == "get":
        profile = get_profile(client, args.name)
        print(json.dumps(profile, indent=2))

    elif args.action == "delete":
        delete_profile(client, args.name)
        log.info("Managed process profile '%s' deleted", args.name)
