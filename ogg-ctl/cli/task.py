import json
import logging

from cli.output import print_info, print_name_list
from api.errors import die
from api.trail import parse_trail
from api.task_ops import (
    build_purge_trail_payload,
    list_purge_trail_tasks,
    validate_trails_exist,
    get_task,
    create_task,
    update_task,
    delete_task,
    send_task_cmd,
)

log = logging.getLogger(__name__)


def _parse_trails(args):
    return [parse_trail(entry) for entry in args.trail]


def _format_task(name, task):
    cmd = task.get("command", {})
    sched = task.get("schedule", {}).get("every", {})
    keep = (cmd.get("keep") or [{}])[0]
    trails = cmd.get("trails", [])
    return {
        "name":            name,
        "status":          task.get("status", ""),
        "enabled":         task.get("enabled", False),
        "critical":        task.get("critical", False),
        "use_checkpoints": cmd.get("useCheckpoints", False),
        "trails":          ", ".join(f"{t['name']}:{t['path']}" for t in trails),
        "keep":            f"{keep.get('value')} {keep.get('units')} ({keep.get('type')})",
        "schedule":        f"every {sched.get('value')} {sched.get('units')}",
    }


def handle_task(args, client):
    if args.action != "purge-trail":
        die(f"Unknown task type '{args.action}'")

    sub = args.subaction

    if sub == "list":
        tasks = list_purge_trail_tasks(client)
        if not tasks:
            log.info("No purge trail tasks found")
            return
        names = [t.get("name", t.get("$schema", "")) for t in tasks]
        print_name_list(names)

    elif sub == "get":
        task = get_task(client, args.name)
        print(json.dumps(task, indent=2))

    elif sub == "create":
        trails = _parse_trails(args)
        if not getattr(args, "skip_trail_check", False):
            validate_trails_exist(client, trails)
        payload = build_purge_trail_payload(
            trails=trails,
            keep_value=args.keep_value,
            keep_units=args.keep_units,
            schedule_value=args.schedule_value,
            schedule_units=args.schedule_units,
            enabled=args.enabled or False,
            critical=args.critical or False,
            use_checkpoints=args.use_checkpoints or False,
        )
        create_task(client, args.name, payload)
        log.info("Purge trail task '%s' created", args.name)

    elif sub == "update":
        existing = get_task(client, args.name)
        cmd = existing.get("command", {})
        sched = existing.get("schedule", {}).get("every", {})
        keep = (cmd.get("keep") or [{}])[0]

        trails = _parse_trails(args) if args.trail else cmd.get("trails", [])
        payload = build_purge_trail_payload(
            trails=trails,
            keep_value=args.keep_value   if args.keep_value   is not None else keep.get("value"),
            keep_units=args.keep_units   if args.keep_units   is not None else keep.get("units"),
            schedule_value=args.schedule_value if args.schedule_value is not None else sched.get("value"),
            schedule_units=args.schedule_units if args.schedule_units is not None else sched.get("units"),
            enabled=args.enabled         if args.enabled       is not None else existing.get("enabled", False),
            critical=args.critical       if args.critical      is not None else existing.get("critical", False),
            use_checkpoints=args.use_checkpoints if args.use_checkpoints is not None else cmd.get("useCheckpoints", False),
        )
        update_task(client, args.name, payload)
        log.info("Purge trail task '%s' updated", args.name)

    elif sub == "delete":
        delete_task(client, args.name)
        log.info("Purge trail task '%s' deleted", args.name)

    elif sub in ("start", "stop", "enable", "disable"):
        send_task_cmd(client, args.name, sub)
        log.info("Purge trail task '%s': %s", args.name, sub)

    else:
        die(f"Unknown subaction '{sub}'")
