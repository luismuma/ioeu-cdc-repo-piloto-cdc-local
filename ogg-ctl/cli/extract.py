import logging

from cli._io import load_lines
import json as _json
from cli.output import print_status_table, print_info, print_rows, is_json_mode
from api.errors import die
from api.trail import parse_trail, TrailError
from api.tablelist import read_tables_file, parse_tables_json, format_extract_table_line
from api.prm_builder import build_prm_lines
from api.managed_process_profile_ops import profile_ref
from api._common import build_managed_process_settings
from api.extract_ops import (
    create_extract_payload,
    create_extract,
    validate_only_create,
    register_extract,
    attach_trail,
    list_extracts,
    get_extract_status,
    get_extract_params,
    set_extract_params,
    append_extract_params,
    get_extract_info,
    get_extract_history,
    get_extract_report,
    send_command,
    start_extract,
    stop_extract,
    update_extract,
    delete_extract,
    get_table_stats,
)

log = logging.getLogger(__name__)


def _resolve_mps(args):
    """Return the managedProcessSettings value (dict or profile-name string), or None."""
    has_inline = any(getattr(args, k, None) is not None for k in [
        "auto_start", "auto_restart",
    ])
    if args.managed_process_profile and has_inline:
        die("--managed-process-profile cannot be combined with inline --auto-start/--auto-restart flags")
    if args.managed_process_profile:
        return profile_ref(args.managed_process_profile)
    return build_managed_process_settings(
        auto_start=args.auto_start,
        auto_start_delay=args.auto_start_delay,
        auto_restart=args.auto_restart,
        auto_restart_retries=args.auto_restart_retries,
        auto_restart_delay=args.auto_restart_delay,
        on_success=args.on_success,
        window=args.window,
        disable_on_failure=args.disable_on_failure,
    )


# Single source of truth: extract type -> profile (template) + source_type (API payload)
EXTRACT_TYPES = {
    "integrated":   {"profile": "oracle_integrated",   "source_type": "tranlogs"},
    "initial_load": {"profile": "oracle_initial_load", "source_type": "tables"},
    "mongo_classic": {"profile": "mongo_classic",      "source_type": "tranlogs"},
}


def _load_tables(args):
    """Return tables list from either --tables-file (path) or --tables-json (JSON payload)."""
    if args.tables_json:
        return parse_tables_json(args.tables_json, for_resource="extract")
    return read_tables_file(args.tables_file)




def handle_extract(args, client, cfg):
    if args.action == "create":
        et = EXTRACT_TYPES[args.extract_type]
        profile = et["profile"]
        source_type = et["source_type"]
        domain = args.domain or cfg.get("domain", "OracleGoldenGate")
        description = args.description or cfg.get("description", "Created via automation")

        try:
            parse_trail(args.trail)
        except TrailError as e:
            die(str(e))

        tables = _load_tables(args)
        prm_lines = build_prm_lines(
            resource="extract",
            profile=profile,
            context={
                "extract_name": args.name,
                "useridalias": args.useridalias,
                "domain": domain,
                "trail_path": args.trail,
                "sourcecatalog": args.pdb,
                "table_lines": [format_extract_table_line(t) for t in tables],
            },
        )
        payload = create_extract_payload(
            prm_lines=prm_lines,
            description=description,
            source_type=source_type,
            critical=args.critical or cfg.get("critical", False),
            encryption_profile=args.encryption_profile or cfg.get("encryption_profile"),
            managed_process_settings=_resolve_mps(args),
        )
        create_extract(client, args.name, payload)
        log.info("Extract %s created", args.name)
        if source_type == "tranlogs":
            attach_trail(client, args.name, args.trail)
            log.info("Trail %s attached to extract %s", args.trail, args.name)

    elif args.action == "register":
        register_extract(
            client=client,
            extract_name=args.name,
            optimized=args.optimized,
            replace=args.replace,
        )
        log.info("Extract %s registered", args.name)

    elif args.action == "validate":
        et = EXTRACT_TYPES[args.extract_type]
        profile = et["profile"]
        source_type = et["source_type"]
        domain = args.domain or cfg.get("domain", "OracleGoldenGate")
        if not args.trail:
            die("--trail is required")
        trail = args.trail
        description = args.description or cfg.get("description", "Created via automation")

        try:
            parse_trail(trail)
        except TrailError as e:
            die(str(e))

        tables = _load_tables(args)
        prm_lines = build_prm_lines(
            resource="extract",
            profile=profile,
            context={
                "extract_name": args.name,
                "useridalias": args.useridalias,
                "domain": domain,
                "trail_path": trail,
                "sourcecatalog": args.pdb,
                "table_lines": [format_extract_table_line(t) for t in tables],
            },
        )
        payload = create_extract_payload(
            prm_lines=prm_lines,
            description=description,
            source_type=source_type,
            critical=args.critical or cfg.get("critical", False),
            encryption_profile=args.encryption_profile or cfg.get("encryption_profile"),
            managed_process_settings=_resolve_mps(args),
        )
        validate_only_create(client, args.name, payload)
        log.info("Extract %s validation OK (create then delete succeeded)", args.name)

    elif args.action == "list":
        extracts = list_extracts(client)
        if not extracts:
            log.info("No extracts found")
            return
        print_status_table([(e["name"], e["status"]) for e in extracts])

    elif args.action == "start":
        start_extract(client, args.name)
        log.info("Extract %s started", args.name)

    elif args.action == "stop":
        stop_extract(client, args.name)
        log.info("Extract %s stopped", args.name)

    elif args.action == "status":
        status = get_extract_status(client, args.name)
        log.info("Extract %s: %s", args.name, status)

    elif args.action == "params":
        if args.subaction == "get":
            lines = get_extract_params(client, args.name)
            print("\n".join(lines))
        elif args.subaction == "set":
            lines = load_lines(args)
            set_extract_params(client, args.name, lines)
            log.info("Extract %s params replaced (%d lines)", args.name, len(lines))
        elif args.subaction == "append":
            lines = load_lines(args)
            append_extract_params(client, args.name, lines)
            log.info("Extract %s params appended (%d new lines)", args.name, len(lines))

    elif args.action == "info":
        info = get_extract_info(client, args.name)
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

    elif args.action == "history":
        history = get_extract_history(client, args.name)
        if not history:
            log.info("No history found for extract %s", args.name)
            return
        if is_json_mode():
            print(_json.dumps(history, indent=2))
        else:
            for i, h in enumerate(history):
                print_info({
                    "run":       i + 1,
                    "status":    h.get("status"),
                    "started":   h.get("started"),
                    "finished":  h.get("finished"),
                    "pid":       h.get("processId"),
                    "exit_code": h.get("exitCode"),
                })

    elif args.action == "report":
        report_name = args.report or f"{args.name}.rpt"
        lines = get_extract_report(client, args.name, report_name)
        if args.head is not None:
            lines = lines[:args.head]
        elif args.tail is not None:
            lines = lines[-args.tail:]
        print("\n".join(lines))

    elif args.action == "stats":
        if args.period in ("daily", "total"):
            rows = get_table_stats(client, args.name, args.period)
            print_rows(rows)
        else:
            parts = []
            if args.period:
                parts.append(args.period.upper())
            if args.table:
                parts.append(f"TABLE {args.table}")
            arguments = ", ".join(parts) if parts else None
            reply, reply_data = send_command(client, args.name, "STATS", arguments=arguments)
            if is_json_mode() and reply_data:
                print(_json.dumps(reply_data, indent=2))
            elif reply:
                print(reply)

    elif args.action == "cmd":
        reply, reply_data = send_command(
            client, args.name, args.command.upper(),
            arguments=args.arguments,
            is_reported=args.is_reported,
        )
        if is_json_mode() and reply_data:
            print(_json.dumps(reply_data, indent=2))
        elif reply:
            print(reply)
        else:
            log.info("Command '%s' sent to extract %s (no reply)", args.command, args.name)

    elif args.action == "update":
        update_extract(
            client, args.name,
            description=args.description,
            critical=args.critical,
            encryption_profile=args.encryption_profile,
            managed_process_settings=_resolve_mps(args),
        )
        log.info("Extract %s updated", args.name)

    elif args.action == "delete":
        delete_extract(client, args.name)
        log.info("Extract %s deleted", args.name)
