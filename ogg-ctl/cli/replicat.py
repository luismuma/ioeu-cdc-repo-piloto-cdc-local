import logging

from cli._io import load_lines, load_properties, load_json_object
import json as _json
from cli.output import print_status_table, print_info, print_rows, is_json_mode
from api.errors import die
from api.trail import parse_trail, TrailError
from api.tablelist import read_tables_file, parse_tables_json, format_replicat_map_line
from api.prm_builder import (
    build_prm_lines, load_target_connection, apply_target_field_map,
)
from api.managed_process_profile_ops import profile_ref
from api._common import build_managed_process_settings
from api.replicat_ops import (
    create_replicat_payload,
    create_replicat,
    validate_only_create,
    list_replicats,
    get_replicat_status,
    get_replicat_params,
    set_replicat_params,
    append_replicat_params,
    get_replicat_properties,
    set_replicat_properties,
    merge_replicat_properties,
    render_replicat_properties,
    get_replicat_info,
    get_replicat_history,
    get_replicat_report,
    send_command,
    start_replicat,
    stop_replicat,
    update_replicat,
    delete_replicat,
    get_table_stats,
)

log = logging.getLogger(__name__)

REPLICAT_TYPES = {
    "classic":     {"profile": "oracle_classic",     "mode": {"type": "nonintegrated"}},
    "coordinated": {"profile": "oracle_coordinated", "mode": {"type": "coordinated"}},
}


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


def _build_source(trail):
    parsed = parse_trail(trail)
    source = {"$schema": "ogg:trailRef", "name": parsed["name"]}
    if "path" in parsed:
        source["path"] = parsed["path"]
    return source


def _load_tables(args):
    """Return tables list from either --tables-file (path) or --tables-json (JSON payload)."""
    if args.tables_json:
        return parse_tables_json(args.tables_json, for_resource="replicat")
    return read_tables_file(args.tables_file)


def _validate_target_flags(args):
    """Enforce flag combinations driven by --target:
       oracle           → --useridalias required; --target-name / --target-vars-json not applicable
       kafka_confluent  → at least one of --target-name or --target-vars-json required; --useridalias not applicable
       s3               → same as kafka_confluent
    """
    if args.target == "oracle":
        if not args.useridalias:
            die("--useridalias is required when --target=oracle")
        if args.target_vars_json:
            die("--target-vars-json is not used with --target=oracle")
        if getattr(args, "target_name", None):
            die("--target-name is not used with --target=oracle")
    else:
        if not args.target_vars_json and not getattr(args, "target_name", None):
            die(f"--target-name or --target-vars-json is required when --target={args.target}")
        if args.useridalias:
            die(f"--useridalias does not apply when --target={args.target} (Big Data has no target DB)")


def _properties_file_path(cfg, replicat_name):
    """Absolute path of the replicat's .properties file on the OGG server.
    Used for TARGETDB LIBFILE SET property=... in the PRM."""
    ogg_data_root = cfg.get("ogg_data_root", "/u01/ogg/data")
    deployment    = cfg.get("deployment", "")
    return f"{ogg_data_root}/gg_deployments/ogg_daa_rep/{deployment}/etc/conf/ogg/{replicat_name}.properties"




def handle_replicat(args, client, cfg):
    if args.action == "create":
        _validate_target_flags(args)
        rt = REPLICAT_TYPES[args.replicat_type]
        profile = rt["profile"]
        mode = dict(rt["mode"])
        domain = args.domain or cfg.get("domain", "OracleGoldenGate")
        description = args.description or cfg.get("description", "Created via automation")
        checkpoint_table = (
            args.checkpoint_table
            or cfg.get("checkpoint_table")
        )

        if args.replicat_type == "coordinated" and args.max_threads:
            mode["maxThreads"] = args.max_threads

        try:
            source = _build_source(args.trail)
        except TrailError as e:
            die(str(e))

        tables = _load_tables(args)
        prm_lines = build_prm_lines(
            resource="replicat",
            profile=profile,
            context={
                "replicat_name":        args.name,
                "target":               args.target,
                "useridalias":          args.useridalias,
                "domain":               domain,
                "sourcecatalog":        args.pdb,
                "properties_file_path": _properties_file_path(cfg, args.name),
                "map_lines":            [format_replicat_map_line(t) for t in tables],
            },
        )
        payload = create_replicat_payload(
            prm_lines=prm_lines,
            description=description,
            mode=mode,
            source=source,
            checkpoint_table=checkpoint_table,
            critical=args.critical or cfg.get("critical", False),
            encryption_profile=args.encryption_profile or cfg.get("encryption_profile"),
            managed_process_settings=_resolve_mps(args),
        )
        create_replicat(client, args.name, payload)
        log.info("Replicat %s created", args.name)

        # For Big Data targets, also render+upload the handler .properties file
        if args.target != "oracle":
            context = {
                "replicat_name":   args.name,
                "deployment_name": cfg.get("deployment"),
                "env":             cfg.get("env"),
                "db_engine":       cfg.get("db_engine", ""),
            }
            if getattr(args, "target_name", None):
                target_data = load_target_connection(args.target_name)
                context.update(apply_target_field_map(args.target, target_data))
            if args.target_vars_json:
                context.update(load_json_object(args.target_vars_json, "--target-vars-json"))
            try:
                render_replicat_properties(client, args.name, args.target, context)
                log.info("Replicat %s properties rendered from template '%s'", args.name, args.target)
            except Exception as e:
                log.error("Replicat %s was created but properties render failed: %s", args.name, e)
                log.error("Fix the variables and rerun: replicat properties set %s --template %s --target-name <TARGET> --vars-json '...'", args.name, args.target)
                die(str(e))

    elif args.action == "validate":
        if args.target == "oracle" and not args.useridalias:
            die("--useridalias is required when --target=oracle")
        rt = REPLICAT_TYPES[args.replicat_type]
        profile = rt["profile"]
        mode = dict(rt["mode"])
        domain = args.domain or cfg.get("domain", "OracleGoldenGate")
        if not args.trail:
            die("--trail is required")
        trail = args.trail
        description = args.description or cfg.get("description", "Created via automation")
        checkpoint_table = (
            args.checkpoint_table
            or cfg.get("checkpoint_table")
        )

        if args.replicat_type == "coordinated" and args.max_threads:
            mode["maxThreads"] = args.max_threads

        try:
            source = _build_source(trail)
        except TrailError as e:
            die(str(e))

        tables = _load_tables(args)
        prm_lines = build_prm_lines(
            resource="replicat",
            profile=profile,
            context={
                "replicat_name":        args.name,
                "target":               args.target,
                "useridalias":          args.useridalias,
                "domain":               domain,
                "sourcecatalog":        args.pdb,
                "properties_file_path": _properties_file_path(cfg, args.name),
                "map_lines":            [format_replicat_map_line(t) for t in tables],
            },
        )
        payload = create_replicat_payload(
            prm_lines=prm_lines,
            description=description,
            mode=mode,
            source=source,
            checkpoint_table=checkpoint_table,
            critical=args.critical or cfg.get("critical", False),
            encryption_profile=args.encryption_profile or cfg.get("encryption_profile"),
            managed_process_settings=_resolve_mps(args),
        )
        validate_only_create(client, args.name, payload)
        log.info("Replicat %s validation OK (create then delete succeeded)", args.name)

    elif args.action == "list":
        replicats = list_replicats(client)
        if not replicats:
            log.info("No replicats found")
            return
        print_status_table([(r["name"], r["status"]) for r in replicats])

    elif args.action == "start":
        start_replicat(client, args.name)
        log.info("Replicat %s started", args.name)

    elif args.action == "stop":
        stop_replicat(client, args.name)
        log.info("Replicat %s stopped", args.name)

    elif args.action == "status":
        status = get_replicat_status(client, args.name)
        log.info("Replicat %s: %s", args.name, status)

    elif args.action == "params":
        if args.subaction == "get":
            lines = get_replicat_params(client, args.name)
            print("\n".join(lines))
        elif args.subaction == "set":
            lines = load_lines(args)
            set_replicat_params(client, args.name, lines)
            log.info("Replicat %s params replaced (%d lines)", args.name, len(lines))
        elif args.subaction == "append":
            lines = load_lines(args)
            append_replicat_params(client, args.name, lines)
            log.info("Replicat %s params appended (%d new lines)", args.name, len(lines))

    elif args.action == "properties":
        if args.subaction == "get":
            props = get_replicat_properties(client, args.name)
            print_info(props)
        elif args.subaction == "set":
            if args.template:
                context = {
                    "replicat_name":   args.name,
                    "deployment_name": cfg.get("deployment"),
                    "env":             cfg.get("env"),
                    "db_engine":       cfg.get("db_engine", ""),
                }
                if args.target_name:
                    target_data = load_target_connection(args.target_name)
                    context.update(apply_target_field_map(args.template, target_data))
                if args.vars_json:
                    context.update(load_json_object(args.vars_json, "--vars-json"))
                render_replicat_properties(client, args.name, args.template, context)
                log.info("Replicat %s properties set from template '%s'", args.name, args.template)
            else:
                props = load_properties(args)
                set_replicat_properties(client, args.name, props)
                log.info("Replicat %s properties replaced (%d keys)", args.name, len(props))
        elif args.subaction == "merge":
            updates = load_properties(args)
            merge_replicat_properties(client, args.name, updates)
            log.info("Replicat %s properties merged (%d keys updated/added)", args.name, len(updates))

    elif args.action == "info":
        info = get_replicat_info(client, args.name)
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
        history = get_replicat_history(client, args.name)
        if not history:
            log.info("No history found for replicat %s", args.name)
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
        lines = get_replicat_report(client, args.name, report_name)
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
            log.info("Command '%s' sent to replicat %s (no reply)", args.command, args.name)

    elif args.action == "update":
        update_replicat(
            client, args.name,
            description=args.description,
            critical=args.critical,
            encryption_profile=args.encryption_profile,
            managed_process_settings=_resolve_mps(args),
        )
        log.info("Replicat %s updated", args.name)

    elif args.action == "delete":
        delete_replicat(client, args.name)
        log.info("Replicat %s deleted", args.name)
