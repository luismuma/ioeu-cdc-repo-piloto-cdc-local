import argparse


class _WideHelpFormatter(argparse.HelpFormatter):
    def __init__(self, prog):
        super().__init__(prog, max_help_position=40, width=100)

    def _format_usage(self, usage, actions, groups, prefix):
        # Put positional arguments before optional arguments in the usage line
        # so "EXTRACT_NAME" appears right after the command, not at the end.
        if not hasattr(self, "_format_actions_usage"):
            return super()._format_usage(usage, actions, groups, prefix)
        if usage is None:
            optionals = [a for a in actions if a.option_strings]
            positionals = [a for a in actions if not a.option_strings]
            pos_str = self._format_actions_usage(positionals, groups)
            opt_str = self._format_actions_usage(optionals, groups)
            parts = [s for s in [pos_str, opt_str] if s]
            prog = '%(prog)s' % dict(prog=self._prog)
            usage = ' '.join([prog] + parts) if parts else prog
        return super()._format_usage(usage, actions, groups, prefix)

    def _format_action(self, action):
        # argparse caps help_position at (_action_max_length + 2), ignoring
        # the current indent level. For nested items like subparser choices
        # (indent=4) this leaves too little room for long names like
        # "credential-alias". Temporarily inflate _action_max_length by the
        # current indent so the cap is computed correctly.
        saved = self._action_max_length
        self._action_max_length = saved + self._current_indent
        result = super()._format_action(action)
        self._action_max_length = saved
        return result


def build_parser():
    # Common shared flags that don't touch OGG (logging, output format).
    common_parent = argparse.ArgumentParser(add_help=False)
    common_parent.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging to stderr")
    common_parent.add_argument("--format", choices=["auto", "table", "json"], default="auto",
                               help="Output format: auto (JSON unless TTY), table, or json (default: auto)")

    # For every command that calls the OGG API.
    config_parent = argparse.ArgumentParser(add_help=False, parents=[common_parent])
    config_parent.add_argument("--deployment", required=True, metavar="DEPLOYMENT",
                               help="Deployment key (e.g. oracle_pre) or path to a deployment YAML file")

    p = argparse.ArgumentParser(
        description="Oracle GoldenGate Microservices automation CLI",
        formatter_class=_WideHelpFormatter,
    )

    resource_sub = p.add_subparsers(dest="resource", required=True, metavar="RESOURCE")

    _add_extract_parsers(resource_sub, config_parent)
    _add_replicat_parsers(resource_sub, config_parent)
    _add_distribution_path_parsers(resource_sub, config_parent)
    _add_receiver_path_parsers(resource_sub, config_parent)
    _add_config_file_parsers(resource_sub, config_parent)
    _add_credential_alias_parsers(resource_sub, config_parent)
    _add_connection_parsers(resource_sub, config_parent)
    _add_managed_process_profile_parsers(resource_sub, config_parent)
    _add_task_parsers(resource_sub, config_parent)
    _add_user_parsers(resource_sub, config_parent)
    _add_deployment_parsers(resource_sub, config_parent)
    _add_trandata_parsers(resource_sub, config_parent)
    # Templates is local-only (no API calls), so it uses the lighter parent without --deployment
    _add_templates_parsers(resource_sub, common_parent)

    return p


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sub(action_sub, name, **kwargs):
    """add_parser wrapper that always applies _WideHelpFormatter."""
    return action_sub.add_parser(name, formatter_class=_WideHelpFormatter, **kwargs)


def _rename_optionals(parser):
    """Rename the default 'optional arguments' group to 'options' so it
    doesn't clash with the explicit 'optional arguments' group we add."""
    parser._optionals.title = "options"


def _with_req(parser):
    """Rename 'optional arguments' to 'options' and move --deployment into a
    new 'required arguments' group.  Use this for every subparser so that
    --deployment always appears under the right section."""
    _rename_optionals(parser)
    req = parser.add_argument_group("required arguments")
    for action in list(parser._optionals._group_actions):
        if "--deployment" in getattr(action, "option_strings", []):
            parser._optionals._group_actions.remove(action)
            req._group_actions.append(action)
            break
    return req


def _req_opt(parser):
    """Return (required_group, optional_group) for a subparser with a mix of
    required and optional flags."""
    req = _with_req(parser)
    opt = parser.add_argument_group("optional arguments")
    return req, opt


def _add_managed_process_args(group):
    """Add the full set of inline managed-process flags to an argument group.
    Also adds --managed-process-profile as a named-profile alternative."""
    group.add_argument("--managed-process-profile", metavar="PROFILE",
                       help="Reference a named managed-process profile (mutually exclusive with inline flags below)")
    group.add_argument("--auto-start", action=argparse.BooleanOptionalAction, default=None,
                       help="Auto-start process when Administration Server starts")
    group.add_argument("--auto-start-delay", type=int, metavar="SEC",
                       help="Seconds to wait before auto-starting (default: 0)")
    group.add_argument("--auto-restart", action=argparse.BooleanOptionalAction, default=None,
                       help="Auto-restart process if it abends")
    group.add_argument("--auto-restart-retries", type=int, metavar="N",
                       help="Max auto-restart attempts (default: 9)")
    group.add_argument("--auto-restart-delay", type=int, metavar="SEC",
                       help="Seconds between auto-restart attempts (default: 0)")
    group.add_argument("--on-success", action=argparse.BooleanOptionalAction, default=None,
                       help="Restart even on clean process exit (--on-success / --no-on-success)")
    group.add_argument("--window", type=int, metavar="SEC",
                       help="Time window in seconds for counting restart retries (default: 0)")
    group.add_argument("--disable-on-failure", action=argparse.BooleanOptionalAction, default=None,
                       help="Disable process when all retries are exhausted (--disable-on-failure / --no-disable-on-failure)")


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def _add_extract_parsers(resource_sub, config_parent):
    extract_p = resource_sub.add_parser("extract", help="Manage Extract processes")
    action_sub = extract_p.add_subparsers(dest="action", required=True, metavar="ACTION")

    # extract create
    c = _sub(action_sub,"create", parents=[config_parent], help="Create extract and attach trail")
    c.add_argument("name", metavar="EXTRACT_NAME", help="Extract name, e.g. ETAPI1")
    req, opt = _req_opt(c)
    req.add_argument("--extract-type", required=True, choices=["integrated", "initial_load", "mongo_classic"],
                     help="Extract type: integrated (LogMiner), initial_load (direct read), mongo_classic (MongoDB change streams)")
    req.add_argument("--trail", required=True, help="Trail name or path, e.g. et or north/et")
    req.add_argument("--useridalias", required=True, metavar="ALIAS", help="Credential alias")
    tables_src = req.add_mutually_exclusive_group(required=True)
    tables_src.add_argument("--tables-file", metavar="FILE",
                            help="Plain-text file with SCHEMA.TABLE per line")
    tables_src.add_argument("--tables-json", metavar="JSON",
                            help="JSON payload (string): array of objects with name + optional columns / columns_except / sqlpredicate / filter / keycols")
    opt.add_argument("--pdb", metavar="PDB_NAME", help="PDB name for SOURCECATALOG")
    opt.add_argument("--domain", help="Credential domain (default: OracleGoldenGate)")
    opt.add_argument("--description", help="Process description (overrides config default)")
    opt.add_argument("--critical", action="store_true", help="Set critical=true")
    opt.add_argument("--encryption-profile", metavar="PROFILE", help="Encryption profile name")
    _add_managed_process_args(opt)

    # extract register
    r = _sub(action_sub,"register", parents=[config_parent], help="Register existing extract with database")
    r.add_argument("name", metavar="EXTRACT_NAME", help="Extract name, e.g. ETAPI1")
    _with_req(r)
    opt = r.add_argument_group("optional arguments")
    opt.add_argument("--optimized", action="store_true", help="Registration optimized=true")
    opt.add_argument("--replace", action="store_true", help="Registration replace=true")

    # extract validate
    v = _sub(action_sub,"validate", parents=[config_parent], help="Validate extract config (create then delete)")
    v.add_argument("name", metavar="EXTRACT_NAME", help="Extract name, e.g. ETAPI1")
    req, opt = _req_opt(v)
    req.add_argument("--extract-type", required=True, choices=["integrated", "initial_load", "mongo_classic"],
                     help="Extract type: integrated (LogMiner), initial_load (direct read), mongo_classic (MongoDB change streams)")
    req.add_argument("--useridalias", required=True, metavar="ALIAS", help="Credential alias")
    tables_src = req.add_mutually_exclusive_group(required=True)
    tables_src.add_argument("--tables-file", metavar="FILE",
                            help="Plain-text file with SCHEMA.TABLE per line")
    tables_src.add_argument("--tables-json", metavar="JSON",
                            help="JSON payload (string): array of objects with name + optional columns / columns_except / sqlpredicate / filter / keycols")
    opt.add_argument("--trail", help="Trail name or path (falls back to config default)")
    opt.add_argument("--pdb", metavar="PDB_NAME", help="PDB name for SOURCECATALOG")
    opt.add_argument("--domain", help="Credential domain (default: OracleGoldenGate)")
    opt.add_argument("--description", help="Process description (overrides config default)")
    opt.add_argument("--critical", action="store_true", help="Set critical=true")
    opt.add_argument("--encryption-profile", metavar="PROFILE", help="Encryption profile name")
    _add_managed_process_args(opt)

    # extract list
    _with_req(_sub(action_sub,"list", parents=[config_parent], help="List all extract processes"))

    # extract start / stop / status / info / history / delete
    for action, helptext in [
        ("start",   "Start an existing extract"),
        ("stop",    "Stop a running extract"),
        ("status",  "Get extract status"),
        ("info",    "Get detailed extract status (lag, position, PID, last started)"),
        ("history", "Show run history (start/finish timestamps and exit codes)"),
        ("delete",  "Delete an extract (must be stopped, abended, or killed first)"),
    ]:
        sub = _sub(action_sub,action, parents=[config_parent], help=helptext)
        sub.add_argument("name", metavar="EXTRACT_NAME", help="Extract name, e.g. ETAPI1")
        _with_req(sub)

    # extract update
    u = _sub(action_sub,"update", parents=[config_parent], help="Update extract properties (description, critical, encryption-profile, managed-process)")
    u.add_argument("name", metavar="EXTRACT_NAME", help="Extract name, e.g. ETAPI1")
    _with_req(u)
    opt = u.add_argument_group("optional arguments")
    opt.add_argument("--description", help="New process description")
    opt.add_argument("--critical", action=argparse.BooleanOptionalAction, default=None,
                     help="Set or clear critical flag (--critical / --no-critical)")
    opt.add_argument("--encryption-profile", metavar="PROFILE", help="New encryption profile name")
    _add_managed_process_args(opt)

    # extract params {get|set|append}
    p_params = _sub(action_sub, "params", help="Read or modify the extract parameter file (.prm)")
    params_sub = p_params.add_subparsers(dest="subaction", required=True, metavar="SUBACTION")

    g = _sub(params_sub, "get", parents=[config_parent], help="Print the parameter file")
    g.add_argument("name", metavar="EXTRACT_NAME", help="Extract name, e.g. ETAPI1")
    _with_req(g)

    for sub, helptext in [
        ("set",    "Replace the parameter file with new content"),
        ("append", "Append new lines to the parameter file"),
    ]:
        sp = _sub(params_sub, sub, parents=[config_parent], help=helptext)
        sp.add_argument("name", metavar="EXTRACT_NAME", help="Extract name, e.g. ETAPI1")
        req = _with_req(sp)
        src = req.add_mutually_exclusive_group(required=True)
        src.add_argument("--from-file", metavar="FILE",
                         help="Path to a local file containing PRM lines (one per line)")
        src.add_argument("--lines-json", metavar="JSON",
                         help="JSON payload (string): array of PRM lines, e.g. '[\"line1\",\"line2\"]'")

    # extract report
    rp = _sub(action_sub,"report", parents=[config_parent], help="Print extract report file")
    rp.add_argument("name", metavar="EXTRACT_NAME", help="Extract name, e.g. ETAPI1")
    _with_req(rp)
    opt = rp.add_argument_group("optional arguments")
    opt.add_argument("--report", metavar="FILENAME", help="Report filename (default: NAME.rpt)")
    rp_lines = opt.add_mutually_exclusive_group()
    rp_lines.add_argument("--head", type=int, metavar="N", help="Print first N lines")
    rp_lines.add_argument("--tail", type=int, metavar="N", help="Print last N lines")

    # extract stats
    st = _sub(action_sub,"stats", parents=[config_parent], help="Show extract statistics (STATS command)")
    st.add_argument("name", metavar="EXTRACT_NAME", help="Extract name, e.g. ETAPI1")
    _with_req(st)
    opt = st.add_argument_group("optional arguments")
    opt.add_argument("--period", choices=["hourly", "daily", "total"], metavar="PERIOD",
                     help="Stats time period: hourly (text), daily (since midnight), or total (since start)")
    opt.add_argument("--table", metavar="SCHEMA.TABLE", help="Limit stats to a specific table (hourly only)")

    # extract cmd
    cm = _sub(action_sub,"cmd", parents=[config_parent],
                                help="Send any GGSCI command to the extract process")
    cm.add_argument("name", metavar="EXTRACT_NAME", help="Extract name, e.g. ETAPI1")
    cm.add_argument("command", metavar="COMMAND", help=(
        "Command to send. No-arg commands: STATUS STOP FORCESTOP RESUME GETLAG "
        "GETTCPSTATS RESETTCPSTATS LOGEND LOGSTATS ROLLOVER. "
        "Commands with --arguments: STATS SHOWTRANS SHOWTABLECACHE SHOWPARTITIONCACHE "
        "TRACE TRACE2 SKIPTRANS FORCETRANS CACHEMGR REPORT PROBE BR RTC DEPINFO "
        "GETPARAMINFO HANDLECOLLISIONS NOHANDLECOLLISIONS TRANLOGOPTIONS TRACEINIT DEPENDENCYINFO."
    ))
    _with_req(cm)
    opt = cm.add_argument_group("optional arguments")
    opt.add_argument("--arguments", metavar="ARGS", help="Command arguments string")
    opt.add_argument("--is-reported", action="store_true",
                     help="Write command output to the extract report file")


# ---------------------------------------------------------------------------
# Replicat
# ---------------------------------------------------------------------------

def _add_replicat_parsers(resource_sub, config_parent):
    replicat_p = resource_sub.add_parser("replicat", help="Manage Replicat processes")
    action_sub = replicat_p.add_subparsers(dest="action", required=True, metavar="ACTION")

    # replicat create
    c = _sub(action_sub,"create", parents=[config_parent], help="Create a replicat process")
    c.add_argument("name", metavar="REPLICAT_NAME", help="Replicat name, e.g. RPAPI1")
    req, opt = _req_opt(c)
    req.add_argument("--replicat-type", required=True, choices=["classic", "coordinated"],
                     help="Replicat type: classic (nonintegrated) or coordinated (parallel apply)")
    req.add_argument("--target", required=True, choices=["oracle", "kafka_confluent", "s3"],
                     help="Target technology. 'oracle' uses USERIDALIAS; "
                          "'kafka_confluent' and 's3' render a Big Data properties file and "
                          "(for coordinated) add a TARGETDB LIBFILE line to the PRM.")
    req.add_argument("--trail", required=True, help="Source trail name or path, e.g. ea or ggnorth/ea")
    tables_src = req.add_mutually_exclusive_group(required=True)
    tables_src.add_argument("--tables-file", metavar="FILE",
                            help="Plain-text file with SCHEMA.TABLE per line")
    tables_src.add_argument("--tables-json", metavar="JSON",
                            help="JSON payload (string): array of objects with name + optional columns_except / filter / keycols")
    opt.add_argument("--useridalias", metavar="ALIAS",
                     help="Credential alias for the target DB (required when --target=oracle)")
    opt.add_argument("--target-name", metavar="TARGET",
                     help="Target name from target_connections.yaml (e.g. KAFKA_GR_01_DEV); "
                          "auto-fills connection variables for the Big Data properties template")
    opt.add_argument("--target-vars-json", metavar="JSON",
                     help="JSON payload (string): per-replicat template variables (kafka_topic_prefix, database, schema, ...). "
                          "Overrides --target-name values. "
                          "Use `oggctl.py templates show <name>` to see what variables each template needs.")
    opt.add_argument("--checkpoint-table", metavar="SCHEMA.TABLE",
                     help="Checkpoint table, e.g. ggadmin.ggs_checkpoint (default: from config)")
    opt.add_argument("--pdb", metavar="PDB_NAME", help="Source PDB name for SOURCECATALOG")
    opt.add_argument("--domain", help="Credential domain (default: OracleGoldenGate)")
    opt.add_argument("--description", help="Process description (overrides config default)")
    opt.add_argument("--critical", action="store_true", help="Set critical=true")
    opt.add_argument("--encryption-profile", metavar="PROFILE", help="Encryption profile name")
    opt.add_argument("--max-threads", type=int, default=None,
                     help="Max apply threads for coordinated replicat (default: 25, ignored for classic)")
    _add_managed_process_args(opt)

    # replicat validate
    v = _sub(action_sub,"validate", parents=[config_parent], help="Validate replicat config (create then delete)")
    v.add_argument("name", metavar="REPLICAT_NAME", help="Replicat name, e.g. RPAPI1")
    req, opt = _req_opt(v)
    req.add_argument("--replicat-type", required=True, choices=["classic", "coordinated"],
                     help="Replicat type: classic (nonintegrated) or coordinated (parallel apply)")
    req.add_argument("--target", required=True, choices=["oracle", "kafka_confluent", "s3"],
                     help="Target technology (same as create — only the PRM is validated; no properties upload)")
    tables_src = req.add_mutually_exclusive_group(required=True)
    tables_src.add_argument("--tables-file", metavar="FILE",
                            help="Plain-text file with SCHEMA.TABLE per line")
    tables_src.add_argument("--tables-json", metavar="JSON",
                            help="JSON payload (string): array of objects with name + optional columns_except / filter / keycols")
    opt.add_argument("--useridalias", metavar="ALIAS",
                     help="Credential alias (required when --target=oracle)")
    opt.add_argument("--trail", help="Source trail name or path (falls back to config default)")
    opt.add_argument("--checkpoint-table", metavar="SCHEMA.TABLE",
                     help="Checkpoint table (default: from config)")
    opt.add_argument("--pdb", metavar="PDB_NAME", help="Source PDB name for SOURCECATALOG")
    opt.add_argument("--domain", help="Credential domain (default: OracleGoldenGate)")
    opt.add_argument("--description", help="Process description (overrides config default)")
    opt.add_argument("--critical", action="store_true", help="Set critical=true")
    opt.add_argument("--encryption-profile", metavar="PROFILE", help="Encryption profile name")
    opt.add_argument("--max-threads", type=int, default=None,
                     help="Max apply threads for coordinated replicat (default: 25, ignored for classic)")
    _add_managed_process_args(opt)

    # replicat list
    _with_req(_sub(action_sub,"list", parents=[config_parent], help="List all replicat processes"))

    # replicat start / stop / status / info / history / delete
    for action, helptext in [
        ("start",   "Start an existing replicat"),
        ("stop",    "Stop a running replicat"),
        ("status",  "Get replicat status"),
        ("info",    "Get detailed replicat status (lag, position, PID, last started)"),
        ("history", "Show run history (start/finish timestamps and exit codes)"),
        ("delete",  "Delete a replicat (must be stopped, abended, or killed first)"),
    ]:
        sub = _sub(action_sub,action, parents=[config_parent], help=helptext)
        sub.add_argument("name", metavar="REPLICAT_NAME", help="Replicat name, e.g. RPAPI1")
        _with_req(sub)

    # replicat update
    u = _sub(action_sub,"update", parents=[config_parent], help="Update replicat properties (description, critical, encryption-profile, managed-process)")
    u.add_argument("name", metavar="REPLICAT_NAME", help="Replicat name, e.g. RPAPI1")
    _with_req(u)
    opt = u.add_argument_group("optional arguments")
    opt.add_argument("--description", help="New process description")
    opt.add_argument("--critical", action=argparse.BooleanOptionalAction, default=None,
                     help="Set or clear critical flag (--critical / --no-critical)")
    opt.add_argument("--encryption-profile", metavar="PROFILE", help="New encryption profile name")
    _add_managed_process_args(opt)

    # replicat params {get|set|append}
    p_params = _sub(action_sub, "params", help="Read or modify the replicat parameter file (.prm)")
    params_sub = p_params.add_subparsers(dest="subaction", required=True, metavar="SUBACTION")

    g = _sub(params_sub, "get", parents=[config_parent], help="Print the parameter file")
    g.add_argument("name", metavar="REPLICAT_NAME", help="Replicat name, e.g. RPAPI1")
    _with_req(g)

    for sub, helptext in [
        ("set",    "Replace the parameter file with new content"),
        ("append", "Append new lines to the parameter file"),
    ]:
        sp = _sub(params_sub, sub, parents=[config_parent], help=helptext)
        sp.add_argument("name", metavar="REPLICAT_NAME", help="Replicat name, e.g. RPAPI1")
        req = _with_req(sp)
        src = req.add_mutually_exclusive_group(required=True)
        src.add_argument("--from-file", metavar="FILE",
                         help="Path to a local file containing PRM lines (one per line)")
        src.add_argument("--lines-json", metavar="JSON",
                         help="JSON payload (string): array of PRM lines, e.g. '[\"line1\",\"line2\"]'")

    # replicat properties {get|set|merge}  — Java .properties file for Big Data replicats
    p_props = _sub(action_sub, "properties",
                   help="Read or modify the replicat .properties file (Big Data replicats: Kafka, S3, ...)")
    props_sub = p_props.add_subparsers(dest="subaction", required=True, metavar="SUBACTION")

    g = _sub(props_sub, "get", parents=[config_parent], help="Print the properties as key=value pairs (or JSON)")
    g.add_argument("name", metavar="REPLICAT_NAME", help="Replicat name, e.g. RPAPI1")
    _with_req(g)

    # replicat properties set — full replace: raw or template-based
    ps = _sub(props_sub, "set", parents=[config_parent],
              help="Replace the entire properties file (raw key/values or rendered from a built-in template)")
    ps.add_argument("name", metavar="REPLICAT_NAME", help="Replicat name, e.g. RPAPI1")
    req = _with_req(ps)
    src = req.add_mutually_exclusive_group(required=True)
    src.add_argument("--from-file", metavar="FILE",
                     help="Path to a Java .properties file (key=value per line, # comments)")
    src.add_argument("--properties-json", metavar="JSON",
                     help="JSON object of key/value pairs, e.g. '{\"gg.handler.kafka.topic\":\"orders\"}'")
    src.add_argument("--template", choices=["s3", "kafka_confluent"],
                     help="Render a built-in template and upload the result")
    opt = ps.add_argument_group("template options (use with --template)")
    opt.add_argument("--target-name", metavar="TARGET",
                     help="Target name from target_connections.yaml (e.g. S3_DA_PRE, KAFKA_GR_DEV); "
                          "auto-fills connection variables for the template")
    opt.add_argument("--vars-json", metavar="JSON",
                     help="JSON object of template variables; overrides --target-name values")

    # replicat properties merge — partial update, raw only
    pm = _sub(props_sub, "merge", parents=[config_parent],
              help="Add or override specific keys, leaving others untouched")
    pm.add_argument("name", metavar="REPLICAT_NAME", help="Replicat name, e.g. RPAPI1")
    req = _with_req(pm)
    src = req.add_mutually_exclusive_group(required=True)
    src.add_argument("--from-file", metavar="FILE",
                     help="Path to a Java .properties file (key=value per line, # comments)")
    src.add_argument("--properties-json", metavar="JSON",
                     help="JSON object of key/value pairs, e.g. '{\"gg.handler.kafka.topic\":\"orders\"}'")

    # replicat report
    rp = _sub(action_sub,"report", parents=[config_parent], help="Print replicat report file")
    rp.add_argument("name", metavar="REPLICAT_NAME", help="Replicat name, e.g. RPAPI1")
    _with_req(rp)
    opt = rp.add_argument_group("optional arguments")
    opt.add_argument("--report", metavar="FILENAME", help="Report filename (default: NAME.rpt)")
    rp_lines = opt.add_mutually_exclusive_group()
    rp_lines.add_argument("--head", type=int, metavar="N", help="Print first N lines")
    rp_lines.add_argument("--tail", type=int, metavar="N", help="Print last N lines")

    # replicat stats
    st = _sub(action_sub,"stats", parents=[config_parent], help="Show replicat statistics (STATS command)")
    st.add_argument("name", metavar="REPLICAT_NAME", help="Replicat name, e.g. RPAPI1")
    _with_req(st)
    opt = st.add_argument_group("optional arguments")
    opt.add_argument("--period", choices=["hourly", "daily", "total"], metavar="PERIOD",
                     help="Stats time period: hourly (text), daily (since midnight), or total (since start)")
    opt.add_argument("--table", metavar="SCHEMA.TABLE", help="Limit stats to a specific table (hourly only)")

    # replicat cmd
    cm = _sub(action_sub,"cmd", parents=[config_parent],
                                help="Send any GGSCI command to the replicat process")
    cm.add_argument("name", metavar="REPLICAT_NAME", help="Replicat name, e.g. RPAPI1")
    cm.add_argument("command", metavar="COMMAND", help=(
        "Command to send. No-arg commands: STATUS STOP FORCESTOP RESUME GETLAG "
        "GETTCPSTATS RESETTCPSTATS LOGEND LOGSTATS ROLLOVER. "
        "Commands with --arguments: STATS SHOWTRANS SHOWTABLECACHE SHOWPARTITIONCACHE "
        "TRACE TRACE2 SKIPTRANS FORCETRANS CACHEMGR REPORT PROBE BR RTC DEPINFO "
        "GETPARAMINFO HANDLECOLLISIONS NOHANDLECOLLISIONS TRANLOGOPTIONS TRACEINIT DEPENDENCYINFO."
    ))
    _with_req(cm)
    opt = cm.add_argument_group("optional arguments")
    opt.add_argument("--arguments", metavar="ARGS", help="Command arguments string")
    opt.add_argument("--is-reported", action="store_true",
                     help="Write command output to the replicat report file")


# ---------------------------------------------------------------------------
# Distribution Path
# ---------------------------------------------------------------------------

def _add_distribution_path_parsers(resource_sub, config_parent):
    dp_p = resource_sub.add_parser("distribution-path", help="Manage Distribution Path processes")
    action_sub = dp_p.add_subparsers(dest="action", required=True, metavar="ACTION")

    # distribution-path create
    c = _sub(action_sub,"create", parents=[config_parent], help="Create a distribution path")
    c.add_argument("name", metavar="PATH_NAME", help="Distribution path name, e.g. dp-north")
    req, opt = _req_opt(c)
    req.add_argument("--source-uri", required=True, metavar="URI",
                     help="Source trail URI, e.g. trail://localhost/services/v2/trail-files/local/et")
    req.add_argument("--target-uri", required=True, metavar="URI",
                     help="Target receiver URI, e.g. wss://receiver-host:9014")
    opt.add_argument("--source-alias", metavar="ALIAS", help="Credential alias for source authentication")
    opt.add_argument("--target-alias", metavar="ALIAS", help="Credential alias for target authentication")
    opt.add_argument("--domain", help="Credential domain for aliases (default: OracleGoldenGate)")
    opt.add_argument("--begin", default="now",
                     help="Start position: 'now' or sequence:offset e.g. 0:0 (default: now)")
    opt.add_argument("--description", help="Distribution path description")
    opt.add_argument("--critical", action="store_true", help="Set critical=true")
    opt.add_argument("--encryption-profile", metavar="PROFILE", help="Encryption profile name")

    # distribution-path list
    _with_req(_sub(action_sub,"list", parents=[config_parent], help="List all distribution paths"))

    # distribution-path start / stop / status / info / delete
    for action, helptext in [
        ("start",  "Start an existing distribution path"),
        ("stop",   "Stop a running distribution path"),
        ("status", "Get distribution path status"),
        ("info",   "Get detailed distribution path status (lag, position, PID, last started)"),
        ("delete", "Delete a distribution path (must be stopped, abended, or killed first)"),
    ]:
        sub = _sub(action_sub,action, parents=[config_parent], help=helptext)
        sub.add_argument("name", metavar="PATH_NAME", help="Distribution path name")
        _with_req(sub)

    # distribution-path stats
    st = _sub(action_sub,"stats", parents=[config_parent],
                                help="Show distribution path statistics")
    st.add_argument("name", metavar="PATH_NAME", help="Distribution path name")
    _with_req(st)

    # distribution-path update
    u = _sub(action_sub,"update", parents=[config_parent], help="Update distribution path properties (description, critical, encryption-profile)")
    u.add_argument("name", metavar="PATH_NAME", help="Distribution path name")
    _with_req(u)
    opt = u.add_argument_group("optional arguments")
    opt.add_argument("--description", help="New path description")
    opt.add_argument("--critical", action=argparse.BooleanOptionalAction, default=None,
                     help="Set or clear critical flag (--critical / --no-critical)")
    opt.add_argument("--encryption-profile", metavar="PROFILE", help="New encryption profile name")


# ---------------------------------------------------------------------------
# Receiver Path
# ---------------------------------------------------------------------------

def _add_receiver_path_parsers(resource_sub, config_parent):
    rp_p = resource_sub.add_parser("receiver-path", help="Manage Receiver Path processes")
    action_sub = rp_p.add_subparsers(dest="action", required=True, metavar="ACTION")

    # receiver-path create
    c = _sub(action_sub, "create", parents=[config_parent], help="Create a receiver path")
    c.add_argument("name", metavar="PATH_NAME", help="Receiver path name")
    req, opt = _req_opt(c)
    req.add_argument("--source-uri", required=True, metavar="URI",
                     help="Source distribution server URI, e.g. wss://dist-host:9013")
    req.add_argument("--target-uri", required=True, metavar="URI",
                     help="Target local trail URI, e.g. trail://localhost/services/v2/trail-files/local/rt")
    opt.add_argument("--source-alias", metavar="ALIAS", help="Credential alias for source authentication")
    opt.add_argument("--target-alias", metavar="ALIAS", help="Credential alias for target authentication")
    opt.add_argument("--domain", help="Credential domain for aliases (default: OracleGoldenGate)")
    opt.add_argument("--begin", default="now",
                     help="Start position: 'now' or sequence:offset e.g. 0:0 (default: now)")
    opt.add_argument("--description", help="Receiver path description")
    opt.add_argument("--critical", action="store_true", help="Set critical=true")
    opt.add_argument("--encryption-profile", metavar="PROFILE", help="Encryption profile name")

    # receiver-path list
    _with_req(_sub(action_sub, "list", parents=[config_parent], help="List all receiver paths"))

    # receiver-path start / stop / status / info / delete
    for action, helptext in [
        ("start",  "Start an existing receiver path"),
        ("stop",   "Stop a running receiver path"),
        ("status", "Get receiver path status"),
        ("info",   "Get detailed receiver path status (lag, position, PID, last started)"),
        ("delete", "Delete a receiver path (must be stopped, abended, or killed first)"),
    ]:
        sub = _sub(action_sub, action, parents=[config_parent], help=helptext)
        sub.add_argument("name", metavar="PATH_NAME", help="Receiver path name")
        _with_req(sub)

    # receiver-path stats
    st = _sub(action_sub, "stats", parents=[config_parent],
              help="Show receiver path statistics")
    st.add_argument("name", metavar="PATH_NAME", help="Receiver path name")
    _with_req(st)

    # receiver-path update
    u = _sub(action_sub, "update", parents=[config_parent],
             help="Update receiver path properties (description, critical, encryption-profile)")
    u.add_argument("name", metavar="PATH_NAME", help="Receiver path name")
    _with_req(u)
    opt = u.add_argument_group("optional arguments")
    opt.add_argument("--description", help="New path description")
    opt.add_argument("--critical", action=argparse.BooleanOptionalAction, default=None,
                     help="Set or clear critical flag (--critical / --no-critical)")
    opt.add_argument("--encryption-profile", metavar="PROFILE", help="New encryption profile name")


# ---------------------------------------------------------------------------
# Config File
# ---------------------------------------------------------------------------

def _add_config_file_parsers(resource_sub, config_parent):
    cfg_p = resource_sub.add_parser("config-file", help="Manage OGG configuration files (.properties, .prm, .conf, .inc)")
    action_sub = cfg_p.add_subparsers(dest="action", required=True, metavar="ACTION")

    # config-file list
    _with_req(_sub(action_sub,"list", parents=[config_parent], help="List all configuration files"))

    # config-file get
    g = _sub(action_sub,"get", parents=[config_parent], help="Print contents of a configuration file")
    g.add_argument("name", metavar="FILE_NAME", help="Config file name, e.g. handler.properties or GLOBALS")
    _with_req(g)

    # config-file create — raw lines or template-based (upserts when --template is used)
    cr = _sub(action_sub, "create", parents=[config_parent],
              help="Upload a configuration file (raw lines or rendered from a built-in template)")
    cr.add_argument("name", metavar="FILE_NAME",
                    help="Config file name, e.g. kafka_producer_DEV_oauth.properties")
    req = _with_req(cr)
    src = req.add_mutually_exclusive_group(required=True)
    src.add_argument("--from-file", metavar="FILE",
                     help="Path to a local file to upload (one line per row)")
    src.add_argument("--lines-json", metavar="JSON",
                     help="JSON payload (string): array of file lines, e.g. '[\"line1\",\"line2\"]'")
    src.add_argument("--template", choices=["kafka_producer", "kafka_client_jaas"],
                     help="Render a built-in template and upload the result (upserts if file exists)")
    opt = cr.add_argument_group("template options (use with --template)")
    opt.add_argument("--target-name", metavar="TARGET",
                     help="Target name from target_connections.yaml (e.g. KAFKA_GR_DEV); "
                          "auto-fills connection variables for the template")
    opt.add_argument("--vars-json", metavar="JSON",
                     help="JSON object of template variables; overrides --target-name values")
    opt.add_argument("--secret-env", metavar="VAR=ENV_VAR", action="append",
                     help="Inject a secret from an environment variable: TEMPLATE_VAR=ENV_VAR_NAME "
                          "(repeatable, value is never logged). "
                          "Example: --secret-env client_secret=OAUTH_CLIENT_SECRET")

    # config-file replace — raw lines only, file must already exist
    rp = _sub(action_sub, "replace", parents=[config_parent],
              help="Replace an existing configuration file (fails if it doesn't exist)")
    rp.add_argument("name", metavar="FILE_NAME",
                    help="Config file name, e.g. handler.properties")
    req = _with_req(rp)
    src = req.add_mutually_exclusive_group(required=True)
    src.add_argument("--from-file", metavar="FILE",
                     help="Path to a local file to upload (one line per row)")
    src.add_argument("--lines-json", metavar="JSON",
                     help="JSON payload (string): array of file lines, e.g. '[\"line1\",\"line2\"]'")

    # config-file delete
    d = _sub(action_sub,"delete", parents=[config_parent], help="Delete a configuration file")
    d.add_argument("name", metavar="FILE_NAME", help="Config file name, e.g. handler.properties")
    _with_req(d)


# ---------------------------------------------------------------------------
# Credential Alias
# ---------------------------------------------------------------------------

def _add_credential_alias_parsers(resource_sub, config_parent):
    alias_p = resource_sub.add_parser("credential-alias", help="Manage OGG credential store aliases")
    alias_sub = alias_p.add_subparsers(dest="action", required=True, metavar="ACTION")

    # credential-alias create
    ac = _sub(alias_sub, "create", parents=[config_parent],
              help="Add a credential alias (userid + password) to the credential store")
    req, opt = _req_opt(ac)
    req.add_argument("--alias", required=True, metavar="ALIAS", help="Credential alias name, e.g. GGNORTH")
    req.add_argument("--userid", required=True, metavar="CONNECT_STRING",
                     help="Database user string, e.g. c##ggadmin@//host:1521/ORCLCDB")
    pw_src = opt.add_mutually_exclusive_group()
    pw_src.add_argument("--password-file", metavar="FILE",
                        help="File containing the password (one line, no trailing newline)")
    pw_src.add_argument("--password-env", metavar="VAR_NAME",
                        help="Environment variable containing the password (best for AAP + Vault)")
    opt.add_argument("--domain", default="OracleGoldenGate",
                     help="Credential domain (default: OracleGoldenGate)")

    # credential-alias list
    al = _sub(alias_sub, "list", parents=[config_parent],
              help="List credential aliases (all domains, or filtered by --domain)")
    _with_req(al)
    opt = al.add_argument_group("optional arguments")
    opt.add_argument("--domain", default=None,
                     help="List aliases in this domain only (default: list all domains)")

    # credential-alias info
    ai = _sub(alias_sub, "info", parents=[config_parent],
              help="Retrieve credential alias info (userid) from the credential store")
    req, opt = _req_opt(ai)
    req.add_argument("--alias", required=True, metavar="ALIAS", help="Credential alias name, e.g. GGNORTH")
    opt.add_argument("--domain", default="OracleGoldenGate",
                     help="Credential domain (default: OracleGoldenGate)")

    # credential-alias validate
    av = _sub(alias_sub, "validate", parents=[config_parent],
              help="Validate credential alias (check database connectivity)")
    req, opt = _req_opt(av)
    req.add_argument("--alias", required=True, metavar="ALIAS", help="Credential alias name, e.g. GGNORTH")
    opt.add_argument("--domain", default="OracleGoldenGate",
                     help="Credential domain (default: OracleGoldenGate)")

    # credential-alias delete
    ad = _sub(alias_sub, "delete", parents=[config_parent],
              help="Delete a credential alias from the credential store")
    req, opt = _req_opt(ad)
    req.add_argument("--alias", required=True, metavar="ALIAS", help="Credential alias name, e.g. GGNORTH")
    opt.add_argument("--domain", default="OracleGoldenGate",
                     help="Credential domain (default: OracleGoldenGate)")


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def _add_connection_parsers(resource_sub, config_parent):
    conn_p = resource_sub.add_parser("connection", help="Manage named OGG connections")
    conn_sub = conn_p.add_subparsers(dest="action", required=True, metavar="ACTION")

    # connection create
    cc = _sub(conn_sub, "create", parents=[config_parent],
              help="Create a named connection backed by an existing credential alias")
    cc.add_argument("name", metavar="CONNECTION_NAME",
                    help="Connection name, e.g. DBNORTH or OracleGoldenGate.GGNORTH")
    req, opt = _req_opt(cc)
    req.add_argument("--alias", required=True, metavar="ALIAS", help="Credential alias to link")
    opt.add_argument("--domain", default="OracleGoldenGate",
                     help="Credential domain (default: OracleGoldenGate)")

    # connection list
    _with_req(_sub(conn_sub, "list", parents=[config_parent],
                   help="List all known database connections"))

    # connection delete
    cdel = _sub(conn_sub, "delete", parents=[config_parent],
                help="Delete a named connection")
    cdel.add_argument("name", metavar="CONNECTION_NAME", help="Connection name, e.g. DBNORTH")
    _with_req(cdel)


# ---------------------------------------------------------------------------
# Managed Process Profile
# ---------------------------------------------------------------------------

def _add_managed_process_profile_parsers(resource_sub, config_parent):
    mpp_p = resource_sub.add_parser(
        "managed-process-profile",
        help="Manage reusable managed-process settings profiles (auto-start / auto-restart)",
    )
    action_sub = mpp_p.add_subparsers(dest="action", required=True, metavar="ACTION")

    # managed-process-profile create
    c = _sub(action_sub, "create", parents=[config_parent],
             help="Create a named managed-process settings profile")
    c.add_argument("name", metavar="PROFILE_NAME", help="Short profile name, e.g. default-restart")
    _with_req(c)
    opt = c.add_argument_group("optional arguments")
    opt.add_argument("--auto-start", action=argparse.BooleanOptionalAction, default=None,
                     help="Auto-start process when Administration Server starts")
    opt.add_argument("--auto-start-delay", type=int, metavar="SEC",
                     help="Seconds to wait before auto-starting (default: 0)")
    opt.add_argument("--auto-restart", action=argparse.BooleanOptionalAction, default=None,
                     help="Auto-restart process if it abends")
    opt.add_argument("--auto-restart-retries", type=int, metavar="N",
                     help="Max auto-restart attempts (default: 9)")
    opt.add_argument("--auto-restart-delay", type=int, metavar="SEC",
                     help="Seconds between auto-restart attempts (default: 0)")
    opt.add_argument("--on-success", action=argparse.BooleanOptionalAction, default=None,
                     help="Restart even on clean process exit (--on-success / --no-on-success)")
    opt.add_argument("--window", type=int, metavar="SEC",
                     help="Time window in seconds for counting restart retries (default: 0)")
    opt.add_argument("--disable-on-failure", action=argparse.BooleanOptionalAction, default=None,
                     help="Disable process when all retries are exhausted")

    # managed-process-profile update
    u = _sub(action_sub, "update", parents=[config_parent],
             help="Replace a managed-process settings profile")
    u.add_argument("name", metavar="PROFILE_NAME", help="Profile name to update")
    _with_req(u)
    opt = u.add_argument_group("optional arguments")
    opt.add_argument("--auto-start", action=argparse.BooleanOptionalAction, default=None,
                     help="Auto-start process when Administration Server starts")
    opt.add_argument("--auto-start-delay", type=int, metavar="SEC",
                     help="Seconds to wait before auto-starting (default: 0)")
    opt.add_argument("--auto-restart", action=argparse.BooleanOptionalAction, default=None,
                     help="Auto-restart process if it abends")
    opt.add_argument("--auto-restart-retries", type=int, metavar="N",
                     help="Max auto-restart attempts (default: 9)")
    opt.add_argument("--auto-restart-delay", type=int, metavar="SEC",
                     help="Seconds between auto-restart attempts (default: 0)")
    opt.add_argument("--on-success", action=argparse.BooleanOptionalAction, default=None,
                     help="Restart even on clean process exit (--on-success / --no-on-success)")
    opt.add_argument("--window", type=int, metavar="SEC",
                     help="Time window in seconds for counting restart retries (default: 0)")
    opt.add_argument("--disable-on-failure", action=argparse.BooleanOptionalAction, default=None,
                     help="Disable process when all retries are exhausted")

    # managed-process-profile list
    _with_req(_sub(action_sub, "list", parents=[config_parent],
                   help="List all managed-process settings profiles"))

    # managed-process-profile get
    g = _sub(action_sub, "get", parents=[config_parent],
             help="Show a managed-process settings profile")
    g.add_argument("name", metavar="PROFILE_NAME", help="Profile name")
    _with_req(g)

    # managed-process-profile delete
    d = _sub(action_sub, "delete", parents=[config_parent],
             help="Delete a managed-process settings profile")
    d.add_argument("name", metavar="PROFILE_NAME", help="Profile name")
    _with_req(d)


# ---------------------------------------------------------------------------
# User Management
# ---------------------------------------------------------------------------

_ROLES = ("Administrator", "Operator", "User", "Security")


def _add_user_parsers(resource_sub, config_parent):
    u_p = resource_sub.add_parser("user", help="Manage OGG service users")
    action_sub = u_p.add_subparsers(dest="action", required=True, metavar="ACTION")

    # user list
    lst = _sub(action_sub, "list", parents=[config_parent], help="List users (all roles or a specific role)")
    _with_req(lst)
    lst.add_argument("--role", choices=_ROLES, metavar="ROLE",
                     help=f"Filter by role: {', '.join(_ROLES)}")

    # user get
    g = _sub(action_sub, "get", parents=[config_parent], help="Get user details")
    g.add_argument("username", metavar="USERNAME", help="Username")
    _with_req(g)
    g.add_argument("--role", required=True, choices=_ROLES, metavar="ROLE",
                   help=f"User role: {', '.join(_ROLES)}")

    # user create
    c = _sub(action_sub, "create", parents=[config_parent], help="Create a new user")
    c.add_argument("username", metavar="USERNAME", help="Username")
    req, opt = _req_opt(c)
    req.add_argument("--role", required=True, choices=_ROLES, metavar="ROLE",
                     help=f"User role: {', '.join(_ROLES)}")
    req.add_argument("--password-file", required=True, metavar="FILE",
                     help="Path to file containing the user password")
    opt.add_argument("--info", metavar="TEXT", help="Optional user description")

    # user update
    u = _sub(action_sub, "update", parents=[config_parent], help="Update user password or info")
    u.add_argument("username", metavar="USERNAME", help="Username")
    _with_req(u)
    u.add_argument("--role", required=True, choices=_ROLES, metavar="ROLE",
                   help=f"User role: {', '.join(_ROLES)}")
    opt = u.add_argument_group("optional arguments")
    opt.add_argument("--password-file", metavar="FILE",
                     help="Path to file containing the new password")
    opt.add_argument("--info", metavar="TEXT", help="New user description")

    # user delete
    d = _sub(action_sub, "delete", parents=[config_parent], help="Delete a user")
    d.add_argument("username", metavar="USERNAME", help="Username")
    _with_req(d)
    d.add_argument("--role", required=True, choices=_ROLES, metavar="ROLE",
                   help=f"User role: {', '.join(_ROLES)}")


# ---------------------------------------------------------------------------
# Deployment (Service Manager)
# ---------------------------------------------------------------------------

def _add_deployment_parsers(resource_sub, config_parent):
    dp = resource_sub.add_parser("deployment",
                                 help="Query deployments via the OGG Service Manager API")
    action_sub = dp.add_subparsers(dest="action", required=True, metavar="ACTION")

    # deployment list
    _with_req(_sub(action_sub, "list", parents=[config_parent],
                   help="List all deployments visible to the Service Manager"))

    # deployment status
    s = _sub(action_sub, "status", parents=[config_parent],
             help="Show the status of a deployment (defaults to the inventory deployment name)")
    _with_req(s)
    s.add_argument("name", metavar="DEPLOYMENT_NAME", nargs="?",
                   help="Deployment name (defaults to the value in the inventory entry)")

    # deployment info
    i = _sub(action_sub, "info", parents=[config_parent],
             help="Show detailed info for a deployment (status, enabled, cluster role)")
    _with_req(i)
    i.add_argument("name", metavar="DEPLOYMENT_NAME", nargs="?",
                   help="Deployment name (defaults to the value in the inventory entry)")

    # deployment stop
    s = _sub(action_sub, "stop", parents=[config_parent],
             help="Gracefully stop a deployment via the Service Manager (stops admin/dist/recv/pm services)")
    _with_req(s)
    s.add_argument("name", metavar="DEPLOYMENT_NAME", nargs="?",
                   help="Deployment name (defaults to the inventory entry name)")

    # deployment check-auth
    _with_req(_sub(action_sub, "check-auth", parents=[config_parent],
                   help="Probe which auth method (basic or Entra ID Bearer) is active for this deployment"))

    # deployment messages
    mg = _sub(action_sub, "messages", parents=[config_parent],
              help="List OGG system messages (errors, warnings, info)")
    _with_req(mg)
    opt = mg.add_argument_group("filter options")
    opt.add_argument("--severity", choices=["ERROR", "WARNING", "INFO"],
                     metavar="SEVERITY", help="Filter by severity: ERROR, WARNING, or INFO")
    opt.add_argument("--since", metavar="ISO_DATETIME",
                     help="Return messages issued after this timestamp (e.g. 2026-09-15T00:00:00Z)")
    opt.add_argument("--limit", type=int, metavar="N",
                     help="Maximum number of messages to return")


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------

_TASK_TIME_UNITS = ("seconds", "minutes", "hours", "days")


def _add_task_parsers(resource_sub, config_parent):
    task_p = resource_sub.add_parser("task", help="Manage OGG scheduled tasks (purge trails, ...)")
    type_sub = task_p.add_subparsers(dest="action", required=True, metavar="TASK_TYPE")

    # ── purge-trail ──────────────────────────────────────────────────────────
    pt_p = _sub(type_sub, "purge-trail", help="Manage purge trail tasks")
    pt_sub = pt_p.add_subparsers(dest="subaction", required=True, metavar="SUBACTION")

    # purge-trail list
    _with_req(_sub(pt_sub, "list", parents=[config_parent], help="List all purge trail tasks"))

    # purge-trail get
    g = _sub(pt_sub, "get", parents=[config_parent], help="Show a purge trail task")
    g.add_argument("name", metavar="TASK_NAME")
    _with_req(g)

    # purge-trail create
    c = _sub(pt_sub, "create", parents=[config_parent], help="Create a purge trail task")
    c.add_argument("name", metavar="TASK_NAME", help="Task name, e.g. purge-primary")
    req, opt = _req_opt(c)
    req.add_argument("--trail", required=True, metavar="TRAIL", action="append",
                     help="Trail to purge: name or path/name (repeatable, e.g. --trail et or --trail north/et)")
    req.add_argument("--keep-value", required=True, type=int, metavar="N",
                     help="Minimum age of trail files to keep (numeric value)")
    req.add_argument("--keep-units", required=True, choices=_TASK_TIME_UNITS, metavar="UNITS",
                     help=f"Time units for --keep-value: {', '.join(_TASK_TIME_UNITS)}")
    req.add_argument("--schedule-value", required=True, type=int, metavar="N",
                     help="How often to run (numeric value)")
    req.add_argument("--schedule-units", required=True, choices=_TASK_TIME_UNITS, metavar="UNITS",
                     help=f"Time units for --schedule-value: {', '.join(_TASK_TIME_UNITS)}")
    opt.add_argument("--enabled", action=argparse.BooleanOptionalAction, default=None,
                     help="Enable the task immediately after creation (default: false)")
    opt.add_argument("--critical", action=argparse.BooleanOptionalAction, default=None,
                     help="Mark task as critical (default: false)")
    opt.add_argument("--use-checkpoints", action=argparse.BooleanOptionalAction, default=None,
                     help="Use checkpoints when purging (default: false)")
    opt.add_argument("--skip-trail-check", action="store_true", default=False,
                     help="Skip the pre-flight trail existence check (use for initial_load trails "
                          "not registered in the OGG trail catalog)")

    # purge-trail update
    u = _sub(pt_sub, "update", parents=[config_parent], help="Update a purge trail task (read-modify-write)")
    u.add_argument("name", metavar="TASK_NAME")
    req, opt = _req_opt(u)
    opt.add_argument("--trail", metavar="TRAIL", action="append",
                     help="Replace trail list: name or path/name (repeatable); omit to keep existing trails")
    opt.add_argument("--keep-value", type=int, metavar="N", default=None)
    opt.add_argument("--keep-units", choices=_TASK_TIME_UNITS, metavar="UNITS", default=None)
    opt.add_argument("--schedule-value", type=int, metavar="N", default=None)
    opt.add_argument("--schedule-units", choices=_TASK_TIME_UNITS, metavar="UNITS", default=None)
    opt.add_argument("--enabled", action=argparse.BooleanOptionalAction, default=None)
    opt.add_argument("--critical", action=argparse.BooleanOptionalAction, default=None)
    opt.add_argument("--use-checkpoints", action=argparse.BooleanOptionalAction, default=None)

    # purge-trail delete
    d = _sub(pt_sub, "delete", parents=[config_parent], help="Delete a purge trail task")
    d.add_argument("name", metavar="TASK_NAME")
    _with_req(d)

    # purge-trail start / stop / enable / disable
    for cmd, helptext in [
        ("start",   "Start the purge trail task"),
        ("stop",    "Stop a running purge trail task"),
        ("enable",  "Enable the purge trail task (runs on schedule)"),
        ("disable", "Disable the purge trail task (keeps config, stops scheduling)"),
    ]:
        sub = _sub(pt_sub, cmd, parents=[config_parent], help=helptext)
        sub.add_argument("name", metavar="TASK_NAME")
        _with_req(sub)


# ---------------------------------------------------------------------------
# Templates (built-in Jinja templates for replicat properties / config files)
# ---------------------------------------------------------------------------

def _add_trandata_parsers(resource_sub, config_parent):
    tp = resource_sub.add_parser(
        "trandata",
        help="Manage Oracle supplemental logging (TRANDATA) via the OGG Administration Service",
    )
    action_sub = tp.add_subparsers(dest="action", required=True, metavar="ACTION")

    # ── Shared target-selection args ──────────────────────────────────────────
    # --table (repeatable), --tables-json, and --schema are mutually exclusive;
    # validation is enforced in the handler rather than in argparse so that the
    # error messages can reference all three together.

    def _add_connection(group):
        group.add_argument(
            "--connection", required=True, metavar="ALIAS",
            help="Credential alias name (e.g. ACNPDB_PRIM_DEV). If no dot is present the "
                 "--domain prefix is prepended automatically (default: OracleGoldenGate.ALIAS)",
        )

    def _add_domain_opt(group):
        group.add_argument(
            "--domain", default="OracleGoldenGate", metavar="DOMAIN",
            help="Credential domain prefix used when --connection has no dot "
                 "(default: OracleGoldenGate)",
        )

    def _add_target_group(parser):
        tgt = parser.add_argument_group(
            "target (provide exactly one: --table, --tables-json, or --schema)"
        )
        tgt.add_argument(
            "--table", action="append", metavar="SCHEMA.TABLE",
            help="Table name with schema prefix; repeat to specify multiple tables "
                 "(wildcards accepted, e.g. SOE.*)",
        )
        tgt.add_argument(
            "--tables-json", metavar="JSON|@FILE",
            help='JSON array of table names or objects — e.g. \'["SOE.ORDERS","SOE.ITEMS"]\' '
                 'or \'[{"tableName":"SOE.ORDERS","allColumns":true}]\'. '
                 "Prefix with @ to read from a file.",
        )
        tgt.add_argument(
            "--schema", metavar="SCHEMA",
            help="Schema name — applies supplemental logging to all tables in the schema",
        )

    def _add_logging_opts(group):
        group.add_argument(
            "--all-columns", action="store_true", default=False,
            help="Log all columns (default: false — only scheduling/PK columns are logged)",
        )
        group.add_argument(
            "--no-scheduling-columns", action="store_true", default=False,
            help="Disable logging of PK/FK/unique-key columns (scheduling columns are on by default)",
        )
        group.add_argument(
            "--columns", metavar="COL1,COL2,...",
            help="Comma-separated list of specific columns to log (table-level only)",
        )
        group.add_argument(
            "--prepare-csn-mode",
            choices=["nowait", "wait", "lock", "none"],
            default=None,
            metavar="MODE",
            help="SCN preparation mode: nowait (default), wait, lock, or none",
        )

    # ── trandata add ──────────────────────────────────────────────────────────
    s = _sub(action_sub, "add", parents=[config_parent],
             help="Enable supplemental logging — idempotent (skips if already enabled)")
    req, opt = _req_opt(s)
    _add_connection(req)
    _add_target_group(s)
    _add_logging_opts(opt)
    _add_domain_opt(opt)

    # ── trandata delete ───────────────────────────────────────────────────────
    s = _sub(action_sub, "delete", parents=[config_parent],
             help="Disable supplemental logging — idempotent (skips if not enabled)")
    req, opt = _req_opt(s)
    _add_connection(req)
    _add_target_group(s)
    # By default both allColumns and schedulingColumns logging are removed.
    # --no-scheduling-columns narrows the scope to only remove allColumns logging.
    opt.add_argument(
        "--no-scheduling-columns", action="store_true", default=False,
        help="Remove only all-column logging, leaving scheduling-column logging in place",
    )
    _add_domain_opt(opt)

    # ── trandata modify ───────────────────────────────────────────────────────
    s = _sub(action_sub, "modify", parents=[config_parent],
             help="Replace supplemental logging settings (delete existing + re-add with new options)")
    req, opt = _req_opt(s)
    _add_connection(req)
    _add_target_group(s)
    _add_logging_opts(opt)
    _add_domain_opt(opt)

    # ── trandata info ─────────────────────────────────────────────────────────
    s = _sub(action_sub, "info", parents=[config_parent],
             help="Show current supplemental logging configuration")
    req, opt = _req_opt(s)
    _add_connection(req)
    _add_target_group(s)
    _add_domain_opt(opt)

    # ── trandata check-support ────────────────────────────────────────────────
    s = _sub(action_sub, "check-support", parents=[config_parent],
             help="Check OGG capture support for a table or schema")
    req, opt = _req_opt(s)
    _add_connection(req)
    _add_target_group(s)
    _add_domain_opt(opt)


def _add_templates_parsers(resource_sub, common_parent):
    """Templates is local-only — no --deployment, no API calls.  Uses common_parent
    (just --verbose / --format) instead of the full config_parent."""
    tp = resource_sub.add_parser("templates",
                                 help="Inspect built-in templates and their variables (no API calls)")
    action_sub = tp.add_subparsers(dest="action", required=True, metavar="ACTION")

    # templates list
    _sub(action_sub, "list", parents=[common_parent],
         help="List all registered templates with their kind")

    # templates show
    s = _sub(action_sub, "show", parents=[common_parent],
             help="Show required / optional / auto-injected variables for a template")
    s.add_argument("name", metavar="TEMPLATE_NAME",
                   help="Template name, e.g. s3, kafka_confluent, kafka_producer")
