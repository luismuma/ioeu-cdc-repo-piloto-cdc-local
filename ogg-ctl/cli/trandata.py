import json
import logging

from api.trandata_ops import (
    table_add_idempotent,
    table_delete_idempotent,
    table_modify,
    table_info,
    table_check_support,
    schema_add_idempotent,
    schema_delete_idempotent,
    schema_modify,
    schema_info,
    schema_check_support,
)
from api.errors import UserInputError
from cli.output import print_info, print_status_table

log = logging.getLogger(__name__)


# ── Input parsing helpers ─────────────────────────────────────────────────────

def _parse_tables_json(raw, cli_opts):
    """Parse --tables-json value into a list of per-table opts dicts.

    Accepts either:
      ["SCHEMA.TABLE1", "SCHEMA.TABLE2"]              — table names only; cli_opts applied to all
      [{"tableName": "SCHEMA.TABLE", "allColumns": true}, ...]  — per-table config (API camelCase)

    CLI flags are used as defaults; per-entry values override them.
    """
    if raw.startswith("@"):
        try:
            raw = open(raw[1:]).read()  # noqa: WPS515
        except OSError as e:
            raise UserInputError(f"Cannot read tables JSON file: {e}")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise UserInputError(f"Invalid --tables-json: {e}")
    if not isinstance(data, list):
        raise UserInputError("--tables-json must be a JSON array")

    result = []
    for i, item in enumerate(data):
        if isinstance(item, str):
            result.append({"tableName": item, **cli_opts})
        elif isinstance(item, dict):
            if "tableName" not in item:
                raise UserInputError(f"Entry {i} in --tables-json is missing 'tableName'")
            tc = {"tableName": item["tableName"]}
            # Merge cli_opts as defaults; per-entry API-native camelCase fields take precedence
            tc["all_columns"] = item.get("allColumns", cli_opts.get("all_columns", False))
            sc = item.get("schedulingColumns")
            if sc is not None:
                tc["scheduling_columns"] = sc
            elif "scheduling_columns" in cli_opts:
                tc["scheduling_columns"] = cli_opts["scheduling_columns"]
            cols = item.get("columns")
            if cols:
                tc["columns"] = cols
            elif cli_opts.get("columns"):
                tc["columns"] = cli_opts["columns"]
            csn = item.get("prepareCsnMode")
            if csn:
                tc["prepare_csn_mode"] = csn
            elif cli_opts.get("prepare_csn_mode"):
                tc["prepare_csn_mode"] = cli_opts["prepare_csn_mode"]
            result.append(tc)
        else:
            raise UserInputError(
                f"Entry {i} in --tables-json must be a string or an object with 'tableName'"
            )
    return result


def _resolve_connection(connection, domain="OracleGoldenGate"):
    """Return the fully-qualified OGG connection name.

    If --connection already contains a dot (e.g. 'OracleGoldenGate.ACNPDB_PRIM_DEV')
    it is used as-is; otherwise the domain prefix is prepended so that the API path
    matches how OGG stores connections by default.
    """
    if "." in connection:
        return connection
    full = f"{domain}.{connection}"
    log.debug("Connection '%s' has no domain — resolved to '%s'", connection, full)
    return full


def _opts_from_args(args):
    """Build a trandata opts dict from the parsed CLI arguments."""
    opts = {}
    if getattr(args, "all_columns", False):
        opts["all_columns"] = True
    if getattr(args, "no_scheduling_columns", False):
        opts["scheduling_columns"] = False
    cols = getattr(args, "columns", None)
    if cols:
        opts["columns"] = [c.strip() for c in cols.split(",")]
    csn = getattr(args, "prepare_csn_mode", None)
    if csn:
        opts["prepare_csn_mode"] = csn
    return opts


def _resolve_targets(args, cli_opts):
    """Return (table_configs, schema) from args.

    table_configs: list of dicts with 'tableName' + per-table opts
    schema:        schema name string, or None
    Exactly one of the two will be non-empty.
    """
    schema = getattr(args, "schema", None) or None
    tables = getattr(args, "table", None) or []
    tables_json_raw = getattr(args, "tables_json", None)

    has_tables = bool(tables or tables_json_raw)
    has_schema = bool(schema)

    if not has_tables and not has_schema:
        raise UserInputError(
            "Provide at least one of --table, --tables-json, or --schema"
        )
    if has_tables and has_schema:
        raise UserInputError("--table / --tables-json and --schema are mutually exclusive")
    if tables and tables_json_raw:
        raise UserInputError("--table and --tables-json are mutually exclusive")

    table_configs = []
    if tables_json_raw:
        table_configs = _parse_tables_json(tables_json_raw, cli_opts)
    elif tables:
        table_configs = [{"tableName": t, **cli_opts} for t in tables]

    return table_configs, schema


# ── Output helpers ────────────────────────────────────────────────────────────

def _print_table_info(tables):
    for t in tables:
        print_info({
            "table":             t.get("tableName", ""),
            "enabled":           t.get("enabled", False),
            "primaryKey":        t.get("primaryKey", ""),
            "schedulingColumns": t.get("schedulingColumns", ""),
            "allColumns":        t.get("allColumns", False),
            "columns":           ", ".join(t.get("columns") or []) or "(none specified)",
        })


def _print_schema_info(schemas):
    for s in schemas:
        print_info({
            "schema":            s.get("schemaName", ""),
            "enabled":           s.get("enabled", False),
            "schedulingColumns": s.get("schedulingColumns", ""),
            "allColumns":        s.get("allColumns", False),
            "numPreparedTables": s.get("numPreparedTables", 0),
        })


def _print_support(tables):
    for t in tables:
        schema_name = t.get("schemaName", "")
        table_name = t.get("tableName", "")
        qualified = f"{schema_name}.{table_name}" if schema_name else table_name
        details = ", ".join(t.get("details") or []) or "tableFullSupport"
        print_info({
            "table":              qualified,
            "details":            details,
            "logicalReplication": t.get("logicalReplication", ""),
        })


# ── Main handler ──────────────────────────────────────────────────────────────

def handle_trandata(args, client, cfg):
    action = args.action
    connection = _resolve_connection(
        args.connection, getattr(args, "domain", "OracleGoldenGate")
    )
    cli_opts = _opts_from_args(args)
    table_configs, schema = _resolve_targets(args, cli_opts)

    if action == "add":
        if schema:
            changed = schema_add_idempotent(client, connection, schema, cli_opts)
            log.info(
                "Schema '%s' trandata: %s", schema,
                "enabled" if changed else "already enabled — nothing to do",
            )
        else:
            results = []
            for tc in table_configs:
                tc = dict(tc)
                name = tc.pop("tableName")
                changed = table_add_idempotent(client, connection, name, tc)
                results.append((name, "enabled" if changed else "already enabled"))
            print_status_table(results)

    elif action == "delete":
        if schema:
            changed = schema_delete_idempotent(client, connection, schema, cli_opts)
            log.info(
                "Schema '%s' trandata: %s", schema,
                "deleted" if changed else "not enabled — nothing to do",
            )
        else:
            results = []
            for tc in table_configs:
                tc = dict(tc)
                name = tc.pop("tableName")
                changed = table_delete_idempotent(client, connection, name, tc)
                results.append((name, "deleted" if changed else "not enabled"))
            print_status_table(results)

    elif action == "modify":
        if schema:
            schema_modify(client, connection, schema, cli_opts)
            log.info("Schema '%s' trandata updated on connection '%s'", schema, connection)
        else:
            results = []
            for tc in table_configs:
                tc = dict(tc)
                name = tc.pop("tableName")
                table_modify(client, connection, name, tc)
                results.append((name, "updated"))
            print_status_table(results)

    elif action == "info":
        if schema:
            schemas = schema_info(client, connection, schema)
            if not schemas:
                log.info("No trandata info found for schema '%s' on '%s'", schema, connection)
            _print_schema_info(schemas)
        else:
            for tc in table_configs:
                tables = table_info(client, connection, tc["tableName"])
                if not tables:
                    log.info(
                        "No trandata info found for '%s' on '%s'",
                        tc["tableName"], connection,
                    )
                _print_table_info(tables)

    elif action == "check-support":
        if schema:
            tables = schema_check_support(client, connection, schema)
            _print_support(tables)
        else:
            for tc in table_configs:
                tables = table_check_support(client, connection, tc["tableName"])
                _print_support(tables)
