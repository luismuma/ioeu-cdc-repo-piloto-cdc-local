import logging

from api.errors import OGGAPIError, format_api_error
from api._common import log_ogg_messages

log = logging.getLogger(__name__)


# ── Internal POST helpers ─────────────────────────────────────────────────────

def _post_table(client, connection, body):
    r = client.post(f"/connections/{connection}/trandata/table", body)
    if r.status_code != 200:
        raise OGGAPIError(
            f"Trandata table '{body.get('operation')}' failed {format_api_error(r)}"
        )
    log_ogg_messages(r)
    return r.json().get("response", {})


def _post_schema(client, connection, body):
    r = client.post(f"/connections/{connection}/trandata/schema", body)
    if r.status_code != 200:
        raise OGGAPIError(
            f"Trandata schema '{body.get('operation')}' failed {format_api_error(r)}"
        )
    log_ogg_messages(r)
    return r.json().get("response", {})


# ── Body builders ─────────────────────────────────────────────────────────────

def _table_add_body(table_name, opts):
    body = {"operation": "add", "tableName": table_name}
    if opts.get("all_columns"):
        body["allColumns"] = True
    if opts.get("scheduling_columns") is not None:
        body["schedulingColumns"] = opts["scheduling_columns"]
    if opts.get("columns"):
        body["columns"] = list(opts["columns"])
    if opts.get("prepare_csn_mode"):
        body["prepareCsnMode"] = opts["prepare_csn_mode"]
    return body


def _table_delete_body(table_name, opts):
    # Always include allColumns=True — the API default is false which would silently
    # leave all-column supplemental logging in place and only remove scheduling columns.
    body = {"operation": "delete", "tableName": table_name, "allColumns": True}
    if opts.get("scheduling_columns") is not None:
        body["schedulingColumns"] = opts["scheduling_columns"]
    return body


def _schema_add_body(schema_name, opts):
    body = {"operation": "add", "schemaName": schema_name}
    if opts.get("all_columns"):
        body["allColumns"] = True
    if opts.get("scheduling_columns") is not None:
        body["schedulingColumns"] = opts["scheduling_columns"]
    if opts.get("prepare_csn_mode"):
        body["prepareCsnMode"] = opts["prepare_csn_mode"]
    return body


def _schema_delete_body(schema_name, opts):
    # Always include allColumns=True — same reason as _table_delete_body.
    body = {"operation": "delete", "schemaName": schema_name, "allColumns": True}
    if opts.get("scheduling_columns") is not None:
        body["schedulingColumns"] = opts["scheduling_columns"]
    return body


# ── Table-level operations ────────────────────────────────────────────────────

def table_info(client, connection, table_name):
    """Return the trandata info list for a table (wildcards accepted)."""
    resp = _post_table(client, connection, {"operation": "info", "tableName": table_name})
    return resp.get("tables", [])


def table_check_support(client, connection, table_name):
    """Return OGG support details for a table."""
    resp = _post_table(client, connection, {"operation": "checkSupport", "tableName": table_name})
    return resp.get("tables", [])


def table_add(client, connection, table_name, opts=None):
    """Add supplemental logging for a table."""
    _post_table(client, connection, _table_add_body(table_name, opts or {}))


def table_delete(client, connection, table_name, opts=None):
    """Remove supplemental logging for a table."""
    _post_table(client, connection, _table_delete_body(table_name, opts or {}))


def table_add_idempotent(client, connection, table_name, opts=None):
    """Add supplemental logging; skip if already enabled. Returns True if changed."""
    existing = table_info(client, connection, table_name)
    if existing and existing[0].get("enabled"):
        log.info(
            "Trandata already enabled for '%s' on connection '%s' — skipping",
            table_name, connection,
        )
        return False
    table_add(client, connection, table_name, opts)
    return True


def table_delete_idempotent(client, connection, table_name, opts=None):
    """Remove supplemental logging; skip if not enabled. Returns True if changed."""
    existing = table_info(client, connection, table_name)
    if not existing or not existing[0].get("enabled"):
        log.info(
            "Trandata not enabled for '%s' on connection '%s' — skipping",
            table_name, connection,
        )
        return False
    table_delete(client, connection, table_name, opts)
    return True


def table_modify(client, connection, table_name, opts=None):
    """Force-replace supplemental logging with new settings (delete if exists, then add)."""
    existing = table_info(client, connection, table_name)
    if existing and existing[0].get("enabled"):
        table_delete(client, connection, table_name)
    table_add(client, connection, table_name, opts)


# ── Schema-level operations ───────────────────────────────────────────────────

def schema_info(client, connection, schema_name):
    """Return the trandata info list for a schema."""
    resp = _post_schema(client, connection, {"operation": "info", "schemaName": schema_name})
    return resp.get("schemas", [])


def schema_check_support(client, connection, schema_name):
    """Return OGG support details for all tables in a schema."""
    resp = _post_schema(client, connection, {"operation": "checkSupport", "schemaName": schema_name})
    return resp.get("tables", [])


def schema_add(client, connection, schema_name, opts=None):
    """Add supplemental logging for all tables in a schema."""
    _post_schema(client, connection, _schema_add_body(schema_name, opts or {}))


def schema_delete(client, connection, schema_name, opts=None):
    """Remove supplemental logging for all tables in a schema."""
    _post_schema(client, connection, _schema_delete_body(schema_name, opts or {}))


def schema_add_idempotent(client, connection, schema_name, opts=None):
    """Add schema-level supplemental logging; skip if already enabled. Returns True if changed."""
    existing = schema_info(client, connection, schema_name)
    if existing and existing[0].get("enabled"):
        log.info(
            "Trandata already enabled for schema '%s' on connection '%s' — skipping",
            schema_name, connection,
        )
        return False
    schema_add(client, connection, schema_name, opts)
    return True


def schema_delete_idempotent(client, connection, schema_name, opts=None):
    """Remove schema-level supplemental logging; skip if not enabled. Returns True if changed."""
    existing = schema_info(client, connection, schema_name)
    if not existing or not existing[0].get("enabled"):
        log.info(
            "Trandata not enabled for schema '%s' on connection '%s' — skipping",
            schema_name, connection,
        )
        return False
    schema_delete(client, connection, schema_name, opts)
    return True


def schema_modify(client, connection, schema_name, opts=None):
    """Force-replace schema-level supplemental logging with new settings."""
    existing = schema_info(client, connection, schema_name)
    if existing and existing[0].get("enabled"):
        schema_delete(client, connection, schema_name)
    schema_add(client, connection, schema_name, opts)
