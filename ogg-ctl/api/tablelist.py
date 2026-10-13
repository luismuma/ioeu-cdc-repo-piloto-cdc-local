import json
import re

_NAME_RE = re.compile(r"^([A-Z_][A-Z0-9_]*)\.([A-Z_][A-Z0-9_]*)$")


def _parse_name(name: str):
    n = name.strip().upper()
    m = _NAME_RE.match(n)
    if not m:
        raise ValueError(f"Invalid table name (expected SCHEMA.TABLE): {name}")
    return m.group(1), m.group(2)


def read_tables_file(path: str):
    """Plain SCHEMA.TABLE per line. Returns [{schema, table}, ...]."""
    tables = []
    seen = set()
    with open(path, "r") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            schema, table = _parse_name(line)
            key = (schema, table)
            if key in seen:
                continue
            seen.add(key)
            tables.append({"schema": schema, "table": table})

    if not tables:
        raise ValueError("Tables file is empty after removing comments/blank lines")
    return tables


_EXTRACT_FIELDS  = {"name", "columns", "columns_except", "sqlpredicate", "filter", "keycols", "comment"}
_REPLICAT_FIELDS = {"name", "columns", "columns_except", "filter", "keycols", "comment", "threadrange"}


def parse_tables_json(payload: str, for_resource: str = "extract"):
    """Parse a JSON payload (string) describing tables.  Required field per entry: name.
    Optional fields depend on `for_resource`:
      - extract:  columns | columns_except (mutually exclusive), sqlpredicate, filter, keycols
      - replicat: columns_except, filter, keycols
    Returns [{schema, table, columns?, columns_except?, sqlpredicate?, filter?, keycols?}, ...]."""
    allowed = _REPLICAT_FIELDS if for_resource == "replicat" else _EXTRACT_FIELDS

    try:
        data = json.loads(payload)
    except json.JSONDecodeError as e:
        import ast
        try:
            data = ast.literal_eval(payload)
        except (ValueError, SyntaxError):
            hint = ""
            if payload.startswith("'") or payload.endswith("'") or '"' not in payload:
                hint = (
                    "\n  Hint: PowerShell is mangling the double quotes in your JSON string."
                    "\n  Use ConvertTo-Json instead:"
                    "\n    --tables-json (ConvertTo-Json @(@{name='SCHEMA.TABLE'}) -Compress)"
                    "\n  Or use escaped double quotes:"
                    "\n    --tables-json '[{\\\"name\\\": \\\"SCHEMA.TABLE\\\"}]'"
                )
            raise ValueError(f"Invalid JSON payload for --tables-json: {e}{hint}")
    if not isinstance(data, list):
        raise ValueError("Tables JSON must be a top-level array of objects")

    tables = []
    seen = set()
    for i, entry in enumerate(data):
        if not isinstance(entry, dict):
            raise ValueError(f"Tables JSON entry #{i} is not an object")
        if "name" not in entry:
            raise ValueError(f"Tables JSON entry #{i} is missing required field 'name'")
        bad = set(entry) - allowed
        if bad:
            raise ValueError(
                f"Tables JSON entry '{entry['name']}': field(s) {sorted(bad)} not supported for {for_resource}. "
                f"Allowed: {sorted(allowed)}"
            )

        # Normalise list fields: accept both a comma-separated string and a proper JSON array
        for list_field in ("columns", "columns_except", "keycols"):
            if list_field in entry and isinstance(entry[list_field], str):
                entry[list_field] = [c.strip() for c in entry[list_field].split(",") if c.strip()]

        schema, table = _parse_name(entry["name"])
        key = (schema, table)
        if key in seen:
            continue
        seen.add(key)

        cols = entry.get("columns") or None
        cols_except = entry.get("columns_except") or None
        if cols and cols_except:
            raise ValueError(
                f"Tables JSON entry '{entry['name']}': 'columns' and 'columns_except' are mutually exclusive"
            )

        t = {"schema": schema, "table": table}
        if cols:
            t["columns"] = [str(c).strip().upper() for c in cols]
        if cols_except:
            t["columns_except"] = [str(c).strip().upper() for c in cols_except]
        if entry.get("sqlpredicate"):
            t["sqlpredicate"] = entry["sqlpredicate"]
        if entry.get("filter"):
            t["filter"] = entry["filter"]
        if entry.get("keycols"):
            t["keycols"] = [str(c).strip().upper() for c in entry["keycols"]]
        if entry.get("comment"):
            # Free-text comment appended to the TABLE/MAP line as a SQL '-- ...' annotation.
            # Newlines are stripped so the result is always a single line.
            t["comment"] = " ".join(str(entry["comment"]).split())
        if entry.get("threadrange"):
            # OGG coordinated replicat per-MAP THREADRANGE clause, e.g. 'threadrange(1-4)'.
            # Accept either an explicit range string ('1-4') or just an int.
            t["threadrange"] = str(entry["threadrange"]).strip()
        tables.append(t)

    if not tables:
        raise ValueError("Tables JSON is empty")
    return tables


def format_extract_table_line(t: dict) -> str:
    """Build a single 'TABLE SCHEMA.TABLE, <clauses>;' line for an Extract .prm file.
    If t has a 'comment' field, it's appended after the ';' as a SQL '-- ...' annotation."""
    clauses = []
    if t.get("columns"):
        clauses.append(f"COLS ({', '.join(t['columns'])})")
    if t.get("columns_except"):
        clauses.append(f"COLSEXCEPT ({', '.join(t['columns_except'])})")
    if t.get("sqlpredicate"):
        clauses.append(f'SQLPREDICATE "{t["sqlpredicate"]}"')
    if t.get("filter"):
        clauses.append(f"FILTER ({t['filter']})")
    if t.get("keycols"):
        clauses.append(f"KEYCOLS ({', '.join(t['keycols'])})")

    base = f"TABLE {t['schema']}.{t['table']}"
    line = f"{base}, {', '.join(clauses)};" if clauses else f"{base};"
    if t.get("comment"):
        line += f"  -- {t['comment']}"
    return line


def format_replicat_map_line(t: dict) -> str:
    """Build a single 'MAP src.tab, TARGET tgt.tab, <clauses>;' line for a Replicat .prm file.
    Target schema/table mirror the source (1-to-1 mapping, matching existing convention).
    If t has a 'comment' field, it's appended after the ';' as a SQL '-- ...' annotation.

    Column restriction:
      - 'columns'       : explicit allow-list → emits COLMAP(COL=COL, ...) with 1-to-1
                          source=target mapping. Only the listed columns flow to the target.
      - 'columns_except': blocklist → emits COLEXCEPT (...).
    The two are mutually exclusive (already enforced in parse_tables_json)."""
    clauses = []
    if t.get("columns"):
        # 1-to-1 src=tgt column mapping. Compact format (no whitespace inside the
        # parens, no space after commas) — matches the team's PRM convention; OGG
        # accepts either form.
        cols = t["columns"]
        clauses.append(f"COLMAP({','.join(f'{c}={c}' for c in cols)})")
    if t.get("columns_except"):
        clauses.append(f"COLEXCEPT ({', '.join(t['columns_except'])})")
    if t.get("filter"):
        clauses.append(f"FILTER ({t['filter']})")
    if t.get("keycols"):
        clauses.append(f"KEYCOLS ({', '.join(t['keycols'])})")
    if t.get("threadrange"):
        clauses.append(f"threadrange({t['threadrange']})")

    base = f"map {t['schema']}.{t['table']}, target {t['schema']}.{t['table']}"
    line = f"{base}, {', '.join(clauses)};" if clauses else f"{base};"
    if t.get("comment"):
        line += f"  -- {t['comment']}"
    return line
