import json
from pathlib import Path

from api.errors import die


def load_lines(args):
    """Return file lines (list of strings) from either --from-file (path) or --lines-json (JSON array)."""
    if args.lines_json:
        try:
            lines = json.loads(args.lines_json)
        except json.JSONDecodeError as e:
            die(f"Invalid JSON payload for --lines-json: {e}")
        if not isinstance(lines, list) or not all(isinstance(s, str) for s in lines):
            die("--lines-json must be a JSON array of strings")
        return lines
    return Path(args.from_file).read_text().splitlines()


def load_json_object(payload, field_name="--vars-json"):
    """Parse a JSON object payload (top-level must be {...}).
    Falls back to Python literal syntax (ast.literal_eval) so PowerShell users
    who lost double quotes during argument passing still succeed.
    Exits with a helpful PowerShell hint on parse failure."""
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
                    "\n  Use ConvertTo-Json:"
                    f"\n    {field_name} (ConvertTo-Json @{{key='value'}} -Compress)"
                    "\n  Or escape double quotes with backslash:"
                    f"\n    {field_name} '{{\\\"key\\\": \\\"value\\\"}}'"
                    "\n  Or use the stop-parsing token:"
                    f"\n    python --% oggctl.py ... {field_name} \"{{\\\"key\\\":\\\"value\\\"}}\""
                )
            die(f"Invalid JSON payload for {field_name}: {e}{hint}")
    if not isinstance(data, dict):
        die(f"{field_name} must be a JSON object")
    return data


def load_properties(args):
    """Return a properties dict from either --from-file (Java .properties) or --properties-json (JSON object)."""
    from api.replicat_ops import parse_properties_lines  # local to avoid cycle at import time
    if args.properties_json:
        try:
            props = json.loads(args.properties_json)
        except json.JSONDecodeError as e:
            die(f"Invalid JSON payload for --properties-json: {e}")
        if not isinstance(props, dict) or not all(isinstance(k, str) for k in props):
            die("--properties-json must be a JSON object with string keys")
        return {k: str(v) for k, v in props.items()}
    return parse_properties_lines(Path(args.from_file).read_text().splitlines())
