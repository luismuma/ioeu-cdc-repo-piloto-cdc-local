import json
import sys

_STATUS_COLORS = {
    "running":  "\033[32m",
    "abended":  "\033[31m",
    "stopped":  "\033[33m",
    "starting": "\033[36m",
    "stopping": "\033[36m",
}
_RESET = "\033[0m"

# Auto-detect: JSON when stdout is not a terminal (e.g. AAP command module, pipe)
_json_mode = not sys.stdout.isatty()


def is_json_mode() -> bool:
    return _json_mode


def set_format(fmt: str):
    """Override output format. fmt: 'json' | 'table' | 'auto'."""
    global _json_mode
    if fmt == "json":
        _json_mode = True
    elif fmt == "table":
        _json_mode = False
    # 'auto' keeps the isatty() default set at import time


def _colorize(status):
    if _json_mode or not sys.stdout.isatty():
        return status
    color = _STATUS_COLORS.get(status.lower(), "")
    return f"{color}{status}{_RESET}" if color else status


def print_status_table(rows):
    if _json_mode:
        print(json.dumps([{"name": n, "status": s} for n, s in rows], indent=2))
        return
    name_w = max((len(r[0]) for r in rows), default=4)
    name_w = max(name_w, 4)
    print(f"{'NAME':<{name_w}}  STATUS")
    print("─" * (name_w + 8))
    for name, status in rows:
        print(f"{name:<{name_w}}  {_colorize(status)}")


def print_name_list(names):
    if _json_mode:
        print(json.dumps(names, indent=2))
        return
    name_w = max((len(n) for n in names), default=4)
    name_w = max(name_w, 4)
    print("NAME")
    print("─" * name_w)
    for name in names:
        print(name)


def print_info(data: dict):
    """Print a flat key→value dict as JSON or aligned key: value pairs."""
    if _json_mode:
        print(json.dumps(data, indent=2))
        return
    if not data:
        return
    key_w = max(len(k) for k in data)
    for k, v in data.items():
        print(f"{k:<{key_w}} : {v if v is not None else 'N/A'}")


def print_rows(rows: list):
    """Print a list of dicts as a JSON array or as sequential print_info blocks."""
    if _json_mode:
        print(json.dumps(rows, indent=2))
        return
    for i, row in enumerate(rows):
        if i:
            print()
        print_info(row)
