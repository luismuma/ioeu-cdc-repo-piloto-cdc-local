import sys

class OGGError(Exception):
    pass

class OGGAPIError(OGGError):
    pass

class UserInputError(Exception):
    pass


def format_api_error(r):
    """Extract a clean error string from an OGG REST API error response.

    Returns e.g.: '(HTTP 500): [OGG-08110] Login failed. ORA-24459: ...'
    Falls back to the raw response text if JSON parsing fails.
    """
    try:
        body = r.json()
        messages = body.get("messages", [])
        if messages:
            parts = []
            for msg in messages:
                code = msg.get("code", "")
                title = msg.get("title", "").strip()
                parts.append(f"[{code}] {title}" if code else title)
            detail = "; ".join(parts)
            return f"(HTTP {r.status_code}): {detail}"
    except Exception:
        pass
    return f"(HTTP {r.status_code}): {r.text}"


def die(msg: str, code: int = 2) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)

