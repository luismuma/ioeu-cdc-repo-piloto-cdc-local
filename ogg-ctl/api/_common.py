import logging

log = logging.getLogger(__name__)


def log_ogg_messages(r):
    """Log OGG messages returned in the API response body."""
    try:
        for msg in r.json().get("messages", []):
            severity = msg.get("severity", "INFO").upper()
            code = msg.get("code", "")
            title = msg.get("title", "")
            log.debug("OGG [%s] %s: %s", severity, code, title)
    except Exception:
        pass


def build_managed_process_settings(
    auto_start=None,
    auto_start_delay=None,
    auto_restart=None,
    auto_restart_retries=None,
    auto_restart_delay=None,
    on_success=None,
    window=None,
    disable_on_failure=None,
):
    """Build a managedProcessSettings dict for the OGG API, or return None if
    no managed-process options were specified."""
    if auto_start is None and auto_restart is None:
        return None
    settings = {"$schema": "ogg:managedProcessSettings"}
    if auto_start is not None:
        entry = {"enabled": bool(auto_start)}
        if auto_start_delay is not None:
            entry["delay"] = auto_start_delay
        settings["autoStart"] = entry
    if auto_restart is not None:
        entry = {"enabled": bool(auto_restart)}
        if auto_restart_retries is not None:
            entry["retries"] = auto_restart_retries
        if auto_restart_delay is not None:
            entry["delay"] = auto_restart_delay
        if on_success is not None:
            entry["onSuccess"] = bool(on_success)
        if window is not None:
            entry["window"] = window
        if disable_on_failure is not None:
            entry["disableOnFailure"] = bool(disable_on_failure)
        settings["autoRestart"] = entry
    return settings


def build_path_endpoint(uri, alias=None, domain="OracleGoldenGate"):
    """Build an endpoint dict for a distribution or receiver path."""
    endpoint = {
        "$schema": "ogg:distPathEndpoint",
        "uri": uri,
    }
    if alias:
        endpoint["authenticationMethod"] = {
            "$schema": "ogg:credentialsRef",
            "domain": domain,
            "alias": alias,
        }
    return endpoint
