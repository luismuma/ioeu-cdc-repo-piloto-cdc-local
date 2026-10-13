import logging
import os

import requests as _requests

from api.errors import OGGAPIError

log = logging.getLogger(__name__)


def entra_cfg_from_inventory(cfg):
    """Extract Entra ID auth parameters from a merged inventory cfg dict.

    Returns a dict with (tenant_id, client_id, secret_env_name) or None if any
    required field is absent (meaning Entra ID is not configured for this deployment).
    The actual client_secret is read from the environment lazily at token-fetch time.
    """
    tenant_id       = cfg.get("entraid_tenant_id")
    env             = cfg.get("env", "").lower()          # dev / pre / pro
    client_id       = cfg.get(f"entraid_client_id_{env}")
    secret_env_name = cfg.get("entraid_client_secret")    # env var name, not the value

    if not (tenant_id and client_id and secret_env_name):
        return None

    return {
        "tenant_id":       tenant_id,
        "client_id":       client_id,
        "secret_env_name": secret_env_name,
    }


def fetch_bearer_token(tenant_id, client_id, client_secret):
    """Request an OAuth2 client-credentials Bearer token from Azure AD / Entra ID.

    scope is derived as '<client_id>/.default' (standard for Azure app registrations).
    Raises OGGAPIError on any failure.
    """
    url   = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    scope = f"{client_id}/.default"
    log.debug("Fetching Entra ID Bearer token from %s (client_id=%s)", url, client_id)
    try:
        resp = _requests.post(url, data={
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
            "scope":         scope,
        }, timeout=30)
    except Exception as e:
        raise OGGAPIError(f"Entra ID token request failed (connection error): {e}") from e

    if resp.status_code != 200:
        raise OGGAPIError(
            f"Entra ID token request failed (HTTP {resp.status_code}): {resp.text[:300]}"
        )

    token = resp.json().get("access_token")
    if not token:
        raise OGGAPIError("Entra ID token response did not contain access_token")

    log.debug("Entra ID Bearer token acquired")
    return token
