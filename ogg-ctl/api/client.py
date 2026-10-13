from pathlib import Path
import logging
import os
import requests
from requests.exceptions import Timeout, ConnectionError as RequestsConnectionError

from api.errors import OGGAPIError

log = logging.getLogger(__name__)

# 30 s to establish a TCP connection; 180 s to receive the first response byte.
#client ──(30s limit)──> TCP connect to OGG host:port
#client ──(180s limit)──> waiting for HTTP response body
DEFAULT_TIMEOUT = (30, 180)

# nginx reverse-proxy service path names
# "service_manager" is intentionally absent — its URL has no service-path segment
_NGINX_SERVICE_PATH = {
    "admin":        "adminsrvr",
    "distribution": "distsrvr",
    "receiver":     "recvsrvr",
    "performance":  "pmsrvr",
}


class OGGClient:
    """HTTP client for the OGG Microservices REST API.

    Auth strategy (auto-detect):
      - If OGG_ADMIN_PASSWORD is set: try basic auth first.  On 401, fall back
        to Entra ID Bearer (if configured), then lock in whichever succeeds.
      - If OGG_ADMIN_PASSWORD is not set but Entra ID is configured: go straight
        to Bearer without probing basic.
      - If neither is available: fail at construction time.

    _auth_mode values:
      "probing" — first request will try basic; may fall back to bearer on 401
      "basic"   — locked in to basic auth for this session
      "bearer"  — locked in to Bearer; token is refreshed once on 401 (expiry)
    """

    def __init__(self, admin_url, api_version, user, verify_tls,
                 password_file=None, password=None, entra_cfg=None,
                 distribution_url=None, receiver_url=None, performance_url=None,
                 service_manager_url=None,
                 deployment_name=None, nginx_proxy=False,
                 timeout=DEFAULT_TIMEOUT):
        self.api_version = api_version
        self.user = user
        self.verify_tls = verify_tls
        self.timeout = timeout
        self._nginx_proxy = nginx_proxy
        self._deployment_name = deployment_name
        self._entra_cfg = entra_cfg      # dict or None
        self._bearer_token = None

        if password is not None:
            self.password = password            # explicit value — highest priority
        elif password_file:
            self.password = Path(password_file).read_text().strip()
        else:
            self.password = os.environ.get("OGG_ADMIN_PASSWORD", "")

        if not self.password and not self._entra_cfg:
            raise OGGAPIError(
                "No authentication available: set OGG_ADMIN_PASSWORD or configure "
                "Entra ID (entraid_* fields in inventory)"
            )

        # Skip the basic probe entirely if no password is configured.
        self._auth_mode = "probing" if self.password else "bearer"
        self.auth = (self.user, self.password) if self.password else None

        admin = admin_url.rstrip("/")
        self._base_url = admin
        self._service_urls = {
            "admin":           admin,
            "distribution":    (distribution_url or admin).rstrip("/"),
            "receiver":        (receiver_url or admin).rstrip("/"),
            "performance":     (performance_url or admin).rstrip("/"),
            "service_manager": (service_manager_url or admin).rstrip("/"),
        }

    def _base_for(self, service="admin"):
        if self._nginx_proxy:
            if service == "service_manager":
                return f"{self._base_url}/services/{self.api_version}"
            svc_path = _NGINX_SERVICE_PATH[service]
            return f"{self._base_url}/services/{self._deployment_name}/{svc_path}/{self.api_version}"
        return f"{self._service_urls[service]}/services/{self.api_version}"

    def _get_bearer_token(self):
        """Read the client secret from env and fetch a fresh Bearer token."""
        from api.auth import fetch_bearer_token
        entra = self._entra_cfg
        if not entra:
            raise OGGAPIError(
                "Bearer auth required but Entra ID is not configured for this deployment"
            )
        # Direct secret value (passed by multi-env monitoring) takes priority over env var.
        client_secret = entra.get("secret_value") or os.environ.get(entra["secret_env_name"])
        if not client_secret:
            raise OGGAPIError(
                f"Entra ID Bearer auth failed: env var '{entra['secret_env_name']}' is not set. "
                "Export it with the Entra ID client secret and retry."
            )
        return fetch_bearer_token(entra["tenant_id"], entra["client_id"], client_secret)

    def _raw_request(self, fn, url, *, use_bearer=False, **kwargs):
        """Single HTTP call — either basic auth or Bearer header."""
        if use_bearer:
            headers = kwargs.pop("headers", {})
            headers["Authorization"] = f"Bearer {self._bearer_token}"
            return fn(url, headers=headers, verify=self.verify_tls,
                      timeout=self.timeout, **kwargs)
        return fn(url, auth=self.auth, verify=self.verify_tls,
                  timeout=self.timeout, **kwargs)

    def _request(self, fn, url, **kwargs):
        try:
            if self._auth_mode == "basic":
                r = self._raw_request(fn, url, **kwargs)

            elif self._auth_mode == "bearer":
                if self._bearer_token is None:
                    self._bearer_token = self._get_bearer_token()
                r = self._raw_request(fn, url, use_bearer=True, **kwargs)
                if r.status_code == 401:
                    log.debug("Bearer token expired, refreshing")
                    self._bearer_token = self._get_bearer_token()
                    r = self._raw_request(fn, url, use_bearer=True, **kwargs)

            else:  # probing
                r = self._raw_request(fn, url, **kwargs)
                log.debug("%s %s -> HTTP %s (auth probe: basic)",
                          fn.__name__.upper(), url, r.status_code)
                if r.status_code != 401:
                    self._auth_mode = "basic"
                    log.debug("Auth probe: basic auth accepted — locking in")
                    return r

                if not self._entra_cfg:
                    # basic failed and no Entra ID configured — let the 401 surface
                    self._auth_mode = "basic"
                    return r

                log.debug("Auth probe: basic rejected (401), trying Entra ID Bearer")
                self._bearer_token = self._get_bearer_token()
                r = self._raw_request(fn, url, use_bearer=True, **kwargs)
                if r.status_code != 401:
                    self._auth_mode = "bearer"
                    log.debug("Auth probe: Bearer token accepted — locking in")
                return r

            log.debug("%s %s -> HTTP %s", fn.__name__.upper(), url, r.status_code)
            return r

        except Timeout:
            raise OGGAPIError(f"Request timed out after {self.timeout}s: {url}")
        except RequestsConnectionError as e:
            raise OGGAPIError(f"Connection failed: {url} — {e}")

    @property
    def active_auth_mode(self):
        """Returns the currently active auth mode string for diagnostics."""
        return self._auth_mode

    def get(self, path, service="admin"):
        return self._request(requests.get, f"{self._base_for(service)}{path}")

    def post(self, path, payload, service="admin"):
        return self._request(requests.post, f"{self._base_for(service)}{path}", json=payload)

    def put(self, path, payload, service="admin"):
        return self._request(requests.put, f"{self._base_for(service)}{path}", json=payload)

    def patch(self, path, payload, service="admin"):
        return self._request(requests.patch, f"{self._base_for(service)}{path}", json=payload)

    def delete(self, path, service="admin"):
        return self._request(requests.delete, f"{self._base_for(service)}{path}")
