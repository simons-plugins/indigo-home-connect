"""OAuth for the Home Connect API: Device Flow (production) + Authorization Code
Grant (simulator), token persistence and proactive refresh scheduling.

Design (PRD §2, §4 "Token storage"):

* Tokens live in a standalone JSON file (0600), keyed by ``client_id`` so
  switching clients keeps old tokens. Each entry stores the granted scopes and
  an absolute expiry timestamp.
* This module owns **no thread**. The caller drives refresh from its own worker
  via :meth:`next_refresh_due` / :meth:`refresh_if_needed`, and drives the
  interactive device-flow poll loop via :meth:`run_device_flow` (a blocking
  helper) or by calling :meth:`start_device_flow` / :meth:`poll_device_flow`
  directly.
* Thread-safe: all token-store and device-flow state is guarded by a lock.

Never imports ``indigo``. All HTTP goes through an injected ``HomeConnectAPI``.
"""
import json
import logging
import os
import threading
import time
import urllib.parse

from hc_api import HomeConnectError, redact

SCOPES = "IdentifyAppliance Monitor Settings Control"
SIMULATOR_REDIRECT_URI = "https://apiclient.home-connect.com/o2c.html"

DEVICE_AUTH_PATH = "/security/oauth/device_authorization"
AUTHORIZE_PATH = "/security/oauth/authorize"
TOKEN_PATH = "/security/oauth/token"

# Refresh proactively 1 h before expiry; keep >= 6 s between refresh attempts
# (refresh limit is 10/min) — see PRD §2.
REFRESH_WINDOW = 60 * 60
MIN_REFRESH_INTERVAL = 6
DEFAULT_DEVICE_INTERVAL = 5
DEFAULT_DEVICE_EXPIRES = 300
SLOW_DOWN_STEP = 5

# Auth states surfaced to the plugin / status line.
STATE_UNAUTHORIZED = "unauthorized"
STATE_PENDING = "pending"
STATE_AUTHORIZED = "authorized"
STATE_AUTH_REQUIRED = "authorization_required"   # revoked / 60-day expiry


class HomeConnectAuth:
    """Holds the token store for one client id and performs the OAuth flows."""

    def __init__(self, api, token_path, client_id, client_secret="", simulator=False,
                 logger=None, now=time.time, sleep=time.sleep):
        self._api = api
        self._token_path = token_path
        self._client_id = client_id or ""
        self._client_secret = client_secret or ""
        self._simulator = simulator
        self._logger = logger or logging.getLogger("hc_auth")
        self._now = now
        self._sleep = sleep

        self._lock = threading.RLock()
        self._tokens = {}                 # {client_id: entry}
        self._device = None               # active device-flow context
        self._device_interval = DEFAULT_DEVICE_INTERVAL
        self._last_refresh_attempt = 0.0
        self._refresh_backoff_until = 0.0
        self._state = STATE_UNAUTHORIZED

        self._load()
        if self._client_id in self._tokens:
            self._state = STATE_AUTHORIZED

    # -- Token store persistence --------------------------------------------
    def _load(self):
        try:
            with open(self._token_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, dict):
                self._tokens = {k: v for k, v in data.items() if isinstance(v, dict)}
        except (OSError, ValueError):
            self._tokens = {}

    def _save(self):
        directory = os.path.dirname(self._token_path) or "."
        try:
            os.makedirs(directory, exist_ok=True)
        except OSError:
            pass
        # Write then chmod 0600 (create restricted where the platform allows).
        with open(self._token_path, "w", encoding="utf-8") as handle:
            json.dump(self._tokens, handle)
        try:
            os.chmod(self._token_path, 0o600)
        except OSError:
            pass

    # -- Public state accessors ---------------------------------------------
    @property
    def client_id(self):
        return self._client_id

    @property
    def simulator(self):
        return self._simulator

    def state(self):
        with self._lock:
            return self._state

    def is_authorized(self):
        with self._lock:
            return self._client_id in self._tokens

    def authorization_header(self):
        """Return the current access token (used by :class:`HomeConnectAPI`)."""
        with self._lock:
            entry = self._tokens.get(self._client_id)
            return entry.get("access_token") if entry else None

    def scopes(self):
        with self._lock:
            entry = self._tokens.get(self._client_id)
            return list(entry.get("scopes", [])) if entry else SCOPES.split()

    def access_expires_at(self):
        with self._lock:
            entry = self._tokens.get(self._client_id)
            return entry.get("expires_at") if entry else None

    # -- Token storage helper -----------------------------------------------
    def _store_token(self, data):
        access = data["access_token"]
        refresh = data.get("refresh_token")
        expires_in = int(data.get("expires_in", 86400))
        scope = data.get("scope") or SCOPES
        entry = {
            "access_token": access,
            "refresh_token": refresh,
            "expires_at": self._now() + expires_in,
            "scopes": scope.split() if isinstance(scope, str) else list(scope),
            "obtained_at": self._now(),
        }
        with self._lock:
            self._tokens[self._client_id] = entry
            self._state = STATE_AUTHORIZED
            self._save()                  # persist the (new) refresh token immediately
        self._logger.debug("Stored Home Connect token: access=%s refresh=%s expires_in=%ds",
                           redact(access), redact(refresh), expires_in)

    # -- Device Flow (production) -------------------------------------------
    def start_device_flow(self):
        """Request a verification URI + user code. Stashes the device code."""
        form = {"client_id": self._client_id, "scope": SCOPES}
        data = self._api.post_form(DEVICE_AUTH_PATH, form, authorize=False)
        interval = int(data.get("interval", DEFAULT_DEVICE_INTERVAL))
        expires_in = int(data.get("expires_in", DEFAULT_DEVICE_EXPIRES))
        uri = data.get("verification_uri_complete") or data.get("verification_uri")
        user_code = data.get("user_code")
        with self._lock:
            self._device = {"device_code": data["device_code"], "expires_at": self._now() + expires_in}
            self._device_interval = interval
            self._state = STATE_PENDING
        self._logger.info("Home Connect authorization: visit %s and enter code %s", uri, user_code)
        return {
            "verification_uri_complete": uri,
            "verification_uri": data.get("verification_uri"),
            "user_code": user_code,
            "interval": interval,
            "expires_in": expires_in,
        }

    def poll_device_flow(self):
        """One token poll. Returns ``(status, detail)`` where status is one of
        ``pending`` / ``slow_down`` / ``success`` / ``denied`` / ``expired`` /
        ``error``."""
        with self._lock:
            device = self._device
        if not device:
            return ("error", "no active device flow")
        form = {
            "client_id": self._client_id,
            "grant_type": "device_code",
            "device_code": device["device_code"],
        }
        if self._client_secret:
            form["client_secret"] = self._client_secret
        try:
            data = self._token_request(form, retry_urn_grant=True)
        except HomeConnectError as exc:
            key = exc.key
            if key == "authorization_pending":
                return ("pending", None)
            if key == "slow_down":
                with self._lock:
                    self._device_interval += SLOW_DOWN_STEP
                return ("slow_down", None)
            if key == "access_denied":
                with self._lock:
                    self._device = None
                    self._state = STATE_UNAUTHORIZED
                return ("denied", exc.description or "access denied")
            if key == "expired_token":
                with self._lock:
                    self._device = None
                    self._state = STATE_UNAUTHORIZED
                return ("expired", exc.description or "device code expired")
            self._logger.error("Home Connect device-flow poll failed: %s", exc)
            return ("error", str(exc))
        self._store_token(data)
        with self._lock:
            self._device = None
        return ("success", None)

    def run_device_flow(self, on_prompt=None, should_stop=None, max_restarts=1):
        """Blocking driver: start the flow, poll until resolved, auto-restart
        once on expiry. Runs on the *caller's* thread. ``on_prompt(info)`` is
        called each time a new code is issued; ``should_stop()`` cancels."""
        restarts = 0
        while True:
            info = self.start_device_flow()
            if on_prompt:
                on_prompt(info)
            while True:
                if should_stop and should_stop():
                    with self._lock:
                        self._device = None
                        self._state = STATE_UNAUTHORIZED
                    return ("cancelled", None)
                self._sleep(self._device_interval)
                status, detail = self.poll_device_flow()
                if status in ("pending", "slow_down"):
                    continue
                if status == "expired" and restarts < max_restarts:
                    restarts += 1
                    self._logger.info("Home Connect device code expired; restarting authorization")
                    break                 # restart the outer loop
                return (status, detail)

    # -- Authorization Code Grant (simulator only) --------------------------
    def authorize_simulator(self):
        """Simulator auth: auto-approved code grant, no browser interaction."""
        query = urllib.parse.urlencode({
            "client_id": self._client_id,
            "response_type": "code",
            "scope": SCOPES,
            "redirect_uri": SIMULATOR_REDIRECT_URI,
            "user": "me",
        })
        raw = self._api.request("GET", f"{AUTHORIZE_PATH}?{query}",
                                accept="application/json", authorize=False)
        location = raw.header("location")
        if raw.status != 302 or not location:
            raise HomeConnectError(
                f"simulator authorize did not redirect (status {raw.status})", status=raw.status)
        code = self._extract_code(location)
        if not code:
            raise HomeConnectError("simulator authorize redirect had no code")
        form = {
            "client_id": self._client_id,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": SIMULATOR_REDIRECT_URI,   # required by the simulator
        }
        if self._client_secret:
            form["client_secret"] = self._client_secret
        data = self._token_request(form, retry_urn_grant=False)
        self._store_token(data)
        return ("success", None)

    @staticmethod
    def _extract_code(location):
        parsed = urllib.parse.urlparse(location)
        params = urllib.parse.parse_qs(parsed.query)
        values = params.get("code")
        return values[0] if values else None

    # -- Refresh -------------------------------------------------------------
    def next_refresh_due(self):
        """Monotonic-agnostic wall-clock time (via ``now``) the token should be
        refreshed, or ``None`` when there is no token to refresh."""
        with self._lock:
            entry = self._tokens.get(self._client_id)
            if not entry:
                return None
            return entry["expires_at"] - REFRESH_WINDOW

    def refresh_if_needed(self, force=False):
        """Refresh the access token if it is within :data:`REFRESH_WINDOW` of
        expiry (or ``force``). Honors the minimum refresh interval and any
        ``Retry-After`` backoff. Returns True if a refresh succeeded."""
        with self._lock:
            entry = self._tokens.get(self._client_id)
            if not entry:
                return False
            due_at = entry["expires_at"] - REFRESH_WINDOW
            now = self._now()
            if not force and now < due_at:
                return False
            if now < self._refresh_backoff_until:
                return False
            if not force and (now - self._last_refresh_attempt) < MIN_REFRESH_INTERVAL:
                return False
            refresh_token = entry.get("refresh_token")
            self._last_refresh_attempt = now
        if not refresh_token:
            return False
        return self._do_refresh(refresh_token)

    def _do_refresh(self, refresh_token):
        form = {"grant_type": "refresh_token", "refresh_token": refresh_token}
        if self._client_secret:
            form["client_secret"] = self._client_secret
        try:
            data = self._token_request(form, retry_urn_grant=False)
        except HomeConnectError as exc:
            if exc.status == 429 and exc.retry_after:
                with self._lock:
                    self._refresh_backoff_until = self._now() + exc.retry_after
                self._logger.warning("Home Connect token refresh rate-limited; retrying in %ss",
                                     exc.retry_after)
                return False
            if exc.key == "invalid_grant":
                with self._lock:
                    self._state = STATE_AUTH_REQUIRED
                self._logger.error("Home Connect authorization lost (access revoked or unused "
                                   ">60 days). Re-authorize the plugin in its configuration.")
                return False
            self._logger.error("Home Connect token refresh failed: %s", exc)
            return False
        self._store_token(data)
        self._logger.info("Home Connect access token refreshed")
        return True

    def handle_unauthorized(self, used_token):
        """401 hook for :class:`HomeConnectAPI`: force one early refresh if the
        failing request used the current token. Returns True if the token
        changed (so the request may be retried)."""
        with self._lock:
            entry = self._tokens.get(self._client_id)
            current = entry.get("access_token") if entry else None
        if used_token != current:
            return False                  # a refresh already happened concurrently
        if self.refresh_if_needed(force=True):
            return self.authorization_header() != used_token
        return False

    # -- Token endpoint with grant-type fallback ----------------------------
    def _token_request(self, form, retry_urn_grant):
        try:
            return self._api.post_form(TOKEN_PATH, dict(form), authorize=False)
        except HomeConnectError as exc:
            if (retry_urn_grant and form.get("grant_type") == "device_code"
                    and exc.key in ("unsupported_grant_type", "invalid_request")):
                fallback = dict(form)
                fallback["grant_type"] = "urn:ietf:params:oauth:grant-type:device_code"
                return self._api.post_form(TOKEN_PATH, fallback, authorize=False)
            raise
