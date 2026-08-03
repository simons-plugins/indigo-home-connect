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

Concurrency (fixed after Phase 1 review):

* All :class:`HomeConnectAuth` instances pointing at the same token file share a
  single process-wide :class:`TokenStore` (keyed by absolute path). The store is
  the sole writer: every write does ``mkstemp`` → ``chmod 0600`` → ``os.replace``
  so a concurrent reader never sees a truncated/partial file, and re-reads the
  file first so it merges — never blind-overwrites — other clients' keys.
* Token refresh is serialised by a per-client lock held for the whole HTTP
  round-trip. Because Home Connect rotates the refresh token on every use, a
  second caller that arrives while a refresh is in flight waits, then re-reads
  the freshly rotated token instead of re-submitting the now single-use one.
* A superseded instance (config re-authorised) is marked stale and refuses to
  persist a late device-flow result, so it cannot clobber the live token.

Never imports ``indigo``. All HTTP goes through an injected ``HomeConnectAPI``.
"""
import json
import logging
import os
import tempfile
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

# Exponential backoff for a refresh that fails for a generic reason (not 429,
# not invalid_grant): the token endpoint allows only 10/min and 100/day, so a
# broken client secret or a persistent 500 must not be retried every 60 s
# supervisor tick (that alone would blow the endpoint's own daily limit).
REFRESH_BACKOFF_BASE = 60
REFRESH_BACKOFF_MAX = 60 * 60
DEFAULT_DEVICE_INTERVAL = 5
DEFAULT_DEVICE_EXPIRES = 300
SLOW_DOWN_STEP = 5

# Auth states surfaced to the plugin / status line.
STATE_UNAUTHORIZED = "unauthorized"
STATE_PENDING = "pending"
STATE_AUTHORIZED = "authorized"
STATE_AUTH_REQUIRED = "authorization_required"   # revoked / 60-day expiry


# ---------------------------------------------------------------------------
# Shared, process-wide token store (one per absolute path)
# ---------------------------------------------------------------------------
_STORES = {}
_STORES_GUARD = threading.Lock()


def get_token_store(path, logger=None):
    """Return the shared :class:`TokenStore` for ``path`` (created on first use).

    All auth instances that use the same file get the *same* store object, so a
    per-instance write can never blind-overwrite another instance's token.
    """
    abspath = os.path.abspath(path)
    with _STORES_GUARD:
        store = _STORES.get(abspath)
        if store is None:
            store = TokenStore(abspath, logger=logger)
            _STORES[abspath] = store
        return store


class TokenStore:
    """The single writer for one token file. Atomic writes; merge-on-write."""

    def __init__(self, path, logger=None):
        self._path = os.path.abspath(path)
        self._logger = logger or logging.getLogger("hc_auth")
        self._lock = threading.RLock()
        self._refresh_locks = {}
        self._refresh_guard = threading.Lock()
        self._last_refresh_ts = {}
        self._entries = self._read_disk()

    @property
    def path(self):
        return self._path

    def _read_disk(self):
        try:
            with open(self._path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, ValueError):
            return {}
        if not isinstance(data, dict):
            return {}
        return {k: v for k, v in data.items() if isinstance(v, dict)}

    def _atomic_write(self, data):
        directory = os.path.dirname(self._path) or "."
        try:
            os.makedirs(directory, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    json.dump(data, handle)
                os.chmod(tmp, 0o600)          # restrict BEFORE it becomes the real file
                os.replace(tmp, self._path)   # atomic: readers see old or new, never partial
            except OSError:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
        except OSError as exc:
            self._logger.warning("Home Connect token save failed (store left intact): %s", exc)

    def get(self, client_id):
        with self._lock:
            entry = self._entries.get(client_id)
            return dict(entry) if entry is not None else None

    def keys(self):
        with self._lock:
            return list(self._entries.keys())

    def set(self, client_id, entry):
        with self._lock:
            merged = self._read_disk()        # pick up other clients' keys first
            merged.update(self._entries)      # our in-process view wins for our keys
            merged[client_id] = dict(entry)
            self._entries = merged
            self._atomic_write(merged)

    def refresh_lock(self, client_id):
        with self._refresh_guard:
            lock = self._refresh_locks.get(client_id)
            if lock is None:
                lock = threading.Lock()
                self._refresh_locks[client_id] = lock
            return lock

    def note_refresh(self, client_id, ts):
        with self._lock:
            self._last_refresh_ts[client_id] = ts

    def last_refresh(self, client_id):
        with self._lock:
            return self._last_refresh_ts.get(client_id, float("-inf"))


def _expires_at(entry):
    """The entry's numeric expiry, or 0 (= due immediately) when corrupted."""
    value = entry.get("expires_at")
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else 0


class HomeConnectAuth:
    """Holds one client id's OAuth flows over a shared :class:`TokenStore`."""

    def __init__(self, api, token_path, client_id, client_secret="", simulator=False,
                 logger=None, now=time.time, sleep=time.sleep):
        self._api = api
        self._token_path = os.path.abspath(token_path)
        self._store = get_token_store(self._token_path, logger=logger)
        self._client_id = client_id or ""
        self._client_secret = client_secret or ""
        self._simulator = simulator
        self._logger = logger or logging.getLogger("hc_auth")
        self._now = now
        self._sleep = sleep

        self._lock = threading.RLock()
        self._device = None               # active device-flow context
        self._device_interval = DEFAULT_DEVICE_INTERVAL
        self._last_refresh_attempt = 0.0
        self._refresh_backoff_until = 0.0
        self._refresh_failures = 0
        self._stale = False
        self._state = STATE_AUTHORIZED if self._store.get(self._client_id) else STATE_UNAUTHORIZED

    # -- Lifecycle -----------------------------------------------------------
    def mark_stale(self):
        """Supersede this instance: a late device-flow result won't be persisted."""
        with self._lock:
            self._stale = True

    def matches(self, client_id, client_secret, simulator):
        """True when this instance already represents exactly this client config.

        A prefs save with unchanged values is a no-op supersession and must NOT
        mark this instance stale — a device-flow authorization may be pending on
        it, and marking it stale would silently discard the granted token."""
        return (self._client_id == (client_id or "")
                and self._client_secret == (client_secret or "")
                and self._simulator == bool(simulator))

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
        return self._store.get(self._client_id) is not None

    def authorization_header(self):
        """Return the current access token (used by :class:`HomeConnectAPI`)."""
        entry = self._store.get(self._client_id)
        return entry.get("access_token") if entry else None

    def scopes(self):
        entry = self._store.get(self._client_id)
        return list(entry.get("scopes", [])) if entry else SCOPES.split()

    def access_expires_at(self):
        entry = self._store.get(self._client_id)
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
        self._store.set(self._client_id, entry)   # atomic + merges other clients
        with self._lock:
            self._state = STATE_AUTHORIZED
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
        ``cancelled`` / ``error``."""
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
                self._clear_device(STATE_UNAUTHORIZED)
                return ("denied", exc.description or "access denied")
            if key == "expired_token":
                self._clear_device(STATE_UNAUTHORIZED)
                return ("expired", exc.description or "device code expired")
            self._logger.error("Home Connect device-flow poll failed: %s", exc)
            return ("error", str(exc))
        # Refuse to persist a result from a superseded instance.
        with self._lock:
            stale = self._stale
        if stale:
            self._clear_device(STATE_UNAUTHORIZED)
            return ("cancelled", "instance superseded")
        self._store_token(data)
        self._clear_device(None)
        return ("success", None)

    def _clear_device(self, new_state):
        with self._lock:
            self._device = None
            if new_state is not None:
                self._state = new_state

    def run_device_flow(self, on_prompt=None, should_stop=None, max_restarts=1):
        """Blocking driver: poll until resolved, auto-restart once on expiry.
        Reuses a device code already stashed by :meth:`start_device_flow` (the
        one whose user code the caller is displaying) — starting a fresh flow
        here would orphan that code and the user would approve the wrong one.
        Runs on the *caller's* thread. ``on_prompt(info)`` is called each time
        a NEW code is issued; ``should_stop()`` cancels."""
        restarts = 0
        with self._lock:
            device = self._device
            active = device is not None and self._now() < device["expires_at"]
        if not active:
            info = self.start_device_flow()
            if on_prompt:
                on_prompt(info)
        while True:
            if should_stop and should_stop():
                self._clear_device(STATE_UNAUTHORIZED)
                return ("cancelled", None)
            self._sleep(self._device_interval)
            status, detail = self.poll_device_flow()
            if status in ("pending", "slow_down"):
                continue
            if status == "expired" and restarts < max_restarts:
                restarts += 1
                self._logger.info("Home Connect device code expired; restarting authorization")
                info = self.start_device_flow()
                if on_prompt:
                    on_prompt(info)
                continue
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
        """Wall-clock time (via ``now``) the token should be refreshed, or
        ``None`` when there is no token to refresh."""
        entry = self._store.get(self._client_id)
        if not entry:
            return None
        # A hand-edited/truncated entry with a missing or non-numeric
        # expires_at is due immediately — the refresh rewrites it whole (never
        # a KeyError/TypeError loop in the supervisor).
        return _expires_at(entry) - REFRESH_WINDOW

    def refresh_if_needed(self, force=False):
        """Refresh the access token if it is within :data:`REFRESH_WINDOW` of
        expiry (or ``force``). Honors the minimum refresh interval and any
        active backoff — a 429 ``Retry-After`` or the exponential backoff after
        repeated generic failures. Defers (returns False) while the shared
        request gate is closed: a token POST would otherwise block the caller —
        the plugin's supervisor thread — for the whole Retry-After. Concurrent
        callers collapse to one HTTP refresh. Returns True if the current token
        is fresh afterwards."""
        entry = self._store.get(self._client_id)
        if not entry:
            return False
        if self._api.gate_wait_remaining() > 0:
            return False              # gate closed: retried on a later tick
        with self._lock:
            if self._state == STATE_AUTH_REQUIRED:
                return False              # dead refresh token: re-auth required, don't resubmit
            now = self._now()
            due_at = _expires_at(entry) - REFRESH_WINDOW
            if not force and now < due_at:
                return False
            if now < self._refresh_backoff_until:
                return False
            if not force and (now - self._last_refresh_attempt) < MIN_REFRESH_INTERVAL:
                return False
            self._last_refresh_attempt = now
        seen_access = entry.get("access_token")
        refresh_token = entry.get("refresh_token")
        if not refresh_token:
            return False
        return self._do_refresh(refresh_token, seen_access)

    def _do_refresh(self, refresh_token, seen_access):
        # Serialise per-client for the whole HTTP round-trip: the refresh token
        # is single-use, so a second caller must wait then re-read, not resubmit.
        with self._store.refresh_lock(self._client_id):
            # A refresh that completed moments ago (concurrent caller, or a second
            # forced 401 refresh) already produced a fresh token — don't resubmit
            # the now single-use refresh token.
            if (self._now() - self._store.last_refresh(self._client_id)) < MIN_REFRESH_INTERVAL:
                return True
            current = self._store.get(self._client_id)
            if current and current.get("access_token") != seen_access:
                return True               # another caller already refreshed
            use_token = current.get("refresh_token") if current else refresh_token
            form = {"grant_type": "refresh_token", "refresh_token": use_token}
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
                # Generic failure (bad secret, token-endpoint 500, transport):
                # back off exponentially — the supervisor re-drives every 60 s,
                # and the token endpoint allows only 100 refreshes/day.
                self._refresh_failures += 1
                backoff = min(REFRESH_BACKOFF_BASE * (2 ** (self._refresh_failures - 1)),
                              REFRESH_BACKOFF_MAX)
                with self._lock:
                    self._refresh_backoff_until = self._now() + backoff
                self._logger.error("Home Connect token refresh failed: %s — next attempt in %ds",
                                   exc, backoff)
                return False
            self._store_token(data)
            self._store.note_refresh(self._client_id, self._now())
            self._refresh_failures = 0
            self._logger.info("Home Connect access token refreshed")
            return True

    def handle_unauthorized(self, used_token):
        """401 hook for :class:`HomeConnectAPI`: force one early refresh if the
        failing request used the current token. Concurrent 401s collapse to a
        single refresh; returns True if the token changed (retry the request)."""
        entry = self._store.get(self._client_id)
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
