"""Stdlib-only HTTP client for the Home Connect cloud API.

Implements the plugin-wide request gate, retry policy, ``Retry-After`` handling,
a daily request-budget counter and the error-envelope parsing described in the
PRD (``docs/plans/PRD-indigo-home-connect.md`` §2-3). No third-party deps — the
transport is ``http.client`` so it can be extended with SSE streaming in Phase 2.

The transport is injectable (``connection_factory``) so tests can drive it with a
fake connection without touching the network. This module never imports
``indigo`` — all Indigo-touching code lives in ``plugin.py``.
"""
import http.client
import json
import logging
import re
import socket
import threading
import time
import urllib.parse
from datetime import datetime, timezone

# Hosts / content types -------------------------------------------------------
PROD_HOST = "api.home-connect.com"
SIMULATOR_HOST = "simulator.home-connect.com"
HC_CONTENT_TYPE = "application/vnd.bsh.sdk.v1+json"
FORM_CONTENT_TYPE = "application/x-www-form-urlencoded"
SSE_CONTENT_TYPE = "text/event-stream"

# SSE read timeout must be > the 55 s keep-alive so a silent stream is caught as
# dead (PRD §3.1). Each socket read blocks up to this long before raising.
STREAM_READ_TIMEOUT = 120
_STREAM_CHUNK = 8192

# Never retry these status codes (see PRD §3 / homebridge-homeconnect).
NO_RETRY_STATUS = frozenset({400, 403, 404, 405, 406, 409, 415})
IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "PUT", "DELETE", "OPTIONS", "TRACE"})

# Daily request budget (per client+account). Warn before BSH blocks the client.
DAILY_BUDGET = 1000
BUDGET_WARN_AT = 800

DEFAULT_TIMEOUT = 30
DEFAULT_MAX_RETRIES = 3

# A 429 with no Retry-After header is real (BSH's non-time-based limits — the
# 10-channel cap, the OAuth limits — return a bare Too Many Requests): apply a
# synthetic gate delay so an error loop can never hammer the API.
DEFAULT_RETRY_AFTER = 60

# Base delay between retry attempts for transport/5xx failures, doubling per
# attempt. 429 is excluded — the request gate already enforces Retry-After.
# Back-to-back retries feed BSH's 10-successive-errors-per-10-min block.
RETRY_BACKOFF_BASE = 2.0

# While waiting out the gate, wake this often to notice abort()/shutdown.
_GATE_WAIT_SLICE = 1.0

# BSH returns localized program/option/setting display ``name``s keyed off the
# request's Accept-Language. No config UI for it (PRD keeps auth-only prefs); a
# module constant is enough to get the app's names ("Kurz 60" etc.) instead of
# raw key tails. GB English matches Simon's appliances.
DEFAULT_ACCEPT_LANGUAGE = "en-GB"

_HAID_IN_PATH = re.compile(r"(homeappliances/)([^/?]+)")


def redact(value):
    """Redact a token or haId to first-4 + last-8 chars (PRD §2 redaction rule)."""
    if not value:
        return value
    text = str(value)
    if len(text) <= 12:
        return "…"
    return f"{text[:4]}…{text[-8:]}"


def redact_path(path):
    """Redact haIds embedded in a request path so paths are safe to log."""
    if not path:
        return path
    return _HAID_IN_PATH.sub(lambda m: m.group(1) + redact(m.group(2)), path)


class HomeConnectError(Exception):
    """A Home Connect API failure.

    Carries the HTTP ``status`` (``None`` for transport-level failures), the
    parsed error-envelope ``key`` / ``description`` and any ``retry_after``
    (seconds) advertised by the server.
    """

    def __init__(self, message, status=None, key=None, description=None, retry_after=None):
        super().__init__(message)
        self.status = status
        self.key = key
        self.description = description
        self.retry_after = retry_after


class RawResponse:
    """A minimal HTTP response: status, lower-cased headers and body bytes."""

    __slots__ = ("status", "headers", "body")

    def __init__(self, status, headers, body):
        self.status = status
        self.headers = headers
        self.body = body

    def header(self, name, default=None):
        return self.headers.get(name.lower(), default)

    def text(self):
        return self.body.decode("utf-8", "replace") if self.body else ""


class StreamResponse:
    """A live server-sent-events response.

    Owns the open connection; iterate :meth:`lines` to get decoded text lines
    (newlines stripped) as the server produces them. Each read blocks up to the
    socket timeout, so a silent stream surfaces as a :class:`HomeConnectError`.
    Safe to :meth:`close` from another thread to unblock a stuck reader.
    """

    def __init__(self, conn, resp, path, logger):
        self._conn = conn
        self._resp = resp
        self._path = path
        self._logger = logger
        self._closed = False

    def lines(self):
        buf = ""
        try:
            while True:
                chunk = self._resp.read(_STREAM_CHUNK)
                if not chunk:
                    break                      # server closed the stream
                buf += chunk.decode("utf-8", "replace")
                parts = buf.split("\n")
                buf = parts.pop()              # keep any trailing partial line
                for line in parts:
                    yield line.rstrip("\r")
        except (OSError, http.client.HTTPException, AttributeError, ValueError) as exc:
            # AttributeError/ValueError show up when the socket is closed from
            # another thread mid-chunked-read (shutdown); surface them as a clean
            # HomeConnectError so the reconnect loop logs one line, not a traceback.
            raise HomeConnectError(
                f"event stream read failed on {redact_path(self._path)}: {exc}") from exc
        finally:
            self.close()

    def close(self):
        if self._closed:
            return
        self._closed = True
        # shutdown() interrupts a recv() blocked in another thread promptly;
        # conn.close() alone does not reliably wake a cross-thread blocked read.
        sock = getattr(self._conn, "sock", None)
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        try:
            self._conn.close()
        except Exception:  # pylint: disable=broad-except
            pass


def _default_connection_factory(host, timeout):
    return http.client.HTTPSConnection(host, timeout=timeout)


class HomeConnectAPI:
    """Low-level HTTP client with a plugin-wide request gate and retry policy."""

    def __init__(self, host, logger=None, connection_factory=None, user_agent="indigo-home-connect",
                 timeout=DEFAULT_TIMEOUT, max_retries=DEFAULT_MAX_RETRIES,
                 monotonic=time.monotonic, sleep=None,
                 wall_now=lambda: datetime.now(timezone.utc)):
        # ``sleep`` is a test seam: when injected, gate/backoff waits call it with
        # the whole delay (a fake clock jumps forward). In production it stays
        # None and waits use the abort event, so shutdown interrupts them.
        self._host = host
        self._logger = logger or logging.getLogger("hc_api")
        self._connection_factory = connection_factory or _default_connection_factory
        self._user_agent = user_agent
        self._timeout = timeout
        self._max_retries = max_retries
        self._monotonic = monotonic
        self._sleep = sleep
        self._wall_now = wall_now

        # Request gate: the earliest monotonic time the next request may issue.
        self._earliest_retry = 0.0
        self._gate_logged = False
        # Set once when this client is superseded or the plugin shuts down; any
        # thread waiting out the gate (or a retry backoff) raises promptly
        # instead of finishing a Retry-After that can run to hours.
        self._abort = threading.Event()

        # Daily budget counter (resets at midnight UTC).
        self._counter_date = None
        self._request_count = 0
        self._budget_warned = False

        # Auth wiring (set by hc_auth).
        self._token_provider = None
        self._on_unauthorized = None
        self._last_used_token = None

    # -- Auth wiring ---------------------------------------------------------
    @property
    def host(self):
        return self._host

    def abort(self):
        """Permanently unblock this client's gate/backoff waits (plugin shutdown
        or a prefs rebuild superseding this client). Waiting threads raise a
        :class:`HomeConnectError` instead of sleeping out the Retry-After."""
        self._abort.set()

    def gate_wait_remaining(self):
        """Seconds until the request gate opens (0.0 when it is open now).

        Lets UI-path callers refuse fast ("rate limited — try later") instead of
        blocking an Indigo thread behind a Retry-After that can run to hours."""
        return max(0.0, self._earliest_retry - self._monotonic())

    def set_token_provider(self, provider):
        """``provider()`` returns the current bearer access token (or ``None``)."""
        self._token_provider = provider

    def set_unauthorized_handler(self, handler):
        """``handler(used_token)`` is called on a 401; return True to retry once."""
        self._on_unauthorized = handler

    # -- Public request helpers ---------------------------------------------
    def get_json(self, path, authorize=True, accept_language=DEFAULT_ACCEPT_LANGUAGE):
        """GET + parse JSON. ``accept_language`` sets the ``Accept-Language`` header
        so BSH returns localized display ``name``s (menus show the app's names, not
        raw key tails); pass ``None`` to omit it."""
        headers = {"Accept-Language": accept_language} if accept_language else None
        raw = self.request("GET", path, headers=headers, accept=HC_CONTENT_TYPE, authorize=authorize)
        return self._parse_json(raw, path)

    def put_json(self, path, payload, authorize=True, no_retry=False):
        """PUT a JSON body. ``no_retry`` disables the idempotent retry loop for
        calls that must never be re-sent on a timeout — the program-start PUT,
        where a retried request could double-start an appliance (PRD §3.3). A 401
        is still refreshed-and-resent once even under ``no_retry``: a 401 is
        provably-not-executed, so resending after a token refresh cannot
        double-start."""
        body = json.dumps(payload)
        raw = self.request("PUT", path, headers={"Content-Type": HC_CONTENT_TYPE},
                           body=body, accept=HC_CONTENT_TYPE, authorize=authorize, no_retry=no_retry)
        return raw

    def post_form(self, path, form, authorize=False):
        """POST an ``x-www-form-urlencoded`` body (used for all OAuth token calls)."""
        body = urllib.parse.urlencode(form)
        raw = self.request("POST", path,
                           headers={"Content-Type": FORM_CONTENT_TYPE},
                           body=body, accept="application/json", authorize=authorize)
        return self._parse_json(raw, path)

    # -- Core request loop ---------------------------------------------------
    def request(self, method, path, *, headers=None, body=None, accept=HC_CONTENT_TYPE,
                authorize=True, on_unauthorized=None, no_retry=False):
        method = method.upper()
        # ``no_retry`` forces a single attempt even for idempotent methods so a
        # program-start PUT is never re-sent after a lost response (PRD §3.3).
        retryable = method in IDEMPOTENT_METHODS and not no_retry
        attempt = 0
        unauthorized_retried = False
        while True:
            self._wait_for_gate()
            try:
                raw = self._perform(method, path, headers, body, accept, authorize)
            except HomeConnectError:
                if self._can_retry(None, retryable, attempt):
                    self._backoff_before_retry(attempt)
                    attempt += 1
                    continue
                raise

            if 200 <= raw.status < 300 or raw.status == 302:
                return raw

            if raw.status == 429:
                self._apply_retry_after(raw)
            err = self._build_error(raw, method, path)

            # A 401 is provably-not-executed (the server rejected the request
            # before acting), so one refresh-and-resend is safe even for a
            # no_retry program start — this branch is independent of ``retryable``.
            if raw.status == 401 and not unauthorized_retried:
                handler = on_unauthorized or self._on_unauthorized
                if handler and handler(self._last_used_token):
                    unauthorized_retried = True
                    continue

            if self._can_retry(raw.status, retryable, attempt):
                if raw.status != 429:         # a 429 retry waits at the gate instead
                    self._backoff_before_retry(attempt)
                attempt += 1
                continue
            raise err

    # -- Streaming (SSE) -----------------------------------------------------
    def open_stream(self, path, *, accept=SSE_CONTENT_TYPE, read_timeout=STREAM_READ_TIMEOUT,
                    authorize=True, abort_check=None):
        """Open a long-lived streaming GET and return a :class:`StreamResponse`.

        Goes through the shared request gate (so a 429 ``Retry-After`` from a
        failed open pushes the gate for *all* requests) and counts one request
        against the daily budget. A 401 is routed through the same unauthorized
        handler as :meth:`request` — one forced token refresh, then a single
        retry — so a reconnect loop never re-opens forever with a dead token.
        Non-2xx responses (after that single retry) raise a
        :class:`HomeConnectError`; the caller (``hc_events``) runs the reconnect
        loop.
        """
        unauthorized_retried = False
        while True:
            self._wait_for_gate(abort_check)
            conn = self._connection_factory(self._host, read_timeout)
            req_headers = {"Accept": accept, "User-Agent": self._user_agent,
                           "Cache-Control": "no-cache"}
            if authorize and self._token_provider:
                token = self._token_provider()
                self._last_used_token = token
                if token:
                    req_headers["Authorization"] = f"Bearer {token}"
            else:
                self._last_used_token = None

            self._count_request()
            try:
                conn.request("GET", path, headers=req_headers)
                resp = conn.getresponse()
            except (OSError, http.client.HTTPException) as exc:
                self._safe_close(conn)
                raise HomeConnectError(
                    f"transport error opening stream {redact_path(path)}: {exc}") from exc

            if 200 <= resp.status < 300:
                return StreamResponse(conn, resp, path, self._logger)

            # Failed to open: drain a small error body, honor Retry-After.
            headers = {k.lower(): v for k, v in resp.getheaders()}
            try:
                body = resp.read()
            except (OSError, http.client.HTTPException):
                body = b""
            self._safe_close(conn)
            raw = RawResponse(resp.status, headers, body)
            if resp.status == 429:
                self._apply_retry_after(raw)
            err = self._build_error(raw, "GET", path)

            if resp.status == 401 and not unauthorized_retried:
                handler = self._on_unauthorized
                if handler and handler(self._last_used_token):
                    unauthorized_retried = True
                    continue                  # refreshed token -> retry the open once
            raise err

    @staticmethod
    def _safe_close(conn):
        try:
            conn.close()
        except Exception:  # pylint: disable=broad-except
            pass

    # -- Internals -----------------------------------------------------------
    def _perform(self, method, path, headers, body, accept, authorize):
        conn = self._connection_factory(self._host, self._timeout)
        req_headers = {"Accept": accept, "User-Agent": self._user_agent}
        if headers:
            req_headers.update(headers)
        if authorize and self._token_provider:
            token = self._token_provider()
            self._last_used_token = token
            if token:
                req_headers["Authorization"] = f"Bearer {token}"
        else:
            self._last_used_token = None

        self._count_request()
        try:
            conn.request(method, path, body=body, headers=req_headers)
            resp = conn.getresponse()
            status = resp.status
            resp_headers = {k.lower(): v for k, v in resp.getheaders()}
            resp_body = resp.read()
        except (OSError, http.client.HTTPException) as exc:
            raise HomeConnectError(f"transport error on {method} {redact_path(path)}: {exc}") from exc
        finally:
            try:
                conn.close()
            except Exception:  # pylint: disable=broad-except
                pass
        return RawResponse(status, resp_headers, resp_body)

    def _wait_for_gate(self, abort_check=None):
        try:
            while True:
                delay = self._earliest_retry - self._monotonic()
                if delay <= 0:
                    break
                if self._abort.is_set() or (abort_check is not None and abort_check()):
                    raise HomeConnectError(
                        f"request abandoned while rate-limited ({delay:.0f}s of Retry-After remaining)")
                if not self._gate_logged:
                    self._logger.warning("Home Connect rate limit: waiting %.1fs before next request",
                                         delay)
                    self._gate_logged = True
                if self._sleep is not None:
                    self._sleep(delay)        # injected test clock jumps the whole delay
                else:
                    self._abort.wait(min(delay, _GATE_WAIT_SLICE))
        finally:
            # Reset on the abandoned path too: an abort_check exit (stream stop)
            # must not permanently suppress the warning for this client.
            self._gate_logged = False

    def _backoff_before_retry(self, attempt):
        """Wait (interruptibly) before a transport/5xx retry so a struggling
        endpoint is never hit back-to-back."""
        delay = RETRY_BACKOFF_BASE * (2 ** attempt)
        if self._sleep is not None:
            self._sleep(delay)
        else:
            self._abort.wait(delay)
        if self._abort.is_set():
            raise HomeConnectError("request abandoned during retry backoff")

    def _apply_retry_after(self, raw):
        seconds = self._parse_retry_after(raw)
        if seconds is None:
            seconds = DEFAULT_RETRY_AFTER     # headerless 429 (non-time-based limit)
        self._earliest_retry = max(self._earliest_retry, self._monotonic() + seconds)

    @staticmethod
    def _parse_retry_after(raw):
        value = raw.header("retry-after")
        if value is None:
            return None
        try:
            return max(0, int(float(value)))
        except (TypeError, ValueError):
            return None

    def _can_retry(self, status, retryable, attempt):
        if attempt >= self._max_retries:
            return False
        if not retryable:
            return False
        if status is None:            # transport-level failure
            return True
        if status in NO_RETRY_STATUS:
            return False
        if status == 429 or status == 408 or 500 <= status < 600:
            return True
        return False

    def _count_request(self):
        today = self._wall_now().date()
        if today != self._counter_date:
            self._counter_date = today
            self._request_count = 0
            self._budget_warned = False
        self._request_count += 1
        self._logger.debug("Home Connect request %d/%d today", self._request_count, DAILY_BUDGET)
        if self._request_count >= BUDGET_WARN_AT and not self._budget_warned:
            self._logger.warning("Home Connect request budget high: %d/%d today",
                                 self._request_count, DAILY_BUDGET)
            self._budget_warned = True

    def _build_error(self, raw, method, path):
        key = None
        description = None
        text = raw.text()
        ctype = raw.header("content-type", "") or ""
        looks_json = "json" in ctype or text.strip().startswith("{")
        if looks_json:
            try:
                data = json.loads(text)
            except ValueError:
                data = None
            if isinstance(data, dict):
                err = data.get("error")
                if isinstance(err, dict):                 # {"error": {"key", "description"}}
                    key = err.get("key")
                    description = err.get("description")
                elif isinstance(err, str):                # OAuth {"error", "error_description"}
                    key = err
                    description = data.get("error_description")
        message = f"HTTP {raw.status} on {method} {redact_path(path)}"
        if key:
            message += f" [{key}]"
        if description:
            message += f": {description}"
        return HomeConnectError(message, status=raw.status, key=key, description=description,
                                retry_after=self._parse_retry_after(raw))

    def _parse_json(self, raw, path):
        text = raw.text()
        try:
            return json.loads(text)
        except ValueError as exc:
            raise HomeConnectError(
                f"invalid JSON response on {redact_path(path)}: {exc}", status=raw.status) from exc
