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
        except (OSError, http.client.HTTPException) as exc:
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
                 monotonic=time.monotonic, sleep=time.sleep,
                 wall_now=lambda: datetime.now(timezone.utc)):
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

    def set_token_provider(self, provider):
        """``provider()`` returns the current bearer access token (or ``None``)."""
        self._token_provider = provider

    def set_unauthorized_handler(self, handler):
        """``handler(used_token)`` is called on a 401; return True to retry once."""
        self._on_unauthorized = handler

    # -- Public request helpers ---------------------------------------------
    def get_json(self, path, authorize=True):
        raw = self.request("GET", path, accept=HC_CONTENT_TYPE, authorize=authorize)
        return self._parse_json(raw, path)

    def put_json(self, path, payload, authorize=True):
        body = json.dumps(payload)
        raw = self.request("PUT", path, headers={"Content-Type": HC_CONTENT_TYPE},
                           body=body, accept=HC_CONTENT_TYPE, authorize=authorize)
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
                authorize=True, on_unauthorized=None):
        method = method.upper()
        idempotent = method in IDEMPOTENT_METHODS
        attempt = 0
        unauthorized_retried = False
        while True:
            self._wait_for_gate()
            try:
                raw = self._perform(method, path, headers, body, accept, authorize)
            except HomeConnectError:
                if self._can_retry(None, idempotent, attempt):
                    attempt += 1
                    continue
                raise

            if 200 <= raw.status < 300 or raw.status == 302:
                return raw

            if raw.status == 429:
                self._apply_retry_after(raw)
            err = self._build_error(raw, method, path)

            if raw.status == 401 and not unauthorized_retried and idempotent:
                handler = on_unauthorized or self._on_unauthorized
                if handler and handler(self._last_used_token):
                    unauthorized_retried = True
                    continue

            if self._can_retry(raw.status, idempotent, attempt):
                attempt += 1
                continue
            raise err

    # -- Streaming (SSE) -----------------------------------------------------
    def open_stream(self, path, *, accept=SSE_CONTENT_TYPE, read_timeout=STREAM_READ_TIMEOUT,
                    authorize=True):
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
            self._wait_for_gate()
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

    def _wait_for_gate(self):
        delay = self._earliest_retry - self._monotonic()
        if delay > 0:
            if not self._gate_logged:
                self._logger.warning("Home Connect rate limit: waiting %.1fs before next request", delay)
                self._gate_logged = True
            self._sleep(delay)
        else:
            self._gate_logged = False

    def _apply_retry_after(self, raw):
        seconds = self._parse_retry_after(raw)
        if seconds is not None:
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

    def _can_retry(self, status, idempotent, attempt):
        if attempt >= self._max_retries:
            return False
        if not idempotent:
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
