"""Shared test doubles for the Home Connect plugin unit tests.

Provides a scripted HTTP transport for :class:`HomeConnectAPI` (drives the real
request gate / retry code) and a lightweight fake API for :class:`HomeConnectAuth`
(exercises the auth logic without HTTP). No network, no ``indigo``.
"""
from collections import deque

from hc_api import HomeConnectError


class FakeHTTPResponse:
    """Mimics ``http.client.HTTPResponse`` enough for ``HomeConnectAPI._perform``."""

    def __init__(self, status, headers=None, body=b""):
        self.status = status
        self._headers = headers or {}
        self._body = body if isinstance(body, bytes) else body.encode("utf-8")

    def getheaders(self):
        return list(self._headers.items())

    def read(self):
        return self._body


class FakeStreamHTTPResponse:
    """Mimics a streaming ``http.client.HTTPResponse`` for ``open_stream``.

    ``read(n)`` pops the next queued chunk; a queued ``Exception`` is raised (to
    simulate a socket read timeout), and an exhausted queue returns ``b""`` (the
    server closing the stream).
    """

    def __init__(self, status, headers=None, chunks=()):
        self.status = status
        self._headers = headers or {}
        self._chunks = deque(chunks)

    def getheaders(self):
        return list(self._headers.items())

    def read(self, amt=None):  # noqa: ARG002 - amt ignored, chunks are pre-sized
        if not self._chunks:
            return b""
        item = self._chunks.popleft()
        if isinstance(item, Exception):
            raise item
        return item if isinstance(item, bytes) else item.encode("utf-8")


class ScriptedTransport:
    """A ``connection_factory`` that serves queued responses and records requests."""

    def __init__(self):
        self.responses = deque()
        self.requests = []
        self.closed = 0

    def queue(self, status, headers=None, body=b""):
        self.responses.append(FakeHTTPResponse(status, headers, body))
        return self

    def queue_exception(self, exc):
        self.responses.append(exc)
        return self

    def queue_stream(self, status, headers=None, chunks=()):
        self.responses.append(FakeStreamHTTPResponse(status, headers, chunks))
        return self

    def factory(self, host, timeout):
        return _ScriptedConnection(self, host)


class _ScriptedConnection:
    def __init__(self, transport, host):
        self._transport = transport
        self._host = host

    def request(self, method, url, body=None, headers=None):
        self._transport.requests.append({
            "method": method, "url": url, "body": body,
            "headers": headers, "host": self._host,
        })

    def getresponse(self):
        item = self._transport.responses.popleft()
        if isinstance(item, Exception):
            raise item
        return item

    def close(self):
        self._transport.closed += 1


class Clock:
    """Controllable monotonic clock; ``sleep`` advances it and records the delay."""

    def __init__(self, start=0.0):
        self.t = start
        self.slept = []

    def monotonic(self):
        return self.t

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.t += seconds


class FakeRaw:
    """Stand-in for :class:`hc_api.RawResponse` used by the simulator auth path."""

    def __init__(self, status, headers=None):
        self.status = status
        self._headers = {k.lower(): v for k, v in (headers or {}).items()}

    def header(self, name, default=None):
        return self._headers.get(name.lower(), default)


class FakeAPI:
    """Records token POSTs / GETs for :class:`HomeConnectAuth`; returns or raises
    scripted items (a ``dict`` is returned, an ``Exception`` is raised)."""

    def __init__(self):
        self.post_calls = []
        self.post_results = deque()
        self.request_results = deque()
        self.token_provider = None
        self.unauthorized_handler = None
        self.gate_wait = 0.0            # settable: simulates a closed request gate

    def gate_wait_remaining(self):
        return self.gate_wait

    def queue_post(self, item):
        self.post_results.append(item)
        return self

    def queue_request(self, item):
        self.request_results.append(item)
        return self

    def post_form(self, path, form, authorize=False):
        self.post_calls.append({"path": path, "form": dict(form), "authorize": authorize})
        item = self.post_results.popleft()
        if isinstance(item, Exception):
            raise item
        return item

    def request(self, method, path, accept=None, authorize=False):
        item = self.request_results.popleft()
        if isinstance(item, Exception):
            raise item
        return item

    def set_token_provider(self, provider):
        self.token_provider = provider

    def set_unauthorized_handler(self, handler):
        self.unauthorized_handler = handler


def oauth_error(key, status=400, retry_after=None, description=None):
    return HomeConnectError(f"HTTP {status} [{key}]", status=status, key=key,
                            description=description, retry_after=retry_after)


class RecordingScheduler:
    """Fake ``schedule(fn, delay)`` for appliance/coordinator tests.

    ``post`` records ``(fn, delay)`` without running it; the test drives
    execution with :meth:`run_next` / :meth:`run_all`, so re-read backoff and
    abandonment are fully controllable.
    """

    def __init__(self):
        self.jobs = deque()
        self.history = []

    def post(self, fn, delay=0):
        self.jobs.append((fn, delay))
        self.history.append(delay)

    def run_next(self):
        fn, _ = self.jobs.popleft()
        fn()

    def run_all(self, limit=100):
        count = 0
        while self.jobs and count < limit:
            self.run_next()
            count += 1
        return count


class FakeReader:
    """Scripted appliance reader for :class:`HomeConnectAppliance` tests.

    Each ``get_*`` returns the queued value or raises a queued
    :class:`HomeConnectError`, recording the call order in ``calls``.
    """

    def __init__(self):
        self.calls = []
        self._queues = {}

    def queue(self, name, *items):
        self._queues.setdefault(name, deque()).extend(items)
        return self

    def _next(self, name, default):
        self.calls.append(name)
        queue = self._queues.get(name)
        if not queue:
            return default
        item = queue.popleft()
        if isinstance(item, Exception):
            raise item
        return item

    def get_appliance(self, haid):  # noqa: ARG002
        return self._next("appliance", {})

    def get_status(self, haid):  # noqa: ARG002
        return self._next("status", [])

    def get_settings(self, haid):  # noqa: ARG002
        return self._next("settings", [])

    def get_selected_program(self, haid):  # noqa: ARG002
        return self._next("selected_program", None)

    def get_active_program(self, haid):  # noqa: ARG002
        return self._next("active_program", None)
