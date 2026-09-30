"""Unit tests for hc_api.py streaming seam (open_stream + StreamResponse)."""
import http.client
import http.server
import socket
import threading
import time
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from hc_api import HomeConnectAPI, HomeConnectError, StreamResponse
from support import Clock, ScriptedTransport

HC_JSON = {"content-type": "application/vnd.bsh.sdk.v1+json"}


def make_api(transport, logger=None, clock=None):
    clock = clock or Clock()
    return HomeConnectAPI(
        host="api.home-connect.com",
        logger=logger or Mock(),
        connection_factory=transport.factory,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        wall_now=lambda: datetime(2026, 8, 2, tzinfo=timezone.utc),
    )


def test_open_stream_returns_lines_split_across_chunks():
    transport = ScriptedTransport()
    # Event framing split awkwardly across chunk boundaries.
    transport.queue_stream(200, HC_JSON, [b"event:STATUS\nda", b"ta:{}\n\n", b""])
    api = make_api(transport)
    stream = api.open_stream("/api/homeappliances/events")
    lines = list(stream.lines())
    assert lines == ["event:STATUS", "data:{}", ""]


def test_open_stream_counts_one_request():
    transport = ScriptedTransport()
    transport.queue_stream(200, HC_JSON, [b""])
    api = make_api(transport)
    api.open_stream("/api/homeappliances/events")
    assert api._request_count == 1  # pylint: disable=protected-access


def test_open_stream_429_pushes_gate_and_raises():
    transport = ScriptedTransport()
    transport.queue_stream(429, {"content-type": "application/json", "retry-after": "30"}, [b"{}"])
    clock = Clock()
    api = make_api(transport, clock=clock)
    with pytest.raises(HomeConnectError) as exc:
        api.open_stream("/api/homeappliances/events")
    assert exc.value.status == 429
    # The gate is now pushed 30s forward for everyone.
    assert api._earliest_retry == pytest.approx(30)  # pylint: disable=protected-access


def test_open_stream_non_2xx_raises_with_key():
    transport = ScriptedTransport()
    import json
    body = json.dumps({"error": {"key": "SDK.Error.Unauthorized"}})
    transport.queue_stream(401, {"content-type": "application/json"}, [body.encode()])
    api = make_api(transport)
    with pytest.raises(HomeConnectError) as exc:
        api.open_stream("/api/homeappliances/events")
    assert exc.value.status == 401


def test_stream_read_timeout_raises_homeconnect_error():
    transport = ScriptedTransport()
    transport.queue_stream(200, HC_JSON, [b"event:STATUS\n", socket.timeout("timed out")])
    api = make_api(transport)
    stream = api.open_stream("/api/homeappliances/events")
    with pytest.raises(HomeConnectError):
        list(stream.lines())


def test_stream_close_is_idempotent_and_closes_connection():
    transport = ScriptedTransport()
    transport.queue_stream(200, HC_JSON, [b""])
    api = make_api(transport)
    stream = api.open_stream("/api/homeappliances/events")
    list(stream.lines())          # exhausting the stream closes it once
    stream.close()                # second close is a no-op
    assert transport.closed == 1


# -- 401 -> forced refresh + single retry (matches request() semantics) -------

def test_open_stream_401_refreshes_then_retries_with_new_token():
    transport = ScriptedTransport()
    transport.queue_stream(401, {"content-type": "application/json"},
                           [b'{"error":{"key":"invalid_token"}}'])
    transport.queue_stream(200, HC_JSON, [b"event:STATUS\ndata:{}\n\n", b""])
    api = make_api(transport)
    tokens = {"cur": "DEAD"}
    api.set_token_provider(lambda: tokens["cur"])
    refreshes = {"n": 0}

    def handler(used_token):
        refreshes["n"] += 1
        tokens["cur"] = "FRESH"       # simulate a successful refresh
        return True                   # token changed -> retry the open once

    api.set_unauthorized_handler(handler)
    stream = api.open_stream("/api/homeappliances/events")
    assert refreshes["n"] == 1        # exactly one refresh attempt
    auth_headers = [r["headers"].get("Authorization") for r in transport.requests]
    assert auth_headers == ["Bearer DEAD", "Bearer FRESH"]
    assert list(stream.lines()) == ["event:STATUS", "data:{}", ""]


def test_open_stream_401_refresh_failure_raises_without_looping():
    transport = ScriptedTransport()
    transport.queue_stream(401, {"content-type": "application/json"}, [b"{}"])
    api = make_api(transport)
    api.set_token_provider(lambda: "DEAD")
    api.set_unauthorized_handler(lambda used_token: False)   # refresh failed
    with pytest.raises(HomeConnectError) as exc:
        api.open_stream("/api/homeappliances/events")
    assert exc.value.status == 401
    assert len(transport.requests) == 1     # not retried -> no reconnect storm


# -- StreamResponse.close() interrupts a blocked recv via socket.shutdown ------

class _FakeSock:
    def __init__(self):
        self.shutdowns = []

    def shutdown(self, how):
        self.shutdowns.append(how)


class _FakeConn:
    def __init__(self):
        self.sock = _FakeSock()
        self.closed = 0

    def close(self):
        self.closed += 1


def test_stream_close_shuts_down_socket_before_close():
    conn = _FakeConn()
    stream = StreamResponse(conn, Mock(), "/api/homeappliances/events", Mock())
    stream.close()
    assert conn.sock.shutdowns == [socket.SHUT_RDWR]
    assert conn.closed == 1
    stream.close()                          # idempotent
    assert conn.closed == 1


def test_request_count_property_reports_today_and_zero_after_day_rollover():
    transport = ScriptedTransport()
    transport.queue_stream(200, HC_JSON, [b""])
    api = make_api(transport)
    assert api.request_count == 0
    api.open_stream("/api/homeappliances/events")
    assert api.request_count == 1
    api._wall_now = lambda: datetime(2026, 8, 3, tzinfo=timezone.utc)  # pylint: disable=protected-access
    assert api.request_count == 0            # stale yesterday count must not read as today's


# -- StreamResponse delivers small frames promptly on a live chunked stream -----

class _OneKeepAliveThenStall(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    stall = threading.Event()

    def do_GET(self):  # noqa: N802 - http.server API
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        frame = b"event:KEEP-ALIVE\ndata:\n\n"
        self.wfile.write(b"%x\r\n%s\r\n" % (len(frame), frame))
        self.wfile.flush()
        self.stall.wait(8)

    def log_message(self, *args):  # noqa: D401
        pass


def test_lines_yields_small_frame_without_waiting_for_a_full_buffer():
    _OneKeepAliveThenStall.stall.clear()
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _OneKeepAliveThenStall)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    conn = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=6)
    stream = None
    try:
        conn.request("GET", "/events")
        stream = StreamResponse(conn, conn.getresponse(), "/events", Mock())
        received = []

        def consume():
            try:
                for line in stream.lines():
                    received.append(line)
            except HomeConnectError:
                pass

        reader = threading.Thread(target=consume, daemon=True)
        reader.start()
        deadline = time.monotonic() + 2.0
        while len(received) < 3 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert received == ["event:KEEP-ALIVE", "data:", ""]
    finally:
        _OneKeepAliveThenStall.stall.set()
        if stream is not None:
            stream.close()
        conn.close()
        server.shutdown()
        server.server_close()


class _ChunkedScript(http.server.BaseHTTPRequestHandler):
    """Sends ``script`` (a list of (bytes, pause_seconds_after)) as separate HTTP chunks."""
    protocol_version = "HTTP/1.1"
    script = []

    def do_GET(self):  # noqa: N802 - http.server API
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        for payload, pause in self.script:
            self.wfile.write(b"%x\r\n%s\r\n" % (len(payload), payload))
            self.wfile.flush()
            time.sleep(pause)
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()

    def log_message(self, *args):  # noqa: D401
        pass


def _read_all_lines_from_real_socket(script):
    _ChunkedScript.script = script
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _ChunkedScript)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    conn = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=6)
    stream = None
    try:
        conn.request("GET", "/events")
        stream = StreamResponse(conn, conn.getresponse(), "/events", Mock())
        return list(stream.lines())
    finally:
        if stream is not None:
            stream.close()
        conn.close()
        server.shutdown()
        server.server_close()


def test_lines_reassembles_a_line_split_across_http_chunks():
    lines = _read_all_lines_from_real_socket([(b"event:KEEP-A", 0.2), (b"LIVE\n\n", 0.0)])
    assert lines == ["event:KEEP-ALIVE", ""]


def test_lines_decodes_a_multibyte_character_split_across_http_chunks():
    lines = _read_all_lines_from_real_socket([(b"data:\xc3", 0.2), (b"\xbc\n", 0.0)])
    assert lines == ["data:\u00fc"]          # "ü", not two replacement characters
