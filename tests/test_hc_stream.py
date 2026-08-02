"""Unit tests for hc_api.py streaming seam (open_stream + StreamResponse)."""
import socket
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from hc_api import HomeConnectAPI, HomeConnectError
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
