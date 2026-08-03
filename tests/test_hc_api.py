"""Unit tests for hc_api.py — request gate, retries, budget, error parsing."""
import json
from datetime import date, datetime, timezone
from unittest.mock import Mock

import pytest

import hc_api
from hc_api import HomeConnectAPI, HomeConnectError, redact, redact_path
from support import Clock, ScriptedTransport

HC_JSON = {"content-type": "application/vnd.bsh.sdk.v1+json"}


def make_api(transport, logger=None, clock=None, wall_now=None, max_retries=3):
    clock = clock or Clock()
    return HomeConnectAPI(
        host="api.home-connect.com",
        logger=logger or Mock(),
        connection_factory=transport.factory,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        wall_now=wall_now or (lambda: datetime(2026, 8, 2, tzinfo=timezone.utc)),
        max_retries=max_retries,
    )


# -- Redaction ----------------------------------------------------------------

def test_redact_shows_first4_last8():
    assert redact("ABCDEFGHIJKLMNOPQR") == "ABCD…KLMNOPQR"


def test_redact_short_value_fully_masked():
    assert redact("shorttoken") == "…"
    assert redact("") == ""


def test_redact_path_hides_haid():
    path = "/api/homeappliances/BOSCH-HCS01-0123456789AB/programs/active"
    redacted = redact_path(path)
    assert "0123456789AB" not in redacted or "BOSC…" in redacted
    assert "BOSC…" in redacted


# -- Error envelope parsing ---------------------------------------------------

def test_error_envelope_json_key_description():
    transport = ScriptedTransport()
    body = json.dumps({"error": {"key": "SDK.Error.UnsupportedSetting", "description": "nope"}})
    transport.queue(409, HC_JSON, body)
    api = make_api(transport)
    with pytest.raises(HomeConnectError) as exc:
        api.get_json("/api/homeappliances/x/settings")
    assert exc.value.status == 409
    assert exc.value.key == "SDK.Error.UnsupportedSetting"
    assert exc.value.description == "nope"


def test_error_envelope_non_json_html_body():
    transport = ScriptedTransport()
    transport.queue(503, {"content-type": "text/html"}, "<html>gateway blew up</html>")
    api = make_api(transport, max_retries=0)  # don't retry, just parse+raise
    with pytest.raises(HomeConnectError) as exc:
        api.get_json("/api/homeappliances")
    assert exc.value.status == 503
    assert exc.value.key is None       # HTML body -> no key, must not crash


def test_oauth_style_error_string_parsed():
    transport = ScriptedTransport()
    body = json.dumps({"error": "authorization_pending", "error_description": "waiting"})
    transport.queue(400, {"content-type": "application/json"}, body)
    api = make_api(transport)
    with pytest.raises(HomeConnectError) as exc:
        api.post_form("/security/oauth/token", {"grant_type": "device_code"})
    assert exc.value.key == "authorization_pending"
    assert exc.value.description == "waiting"


# -- Retry policy -------------------------------------------------------------

def test_retry_on_5xx_for_get():
    transport = ScriptedTransport()
    transport.queue(500, HC_JSON, "{}")
    transport.queue(200, HC_JSON, json.dumps({"data": {"ok": True}}))
    api = make_api(transport)
    result = api.get_json("/api/homeappliances")
    assert result == {"data": {"ok": True}}
    assert len(transport.requests) == 2


def test_never_retry_on_409():
    transport = ScriptedTransport()
    transport.queue(409, HC_JSON, json.dumps({"error": {"key": "WrongOperationState"}}))
    api = make_api(transport)
    with pytest.raises(HomeConnectError):
        api.request("GET", "/api/homeappliances/x/programs/active")
    assert len(transport.requests) == 1     # not retried


def test_non_idempotent_post_not_retried_on_5xx():
    transport = ScriptedTransport()
    transport.queue(500, HC_JSON, "{}")
    api = make_api(transport)
    with pytest.raises(HomeConnectError):
        api.post_form("/security/oauth/token", {"grant_type": "refresh_token"})
    assert len(transport.requests) == 1


# -- Rate-limit gate ----------------------------------------------------------

def test_request_gate_blocks_during_retry_after_window():
    transport = ScriptedTransport()
    transport.queue(429, {"content-type": "application/json", "retry-after": "30"}, "{}")
    transport.queue(200, HC_JSON, json.dumps({"data": {}}))
    clock = Clock()
    api = make_api(transport, clock=clock)
    result = api.get_json("/api/homeappliances")
    assert result == {"data": {}}
    # The 429 pushed the gate forward 30s; the retry waited exactly that long.
    assert clock.slept == [30]


def test_gate_countdown_logged_once():
    transport = ScriptedTransport()
    transport.queue(429, {"content-type": "application/json", "retry-after": "10"}, "{}")
    transport.queue(200, HC_JSON, "{}")
    logger = Mock()
    api = make_api(transport, logger=logger)
    api.get_json("/api/homeappliances")
    rate_warnings = [c for c in logger.warning.call_args_list if "rate limit" in str(c)]
    assert len(rate_warnings) == 1


# -- Daily budget counter -----------------------------------------------------

def test_daily_counter_warns_once_at_threshold(monkeypatch):
    monkeypatch.setattr(hc_api, "BUDGET_WARN_AT", 3)
    transport = ScriptedTransport()
    for _ in range(3):
        transport.queue(200, HC_JSON, "{}")
    logger = Mock()
    api = make_api(transport, logger=logger)
    for _ in range(3):
        api.get_json("/api/homeappliances")
    budget_warnings = [c for c in logger.warning.call_args_list if "budget" in str(c)]
    assert len(budget_warnings) == 1


def test_daily_counter_resets_at_midnight_utc():
    transport = ScriptedTransport()
    for _ in range(3):
        transport.queue(200, HC_JSON, "{}")
    days = [date(2026, 8, 2), date(2026, 8, 2), date(2026, 8, 3)]
    calls = {"n": 0}

    def wall_now():
        d = days[min(calls["n"], len(days) - 1)]
        calls["n"] += 1
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)

    api = make_api(transport, wall_now=wall_now)
    api.get_json("/x")
    api.get_json("/x")
    assert api._request_count == 2      # pylint: disable=protected-access
    api.get_json("/x")                  # new UTC day
    assert api._request_count == 1      # pylint: disable=protected-access


# -- Transport errors ---------------------------------------------------------

def test_transport_error_retried_for_get():
    transport = ScriptedTransport()
    transport.queue_exception(OSError("connection reset"))
    transport.queue(200, HC_JSON, json.dumps({"data": {}}))
    api = make_api(transport)
    assert api.get_json("/api/homeappliances") == {"data": {}}
    assert len(transport.requests) == 2


# -- no_retry (program-start double-start guard, PRD §3.3) --------------------

def test_put_no_retry_not_retried_on_transport_error():
    transport = ScriptedTransport()
    transport.queue_exception(OSError("timed out"))
    api = make_api(transport)
    with pytest.raises(HomeConnectError):
        api.put_json("/api/homeappliances/x/programs/active", {"data": {}}, no_retry=True)
    assert len(transport.requests) == 1     # a lost response must NOT re-send the start


def test_put_no_retry_not_retried_on_5xx():
    transport = ScriptedTransport()
    transport.queue(500, HC_JSON, "{}")
    api = make_api(transport)
    with pytest.raises(HomeConnectError):
        api.put_json("/api/homeappliances/x/programs/active", {"data": {}}, no_retry=True)
    assert len(transport.requests) == 1


def test_put_retried_by_default_on_5xx():
    transport = ScriptedTransport()
    transport.queue(500, HC_JSON, "{}")
    transport.queue(204, HC_JSON, b"")
    api = make_api(transport)
    api.put_json("/api/homeappliances/x/settings/y", {"data": {}})   # idempotent -> retried
    assert len(transport.requests) == 2


def test_no_retry_start_refreshes_and_resends_on_401():
    # A 401 is provably-not-executed, so refresh+resend is safe even for a
    # no_retry program start — the double-start guard must NOT block it.
    transport = ScriptedTransport()
    transport.queue(401, {"content-type": "application/json"},
                    json.dumps({"error": "invalid_token"}))
    transport.queue(204, HC_JSON, b"")
    api = make_api(transport)
    refreshes = {"n": 0}

    def handler(_used_token):
        refreshes["n"] += 1
        return True                         # "token refreshed" -> retry the start once

    api.set_unauthorized_handler(handler)
    api.put_json("/api/homeappliances/x/programs/active", {"data": {}}, no_retry=True)
    assert refreshes["n"] == 1
    assert len(transport.requests) == 2     # one 401, one resend — success


def test_get_json_sends_accept_language_header():
    transport = ScriptedTransport()
    transport.queue(200, HC_JSON, json.dumps({"data": {}}))
    api = make_api(transport)
    api.get_json("/api/homeappliances/x/programs")
    headers = transport.requests[0]["headers"]
    assert headers.get("Accept-Language") == "en-GB"       # localized display names


def test_get_json_omits_accept_language_when_none():
    transport = ScriptedTransport()
    transport.queue(200, HC_JSON, json.dumps({"data": {}}))
    api = make_api(transport)
    api.get_json("/api/homeappliances", accept_language=None)
    assert "Accept-Language" not in transport.requests[0]["headers"]


def test_no_retry_start_401_without_handler_raises_once():
    transport = ScriptedTransport()
    transport.queue(401, {"content-type": "application/json"}, json.dumps({"error": "x"}))
    api = make_api(transport)
    with pytest.raises(HomeConnectError):
        api.put_json("/api/homeappliances/x/programs/active", {"data": {}}, no_retry=True)
    assert len(transport.requests) == 1     # no handler -> no resend


# -- Red-team wave 1 (#8 gate abort, #11 headerless 429, #13 retry backoff) ---

def test_headerless_429_applies_default_gate():
    # BSH's non-time-based limits return 429 with NO Retry-After; a synthetic
    # gate delay must still apply or an error loop hammers the API.
    transport = ScriptedTransport()
    transport.queue(429, {"content-type": "application/json"}, "{}")
    transport.queue(200, HC_JSON, json.dumps({"data": {}}))
    clock = Clock()
    api = make_api(transport, clock=clock)
    api.get_json("/api/homeappliances")
    assert hc_api.DEFAULT_RETRY_AFTER in clock.slept


def test_abort_raises_instead_of_waiting_out_gate():
    transport = ScriptedTransport()
    transport.queue(429, {"content-type": "application/json", "retry-after": "3600"}, "{}")
    clock = Clock()
    api = make_api(transport, clock=clock, max_retries=0)
    with pytest.raises(HomeConnectError):
        api.get_json("/api/homeappliances")          # sets the gate 1h out
    api.abort()
    with pytest.raises(HomeConnectError) as exc:
        api.get_json("/api/homeappliances")          # must NOT sleep 3600s
    assert "abandoned" in str(exc.value)
    assert 3600 not in clock.slept


def test_gate_wait_remaining_reports_window():
    transport = ScriptedTransport()
    transport.queue(429, {"content-type": "application/json", "retry-after": "300"}, "{}")
    clock = Clock()
    api = make_api(transport, clock=clock, max_retries=0)
    assert api.gate_wait_remaining() == 0.0
    with pytest.raises(HomeConnectError):
        api.get_json("/api/homeappliances")
    assert api.gate_wait_remaining() == 300.0


def test_retry_backoff_between_5xx_attempts():
    # Back-to-back retries feed the 10-successive-errors block: each retry must
    # wait, doubling per attempt.
    transport = ScriptedTransport()
    transport.queue(500, HC_JSON, "{}")
    transport.queue(500, HC_JSON, "{}")
    transport.queue(200, HC_JSON, json.dumps({"data": {}}))
    clock = Clock()
    api = make_api(transport, clock=clock)
    api.get_json("/api/homeappliances")
    assert clock.slept == [2.0, 4.0]
    assert len(transport.requests) == 3
