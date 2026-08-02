"""Unit tests for hc_control.py — guard rails, rate limiters, option coercion,
payload shapes, capability-cache menus and the one-shot start watch.

Every guard-rail test asserts NOT ONLY that a bad call raises ControlRefused but
that no HTTP request was made (the "don't make the request" firewall, PRD §3.4).
The controller is driven through a real HomeConnectAPI + ScriptedTransport so the
request gate / no-retry paths are exercised, not mocked away.
"""
import json
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

import hc_control
from hc_api import HomeConnectAPI, HomeConnectError
from hc_appliance import (HomeConnectAppliance, LOCAL_CONTROL, OPERATION_STATE,
                          REMOTE_CONTROL, REMOTE_START)
from hc_cache import DiskCache
from hc_control import (Controller, ControlRefused, RateLimiter, StartWatch,
                        POWER_STATE_KEY, PAUSE_COMMAND, RESUME_COMMAND, OPEN_DOOR_COMMAND)
from support import Clock, RecordingScheduler, ScriptedTransport

HC_JSON = {"content-type": "application/vnd.bsh.sdk.v1+json"}
HAID = "BOSCH-HCS01-0123456789AB"


def _op(state):
    return f"BSH.Common.EnumType.OperationState.{state}"


def _power(state):
    return f"BSH.Common.EnumType.PowerState.{state}"


def make_appliance(connected=True, op="Ready", remote_control=True, remote_start=True,
                   local_control=False, power="On", name="Dishwasher", hc_type="Dishwasher"):
    appliance = HomeConnectAppliance(
        HAID, {"name": name, "type": hc_type, "connected": connected},
        Mock(), Mock(), logger=Mock())
    items = []
    if op is not None:
        items.append({"key": OPERATION_STATE, "value": _op(op)})
    if remote_control is not None:
        items.append({"key": REMOTE_CONTROL, "value": remote_control})
    if remote_start is not None:
        items.append({"key": REMOTE_START, "value": remote_start})
    if local_control is not None:
        items.append({"key": LOCAL_CONTROL, "value": local_control})
    if power is not None:
        items.append({"key": POWER_STATE_KEY, "value": _power(power)})
    appliance.merge_items(items)
    return appliance


def make_controller(transport, tmp_path, now=None):
    clock = Clock()
    api = HomeConnectAPI(
        host="api.home-connect.com", logger=Mock(), connection_factory=transport.factory,
        monotonic=clock.monotonic, sleep=clock.sleep,
        wall_now=lambda: datetime(2026, 8, 2, tzinfo=timezone.utc))
    cache = DiskCache(str(tmp_path / "cache.json"), "test", logger=Mock())
    controller = Controller(api, cache, logger=Mock(), now=now or Clock().monotonic)
    return controller


def puts(transport):
    return [r for r in transport.requests if r["method"] == "PUT"]


# ---------------------------------------------------------------------------
# Option coercion + parsing
# ---------------------------------------------------------------------------
def test_coerce_bool_int_string():
    assert hc_control.coerce_value("true") is True
    assert hc_control.coerce_value("FALSE") is False
    assert hc_control.coerce_value("3600") == 3600
    assert hc_control.coerce_value("-5") == -5
    assert hc_control.coerce_value("Eco50") == "Eco50"
    assert hc_control.coerce_value(" 40.5 ") == "40.5"    # no float coercion (spec: Int/Bool/String)


def test_parse_options_multiline_and_comments():
    text = "BSH.Common.Option.StartInRelative=3600\n# a comment\n\nFoo.Bar=true\nBaz=hello"
    assert hc_control.parse_options(text) == [
        {"key": "BSH.Common.Option.StartInRelative", "value": 3600},
        {"key": "Foo.Bar", "value": True},
        {"key": "Baz", "value": "hello"},
    ]


def test_parse_options_rejects_malformed_line():
    with pytest.raises(ControlRefused):
        hc_control.parse_options("no-equals-here")
    with pytest.raises(ControlRefused):
        hc_control.parse_options("=novalue")


def test_parse_options_empty():
    assert hc_control.parse_options("") == []
    assert hc_control.parse_options(None) == []


# ---------------------------------------------------------------------------
# Rate limiter: window + slide
# ---------------------------------------------------------------------------
def test_rate_limiter_refuses_sixth_within_window():
    clock = Clock()
    limiter = RateLimiter(5, "Program start", now=clock.monotonic)
    for _ in range(5):
        limiter.acquire()
    with pytest.raises(ControlRefused):
        limiter.acquire()


def test_rate_limiter_window_slides():
    clock = Clock()
    limiter = RateLimiter(5, "Program start", window=60.0, now=clock.monotonic)
    for _ in range(5):
        limiter.acquire()
    clock.t += 61                       # whole window elapses
    limiter.acquire()                   # now allowed again — no raise


# ---------------------------------------------------------------------------
# Start guard rails — each precondition refuses with NO HTTP
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("kwargs", [
    {"connected": False},
    {"remote_control": False},
    {"remote_start": False},
    {"local_control": True},
    {"op": "Run"},                      # not Ready
    {"op": None},                       # unknown -> refuse
    {"remote_control": None},           # unknown -> refuse (strict pre-flight)
])
def test_start_refused_locally_makes_no_http(kwargs, tmp_path):
    transport = ScriptedTransport()
    controller = make_controller(transport, tmp_path)
    appliance = make_appliance(**kwargs)
    with pytest.raises(ControlRefused):
        controller.start_program(appliance, "BSH.Common.Program.Auto")
    assert transport.requests == []     # firewall: nothing was sent


def test_start_success_payload_and_no_retry(tmp_path):
    transport = ScriptedTransport()
    transport.queue(204, HC_JSON, b"")
    controller = make_controller(transport, tmp_path)
    appliance = make_appliance()        # ready + remote start allowed
    controller.start_program(appliance, "Dishcare.Dishwasher.Program.Auto2",
                             [{"key": "Foo", "value": 1}])
    sent = puts(transport)
    assert len(sent) == 1
    assert sent[0]["url"] == f"/api/homeappliances/{HAID}/programs/active"
    body = json.loads(sent[0]["body"])
    # data.key is the program key for a program PUT; options passed through.
    assert body["data"]["key"] == "Dishcare.Dishwasher.Program.Auto2"
    assert body["data"]["options"] == [{"key": "Foo", "value": 1}]


def test_start_program_put_not_retried_on_transport_error(tmp_path):
    transport = ScriptedTransport()
    transport.queue_exception(OSError("connection reset"))   # a single failure...
    controller = make_controller(transport, tmp_path)
    appliance = make_appliance()
    with pytest.raises(HomeConnectError):
        controller.start_program(appliance, "BSH.Common.Program.Auto")
    assert len(transport.requests) == 1                      # ...NOT retried (no double-start)


def test_sixth_start_refused_before_http(tmp_path):
    transport = ScriptedTransport()
    for _ in range(5):
        transport.queue(204, HC_JSON, b"")
    controller = make_controller(transport, tmp_path, now=lambda: 1000.0)   # frozen clock
    for _ in range(5):
        controller.start_program(make_appliance(), "BSH.Common.Program.Auto")
    before = len(transport.requests)
    with pytest.raises(ControlRefused):
        controller.start_program(make_appliance(), "BSH.Common.Program.Auto")
    assert len(transport.requests) == before                 # 6th never hit the wire


# ---------------------------------------------------------------------------
# Stop guard rails + state gating
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("state", ["DelayedStart", "Run", "Pause", "ActionRequired"])
def test_stop_allowed_from_stoppable_states(state, tmp_path):
    transport = ScriptedTransport()
    transport.queue(204, HC_JSON, b"")
    controller = make_controller(transport, tmp_path)
    controller.stop_program(make_appliance(op=state))
    assert [r["method"] for r in transport.requests] == ["DELETE"]


@pytest.mark.parametrize("state", ["Ready", "Finished", "Inactive", None])
def test_stop_refused_from_non_stoppable_states(state, tmp_path):
    transport = ScriptedTransport()
    controller = make_controller(transport, tmp_path)
    with pytest.raises(ControlRefused):
        controller.stop_program(make_appliance(op=state))
    assert transport.requests == []


def test_sixth_stop_refused_before_http(tmp_path):
    transport = ScriptedTransport()
    for _ in range(5):
        transport.queue(204, HC_JSON, b"")
    controller = make_controller(transport, tmp_path, now=lambda: 500.0)
    for _ in range(5):
        controller.stop_program(make_appliance(op="Run"))
    before = len(transport.requests)
    with pytest.raises(ControlRefused):
        controller.stop_program(make_appliance(op="Run"))
    assert len(transport.requests) == before


# ---------------------------------------------------------------------------
# Command capability gating (pause / resume / send)
# ---------------------------------------------------------------------------
def _queue_commands(transport, *keys):
    body = json.dumps({"data": {"commands": [{"key": k} for k in keys]}})
    transport.queue(200, HC_JSON, body)


def test_pause_refused_when_commands_unsupported_404(tmp_path):
    transport = ScriptedTransport()
    transport.queue(404, HC_JSON, "{}")            # GET /commands -> unsupported
    controller = make_controller(transport, tmp_path)
    with pytest.raises(ControlRefused):
        controller.pause_program(make_appliance(op="Run"))
    assert puts(transport) == []                   # no PUT command issued


def test_pause_issues_command_when_available(tmp_path):
    transport = ScriptedTransport()
    _queue_commands(transport, PAUSE_COMMAND, RESUME_COMMAND)
    transport.queue(204, HC_JSON, b"")
    controller = make_controller(transport, tmp_path)
    controller.pause_program(make_appliance(op="Run"))
    sent = puts(transport)
    assert sent[0]["url"] == f"/api/homeappliances/{HAID}/commands/{PAUSE_COMMAND}"
    body = json.loads(sent[0]["body"])
    assert body["data"] == {"key": PAUSE_COMMAND, "value": True}    # data.key matches path key


def test_resume_refused_when_only_pause_available(tmp_path):
    transport = ScriptedTransport()
    _queue_commands(transport, PAUSE_COMMAND)      # resume absent
    controller = make_controller(transport, tmp_path)
    with pytest.raises(ControlRefused):
        controller.resume_program(make_appliance(op="Pause"))
    assert puts(transport) == []


def test_send_command_refused_when_disconnected(tmp_path):
    transport = ScriptedTransport()
    controller = make_controller(transport, tmp_path)
    with pytest.raises(ControlRefused):
        controller.send_command(make_appliance(connected=False), OPEN_DOOR_COMMAND)
    assert transport.requests == []                # connection check precedes the /commands read


# ---------------------------------------------------------------------------
# Power-constraint gating (Dryer read-only)
# ---------------------------------------------------------------------------
def _queue_power(transport, *allowed):
    body = json.dumps({"data": {"key": POWER_STATE_KEY, "value": _power("On"),
                                "constraints": {"allowedvalues": [_power(a) for a in allowed]}}})
    transport.queue(200, HC_JSON, body)


def test_dryer_power_off_refused_read_only(tmp_path):
    transport = ScriptedTransport()
    _queue_power(transport, "On")                  # dryer: On only
    controller = make_controller(transport, tmp_path)
    with pytest.raises(ControlRefused):
        controller.set_power(make_appliance(hc_type="Dryer"), _power("Off"))
    assert puts(transport) == []                   # the GET happened, but no PUT


def test_power_off_allowed_when_in_constraints(tmp_path):
    transport = ScriptedTransport()
    _queue_power(transport, "On", "Off")
    transport.queue(204, HC_JSON, b"")
    controller = make_controller(transport, tmp_path)
    controller.set_power(make_appliance(), _power("Off"))
    sent = puts(transport)
    assert sent[0]["url"] == f"/api/homeappliances/{HAID}/settings/{POWER_STATE_KEY}"
    body = json.loads(sent[0]["body"])
    assert body["data"] == {"key": POWER_STATE_KEY, "value": _power("Off")}


# ---------------------------------------------------------------------------
# Select + set_setting payloads
# ---------------------------------------------------------------------------
def test_select_program_when_powered(tmp_path):
    transport = ScriptedTransport()
    transport.queue(204, HC_JSON, b"")
    controller = make_controller(transport, tmp_path)
    controller.select_program(make_appliance(op="Ready", remote_start=False),
                              "Dishcare.Dishwasher.Program.Eco50")
    sent = puts(transport)
    assert sent[0]["url"] == f"/api/homeappliances/{HAID}/programs/selected"


def test_select_refused_when_powered_off(tmp_path):
    transport = ScriptedTransport()
    controller = make_controller(transport, tmp_path)
    with pytest.raises(ControlRefused):
        controller.select_program(make_appliance(power="Off"), "X.Program")
    assert transport.requests == []


def test_set_setting_data_key_matches_path(tmp_path):
    transport = ScriptedTransport()
    transport.queue(204, HC_JSON, b"")
    controller = make_controller(transport, tmp_path)
    controller.set_setting(make_appliance(), "BSH.Common.Setting.ChildLock", True)
    sent = puts(transport)
    assert sent[0]["url"] == f"/api/homeappliances/{HAID}/settings/BSH.Common.Setting.ChildLock"
    body = json.loads(sent[0]["body"])
    assert body["data"] == {"key": "BSH.Common.Setting.ChildLock", "value": True}


# ---------------------------------------------------------------------------
# Capability menus served from cache (no HTTP on the second call)
# ---------------------------------------------------------------------------
def test_available_programs_cached_after_first_fetch(tmp_path):
    transport = ScriptedTransport()
    body = json.dumps({"data": {"programs": [{"key": "P1", "name": "Eco 50"}]}})
    transport.queue(200, HC_JSON, body)
    controller = make_controller(transport, tmp_path)
    appliance = make_appliance()
    first = controller.available_programs(appliance)
    assert first == [{"key": "P1", "name": "Eco 50"}]
    count_after_first = len(transport.requests)
    second = controller.available_programs(appliance)     # from cache: no new HTTP
    assert second == first
    assert len(transport.requests) == count_after_first == 1


def test_available_commands_404_cached_as_empty(tmp_path):
    transport = ScriptedTransport()
    transport.queue(404, HC_JSON, "{}")
    controller = make_controller(transport, tmp_path)
    appliance = make_appliance()
    assert controller.available_commands(appliance) == []
    assert controller.available_commands(appliance) == []
    assert len(transport.requests) == 1                   # cached, no second GET


# ---------------------------------------------------------------------------
# Start watch (observer API + delayed check)
# ---------------------------------------------------------------------------
def test_start_watch_warns_if_still_ready_after_timeout():
    appliance = make_appliance(op="Ready")
    scheduler = RecordingScheduler()
    logger = Mock()
    StartWatch(appliance, scheduler.post, logger=logger, delay=15)
    scheduler.run_all()                                   # fire the timeout while still Ready
    assert logger.warning.called
    assert "did not start" in logger.warning.call_args[0][0]


def test_start_watch_silent_when_program_starts():
    appliance = make_appliance(op="Ready")
    scheduler = RecordingScheduler()
    logger = Mock()
    StartWatch(appliance, scheduler.post, logger=logger, delay=15)
    # SSE reports the appliance moved to Run before the timeout fires:
    appliance.merge_items([{"key": OPERATION_STATE, "value": _op("Run")}])
    scheduler.run_all()
    assert not logger.warning.called
