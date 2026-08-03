"""Unit tests for hc_control.py — guard rails, rate limiters, option coercion,
payload shapes, capability-cache menus and the one-shot start watch.

Every guard-rail test asserts NOT ONLY that a bad call raises ControlRefused but
that no HTTP request was made (the "don't make the request" firewall, PRD §3.4).
The controller is driven through a real HomeConnectAPI + ScriptedTransport so the
request gate / no-retry paths are exercised, not mocked away.
"""
import json
import threading
import time
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


def make_controller(transport, tmp_path, now=None, ready_timeout=25.0):
    clock = Clock()
    api = HomeConnectAPI(
        host="api.home-connect.com", logger=Mock(), connection_factory=transport.factory,
        monotonic=clock.monotonic, sleep=clock.sleep,
        wall_now=lambda: datetime(2026, 8, 2, tzinfo=timezone.utc))
    cache = DiskCache(str(tmp_path / "cache.json"), "test", logger=Mock())
    controller = Controller(api, cache, logger=Mock(), now=now or Clock().monotonic,
                            ready_timeout=ready_timeout)
    return controller


def puts(transport):
    return [r for r in transport.requests if r["method"] == "PUT"]


def methods_urls(transport):
    return [(r["method"], r["url"]) for r in transport.requests]


def _wait_until(predicate, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return False


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


@pytest.mark.parametrize("text", [
    "key=",                 # empty value -> refuse (would otherwise send "")
    "key=   ",              # whitespace-only value -> refuse
    "K=1\nK=2",             # duplicate key -> refuse (contract: not last-wins)
])
def test_parse_options_hostile_refused(text):
    with pytest.raises(ControlRefused):
        hc_control.parse_options(text)


def test_parse_options_hostile_coercions():
    # unicode key preserved; huge int -> Python bigint; "+5" -> int 5;
    # "1e3" is not an int literal -> stays string (documented in coerce_value).
    result = hc_control.parse_options("Ключ=1\nBig=999999999999999999999\nPlus=+5\nExp=1e3")
    assert result == [
        {"key": "Ключ", "value": 1},
        {"key": "Big", "value": 999999999999999999999},
        {"key": "Plus", "value": 5},
        {"key": "Exp", "value": "1e3"},
    ]


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


def test_rate_limiter_thread_safe_exactly_limit_succeed():
    import threading
    limiter = RateLimiter(5, "Program start")     # real monotonic clock; all within window
    barrier = threading.Barrier(10)
    results = []
    lock = threading.Lock()

    def worker():
        barrier.wait()                            # release all 10 at once
        try:
            limiter.acquire()
            outcome = True
        except ControlRefused:
            outcome = False
        with lock:
            results.append(outcome)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sum(results) == 5                       # exactly the limit succeed, no over-grant


# ---------------------------------------------------------------------------
# Start guard rails — each precondition refuses with NO HTTP
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("kwargs", [
    {"connected": False},
    {"remote_control": False},
    {"remote_start": False},
    {"local_control": True},
    {"op": "Run"},                      # not Ready
])
def test_start_refused_locally_makes_no_http(kwargs, tmp_path):
    transport = ScriptedTransport()
    controller = make_controller(transport, tmp_path)
    appliance = make_appliance(**kwargs)
    with pytest.raises(ControlRefused):
        controller.start_program(appliance, "BSH.Common.Program.Auto")
    assert transport.requests == []     # firewall: nothing was sent


@pytest.mark.parametrize("kwargs", [
    {"op": None},                       # never reported (e.g. just after restart)
    {"remote_control": None},
    {"remote_start": None},
])
def test_start_with_unknown_state_sends_request(kwargs, tmp_path):
    # UNKNOWN must not refuse (#19): refusing on None told the user to enable a
    # Remote Control setting that was fine. The request goes out; a genuine
    # refusal comes back as a 409 mapped to the actionable hint.
    transport = ScriptedTransport()
    transport.queue(204, HC_JSON, b"")
    controller = make_controller(transport, tmp_path)
    appliance = make_appliance(**kwargs)
    controller.start_program(appliance, "BSH.Common.Program.Auto")
    assert len(puts(transport)) == 1    # sent, appliance stays authoritative


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
def _queue_power(transport, *allowed, access=None):
    constraints = {"allowedvalues": [_power(a) for a in allowed]}
    if access is not None:
        constraints["access"] = access
    body = json.dumps({"data": {"key": POWER_STATE_KEY, "value": _power("On"),
                                "constraints": constraints}})
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
# Auto power-on (dishwasher self-off) — start_program(power_on_first=True)
# ---------------------------------------------------------------------------
def _off_appliance():
    # Off dishwasher: reports PowerState Off / OperationState Inactive, remote
    # flags off (they only mean anything once the appliance is on).
    return make_appliance(power="Off", op="Inactive", remote_control=False, remote_start=False)


def test_auto_power_on_full_sequence(tmp_path):
    transport = ScriptedTransport()
    _queue_power(transport, "Off", "On", access="readWrite")   # GET constraints
    transport.queue(204, HC_JSON, b"")                         # PUT PowerState=On
    transport.queue(204, HC_JSON, b"")                         # PUT programs/active
    controller = make_controller(transport, tmp_path, ready_timeout=2.0)
    appliance = _off_appliance()

    def fire():
        # Simulate the SSE Ready + remote-flags event arriving after power-on.
        _wait_until(lambda: any(r["method"] == "PUT" and "settings" in r["url"]
                                for r in transport.requests))
        appliance.merge_items([
            {"key": POWER_STATE_KEY, "value": _power("On")},
            {"key": REMOTE_CONTROL, "value": True},
            {"key": REMOTE_START, "value": True},
            {"key": OPERATION_STATE, "value": _op("Ready")},
        ])

    thread = threading.Thread(target=fire)
    thread.start()
    controller.start_program(appliance, "Dishcare.Dishwasher.Program.Auto2", power_on_first=True)
    thread.join(timeout=5)

    # Exact sequence: read constraints -> power on -> (Ready) -> start.
    assert methods_urls(transport) == [
        ("GET", f"/api/homeappliances/{HAID}/settings/{POWER_STATE_KEY}"),
        ("PUT", f"/api/homeappliances/{HAID}/settings/{POWER_STATE_KEY}"),
        ("PUT", f"/api/homeappliances/{HAID}/programs/active"),
    ]
    assert json.loads(transport.requests[1]["body"])["data"] == {"key": POWER_STATE_KEY,
                                                                 "value": _power("On")}
    assert json.loads(transport.requests[2]["body"])["data"]["key"] == \
        "Dishcare.Dishwasher.Program.Auto2"
    # The power-on PUT is not a program start: exactly one start-limiter slot used.
    assert len(controller._start_limiter._events) == 1


def test_auto_power_on_ready_timeout_refuses_no_start(tmp_path):
    transport = ScriptedTransport()
    _queue_power(transport, "Off", "On", access="readWrite")
    transport.queue(204, HC_JSON, b"")                         # PUT PowerState=On
    controller = make_controller(transport, tmp_path, ready_timeout=0.1)
    with pytest.raises(ControlRefused) as exc:
        controller.start_program(_off_appliance(), "P", power_on_first=True)   # Ready never arrives
    assert "did not become Ready" in exc.value.reason
    assert methods_urls(transport) == [
        ("GET", f"/api/homeappliances/{HAID}/settings/{POWER_STATE_KEY}"),
        ("PUT", f"/api/homeappliances/{HAID}/settings/{POWER_STATE_KEY}"),
    ]                                                           # NO programs/active


def test_auto_power_on_refused_when_power_read_only(tmp_path):
    transport = ScriptedTransport()
    _queue_power(transport, "Off", "On", access="read")        # power is read-only
    controller = make_controller(transport, tmp_path)
    with pytest.raises(ControlRefused) as exc:
        controller.start_program(_off_appliance(), "P", power_on_first=True)
    assert "cannot be powered on remotely" in exc.value.reason
    assert puts(transport) == []                               # constraints GET only, no PUT


def test_power_on_first_false_gives_normal_refusal_no_power_put(tmp_path):
    transport = ScriptedTransport()
    controller = make_controller(transport, tmp_path)
    # power_on_first defaults False -> the normal guard rails run against the off
    # appliance and refuse (here on Remote Control, off because the appliance is
    # off); crucially no power-on is attempted.
    with pytest.raises(ControlRefused):
        controller.start_program(_off_appliance(), "P")
    assert transport.requests == []                            # no power GET/PUT at all


def test_auto_power_on_power_put_409_surfaced_no_start(tmp_path):
    transport = ScriptedTransport()
    _queue_power(transport, "Off", "On", access="readWrite")
    transport.queue(409, HC_JSON, json.dumps({"error": {"key": "Conflict"}}))   # PUT PowerState
    controller = make_controller(transport, tmp_path)
    with pytest.raises(HomeConnectError):
        controller.start_program(_off_appliance(), "P", power_on_first=True)
    assert methods_urls(transport) == [
        ("GET", f"/api/homeappliances/{HAID}/settings/{POWER_STATE_KEY}"),
        ("PUT", f"/api/homeappliances/{HAID}/settings/{POWER_STATE_KEY}"),
    ]                                                           # 409 on power-on, never started


def test_power_on_first_noop_when_already_on(tmp_path):
    transport = ScriptedTransport()
    transport.queue(204, HC_JSON, b"")                         # only the start PUT
    controller = make_controller(transport, tmp_path)
    controller.start_program(make_appliance(), "P", power_on_first=True)   # already On + Ready
    assert methods_urls(transport) == [("PUT", f"/api/homeappliances/{HAID}/programs/active")]


def test_select_auto_power_on_then_selects(tmp_path):
    transport = ScriptedTransport()
    _queue_power(transport, "Off", "On", access="readWrite")
    transport.queue(204, HC_JSON, b"")                         # PUT PowerState=On
    transport.queue(204, HC_JSON, b"")                         # PUT programs/selected
    controller = make_controller(transport, tmp_path, ready_timeout=2.0)
    appliance = _off_appliance()

    def fire():
        _wait_until(lambda: any(r["method"] == "PUT" and "settings" in r["url"]
                                for r in transport.requests))
        appliance.merge_items([{"key": POWER_STATE_KEY, "value": _power("On")},
                               {"key": OPERATION_STATE, "value": _op("Ready")}])

    thread = threading.Thread(target=fire)
    thread.start()
    controller.select_program(appliance, "P", power_on_first=True)
    thread.join(timeout=5)
    assert methods_urls(transport) == [
        ("GET", f"/api/homeappliances/{HAID}/settings/{POWER_STATE_KEY}"),
        ("PUT", f"/api/homeappliances/{HAID}/settings/{POWER_STATE_KEY}"),
        ("PUT", f"/api/homeappliances/{HAID}/programs/selected"),
    ]


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
def test_all_programs_cached_after_first_fetch(tmp_path):
    transport = ScriptedTransport()
    body = json.dumps({"data": {"programs": [{"key": "P1", "name": "Eco 50"}]}})
    transport.queue(200, HC_JSON, body)
    controller = make_controller(transport, tmp_path)
    appliance = make_appliance()
    first = controller.all_programs(appliance)
    assert first == [{"key": "P1", "name": "Eco 50"}]
    # Built from GET /programs (the all-programs list), not /programs/available.
    assert transport.requests[0]["url"] == f"/api/homeappliances/{HAID}/programs"
    second = controller.all_programs(appliance)           # from cache: no new HTTP
    assert second == first
    assert len(transport.requests) == 1


def test_all_programs_distinct_cache_key(tmp_path):
    # The all-programs fetch must not collide with a same-haId power/commands entry.
    transport = ScriptedTransport()
    transport.queue(200, HC_JSON, json.dumps({"data": {"programs": [{"key": "P1"}]}}))
    _queue_commands(transport, PAUSE_COMMAND)
    controller = make_controller(transport, tmp_path)
    appliance = make_appliance()
    assert controller.all_programs(appliance) == [{"key": "P1"}]
    assert {c["key"] for c in controller.available_commands(appliance)} == {PAUSE_COMMAND}
    assert len(transport.requests) == 2                   # two distinct keys, two fetches


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
def test_start_watch_default_window_is_60s():
    assert hc_control.START_WATCH_DELAY == 60.0


def test_start_watch_warns_if_still_ready_after_timeout():
    appliance = make_appliance(op="Ready")
    scheduler = RecordingScheduler()
    logger = Mock()
    StartWatch(appliance, scheduler.post, logger=logger, delay=60)
    scheduler.run_all()                                   # fire the timeout while still Ready
    assert logger.warning.called
    message = logger.warning.call_args[0][0]
    # Uncertainty, not failure — the start may still begin.
    assert "has not reported starting" in message
    assert "may still begin" in message
    assert "did not start" not in message


@pytest.mark.parametrize("state", ["Run", "DelayedStart"])
def test_start_watch_silent_when_program_starts(state):
    appliance = make_appliance(op="Ready")
    scheduler = RecordingScheduler()
    logger = Mock()
    StartWatch(appliance, scheduler.post, logger=logger, delay=60)
    # SSE reports the appliance reached a started state before the timeout fires:
    appliance.merge_items([{"key": OPERATION_STATE, "value": _op(state)}])
    scheduler.run_all()
    assert not logger.warning.called
    assert appliance._observers.get(OPERATION_STATE) in (None, [])   # unsubscribed


def test_start_watch_timeout_exception_safe_and_unsubscribes():
    appliance = make_appliance(op="Ready")
    scheduler = RecordingScheduler()
    logger = Mock()
    StartWatch(appliance, scheduler.post, logger=logger, delay=60)
    # Simulate a torn-down appliance (e.g. plugin shutdown) so the check raises.
    appliance.get = Mock(side_effect=RuntimeError("appliance gone"))
    scheduler.run_all()                                # timer fires -> must not raise
    assert logger.exception.called                     # logged cleanly
    assert appliance._observers.get(OPERATION_STATE) in (None, [])   # still unsubscribed


# -- Red-team wave 1: fail-fast when the gate is closed (#8) ------------------

def test_control_refused_locally_when_gate_closed(tmp_path):
    # A Retry-After can run to hours; an Indigo action thread must be refused
    # with a time, never blocked on the gate.
    transport = ScriptedTransport()
    controller = make_controller(transport, tmp_path)
    controller._api._earliest_retry = 600.0        # gate closed for 10 minutes
    with pytest.raises(ControlRefused) as exc:
        controller.start_program(make_appliance(), "BSH.Common.Program.Auto")
    assert "rate-limited" in str(exc.value)
    assert transport.requests == []                # nothing left the plugin


def test_capability_load_serves_stale_cache_when_gated(tmp_path):
    transport = ScriptedTransport()
    body = json.dumps({"data": {"programs": [{"key": "P1"}]}})
    transport.queue(200, HC_JSON, body)
    controller = make_controller(transport, tmp_path)
    appliance = make_appliance()
    assert controller.all_programs(appliance) == [{"key": "P1"}]   # first load cached
    # Entry expired AND the gate closed: the stale value must serve, no HTTP.
    controller._cache._entries[f"all-programs:{HAID}"]["ts"] = -999_999
    controller._api._earliest_retry = 600.0
    assert controller.all_programs(appliance) == [{"key": "P1"}]
    assert len(transport.requests) == 1            # no second request attempted


@pytest.mark.parametrize("op", [
    lambda c, a: c.start_program(a, "Dishcare.Dishwasher.Program.Auto2"),
    lambda c, a: c.select_program(a, "Dishcare.Dishwasher.Program.Auto2"),
    lambda c, a: c.stop_program(a),
    lambda c, a: c.send_command(a, "BSH.Common.Command.PauseProgram"),
    lambda c, a: c.set_setting(a, "SomeKey", "value"),
])
def test_every_control_entry_point_refused_when_gate_closed(tmp_path, op):
    # Each entry point carries its own gate check; dropping any one silently
    # reverts that action to blocking an Indigo thread for hours.
    transport = ScriptedTransport()
    controller = make_controller(transport, tmp_path)
    controller._api._earliest_retry = 600.0
    with pytest.raises(ControlRefused) as exc:
        op(controller, make_appliance())
    assert "rate-limited" in str(exc.value)
    assert transport.requests == []


def test_gated_capability_load_with_no_cache_raises_without_http(tmp_path):
    # No stale entry to fall back on: the loader's gate check must surface the
    # rate limit as a HomeConnectError (menus degrade to a logged warning +
    # empty list) and attempt zero HTTP.
    transport = ScriptedTransport()
    controller = make_controller(transport, tmp_path)
    controller._api._earliest_retry = 600.0
    with pytest.raises(HomeConnectError) as exc:
        controller.all_programs(make_appliance())
    assert exc.value.status == 429
    assert transport.requests == []


# -- Red-team wave 2: ValueWatch (#18) + unknown-state pass-through (#19) -----

def test_value_watch_converged_value_no_warning():
    appliance = make_appliance()
    logger = Mock()
    scheduled = []
    hc_control.ValueWatch(appliance, "BSH.Common.Setting.PowerState",
                          "BSH.Common.EnumType.PowerState.Off",
                          lambda fn, delay: scheduled.append(fn), logger)
    appliance.merge_items([{"key": "BSH.Common.Setting.PowerState",
                            "value": "BSH.Common.EnumType.PowerState.Off"}])
    scheduled[0]()                               # timeout fires after convergence
    assert not logger.warning.called


def test_value_watch_warns_when_value_never_lands():
    appliance = make_appliance(power="On")
    logger = Mock()
    scheduled = []
    hc_control.ValueWatch(appliance, "BSH.Common.Setting.PowerState",
                          "BSH.Common.EnumType.PowerState.Off",
                          lambda fn, delay: scheduled.append(fn), logger,
                          describe="power state")
    scheduled[0]()                               # nothing arrived over SSE
    message = " ".join(str(c) for c in logger.warning.call_args_list)
    assert "accepted but has not been reflected" in message
    assert "may still apply" in message          # uncertainty, not failure (cloud lag)
    assert "power state" in message


def test_value_watch_unsubscribes_after_timeout():
    appliance = make_appliance()
    scheduled = []
    watch = hc_control.ValueWatch(appliance, "SomeKey", "v",
                                  lambda fn, delay: scheduled.append(fn), Mock())
    scheduled[0]()
    # A later change must not resurrect the watch (observer removed).
    appliance.merge_items([{"key": "SomeKey", "value": "v"}])
    assert watch._done


@pytest.mark.parametrize("current,expected,should_match", [
    ("BSH.Common.EnumType.PowerState.Off", "BSH.Common.EnumType.PowerState.Off", True),
    (True, True, True),
    (1, True, True),                    # Python equality: SSE 1 vs coerced True
    (45, 45.0, True),                   # float echo of an int setting
    ("5.5", 5.5, True),                 # str fallback catches numeric echo
    ("BSH.Common.EnumType.PowerState.On", "BSH.Common.EnumType.PowerState.Off", False),
    (None, "x", False),
])
def test_value_watch_matches_representations(current, expected, should_match):
    assert hc_control.ValueWatch._matches(current, expected) is should_match
