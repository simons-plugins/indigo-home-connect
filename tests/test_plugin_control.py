"""Tests for the Phase 4 control wiring in plugin.py: action dispatch + error
handling, dynamic menus served from the capability cache, and
validateActionConfigUi. The controller is real (ScriptedTransport underneath) so
the plugin<->hc_control seam is exercised end to end without network or indigo.
"""
import json
from datetime import datetime, timezone
from unittest.mock import Mock

import indigo   # the conftest fake
import plugin
import hc_control
from hc_api import HomeConnectAPI
from hc_appliance import (HomeConnectAppliance, OPERATION_STATE, REMOTE_CONTROL,
                          REMOTE_START, LOCAL_CONTROL)
from hc_cache import DiskCache
from hc_control import Controller, POWER_STATE_KEY, PAUSE_COMMAND
from support import Clock, ScriptedTransport

HC_JSON = {"content-type": "application/vnd.bsh.sdk.v1+json"}
HAID = "HA-1"


class _Action:
    def __init__(self, props=None, device_id=0):
        self.props = dict(props or {})
        self.deviceId = device_id


class _FakeCoord:
    def __init__(self, appliances):
        self._appliances = list(appliances)

    def appliances(self):
        return list(self._appliances)


def _plugin(transport, tmp_path, ready_timeout=25.0):
    p = plugin.Plugin("com.simons-plugins.homeconnect", "Home Connect", "2026.0.5", {})
    clock = Clock()
    api_logger = Mock()
    api = HomeConnectAPI(host="api.home-connect.com", logger=api_logger,
                         connection_factory=transport.factory,
                         monotonic=clock.monotonic, sleep=clock.sleep,
                         wall_now=lambda: datetime(2026, 8, 2, tzinfo=timezone.utc))
    cache = DiskCache(str(tmp_path / "cache.json"), "test", logger=Mock())
    p._controller = Controller(api, cache, logger=Mock(), now=clock.monotonic,
                               ready_timeout=ready_timeout)
    p.logger = Mock()
    # Record StartWatch scheduling instead of firing a real 15s timer, so tests
    # can assert whether a watch was armed.
    p._schedule_later = Mock()
    p._test_api = api
    p._test_clock = clock
    p._test_api_logger = api_logger
    return p


def _appliance(op="Ready", hc_type="Dishwasher"):
    appliance = HomeConnectAppliance(
        HAID, {"name": hc_type, "type": hc_type, "connected": True},
        Mock(), Mock(), logger=Mock())
    appliance.merge_items([
        {"key": OPERATION_STATE, "value": f"BSH.Common.EnumType.OperationState.{op}"},
        {"key": REMOTE_CONTROL, "value": True},
        {"key": REMOTE_START, "value": True},
        {"key": LOCAL_CONTROL, "value": False},
        {"key": POWER_STATE_KEY, "value": "BSH.Common.EnumType.PowerState.On"},
    ])
    return appliance


def _device(dev_id=1, haid=HAID):
    return indigo.Device(id=dev_id, name="Kitchen DW", deviceTypeId="dishwasher",
                         pluginProps={"haId": haid, "offWhenDisconnected": True})


def _puts(transport):
    return [r for r in transport.requests if r["method"] == "PUT"]


# ---------------------------------------------------------------------------
# Action dispatch
# ---------------------------------------------------------------------------
def test_start_program_dispatches_put(tmp_path):
    transport = ScriptedTransport()
    transport.queue(204, HC_JSON, b"")
    p = _plugin(transport, tmp_path)
    appliance = _appliance()
    p._coordinator = _FakeCoord([appliance])
    dev = _device()
    p.startProgram(_Action({"program": "Dishcare.Dishwasher.Program.Auto2"}), dev)
    sent = _puts(transport)
    assert len(sent) == 1
    assert sent[0]["url"] == f"/api/homeappliances/{HAID}/programs/active"
    assert json.loads(sent[0]["body"])["data"]["key"] == "Dishcare.Dishwasher.Program.Auto2"


def test_control_refused_is_logged_not_raised(tmp_path):
    transport = ScriptedTransport()
    p = _plugin(transport, tmp_path)
    # Remote start not allowed -> local refusal, no HTTP, error logged.
    appliance = _appliance()
    appliance.merge_items([{"key": REMOTE_START, "value": False}])
    p._coordinator = _FakeCoord([appliance])
    p.startProgram(_Action({"program": "X.Program"}), _device())
    assert transport.requests == []
    assert p.logger.error.called
    assert "Remote Start" in p.logger.error.call_args[0][2]


def test_bad_option_override_refused_and_logged(tmp_path):
    transport = ScriptedTransport()
    p = _plugin(transport, tmp_path)
    p._coordinator = _FakeCoord([_appliance()])
    p.startProgram(_Action({"program": "X.Program", "optionOverrides": "garbage"}), _device())
    assert transport.requests == []                 # coercion failed before any HTTP
    assert p.logger.error.called


def test_stop_program_dispatches_delete(tmp_path):
    transport = ScriptedTransport()
    transport.queue(204, HC_JSON, b"")
    p = _plugin(transport, tmp_path)
    p._coordinator = _FakeCoord([_appliance(op="Run")])
    p.stopProgram(_Action(), _device())
    assert [r["method"] for r in transport.requests] == ["DELETE"]


def test_action_with_no_device_logs_error(tmp_path):
    transport = ScriptedTransport()
    p = _plugin(transport, tmp_path)
    p._coordinator = _FakeCoord([])
    p.startProgram(_Action({"program": "X"}), None)
    assert p.logger.error.called
    assert transport.requests == []


def test_action_when_appliance_absent_logs_error(tmp_path):
    transport = ScriptedTransport()
    p = _plugin(transport, tmp_path)
    p._coordinator = _FakeCoord([])                 # appliance not discovered
    p.pauseProgram(_Action(), _device(haid="HA-MISSING"))
    assert p.logger.error.called
    assert transport.requests == []


def _off_appliance():
    appliance = HomeConnectAppliance(
        HAID, {"name": "Dishwasher", "type": "Dishwasher", "connected": True},
        Mock(), Mock(), logger=Mock())
    appliance.merge_items([
        {"key": OPERATION_STATE, "value": "BSH.Common.EnumType.OperationState.Inactive"},
        {"key": POWER_STATE_KEY, "value": "BSH.Common.EnumType.PowerState.Off"},
    ])
    return appliance


# ---------------------------------------------------------------------------
# Auto power-on checkbox plumbing (powerOnFirst)
# ---------------------------------------------------------------------------
def test_power_on_first_default_true_attempts_power_on(tmp_path):
    # No powerOnFirst prop -> defaults True -> off appliance triggers the power-on
    # path (constraints GET + PowerState PUT); Ready never arrives -> refusal.
    transport = ScriptedTransport()
    body = json.dumps({"data": {"constraints": {
        "allowedvalues": ["BSH.Common.EnumType.PowerState.Off", "BSH.Common.EnumType.PowerState.On"],
        "access": "readWrite"}}})
    transport.queue(200, HC_JSON, body)              # GET power constraints
    transport.queue(204, HC_JSON, b"")               # PUT PowerState=On
    p = _plugin(transport, tmp_path, ready_timeout=0.1)
    p._coordinator = _FakeCoord([_off_appliance()])
    p.startProgram(_Action({"program": "P"}), _device())    # no powerOnFirst key
    assert [(r["method"], r["url"].split("/")[-1]) for r in transport.requests] == [
        ("GET", POWER_STATE_KEY), ("PUT", POWER_STATE_KEY)]     # power-on attempted
    assert p.logger.error.called                     # timed out -> actionable error, no start


def test_power_on_first_false_skips_power(tmp_path):
    transport = ScriptedTransport()
    p = _plugin(transport, tmp_path)
    p._coordinator = _FakeCoord([_off_appliance()])
    p.startProgram(_Action({"program": "P", "powerOnFirst": False}), _device())
    assert transport.requests == []                  # off + no power-on -> straight refusal
    assert p.logger.error.called


# ---------------------------------------------------------------------------
# Remote refusal / rate limit at the plugin seam
# ---------------------------------------------------------------------------
def test_remote_409_logs_once_no_watch_limiter_consumed(tmp_path):
    transport = ScriptedTransport()
    transport.queue(409, HC_JSON, json.dumps({"error": {"key": "BSH.Common.Error.Conflict"}}))
    p = _plugin(transport, tmp_path)
    p._coordinator = _FakeCoord([_appliance()])
    p.startProgram(_Action({"program": "X.Program"}), _device())
    # Exactly one actionable error, nothing raised into Indigo:
    assert p.logger.error.call_count == 1
    # The failed start did NOT arm a StartWatch (only success does):
    assert p._schedule_later.call_count == 0
    # ...but the limiter slot was consumed (the request reached BSH):
    assert len(p._controller._start_limiter._events) == 1
    assert [r["method"] for r in transport.requests] == ["PUT"]     # one attempt, not retried


def test_409_error_message_is_actionable(tmp_path):
    # A 409 from BSH must be logged with a concrete next step, not just "HTTP 409".
    transport = ScriptedTransport()
    transport.queue(409, HC_JSON, json.dumps({"error": {"key": "BSH.Common.Error.Conflict"}}))
    p = _plugin(transport, tmp_path)
    p._coordinator = _FakeCoord([_appliance()])
    p.startProgram(_Action({"program": "X.Program"}), _device())
    assert p.logger.error.called
    message = " ".join(str(a) for a in p.logger.error.call_args[0])
    assert "door is shut" in message and "Remote Control" in message


def test_control_error_hint_maps_statuses():
    from hc_api import HomeConnectError
    assert "rate-limiting" in plugin._control_error_hint(HomeConnectError("x", status=429))
    assert "Control scope" in plugin._control_error_hint(HomeConnectError("x", status=403))
    assert "door is shut" in plugin._control_error_hint(HomeConnectError("x", status=409))
    # An unmapped status falls back to the raw error string.
    assert plugin._control_error_hint(HomeConnectError("boom", status=500)) == "boom"


def test_429_retry_after_second_start_refused_locally_not_blocking(tmp_path):
    transport = ScriptedTransport()
    transport.queue(429, {"content-type": "application/json", "retry-after": "30"}, "{}")
    p = _plugin(transport, tmp_path)
    p._coordinator = _FakeCoord([_appliance()])
    # First start hits the 429: surfaces once, no retry (no_retry start).
    p.startProgram(_Action({"program": "X.Program"}), _device())
    assert p.logger.error.call_count == 1
    # Second start within the Retry-After window must be REFUSED locally — an
    # Indigo action thread must never block out a gate that can run to hours.
    p.startProgram(_Action({"program": "X.Program"}), _device())
    assert p.logger.error.call_count == 2
    message = " ".join(str(a) for a in p.logger.error.call_args[0])
    assert "rate-limited" in message and "try again" in message
    assert 30 not in p._test_clock.slept                        # never slept the gate out
    assert [r["method"] for r in transport.requests] == ["PUT"]  # no second HTTP at all


# ---------------------------------------------------------------------------
# Dynamic menus from cache (device resolved via targetId)
# ---------------------------------------------------------------------------
def test_program_list_from_cache_no_http_second_call(tmp_path):
    transport = ScriptedTransport()
    body = json.dumps({"data": {"programs": [
        {"key": "Dishcare.Dishwasher.Program.Auto2", "name": "Auto"},
        {"key": "Dishcare.Dishwasher.Program.Eco50", "name": "Eco 50"},
    ]}})
    transport.queue(200, HC_JSON, body)
    p = _plugin(transport, tmp_path)
    dev = _device()
    indigo.devices._devices.clear()
    indigo.devices.add(dev)
    p._coordinator = _FakeCoord([_appliance()])
    first = p.programListForDevice(targetId=dev.id)        # device via targetId (real contract)
    assert ("Dishcare.Dishwasher.Program.Auto2", "Auto") in first
    assert len(transport.requests) == 1
    p.programListForDevice(targetId=dev.id)                # cache hit -> no new HTTP
    assert len(transport.requests) == 1


def test_program_list_resolves_via_deviceid_fallback(tmp_path):
    transport = ScriptedTransport()
    transport.queue(200, HC_JSON, json.dumps({"data": {"programs": [{"key": "P1", "name": "One"}]}}))
    p = _plugin(transport, tmp_path)
    dev = _device()
    indigo.devices._devices.clear()
    indigo.devices.add(dev)
    p._coordinator = _FakeCoord([_appliance()])
    # No targetId (0) -> fall back to valuesDict["deviceId"].
    options = p.programListForDevice(valuesDict={"deviceId": str(dev.id)}, targetId=0)
    assert options == [("P1", "One")]


def test_program_list_empty_without_device(tmp_path):
    transport = ScriptedTransport()
    p = _plugin(transport, tmp_path)
    p._coordinator = _FakeCoord([_appliance()])
    assert p.programListForDevice(targetId=0) == []
    assert p.programListForDevice(valuesDict={}, targetId=0) == []


def test_menu_fetch_failure_returns_empty_and_warns(tmp_path):
    transport = ScriptedTransport()
    transport.queue(403, HC_JSON, json.dumps({"error": {"key": "Forbidden"}}))   # not retried
    p = _plugin(transport, tmp_path)
    dev = _device()
    indigo.devices._devices.clear()
    indigo.devices.add(dev)
    p._coordinator = _FakeCoord([_appliance()])
    assert p.programListForDevice(targetId=dev.id) == []     # no raise into the ConfigUI thread
    warnings = [c for c in p.logger.warning.call_args_list if "could not load programs" in str(c)]
    assert len(warnings) == 1


def test_program_list_dryer_regression_lists_all_with_unavailable_suffixed(tmp_path):
    # Field scenario: /programs returns 13 (one available=False), while
    # /programs/available would return only 1. The menu must list all 13, the
    # unavailable one suffixed, and exclude an execution=none entry.
    programs = [{"key": f"P{i}", "name": f"Program {i}",
                 "constraints": {"available": True, "execution": "selectandstart"}}
                for i in range(13)]
    programs[3]["constraints"]["available"] = False          # one not currently available
    programs.append({"key": "PNone", "name": "Diagnosis",
                     "constraints": {"execution": "none"}})   # never startable -> excluded
    transport = ScriptedTransport()
    transport.queue(200, HC_JSON, json.dumps({"data": {"programs": programs}}))
    p = _plugin(transport, tmp_path)
    dev = _device()
    indigo.devices._devices.clear()
    indigo.devices.add(dev)
    p._coordinator = _FakeCoord([_appliance(hc_type="Dryer")])
    options = p.programListForDevice(targetId=dev.id)
    assert len(options) == 13                                # all real programs, none dropped
    labels = {k: label for k, label in options}
    assert labels["P3"].endswith("(not currently available)")
    assert labels["P0"] == "Program 0"                       # available -> no suffix
    assert "PNone" not in labels                             # execution=none excluded


def test_program_list_prefers_localized_name_else_key_tail(tmp_path):
    transport = ScriptedTransport()
    transport.queue(200, HC_JSON, json.dumps({"data": {"programs": [
        {"key": "LaundryCare.Dryer.Program.Cotton", "name": "Baumwolle"},   # localized name
        {"key": "LaundryCare.Dryer.Program.Mix60"},                          # no name -> key tail
    ]}}))
    p = _plugin(transport, tmp_path)
    dev = _device()
    indigo.devices._devices.clear()
    indigo.devices.add(dev)
    p._coordinator = _FakeCoord([_appliance(hc_type="Dryer")])
    labels = {k: label for k, label in p.programListForDevice(targetId=dev.id)}
    assert labels["LaundryCare.Dryer.Program.Cotton"] == "Baumwolle"        # localized wins
    assert labels["LaundryCare.Dryer.Program.Mix60"] == "Mix 60"            # prettified key tail


def test_power_state_list_filters_to_allowed(tmp_path):
    transport = ScriptedTransport()
    body = json.dumps({"data": {"constraints": {"allowedvalues": [
        "BSH.Common.EnumType.PowerState.On"]}}})    # dryer: On only
    transport.queue(200, HC_JSON, body)
    p = _plugin(transport, tmp_path)
    dev = _device()
    indigo.devices._devices.clear()
    indigo.devices.add(dev)
    p._coordinator = _FakeCoord([_appliance()])
    options = p.powerStateListForDevice(targetId=dev.id)
    assert options == [("BSH.Common.EnumType.PowerState.On", "On")]


def test_command_list_from_cache(tmp_path):
    transport = ScriptedTransport()
    body = json.dumps({"data": {"commands": [
        {"key": "BSH.Common.Command.OpenDoor"},
        {"key": PAUSE_COMMAND},
    ]}})
    transport.queue(200, HC_JSON, body)
    p = _plugin(transport, tmp_path)
    dev = _device()
    indigo.devices._devices.clear()
    indigo.devices.add(dev)
    p._coordinator = _FakeCoord([_appliance()])
    keys = {k for k, _ in p.commandListForDevice(targetId=dev.id)}
    assert keys == {"BSH.Common.Command.OpenDoor", PAUSE_COMMAND}


# ---------------------------------------------------------------------------
# validateActionConfigUi
# ---------------------------------------------------------------------------
def test_validate_start_requires_program(tmp_path):
    p = _plugin(ScriptedTransport(), tmp_path)
    ok, _values, errors = p.validateActionConfigUi({"program": ""}, "startProgram", 5)
    assert ok is False
    assert "program" in errors


def test_validate_start_rejects_bad_overrides(tmp_path):
    p = _plugin(ScriptedTransport(), tmp_path)
    ok, _values, errors = p.validateActionConfigUi(
        {"program": "X", "optionOverrides": "garbage"}, "startProgram", 5)
    assert ok is False
    assert "optionOverrides" in errors


def test_validate_start_ok(tmp_path):
    p = _plugin(ScriptedTransport(), tmp_path)
    result = p.validateActionConfigUi(
        {"program": "X", "optionOverrides": "Foo=1"}, "startProgram", 5)
    assert result[0] is True


def test_validate_send_command_requires_command(tmp_path):
    p = _plugin(ScriptedTransport(), tmp_path)
    ok, _values, errors = p.validateActionConfigUi({"command": ""}, "sendCommand", 5)
    assert ok is False
    assert "command" in errors


def test_validate_set_setting_requires_key(tmp_path):
    p = _plugin(ScriptedTransport(), tmp_path)
    ok, _values, errors = p.validateActionConfigUi(
        {"settingKey": "  "}, "setSetting", 5)
    assert ok is False
    assert "settingKey" in errors
