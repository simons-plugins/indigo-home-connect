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


def _plugin(transport, tmp_path):
    p = plugin.Plugin("com.simons-plugins.homeconnect", "Home Connect", "2026.0.5", {})
    clock = Clock()
    api = HomeConnectAPI(host="api.home-connect.com", logger=Mock(),
                         connection_factory=transport.factory,
                         monotonic=clock.monotonic, sleep=clock.sleep,
                         wall_now=lambda: datetime(2026, 8, 2, tzinfo=timezone.utc))
    cache = DiskCache(str(tmp_path / "cache.json"), "test", logger=Mock())
    p._controller = Controller(api, cache, logger=Mock())
    p.logger = Mock()
    p._schedule_later = lambda fn, delay: None      # never fire a real 15s timer in tests
    return p


def _appliance(op="Ready"):
    appliance = HomeConnectAppliance(
        HAID, {"name": "Dishwasher", "type": "Dishwasher", "connected": True},
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


# ---------------------------------------------------------------------------
# Dynamic menus from cache
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
    values = {"deviceId": str(dev.id)}
    first = p.programListForDevice(valuesDict=values)
    assert ("Dishcare.Dishwasher.Program.Auto2", "Auto") in first
    assert len(transport.requests) == 1
    p.programListForDevice(valuesDict=values)       # cache hit -> no new HTTP
    assert len(transport.requests) == 1


def test_program_list_empty_without_device(tmp_path):
    transport = ScriptedTransport()
    p = _plugin(transport, tmp_path)
    p._coordinator = _FakeCoord([_appliance()])
    assert p.programListForDevice(valuesDict={}) == []
    assert p.programListForDevice(valuesDict={"deviceId": "0"}) == []


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
    options = p.powerStateListForDevice(valuesDict={"deviceId": str(dev.id)})
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
    keys = {k for k, _ in p.commandListForDevice(valuesDict={"deviceId": str(dev.id)})}
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
