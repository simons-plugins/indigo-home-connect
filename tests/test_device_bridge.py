"""Unit tests for device_bridge.py — observer->device sync, status summary,
dynamic states, detach/unsubscribe and the disconnect policy."""
from unittest.mock import Mock

import indigo   # the conftest fake

from device_bridge import ApplianceBridge, summarize_status
from hc_appliance import HomeConnectAppliance
from support import FakeReader, RecordingScheduler

OP = "BSH.Common.Status.OperationState"
POWER = "BSH.Common.Setting.PowerState"
ACTIVE = "BSH.Common.Root.ActiveProgram"
REMAINING = "BSH.Common.Option.RemainingProgramTime"
PROGRESS = "BSH.Common.Option.ProgramProgress"


def make_appliance(info=None):
    return HomeConnectAppliance(
        "BOSCH-HCS01-0123456789AB",
        info or {"name": "Dishwasher", "type": "Dishwasher", "connected": True},
        FakeReader(), RecordingScheduler().post, logger=Mock())


def make_bridge(device_type_id="dishwasher", props=None, treat_off=True):
    dev = indigo.Device(id=7, name="Dishwasher", deviceTypeId=device_type_id,
                        pluginProps=props or {"haId": "BOSCH-HCS01-0123456789AB"})
    bridge = ApplianceBridge(dev, device_type_id, logger=Mock(),
                             treat_disconnected_as_off=treat_off)
    return bridge, dev


# -- summarize_status (pure) --------------------------------------------------

def test_summary_running_with_program_and_remaining():
    snap = {"connected": True, OP: "BSH.Common.EnumType.OperationState.Run",
            ACTIVE: "Dishcare.Dishwasher.Program.Eco50", REMAINING: 5040}
    assert summarize_status(snap, True) == "Run · Eco 50 · 1:24 remaining"


def test_summary_running_without_remaining_omits_time():
    snap = {"connected": True, OP: "BSH.Common.EnumType.OperationState.Run",
            ACTIVE: "Dishcare.Dishwasher.Program.Auto2"}
    assert summarize_status(snap, True) == "Run · Auto 2"


def test_summary_power_off():
    snap = {"connected": True, OP: "BSH.Common.EnumType.OperationState.Inactive",
            POWER: "BSH.Common.EnumType.PowerState.Off"}
    assert summarize_status(snap, True) == "Off"


def test_summary_standby():
    snap = {"connected": True, POWER: "BSH.Common.EnumType.PowerState.Standby"}
    assert summarize_status(snap, True) == "Standby"


def test_summary_ready():
    snap = {"connected": True, OP: "BSH.Common.EnumType.OperationState.Ready"}
    assert summarize_status(snap, True) == "Ready"


def test_summary_disconnected_as_off():
    assert summarize_status({"connected": False}, True) == "Off"


def test_summary_disconnected_shown():
    assert summarize_status({"connected": False}, False) == "Disconnected"


# -- observer -> batched device updates ---------------------------------------

def test_events_produce_batched_state_updates():
    appliance = make_appliance()
    bridge, dev = make_bridge()
    bridge.attach(appliance)
    appliance.merge_items([
        {"key": OP, "value": "BSH.Common.EnumType.OperationState.Run"},
        {"key": ACTIVE, "value": "Dishcare.Dishwasher.Program.Eco50"},
        {"key": REMAINING, "value": 5040},
        {"key": PROGRESS, "value": 42},
    ])
    assert dev.states["operationState"] == "Run"
    assert dev.states["programActive"] == "Eco50"
    assert dev.states["remainingTime"] == 5040
    assert dev.states["remainingTimeFormatted"] == "1:24"
    assert dev.states["programProgress"] == 42
    assert dev.states["status"] == "Run · Eco 50 · 1:24 remaining"


def test_boolean_and_event_states_mapped():
    appliance = make_appliance()
    bridge, dev = make_bridge()
    bridge.attach(appliance)
    appliance.merge_items([
        {"key": "BSH.Common.Status.RemoteControlActive", "value": True},
        {"key": "BSH.Common.Status.RemoteControlStartAllowed", "value": False},
    ])
    appliance.handle_event_items([
        {"key": "Dishcare.Dishwasher.Event.SaltNearlyEmpty", "timestamp": 1,
         "value": "BSH.Common.EnumType.EventPresentState.Present"},
    ])
    assert dev.states["remoteControlActive"] is True
    assert dev.states["remoteStartAllowed"] is False
    assert dev.states["saltNearlyEmpty"] is True


def test_last_event_tracked():
    appliance = make_appliance()
    bridge, dev = make_bridge()
    bridge.attach(appliance)
    appliance.handle_event_items([
        {"key": "BSH.Common.Event.ProgramFinished", "timestamp": 10,
         "value": "BSH.Common.EnumType.EventPresentState.Present"},
    ])
    assert dev.states["lastEvent"] == "ProgramFinished"
    assert dev.states["lastEventTime"]      # set to an ISO timestamp string


# -- dynamic states -----------------------------------------------------------

def test_unknown_key_becomes_dynamic_state():
    appliance = make_appliance()
    bridge, dev = make_bridge()
    bridge.attach(appliance)
    appliance.merge_items([{"key": "Vendor.Weird.Undocumented", "value": "hi"}])
    assert dev.pluginProps["dynamicStateKeys"] == "vendorWeirdUndocumented"
    assert dev.state_list_changed >= 1
    assert dev.states["vendorWeirdUndocumented"] == "hi"


def test_dynamic_state_registered_once():
    appliance = make_appliance()
    bridge, dev = make_bridge()
    bridge.attach(appliance)
    appliance.merge_items([{"key": "Vendor.Weird.Undocumented", "value": "a"}])
    changed_after_first = dev.state_list_changed
    appliance.merge_items([{"key": "Vendor.Weird.Undocumented", "value": "b"}])
    assert dev.state_list_changed == changed_after_first    # no re-register
    assert dev.states["vendorWeirdUndocumented"] == "b"


def test_getdevicestatelist_reads_dynamic_prop():
    """The dynamic key the bridge persists is what plugin.getDeviceStateList reads."""
    appliance = make_appliance()
    bridge, dev = make_bridge()
    bridge.attach(appliance)
    appliance.merge_items([{"key": "Vendor.Weird.Undocumented", "value": "x"}])
    keys = [part for part in dev.pluginProps["dynamicStateKeys"].split(",") if part]
    assert "vendorWeirdUndocumented" in keys


# -- detach / unsubscribe -----------------------------------------------------

def test_detach_stops_further_updates():
    appliance = make_appliance()
    bridge, dev = make_bridge()
    bridge.attach(appliance)
    appliance.merge_items([{"key": OP, "value": "BSH.Common.EnumType.OperationState.Run"}])
    batches_before = len(dev.batches)
    bridge.detach()
    appliance.merge_items([{"key": OP, "value": "BSH.Common.EnumType.OperationState.Ready"}])
    assert len(dev.batches) == batches_before         # no writes after detach
    assert dev.states["operationState"] == "Run"      # unchanged


# -- PAIRED-later registration ------------------------------------------------

def test_mark_waiting_then_attach():
    bridge, dev = make_bridge()
    bridge.mark_waiting()
    assert dev.states["status"] == "Waiting"
    # Appliance appears later:
    appliance = make_appliance()
    bridge.attach(appliance)
    appliance.merge_items([{"key": OP, "value": "BSH.Common.EnumType.OperationState.Ready"}])
    assert dev.states["operationState"] == "Ready"


# -- disconnect policy / error state ------------------------------------------

def test_disconnect_shown_sets_error_state():
    appliance = make_appliance(info={"name": "D", "type": "Dishwasher", "connected": False})
    bridge, dev = make_bridge(treat_off=False)
    bridge.attach(appliance)
    assert dev.error_state == "Disconnected"
    assert dev.states["status"] == "Disconnected"


def test_disconnect_as_off_clears_error_state():
    appliance = make_appliance(info={"name": "D", "type": "Dishwasher", "connected": False})
    bridge, dev = make_bridge(treat_off=True)
    bridge.attach(appliance)
    assert dev.error_state is None
    assert dev.states["status"] == "Off"


def test_fridge_setpoints_number_states():
    appliance = make_appliance(info={"name": "Fridge", "type": "FridgeFreezer", "connected": True})
    bridge, dev = make_bridge(device_type_id="fridgeFreezer")
    bridge.attach(appliance)
    appliance.merge_items([
        {"key": "Refrigeration.FridgeFreezer.Setting.SetpointTemperatureRefrigerator", "value": 4},
        {"key": "Refrigeration.FridgeFreezer.Setting.SuperModeFreezer", "value": True},
    ])
    assert dev.states["setpointTemperatureRefrigerator"] == 4
    assert dev.states["superModeFreezer"] is True
