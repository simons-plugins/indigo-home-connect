"""Tests for the Phase 3 device layer in plugin.py: bridge lifecycle, dynamic
state list, ConfigUI list/validation, and the comm-property-change gate."""
from unittest.mock import Mock

import indigo   # the conftest fake
import plugin
from hc_appliance import HomeConnectAppliance

OP = "BSH.Common.Status.OperationState"


def _plugin():
    return plugin.Plugin("com.simons-plugins.homeconnect", "Home Connect", "2026.0.4", {})


def _appliance(haid, name, hc_type):
    return HomeConnectAppliance(haid, {"name": name, "type": hc_type, "connected": True},
                                Mock(), Mock(), logger=Mock())


class _FakeCoord:
    def __init__(self, appliances):
        self._appliances = list(appliances)

    def appliances(self):
        return list(self._appliances)


def _device(dev_id, type_id, haid, off=True):
    return indigo.Device(id=dev_id, name=f"dev{dev_id}", deviceTypeId=type_id,
                         pluginProps={"haId": haid, "offWhenDisconnected": off})


# -- deviceStartComm / attach -------------------------------------------------

def test_device_start_attaches_to_present_appliance():
    p = _plugin()
    appliance = _appliance("HA-1", "Dishwasher", "Dishwasher")
    p._coordinator = _FakeCoord([appliance])
    dev = _device(1, "dishwasher", "HA-1")
    p.deviceStartComm(dev)
    assert dev.id in p._bridges
    appliance.merge_items([{"key": OP, "value": "BSH.Common.EnumType.OperationState.Run"}])
    assert dev.states["operationState"] == "Run"


def test_device_start_marks_waiting_when_appliance_absent():
    p = _plugin()
    p._coordinator = _FakeCoord([])
    dev = _device(2, "dishwasher", "HA-LATER")
    p.deviceStartComm(dev)
    assert dev.states["status"] == "Waiting"


def test_paired_later_attaches_bridge():
    p = _plugin()
    p._coordinator = _FakeCoord([])
    dev = _device(3, "dishwasher", "HA-LATER")
    p.deviceStartComm(dev)
    # Appliance turns up after the device was created:
    appliance = _appliance("HA-LATER", "Dishwasher", "Dishwasher")
    p._appliance_discovered(appliance)
    appliance.merge_items([{"key": OP, "value": "BSH.Common.EnumType.OperationState.Ready"}])
    assert dev.states["operationState"] == "Ready"


def test_device_stop_detaches():
    p = _plugin()
    appliance = _appliance("HA-1", "Dishwasher", "Dishwasher")
    p._coordinator = _FakeCoord([appliance])
    dev = _device(4, "dishwasher", "HA-1")
    p.deviceStartComm(dev)
    p.deviceStopComm(dev)
    assert dev.id not in p._bridges
    batches_before = len(dev.batches)
    appliance.merge_items([{"key": OP, "value": "BSH.Common.EnumType.OperationState.Run"}])
    assert len(dev.batches) == batches_before      # detached: no more writes


# -- getDeviceStateList / display state ---------------------------------------

def test_get_device_state_list_appends_dynamic_states():
    p = _plugin()
    dev = indigo.Device(id=5, name="d5", deviceTypeId="dishwasher",
                        pluginProps={"dynamicStateKeys": "bazQux,fooBar"})
    state_list = p.getDeviceStateList(dev)
    keys = [entry["Key"] for entry in state_list]
    assert keys == ["bazQux", "fooBar"]            # sorted, string states


def test_get_device_state_list_dedupes_against_base():
    p = _plugin()
    indigo.PluginBase._base_state_lists = {"dishwasher": [{"Key": "status"}]}
    try:
        dev = indigo.Device(id=6, name="d6", deviceTypeId="dishwasher",
                            pluginProps={"dynamicStateKeys": "status,fooBar"})
        keys = [entry["Key"] for entry in p.getDeviceStateList(dev)]
        assert keys.count("status") == 1
        assert "fooBar" in keys
    finally:
        indigo.PluginBase._base_state_lists = {}


def test_display_state_id_is_status():
    p = _plugin()
    dev = _device(7, "oven", "HA-9")
    assert p.getDeviceDisplayStateId(dev) == "status"


# -- ConfigUI: listAppliances + validation ------------------------------------

def test_list_appliances_filters_by_type():
    p = _plugin()
    indigo.devices._devices.clear()
    p._coordinator = _FakeCoord([
        _appliance("HA-DW", "Dishwasher", "Dishwasher"),
        _appliance("HA-OV", "Oven", "Oven"),
    ])
    dishwashers = p.listAppliances(typeId="dishwasher")
    assert [haid for haid, _ in dishwashers] == ["HA-DW"]


def test_list_appliances_generic_lists_all():
    p = _plugin()
    indigo.devices._devices.clear()
    p._coordinator = _FakeCoord([
        _appliance("HA-DW", "Dishwasher", "Dishwasher"),
        _appliance("HA-OV", "Oven", "Oven"),
    ])
    haids = {haid for haid, _ in p.listAppliances(typeId="homeConnectAppliance")}
    assert haids == {"HA-DW", "HA-OV"}


def test_list_appliances_marks_in_use():
    p = _plugin()
    indigo.devices._devices.clear()
    indigo.devices.add(_device(20, "dishwasher", "HA-DW"))   # already uses HA-DW
    p._coordinator = _FakeCoord([_appliance("HA-DW", "Dishwasher", "Dishwasher")])
    options = p.listAppliances(typeId="dishwasher", targetId=99)
    assert options[0][0] == "HA-DW"
    assert "(in use)" in options[0][1]


def test_list_appliances_excludes_self_from_in_use():
    p = _plugin()
    indigo.devices._devices.clear()
    indigo.devices.add(_device(30, "dishwasher", "HA-DW"))
    p._coordinator = _FakeCoord([_appliance("HA-DW", "Dishwasher", "Dishwasher")])
    options = p.listAppliances(typeId="dishwasher", targetId=30)   # editing that device
    assert "(in use)" not in options[0][1]


def test_list_appliances_empty_without_coordinator():
    p = _plugin()
    p._coordinator = None
    assert p.listAppliances(typeId="dishwasher") == []


def test_validate_requires_appliance_selection():
    p = _plugin()
    ok, _values, errors = p.validateDeviceConfigUi({"haId": ""}, "dishwasher", 0)
    assert ok is False
    assert "haId" in errors


def test_validate_passes_with_selection():
    p = _plugin()
    result = p.validateDeviceConfigUi({"haId": "HA-1"}, "dishwasher", 0)
    assert result[0] is True


# -- didDeviceCommPropertyChange ----------------------------------------------

def test_comm_property_change_on_haid():
    p = _plugin()
    old = _device(8, "dishwasher", "HA-1")
    new = _device(8, "dishwasher", "HA-2")
    assert p.didDeviceCommPropertyChange(old, new) is True


def test_comm_property_change_on_policy():
    p = _plugin()
    old = _device(9, "dishwasher", "HA-1", off=True)
    new = _device(9, "dishwasher", "HA-1", off=False)
    assert p.didDeviceCommPropertyChange(old, new) is True


def test_comm_property_change_ignores_other_props():
    p = _plugin()
    old = _device(10, "dishwasher", "HA-1")
    new = _device(10, "dishwasher", "HA-1")
    new.pluginProps["somethingElse"] = "changed"
    assert p.didDeviceCommPropertyChange(old, new) is False
