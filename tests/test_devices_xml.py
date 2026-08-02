"""Structural validation of Devices.xml against hc_constants."""
import xml.etree.ElementTree as ET
from pathlib import Path

import hc_constants as hc

DEVICES_XML = (
    Path(__file__).parent.parent
    / "Home Connect.indigoPlugin" / "Contents" / "Server Plugin" / "Devices.xml"
)

EXPECTED_TYPES = ["dishwasher", "dryer", "washer", "oven", "coffeeMaker",
                  "fridgeFreezer", "homeConnectAppliance"]


def _devices():
    root = ET.parse(DEVICES_XML).getroot()
    return {dev.get("id"): dev for dev in root.findall("Device")}


def test_all_seven_types_present():
    assert sorted(_devices()) == sorted(EXPECTED_TYPES)


def test_state_ids_unique_per_type():
    for type_id, dev in _devices().items():
        ids = [state.get("id") for state in dev.findall("States/State")]
        assert len(ids) == len(set(ids)), f"duplicate state id in {type_id}"


def test_common_block_present_in_every_type():
    common = set(hc.common_state_ids())
    for type_id, dev in _devices().items():
        ids = {state.get("id") for state in dev.findall("States/State")}
        missing = common - ids
        assert not missing, f"{type_id} missing common states {missing}"


def test_display_state_exists_and_is_status():
    for type_id, dev in _devices().items():
        ids = {state.get("id") for state in dev.findall("States/State")}
        display = dev.findtext("UiDisplayStateId")
        assert display == "status", f"{type_id} display state is {display}"
        assert display in ids


def test_type_specific_states_declared():
    """Every mapped type-specific state_id appears in that type's XML block."""
    devices = _devices()
    for type_id in EXPECTED_TYPES:
        ids = {state.get("id") for state in devices[type_id].findall("States/State")}
        for state_id, _kind in hc.TYPE_MAP[type_id].values():
            assert state_id in ids, f"{type_id} XML missing {state_id}"


def test_every_declared_state_id_is_valid():
    """State IDs must be ASCII-alnum, letter-start, no underscores (field notes)."""
    for type_id, dev in _devices().items():
        for state in dev.findall("States/State"):
            sid = state.get("id")
            assert sid and sid[0].isascii() and sid[0].isalpha(), f"{type_id}: {sid}"
            assert all(char.isascii() and char.isalnum() for char in sid), f"{type_id}: {sid}"


def test_each_type_has_appliance_picker_and_policy():
    for type_id, dev in _devices().items():
        field_ids = {f.get("id") for f in dev.findall("ConfigUI/Field")}
        assert "haId" in field_ids, f"{type_id} missing haId picker"
        assert "offWhenDisconnected" in field_ids, f"{type_id} missing policy checkbox"
