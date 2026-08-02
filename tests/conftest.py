"""Shared pytest fixtures.

Installs a minimal fake `indigo` module so plugin code imports outside the
Indigo server, and puts the Server Plugin directory on sys.path. The fake grew
in Phase 3 to cover the device layer: an `indigo.Device`, a `devices` registry,
`Dict`/`List` aliases and the `PluginBase` state-list helpers the plugin uses.
"""
import sys
import types
from pathlib import Path

SERVER_PLUGIN_DIR = (
    Path(__file__).parent.parent
    / "Home Connect.indigoPlugin"
    / "Contents"
    / "Server Plugin"
)
sys.path.insert(0, str(SERVER_PLUGIN_DIR))


class _FakeDevice:
    """Stand-in for ``indigo.Device`` — records state/prop writes for assertions."""

    def __init__(self, id=1, name="Appliance", deviceTypeId="dishwasher",  # noqa: A002
                 pluginProps=None, states=None):
        self.id = id
        self.name = name
        self.deviceTypeId = deviceTypeId
        self.pluginProps = dict(pluginProps or {})
        self.states = dict(states or {})
        self.error_state = None
        self.error_calls = []
        self.state_list_changed = 0
        self.batches = []          # each updateStatesOnServer call, as a list of dicts

    def updateStatesOnServer(self, items):
        batch = []
        for item in items:
            self.states[item["key"]] = item.get("value")
            batch.append(dict(item))
        self.batches.append(batch)

    def updateStateOnServer(self, key, value=None, uiValue=None, **kwargs):  # noqa: ARG002
        self.states[key] = value

    def setErrorStateOnServer(self, message):
        self.error_state = message
        self.error_calls.append(message)

    def replacePluginPropsOnServer(self, props):
        self.pluginProps = dict(props)

    def stateListOrDisplayStateIdChanged(self):
        self.state_list_changed += 1


class _FakeDevices:
    """Minimal ``indigo.devices`` collection supporting ``iter``/indexing."""

    def __init__(self):
        self._devices = {}

    def add(self, device):
        self._devices[device.id] = device
        return device

    def __iter__(self):
        return iter(self._devices.values())

    def __getitem__(self, dev_id):
        return self._devices[dev_id]

    def __contains__(self, dev_id):
        return dev_id in self._devices

    def iter(self, filter=None):  # noqa: A002 - matches Indigo's signature
        return list(self._devices.values())

    def iterkeys(self):
        return list(self._devices.keys())


class _FakePluginBase:
    """Stand-in for indigo.PluginBase, just enough to subclass and construct."""

    class StopThread(Exception):
        pass

    # Tests can inject per-type base state lists to exercise dedupe.
    _base_state_lists = {}

    def __init__(self, plugin_id, plugin_display_name, plugin_version, plugin_prefs):
        import logging
        self.pluginId = plugin_id
        self.pluginDisplayName = plugin_display_name
        self.pluginVersion = plugin_version
        self.pluginPrefs = plugin_prefs
        self.logger = logging.getLogger("Plugin")

    def sleep(self, seconds):
        raise self.StopThread()

    def getDeviceStateList(self, dev):
        return list(self._base_state_lists.get(dev.deviceTypeId, []))

    def getDeviceStateDictForStringType(self, stateId, triggerLabel, controlPageLabel):  # noqa: N803
        return {"Key": stateId, "Type": "String",
                "TriggerLabel": triggerLabel, "ControlPageLabel": controlPageLabel}


def _install_fake_indigo():
    fake = types.ModuleType("indigo")
    fake.PluginBase = _FakePluginBase
    fake.Device = _FakeDevice
    fake.devices = _FakeDevices()
    fake.Dict = dict
    fake.List = list
    sys.modules.setdefault("indigo", fake)


_install_fake_indigo()
