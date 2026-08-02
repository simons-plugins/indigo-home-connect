"""Shared pytest fixtures.

Installs a minimal fake `indigo` module so plugin code imports outside the
Indigo server, and puts the Server Plugin directory on sys.path.
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


class _FakePluginBase:
    """Stand-in for indigo.PluginBase, just enough to subclass and construct."""

    class StopThread(Exception):
        pass

    def __init__(self, plugin_id, plugin_display_name, plugin_version, plugin_prefs):
        import logging
        self.pluginId = plugin_id
        self.pluginDisplayName = plugin_display_name
        self.pluginVersion = plugin_version
        self.pluginPrefs = plugin_prefs
        self.logger = logging.getLogger("Plugin")

    def sleep(self, seconds):
        raise self.StopThread()


def _install_fake_indigo():
    fake = types.ModuleType("indigo")
    fake.PluginBase = _FakePluginBase
    sys.modules.setdefault("indigo", fake)


_install_fake_indigo()
