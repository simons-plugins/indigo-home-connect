"""Phase 0 smoke tests: the plugin skeleton imports and constructs."""
import xml.etree.ElementTree as ET
from unittest.mock import Mock

from conftest import SERVER_PLUGIN_DIR


def test_plugin_imports_and_constructs():
    import plugin

    p = plugin.Plugin("com.simons-plugins.homeconnect", "Home Connect", "2026.0.1", {})
    assert p.debug is False
    p.startup()
    p.runConcurrentThread()  # StopThread raised by fake sleep, caught by loop
    p.shutdown()


def test_startup_logs_version_and_device_count():
    import plugin

    p = plugin.Plugin("com.simons-plugins.homeconnect", "Home Connect", "2026.1.0", {})
    p.logger = Mock()
    p.startup()
    summary = " ".join(str(c) for c in p.logger.info.call_args_list)
    assert "2026.1.0" in summary and "device(s) configured" in summary
    p.shutdown()


def test_debug_pref_honoured():
    import plugin

    p = plugin.Plugin("id", "name", "v", {"showDebugInfo": True})
    assert p.debug is True


def test_xml_files_are_well_formed():
    for name in ("Devices.xml", "Actions.xml", "PluginConfig.xml"):
        ET.parse(SERVER_PLUGIN_DIR / name)
