"""Home Connect (Bosch/Siemens) plugin for Indigo.

Surfaces Home Connect appliances as Indigo devices via the cloud API
(https://api-docs.home-connect.com): live state over the SSE event stream,
control over REST.

Phase 0 scaffold — lifecycle skeleton only.
"""
import indigo


class Plugin(indigo.PluginBase):
    """Main plugin class."""

    def __init__(self, plugin_id, plugin_display_name, plugin_version, plugin_prefs):
        super().__init__(plugin_id, plugin_display_name, plugin_version, plugin_prefs)
        self.debug = plugin_prefs.get("showDebugInfo", False)

    def startup(self):
        self.logger.info("Home Connect plugin starting")

    def shutdown(self):
        self.logger.info("Home Connect plugin stopped")

    def runConcurrentThread(self):
        try:
            while True:
                self.sleep(60)
        except self.StopThread:
            pass

    def closedPrefsConfigUi(self, values_dict, user_cancelled):
        if not user_cancelled:
            self.debug = values_dict.get("showDebugInfo", False)
