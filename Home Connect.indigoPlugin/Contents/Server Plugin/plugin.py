"""Home Connect (Bosch/Siemens) plugin for Indigo.

Surfaces Home Connect appliances as Indigo devices via the cloud API
(https://api-docs.home-connect.com): live state over the SSE event stream,
control over REST.

Phase 1 — OAuth (Device Flow + simulator), token store, request/rate-limit
client. All Indigo-touching code lives here; the ``hc_*`` modules are pure
stdlib and never import ``indigo``.
"""
import os
import threading

import indigo

from hc_api import HomeConnectAPI, HomeConnectError, PROD_HOST, SIMULATOR_HOST, redact
from hc_auth import (HomeConnectAuth, STATE_AUTHORIZED, STATE_PENDING,
                     STATE_AUTH_REQUIRED, STATE_UNAUTHORIZED)

TOKEN_FILENAME = "com.simons-plugins.homeconnect.tokens.json"

_STATE_TEXT = {
    STATE_AUTHORIZED: "Authorized.",
    STATE_PENDING: "Authorization in progress — check the Event Log for the link.",
    STATE_AUTH_REQUIRED: "Authorization required — please Authorize again.",
    STATE_UNAUTHORIZED: "Not authorized.",
}


class Plugin(indigo.PluginBase):
    """Main plugin class."""

    def __init__(self, plugin_id, plugin_display_name, plugin_version, plugin_prefs):
        super().__init__(plugin_id, plugin_display_name, plugin_version, plugin_prefs)
        self.debug = plugin_prefs.get("showDebugInfo", False)
        self._api = None
        self._auth = None
        self._auth_thread = None
        self._stop_auth = threading.Event()

    # -- Lifecycle -----------------------------------------------------------
    def startup(self):
        self.logger.info("Home Connect plugin starting")
        self._rebuild_client()

    def shutdown(self):
        self._stop_auth.set()
        self.logger.info("Home Connect plugin stopped")

    def runConcurrentThread(self):
        try:
            while True:
                if self._auth and self._auth.is_authorized():
                    try:
                        self._auth.refresh_if_needed()
                    except Exception as exc:  # pylint: disable=broad-except
                        self.logger.exception(exc)
                self.sleep(60)
        except self.StopThread:
            pass

    # -- Client construction -------------------------------------------------
    def _token_path(self):
        try:
            prefs_dir = os.path.join(indigo.server.getInstallFolderPath(), "Preferences", "Plugins")
        except Exception:  # pylint: disable=broad-except
            prefs_dir = os.path.join(os.path.expanduser("~"), ".indigo-home-connect")
        return os.path.join(prefs_dir, TOKEN_FILENAME)

    def _build_client(self, client_id, client_secret, simulator):
        host = SIMULATOR_HOST if simulator else PROD_HOST
        api = HomeConnectAPI(host=host, logger=self.logger)
        auth = HomeConnectAuth(api, self._token_path(), client_id, client_secret,
                               simulator=simulator, logger=self.logger)
        api.set_token_provider(auth.authorization_header)
        api.set_unauthorized_handler(auth.handle_unauthorized)
        return api, auth

    def _rebuild_client(self):
        client_id = self.pluginPrefs.get("clientId", "").strip()
        client_secret = self.pluginPrefs.get("clientSecret", "").strip()
        simulator = self.pluginPrefs.get("useSimulator", False)
        self._api, self._auth = self._build_client(client_id, client_secret, simulator)

    # -- Config UI -----------------------------------------------------------
    def getPrefsUiValues(self, *args, **kwargs):  # pylint: disable=unused-argument
        values = super().getPrefsUiValues(*args, **kwargs) if hasattr(
            super(), "getPrefsUiValues") else self.pluginPrefs
        try:
            state = self._auth.state() if self._auth else STATE_UNAUTHORIZED
            values["authStatus"] = _STATE_TEXT.get(state, "")
        except Exception:  # pylint: disable=broad-except
            pass
        return values

    def authorizeButtonPressed(self, valuesDict, typeId="", devId=0):  # noqa: N803
        client_id = valuesDict.get("clientId", "").strip()
        if not client_id:
            valuesDict["authStatus"] = "Enter your Home Connect Client ID first."
            return valuesDict
        client_secret = valuesDict.get("clientSecret", "").strip()
        simulator = valuesDict.get("useSimulator", False)

        # Stop any in-flight authorization before starting a new one.
        self._stop_auth.set()
        self._stop_auth = threading.Event()

        api, auth = self._build_client(client_id, client_secret, simulator)
        self._api, self._auth = api, auth

        try:
            if simulator:
                auth.authorize_simulator()
                valuesDict["authInstructions"] = ""
                valuesDict["authUserCode"] = ""
                valuesDict["authStatus"] = "Authorized (simulator)."
                self.logger.info("Home Connect authorized against the simulator")
                return valuesDict

            info = auth.start_device_flow()
            uri = info.get("verification_uri_complete") or info.get("verification_uri") or ""
            valuesDict["authInstructions"] = uri
            valuesDict["authUserCode"] = info.get("user_code") or ""
            valuesDict["authStatus"] = "Waiting for you to authorize in the browser…"
            self._start_device_flow_worker(auth, client_id, self._stop_auth)
        except HomeConnectError as exc:
            valuesDict["authStatus"] = f"Authorization error: {exc}"
            self.logger.error("Home Connect authorization error: %s", exc)
        return valuesDict

    def _start_device_flow_worker(self, auth, client_id, stop_event):
        def _worker():
            status, detail = auth.run_device_flow(should_stop=stop_event.is_set)
            if status == "success":
                self.logger.info("Home Connect authorization successful (client %s)", redact(client_id))
            elif status == "denied":
                self.logger.error("Home Connect authorization denied: %s", detail)
            elif status == "cancelled":
                self.logger.info("Home Connect authorization cancelled")
            else:
                self.logger.error("Home Connect authorization failed: %s", detail)

        self._auth_thread = threading.Thread(target=_worker, name="hc-device-flow", daemon=True)
        self._auth_thread.start()

    def validatePrefsConfigUi(self, valuesDict):  # noqa: N803
        return (True, valuesDict)

    def closedPrefsConfigUi(self, valuesDict, userCancelled):  # noqa: N803
        if userCancelled:
            self._stop_auth.set()
            return
        self.debug = valuesDict.get("showDebugInfo", False)
        self._rebuild_client()
