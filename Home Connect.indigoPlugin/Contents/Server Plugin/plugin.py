"""Home Connect (Bosch/Siemens) plugin for Indigo.

Surfaces Home Connect appliances as Indigo devices via the cloud API
(https://api-docs.home-connect.com): live state over the SSE event stream,
control over REST.

Phase 1 — OAuth (Device Flow + simulator), token store, request/rate-limit
client. Phase 2 — the SSE event stream + per-appliance state engine, owned by
:class:`~hc_events.HomeConnectCoordinator`. All Indigo-touching code lives here;
the ``hc_*`` modules are pure stdlib and never import ``indigo``.
"""
import os
import threading

import indigo

import hc_constants as hc
from device_bridge import ApplianceBridge
from hc_api import HomeConnectAPI, HomeConnectError, PROD_HOST, SIMULATOR_HOST, redact
from hc_auth import (HomeConnectAuth, STATE_AUTHORIZED, STATE_PENDING,
                     STATE_AUTH_REQUIRED, STATE_UNAUTHORIZED)
from hc_events import HomeConnectCoordinator

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
        self._coordinator = None
        self._coordinator_api = None      # the api the running coordinator is wired to
        # Guards the api/auth/coordinator triad against races between the config
        # UI thread (rebuild) and runConcurrentThread (reconcile every 60s).
        self._coord_lock = threading.RLock()
        # dev.id -> ApplianceBridge for every enabled Home Connect device.
        self._bridges = {}
        # Guards _bridges against the worker thread (appliance discovery) racing
        # the main thread (deviceStartComm/deviceStopComm).
        self._dev_lock = threading.RLock()

    # -- Lifecycle -----------------------------------------------------------
    def startup(self):
        self.logger.info("Home Connect plugin starting")
        self._rebuild_client()
        self._reconcile_coordinator()

    def shutdown(self):
        self._stop_auth.set()
        with self._coord_lock:
            self._stop_coordinator()
        self.logger.info("Home Connect plugin stopped")

    def runConcurrentThread(self):
        try:
            while True:
                if self._auth and self._auth.is_authorized():
                    try:
                        self._auth.refresh_if_needed()
                    except Exception as exc:  # pylint: disable=broad-except
                        self.logger.exception(exc)
                # Bring the event stream up once authorized (or down if not).
                self._reconcile_coordinator()
                self.sleep(60)
        except self.StopThread:
            pass

    # -- SSE coordinator -----------------------------------------------------
    def _reconcile_coordinator(self):
        """Bring the coordinator in line with auth + the current client.

        Runs the stream only while fully authorized (a lost/invalid grant leaves
        the store entry in place but flips state to AUTH_REQUIRED, so we key off
        ``state()``, not ``is_authorized()``). If a coordinator is running but
        wired to a superseded api/auth (prefs changed), it is restarted against
        the current one — never leaving two streams live.
        """
        with self._coord_lock:
            authorized = bool(self._auth) and self._auth.state() == STATE_AUTHORIZED
            if not authorized:
                self._stop_coordinator()
                return
            if self._coordinator is not None and self._coordinator_api is not self._api:
                if not self._stop_coordinator():
                    return                # old one still winding down; retry next cycle
            if self._coordinator is None:
                self._start_coordinator()

    def _start_coordinator(self):
        # The coordinator calls _appliance_discovered once per newly-seen
        # appliance (on the worker thread) so device bridges attach whether the
        # device was created before or after discovery. Capture this api/auth so
        # a later prefs change can detect the stale wiring.
        api, auth = self._api, self._auth
        coordinator = HomeConnectCoordinator(
            api, logger=self.logger, on_appliance=self._appliance_discovered,
            auth_ok=lambda: auth.state() == STATE_AUTHORIZED)
        try:
            if coordinator.start():
                self._coordinator = coordinator
                self._coordinator_api = api
        except Exception as exc:  # pylint: disable=broad-except
            self.logger.exception(exc)

    def _stop_coordinator(self):
        """Stop the coordinator; return True only if it fully stopped. On a
        stuck reader thread it stays referenced so we never abandon a live
        stream (and never start a second one on top)."""
        if self._coordinator is None:
            return True
        try:
            stopped = self._coordinator.stop()
        except Exception as exc:  # pylint: disable=broad-except
            self.logger.exception(exc)
            stopped = False
        if stopped:
            self._coordinator = None
            self._coordinator_api = None
        return stopped

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
        with self._coord_lock:
            if self._auth is not None:
                self._auth.mark_stale()   # a late worker from the old instance must not persist
            # Tear down any running stream; it is bound to the old api/host. The
            # supervisor (or startup) restarts it against the rebuilt client.
            self._stop_coordinator()
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
        if self._auth is not None:
            self._auth.mark_stale()       # old worker must not persist a superseded result

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
        self._reconcile_coordinator()

    # -- Device lifecycle ----------------------------------------------------
    def deviceStartComm(self, dev):  # noqa: N803
        """Create a bridge for the device and attach it to its appliance.

        Works whether the appliance is already discovered (attach now) or turns
        up later (the coordinator's PAIRED/discovery hook attaches it via
        :meth:`_appliance_discovered`)."""
        haid = (dev.pluginProps or {}).get("haId")
        treat_off = _as_bool(dev.pluginProps.get("offWhenDisconnected", True))
        bridge = ApplianceBridge(dev, dev.deviceTypeId, logger=self.logger,
                                 treat_disconnected_as_off=treat_off)
        with self._dev_lock:
            self._bridges[dev.id] = bridge
        appliance = self._find_appliance(haid)
        if appliance is not None:
            bridge.attach(appliance)
        else:
            bridge.mark_waiting()
        self.logger.info("Home Connect device '%s' started (haId=%s)", dev.name, redact(haid))

    def deviceStopComm(self, dev):  # noqa: N803
        """Detach and drop the bridge so the device stops receiving writes."""
        with self._dev_lock:
            bridge = self._bridges.pop(dev.id, None)
        if bridge is not None:
            bridge.detach()

    def didDeviceCommPropertyChange(self, origDev, newDev):  # noqa: N803
        """Only restart the device on an haId or off/disconnected-policy change."""
        old, new = origDev.pluginProps, newDev.pluginProps
        return (old.get("haId") != new.get("haId")
                or old.get("offWhenDisconnected") != new.get("offWhenDisconnected"))

    # -- Discovery -> bridge attachment (worker thread) ----------------------
    def _appliance_discovered(self, appliance):
        with self._dev_lock:
            bridges = [b for b in self._bridges.values() if b.haid == appliance.haid]
        for bridge in bridges:
            bridge.attach(appliance)

    def _find_appliance(self, haid):
        if not haid:
            return None
        with self._coord_lock:
            coordinator = self._coordinator
        if coordinator is None:
            return None
        for appliance in coordinator.appliances():
            if appliance.haid == haid:
                return appliance
        return None

    # -- Dynamic state list --------------------------------------------------
    def getDeviceStateList(self, dev):  # noqa: N803
        """Base XML states plus dynamic string states for undocumented keys.

        Dynamic state IDs are persisted in ``dev.pluginProps['dynamicStateKeys']``
        by the bridge, so this survives a bridge that hasn't started yet."""
        state_list = list(indigo.PluginBase.getDeviceStateList(self, dev) or [])
        existing = {self._state_key(entry) for entry in state_list}
        dynamic = (dev.pluginProps or {}).get("dynamicStateKeys", "")
        for sid in sorted(part for part in dynamic.split(",") if part):
            if sid not in existing:
                state_list.append(self.getDeviceStateDictForStringType(sid, sid, sid))
                existing.add(sid)
        return state_list

    @staticmethod
    def _state_key(entry):
        if isinstance(entry, dict):
            return entry.get("Key") or entry.get("key")
        return entry

    def getDeviceDisplayStateId(self, dev):  # noqa: N803
        return "status"

    # -- Device ConfigUI -----------------------------------------------------
    def listAppliances(self, filter="", valuesDict=None, typeId="", targetId=0):  # noqa: A002,N803
        """Dynamic menu of discovered appliances, filtered to the type.

        The generic ``homeConnectAppliance`` type lists every appliance; the
        specific types list only matching BSH appliance types. haIds already
        assigned to another device are marked ``(in use)`` but stay selectable."""
        accepted = hc.DEVICE_TYPE_TO_APPLIANCE_TYPES.get(typeId, None)
        with self._coord_lock:
            coordinator = self._coordinator
        if coordinator is None:
            return []
        used = self._used_haids(exclude_dev_id=targetId)
        options = []
        for appliance in coordinator.appliances():
            if accepted is not None and appliance.type not in accepted:
                continue
            label = f"{appliance.name} ({appliance.type or 'appliance'})"
            if appliance.haid in used:
                label += " (in use)"
            options.append((appliance.haid, label))
        options.sort(key=lambda item: item[1].lower())
        return options

    def _used_haids(self, exclude_dev_id=0):
        used = set()
        for dev in indigo.devices.iter("self"):
            if dev.id == exclude_dev_id:
                continue
            haid = (dev.pluginProps or {}).get("haId")
            if haid:
                used.add(haid)
        return used

    def validateDeviceConfigUi(self, valuesDict, typeId, devId):  # noqa: N803
        if not valuesDict.get("haId"):
            errors = indigo.Dict()
            errors["haId"] = "Select a Home Connect appliance."
            return (False, valuesDict, errors)
        return (True, valuesDict)

    # -- Menu items ----------------------------------------------------------
    def logDiscoveredAppliances(self):
        with self._coord_lock:
            coordinator = self._coordinator
        if coordinator is None:
            self.logger.info("Home Connect: no event stream running (authorize the plugin first).")
            return
        appliances = coordinator.appliances()
        if not appliances:
            self.logger.info("Home Connect: no appliances discovered yet.")
            return
        self.logger.info("Home Connect discovered %d appliance(s):", len(appliances))
        for appliance in appliances:
            self.logger.info("  %s [%s] haId=%s connected=%s",
                             appliance.name, appliance.type or "?",
                             redact(appliance.haid), appliance.connected)


def _as_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "on", "yes", "1")
    return bool(value)
