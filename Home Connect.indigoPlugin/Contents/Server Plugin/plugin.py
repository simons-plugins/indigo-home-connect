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
import hc_control
from device_bridge import ApplianceBridge
from hc_api import HomeConnectAPI, HomeConnectError, PROD_HOST, SIMULATOR_HOST, redact
from hc_auth import (HomeConnectAuth, STATE_AUTHORIZED, STATE_PENDING,
                     STATE_AUTH_REQUIRED, STATE_UNAUTHORIZED)
from hc_cache import DiskCache
from hc_events import HomeConnectCoordinator

TOKEN_FILENAME = "com.simons-plugins.homeconnect.tokens.json"
CACHE_FILENAME = "com.simons-plugins.homeconnect.capabilities.json"

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
        # Control layer (Phase 4): guard rails + rate limiters + capability cache.
        # Rebuilt with the api whenever the client changes.
        self._controller = None
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
        device_count = len(list(indigo.devices.iter("self")))
        self.logger.info("Home Connect %s started with %d device(s) configured",
                         self.pluginVersion, device_count)

    def shutdown(self):
        self._stop_auth.set()
        with self._coord_lock:
            if self._api is not None:
                self._api.abort()     # unblock any thread waiting out the rate-limit gate
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
                had_coordinator = self._coordinator is not None
                self._stop_coordinator()
                # Auth died while devices were live: surface it on every bridged
                # device — otherwise they freeze at their last healthy-looking
                # state indefinitely and triggers on `connected` never fire.
                if had_coordinator and bool(self._auth) \
                        and self._auth.state() == STATE_AUTH_REQUIRED:
                    self._mark_devices_auth_required()
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
            on_discovery=self._discovery_complete,
            supports_programs_for=_supports_programs_for,
            auth_ok=lambda: auth.state() == STATE_AUTHORIZED)
        try:
            if coordinator.start():
                self._coordinator = coordinator
                self._coordinator_api = api
        except Exception as exc:  # pylint: disable=broad-except
            self.logger.exception(exc)

    def _mark_devices_auth_required(self):
        with self._dev_lock:
            bridges = list(self._bridges.values())
        for bridge in bridges:
            bridge.mark_auth_required()

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

    def _cache_path(self):
        try:
            prefs_dir = os.path.join(indigo.server.getInstallFolderPath(), "Preferences", "Plugins")
        except Exception:  # pylint: disable=broad-except
            prefs_dir = os.path.join(os.path.expanduser("~"), ".indigo-home-connect")
        return os.path.join(prefs_dir, CACHE_FILENAME)

    def _build_client(self, client_id, client_secret, simulator):
        host = SIMULATOR_HOST if simulator else PROD_HOST
        api = HomeConnectAPI(host=host, logger=self.logger)
        auth = HomeConnectAuth(api, self._token_path(), client_id, client_secret,
                               simulator=simulator, logger=self.logger)
        api.set_token_provider(auth.authorization_header)
        api.set_unauthorized_handler(auth.handle_unauthorized)
        # The controller is bound to this api; capability cache is client-agnostic
        # (keyed by haId) so it survives a rebuild via the same on-disk file.
        cache = DiskCache(self._cache_path(), self.pluginVersion, logger=self.logger)
        self._controller = hc_control.Controller(api, cache, logger=self.logger)
        return api, auth

    def _rebuild_client(self):
        client_id = self.pluginPrefs.get("clientId", "").strip()
        client_secret = self.pluginPrefs.get("clientSecret", "").strip()
        simulator = self.pluginPrefs.get("useSimulator", False)
        with self._coord_lock:
            if self._auth is not None and self._auth.matches(client_id, client_secret, simulator):
                # Unchanged client: a no-op supersession. Keep the auth instance
                # (a device-flow authorization may be pending on it — marking it
                # stale would discard the granted token), the running stream and
                # the controller's rate-limiter windows.
                return
            if self._auth is not None:
                self._auth.mark_stale()   # a late worker from the old instance must not persist
            if self._api is not None:
                self._api.abort()         # unblock threads stuck at the old client's gate
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
        with self._coord_lock:
            if self._auth is not None:
                self._auth.mark_stale()   # old worker must not persist a superseded result
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
            valuesDict["authStatus"] = (f"Authorization error: {exc}. Check the Client ID is "
                                        "correct and set to Device Flow, then press Authorize again.")
            self.logger.error("Home Connect authorization error: %s — check the Client ID and that "
                              "the application uses OAuth Device Flow", exc)
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
            # The dialog is documented as closable while a device-flow
            # authorization completes in the background — leave the worker
            # running; the granted token is applied when it arrives. A new
            # Authorize press supersedes it, and the code expires on its own.
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
        treat_off = hc.to_bool(dev.pluginProps.get("offWhenDisconnected", True))
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

    def _discovery_complete(self, known_haids):
        """After a full discovery pass, escalate any device whose configured haId
        was not among the discovered appliances (orphaned haId). Costs no extra
        API requests — it reads the set discovery already produced."""
        with self._dev_lock:
            bridges = list(self._bridges.values())
        for bridge in bridges:
            if not bridge.is_attached and bridge.haid and bridge.haid not in known_haids:
                bridge.mark_orphaned()

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

    # -- Control actions (Phase 4) -------------------------------------------
    def startProgram(self, action, dev=None):  # noqa: N802,N803
        dev, dev_id = self._resolve_action_device(action, dev)
        program = action.props.get("program", "")
        overrides = action.props.get("optionOverrides", "")
        # Default True: existing actions created before this feature have no prop
        # and get the new default (matches the checkbox's defaultValue).
        power_on_first = hc.to_bool(action.props.get("powerOnFirst", True))

        def operation(controller, appliance):
            options = hc_control.parse_options(overrides)   # may raise ControlRefused
            controller.start_program(appliance, program, options, power_on_first=power_on_first)
            # Only after a successful start: watch that OperationState actually
            # leaves Ready — a start can fail appliance-side (door/water) with no
            # error. Reached only if start_program did not raise.
            hc_control.StartWatch(appliance, self._schedule_later, self.logger)

        self._run_control(dev, dev_id, "start program", operation)

    def selectProgram(self, action, dev=None):  # noqa: N802,N803
        dev, dev_id = self._resolve_action_device(action, dev)
        program = action.props.get("program", "")
        overrides = action.props.get("optionOverrides", "")
        power_on_first = hc.to_bool(action.props.get("powerOnFirst", True))

        def operation(controller, appliance):
            options = hc_control.parse_options(overrides)
            controller.select_program(appliance, program, options, power_on_first=power_on_first)

        self._run_control(dev, dev_id, "select program", operation)

    def stopProgram(self, action, dev=None):  # noqa: N802,N803
        dev, dev_id = self._resolve_action_device(action, dev)
        self._run_control(dev, dev_id, "stop program", lambda c, a: c.stop_program(a))

    def pauseProgram(self, action, dev=None):  # noqa: N802,N803
        dev, dev_id = self._resolve_action_device(action, dev)
        self._run_control(dev, dev_id, "pause program", lambda c, a: c.pause_program(a))

    def resumeProgram(self, action, dev=None):  # noqa: N802,N803
        dev, dev_id = self._resolve_action_device(action, dev)
        self._run_control(dev, dev_id, "resume program", lambda c, a: c.resume_program(a))

    def sendCommand(self, action, dev=None):  # noqa: N802,N803
        dev, dev_id = self._resolve_action_device(action, dev)
        command = action.props.get("command", "")
        self._run_control(dev, dev_id, "send command", lambda c, a: c.send_command(a, command))

    def setPowerState(self, action, dev=None):  # noqa: N802,N803
        dev, dev_id = self._resolve_action_device(action, dev)
        value = action.props.get("powerState", "")
        self._run_control(dev, dev_id, "set power state", lambda c, a: c.set_power(a, value))

    def setSetting(self, action, dev=None):  # noqa: N802,N803
        dev, dev_id = self._resolve_action_device(action, dev)
        key = action.props.get("settingKey", "").strip()
        value = hc_control.coerce_value(action.props.get("settingValue", ""))
        self._run_control(dev, dev_id, "set setting", lambda c, a: c.set_setting(a, key, value))

    def _run_control(self, dev, dev_id, describe, operation):
        """Resolve the appliance for ``dev`` and run ``operation(controller, appliance)``.

        Local refusals (:class:`hc_control.ControlRefused`) and API failures are
        logged as user-actionable errors; nothing propagates back into Indigo. A
        missing device is distinguished from an unconfigured action so the log
        tells the user which they have."""
        controller = self._controller
        if controller is None:
            self.logger.error("Home Connect: not authorized yet — cannot %s", describe)
            return
        if dev is None:
            if dev_id:
                self.logger.error("Home Connect: the device (id %s) for '%s' no longer exists",
                                  dev_id, describe)
            else:
                self.logger.error("Home Connect: no device selected to %s", describe)
            return
        appliance = self._appliance_for_device(dev)
        if appliance is None:
            self.logger.error("Home Connect '%s': appliance not available yet — cannot %s",
                              dev.name, describe)
            return
        try:
            operation(controller, appliance)
        except hc_control.ControlRefused as exc:
            self.logger.error("Home Connect '%s': %s", dev.name, exc.reason)
        except HomeConnectError as exc:
            self.logger.error("Home Connect '%s': could not %s — %s",
                              dev.name, describe, _control_error_hint(exc))

    def _schedule_later(self, fn, delay):
        """Run ``fn`` after ``delay`` seconds on a daemon timer (start-watch)."""
        timer = threading.Timer(delay, fn)
        timer.daemon = True
        timer.start()

    def _resolve_action_device(self, action, dev):
        """Return ``(device, dev_id)`` for a control action.

        A deviceFilter action passes the selected ``dev`` straight to the
        callback; ``action.deviceId`` is the defensive fallback. ``(None, id)``
        means the action names a device that no longer exists (deleted);
        ``(None, 0)`` means no device is configured — the caller logs each
        distinctly."""
        if dev is not None:
            return dev, getattr(dev, "id", 0) or 0
        dev_id = _int_or_zero(getattr(action, "deviceId", 0))
        if not dev_id:
            return None, 0
        try:
            return indigo.devices[dev_id], dev_id
        except Exception as exc:  # pylint: disable=broad-except
            self.logger.debug("Home Connect: action device %s not found: %s", dev_id, exc)
            return None, dev_id

    def _appliance_for_device(self, dev):
        if dev is None:
            return None
        return self._find_appliance((dev.pluginProps or {}).get("haId"))

    # -- Action ConfigUI dynamic menus (served from the 24h capability cache) -
    def programListForDevice(self, filter="", valuesDict=None, typeId="", targetId=0):  # noqa: A002,N803,ARG002
        """Program menu for Start / Select, built from the ALL-programs list.

        Uses ``GET /programs`` (not ``/programs/available``) because on some
        appliances the available-list only reflects the dial's current program
        (see hc_control.all_programs). Every program is listed; one whose
        ``constraints.available`` is False is suffixed "(not currently available)"
        — the appliance is authoritative and rejects an unusable pick with a clear
        error. ``execution=none`` programs (not startable at all) are excluded;
        other execution values are not filtered on. Localized ``name`` is used
        when present, falling back to the prettified key tail."""
        controller, appliance = self._menu_appliance(targetId, valuesDict)
        if appliance is None:
            return []
        try:
            programs = controller.all_programs(appliance)
        except HomeConnectError as exc:
            self.logger.warning("Home Connect: could not load programs: %s", exc)
            return []
        options = []
        for program in programs:
            key = program.get("key")
            if not key:
                continue
            constraints = program.get("constraints") or {}
            if constraints.get("execution") == "none":
                continue
            label = program.get("name") or hc.prettify_program(hc.enum_tail(key))
            if constraints.get("available", True) is False:
                label += " (not currently available)"
            options.append((key, label))
        options.sort(key=lambda item: item[1].lower())
        return options

    def commandListForDevice(self, filter="", valuesDict=None, typeId="", targetId=0):  # noqa: A002,N803,ARG002
        controller, appliance = self._menu_appliance(targetId, valuesDict)
        if appliance is None:
            return []
        try:
            commands = controller.available_commands(appliance)
        except HomeConnectError as exc:
            self.logger.warning("Home Connect: could not load available commands: %s", exc)
            return []
        options = []
        for command in commands:
            key = command.get("key")
            if not key:
                continue
            name = command.get("name") or hc.prettify_program(hc.enum_tail(key))
            options.append((key, name))
        options.sort(key=lambda item: item[1].lower())
        return options

    def powerStateListForDevice(self, filter="", valuesDict=None, typeId="", targetId=0):  # noqa: A002,N803,ARG002
        controller, appliance = self._menu_appliance(targetId, valuesDict)
        if appliance is None:
            return []
        try:
            allowed = controller.power_allowed_values(appliance)
        except HomeConnectError as exc:
            self.logger.warning("Home Connect: could not load power-state options: %s", exc)
            return []
        return [(value, hc.enum_tail(value)) for value in allowed if value]

    def _menu_appliance(self, targetId, valuesDict):  # noqa: N803
        """Resolve ``(controller, appliance)`` for an action ConfigUI dynamic menu.

        The device a device-scoped action targets arrives as ``targetId`` (the
        object being edited — the contract the E2E-validated indigo-matter plugin
        relies on); ``valuesDict['deviceId']`` is a defensive fallback only. Every
        empty-return path is logged so a blank picker is diagnosable rather than
        silent."""
        controller = self._controller
        if controller is None:
            self.logger.debug("Home Connect menu: no controller yet (not authorized)")
            return None, None
        dev_id = _int_or_zero(targetId)
        if not dev_id and valuesDict:
            dev_id = _int_or_zero(valuesDict.get("deviceId"))
        if not dev_id:
            self.logger.debug("Home Connect menu: no target device selected yet")
            return controller, None
        try:
            dev = indigo.devices[dev_id]
        except Exception as exc:  # pylint: disable=broad-except
            self.logger.warning("Home Connect menu: device %s not found: %s", dev_id, exc)
            return controller, None
        appliance = self._appliance_for_device(dev)
        if appliance is None:
            self.logger.debug("Home Connect menu: appliance for device %s not discovered yet", dev_id)
        return controller, appliance

    def validateActionConfigUi(self, valuesDict, typeId, deviceId):  # noqa: N802,N803,ARG002
        errors = indigo.Dict()
        if typeId in ("startProgram", "selectProgram"):
            if not valuesDict.get("program"):
                errors["program"] = "Choose a program."
            try:
                hc_control.parse_options(valuesDict.get("optionOverrides", ""))
            except hc_control.ControlRefused as exc:
                errors["optionOverrides"] = exc.reason
        elif typeId == "sendCommand" and not valuesDict.get("command"):
            errors["command"] = "Choose a command."
        elif typeId == "setPowerState" and not valuesDict.get("powerState"):
            errors["powerState"] = "Choose a power state."
        elif typeId == "setSetting" and not valuesDict.get("settingKey", "").strip():
            errors["settingKey"] = "Enter a setting key."
        if len(errors) > 0:
            return (False, valuesDict, errors)
        return (True, valuesDict)


def _control_error_hint(exc):
    """Turn a control-call API failure into a message that says what to DO.

    The BSH error text on its own ("HTTP 409 …") tells the user nothing
    actionable, so the documented control statuses (PRD §6) map to a concrete
    next step. Anything else falls back to the raw error string."""
    if exc.status == 409:
        return (f"the appliance refused it ({exc}). Check its door is shut, that Remote Control "
                "and Remote Start are still enabled on the appliance, and that no one is operating "
                "it directly")
    if exc.status == 429:
        return f"Home Connect is rate-limiting ({exc}). Wait a minute, then try again"
    if exc.status == 403:
        return (f"not authorized for this action ({exc}). Re-authorize the plugin so it has the "
                "Control scope")
    return str(exc)


def _int_or_zero(value):
    """Coerce a dynamic-list id (str/int/None) to an int, defaulting to 0."""
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _supports_programs_for(info):
    """Resolve an appliance's program support from its HC ``type`` at discovery.

    Settings-only appliances (fridge/freezer family) return ``False`` so their
    re-read queue skips the selected/active-program endpoints entirely (PRD §3.2
    budget)."""
    return hc.supports_programs(hc.device_type_for((info or {}).get("type")))
