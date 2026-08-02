"""Bridges a :class:`~hc_appliance.HomeConnectAppliance` to one Indigo device.

The bridge is the single place appliance state turns into Indigo state writes.
It subscribes to the appliance's observer API (Phase 2) and, on every change,
pushes a batched ``updateStatesOnServer`` to the injected device object — mapped
states from :mod:`hc_constants`, a computed ``status`` summary uiValue, and any
undocumented keys as dynamic string states (PRD §4).

Design constraints:

* The device is **injected**, so tests drive the bridge with the conftest fake
  and this module never imports ``indigo``. Every Indigo touch is a method call
  on the injected object (``updateStatesOnServer`` / ``setErrorStateOnServer`` /
  ``replacePluginPropsOnServer`` / ``stateListOrDisplayStateIdChanged``).
* Observer callbacks arrive on the coordinator worker thread. A single lock is
  held across the whole of :meth:`ApplianceBridge.push` — snapshot **and** the
  ``updateStatesOnServer`` write — and across ``attach``/``detach``, so two
  pushes serialise (no out-of-order batch) and a ``detach`` cannot land mid-push
  (no write to a stopped device). The write re-checks ``_active`` afterwards so
  a detach triggered *by* the write itself suppresses the trailing policy write.
* Off-vs-disconnected is a per-device policy (PRD §3.7): treat a DISCONNECTED
  appliance as "Off" (clear error) or surface it as a device error state.
"""
import logging
import threading
from datetime import datetime, timezone

from hc_api import redact
import hc_constants as hc
from hc_appliance import CONNECTED, OPERATION_STATE

# Operation-state tails treated as "running" — the summary then shows the
# program + remaining time; otherwise it collapses to a single word.
_RUNNING_STATES = frozenset({"Run", "DelayedStart", "Pause", "ActionRequired", "Aborting"})

# Friendlier labels for the running-state word in the summary.
_OP_LABEL = {
    "Run": "Run",
    "DelayedStart": "Delayed start",
    "Pause": "Paused",
    "ActionRequired": "Action required",
    "Aborting": "Stopping",
    "Finished": "Finished",
    "Ready": "Ready",
    "Inactive": "Off",
}

# Program key roots stored by the state engine (hc_appliance).
_ACTIVE_PROGRAM = "BSH.Common.Root.ActiveProgram"
_SELECTED_PROGRAM = "BSH.Common.Root.SelectedProgram"
_POWER_STATE = "BSH.Common.Setting.PowerState"
_REMAINING = "BSH.Common.Option.RemainingProgramTime"

_DYNAMIC_PROP = "dynamicStateKeys"


def summarize_status(snapshot, treat_disconnected_as_off, last_event=None):
    """Compute the one-line ``status`` summary uiValue for a state snapshot.

    Running -> ``"Run · Eco 50 · 1:24 remaining"`` (program / remaining only when
    present). Otherwise, in priority order: ``Off`` / ``Standby`` (from power or
    an Inactive operation state), a mapped label for a known operation state
    (``Ready``, ``Finished``), the **raw operation-state tail** for any
    unmapped-but-present state, the ``last_event`` label as a fallback, and
    finally ``Ready``. A disconnected appliance is ``Off`` or ``Disconnected``
    per the policy.
    """
    if snapshot.get(CONNECTED) is False:
        return "Off" if treat_disconnected_as_off else "Disconnected"

    op = hc.enum_tail(snapshot.get(OPERATION_STATE))
    power = hc.enum_tail(snapshot.get(_POWER_STATE))

    if op in _RUNNING_STATES:
        parts = [_OP_LABEL.get(op, op)]
        program = _program_short(snapshot)
        if program:
            parts.append(program)
        remaining = snapshot.get(_REMAINING)
        if remaining is not None and hc.to_int(remaining, 0) > 0:
            parts.append(f"{hc.format_remaining(remaining)} remaining")
        return " · ".join(parts)

    if power == "Off":
        return "Off"
    if power == "Standby":
        return "Standby"
    if op == "Inactive":
        return "Off"
    if op:
        return _OP_LABEL.get(op, op)
    if last_event:
        return last_event
    return "Ready"


def _program_short(snapshot):
    key = snapshot.get(_ACTIVE_PROGRAM) or snapshot.get(_SELECTED_PROGRAM)
    return hc.prettify_program(hc.enum_tail(key)) if key else None


class ApplianceBridge:
    """Owns the appliance->device state sync for one Indigo device."""

    def __init__(self, device, device_type_id, logger=None,
                 treat_disconnected_as_off=True, now=None):
        self.device = device
        self.haid = (device.pluginProps or {}).get("haId")
        self.device_type_id = device_type_id
        self._logger = logger or logging.getLogger("device_bridge")
        self._treat_disconnected_as_off = treat_disconnected_as_off
        self._now = now or (lambda: datetime.now(timezone.utc))

        self._lock = threading.RLock()
        self._appliance = None
        self._active = False
        self._orphaned = False
        self._base_ids = hc.state_ids_for(device_type_id)
        self._dynamic_ids = set(_split_csv((device.pluginProps or {}).get(_DYNAMIC_PROP, "")))
        self._last_event = None
        self._last_event_time = None

    @property
    def is_attached(self):
        with self._lock:
            return self._active

    # -- Lifecycle -----------------------------------------------------------
    def attach(self, appliance):
        """Subscribe to ``appliance`` and push its current state immediately.

        Clears carried per-appliance state (last event) and the orphaned flag so
        a device reconfigured onto a different haId does not show the previous
        appliance's last event."""
        with self._lock:
            if self._appliance is appliance and self._active:
                return
            self._appliance = appliance
            self._active = True
            self._orphaned = False
            self._last_event = None
            self._last_event_time = None
        appliance.subscribe(None, self._on_change)
        self.push()

    def detach(self):
        """Unsubscribe and stop writing — a late callback becomes a no-op."""
        with self._lock:
            appliance = self._appliance
            self._appliance = None
            self._active = False
        if appliance is not None:
            appliance.unsubscribe(None, self._on_change)

    def mark_waiting(self):
        """Show a holding state before the appliance has been discovered."""
        try:
            self.device.updateStatesOnServer([
                {"key": "connected", "value": False, "uiValue": "No"},
                {"key": "status", "value": "Waiting", "uiValue": "Waiting for appliance…"},
            ])
        except Exception:  # pylint: disable=broad-except
            self._logger.exception("Home Connect %s: initial waiting-state write failed", self._ctx())

    def mark_orphaned(self):
        """Flag a device whose haId was absent from a full discovery cycle.

        Sets an error state + status so the user sees the appliance is not on the
        account, once (idempotent). If it later appears, ``attach`` clears this."""
        with self._lock:
            if self._orphaned or self._active:
                return
            self._orphaned = True
        self._logger.warning("Home Connect %s: appliance not found on the account", self._ctx())
        try:
            self.device.updateStatesOnServer([
                {"key": "connected", "value": False, "uiValue": "No"},
                {"key": "status", "value": "Not found", "uiValue": "Not found on account"},
            ])
            self.device.setErrorStateOnServer("appliance not found on Home Connect account")
        except Exception:  # pylint: disable=broad-except
            self._logger.exception("Home Connect %s: orphan-state write failed", self._ctx())

    # -- Observer callback (worker thread) -----------------------------------
    def _on_change(self, key, value):
        with self._lock:
            if not self._active:
                return
            if ".Event." in key and hc.event_present_bool(value):
                self._last_event = hc.enum_tail(key)
                self._last_event_time = self._now().isoformat(timespec="seconds")
        self.push()

    # -- State push ----------------------------------------------------------
    def push(self):
        """Recompute and batch-write every state from the appliance snapshot.

        The lock spans snapshot → dynamic registration → write → policy so
        concurrent pushes serialise (no stale batch lands last) and a detach
        cannot interleave with the write (docstring guarantee). After the write
        the ``_active`` re-check drops the trailing policy write if the write
        itself detached the bridge."""
        with self._lock:
            appliance = self._appliance
            if appliance is None:
                return
            snapshot = appliance.state_snapshot()
            updates, new_dynamic = self._build_updates(snapshot)
            if new_dynamic:
                self._register_dynamic(new_dynamic)
            try:
                self.device.updateStatesOnServer(updates)
            except Exception:  # pylint: disable=broad-except
                self._logger.exception("Home Connect %s: state push failed", self._ctx())
            if not self._active:
                return                        # the write detached us; skip policy
            self._apply_connection_policy(snapshot)

    def _ctx(self):
        """Log context for this device — name + redacted haId."""
        return f"{self.device.name} (haId={redact(self.haid)})"

    def _build_updates(self, snapshot):
        updates = []
        new_dynamic = []
        for bsh_key, value in snapshot.items():
            spec = hc.spec_for(self.device_type_id, bsh_key)
            if spec:
                updates.append(self._mapped_update(spec, value))
            else:
                sid = hc.sanitise_state_key(bsh_key)
                if not sid:
                    self._logger.debug("Home Connect %s: dropping unmappable key %r",
                                       self._ctx(), bsh_key)
                    continue
                if sid in self._base_ids:
                    continue
                if sid not in self._dynamic_ids:
                    new_dynamic.append(sid)
                updates.append({"key": sid, "value": _stringify(value), "uiValue": _stringify(value)})

        # Derived: formatted remaining time, last event, and the summary. Keyed on
        # presence (not truthiness) so a null RemainingProgramTime at program end
        # clears the formatted string too.
        if _REMAINING in snapshot:
            formatted = hc.format_remaining(snapshot.get(_REMAINING)) or ""
            updates.append({"key": "remainingTimeFormatted", "value": formatted, "uiValue": formatted})
        # Always emitted (as "" when unset) so an attach-time reset clears any
        # last event carried from a previously-configured appliance (item 7).
        last_event = self._last_event or ""
        last_event_time = self._last_event_time or ""
        updates.append({"key": "lastEvent", "value": last_event, "uiValue": last_event})
        updates.append({"key": "lastEventTime", "value": last_event_time, "uiValue": last_event_time})
        summary = summarize_status(snapshot, self._treat_disconnected_as_off, self._last_event)
        updates.append({"key": "status", "value": summary, "uiValue": summary})
        return updates, new_dynamic

    # Safe cleared value per kind when Home Connect sends null to remove a key
    # (e.g. RemainingProgramTime / ActiveProgram vanish at program end). Real
    # Indigo rejects None for typed states, so clear to the type's empty value.
    _CLEARED = {"enum": "", "int": 0, "num": 0, "bool": False, "event": False}

    @classmethod
    def _mapped_update(cls, spec, value):
        state_id, kind = spec
        if value is None:
            out = cls._CLEARED.get(kind, "")
        elif kind == "enum":
            out = hc.enum_tail(value)
        elif kind == "int":
            out = hc.clamp_percent(value) if state_id == "programProgress" else hc.to_int(value)
        elif kind == "num":
            out = value
        elif kind == "bool":
            out = hc.to_bool(value)
        elif kind == "event":
            out = hc.event_present_bool(value)
        else:
            out = value
        return {"key": state_id, "value": out, "uiValue": _stringify(out)}

    def _register_dynamic(self, new_ids):
        """Persist newly-seen dynamic state IDs and rebuild the device state list.

        Order matters: pluginProps + ``stateListOrDisplayStateIdChanged`` must
        land before the value write so Indigo knows the state exists. On failure
        BOTH the pluginProps write and the in-memory ``_dynamic_ids`` are rolled
        back to their prior values (field notes rule 3) — so the next push retries
        the registration instead of forever emitting an unregistered key. A
        rollback write that itself fails is logged at warning (not swallowed):
        rolled-back and rollback-failed are different states.
        """
        with self._lock:
            previous = set(self._dynamic_ids)
            merged = sorted(self._dynamic_ids | set(new_ids))
            if merged == sorted(previous):
                return
            self._dynamic_ids = set(merged)
        props = dict(self.device.pluginProps)
        before = props.get(_DYNAMIC_PROP, "")
        props[_DYNAMIC_PROP] = ",".join(merged)
        try:
            self.device.replacePluginPropsOnServer(props)
            self.device.stateListOrDisplayStateIdChanged()
        except Exception:  # pylint: disable=broad-except
            # Restore in-memory state too, or registration never retries.
            with self._lock:
                self._dynamic_ids = previous
            self._logger.exception("Home Connect %s: dynamic-state registration failed; "
                                   "will retry on next update", self._ctx())
            rollback = dict(self.device.pluginProps)
            rollback[_DYNAMIC_PROP] = before
            try:
                self.device.replacePluginPropsOnServer(rollback)
            except Exception:  # pylint: disable=broad-except
                self._logger.warning("Home Connect %s: rollback of dynamic-state props "
                                     "also failed", self._ctx())

    def _apply_connection_policy(self, snapshot):
        connected = snapshot.get(CONNECTED)
        try:
            if connected is False and not self._treat_disconnected_as_off:
                self.device.setErrorStateOnServer("Disconnected")
            else:
                self.device.setErrorStateOnServer(None)
        except Exception:  # pylint: disable=broad-except
            self._logger.exception("Home Connect %s: setting error state failed", self._ctx())


def _split_csv(text):
    return [part for part in (text or "").split(",") if part]


def _stringify(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return "Yes" if value else "No"
    return str(value)
