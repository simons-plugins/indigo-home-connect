"""Guard-rail + execution layer for Home Connect control actions (PRD §2-4).

This is the Phase 4 "don't make the request" firewall. Every control call
(start / stop / pause / resume / select program, set power, set setting, send
command) is pre-flight-checked *locally* against the appliance state cache
before any HTTP is issued, so a request that BSH would reject never leaves the
plugin — dodging the 10-successive-errors block and the account lock it can
escalate to. Local refusals raise :class:`ControlRefused` with a
user-actionable ``reason`` string; the plugin logs it as an error the user can
act on ("Remote Start not allowed — enable it on the appliance").

On top of the state pre-flight there are two local token-bucket limiters — five
program starts and five stops per rolling 60 s — that refuse the sixth attempt
*before* any HTTP, matching BSH's hard 5-per-minute limits.

Capability lookups (available programs, available commands, PowerState allowed
values) go through the 24 h :class:`~hc_cache.DiskCache`, so building an action
menu costs at most one live request per appliance per day.

Never imports ``indigo``; all Indigo-touching code lives in ``plugin.py``. HTTP
goes through an injected :class:`~hc_api.HomeConnectAPI`; the delayed start-watch
uses an injected ``schedule(fn, delay)`` and a monotonic clock so tests drive it
deterministically.
"""
import logging
import threading
import time
from collections import deque

import hc_constants as hc
from hc_api import HomeConnectError, redact
from hc_appliance import (LOCAL_CONTROL, OPERATION_STATE, REMOTE_CONTROL,
                          REMOTE_START)

# -- BSH keys we drive -------------------------------------------------------
POWER_STATE_KEY = "BSH.Common.Setting.PowerState"
PAUSE_COMMAND = "BSH.Common.Command.PauseProgram"
RESUME_COMMAND = "BSH.Common.Command.ResumeProgram"
OPEN_DOOR_COMMAND = "BSH.Common.Command.OpenDoor"
PARTLY_OPEN_DOOR_COMMAND = "BSH.Common.Command.PartlyOpenDoor"

# Operation-state tails (enum-tail form) that gate stop and start-confirmation.
_STOPPABLE_STATES = frozenset({"DelayedStart", "Run", "Pause", "ActionRequired"})
_STARTED_STATES = frozenset({"DelayedStart", "Run"})
_READY_STATE = "Ready"
_POWERED_OFF_STATES = frozenset({"Off", "Standby"})

# Local rate limits (PRD §2): five program starts AND five stops per minute.
START_LIMIT = 5
STOP_LIMIT = 5
RATE_WINDOW = 60.0

# How long after a start we allow the appliance to leave Ready before warning.
START_WATCH_DELAY = 15.0


class ControlRefused(Exception):
    """A control call refused locally (never sent). ``reason`` is user-facing."""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


# ---------------------------------------------------------------------------
# Local rate limiter (per plugin, monotonic clock)
# ---------------------------------------------------------------------------
class RateLimiter:
    """A rolling-window limiter: at most ``limit`` acquisitions per ``window``.

    ``acquire`` prunes expired timestamps, refuses (raising
    :class:`ControlRefused`) when the window is full, and otherwise records the
    attempt. Recording on acquire — before the HTTP — means even a request that
    later fails appliance-side still counts, erring toward *fewer* requests,
    which is the safe direction against BSH's hard limit.
    """

    def __init__(self, limit, label, window=RATE_WINDOW, now=time.monotonic):
        self._limit = limit
        self._label = label
        self._window = window
        self._now = now
        self._events = deque()
        self._lock = threading.Lock()

    def acquire(self):
        with self._lock:
            now = self._now()
            while self._events and (now - self._events[0]) >= self._window:
                self._events.popleft()
            if len(self._events) >= self._limit:
                wait = self._window - (now - self._events[0])
                raise ControlRefused(
                    f"{self._label} rate limit reached ({self._limit} per minute) — "
                    f"wait {int(wait) + 1}s and try again")
            self._events.append(now)


# ---------------------------------------------------------------------------
# One-shot start watch (uses the appliance observer API + a delayed check)
# ---------------------------------------------------------------------------
class StartWatch:
    """After a start, warn if OperationState has not left Ready within a delay.

    Registers a one-shot observer on ``OperationState`` (fires as soon as the
    SSE stream reports Run/DelayedStart) and schedules a fallback check. Whichever
    happens first wins: an observed move cancels the warning; the timeout, if the
    appliance is still Ready, logs a warning (a start can fail appliance-side —
    door open, empty water tank — with no error returned to the caller). The
    observer is always unsubscribed so nothing dangles.
    """

    def __init__(self, appliance, schedule, logger=None, delay=START_WATCH_DELAY):
        self._appliance = appliance
        self._logger = logger or logging.getLogger("hc_control")
        self._delay = delay
        self._lock = threading.Lock()
        self._done = False
        appliance.subscribe(OPERATION_STATE, self._on_change)
        schedule(self._timeout, delay)

    def _on_change(self, key, value):  # noqa: ARG002 - key is always OPERATION_STATE
        if hc.enum_tail(value) in _STARTED_STATES:
            self._finish()

    def _timeout(self):
        with self._lock:
            if self._done:
                return
        current = hc.enum_tail(self._appliance.get(OPERATION_STATE))
        if current not in _STARTED_STATES:
            self._logger.warning(
                "Home Connect %s: program did not start within %ds (operation state %s) — "
                "check the door, water supply or tank on the appliance",
                self._appliance.name, int(self._delay), current or "unknown")
        self._finish()

    def _finish(self):
        with self._lock:
            if self._done:
                return
            self._done = True
        self._appliance.unsubscribe(OPERATION_STATE, self._on_change)


# ---------------------------------------------------------------------------
# Value coercion for freeform option / setting fields
# ---------------------------------------------------------------------------
def coerce_value(raw):
    """Coerce a freeform text value to Bool / Int / String (in that order).

    ``true``/``false`` (any case) -> bool; a whole number -> int; anything else
    is left as the trimmed string. Deliberately no float — BSH options are
    enums, durations (int seconds) or booleans (PRD §4 option handling).
    """
    text = raw.strip()
    low = text.lower()
    if low in ("true", "false"):
        return low == "true"
    try:
        return int(text)
    except ValueError:
        return text


def parse_options(text):
    """Parse a ``key=value`` per-line block into an options list.

    Blank lines and ``#`` comments are ignored; each remaining line must be
    ``key=value`` (value coerced by :func:`coerce_value`). A malformed line
    raises :class:`ControlRefused` so the user sees exactly what to fix rather
    than the appliance rejecting an empty/garbage option.
    """
    options = []
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ControlRefused(f"option override '{line}' must be written as key=value")
        key, _, value = line.partition("=")
        key = key.strip()
        if not key:
            raise ControlRefused(f"option override '{line}' has no key before '='")
        options.append({"key": key, "value": coerce_value(value)})
    return options


# ---------------------------------------------------------------------------
# Controller: guard rails + execution
# ---------------------------------------------------------------------------
class Controller:
    """Executes control actions behind local guard rails and rate limiters."""

    def __init__(self, api, cache, logger=None, now=time.monotonic):
        self._api = api
        self._cache = cache
        self._logger = logger or logging.getLogger("hc_control")
        self._start_limiter = RateLimiter(START_LIMIT, "Program start", now=now)
        self._stop_limiter = RateLimiter(STOP_LIMIT, "Program stop", now=now)

    # -- Guard rails (raise ControlRefused; read only the local state cache) --
    @staticmethod
    def _require_connected(appliance):
        if not appliance.connected:
            raise ControlRefused(f"{appliance.name} is offline")

    @classmethod
    def _require_remote_start(cls, appliance):
        """Full remote-start pre-flight (PRD §2): connected, remote control on,
        remote start allowed, not locally controlled, OperationState=Ready."""
        cls._require_connected(appliance)
        if hc.to_bool(appliance.get(LOCAL_CONTROL)):
            raise ControlRefused(f"{appliance.name} is being controlled at the appliance")
        if not hc.to_bool(appliance.get(REMOTE_CONTROL)):
            raise ControlRefused(
                f"Remote Control is not active on {appliance.name} — enable it on the appliance")
        if not hc.to_bool(appliance.get(REMOTE_START)):
            raise ControlRefused(
                f"Remote Start is not allowed on {appliance.name} — enable it on the appliance "
                "(it auto-expires ~24h after you enable it)")
        op = hc.enum_tail(appliance.get(OPERATION_STATE))
        if op != _READY_STATE:
            raise ControlRefused(
                f"{appliance.name} is not ready to start (operation state {op or 'unknown'})")

    @classmethod
    def _require_powered(cls, appliance):
        """Select needs the appliance connected and not powered off (PRD §2)."""
        cls._require_connected(appliance)
        power = hc.enum_tail(appliance.get(POWER_STATE_KEY))
        if power in _POWERED_OFF_STATES:
            raise ControlRefused(f"{appliance.name} is powered off")

    # -- Capability lookups (24 h cache; one live fetch on a miss) ------------
    def available_programs(self, appliance):
        """Cached list of available-program dicts (``key`` + ``name``)."""
        haid = appliance.haid
        return self._cache.get(f"programs:{haid}", lambda: self._load_programs(haid))

    def available_commands(self, appliance):
        """Cached list of available-command dicts. ``[]`` when unsupported (404)."""
        haid = appliance.haid
        return self._cache.get(f"commands:{haid}", lambda: self._load_commands(haid))

    def power_allowed_values(self, appliance):
        """Cached PowerState ``constraints.allowedvalues`` (``[]`` if unknown)."""
        haid = appliance.haid
        return self._cache.get(f"power:{haid}", lambda: self._load_power_values(haid))

    def _load_programs(self, haid):
        data = self._api.get_json(f"/api/homeappliances/{haid}/programs/available")
        return (data or {}).get("data", {}).get("programs", []) or []

    def _load_commands(self, haid):
        try:
            data = self._api.get_json(f"/api/homeappliances/{haid}/commands")
        except HomeConnectError as exc:
            if exc.status == 404 or exc.key == "404":
                return []                     # /commands unsupported -> no commands
            raise
        return (data or {}).get("data", {}).get("commands", []) or []

    def _load_power_values(self, haid):
        data = self._api.get_json(f"/api/homeappliances/{haid}/settings/{POWER_STATE_KEY}")
        constraints = (data or {}).get("data", {}).get("constraints", {}) or {}
        return constraints.get("allowedvalues", []) or []

    # -- Control operations --------------------------------------------------
    def start_program(self, appliance, program_key, options=None):
        """PUT /programs/active behind the full remote-start pre-flight + limiter.

        The PUT is issued with ``no_retry`` so a lost response can never
        double-start the appliance (PRD §3.3)."""
        if not program_key:
            raise ControlRefused("no program selected to start")
        self._require_remote_start(appliance)
        self._start_limiter.acquire()
        payload = {"data": {"key": program_key, "options": list(options or [])}}
        self._logger.info("Home Connect %s: starting program %s (haId=%s)",
                          appliance.name, hc.enum_tail(program_key), redact(appliance.haid))
        self._api.put_json(f"/api/homeappliances/{appliance.haid}/programs/active",
                           payload, no_retry=True)

    def select_program(self, appliance, program_key, options=None):
        """PUT /programs/selected — connected + powered, no remote-start needed."""
        if not program_key:
            raise ControlRefused("no program chosen to select")
        self._require_powered(appliance)
        payload = {"data": {"key": program_key, "options": list(options or [])}}
        self._logger.info("Home Connect %s: selecting program %s (haId=%s)",
                          appliance.name, hc.enum_tail(program_key), redact(appliance.haid))
        self._api.put_json(f"/api/homeappliances/{appliance.haid}/programs/selected", payload)

    def stop_program(self, appliance):
        """DELETE /programs/active — only from a stoppable operation state."""
        self._require_connected(appliance)
        op = hc.enum_tail(appliance.get(OPERATION_STATE))
        if op not in _STOPPABLE_STATES:
            raise ControlRefused(
                f"{appliance.name} has no program running to stop (operation state {op or 'unknown'})")
        self._stop_limiter.acquire()
        self._logger.info("Home Connect %s: stopping active program (haId=%s)",
                          appliance.name, redact(appliance.haid))
        self._api.request("DELETE", f"/api/homeappliances/{appliance.haid}/programs/active",
                          accept="application/vnd.bsh.sdk.v1+json")

    def pause_program(self, appliance):
        """Pause via PUT /commands/PauseProgram (capability-gated)."""
        self.send_command(appliance, PAUSE_COMMAND)

    def resume_program(self, appliance):
        """Resume via PUT /commands/ResumeProgram (capability-gated)."""
        self.send_command(appliance, RESUME_COMMAND)

    def send_command(self, appliance, command_key):
        """PUT /commands/{key} — only if the command is in the available set."""
        self._require_connected(appliance)
        available = {c.get("key") for c in self.available_commands(appliance)}
        if command_key not in available:
            raise ControlRefused(
                f"{appliance.name} does not support the {hc.enum_tail(command_key)} command")
        payload = {"data": {"key": command_key, "value": True}}
        self._logger.info("Home Connect %s: sending command %s (haId=%s)",
                          appliance.name, hc.enum_tail(command_key), redact(appliance.haid))
        self._api.put_json(f"/api/homeappliances/{appliance.haid}/commands/{command_key}", payload)

    def set_power(self, appliance, value):
        """PUT PowerState — only to a value the appliance's constraints allow.

        A read-only power appliance (e.g. the Bosch dryer, On-only) refuses any
        Off/Standby locally because those values are absent from ``allowedvalues``.
        """
        self._require_connected(appliance)
        allowed = self.power_allowed_values(appliance)
        if allowed and value not in allowed:
            raise ControlRefused(
                f"{appliance.name} does not allow power state {hc.enum_tail(value)} "
                f"(allowed: {', '.join(hc.enum_tail(v) for v in allowed) or 'none'})")
        self.set_setting(appliance, POWER_STATE_KEY, value)

    def set_setting(self, appliance, setting_key, value):
        """PUT /settings/{key} — advanced/freeform; data.key matches the path key."""
        self._require_connected(appliance)
        if not setting_key:
            raise ControlRefused("no setting key provided")
        payload = {"data": {"key": setting_key, "value": value}}
        self._logger.info("Home Connect %s: setting %s = %r (haId=%s)",
                          appliance.name, hc.enum_tail(setting_key), value, redact(appliance.haid))
        self._api.put_json(f"/api/homeappliances/{appliance.haid}/settings/{setting_key}", payload)
