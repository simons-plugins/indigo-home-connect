"""Guard-rail + execution layer for Home Connect control actions (PRD §2-4).

This is the Phase 4 "don't make the request" firewall. Every control call
(start / stop / pause / resume / select program, set power, set setting, send
command) is pre-flight-checked *locally* against the appliance state cache
before any HTTP is issued, so a request the cache can PROVE BSH would reject
never leaves the plugin — dodging the 10-successive-errors block and the
account lock it can escalate to. Unknown (never-reported) state deliberately
passes through: the appliance stays authoritative and a genuine refusal comes
back as a 409 with an actionable hint. Local refusals raise :class:`ControlRefused` with a
user-actionable ``reason`` string; the plugin logs it as an error the user can
act on ("Remote Start not allowed — enable it on the appliance").

On top of the state pre-flight there are two local sliding-window limiters — five
program starts and five stops per rolling 60 s — that refuse the sixth attempt
*before* any HTTP, matching BSH's hard 5-per-minute limits. A third refusal
family guards the shared request gate: while a 429 ``Retry-After`` block is in
force (>5 s remaining), control calls refuse locally with a wait time and
capability loads fall back to the stale cache, so no Indigo thread ever blocks
out a rate-limit window.

Capability lookups (available programs, available commands, PowerState allowed
values) go through the 24 h :class:`~hc_cache.DiskCache`, so building an action
menu costs at most one live request per appliance the first time a menu is
opened; every later menu build that day is served from the cache.

Never imports ``indigo``; all Indigo-touching code lives in ``plugin.py``. HTTP
goes through an injected :class:`~hc_api.HomeConnectAPI`; the delayed start and
value watches use an injected ``schedule(fn, delay)`` and a monotonic clock so
tests drive them deterministically.
"""
import logging
import threading
import time
from collections import deque

import hc_constants as hc
from hc_api import HomeConnectError, redact
from hc_appliance import (LOCAL_CONTROL, OPERATION_STATE, REMOTE_CONTROL,
                          REMOTE_START, SELECTED_PROGRAM)

# -- BSH keys we drive -------------------------------------------------------
POWER_STATE_KEY = "BSH.Common.Setting.PowerState"
PAUSE_COMMAND = "BSH.Common.Command.PauseProgram"
RESUME_COMMAND = "BSH.Common.Command.ResumeProgram"
OPEN_DOOR_COMMAND = "BSH.Common.Command.OpenDoor"
PARTLY_OPEN_DOOR_COMMAND = "BSH.Common.Command.PartlyOpenDoor"
POWER_ON_VALUE = "BSH.Common.EnumType.PowerState.On"

# Operation-state tails (enum-tail form) that gate stop and start-confirmation.
_STOPPABLE_STATES = frozenset({"DelayedStart", "Run", "Pause", "ActionRequired"})
_STARTED_STATES = frozenset({"DelayedStart", "Run"})
_READY_STATE = "Ready"
_INACTIVE_STATE = "Inactive"
_OFF_STATE = "Off"
_ON_STATE = "On"
_POWERED_OFF_STATES = frozenset({"Off", "Standby"})
# PowerState constraint access values that permit writing On (None = unreported).
_WRITABLE_ACCESS = frozenset({"readWrite", "writeOnly"})

# Local rate limits (PRD §2): five program starts AND five stops per minute.
START_LIMIT = 5
STOP_LIMIT = 5
RATE_WINDOW = 60.0

# How long after a start we allow the appliance to leave Ready before warning.
# 60s (not 15s): real appliances run pre-start water/door checks and the state
# change reaches us over the cloud SSE stream, so a shorter window fired a
# false-negative "did not start" warning on a start that was actually succeeding.
START_WATCH_DELAY = 60.0

# How long auto-power-on waits (event-driven) for OperationState to reach Ready
# after powering an appliance on, before giving up. Injectable for tests.
READY_TIMEOUT = 25.0

# How long a ValueWatch waits for an accepted setting/power/select write to be
# reflected over SSE before warning. Cloud lag can run to minutes (observed
# 11 min burst on real hardware), so like StartWatch this warns with
# uncertainty, not certainty.
VALUE_WATCH_DELAY = 60.0

# Refuse control/capability HTTP locally when the shared request gate is closed
# for longer than this. Real 429 blocks carry Retry-After values from minutes to
# 24 h; blocking an Indigo action or UI thread on that is far worse than
# refusing with a time.
GATE_REFUSE_THRESHOLD = 5.0


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
    appliance has not reached Run/DelayedStart, logs an *uncertainty* warning (the
    start may still begin — a real appliance runs pre-start water/door checks and
    the state change reaches us over the cloud SSE stream, so this is not a
    failure). The window is 60s to avoid the false-negative a shorter window
    produced on real hardware. The timeout body is exception-guarded and always
    unsubscribes in a ``finally``, so the observer never dangles even if the check
    itself throws (e.g. during plugin shutdown).
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
        # Runs on a timer thread. Guard the whole body so an exception (e.g. the
        # appliance torn down during shutdown) still hits the finally and
        # unsubscribes instead of dying on the timer thread and dangling.
        try:
            with self._lock:
                if self._done:
                    return
            current = hc.enum_tail(self._appliance.get(OPERATION_STATE))
            if current not in _STARTED_STATES:
                self._logger.warning(
                    "Home Connect %s: has not reported starting after %ds (operation state %s) — "
                    "it may still begin; if not, check the door, water supply or tank on the "
                    "appliance", self._appliance.name, int(self._delay), current or "unknown")
        except Exception:  # pylint: disable=broad-except
            self._logger.exception("Home Connect %s: start-watch check failed", self._appliance.name)
        finally:
            self._finish()

    def _finish(self):
        with self._lock:
            if self._done:
                return
            self._done = True
        self._appliance.unsubscribe(OPERATION_STATE, self._on_change)


# ---------------------------------------------------------------------------
# One-shot value watch (settings / power / select — the StartWatch analogue)
# ---------------------------------------------------------------------------
class ValueWatch:
    """After an accepted write, warn if the value never takes effect.

    Home Connect 2xx-accepts a PUT and can still drop it appliance-side
    (disconnected mid-flight, local operation, invalid combination) with no
    error response (#18). Registers a one-shot observer on ``key``; an observed
    change to ``expected`` cancels the warning, otherwise the delayed check
    compares the cache and logs a warning naming the discrepancy. Same
    lifecycle discipline as :class:`StartWatch` (exception-guarded timeout,
    unsubscribe in ``finally``)."""

    def __init__(self, appliance, key, expected, schedule, logger=None,
                 delay=VALUE_WATCH_DELAY, describe=None):
        self._appliance = appliance
        self._key = key
        self._expected = expected
        self._describe = describe or hc.enum_tail(key)
        self._logger = logger or logging.getLogger("hc_control")
        self._delay = delay
        self._lock = threading.Lock()
        self._done = False
        appliance.subscribe(key, self._on_change)
        schedule(self._timeout, delay)

    @staticmethod
    def _matches(current, expected):
        return current == expected or str(current) == str(expected)

    def _on_change(self, key, value):  # noqa: ARG002 - key is the subscribed key
        if self._matches(value, self._expected):
            self._finish()

    def _timeout(self):
        try:
            with self._lock:
                if self._done:
                    return
            current = self._appliance.get(self._key)
            if not self._matches(current, self._expected):
                self._logger.warning(
                    "Home Connect %s: %s was accepted but has not been reflected after %ds "
                    "(wanted %s, appliance reports %s) — it may still apply (cloud updates "
                    "can lag several minutes); if not, check the appliance",
                    self._appliance.name, self._describe, int(self._delay),
                    hc.enum_tail(self._expected) or self._expected,
                    hc.enum_tail(current) or current or "nothing")
        except Exception:  # pylint: disable=broad-except
            self._logger.exception("Home Connect %s: value-watch check failed",
                                   self._appliance.name)
        finally:
            self._finish()

    def _finish(self):
        with self._lock:
            if self._done:
                return
            self._done = True
        self._appliance.unsubscribe(self._key, self._on_change)


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

    Blank lines and ``#`` comments are ignored; each remaining line must be a
    ``key=value`` with a non-empty key *and* a non-empty value (value coerced by
    :func:`coerce_value`). A malformed line — no ``=``, empty key, empty value,
    or a duplicate key — raises :class:`ControlRefused` so the user sees exactly
    what to fix rather than the appliance rejecting a garbage/ambiguous option.
    Duplicate keys are refused (not last-wins) so an accidental repeat can never
    silently drop the value the user thinks they set.
    """
    options = []
    seen = set()
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
        if not value.strip():
            raise ControlRefused(f"option override '{key}' has no value after '='")
        if key in seen:
            raise ControlRefused(f"option override '{key}' is set more than once")
        seen.add(key)
        options.append({"key": key, "value": coerce_value(value)})
    return options


# ---------------------------------------------------------------------------
# Controller: guard rails + execution
# ---------------------------------------------------------------------------
class Controller:
    """Executes control actions behind local guard rails and rate limiters."""

    def __init__(self, api, cache, logger=None, now=time.monotonic, ready_timeout=READY_TIMEOUT):
        self._api = api
        self._cache = cache
        self._logger = logger or logging.getLogger("hc_control")
        self._start_limiter = RateLimiter(START_LIMIT, "Program start", now=now)
        self._stop_limiter = RateLimiter(STOP_LIMIT, "Program stop", now=now)
        self._ready_timeout = ready_timeout

    # -- Guard rails (refuse locally, before any HTTP: state-cache checks raise
    # ControlRefused; the capability-gate check raises HomeConnectError so the
    # stale-cache fallback still works) -------------------------------------
    def _require_gate_open(self):
        """Refuse before any HTTP while the client is rate-limited."""
        wait = self._api.gate_wait_remaining()
        if wait > GATE_REFUSE_THRESHOLD:
            minutes = int(wait // 60) + 1
            raise ControlRefused(
                f"Home Connect is rate-limited — try again in about {minutes} minute(s)")

    def _require_capability_gate(self):
        """Loader-side gate check: raises :class:`~hc_api.HomeConnectError` (not
        ControlRefused) so a rate-limited capability load falls back to the
        stale cached value, and a menu build degrades to a logged warning."""
        wait = self._api.gate_wait_remaining()
        if wait > GATE_REFUSE_THRESHOLD:
            raise HomeConnectError(f"rate-limited for another {wait:.0f}s",
                                   status=429, retry_after=wait)

    @staticmethod
    def _require_connected(appliance):
        if not appliance.connected:
            raise ControlRefused(f"{appliance.name} is offline")

    @classmethod
    def _require_remote_start(cls, appliance):
        """Full remote-start pre-flight (PRD §2): connected, remote control on,
        remote start allowed, not locally controlled, OperationState=Ready.

        UNKNOWN (never reported — e.g. right after a plugin restart, before the
        re-read lands) is NOT refused: refusing on None produced a false
        "enable Remote Control on the appliance" for a setting that was fine
        (#19). The request goes through and a genuine refusal comes back as a
        409 with the actionable hint. Only a value the appliance actually
        REPORTED can refuse locally — unknown never does."""
        cls._require_connected(appliance)
        if hc.to_bool(appliance.get(LOCAL_CONTROL)):
            raise ControlRefused(f"{appliance.name} is being controlled at the appliance")
        remote_control = appliance.get(REMOTE_CONTROL)
        if remote_control is not None and not hc.to_bool(remote_control):
            raise ControlRefused(
                f"Remote Control is not active on {appliance.name} — enable it on the appliance")
        remote_start = appliance.get(REMOTE_START)
        if remote_start is not None and not hc.to_bool(remote_start):
            raise ControlRefused(
                f"Remote Start is not allowed on {appliance.name} — enable it on the appliance "
                "(it auto-expires ~24h after you enable it)")
        op_raw = appliance.get(OPERATION_STATE)
        op = hc.enum_tail(op_raw)
        if op_raw is not None and op != _READY_STATE:
            raise ControlRefused(
                f"{appliance.name} is not ready to start (operation state {op})")

    @classmethod
    def _require_powered(cls, appliance):
        """Select needs the appliance connected and not powered off (PRD §2)."""
        cls._require_connected(appliance)
        power = hc.enum_tail(appliance.get(POWER_STATE_KEY))
        if power in _POWERED_OFF_STATES:
            raise ControlRefused(f"{appliance.name} is powered off")

    # -- Auto power-on (dishwashers self-off after a cycle / on door events) --
    @staticmethod
    def _is_powered_off(appliance):
        """True if the appliance looks powered off. ``PowerState=Off`` or
        ``OperationState=Inactive`` are definitive; an off appliance often reports
        neither field, so connected-but-both-absent is also treated as off (the
        On PUT is idempotent and Ready is verified before starting). An explicit
        ``PowerState=On`` is never treated as off."""
        power = hc.enum_tail(appliance.get(POWER_STATE_KEY))
        if power == _OFF_STATE:
            return True
        if power == _ON_STATE:
            return False
        op = hc.enum_tail(appliance.get(OPERATION_STATE))
        if op == _INACTIVE_STATE:
            return True
        return power is None and op is None

    def _ensure_powered_on(self, appliance):
        """If the appliance is off, power it on and wait (event-driven) for Ready.

        Raises :class:`ControlRefused` when power is not remotely writable or the
        appliance never reaches Ready — in both cases the caller must NOT start.
        A no-op (no request) when the appliance is already on."""
        self._require_connected(appliance)
        if not self._is_powered_off(appliance):
            return
        constraints = self.power_constraints(appliance)
        on_value = next((v for v in (constraints.get("allowedvalues") or [])
                         if hc.enum_tail(v) == _ON_STATE), None)
        access = constraints.get("access")
        if on_value is None or (access is not None and access not in _WRITABLE_ACCESS):
            raise ControlRefused(
                f"{appliance.name} is off and cannot be powered on remotely — "
                "press its power button")
        self._logger.info("Home Connect %s: powering on before program (haId=%s)",
                          appliance.name, redact(appliance.haid))
        # Normal retryable PUT — setting PowerState=On is idempotent, so a retry
        # after a lost response is safe (unlike a program start).
        self._api.put_json(f"/api/homeappliances/{appliance.haid}/settings/{POWER_STATE_KEY}",
                           {"data": {"key": POWER_STATE_KEY, "value": on_value}})
        if not self._wait_ready(appliance):
            raise ControlRefused(
                f"{appliance.name} was powered on but did not become Ready within "
                f"{int(self._ready_timeout)}s — check the appliance display")

    def _wait_ready(self, appliance):
        """Block (on the calling thread) until OperationState reaches Ready or the
        timeout elapses. Event-driven via the appliance observer API — no polling.

        Safe from an Indigo action thread: the ``Event`` is set by the SSE
        dispatch thread's ``merge_items`` -> observer callback, which runs without
        holding the appliance lock, so the action thread waiting here cannot
        deadlock the reader (mirrors StartWatch's subscribe pattern in reverse)."""
        if hc.enum_tail(appliance.get(OPERATION_STATE)) == _READY_STATE:
            return True
        ready = threading.Event()

        def on_change(_key, value):
            if hc.enum_tail(value) == _READY_STATE:
                ready.set()

        appliance.subscribe(OPERATION_STATE, on_change)
        try:
            # Re-check after subscribing: Ready may have arrived in the race
            # between the first check and the subscribe.
            if hc.enum_tail(appliance.get(OPERATION_STATE)) == _READY_STATE:
                return True
            return ready.wait(self._ready_timeout)
        finally:
            appliance.unsubscribe(OPERATION_STATE, on_change)

    # -- Capability lookups (24 h cache; one live fetch on a miss) ------------
    def all_programs(self, appliance):
        """Cached list of ALL program dicts (``key`` + localized ``name`` +
        ``constraints``), built from ``GET /programs`` — not ``/programs/available``.

        Field finding: on some appliances the available-list reflects only the
        program currently on the appliance's dial (observed live: a Bosch dryer
        returned 1 available while ``/programs`` returned 13), so it is useless as
        a start-/select-menu source. The caller marks each entry from
        ``constraints.available`` and lets the appliance stay authoritative — it
        rejects an unusable pick with a clear error. Cached under a key distinct
        from any available-list entry."""
        haid = appliance.haid
        return self._cache.get(f"all-programs:{haid}", lambda: self._load_all_programs(haid))

    def available_commands(self, appliance):
        """Cached list of available-command dicts. ``[]`` when unsupported (404)."""
        haid = appliance.haid
        return self._cache.get(f"commands:{haid}", lambda: self._load_commands(haid))

    def power_constraints(self, appliance):
        """Cached PowerState ``constraints`` dict (``allowedvalues`` + ``access``)."""
        haid = appliance.haid
        return self._cache.get(f"power:{haid}", lambda: self._load_power_constraints(haid))

    def power_allowed_values(self, appliance):
        """Cached PowerState ``constraints.allowedvalues`` (``[]`` if unknown)."""
        return self.power_constraints(appliance).get("allowedvalues", []) or []

    def _load_all_programs(self, haid):
        self._require_capability_gate()
        data = self._api.get_json(f"/api/homeappliances/{haid}/programs")
        return (data or {}).get("data", {}).get("programs", []) or []

    def _load_commands(self, haid):
        self._require_capability_gate()
        try:
            data = self._api.get_json(f"/api/homeappliances/{haid}/commands")
        except HomeConnectError as exc:
            if exc.status == 404 or exc.key == "404":
                return []                     # /commands unsupported -> no commands
            raise
        return (data or {}).get("data", {}).get("commands", []) or []

    def _load_power_constraints(self, haid):
        self._require_capability_gate()
        data = self._api.get_json(f"/api/homeappliances/{haid}/settings/{POWER_STATE_KEY}")
        return (data or {}).get("data", {}).get("constraints", {}) or {}

    # -- Control operations --------------------------------------------------
    def start_program(self, appliance, program_key, options=None, power_on_first=False):
        """PUT /programs/active behind the full remote-start pre-flight + limiter.

        The PUT is issued with ``no_retry`` so a lost response can never
        double-start the appliance (PRD §3.3). When ``power_on_first`` and the
        appliance is off, it is powered on (waiting for Ready) BEFORE the guard
        rails run — remote-start flags only mean anything once the appliance is
        on. The power-on PUT is not a program start; the limiter counts once."""
        if not program_key:
            raise ControlRefused("no program selected to start")
        self._require_gate_open()
        if power_on_first:
            self._ensure_powered_on(appliance)
        self._require_remote_start(appliance)
        self._start_limiter.acquire()
        payload = {"data": {"key": program_key, "options": list(options or [])}}
        self._logger.info("Home Connect %s: starting program %s (haId=%s)",
                          appliance.name, hc.enum_tail(program_key), redact(appliance.haid))
        self._api.put_json(f"/api/homeappliances/{appliance.haid}/programs/active",
                           payload, no_retry=True)

    def select_program(self, appliance, program_key, options=None, power_on_first=False):
        """PUT /programs/selected — connected + powered, no remote-start needed.

        When ``power_on_first`` and the appliance is off, it is powered on (waiting
        for Ready) before the powered check."""
        if not program_key:
            raise ControlRefused("no program chosen to select")
        self._require_gate_open()
        if power_on_first:
            self._ensure_powered_on(appliance)
        self._require_powered(appliance)
        payload = {"data": {"key": program_key, "options": list(options or [])}}
        self._logger.info("Home Connect %s: selecting program %s (haId=%s)",
                          appliance.name, hc.enum_tail(program_key), redact(appliance.haid))
        self._api.put_json(f"/api/homeappliances/{appliance.haid}/programs/selected", payload)

    def stop_program(self, appliance):
        """DELETE /programs/active — only from a stoppable operation state."""
        self._require_gate_open()
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
        self._require_gate_open()
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
        self._require_gate_open()
        self._require_connected(appliance)
        if not setting_key:
            raise ControlRefused("no setting key provided")
        payload = {"data": {"key": setting_key, "value": value}}
        self._logger.info("Home Connect %s: setting %s = %r (haId=%s)",
                          appliance.name, hc.enum_tail(setting_key), value, redact(appliance.haid))
        self._api.put_json(f"/api/homeappliances/{appliance.haid}/settings/{setting_key}", payload)
