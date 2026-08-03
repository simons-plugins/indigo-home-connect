"""Per-appliance state engine for the Home Connect plugin.

One :class:`HomeConnectAppliance` per ``haId``. It holds a flat ``key -> value``
cache (statuses + settings + program options + a synthetic ``connected`` bool),
an observer API for Phase 3 (Indigo triggers), and a sequential re-read queue
that refreshes appliance state after a (re)connect while never burning the
1000 req/day budget on offline appliances (PRD §3.2).

Design mirrors homebridge-homeconnect's ``HomeConnectDevice`` (ideas, not code):

* STATUS / NOTIFY items and re-read results merge into the cache.
* EVENT items are discrete occurrences and are de-duplicated by
  ``(haId, key, timestamp)`` so a dishwasher re-sending ``ProgramFinished`` on
  reconnect never double-fires downstream (PRD §3.7).
* Synthetic START/STOP (from the SSE reader) drive reconnect re-reads and, when
  the stream stays down, mark the appliance disconnected.

Threading: mutation of the cache/queue is guarded by a lock. Actual network
reads run on the coordinator's single worker thread via an injected
``schedule(fn, delay)`` callable, so this class never blocks the SSE reader.
Never imports ``indigo``.
"""
import collections
import logging
import threading
import time

from hc_api import HomeConnectError

# Synthetic key for the appliance's reachability (not a BSH key).
CONNECTED = "connected"

# Program roots populated from selected/active-program re-reads.
SELECTED_PROGRAM = "BSH.Common.Root.SelectedProgram"
ACTIVE_PROGRAM = "BSH.Common.Root.ActiveProgram"
OPERATION_STATE = "BSH.Common.Status.OperationState"
LOCAL_CONTROL = "BSH.Common.Status.LocalControlActive"
REMOTE_CONTROL = "BSH.Common.Status.RemoteControlActive"
REMOTE_START = "BSH.Common.Status.RemoteControlStartAllowed"

# On a STOP with no error, wait this long for the stream to come back before
# marking the appliance disconnected; on an error STOP, act immediately.
DISCONNECT_DELAY = 3.0

# Exponential backoff for a failed state re-read while still connected.
READ_MIN_DELAY = 5.0
READ_MAX_DELAY = 10 * 60.0
READ_FACTOR = 2.0

# A completed full re-read this recent is fresh enough: stream reconnects and
# CONNECTED flaps within the window skip the whole 3-5 GET pass. Appliances
# that self-power-off (dishwashers) flap DISCONNECTED/CONNECTED by design, and
# an unsuppressed re-read per flap can exhaust the daily budget with zero user
# activity. PAIRED / first discovery force a read regardless.
READ_FRESH_WINDOW = 5 * 60.0

# Bound on remembered EVENT dedupe keys (guards unbounded growth).
MAX_SEEN_EVENTS = 512

# The ordered re-read actions (PRD §3.2).
_READ_BASE = ("appliance", "status", "settings")
_READ_PROGRAMS = ("selected_program", "active_program")


class HomeConnectAppliance:
    """Latest state + observer + reconnect re-read queue for one appliance."""

    def __init__(self, haid, info, reader, schedule, logger=None,
                 monotonic=time.monotonic, supports_programs=True):
        self.haid = haid
        self._info = dict(info or {})
        self._reader = reader                 # ApiReader-like: get_appliance/status/...
        self._schedule = schedule             # schedule(fn, delay) -> runs fn on worker
        self._logger = logger or logging.getLogger("hc_appliance")
        self._monotonic = monotonic
        self._supports_programs = supports_programs

        self._lock = threading.RLock()
        self._state = {}
        self._observers = {}                  # key or None -> [callback]

        # EVENT de-duplication (bounded).
        self._seen_events = set()
        self._seen_order = collections.deque()

        # Re-read queue state.
        self._read_actions = None             # list of pending actions, or None
        self._read_delay = 0.0
        self._read_scheduled = False
        self._last_read_complete = None       # monotonic time of last full pass

        # Generation counter cancels a superseded pending-disconnect job.
        self._disconnect_gen = 0

        self._state[CONNECTED] = bool(self._info.get("connected", False))

    # -- Identity / accessors ------------------------------------------------
    @property
    def type(self):
        return self._info.get("type")

    @property
    def name(self):
        return self._info.get("name") or self._info.get("type") or self.haid

    @property
    def connected(self):
        with self._lock:
            return bool(self._state.get(CONNECTED))

    def info(self):
        with self._lock:
            return dict(self._info)

    def get(self, key, default=None):
        with self._lock:
            return self._state.get(key, default)

    def state_snapshot(self):
        with self._lock:
            return dict(self._state)

    # -- Observer API (Phase 3) ---------------------------------------------
    def subscribe(self, key, callback):
        """Register ``callback(key, value)`` for one ``key`` (or ``None`` = all).

        Callbacks run inline on the dispatch thread; an exception in one never
        propagates into the reader nor blocks other observers.
        """
        with self._lock:
            self._observers.setdefault(key, []).append(callback)

    def unsubscribe(self, key, callback):
        """Remove a previously-registered ``callback`` for ``key`` (idempotent).

        Used by the Phase 3 device bridge on ``deviceStopComm`` so a stopped
        device stops receiving state writes; a callback that was never
        registered is silently ignored.
        """
        with self._lock:
            callbacks = self._observers.get(key)
            if not callbacks:
                return
            try:
                callbacks.remove(callback)
            except ValueError:
                return
            if not callbacks:
                del self._observers[key]

    def _notify(self, key, value):
        with self._lock:
            callbacks = list(self._observers.get(key, ())) + list(self._observers.get(None, ()))
        for callback in callbacks:
            try:
                callback(key, value)
            except Exception:  # pylint: disable=broad-except
                self._logger.exception("Home Connect %s: observer for %s failed", self.name, key)

    # -- State merge ---------------------------------------------------------
    def merge_items(self, items):
        """Merge STATUS/NOTIFY/settings/status items into the cache and notify."""
        changed = []
        with self._lock:
            for item in items or ():
                key = item.get("key")
                if key is None:
                    continue
                value = item.get("value")
                self._state[key] = value
                changed.append((key, value))
        for key, value in changed:
            self._notify(key, value)

    def handle_event_items(self, items):
        """Merge discrete EVENT items, de-duplicating by ``(key, timestamp)``.

        Returns the list of ``(key, value)`` pairs that were fresh (not a repeat)
        so callers can decide whether a downstream trigger should fire.
        """
        fresh = []
        with self._lock:
            for item in items or ():
                key = item.get("key")
                if key is None:
                    continue
                dedupe_key = (key, item.get("timestamp"))
                if dedupe_key in self._seen_events:
                    continue
                self._seen_events.add(dedupe_key)
                self._seen_order.append(dedupe_key)
                while len(self._seen_order) > MAX_SEEN_EVENTS:
                    self._seen_events.discard(self._seen_order.popleft())
                value = item.get("value")
                self._state[key] = value
                fresh.append((key, value))
        for key, value in fresh:
            self._notify(key, value)
        return fresh

    # -- Connection lifecycle -----------------------------------------------
    def on_stream_start(self):
        """Synthetic START: cancel any pending disconnect and re-read state
        (the read is skipped when the last full pass is within the 5-min
        freshness window)."""
        with self._lock:
            self._disconnect_gen += 1         # supersede a scheduled disconnect
        self._schedule_read()

    def on_stream_stop(self, error=None):
        """Synthetic STOP: if the stream does not return in time, disconnect."""
        with self._lock:
            self._disconnect_gen += 1
            gen = self._disconnect_gen
        delay = 0.0 if error else DISCONNECT_DELAY
        self._schedule(lambda: self._maybe_disconnect(gen), delay)

    def _maybe_disconnect(self, gen):
        with self._lock:
            if gen != self._disconnect_gen:
                return                        # a START (or newer STOP) superseded us
        self._logger.debug("Home Connect %s: events missed; treating as disconnected", self.name)
        self._set_connected(False)

    def on_connected(self):
        """CONNECTED wire event: mark reachable and re-read state (the read is
        skipped when the last full pass is within the freshness window — a
        flapping appliance must not cost a 5-GET pass per flap)."""
        self._set_connected(True)

    def on_disconnected(self):
        """DISCONNECTED / DEPAIRED wire event: mark unreachable, abandon reads."""
        self._set_connected(False)

    def on_paired(self):
        """PAIRED (or first discovery): re-read state — may be a new appliance."""
        self._schedule_read(force=True)

    def _set_connected(self, value):
        changed = False
        with self._lock:
            previous = self._state.get(CONNECTED)
            self._state[CONNECTED] = value
            changed = previous != value
            if not value:
                self._read_actions = None     # abandon any pending reads
        if value:
            self._schedule_read()
        if changed:
            self._notify(CONNECTED, value)

    # -- Re-read queue (runs on the worker thread) --------------------------
    def _schedule_read(self, force=False):
        with self._lock:
            if not force and self._last_read_complete is not None:
                age = self._monotonic() - self._last_read_complete
                if age < READ_FRESH_WINDOW:
                    self._logger.debug("Home Connect %s: skipping re-read (last full read "
                                       "%.0fs ago)", self.name, age)
                    return
            if self._read_actions is not None:
                return                        # a read is already pending/running
            actions = list(_READ_BASE)
            if self._supports_programs:
                actions.extend(_READ_PROGRAMS)
            self._read_actions = actions
            if self._read_scheduled:
                return
            self._read_scheduled = True
        self._schedule(self._run_read, 0)

    def _run_read(self):
        while True:
            with self._lock:
                if not self._read_actions:    # finished ([]) or abandoned (None)
                    finished = self._read_actions is not None
                    if finished:
                        self._last_read_complete = self._monotonic()
                    self._read_actions = None
                    self._read_scheduled = False
                    connected = self._state.get(CONNECTED)
                    break
                action = self._read_actions[0]
            try:
                self._do_read_action(action)
            except HomeConnectError as exc:
                self._on_read_error(action, exc)
                return
            with self._lock:
                # The appliance may have disconnected mid-read (queue set to None).
                if self._read_actions:
                    self._read_actions.pop(0)
                self._read_delay = 0.0
        if finished and connected and self.get(CONNECTED) is not True:
            self._set_connected(True)

    def _on_read_error(self, action, exc):
        if getattr(exc, "aborted", False):
            # The client was aborted (shutdown / superseded on re-authorize):
            # nothing actually failed and no retry will run — a warning that
            # says "retrying in Ns" here would be false twice over.
            with self._lock:
                self._read_actions = None
                self._read_scheduled = False
            self._logger.debug("Home Connect %s: '%s' read abandoned (client shutting down)",
                               self.name, action)
            return
        with self._lock:
            if not self._state.get(CONNECTED) or self._read_actions is None:
                self._read_actions = None
                self._read_scheduled = False
                self._logger.debug("Home Connect %s: dropping '%s' read (disconnected)",
                                   self.name, action)
                return
            self._read_delay = min(self._read_delay * READ_FACTOR or READ_MIN_DELAY, READ_MAX_DELAY)
            delay = self._read_delay
        self._logger.warning("Home Connect %s: '%s' read failed (%s); retrying in %.0fs",
                             self.name, action, exc, delay)
        self._schedule(self._run_read, delay)

    def _do_read_action(self, action):
        if action == "appliance":
            info = self._reader.get_appliance(self.haid)
            if info:
                with self._lock:
                    self._info.update(info)
                if "connected" in info:
                    self._set_connected(bool(info["connected"]))
        elif action == "status":
            self.merge_items(self._reader.get_status(self.haid))
        elif action == "settings":
            self.merge_items(self._reader.get_settings(self.haid))
        elif action == "selected_program":
            self.merge_items(_program_items(SELECTED_PROGRAM, self._reader.get_selected_program(self.haid)))
        elif action == "active_program":
            self.merge_items(_program_items(ACTIVE_PROGRAM, self._reader.get_active_program(self.haid)))

    # -- Guard rails (Phase 4 extends these) --------------------------------
    def require_connected(self):
        if not self.connected:
            raise HomeConnectError(f"{self.name} is offline")

    def is_operation_state(self, *states):
        current = self.get(OPERATION_STATE)
        return current is not None and current in states

    def require_remote_control(self):
        self.require_connected()
        if self.get(LOCAL_CONTROL):
            raise HomeConnectError(f"{self.name} is being controlled locally")
        if self.get(REMOTE_CONTROL) is False:
            raise HomeConnectError(f"Remote control is not enabled on {self.name}")

    def require_remote_start(self):
        self.require_remote_control()
        if self.get(REMOTE_START) is False:
            raise HomeConnectError(f"Remote start is not enabled on {self.name}")


def _program_items(root_key, program):
    """Flatten a selected/active-program read into cache items (empty if absent)."""
    if not program:
        return []
    items = [{"key": root_key, "value": program.get("key")}]
    for option in program.get("options", ()) or ():
        if option.get("key") is not None:
            items.append(option)
    return items
