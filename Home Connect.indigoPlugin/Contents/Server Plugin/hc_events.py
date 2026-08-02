"""Home Connect SSE event subsystem: stream reader, parser, reconnect loop and
the coordinator that owns the reader + worker threads (PRD §3.1, §3.2, §4).

There is ONE global event stream (`GET /api/homeappliances/events`) for the whole
account — never one per appliance (max 10 channels, and each open costs a
daily-budget request). This module:

* parses the SSE wire format tolerantly (comments, keep-alives, multi-line
  ``data``) and, per the API's historic habit of injecting hex chunk lengths /
  stray HTTP headers, restarts the stream on structurally unparseable input;
* runs an infinite reconnect loop with a 120 s read timeout (dead-stream
  detection) and synthetic START/STOP events to the appliance handlers, honoring
  a 429 ``Retry-After`` through ``hc_api``'s shared request gate;
* routes wire events (STATUS/EVENT/NOTIFY/CONNECTED/DISCONNECTED/PAIRED/DEPAIRED)
  to the right :class:`~hc_appliance.HomeConnectAppliance`;
* discovers appliances at most hourly and runs each appliance's sequential
  re-read queue on a single worker thread.

The coordinator is folded into this module (rather than a separate
``hc_coordinator.py``) because it owns the SSE reader and the worker that runs
the re-read queues those events schedule — keeping thread ownership in one place.
Never imports ``indigo``.
"""
import heapq
import json
import logging
import re
import threading
import time

from hc_api import HomeConnectError, STREAM_READ_TIMEOUT, redact
from hc_appliance import HomeConnectAppliance

EVENTS_PATH = "/api/homeappliances/events"
APPLIANCES_PATH = "/api/homeappliances"

DISCOVERY_INTERVAL = 60 * 60          # appliance-list poll at most hourly (PRD §3.5)

# Reconnect backoff for non-429 stream failures (429 is handled by the gate).
RECONNECT_MIN_DELAY = 1.0
RECONNECT_MAX_DELAY = 60.0

# Synthetic (internal) event types plus the wire types we route.
START = "START"
STOP = "STOP"
KEEP_ALIVE = "KEEP-ALIVE"
STATUS = "STATUS"
EVENT = "EVENT"
NOTIFY = "NOTIFY"
CONNECTED = "CONNECTED"
DISCONNECTED = "DISCONNECTED"
PAIRED = "PAIRED"
DEPAIRED = "DEPAIRED"

_ITEM_EVENTS = frozenset({STATUS, EVENT, NOTIFY})

# A valid SSE field line: "name: value" (single optional space). Anything else
# that is not blank and not a comment is structural garbage -> restart.
_FIELD_RE = re.compile(r"^(\w+):[ ]?(.*)$")

# Errors from a state read that mean "feature absent", not a failure (PRD §3.4).
# ``UnsupportedOperation`` is returned by settings-only appliances (fridge,
# freezer, wine cooler) for the program endpoints — observed live on the
# simulator's FridgeFreezer. Swallowing it stops a retry storm on those types
# until Phase 3 sets ``supports_programs=False`` per appliance type.
_ABSENT_KEYS = frozenset({
    "SDK.Error.NoProgramSelected",
    "SDK.Error.NoProgramActive",
    "SDK.Error.WrongOperationState",
    "SDK.Error.UnsupportedSetting",
    "SDK.Error.UnsupportedOperation",
    "SDK.Simulator.InternalError",
})


class SseParseError(Exception):
    """Structurally corrupt SSE input — the reader kills and restarts the stream."""


class SseEvent:
    """A parsed (or synthetic) event: ``event`` type, optional ``haid``/``data``.

    ``error`` is set only on a synthetic STOP to carry the termination cause.
    """

    __slots__ = ("event", "haid", "data", "error")

    def __init__(self, event, haid=None, data=None, error=None):
        self.event = event
        self.haid = haid
        self.data = data
        self.error = error

    def items(self):
        if isinstance(self.data, dict):
            got = self.data.get("items")
            if isinstance(got, list):
                return got
        return []

    def __repr__(self):
        return f"SseEvent({self.event!r}, haid={redact(self.haid)!r})"


# ---------------------------------------------------------------------------
# SSE parsing
# ---------------------------------------------------------------------------
def parse_sse_lines(lines):
    """Yield one raw field ``dict`` per event from an iterable of text lines.

    A blank line ends an event; ``:`` comment lines are ignored; repeated field
    names concatenate with newlines (multi-line ``data``). A non-blank,
    non-comment line that is not ``name: value`` raises :class:`SseParseError`.
    """
    fields = {}
    for line in lines:
        if line == "":
            if fields:
                yield fields
                fields = {}
            continue
        if line.startswith(":"):
            continue
        match = _FIELD_RE.match(line)
        if not match:
            raise SseParseError(f"unparseable SSE line: {line[:80]!r}")
        name, value = match.group(1), match.group(2)
        fields[name] = f"{fields[name]}\n{value}" if name in fields else value
    if fields:
        yield fields


def build_event(fields):
    """Turn a raw field dict into an :class:`SseEvent` (parses JSON ``data``).

    Applies the known API bug workaround where CONNECTED/DISCONNECTED events
    carry ``haId`` only inside the JSON body, not the ``id:`` line.
    """
    event_type = fields.get("event")
    haid = fields.get("id") or None
    data = None
    raw = fields.get("data")
    if raw:
        try:
            data = json.loads(raw)
        except ValueError as exc:
            raise SseParseError(f"invalid JSON in SSE data: {exc}") from exc
        if not haid and isinstance(data, dict) and data.get("haId"):
            haid = data["haId"]
    return SseEvent(event_type, haid=haid, data=data)


# ---------------------------------------------------------------------------
# SSE reader with infinite reconnect
# ---------------------------------------------------------------------------
class EventStream:
    """Reads the single global SSE stream, reconnecting forever until stopped.

    Dispatches a synthetic START on each successful (re)connect and a STOP on
    every termination (with the error, if any). ``dispatch`` runs on this reader
    thread and must be quick — it never performs network I/O.
    """

    def __init__(self, api, dispatch, logger=None, path=EVENTS_PATH,
                 read_timeout=STREAM_READ_TIMEOUT, sleep=None,
                 reconnect_min=RECONNECT_MIN_DELAY, reconnect_max=RECONNECT_MAX_DELAY):
        self._api = api
        self._dispatch = dispatch
        self._logger = logger or logging.getLogger("hc_events")
        self._path = path
        self._read_timeout = read_timeout
        self._sleep = sleep
        self._reconnect_min = reconnect_min
        self._reconnect_max = reconnect_max
        self._current = None
        self._current_lock = threading.Lock()

    def run(self, stop_event):
        backoff = 0.0
        while not stop_event.is_set():
            error = None
            try:
                stream = self._api.open_stream(self._path, read_timeout=self._read_timeout)
                with self._current_lock:
                    self._current = stream
                self._logger.info("Home Connect event stream connected")
                self._dispatch(SseEvent(START))
                backoff = 0.0
                for event in self._read_events(stream):
                    if stop_event.is_set():
                        break
                    self._dispatch(event)
            except HomeConnectError as exc:
                error = exc
                self._logger.warning("Home Connect event stream ended: %s", exc)
            except Exception as exc:  # pylint: disable=broad-except
                error = exc
                self._logger.exception(exc)
            finally:
                self._close_current()
            self._dispatch(SseEvent(STOP, error=error))
            if stop_event.is_set():
                break
            backoff = min(backoff * 2 or self._reconnect_min, self._reconnect_max)
            self._wait(stop_event, backoff)

    def _read_events(self, stream):
        for fields in parse_sse_lines(stream.lines()):
            event = build_event(fields)
            if not event.event or event.event == KEEP_ALIVE:
                continue                      # heartbeat: the read already reset the timeout
            yield event

    def close(self):
        """Unblock a stuck reader by closing the current stream (thread-safe)."""
        self._close_current()

    def _close_current(self):
        with self._current_lock:
            stream = self._current
            self._current = None
        if stream is not None:
            stream.close()

    def _wait(self, stop_event, seconds):
        if self._sleep is not None:
            self._sleep(seconds)
        else:
            stop_event.wait(seconds)


# ---------------------------------------------------------------------------
# API reader: state reads with "swallow expected errors as absent" (PRD §3.4)
# ---------------------------------------------------------------------------
class ApiReader:
    """Thin wrapper over :class:`~hc_api.HomeConnectAPI` for appliance reads.

    Normalises the JSON envelopes into item lists and swallows the routine
    "feature absent" errors so the re-read queue treats them as success.
    """

    def __init__(self, api):
        self._api = api

    def list_appliances(self):
        data = self._api.get_json(APPLIANCES_PATH)
        return (data or {}).get("data", {}).get("homeappliances", [])

    def get_appliance(self, haid):
        data = self._api.get_json(f"{APPLIANCES_PATH}/{haid}")
        return (data or {}).get("data", {})

    def get_status(self, haid):
        data = self._api.get_json(f"{APPLIANCES_PATH}/{haid}/status")
        return (data or {}).get("data", {}).get("status", [])

    def get_settings(self, haid):
        data = self._api.get_json(f"{APPLIANCES_PATH}/{haid}/settings")
        return (data or {}).get("data", {}).get("settings", [])

    def get_selected_program(self, haid):
        return self._get_program(f"{APPLIANCES_PATH}/{haid}/programs/selected")

    def get_active_program(self, haid):
        return self._get_program(f"{APPLIANCES_PATH}/{haid}/programs/active")

    def get_commands(self, haid):
        try:
            data = self._api.get_json(f"{APPLIANCES_PATH}/{haid}/commands")
        except HomeConnectError as exc:
            if exc.status == 404 or exc.key == "404":
                return []                     # /commands unsupported -> feature absent
            raise
        return (data or {}).get("data", {}).get("commands", [])

    def _get_program(self, path):
        try:
            data = self._api.get_json(path)
        except HomeConnectError as exc:
            if _is_absent(exc):
                return None
            raise
        return (data or {}).get("data", {})


def _is_absent(exc):
    return exc.key in _ABSENT_KEYS


# ---------------------------------------------------------------------------
# Worker scheduler: time-ordered jobs on one thread
# ---------------------------------------------------------------------------
class Scheduler:
    """A single-thread job scheduler: ``post(fn, delay)`` runs ``fn`` later.

    Used for the per-appliance re-read queues (which reschedule themselves with
    backoff) and the hourly discovery poll. ``run_pending`` lets tests drive it
    synchronously with a controllable clock.
    """

    def __init__(self, logger=None, monotonic=time.monotonic):
        self._logger = logger or logging.getLogger("hc_events")
        self._monotonic = monotonic
        self._cond = threading.Condition()
        self._heap = []
        self._seq = 0

    def post(self, fn, delay=0):
        with self._cond:
            self._seq += 1
            heapq.heappush(self._heap, (self._monotonic() + delay, self._seq, fn))
            self._cond.notify()

    def wake(self):
        with self._cond:
            self._cond.notify_all()

    def run_pending(self, now=None):
        """Run every job whose deadline has passed. Returns how many ran."""
        now = self._monotonic() if now is None else now
        ran = 0
        while True:
            with self._cond:
                if not self._heap or self._heap[0][0] > now:
                    break
                _, _, fn = heapq.heappop(self._heap)
            self._safe_run(fn)
            ran += 1
        return ran

    def run(self, stop_event):
        while not stop_event.is_set():
            with self._cond:
                if not self._heap:
                    self._cond.wait(timeout=1.0)
                    continue
                deadline = self._heap[0][0]
                wait = deadline - self._monotonic()
                if wait > 0:
                    self._cond.wait(timeout=min(wait, 1.0))
                    continue
                _, _, fn = heapq.heappop(self._heap)
            self._safe_run(fn)

    def _safe_run(self, fn):
        try:
            fn()
        except Exception as exc:  # pylint: disable=broad-except
            self._logger.exception(exc)


# ---------------------------------------------------------------------------
# Coordinator: owns the SSE reader thread + the worker thread
# ---------------------------------------------------------------------------
class HomeConnectCoordinator:
    """Owns the event stream, the appliance registry and the worker thread."""

    def __init__(self, api, logger=None, monotonic=time.monotonic,
                 on_appliance=None, on_event=None, supports_programs=True,
                 discovery_interval=DISCOVERY_INTERVAL, stream=None, scheduler=None):
        self._api = api
        self._logger = logger or logging.getLogger("hc_events")
        self._monotonic = monotonic
        self._on_appliance = on_appliance     # Phase 3 hook: called once per new appliance
        self._on_event = on_event             # tap for every routed event (tools/tests)
        self._supports_programs = supports_programs
        self._discovery_interval = discovery_interval

        self._reader = ApiReader(api)
        self._scheduler = scheduler or Scheduler(logger=self._logger, monotonic=monotonic)
        self._stream = stream or EventStream(api, dispatch=self._dispatch, logger=self._logger)

        self._lock = threading.RLock()
        self._appliances = {}
        self._stop = threading.Event()
        self._worker_thread = None
        self._stream_thread = None

    # -- Lifecycle -----------------------------------------------------------
    def start(self):
        self._stop.clear()
        self._worker_thread = threading.Thread(
            target=self._scheduler.run, args=(self._stop,), name="hc-worker", daemon=True)
        self._worker_thread.start()
        self._scheduler.post(self._discover, 0)
        self._stream_thread = threading.Thread(
            target=self._stream.run, args=(self._stop,), name="hc-events", daemon=True)
        self._stream_thread.start()
        self._logger.debug("Home Connect coordinator started")

    def stop(self, timeout=5.0):
        self._stop.set()
        self._stream.close()                  # unblock a reader stuck in read()
        self._scheduler.wake()                # unblock the worker's wait()
        for thread in (self._stream_thread, self._worker_thread):
            if thread is not None:
                thread.join(timeout)
        self._stream_thread = None
        self._worker_thread = None
        self._logger.debug("Home Connect coordinator stopped")

    def appliances(self):
        with self._lock:
            return list(self._appliances.values())

    # -- Discovery -----------------------------------------------------------
    def _discover(self):
        try:
            found = self._reader.list_appliances()
        except HomeConnectError as exc:
            self._logger.warning("Home Connect appliance discovery failed: %s", exc)
            found = None
        if found is not None:
            for info in found:
                self._ensure_appliance(info)
        if not self._stop.is_set():
            self._scheduler.post(self._discover, self._discovery_interval)

    def _ensure_appliance(self, info):
        haid = info.get("haId")
        if not haid:
            return None
        with self._lock:
            appliance = self._appliances.get(haid)
            is_new = appliance is None
            if is_new:
                appliance = HomeConnectAppliance(
                    haid, info, self._reader, self._scheduler.post, logger=self._logger,
                    monotonic=self._monotonic, supports_programs=self._supports_programs)
                self._appliances[haid] = appliance
        if is_new:
            self._logger.info("Discovered Home Connect appliance %s [%s] haId=%s",
                              appliance.name, info.get("type") or "?", redact(haid))
            if self._on_appliance:
                try:
                    self._on_appliance(appliance)
                except Exception as exc:  # pylint: disable=broad-except
                    self._logger.exception(exc)
            appliance.on_paired()             # initial state re-read
        return appliance

    # -- Dispatch (runs on the reader thread) --------------------------------
    def _dispatch(self, event):
        if self._on_event:
            try:
                self._on_event(event)
            except Exception as exc:  # pylint: disable=broad-except
                self._logger.exception(exc)

        etype = event.event
        if etype == START:
            for appliance in self.appliances():
                appliance.on_stream_start()
            return
        if etype == STOP:
            for appliance in self.appliances():
                appliance.on_stream_stop(event.error)
            return
        if etype == PAIRED:
            self._scheduler.post(self._discover, 0)
            appliance = self._get(event.haid)
            if appliance is not None:
                appliance.on_paired()
            return

        haid = event.haid
        if not haid:
            self._logger.debug("Home Connect event %s without haId ignored", etype)
            return
        appliance = self._get(haid)
        if appliance is None:
            self._logger.debug("Home Connect event for unknown appliance; scheduling discovery")
            self._scheduler.post(self._discover, 0)
            return

        if etype in (STATUS, NOTIFY):
            appliance.merge_items(event.items())
        elif etype == EVENT:
            appliance.handle_event_items(event.items())
        elif etype == CONNECTED:
            appliance.on_connected()
        elif etype in (DISCONNECTED, DEPAIRED):
            appliance.on_disconnected()
        else:
            self._logger.debug("Home Connect event %s ignored", etype)

    def _get(self, haid):
        with self._lock:
            return self._appliances.get(haid)
