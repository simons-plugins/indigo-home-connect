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
  a 429 ``Retry-After`` through ``hc_api``'s shared request gate, with backoff
  escalating to 15 min after sustained failures (every open is a counted
  request);
* routes wire events on the single worker thread (the reader only parses and
  enqueues — routing ends in Indigo state writes, which must never stall the
  socket read): STATUS/EVENT/NOTIFY/CONNECTED/DISCONNECTED to the right
  :class:`~hc_appliance.HomeConnectAppliance`; PAIRED/DEPAIRED add/remove
  appliances from the registry, and a successful discovery pass prunes haIds
  that vanished from the account;
* renews a suspected zombie stream (keep-alives only for 30 min while a
  program is running — a BSH-confirmed backend failure mode);
* discovers appliances at most hourly and runs each appliance's sequential
  re-read queue on the same worker thread, deferring (never blocking) when the
  rate-limit gate is closed.

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
from hc_appliance import HomeConnectAppliance, OPERATION_STATE

EVENTS_PATH = "/api/homeappliances/events"
APPLIANCES_PATH = "/api/homeappliances"

DISCOVERY_INTERVAL = 60 * 60          # appliance-list poll at most hourly (PRD §3.5)

# One-shot warning threshold for the worker queue: routing shares the worker
# thread, so a queue this deep means events are piling up behind a stall.
QUEUE_DEPTH_WARN = 500

# Reconnect backoff for non-429 stream failures (429 is handled by the gate).
RECONNECT_MIN_DELAY = 1.0
RECONNECT_MAX_DELAY = 60.0

# Every stream open is a counted request, so a sustained outage at the 60 s cap
# would burn ~1440/day — more than the whole budget. After this many consecutive
# failed cycles the delay escalates to the extended value; a stream that then
# survives ``RECONNECT_STABLE_SECONDS`` resets the count. A connect that dies
# faster than that ALSO counts as a failure (connect-drop flapping burns opens
# just like refused opens do).
RECONNECT_ESCALATE_AFTER = 5
RECONNECT_EXTENDED_DELAY = 15 * 60.0
RECONNECT_STABLE_SECONDS = 120.0

# Zombie-stream detection (BSH-confirmed backend defect: KEEP-ALIVEs continue
# but state events silently stop — homebridge-homeconnect#74). The 120 s
# dead-man timer never fires because keep-alives keep arriving, so: when only
# keep-alives have arrived for this long AND the injected activity check says a
# program is running (a running appliance emits progress events every few
# minutes), the stream is presumed zombied and renewed (one counted request).
ZOMBIE_SUSPECT_SECONDS = 30 * 60.0

# Operation-state tails that count as "a program is running" for the zombie
# activity check (mirrors hc_control's stoppable set).
_ACTIVE_OP_TAILS = frozenset({"Run", "DelayedStart", "Pause", "ActionRequired"})

# How often the paused reader re-checks whether authorization has returned.
AUTH_POLL_INTERVAL = 30.0

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
# that is not blank, not a comment and not a bare known field name (see
# _BARE_FIELD_NAMES below) is structural garbage -> restart.
_FIELD_RE = re.compile(r"^(\w+):[ ]?(.*)$")

# The real SSE field names — the only bare (colon-less) tokens accepted as
# empty-value fields rather than treated as stream corruption.
_BARE_FIELD_NAMES = frozenset({"event", "data", "id", "retry"})

# Errors from a state read that mean "feature absent", not a failure (PRD §3.4).
# ``UnsupportedOperation`` is returned by settings-only appliances (fridge,
# freezer, wine cooler) for the program endpoints — observed live on the
# simulator's FridgeFreezer. The coordinator now skips program re-reads for those
# types entirely (``supports_programs_for``), so this is belt-and-braces for any
# appliance that still rejects a program endpoint.
_ABSENT_KEYS = frozenset({
    "SDK.Error.NoProgramSelected",
    "SDK.Error.NoProgramActive",
    "SDK.Error.WrongOperationState",
    "SDK.Error.UnsupportedSetting",
    "SDK.Error.UnsupportedOperation",
    "SDK.Simulator.InternalError",
    # Returned when the cloud has not finished (re)establishing its own link to
    # the appliance; treating it as a failure ground a listed-as-connected
    # appliance through 144 retry requests/day (#16).
    "SDK.Error.HomeAppliance.Connection.Initialization.Failed",
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
            if line in _BARE_FIELD_NAMES:
                # A bare KNOWN field name with no colon is valid SSE (empty
                # value) — it must not restart the stream. Any other bare token
                # stays garbage: this API's corruption habit (injected chunk
                # lengths, fragmented lines) makes an unknown bare token far
                # more likely a broken real line than a novel field, and
                # silently absorbing it would silently LOSE the event.
                fields.setdefault(line, "")
                continue
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
                 reconnect_min=RECONNECT_MIN_DELAY, reconnect_max=RECONNECT_MAX_DELAY,
                 reconnect_extended=RECONNECT_EXTENDED_DELAY,
                 escalate_after=RECONNECT_ESCALATE_AFTER,
                 stable_seconds=RECONNECT_STABLE_SECONDS,
                 auth_ok=None, auth_poll=AUTH_POLL_INTERVAL, monotonic=time.monotonic,
                 activity_check=None, zombie_after=ZOMBIE_SUSPECT_SECONDS):
        self._api = api
        self._dispatch = dispatch
        self._logger = logger or logging.getLogger("hc_events")
        self._path = path
        self._read_timeout = read_timeout
        self._sleep = sleep
        self._reconnect_min = reconnect_min
        self._reconnect_max = reconnect_max
        self._reconnect_extended = reconnect_extended
        self._escalate_after = escalate_after
        self._stable_seconds = stable_seconds
        self._monotonic = monotonic
        # auth_ok() -> True while authorized; False halts the reconnect loop
        # (e.g. refresh failed with invalid_grant -> STATE_AUTH_REQUIRED). The
        # loop idles and resumes only when authorization returns (PRD §6).
        self._auth_ok = auth_ok
        self._auth_poll = auth_poll
        # activity_check() -> True while any appliance is mid-program; gates
        # the zombie-stream renewal so an idle house never churns the stream.
        self._activity_check = activity_check
        self._zombie_after = zombie_after
        self._current = None
        self._current_lock = threading.Lock()

    def run(self, stop_event):
        backoff = 0.0
        failures = 0
        auth_paused_logged = False
        while not stop_event.is_set():
            if self._auth_ok is not None and not self._auth_ok():
                if not auth_paused_logged:
                    self._logger.error("Home Connect event stream paused: authorization required "
                                       "— re-authorize the plugin in its configuration.")
                    auth_paused_logged = True
                self._wait(stop_event, self._auth_poll)   # idle; re-check, never open_stream
                continue
            auth_paused_logged = False

            error = None
            opened_at = None
            try:
                stream = self._api.open_stream(self._path, read_timeout=self._read_timeout,
                                               abort_check=stop_event.is_set)
                with self._current_lock:
                    self._current = stream
                # A coordinator stop() racing the open-to-register gap would
                # have closed a _current that was still None; re-check so the
                # reader never enters lines() on a stream that shutdown can no
                # longer reach.
                if stop_event.is_set():
                    raise HomeConnectError("stream opened during shutdown", aborted=True)
                self._logger.info("Home Connect event stream connected")
                opened_at = self._monotonic()
                self._dispatch(SseEvent(START))
                backoff = 0.0
                for event in self._read_events(stream):
                    if stop_event.is_set():
                        break
                    self._dispatch(event)
            except HomeConnectError as exc:
                error = exc
                if getattr(exc, "zombie_renewal", False):
                    # Deliberate renewal (already logged at info): dispatch the
                    # STOP without an error so appliances take the grace-delay
                    # path — the ~1s reconnect supersedes the disconnect and no
                    # device flaps Off / fires triggers mid-program.
                    error = None
                    self._logger.debug("Home Connect event stream renewed (zombie suspected)")
                elif stop_event.is_set() or exc.aborted:
                    # Deliberate teardown: stop() closed the socket under the
                    # blocked read, or abort() fired just before the stop event
                    # was set — expected either way, not a warning.
                    self._logger.debug("Home Connect event stream closed for shutdown: %s", exc)
                else:
                    self._logger.warning("Home Connect event stream ended: %s — "
                                         "will reconnect automatically", exc)
            except Exception as exc:  # pylint: disable=broad-except
                error = exc
                self._logger.exception(exc)
            finally:
                self._close_current()
            self._dispatch(SseEvent(STOP, error=error))
            if stop_event.is_set():
                break
            # Every open is a counted request: a failed open, or a stream that
            # died before proving stable, counts toward escalation.
            lived = (self._monotonic() - opened_at) if opened_at is not None else 0.0
            if opened_at is not None and lived >= self._stable_seconds:
                failures = 0
            else:
                failures += 1
            backoff = min(backoff * 2 or self._reconnect_min, self._reconnect_max)
            if failures >= self._escalate_after:
                if failures == self._escalate_after:
                    self._logger.warning(
                        "Home Connect event stream failing repeatedly (%d attempts); "
                        "slowing reconnects to every %.0f minutes to protect the daily "
                        "request budget", failures, self._reconnect_extended / 60)
                backoff = self._reconnect_extended
            self._wait(stop_event, backoff)

    def _read_events(self, stream):
        last_real = self._monotonic()
        for fields in parse_sse_lines(stream.lines()):
            event = build_event(fields)
            if not event.event or event.event == KEEP_ALIVE:
                # Heartbeat: the read reset the socket timeout, but keep-alives
                # alone are also the zombie-stream signature (#21) — a running
                # appliance emits progress every few minutes, so a long
                # keep-alive-only stretch mid-program means the backend has
                # stopped generating events. Renew (one counted request).
                if (self._activity_check is not None and self._activity_check()
                        and (self._monotonic() - last_real) >= self._zombie_after):
                    self._logger.info(
                        "Home Connect event stream has sent only keep-alives for %.0f min "
                        "while a program is running — renewing the stream",
                        (self._monotonic() - last_real) / 60)
                    renewal = HomeConnectError("suspected zombie stream")
                    renewal.zombie_renewal = True
                    raise renewal
                continue
            last_real = self._monotonic()
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

    def gate_wait_remaining(self):
        """Seconds until the shared request gate opens (see #20 defer rule:
        readers on the worker thread must reschedule, never block the gate —
        the same thread routes every live SSE event)."""
        return self._api.gate_wait_remaining()

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
    backoff), the hourly discovery poll and — since the reader-thread split —
    routing every SSE event; same-deadline jobs run FIFO via the sequence
    tie-break, which is what preserves event ordering. ``run_pending`` lets
    tests drive it synchronously with a controllable clock.
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
            if len(self._heap) == QUEUE_DEPTH_WARN:
                # Routing shares this thread (#20): depth like this means the
                # worker is stalled (slow Indigo IPC?) while events pile up.
                self._logger.warning("Home Connect worker queue depth reached %d — "
                                     "event processing is falling behind", QUEUE_DEPTH_WARN)
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
                 on_appliance=None, on_event=None, on_discovery=None, supports_programs=True,
                 supports_programs_for=None, discovery_interval=DISCOVERY_INTERVAL,
                 stream=None, scheduler=None, auth_ok=None, on_removed=None):
        self._api = api
        self._logger = logger or logging.getLogger("hc_events")
        self._monotonic = monotonic
        self._on_appliance = on_appliance     # Phase 3 hook: called once per new appliance
        self._on_event = on_event             # tap for every routed event (tools/tests)
        # Called with a haId when its appliance leaves the account (DEPAIRED, or
        # vanished from a successful discovery pass — haId churn); the plugin
        # detaches the bridge and flags the device instead of leaving it
        # attached-but-dead forever (#15).
        self._on_removed = on_removed
        # Called after each completed discovery pass with the set of known haIds,
        # so the plugin can escalate a device whose configured haId never appears
        # (orphaned haId) without issuing any extra API requests.
        self._on_discovery = on_discovery
        # ``supports_programs_for(info) -> bool`` resolves the per-appliance flag
        # from its HC type (plugin wires hc_constants). Settings-only appliances
        # (fridge/freezer) then skip the selected/active-program re-reads that
        # would otherwise burn 2 requests/reconnect against the 1000/day budget
        # (PRD §3.2). Falls back to the coordinator-wide ``supports_programs``.
        self._supports_programs = supports_programs
        self._supports_programs_for = supports_programs_for
        self._discovery_interval = discovery_interval

        self._reader = ApiReader(api)
        self._scheduler = scheduler or Scheduler(logger=self._logger, monotonic=monotonic)
        self._stream = stream or EventStream(api, dispatch=self._dispatch,
                                             logger=self._logger, auth_ok=auth_ok,
                                             activity_check=self._any_program_active)

        self._lock = threading.RLock()
        self._appliances = {}
        self._stop = threading.Event()
        self._worker_thread = None
        self._stream_thread = None

    # -- Lifecycle -----------------------------------------------------------
    def start(self):
        """Start the reader + worker threads. Returns ``False`` (refusing) if a
        previous thread is still alive — a second live stream must be impossible."""
        for thread in (self._stream_thread, self._worker_thread):
            if thread is not None and thread.is_alive():
                self._logger.warning("Home Connect coordinator: refusing to start; %s still alive",
                                     thread.name)
                return False
        self._stop.clear()
        self._worker_thread = threading.Thread(
            target=self._scheduler.run, args=(self._stop,), name="hc-worker", daemon=True)
        self._worker_thread.start()
        self._scheduler.post(self._discover, 0)
        self._stream_thread = threading.Thread(
            target=self._stream.run, args=(self._stop,), name="hc-events", daemon=True)
        self._stream_thread.start()
        self._logger.debug("Home Connect coordinator started")
        return True

    def stop(self, timeout=5.0):
        """Signal shutdown and join both threads. Returns ``True`` only when both
        actually died; if a thread is still blocked after ``timeout`` it logs a
        warning, keeps the live ref (so :meth:`start` refuses a double-stream)
        and returns ``False`` — the caller must retry rather than assume stopped."""
        self._stop.set()
        self._stream.close()                  # shutdown()+close() to unblock read()
        self._scheduler.wake()                # unblock the worker's wait()
        alive = []
        for thread in (self._stream_thread, self._worker_thread):
            if thread is not None:
                thread.join(timeout)
                if thread.is_alive():
                    alive.append(thread)
        if alive:
            for thread in alive:
                self._logger.warning("Home Connect coordinator: %s did not stop within %.0fs; "
                                     "will not start a second stream", thread.name, timeout)
            # Keep only the still-alive refs so start() can see and refuse them.
            self._stream_thread = self._stream_thread if self._stream_thread in alive else None
            self._worker_thread = self._worker_thread if self._worker_thread in alive else None
            return False
        self._stream_thread = None
        self._worker_thread = None
        self._logger.debug("Home Connect coordinator stopped")
        return True

    def appliances(self):
        with self._lock:
            return list(self._appliances.values())

    def _any_program_active(self):
        """True while any known appliance is mid-program (zombie-check gate)."""
        for appliance in self.appliances():
            op = appliance.get(OPERATION_STATE)
            if isinstance(op, str) and op.rsplit(".", 1)[-1] in _ACTIVE_OP_TAILS:
                return True
        return False

    # -- Discovery -----------------------------------------------------------
    def _discover(self):
        # Never block the worker at the rate-limit gate: this thread routes
        # every live SSE event, so a gate-blocked HTTP call here would freeze
        # device updates for the whole Retry-After. Defer instead.
        wait = self._reader.gate_wait_remaining()
        if wait > 0 and not self._stop.is_set():
            self._scheduler.post(self._discover, min(wait + 1.0, self._discovery_interval))
            return
        try:
            found = self._reader.list_appliances()
        except HomeConnectError as exc:
            self._logger.warning("Home Connect appliance discovery failed: %s", exc)
            found = None
        if found is not None:
            for info in found:
                self._ensure_appliance(info)
            # An appliance no longer listed left the account (deleted, or
            # re-paired under a new haId). Prune it, or its bridge stays
            # attached to a dead object forever (#15). Only on a SUCCESSFUL
            # pass — a failed list must never depair the fleet.
            found_haids = {info.get("haId") for info in found}
            with self._lock:
                stale = [haid for haid in self._appliances if haid not in found_haids]
            for haid in stale:
                self._remove_appliance(haid, "absent from discovery")
            if self._on_discovery:
                try:
                    self._on_discovery({a.haid for a in self.appliances()})
                except Exception as exc:  # pylint: disable=broad-except
                    self._logger.exception(exc)
        if not self._stop.is_set():
            self._scheduler.post(self._discover, self._discovery_interval)

    def _remove_appliance(self, haid, reason):
        with self._lock:
            appliance = self._appliances.pop(haid, None)
        if appliance is None:
            return
        self._logger.warning("Home Connect appliance %s left the account (%s)",
                             appliance.name, reason)
        appliance.on_disconnected()           # abandon any queued reads
        if self._on_removed:
            try:
                self._on_removed(haid)
            except Exception as exc:  # pylint: disable=broad-except
                self._logger.exception(exc)

    def _ensure_appliance(self, info):
        haid = info.get("haId")
        if not haid:
            return None
        with self._lock:
            appliance = self._appliances.get(haid)
            is_new = appliance is None
            if is_new:
                supports = self._supports_programs
                if self._supports_programs_for is not None:
                    supports = self._supports_programs_for(info)
                appliance = HomeConnectAppliance(
                    haid, info, self._reader, self._scheduler.post, logger=self._logger,
                    monotonic=self._monotonic, supports_programs=supports)
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

    # -- Dispatch ------------------------------------------------------------
    def _dispatch(self, event):
        """Hand the event to the worker thread (reader thread must stay fast).

        Routing runs observer callbacks that end in Indigo state writes — an
        IPC round-trip to the Indigo server. Doing that on the reader thread
        let a slow Indigo server stall SSE reads past the 120 s dead-stream
        timeout (spurious reconnect + re-read burst, #20). The scheduler is a
        single thread, so event ordering is preserved."""
        self._scheduler.post(lambda: self._route(event), 0)

    # -- Routing (runs on the worker thread) ---------------------------------
    def _route(self, event):
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
        if etype == DEPAIRED:
            if event.haid:
                self._remove_appliance(event.haid, "DEPAIRED")
            else:
                self._logger.debug("Home Connect DEPAIRED without haId ignored")
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
        elif etype == DISCONNECTED:
            appliance.on_disconnected()
        else:
            self._logger.debug("Home Connect event %s ignored", etype)

    def _get(self, haid):
        with self._lock:
            return self._appliances.get(haid)
