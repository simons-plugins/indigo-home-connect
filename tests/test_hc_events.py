"""Unit tests for hc_events.py — SSE parser, reader/reconnect, ApiReader
swallowing, scheduler, and the coordinator."""
import json
import socket
import threading
import time
from collections import deque
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

import hc_events
from hc_api import HomeConnectAPI, HomeConnectError
from hc_events import (ApiReader, EventStream, HomeConnectCoordinator, Scheduler,
                       SseEvent, format_stream_status, SseParseError, build_event, parse_sse_lines,
                       START, STOP, STATUS, DEPAIRED)
from support import Clock, FakeAPI, ScriptedTransport

HC_JSON = {"content-type": "application/vnd.bsh.sdk.v1+json"}


def lines(text):
    return text.split("\n")


# -- SSE parser ---------------------------------------------------------------

def test_parse_single_event():
    events = list(parse_sse_lines(lines("event:STATUS\nid:HAID\ndata:{}\n\n")))
    assert events == [{"event": "STATUS", "id": "HAID", "data": "{}"}]


def test_parse_multiline_data_concatenates():
    events = list(parse_sse_lines(lines("event:NOTIFY\ndata:line1\ndata:line2\n\n")))
    assert events[0]["data"] == "line1\nline2"


def test_parse_ignores_comment_lines():
    events = list(parse_sse_lines(lines(":keep-alive comment\nevent:STATUS\ndata:{}\n\n")))
    assert events == [{"event": "STATUS", "data": "{}"}]


def test_parse_tolerates_optional_space_after_colon():
    events = list(parse_sse_lines(lines("event: STATUS\ndata: {}\n\n")))
    assert events == [{"event": "STATUS", "data": "{}"}]


def test_parse_garbage_line_raises_to_restart():
    # A stray hex chunk length / HTTP header the API historically injected.
    with pytest.raises(SseParseError):
        list(parse_sse_lines(lines("event:STATUS\n1a2f\n\n")))


def test_parse_two_events_separated_by_blank_line():
    text = "event:STATUS\ndata:{}\n\nevent:NOTIFY\ndata:{}\n\n"
    events = list(parse_sse_lines(lines(text)))
    assert [e["event"] for e in events] == ["STATUS", "NOTIFY"]


# -- build_event --------------------------------------------------------------

def test_build_event_parses_json_data():
    event = build_event({"event": "STATUS", "id": "HAID", "data": json.dumps({"items": [1]})})
    assert event.event == "STATUS"
    assert event.haid == "HAID"
    assert event.data == {"items": [1]}


def test_build_event_id_in_data_workaround():
    # CONNECTED with haId only inside the JSON body, no id: line (API bug).
    event = build_event({"event": "CONNECTED", "data": json.dumps({"haId": "INNER"})})
    assert event.haid == "INNER"


def test_build_event_invalid_json_raises_restart():
    with pytest.raises(SseParseError):
        build_event({"event": "NOTIFY", "data": "{not json"})


def test_keep_alive_has_no_data():
    event = build_event({"event": "KEEP-ALIVE"})
    assert event.event == "KEEP-ALIVE"
    assert event.data is None


def test_sse_event_items_helper():
    event = build_event({"event": "STATUS", "id": "H", "data": json.dumps({"items": [{"key": "A"}]})})
    assert event.items() == [{"key": "A"}]
    assert SseEvent("START").items() == []


# -- ApiReader (swallow expected errors as absent) ----------------------------

def make_reader():
    api = FakeAPI()
    return ApiReader(api), api


def test_reader_list_appliances_unwraps_envelope():
    reader, api = make_reader()
    api.get_json = lambda path: {"data": {"homeappliances": [{"haId": "X"}]}}
    assert reader.list_appliances() == [{"haId": "X"}]


def test_reader_selected_program_swallows_no_program_selected():
    reader, api = make_reader()
    api.get_json = Mock(side_effect=HomeConnectError("x", status=409,
                                                     key="SDK.Error.NoProgramSelected"))
    assert reader.get_selected_program("H") is None


def test_reader_active_program_swallows_wrong_operation_state():
    reader, api = make_reader()
    api.get_json = Mock(side_effect=HomeConnectError("x", status=409,
                                                     key="SDK.Error.WrongOperationState"))
    assert reader.get_active_program("H") is None


def test_reader_program_swallows_unsupported_operation():
    # Settings-only appliances (fridge/freezer) return this on program endpoints.
    reader, api = make_reader()
    api.get_json = Mock(side_effect=HomeConnectError("x", status=409,
                                                     key="SDK.Error.UnsupportedOperation"))
    assert reader.get_selected_program("H") is None
    assert reader.get_active_program("H") is None


def test_reader_reraises_unexpected_error():
    reader, api = make_reader()
    api.get_json = Mock(side_effect=HomeConnectError("boom", status=500))
    with pytest.raises(HomeConnectError):
        reader.get_selected_program("H")


def test_reader_commands_swallows_404():
    reader, api = make_reader()
    api.get_json = Mock(side_effect=HomeConnectError("x", status=404))
    assert reader.get_commands("H") == []


# -- Scheduler ----------------------------------------------------------------

def test_scheduler_runs_due_jobs_in_order():
    clock = Clock()
    scheduler = Scheduler(logger=Mock(), monotonic=clock.monotonic)
    order = []
    scheduler.post(lambda: order.append("a"), 0)
    scheduler.post(lambda: order.append("b"), 5)
    scheduler.post(lambda: order.append("c"), 0)
    scheduler.run_pending(now=0)
    assert order == ["a", "c"]          # "b" not due yet
    scheduler.run_pending(now=5)
    assert order == ["a", "c", "b"]


def test_scheduler_job_exception_does_not_stop_others():
    scheduler = Scheduler(logger=Mock())
    ran = []
    scheduler.post(lambda: (_ for _ in ()).throw(RuntimeError("boom")), 0)
    scheduler.post(lambda: ran.append(1), 0)
    scheduler.run_pending(now=time.monotonic() + 1)
    assert ran == [1]


# -- EventStream reconnect over the real gate ---------------------------------

def make_api(transport, clock):
    return HomeConnectAPI(
        host="api.home-connect.com", logger=Mock(),
        connection_factory=transport.factory,
        monotonic=clock.monotonic, sleep=clock.sleep,
        wall_now=lambda: datetime(2026, 8, 2, tzinfo=timezone.utc))


def test_event_stream_start_events_stop_on_close():
    transport = ScriptedTransport()
    transport.queue_stream(200, HC_JSON, [b"event:STATUS\nid:H\ndata:{}\n\n", b""])
    clock = Clock()
    api = make_api(transport, clock)
    dispatched = []
    stop = threading.Event()

    def dispatch(event):
        dispatched.append(event.event)
        if event.event == STOP:
            stop.set()

    stream = EventStream(api, dispatch=dispatch, logger=Mock(), sleep=clock.sleep)
    stream.run(stop)
    assert dispatched == [START, STATUS, STOP]


def test_event_stream_shutdown_error_logs_debug_not_warning():
    # A read failure while stop_event is set is a deliberate shutdown (stop()
    # closed the socket under the blocked read): it must log at debug, never warn.
    transport = ScriptedTransport()
    transport.queue_stream(200, HC_JSON, [b"event:STATUS\n", socket.timeout("closed")])
    clock = Clock()
    api = make_api(transport, clock)
    logger = Mock()
    stop = threading.Event()

    def dispatch(event):
        if event.event == START:
            stop.set()          # simulate stop() arriving mid-stream

    EventStream(api, dispatch=dispatch, logger=logger, sleep=clock.sleep).run(stop)
    warnings = " ".join(str(c) for c in logger.warning.call_args_list)
    debugs = " ".join(str(c) for c in logger.debug.call_args_list)
    assert "event stream ended" not in warnings          # not warned during shutdown
    assert "closed for shutdown" in debugs


def test_event_stream_error_warns_when_not_stopping():
    # The same read failure with stop_event NOT set is a real drop -> warn.
    transport = ScriptedTransport()
    transport.queue_stream(200, HC_JSON, [b"event:STATUS\n", socket.timeout("dropped")])
    clock = Clock()
    api = make_api(transport, clock)
    logger = Mock()
    stop = threading.Event()

    def dispatch(event):
        if event.event == STOP:
            stop.set()          # let the loop exit after the first failed run
        return

    EventStream(api, dispatch=dispatch, logger=logger, sleep=clock.sleep).run(stop)
    warnings = " ".join(str(c) for c in logger.warning.call_args_list)
    assert "event stream ended" in warnings


def test_event_stream_filters_keep_alive():
    transport = ScriptedTransport()
    transport.queue_stream(200, HC_JSON,
                           [b"event:KEEP-ALIVE\n\n", b"event:STATUS\nid:H\ndata:{}\n\n", b""])
    clock = Clock()
    api = make_api(transport, clock)
    dispatched = []
    stop = threading.Event()

    def dispatch(event):
        dispatched.append(event.event)
        if event.event == STOP:
            stop.set()

    EventStream(api, dispatch=dispatch, logger=Mock(), sleep=clock.sleep).run(stop)
    assert dispatched == [START, STATUS, STOP]   # KEEP-ALIVE filtered out


def test_event_stream_reconnects_honoring_retry_after_gate():
    transport = ScriptedTransport()
    # First open is 429 with Retry-After; then a good stream.
    transport.queue_stream(429, {"content-type": "application/json", "retry-after": "30"}, [b"{}"])
    transport.queue_stream(200, HC_JSON, [b"event:STATUS\nid:H\ndata:{}\n\n", b""])
    clock = Clock()
    api = make_api(transport, clock)
    dispatched = []
    stop = threading.Event()

    def dispatch(event):
        dispatched.append(event.event)
        # Stop after the SECOND STOP (429 STOP, then the good run's STOP).
        if dispatched.count(STOP) >= 2:
            stop.set()

    EventStream(api, dispatch=dispatch, logger=Mock(), sleep=clock.sleep).run(stop)
    assert dispatched == [STOP, START, STATUS, STOP]     # no START on the failed open
    # The 429 Retry-After (30s) pushed the shared gate; the reconnect waited at
    # least that long in total (reconnect backoff + gate countdown).
    assert sum(clock.slept) >= 30


class _StubStreamResp:
    def __init__(self, text_lines):
        self._lines = list(text_lines)

    def lines(self):
        return iter(self._lines)

    def close(self):
        pass


class _StubApi:
    """Records open_stream calls; serves queued streams or raises queued errors."""

    def __init__(self, items):
        self._items = deque(items)
        self.open_calls = 0

    def open_stream(self, path, read_timeout=None, abort_check=None):  # noqa: ARG002
        self.open_calls += 1
        item = self._items.popleft()
        if isinstance(item, Exception):
            raise item
        return item


def test_event_stream_pauses_on_auth_required_and_resumes():
    # While auth_ok() is False the loop must NOT open the stream (no reconnect
    # storm with a dead token); it resumes once authorization returns.
    api = _StubApi([_StubStreamResp(["event:STATUS", "id:H", "data:{}", ""])])
    state = {"ok": False}
    dispatched = []
    stop = threading.Event()

    def dispatch(event):
        dispatched.append(event.event)
        if event.event == STATUS:
            stop.set()

    sleeps = []

    def fake_sleep(seconds):
        sleeps.append(seconds)
        state["ok"] = True             # authorization recovers after one idle poll

    EventStream(api, dispatch=dispatch, logger=Mock(), sleep=fake_sleep,
                auth_ok=lambda: state["ok"], auth_poll=30.0).run(stop)

    assert api.open_calls == 1          # never opened while paused; opened once on resume
    assert sleeps == [30.0]             # exactly one idle poll before recovery
    assert dispatched == [START, STATUS, STOP]


# -- Coordinator --------------------------------------------------------------

class StubStream:
    """A stream that blocks until stopped, so start/stop thread hygiene is testable."""

    def __init__(self):
        self.started = threading.Event()
        self._release = threading.Event()

    def run(self, stop_event):
        self.started.set()
        # Block like a live reader until close()/stop is signalled.
        while not stop_event.is_set():
            self._release.wait(0.05)

    def close(self):
        self._release.set()


def make_coordinator(appliances, on_event=None, on_appliance=None):
    api = FakeAPI()
    api.get_json = lambda path: {"data": {"homeappliances": appliances}}
    stub = StubStream()
    coord = HomeConnectCoordinator(api, logger=Mock(), on_event=on_event,
                                   on_appliance=on_appliance, stream=stub)
    return coord, stub


def test_coordinator_start_stop_thread_hygiene():
    coord, stub = make_coordinator([])
    coord.start()
    assert stub.started.wait(2.0)
    assert coord.stop(timeout=2.0) is True
    assert coord._stream_thread is None      # pylint: disable=protected-access
    assert coord._worker_thread is None      # pylint: disable=protected-access


class StuckStream:
    """A reader that close() cannot unblock — models a recv() that outlives the
    join timeout, so stop()'s thread-death verification is actually exercised."""

    def __init__(self):
        self.started = threading.Event()
        self._release = threading.Event()
        self.close_calls = 0

    def run(self, stop_event):  # noqa: ARG002 - deliberately ignores stop_event
        self.started.set()
        self._release.wait()

    def close(self):
        self.close_calls += 1    # does NOT release the reader

    def release(self):
        self._release.set()


def test_coordinator_stop_warns_and_refuses_double_start_when_reader_stuck():
    api = FakeAPI()
    api.get_json = lambda path: {"data": {"homeappliances": []}}
    stuck = StuckStream()
    logger = Mock()
    coord = HomeConnectCoordinator(api, logger=logger, stream=stuck)

    assert coord.start() is True
    assert stuck.started.wait(2.0)

    # close() cannot interrupt the reader before the join timeout: stop() must
    # report failure (not silently claim stopped) and keep the live ref.
    assert coord.stop(timeout=0.2) is False
    assert stuck.close_calls >= 1
    assert any("did not stop" in str(c) for c in logger.warning.call_args_list)

    # A second live stream must be impossible while the reader is still alive.
    assert coord.start() is False

    # Clean up: let the reader exit, then a real stop succeeds.
    stuck.release()
    assert coord.stop(timeout=2.0) is True


def test_coordinator_discovers_appliances_once():
    seen = []
    coord, _ = make_coordinator(
        [{"haId": "HAID-1", "name": "Dishwasher", "type": "Dishwasher", "connected": True}],
        on_appliance=lambda a: seen.append(a.haid))
    coord._discover()                        # pylint: disable=protected-access
    coord._discover()                        # second poll must not re-add
    assert seen == ["HAID-1"]
    assert len(coord.appliances()) == 1


def test_coordinator_routes_status_to_appliance():
    # _dispatch hands off to the worker scheduler (#20 — the reader thread must
    # never do Indigo IPC); run_pending drives the worker synchronously.
    coord, _ = make_coordinator(
        [{"haId": "HAID-1", "name": "D", "type": "Dishwasher", "connected": True}])
    coord._discover()                        # pylint: disable=protected-access
    coord._dispatch(SseEvent(STATUS, haid="HAID-1",                # pylint: disable=protected-access
                             data={"items": [{"key": "BSH.Common.Status.DoorState",
                                              "value": "Open"}]}))
    coord._scheduler.run_pending()           # pylint: disable=protected-access
    appliance = coord.appliances()[0]
    assert appliance.get("BSH.Common.Status.DoorState") == "Open"


def test_coordinator_start_stop_broadcast_to_appliances():
    coord, _ = make_coordinator(
        [{"haId": "HAID-1", "name": "D", "type": "Dishwasher", "connected": True}])
    coord._discover()                        # pylint: disable=protected-access
    tap = []
    coord._on_event = lambda e: tap.append(e.event)   # pylint: disable=protected-access
    coord._dispatch(SseEvent(START))         # pylint: disable=protected-access
    coord._dispatch(SseEvent(STOP, error=None))       # pylint: disable=protected-access
    coord._scheduler.run_pending()           # pylint: disable=protected-access
    assert tap == [START, STOP]              # single worker preserves ordering


def test_coordinator_dispatch_is_nonblocking_for_reader():
    # The reader-thread half of #20: _dispatch must only post, never route.
    coord, _ = make_coordinator([])
    posted = []
    coord._scheduler.post = lambda fn, delay=0: posted.append(delay)  # pylint: disable=protected-access
    coord._dispatch(SseEvent(START))         # pylint: disable=protected-access
    assert posted == [0]                     # queued for the worker, nothing ran inline


def test_coordinator_unknown_appliance_triggers_discovery():
    coord, _ = make_coordinator([])
    posted = []
    coord._scheduler.post = lambda fn, delay=0: posted.append(delay)  # pylint: disable=protected-access
    coord._route(SseEvent(STATUS, haid="UNKNOWN",  # pylint: disable=protected-access
                          data={"items": []}))
    assert posted == [0]                     # discovery scheduled immediately


# -- supports_programs_for wired end-to-end (per-type budget guard) ------------

def _reread_paths_for(appliance_info):
    """Discover one appliance, drain its re-read queue, return the GET paths.

    Uses the production resolver (plugin._supports_programs_for -> hc_constants)
    so the per-type program-read decision is exercised end-to-end, not in
    isolation."""
    import plugin                                                   # noqa: PLC0415

    paths = []

    def get_json(path):
        paths.append(path)
        if path == "/api/homeappliances":
            return {"data": {"homeappliances": [appliance_info]}}
        if path.endswith("/status"):
            return {"data": {"status": []}}
        if path.endswith("/settings"):
            return {"data": {"settings": []}}
        return {"data": {}}

    api = FakeAPI()
    api.get_json = get_json
    coord = HomeConnectCoordinator(
        api, logger=Mock(), stream=StubStream(),
        supports_programs_for=plugin._supports_programs_for)  # pylint: disable=protected-access
    coord._discover()                                         # pylint: disable=protected-access
    coord._scheduler.run_pending()                           # pylint: disable=protected-access
    return paths


def test_fridgefreezer_skips_program_reads_end_to_end():
    paths = _reread_paths_for(
        {"haId": "HA-FF", "name": "Fridge", "type": "FridgeFreezer", "connected": True})
    assert not any(p.endswith(("/programs/selected", "/programs/active")) for p in paths), paths


def test_dishwasher_reads_programs_end_to_end():
    paths = _reread_paths_for(
        {"haId": "HA-DW", "name": "Dish", "type": "Dishwasher", "connected": True})
    assert any(p.endswith("/programs/selected") for p in paths), paths
    assert any(p.endswith("/programs/active") for p in paths), paths


# -- Red-team wave 1: reconnect escalation (#11) -------------------------------

def test_event_stream_escalates_backoff_after_repeated_failed_opens():
    # A sustained outage at the 60s cap would burn ~1440 stream opens/day; after
    # escalate_after consecutive failures the delay must jump to the extended cap.
    api = _StubApi([HomeConnectError("boom")] * 6)
    sleeps = []
    stop = threading.Event()

    def fake_sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) >= 5:
            stop.set()

    logger = Mock()
    EventStream(api, dispatch=lambda e: None, logger=logger, sleep=fake_sleep,
                escalate_after=3, reconnect_extended=900.0).run(stop)
    assert sleeps[:2] == [1.0, 2.0]           # normal doubling below the threshold
    assert sleeps[2:] == [900.0, 900.0, 900.0]  # escalated and stays escalated
    slow_warnings = [c for c in logger.warning.call_args_list if "slowing reconnects" in str(c)]
    assert len(slow_warnings) == 1            # logged once at the transition, not per cycle


def test_event_stream_short_lived_connects_count_toward_escalation():
    # Connect-then-immediate-drop flapping burns opens just like refused opens:
    # streams that die before proving stable must escalate too.
    api = _StubApi([_StubStreamResp([])] * 6)   # opens fine, ends instantly
    sleeps = []
    stop = threading.Event()

    def fake_sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) >= 4:
            stop.set()

    EventStream(api, dispatch=lambda e: None, logger=Mock(), sleep=fake_sleep,
                escalate_after=3, reconnect_extended=900.0,
                monotonic=lambda: 0.0).run(stop)   # frozen clock: lived == 0 < stable
    assert sleeps[-1] == 900.0


def test_event_stream_closes_stream_when_shutdown_wins_open_race():
    # stop() racing the open-to-register gap closes a _current that is still
    # None; the reader must notice and close the fresh stream instead of
    # entering lines() on a stream shutdown can no longer reach.
    stop = threading.Event()

    class _RaceStream:
        def __init__(self):
            self.closed = False

        def lines(self):
            raise AssertionError("reader must never read a post-shutdown stream")

        def close(self):
            self.closed = True

    race_stream = _RaceStream()

    class _RaceApi:
        def open_stream(self, path, read_timeout=None, abort_check=None):  # noqa: ARG002
            stop.set()                     # shutdown lands between open and register
            return race_stream

    dispatched = []
    EventStream(_RaceApi(), dispatch=lambda e: dispatched.append(e.event),
                logger=Mock(), sleep=lambda s: None).run(stop)
    assert race_stream.closed
    assert START not in dispatched         # never claimed to be connected


def test_event_stream_stable_stream_resets_escalation():
    # After escalation, one stream that survives past stable_seconds must reset
    # the failure count: the next drop reconnects fast again (60s cap), not 15min.
    clock = Clock()

    class _LongLivedStream:
        def lines(self):
            clock.t += 500                           # stream "lives" 500s > 120s stable
            return iter([])

        def close(self):
            pass

    streams = deque([HomeConnectError("down")] * 3
                    + [_LongLivedStream()]           # connects, lives past stable
                    + [HomeConnectError("down")] * 2)

    class _Api:
        def open_stream(self, path, read_timeout=None, abort_check=None):  # noqa: ARG002
            item = streams.popleft()
            if isinstance(item, Exception):
                raise item
            return item

    sleeps = []
    stop = threading.Event()

    def fake_sleep(seconds):
        sleeps.append(seconds)
        if not streams:
            stop.set()

    EventStream(_Api(), dispatch=lambda e: None, logger=Mock(), sleep=fake_sleep,
                escalate_after=3, reconnect_extended=900.0,
                monotonic=clock.monotonic).run(stop)
    assert sleeps[2] == 900.0                        # escalated after 3 failures
    assert sleeps[3] == 1.0                          # stable stream reset the count


# -- Red-team wave 2 (#15 removal, #20 dispatch, #21 zombie, L3 parser) --------

def test_parse_bare_field_name_is_valid_not_garbage():
    # SSE allows "data" with no colon (empty value) — it must not restart the
    # stream. All-hex tokens stay garbage (the injected-chunk-length bug), and
    # hex lengths often start with a LETTER, not a digit.
    events = list(parse_sse_lines(lines("event:STATUS\ndata\n\n")))
    assert events == [{"event": "STATUS", "data": ""}]
    for garbage in ("1a2f", "cafe", "DEAD", "a2f1"):
        with pytest.raises(SseParseError):
            list(parse_sse_lines(lines(f"event:STATUS\n{garbage}\n\n")))


def test_depaired_removes_appliance_and_notifies():
    removed = []
    api = FakeAPI()
    api.get_json = lambda path: {"data": {"homeappliances": [
        {"haId": "HA-GONE", "name": "D", "type": "Dishwasher", "connected": True}]}}
    coord = HomeConnectCoordinator(api, logger=Mock(), stream=StubStream(),
                                   on_removed=removed.append)
    coord._discover()                        # pylint: disable=protected-access
    assert len(coord.appliances()) == 1
    coord._route(SseEvent(DEPAIRED, haid="HA-GONE"))  # pylint: disable=protected-access
    assert coord.appliances() == []
    assert removed == ["HA-GONE"]


def test_discovery_prunes_vanished_appliances():
    # haId churn: an appliance re-registered under a new haId vanishes from the
    # list with no DEPAIRED event — a successful pass must prune it.
    removed = []
    listings = deque([
        {"data": {"homeappliances": [
            {"haId": "HA-A", "name": "A", "type": "Dishwasher", "connected": True},
            {"haId": "HA-B", "name": "B", "type": "Dryer", "connected": True}]}},
        {"data": {"homeappliances": [
            {"haId": "HA-A", "name": "A", "type": "Dishwasher", "connected": True}]}},
    ])
    api = FakeAPI()
    api.get_json = lambda path: listings.popleft()
    coord = HomeConnectCoordinator(api, logger=Mock(), stream=StubStream(),
                                   on_removed=removed.append)
    coord._discover()                        # pylint: disable=protected-access
    assert len(coord.appliances()) == 2
    coord._discover()                        # pylint: disable=protected-access
    assert [a.haid for a in coord.appliances()] == ["HA-A"]
    assert removed == ["HA-B"]


def test_failed_discovery_never_depairs():
    removed = []
    listings = deque([
        {"data": {"homeappliances": [
            {"haId": "HA-A", "name": "A", "type": "Dishwasher", "connected": True}]}},
        HomeConnectError("outage", status=503),
    ])

    def get_json(path):
        item = listings.popleft()
        if isinstance(item, Exception):
            raise item
        return item

    api = FakeAPI()
    api.get_json = get_json
    coord = HomeConnectCoordinator(api, logger=Mock(), stream=StubStream(),
                                   on_removed=removed.append)
    coord._discover()                        # pylint: disable=protected-access
    coord._discover()                        # failed pass: fleet must survive
    assert len(coord.appliances()) == 1
    assert removed == []


class _KeepAliveStream:
    def __init__(self, count):
        self._count = count

    def lines(self):
        for _ in range(self._count):
            yield "event:KEEP-ALIVE"
            yield ""

    def close(self):
        pass


def test_zombie_stream_renewed_when_program_active():
    # Keep-alives keep the 120s watchdog quiet while the backend has stopped
    # generating events (BSH-confirmed); mid-program that means renew.
    clock = Clock()

    def ticking_monotonic():
        clock.t += 1000.0
        return clock.t

    stream = EventStream(None, dispatch=lambda e: None, logger=Mock(),
                         monotonic=ticking_monotonic,
                         activity_check=lambda: True, zombie_after=1500.0)
    with pytest.raises(HomeConnectError) as exc:
        list(stream._read_events(_KeepAliveStream(5)))   # pylint: disable=protected-access
    assert "zombie" in str(exc.value)


def test_zombie_stream_left_alone_when_idle():
    clock = Clock()

    def ticking_monotonic():
        clock.t += 1000.0
        return clock.t

    stream = EventStream(None, dispatch=lambda e: None, logger=Mock(),
                         monotonic=ticking_monotonic,
                         activity_check=lambda: False, zombie_after=1500.0)
    assert list(stream._read_events(_KeepAliveStream(5))) == []  # pylint: disable=protected-access


def test_real_events_reset_zombie_clock():
    # A stream interleaving real events with keep-alives must never renew —
    # otherwise every mid-program stream churns each zombie_after window.
    clock = Clock()

    def ticking_monotonic():
        clock.t += 100.0
        return clock.t

    class _MixedStream:
        def lines(self):
            for _ in range(6):
                yield "event:KEEP-ALIVE"
                yield ""
                yield "event:STATUS"
                yield "id:H"
                yield "data:{}"
                yield ""

        def close(self):
            pass

    stream = EventStream(None, dispatch=lambda e: None, logger=Mock(),
                         monotonic=ticking_monotonic,
                         activity_check=lambda: True, zombie_after=1500.0)
    events = list(stream._read_events(_MixedStream()))   # pylint: disable=protected-access
    assert len(events) == 6                              # completed without renewal


class _ScriptedLines:
    def __init__(self, clock, script):
        self._clock = clock
        self._script = script

    def lines(self):
        for at, line in self._script:
            self._clock.t = at
            yield line

    def close(self):
        pass


def test_raw_lines_counted_before_the_parser_drops_them():
    clock = Clock()
    stream = EventStream(None, dispatch=lambda e: None, logger=Mock(), monotonic=clock.monotonic)
    script = [(10.0, ": hb"), (20.0, ""), (30.0, "event:KEEP-ALIVE"), (40.0, ""), (50.0, ":")]
    assert list(stream._read_events(_ScriptedLines(clock, script))) == []   # pylint: disable=protected-access
    clock.t = 100.0
    stats = stream.stats()
    assert stats["raw_lines"] == 5
    assert stats["comment_heartbeats"] == 2           # only the ":" lines
    assert stats["last_raw_line_age"] == 50.0         # 100 - 50
    assert stats["keepalives"] == 1


def test_raw_line_stats_report_none_age_before_any_line():
    stream = EventStream(None, dispatch=lambda e: None, logger=Mock(), monotonic=Clock().monotonic)
    stats = stream.stats()
    assert (stats["raw_lines"], stats["comment_heartbeats"], stats["last_raw_line_age"]) == (0, 0, None)


def test_comment_only_stream_never_triggers_zombie_renewal():
    clock = Clock()
    script = [(i * 1000.0, ": hb") for i in range(1, 20)]
    stream = EventStream(None, dispatch=lambda e: None, logger=Mock(), monotonic=clock.monotonic,
                         activity_check=lambda: True, zombie_after=1500.0)
    assert list(stream._read_events(_ScriptedLines(clock, script))) == []   # pylint: disable=protected-access
    assert stream.stats()["comment_heartbeats"] == 19
    assert stream.stats()["keepalives"] == 0


def test_keepalive_frames_still_trigger_zombie_renewal_amid_comments():
    clock = Clock()
    script = [(10.0, ": hb"), (2000.0, ": hb"), (2001.0, "event:KEEP-ALIVE"), (2002.0, "")]
    stream = EventStream(None, dispatch=lambda e: None, logger=Mock(), monotonic=clock.monotonic,
                         activity_check=lambda: True, zombie_after=1500.0)
    with pytest.raises(HomeConnectError) as exc:
        list(stream._read_events(_ScriptedLines(clock, script)))   # pylint: disable=protected-access
    assert getattr(exc.value, "zombie_renewal", False)


def test_zombie_renewal_stop_carries_no_error():
    # The renewal STOP must take the grace-delay path: with an error, every
    # bridge would flap Off and fire triggers mid-program on every renewal.
    clock = Clock()

    def ticking_monotonic():
        clock.t += 1000.0
        return clock.t

    class _RenewApi:
        def open_stream(self, path, read_timeout=None, abort_check=None):  # noqa: ARG002
            return _KeepAliveStream(5)

    stops = []
    stop = threading.Event()

    def dispatch(event):
        if event.event == STOP:
            stops.append(event)

    def fake_sleep(seconds):  # noqa: ARG001
        stop.set()

    EventStream(_RenewApi(), dispatch=dispatch, logger=Mock(), sleep=fake_sleep,
                monotonic=ticking_monotonic,
                activity_check=lambda: True, zombie_after=1500.0).run(stop)
    assert len(stops) == 1
    assert stops[0].error is None                        # grace path, no flap


def test_discovery_deferred_while_gate_closed():
    # Discovery runs on the worker that also routes events: it must reschedule,
    # never block, when the rate-limit gate is closed.
    api = FakeAPI()
    api.get_json = Mock()
    api.gate_wait = 300.0
    coord = HomeConnectCoordinator(api, logger=Mock(), stream=StubStream())
    posted = []
    coord._scheduler.post = lambda fn, delay=0: posted.append(delay)  # pylint: disable=protected-access
    coord._discover()                        # pylint: disable=protected-access
    assert not api.get_json.called           # no HTTP attempted
    assert posted == [301.0]                 # rescheduled past the gate


def test_reader_program_swallows_connection_init_failed():
    # The cloud's "still (re)establishing its own link" 409: treating it as a
    # failure ground a listed-as-connected appliance through 144 req/day (#16).
    reader, api = make_reader()
    api.get_json = Mock(side_effect=HomeConnectError(
        "x", status=409, key="SDK.Error.HomeAppliance.Connection.Initialization.Failed"))
    assert reader.get_selected_program("H") is None
    assert reader.get_active_program("H") is None


def test_discovery_with_closed_gate_never_blocks_even_during_shutdown():
    # A stop() racing an in-flight _discover must not fall through to the
    # blocking HTTP call: closed gate always skips it, shutdown just skips the
    # reschedule too.
    api = FakeAPI()
    api.get_json = Mock()
    api.gate_wait = 300.0
    coord = HomeConnectCoordinator(api, logger=Mock(), stream=StubStream())
    coord._stop.set()                        # pylint: disable=protected-access
    posted = []
    coord._scheduler.post = lambda fn, delay=0: posted.append(delay)  # pylint: disable=protected-access
    coord._discover()                        # pylint: disable=protected-access
    assert not api.get_json.called           # no HTTP, no block
    assert posted == []                      # and no reschedule while stopping


# -- Liveness counters / stream status (#26) -----------------------------------

class _ClockedStream:
    """Yields keep-alive at t=10 and a STATUS at t=20 on the shared test clock."""

    def __init__(self, clock):
        self._clock = clock

    def lines(self):
        self._clock.t = 10.0
        yield "event:KEEP-ALIVE"
        yield ""
        self._clock.t = 20.0
        yield "event:STATUS"
        yield "id:H"
        yield "data:{}"
        yield ""

    def close(self):
        pass


def test_stream_stats_before_anything_reports_nothing_not_health():
    stats = EventStream(None, dispatch=lambda e: None, logger=Mock()).stats()
    assert stats["connected"] is False
    assert stats["connected_for"] is None and stats["last_connect_age"] is None
    assert stats["keepalives"] == 0 and stats["wire_events"] == 0
    assert stats["last_keepalive_age"] is None and stats["last_wire_event_age"] is None


def test_stream_counts_keepalives_separately_and_ages_use_injected_clock():
    clock = Clock()
    stream = EventStream(None, dispatch=lambda e: None, logger=Mock(),
                         monotonic=clock.monotonic)
    got = list(stream._read_events(_ClockedStream(clock)))   # pylint: disable=protected-access
    assert [e.event for e in got] == [STATUS]
    clock.t = 50.0
    stats = stream.stats()
    assert stats["keepalives"] == 1 and stats["wire_events"] == 1
    assert stats["last_keepalive_age"] == pytest.approx(40.0)
    assert stats["last_wire_event_age"] == pytest.approx(30.0)
    assert stats["last_wire_event_type"] == STATUS


def test_stream_counters_survive_reconnect_and_connected_since_clears():
    transport = ScriptedTransport()
    transport.queue_stream(200, HC_JSON, [b"event:KEEP-ALIVE\n\nevent:STATUS\nid:H\ndata:{}\n\n", b""])
    transport.queue_stream(200, HC_JSON, [b"event:KEEP-ALIVE\n\nevent:NOTIFY\nid:H\ndata:{}\n\n", b""])
    clock = Clock()
    api = make_api(transport, clock)
    stop = threading.Event()
    stops = []
    while_connected = []

    def dispatch(event):
        if event.event == START:
            while_connected.append(stream.stats())
        if event.event == STOP:
            stops.append(stream.stats())
            if len(stops) == 2:
                stop.set()

    stream = EventStream(api, dispatch=dispatch, logger=Mock(), sleep=clock.sleep,
                         monotonic=clock.monotonic)
    stream.run(stop)
    assert all(s["connected"] for s in while_connected) and len(while_connected) == 2
    assert [s["connected"] for s in stops] == [False, False]   # cleared before STOP dispatches
    stats = stream.stats()
    assert stats["connected"] is False and stats["connected_for"] is None   # cleared at stream end
    assert stats["last_connect_age"] is not None
    assert stats["keepalives"] == 2 and stats["wire_events"] == 2           # not reset on reconnect
    assert stats["last_wire_event_type"] == "NOTIFY"


# -- Coordinator: per-appliance last message + diagnostics logging -------------

RAW_HAID = "HAID-12345678901234"
_DW = {"haId": RAW_HAID, "name": "Dishwasher", "type": "Dishwasher", "connected": True}


class _StatsStream(StubStream):
    def stats(self):
        return {"connected": True, "connected_for": 5.0}


def make_diag_coordinator(appliances, clock):
    api = FakeAPI()
    api.get_json = lambda path: {"data": {"homeappliances": appliances}}
    logger = Mock()
    coord = HomeConnectCoordinator(api, logger=logger, stream=_StatsStream(),
                                   monotonic=clock.monotonic)
    coord._discover()                        # pylint: disable=protected-access
    return coord, logger


def _status(haid=RAW_HAID, keys=("BSH.Common.Status.DoorState",), value="Open"):
    return SseEvent(STATUS, haid=haid,
                    data={"items": [{"key": k, "value": value} for k in keys]})


def test_routed_status_records_last_message_and_logs_debug_without_values_or_haid():
    clock = Clock()
    coord, logger = make_diag_coordinator([_DW], clock)
    clock.t = 100.0
    coord._route(_status(keys=("BSH.Common.Status.DoorState",     # pylint: disable=protected-access
                               "BSH.Common.Status.OperationState")))
    clock.t = 160.0
    entry = coord.stream_status()["appliances"][0]
    assert entry["last_message_type"] == STATUS
    assert entry["last_message_age"] == pytest.approx(60.0)
    assert entry["name"] == "Dishwasher" and entry["haid"] != RAW_HAID
    lines = [c.args[0] % c.args[1:] for c in logger.debug.call_args_list
             if c.args and c.args[0].startswith("Home Connect event %s for")]
    assert lines == ["Home Connect event STATUS for Dishwasher (2 item(s): DoorState, OperationState)"]
    assert "Open" not in lines[0] and RAW_HAID not in lines[0]
    args = [c.args for c in logger.debug.call_args_list
            if c.args and c.args[0].startswith("Home Connect event %s for")][0]
    assert not any(arg in (RAW_HAID, "Open") for arg in args[1:])


def test_debug_line_handles_non_dict_and_empty_items():
    coord, logger = make_diag_coordinator([_DW], Clock())
    coord._get(RAW_HAID).merge_items = lambda items: None    # pylint: disable=protected-access
    coord._route(SseEvent(STATUS, haid=RAW_HAID,                     # pylint: disable=protected-access
                          data={"items": ["stray", 5, {"key": "A.B.DoorState", "value": "Open"}]}))
    coord._route(SseEvent(STATUS, haid=RAW_HAID, data={"items": []}))  # pylint: disable=protected-access
    lines = [c.args[0] % c.args[1:] for c in logger.debug.call_args_list
             if c.args and c.args[0].startswith("Home Connect event %s for")]
    assert lines == ["Home Connect event STATUS for Dishwasher (1 item(s): DoorState)",
                     "Home Connect event STATUS for Dishwasher (0 item(s): none)"]


def test_debug_line_caps_key_list_at_five():
    coord, logger = make_diag_coordinator([_DW], Clock())
    coord._route(_status(keys=tuple(f"A.B.K{i}" for i in range(7))))   # pylint: disable=protected-access
    args = [c.args for c in logger.debug.call_args_list
            if c.args and c.args[0].startswith("Home Connect event %s for")][0]
    assert args[3] == 7 and args[4] == "K0, K1, K2, K3, K4, …"


def _first_lines(logger):
    return [c.args for c in logger.info.call_args_list
            if c.args and "first event after connect" in c.args[0]]


def test_first_event_info_fires_once_per_start_not_before_start():
    coord, logger = make_diag_coordinator([_DW], Clock())
    coord._route(_status())                  # pylint: disable=protected-access
    assert _first_lines(logger) == []        # no START yet
    coord._route(SseEvent(START))            # pylint: disable=protected-access
    coord._route(_status())                  # pylint: disable=protected-access
    coord._route(_status())                  # pylint: disable=protected-access
    assert len(_first_lines(logger)) == 1
    assert _first_lines(logger)[0][1:] == (STATUS, "Dishwasher")
    coord._route(SseEvent(START))            # pylint: disable=protected-access
    coord._route(SseEvent("NOTIFY", haid=RAW_HAID, data={"items": []}))  # pylint: disable=protected-access
    assert len(_first_lines(logger)) == 2


def test_connection_notice_does_not_consume_first_event_line():
    coord, logger = make_diag_coordinator([_DW], Clock())
    coord._route(SseEvent(START))            # pylint: disable=protected-access
    coord._route(SseEvent("CONNECTED", haid=RAW_HAID))     # pylint: disable=protected-access
    coord._route(SseEvent("DISCONNECTED", haid=RAW_HAID))  # pylint: disable=protected-access
    assert _first_lines(logger) == []
    coord._route(_status())                  # pylint: disable=protected-access
    assert len(_first_lines(logger)) == 1
    coord._route(SseEvent(STOP))             # pylint: disable=protected-access
    coord._route(SseEvent(START))            # pylint: disable=protected-access
    coord._route(_status())                  # pylint: disable=protected-access
    assert len(_first_lines(logger)) == 2


def test_diagnostics_failure_never_drops_the_state_merge(monkeypatch):
    coord, logger = make_diag_coordinator([_DW], Clock())
    appliance = coord._get(RAW_HAID)         # pylint: disable=protected-access
    merged = []
    appliance.merge_items = merged.append

    def boom(_self):
        raise RuntimeError("boom")

    monkeypatch.setattr(type(appliance), "name", property(boom))
    coord._route(_status())                  # pylint: disable=protected-access
    assert len(merged) == 1                  # STATUS items still reached the appliance
    logger.exception.assert_called_once_with("Home Connect event diagnostics failed")


def test_unknown_appliance_event_creates_no_last_message_and_no_first_event_line():
    coord, logger = make_diag_coordinator([_DW], Clock())
    coord._scheduler.post = lambda fn, delay=0: None                # pylint: disable=protected-access
    coord._route(SseEvent(START))            # pylint: disable=protected-access
    coord._route(_status(haid="UNKNOWN-HAID-000000"))               # pylint: disable=protected-access
    assert coord.stream_status()["appliances"][0]["last_message_age"] is None
    assert not [c for c in logger.info.call_args_list
                if c.args and "first event after connect" in c.args[0]]


def test_appliance_without_messages_reports_none_not_zero():
    other = {"haId": "OTHER-HAID-1234567", "name": "Oven", "type": "Oven", "connected": False}
    coord, _ = make_diag_coordinator([_DW, other], Clock())
    coord._route(_status())                  # pylint: disable=protected-access
    entries = {e["name"]: e for e in coord.stream_status()["appliances"]}
    assert entries["Dishwasher"]["last_message_type"] == STATUS
    assert entries["Oven"]["last_message_age"] is None and entries["Oven"]["connected"] is False
    assert coord.stream_status()["connected"] is True     # stream stats merged in


# -- format_stream_status verdicts ----------------------------------------------

def _st(**kw):
    base = {"connected": True, "connected_for": 600.0, "last_connect_age": 600.0,
            "keepalives": 0, "last_keepalive_age": None, "wire_events": 0,
            "raw_lines": 0, "last_raw_line_age": None, "comment_heartbeats": 0,
            "last_wire_event_age": None, "last_wire_event_type": None,
            "appliances": [], "requests_today": 7, "daily_budget": 1000}
    base.update(kw)
    return base


def _verdict(status):
    return format_stream_status(status)[-1]


def test_verdict_a_not_connected_never_claims_events():
    # Events were received earlier in the process, but the stream is down now.
    out = format_stream_status(_st(connected=False, connected_for=None, last_connect_age=180.0,
                                   wire_events=3, last_wire_event_age=200.0,
                                   last_wire_event_type=STATUS))
    assert "NOT connected" in out[1] and "last connected 3m ago" in out[1]
    assert "NOT connected" in out[-1]
    assert "receiving events" not in " ".join(out)


def test_verdict_a_never_connected():
    out = format_stream_status(_st(connected=False, connected_for=None, last_connect_age=None))
    assert "never connected since plugin start" in out[1]


def test_verdict_b_connected_ten_minutes_keepalives_zero_events_is_not_healthy():
    out = format_stream_status(_st(connected_for=600.0, keepalives=14, last_keepalive_age=32.0,
                                   raw_lines=30, last_raw_line_age=32.0, comment_heartbeats=2))
    assert "NO events received in 10m" in out[-1]
    assert "trigger a change" in out[-1]
    assert ("(server data last received 32s ago; KEEP-ALIVE frames: 14, "
            "comment heartbeats: 2)") in out[-1]
    assert "  raw stream lines: 30 (last 32s ago), comment heartbeats: 2" in out
    assert "the cloud is sending nothing" not in out[-1]
    assert "arriving" not in " ".join(out)
    assert "  keep-alives: 14 (last 32s ago)" in out
    assert "  wire events: 0 since plugin start" in out


def test_verdict_b_without_keepalives_words_neutrally():
    verdict = _verdict(_st(connected_for=900.0))
    assert "NO events received in 15m" in verdict
    assert "no data at all from the server on this connection" in verdict
    assert "KEEP-ALIVE frames" not in verdict


def test_verdict_b_raw_data_only_from_a_previous_connection_reads_as_no_data():
    verdict = _verdict(_st(connected_for=600.0, raw_lines=9, last_raw_line_age=4000.0))
    assert "no data at all from the server on this connection" in verdict


def test_verdict_b_heartbeats_only_shows_data_but_not_events():
    verdict = _verdict(_st(connected_for=600.0, raw_lines=5, last_raw_line_age=40.0,
                           comment_heartbeats=5))
    assert "NO events received in 10m" in verdict
    assert "server data last received 40s ago; KEEP-ALIVE frames: 0, comment heartbeats: 5" in verdict


def test_verdict_b_unavailable_raw_counters_are_reported_not_read_as_zero():
    status = _st(connected_for=600.0)
    for key in ("raw_lines", "last_raw_line_age", "comment_heartbeats"):
        del status[key]
    out = format_stream_status(status)
    assert "raw line counters unavailable" in out[-1]
    assert "no data at all" not in out[-1]
    assert "  raw stream lines: unavailable" in out


def test_format_raw_stream_lines_zero_and_nonzero():
    assert "  raw stream lines: 0 since plugin start, comment heartbeats: 0" in \
        format_stream_status(_st())
    out = format_stream_status(_st(raw_lines=1234, last_raw_line_age=7.0, comment_heartbeats=0))
    assert "  raw stream lines: 1234 (last 7s ago), comment heartbeats: 0" in out


def test_verdict_b_events_only_from_a_previous_connection_do_not_count():
    # Per-process counter says 3 events, but the last one predates this connection.
    verdict = _verdict(_st(connected_for=600.0, wire_events=3, last_wire_event_age=4000.0,
                           last_wire_event_type=STATUS))
    assert "NO events received in 10m" in verdict


def test_verdict_c_too_early_to_call_it_silent():
    verdict = _verdict(_st(connected_for=120.0))
    assert "connected 2m ago, no events yet (too early to call it silent)" in verdict


def _app(name="Dishwasher", age=240.0, kind=STATUS):
    return {"name": name, "type": "Dishwasher", "haid": "1040…4372", "connected": True,
            "last_message_age": age, "last_message_type": kind}


def test_verdict_d_status_updates_arriving_is_neutral_not_normal():
    out = format_stream_status(_st(connected_for=3600.0, wire_events=3, last_wire_event_age=240.0,
                                   last_wire_event_type=STATUS, appliances=[_app()]))
    assert "  wire events: 3 (last STATUS 4m ago)" in out
    assert out[1] == "  stream: connected for 1h00m"
    assert out[-1] == ("  verdict: status updates are arriving (last STATUS for Dishwasher 4m ago); "
                       "an idle appliance can legitimately be quiet")
    assert "normally" not in out[-1]


def test_verdict_status_from_before_this_connection_does_not_count():
    verdict = _verdict(_st(connected_for=600.0, wire_events=3, last_wire_event_age=4000.0,
                           appliances=[_app(age=4000.0)]))
    assert "NO events received" in verdict and "arriving" not in verdict


def test_verdict_only_connection_events_is_not_status_updates():
    verdict = _verdict(_st(connected_for=600.0, wire_events=2, last_wire_event_age=30.0,
                           last_wire_event_type="CONNECTED",
                           appliances=[_app(age=30.0, kind="CONNECTED")]))
    assert "only connection events so far (last CONNECTED 30s ago), no status updates" in verdict
    assert "arriving" not in verdict


def test_verdict_events_not_routed_to_any_known_appliance():
    verdict = _verdict(_st(connected_for=600.0, wire_events=3, last_wire_event_age=30.0,
                           last_wire_event_type=STATUS,
                           appliances=[_app(age=None, kind=None)]))
    assert "none were routed" in verdict and "3 wire events" in verdict
    assert "status updates are arriving" not in verdict


def test_verdict_only_connection_and_unknown_events_fed_through_a_real_stream():
    clock = Clock()
    coord, _ = make_diag_coordinator([_DW], clock)
    stream = EventStream(None, dispatch=lambda e: None, logger=Mock(), monotonic=clock.monotonic)
    coord._stream = stream                   # pylint: disable=protected-access
    coord._scheduler.post = lambda fn, delay=0: None          # pylint: disable=protected-access
    stream._set_connected(0.0)               # pylint: disable=protected-access

    class _Wire:
        def lines(self):
            clock.t = 30.0
            yield from ["event:CONNECTED", "id:" + RAW_HAID, "data:{}", "",
                        "event:DISCONNECTED", "id:" + RAW_HAID, "data:{}", "",
                        "event:SOMETHING-NEW", "data:{}", ""]

    for event in stream._read_events(_Wire()):                # pylint: disable=protected-access
        coord._route(event)                  # pylint: disable=protected-access
    clock.t = 60.0
    status = coord.stream_status()
    assert status["wire_events"] == 3
    verdict = _verdict(status)
    assert "only connection events so far (last DISCONNECTED" in verdict
    assert "status updates are arriving" not in verdict


def test_reconnect_with_only_keepalives_reads_silent_after_five_minutes():
    clock = Clock()
    stream = EventStream(None, dispatch=lambda e: None, logger=Mock(), monotonic=clock.monotonic)
    # Connection 1 delivered a STATUS (counters are per-process and survive).
    list(stream._read_events(_ClockedStream(clock)))         # pylint: disable=protected-access
    assert stream.stats()["wire_events"] == 1
    seen = {}

    class _KeepAlivesOnly:
        def lines(self):
            clock.t = 1000.0
            stream._set_connected(clock.t)   # pylint: disable=protected-access
            yield "event:KEEP-ALIVE"
            yield ""
            clock.t = 1000.0 + 360.0
            yield "event:KEEP-ALIVE"
            yield ""
            seen["status"] = stream.stats()

    assert list(stream._read_events(_KeepAlivesOnly())) == []   # pylint: disable=protected-access
    status = seen["status"]
    status["appliances"] = [_app(age=1e6)]   # its STATUS predates this connection
    verdict = _verdict(status)
    assert "NO events received in 6m" in verdict
    assert "server data last received 0s ago; KEEP-ALIVE frames: 3, comment heartbeats: 0" in verdict
    assert "arriving" not in verdict


def test_format_lists_appliances_and_request_budget():
    out = format_stream_status(_st(appliances=[
        {"name": "Dishwasher", "type": "Dishwasher", "haid": "1040…4372", "connected": True,
         "last_message_age": 240.0, "last_message_type": "NOTIFY"},
        {"name": "Oven", "type": None, "haid": "2222…9999", "connected": False,
         "last_message_age": None, "last_message_type": None}]))
    assert ("  Dishwasher [Dishwasher] haId=1040…4372: connected=True, "
            "last message: NOTIFY 4m ago") in out
    assert ("  Oven [?] haId=2222…9999: connected=False, "
            "last message: none since plugin start") in out
    assert "  requests today: 7/1000" in out


def test_format_reports_request_counter_unavailable_explicitly():
    out = format_stream_status(_st(requests_today="unavailable", daily_budget=None))
    assert "  requests today: unavailable" in out
