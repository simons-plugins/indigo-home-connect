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
                       SseEvent, SseParseError, build_event, parse_sse_lines,
                       START, STOP, STATUS)
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

    def open_stream(self, path, read_timeout=None):  # noqa: ARG002
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
    coord, _ = make_coordinator(
        [{"haId": "HAID-1", "name": "D", "type": "Dishwasher", "connected": True}])
    coord._discover()                        # pylint: disable=protected-access
    coord._dispatch(SseEvent(STATUS, haid="HAID-1",                # pylint: disable=protected-access
                             data={"items": [{"key": "BSH.Common.Status.DoorState",
                                              "value": "Open"}]}))
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
    assert tap == [START, STOP]


def test_coordinator_unknown_appliance_triggers_discovery():
    coord, _ = make_coordinator([])
    posted = []
    coord._scheduler.post = lambda fn, delay=0: posted.append(delay)  # pylint: disable=protected-access
    coord._dispatch(SseEvent(STATUS, haid="UNKNOWN",  # pylint: disable=protected-access
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
