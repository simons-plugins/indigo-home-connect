"""Unit tests for hc_appliance.py — state merge, EVENT dedupe, re-read queue,
observer isolation, and connection lifecycle."""
from unittest.mock import Mock

from hc_api import HomeConnectError
from hc_appliance import (HomeConnectAppliance, CONNECTED, DISCONNECT_DELAY,
                          READ_MIN_DELAY, SELECTED_PROGRAM)
from support import Clock, FakeReader, RecordingScheduler


def make_appliance(reader=None, scheduler=None, info=None, supports_programs=True):
    reader = reader or FakeReader()
    scheduler = scheduler or RecordingScheduler()
    appliance = HomeConnectAppliance(
        "BOSCH-HCS01-0123456789AB", info or {"name": "Dishwasher", "type": "Dishwasher",
                                             "connected": True},
        reader, scheduler.post, logger=Mock(), supports_programs=supports_programs)
    return appliance, reader, scheduler


# -- State merge --------------------------------------------------------------

def test_merge_items_updates_cache_and_notifies():
    appliance, _, _ = make_appliance()
    seen = []
    appliance.subscribe("BSH.Common.Status.OperationState", lambda k, v: seen.append((k, v)))
    appliance.merge_items([
        {"key": "BSH.Common.Status.OperationState", "value": "BSH.Common.EnumType.OperationState.Run"},
        {"key": "BSH.Common.Status.DoorState", "value": "Closed"},
    ])
    assert appliance.get("BSH.Common.Status.OperationState").endswith("Run")
    assert appliance.get("BSH.Common.Status.DoorState") == "Closed"
    assert seen == [("BSH.Common.Status.OperationState",
                     "BSH.Common.EnumType.OperationState.Run")]


def test_wildcard_observer_sees_every_key():
    appliance, _, _ = make_appliance()
    seen = []
    appliance.subscribe(None, lambda k, v: seen.append(k))
    appliance.merge_items([{"key": "A", "value": 1}, {"key": "B", "value": 2}])
    assert seen == ["A", "B"]


def test_unknown_key_stored_not_crashed():
    appliance, _, _ = make_appliance()
    appliance.merge_items([{"key": "Vendor.Weird.Undocumented", "value": {"nested": 1}}])
    assert appliance.get("Vendor.Weird.Undocumented") == {"nested": 1}


# -- EVENT de-duplication -----------------------------------------------------

def test_event_items_deduped_by_key_and_timestamp():
    appliance, _, _ = make_appliance()
    fired = []
    appliance.subscribe("BSH.Common.Event.ProgramFinished", lambda k, v: fired.append(v))
    item = {"key": "BSH.Common.Event.ProgramFinished", "timestamp": 1717000000,
            "value": "BSH.Common.EnumType.EventPresentState.Present"}
    first = appliance.handle_event_items([item])
    # Same (key, timestamp) re-sent on reconnect must not fire again.
    second = appliance.handle_event_items([dict(item)])
    assert len(first) == 1
    assert second == []
    assert fired == ["BSH.Common.EnumType.EventPresentState.Present"]


def test_event_same_key_new_timestamp_fires_again():
    appliance, _, _ = make_appliance()
    fired = []
    appliance.subscribe("BSH.Common.Event.ProgramFinished", lambda k, v: fired.append(v))
    appliance.handle_event_items([{"key": "BSH.Common.Event.ProgramFinished",
                                   "timestamp": 1, "value": "Present"}])
    appliance.handle_event_items([{"key": "BSH.Common.Event.ProgramFinished",
                                   "timestamp": 2, "value": "Present"}])
    assert fired == ["Present", "Present"]


# -- Observer isolation -------------------------------------------------------

def test_raising_observer_does_not_break_dispatch():
    appliance, _, _ = make_appliance()
    delivered = []

    def boom(key, value):
        raise RuntimeError("observer blew up")

    appliance.subscribe(None, boom)
    appliance.subscribe(None, lambda k, v: delivered.append(k))
    appliance.merge_items([{"key": "A", "value": 1}])
    assert delivered == ["A"]      # second observer still ran


# -- Re-read queue ------------------------------------------------------------

def test_reread_runs_actions_in_order():
    reader = FakeReader()
    reader.queue("appliance", {"connected": True})
    reader.queue("status", [{"key": "BSH.Common.Status.DoorState", "value": "Open"}])
    reader.queue("settings", [{"key": "BSH.Common.Setting.PowerState", "value": "On"}])
    reader.queue("selected_program", {"key": "Dishcare.Dishwasher.Program.Eco50",
                                      "options": [{"key": "opt", "value": 1}]})
    reader.queue("active_program", None)
    appliance, _, scheduler = make_appliance(reader=reader)

    appliance.on_paired()          # schedules the read
    scheduler.run_all()

    assert reader.calls == ["appliance", "status", "settings",
                            "selected_program", "active_program"]
    assert appliance.get("BSH.Common.Status.DoorState") == "Open"
    assert appliance.get("BSH.Common.Setting.PowerState") == "On"
    assert appliance.get(SELECTED_PROGRAM) == "Dishcare.Dishwasher.Program.Eco50"
    assert appliance.get("opt") == 1


def test_reread_skips_programs_when_unsupported():
    reader = FakeReader()
    appliance, _, scheduler = make_appliance(reader=reader, supports_programs=False)
    appliance.on_paired()
    scheduler.run_all()
    assert reader.calls == ["appliance", "status", "settings"]


def test_reread_retries_with_backoff_on_error():
    reader = FakeReader()
    reader.queue("appliance", {"connected": True})
    reader.queue("status", HomeConnectError("boom", status=500))
    appliance, _, scheduler = make_appliance(reader=reader)

    appliance.on_paired()
    scheduler.run_next()           # runs appliance + status(fails) -> reschedules
    # A retry job was posted with the minimum backoff delay.
    assert scheduler.history[-1] == READ_MIN_DELAY
    # Now let the retry succeed.
    reader.queue("status", [{"key": "BSH.Common.Status.DoorState", "value": "Closed"}])
    reader.queue("settings", [])
    reader.queue("selected_program", None)
    reader.queue("active_program", None)
    scheduler.run_all()
    assert appliance.get("BSH.Common.Status.DoorState") == "Closed"


def test_reread_abandoned_when_disconnected():
    reader = FakeReader()
    reader.queue("appliance", {"connected": True})
    reader.queue("status", HomeConnectError("boom", status=500))
    appliance, _, scheduler = make_appliance(reader=reader)

    appliance.on_paired()
    scheduler.run_next()           # appliance ok, status fails -> retry scheduled
    calls_before = list(reader.calls)

    appliance.on_disconnected()    # abandon the queue; never burn budget offline
    scheduler.run_all()            # the retry job runs but must do no reads
    assert reader.calls == calls_before
    assert appliance.connected is False


# -- Connection lifecycle -----------------------------------------------------

def test_start_schedules_reread():
    appliance, reader, scheduler = make_appliance()
    appliance.on_stream_start()
    assert len(scheduler.jobs) == 1
    scheduler.run_all()
    assert reader.calls[:3] == ["appliance", "status", "settings"]


def test_stop_without_error_schedules_delayed_disconnect():
    appliance, _, scheduler = make_appliance()
    changes = []
    appliance.subscribe(CONNECTED, lambda k, v: changes.append(v))
    appliance.on_stream_stop(error=None)
    assert scheduler.history[-1] == DISCONNECT_DELAY
    scheduler.run_all()
    assert appliance.connected is False
    assert changes == [False]


def test_stop_with_error_disconnects_immediately():
    appliance, _, scheduler = make_appliance()
    appliance.on_stream_stop(error=HomeConnectError("dead"))
    assert scheduler.history[-1] == 0.0


def test_start_cancels_pending_disconnect():
    appliance, _, scheduler = make_appliance()
    appliance.on_stream_stop(error=None)     # schedules disconnect job
    appliance.on_stream_start()              # supersedes it (+ schedules a read)
    scheduler.run_all()
    # The disconnect job was cancelled by the newer generation.
    assert appliance.connected is True


def test_connected_event_marks_connected_and_rereads():
    appliance, reader, scheduler = make_appliance(info={"name": "D", "type": "Dishwasher",
                                                        "connected": False})
    assert appliance.connected is False
    appliance.on_connected()
    scheduler.run_all()
    assert appliance.connected is True
    assert "appliance" in reader.calls


def test_guard_require_connected():
    appliance, _, _ = make_appliance(info={"name": "D", "connected": False})
    try:
        appliance.require_connected()
        assert False, "expected HomeConnectError"
    except HomeConnectError:
        pass


# -- Red-team wave 1: re-read freshness suppression (#12) ---------------------

def test_reconnect_within_fresh_window_skips_reread():
    # Stream reconnects and CONNECTED flaps just after a full read must NOT
    # cost another 5-GET pass; a flapping appliance can otherwise exhaust the
    # daily budget with zero user activity.
    clock = Clock()
    reader = FakeReader()
    sched = RecordingScheduler()
    appliance = HomeConnectAppliance("H", {"connected": True}, reader, sched.post,
                                     logger=Mock(), monotonic=clock.monotonic)
    appliance.on_paired()
    sched.run_all()
    assert len(reader.calls) == 5                # appliance/status/settings/sel/act

    appliance.on_stream_start()                  # reconnect moments later
    sched.run_all()
    appliance.on_connected()                     # CONNECTED flap
    sched.run_all()
    assert len(reader.calls) == 5                # both suppressed

    clock.t += 400                               # past the 5-minute window
    appliance.on_stream_start()
    sched.run_all()
    assert len(reader.calls) == 10               # re-read runs again


def test_paired_forces_reread_despite_fresh_window():
    clock = Clock()
    reader = FakeReader()
    sched = RecordingScheduler()
    appliance = HomeConnectAppliance("H", {"connected": True}, reader, sched.post,
                                     logger=Mock(), monotonic=clock.monotonic)
    appliance.on_paired()
    sched.run_all()
    appliance.on_paired()                        # PAIRED may mean a NEW appliance
    sched.run_all()
    assert len(reader.calls) == 10
