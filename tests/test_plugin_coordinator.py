"""Tests for plugin.py coordinator reconciliation: auth-state gating and the
stale-api restart that guarantees the ONE-global-stream invariant across a
prefs-change rebuild racing the 60s supervisor."""
import plugin
from hc_auth import STATE_AUTHORIZED, STATE_AUTH_REQUIRED


def _plugin():
    return plugin.Plugin("com.simons-plugins.homeconnect", "Home Connect", "2026.0.3", {})


class _FakeAuth:
    def __init__(self, state=STATE_AUTHORIZED):
        self._state = state

    def state(self):
        return self._state

    def is_authorized(self):
        return self._state == STATE_AUTHORIZED

    def mark_stale(self):
        pass


class _FakeCoordinator:
    events = []

    def __init__(self, api, **kwargs):        # noqa: D401 - matches real signature
        self.api = api
        self.started = False
        self.stopped = False
        _FakeCoordinator.events.append(("create", id(self)))

    def start(self):
        self.started = True
        _FakeCoordinator.events.append(("start", id(self)))
        return True

    def stop(self, timeout=5.0):              # noqa: ARG002
        self.stopped = True
        _FakeCoordinator.events.append(("stop", id(self)))
        return True


def _patch(monkeypatch):
    _FakeCoordinator.events = []
    monkeypatch.setattr(plugin, "HomeConnectCoordinator", _FakeCoordinator)


def test_reconcile_starts_only_when_authorized(monkeypatch):
    _patch(monkeypatch)
    p = _plugin()
    p._api = object()                         # pylint: disable=protected-access
    p._auth = _FakeAuth(STATE_AUTH_REQUIRED)  # pylint: disable=protected-access
    p._reconcile_coordinator()                # pylint: disable=protected-access
    assert p._coordinator is None             # pylint: disable=protected-access

    p._auth = _FakeAuth(STATE_AUTHORIZED)     # pylint: disable=protected-access
    p._reconcile_coordinator()                # pylint: disable=protected-access
    assert p._coordinator is not None and p._coordinator.started  # pylint: disable=protected-access


def test_reconcile_stops_when_authorization_lost(monkeypatch):
    _patch(monkeypatch)
    p = _plugin()
    auth = _FakeAuth(STATE_AUTHORIZED)
    p._api = object()                         # pylint: disable=protected-access
    p._auth = auth                            # pylint: disable=protected-access
    p._reconcile_coordinator()                # pylint: disable=protected-access
    coord = p._coordinator                    # pylint: disable=protected-access
    assert coord is not None

    auth._state = STATE_AUTH_REQUIRED         # invalid_grant flips state, store stays
    p._reconcile_coordinator()                # pylint: disable=protected-access
    assert coord.stopped
    assert p._coordinator is None             # pylint: disable=protected-access


def test_reconcile_restarts_on_stale_api_never_two_streams(monkeypatch):
    _patch(monkeypatch)
    p = _plugin()
    api1 = object()
    p._api = api1                             # pylint: disable=protected-access
    p._auth = _FakeAuth(STATE_AUTHORIZED)     # pylint: disable=protected-access
    p._reconcile_coordinator()                # pylint: disable=protected-access
    old = p._coordinator                      # pylint: disable=protected-access
    assert old.api is api1

    # Prefs change swapped in a new api; the coordinator is now wired to a stale
    # client and must be restarted against the current one.
    api2 = object()
    p._api = api2                             # pylint: disable=protected-access
    p._reconcile_coordinator()                # pylint: disable=protected-access
    new = p._coordinator                      # pylint: disable=protected-access
    assert new is not old
    assert new.api is api2
    assert old.stopped
    # The old stream was stopped strictly before the new one started.
    events = _FakeCoordinator.events
    assert events.index(("stop", id(old))) < events.index(("start", id(new)))


# -- Red-team wave 1: auth loss surfaces on devices (#10), no-op supersession (#9)

from unittest.mock import Mock


def test_auth_loss_marks_bridged_devices(monkeypatch):
    # When the refresh token dies mid-flight, devices must NOT freeze at their
    # last healthy-looking state: every bridge gets mark_auth_required().
    _patch(monkeypatch)
    p = _plugin()
    auth = _FakeAuth(STATE_AUTHORIZED)
    p._api = object()                         # pylint: disable=protected-access
    p._auth = auth                            # pylint: disable=protected-access
    p._reconcile_coordinator()                # pylint: disable=protected-access
    bridge = Mock()
    p._bridges[1] = bridge                    # pylint: disable=protected-access

    auth._state = STATE_AUTH_REQUIRED
    p._reconcile_coordinator()                # pylint: disable=protected-access
    bridge.mark_auth_required.assert_called_once()


def test_rebuild_with_unchanged_client_is_noop_supersession():
    # A prefs save with unchanged values must keep the live auth instance —
    # a pending device-flow authorization dies if it is marked stale.
    p = _plugin()

    class _MatchingAuth(_FakeAuth):
        def __init__(self):
            super().__init__()
            self.staled = False

        def matches(self, client_id, client_secret, simulator):  # noqa: ARG002
            return True

        def mark_stale(self):
            self.staled = True

    auth = _MatchingAuth()
    api = object()
    p._api = api                              # pylint: disable=protected-access
    p._auth = auth                            # pylint: disable=protected-access
    p._rebuild_client()                       # pylint: disable=protected-access
    assert p._auth is auth                    # pylint: disable=protected-access
    assert p._api is api                      # pylint: disable=protected-access
    assert not auth.staled


def test_auth_loss_marking_waits_for_coordinator_to_actually_stop(monkeypatch):
    # A stuck coordinator can still push stale state over the auth-required
    # write: devices are only marked once stop() really succeeded; the next
    # reconcile tick retries.
    _patch(monkeypatch)
    p = _plugin()
    auth = _FakeAuth(STATE_AUTHORIZED)
    p._api = object()                         # pylint: disable=protected-access
    p._auth = auth                            # pylint: disable=protected-access
    p._reconcile_coordinator()                # pylint: disable=protected-access
    coord = p._coordinator                    # pylint: disable=protected-access
    bridge = Mock()
    p._bridges[1] = bridge                    # pylint: disable=protected-access

    coord.stop = lambda timeout=5.0: False    # reader thread stuck
    auth._state = STATE_AUTH_REQUIRED
    p._reconcile_coordinator()                # pylint: disable=protected-access
    bridge.mark_auth_required.assert_not_called()

    coord.stop = lambda timeout=5.0: True     # next tick: stop succeeds
    p._reconcile_coordinator()                # pylint: disable=protected-access
    bridge.mark_auth_required.assert_called_once()
