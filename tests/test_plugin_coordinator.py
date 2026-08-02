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
