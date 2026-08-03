"""Tests for the plugin's Authorize-button config flow (plugin.py wiring).

The ``_build_client`` seam is patched so no network / real auth object is used.
"""
import plugin


def _plugin():
    return plugin.Plugin("com.simons-plugins.homeconnect", "Home Connect", "2026.0.2", {})


class _FakeAuthSimulator:
    def authorize_simulator(self):
        return ("success", None)


class _FakeAuthDeviceFlow:
    def __init__(self):
        self.prompted = False

    def start_device_flow(self):
        return {
            "verification_uri_complete": "https://verify.example/xyz",
            "verification_uri": "https://verify.example",
            "user_code": "CODE-1234",
        }

    def run_device_flow(self, should_stop=None):   # noqa: D401
        return ("success", None)


def test_authorize_button_requires_client_id():
    p = _plugin()
    out = p.authorizeButtonPressed({"clientId": "", "useSimulator": False})
    assert "Client ID" in out["authStatus"]
    assert p._auth_thread is None          # pylint: disable=protected-access


def test_authorize_button_simulator(monkeypatch):
    p = _plugin()
    monkeypatch.setattr(p, "_build_client", lambda cid, sec, sim: (object(), _FakeAuthSimulator()))
    out = p.authorizeButtonPressed({"clientId": "abc", "useSimulator": True})
    assert "simulator" in out["authStatus"].lower()


def test_authorize_button_device_flow_shows_code_and_starts_worker(monkeypatch):
    p = _plugin()
    monkeypatch.setattr(p, "_build_client", lambda cid, sec, sim: (object(), _FakeAuthDeviceFlow()))
    out = p.authorizeButtonPressed({"clientId": "abc", "useSimulator": False})
    assert out["authInstructions"] == "https://verify.example/xyz"
    assert out["authUserCode"] == "CODE-1234"
    assert "Waiting" in out["authStatus"]
    thread = p._auth_thread                 # pylint: disable=protected-access
    assert thread is not None
    thread.join(timeout=2)


def test_cancel_close_leaves_device_flow_worker_running(monkeypatch):
    # The dialog is documented as closable while authorization completes in the
    # background: Cancel must not kill the polling worker (the granted token
    # would be silently discarded and the device code is spent server-side).
    p = _plugin()
    monkeypatch.setattr(p, "_build_client", lambda cid, sec, sim: (object(), _FakeAuthDeviceFlow()))
    p.authorizeButtonPressed({"clientId": "abc", "useSimulator": False})
    stop_event = p._stop_auth               # pylint: disable=protected-access
    p.closedPrefsConfigUi({}, userCancelled=True)
    assert not stop_event.is_set()


def test_authorize_press_aborts_superseded_api(monkeypatch):
    # A thread waiting out the old client's rate-limit gate must be unblocked
    # when the user re-authorizes, same as a prefs-change rebuild.
    from unittest.mock import Mock
    p = _plugin()
    old_api = Mock()
    p._api = old_api                        # pylint: disable=protected-access
    monkeypatch.setattr(p, "_build_client", lambda cid, sec, sim: (object(), _FakeAuthDeviceFlow()))
    p.authorizeButtonPressed({"clientId": "abc", "useSimulator": False})
    old_api.abort.assert_called_once()


def test_authorize_press_stops_coordinator_wired_to_old_api(monkeypatch):
    # Aborting the old api without stopping its coordinator leaves the stream
    # loop error-spinning against a dead client (false "failing repeatedly /
    # slowing reconnects" warnings) until the next reconcile tick.
    from unittest.mock import Mock
    p = _plugin()
    coordinator = Mock()
    coordinator.stop.return_value = True
    p._coordinator = coordinator
    p._coordinator_api = object()
    p._api = Mock()
    monkeypatch.setattr(p, "_build_client", lambda cid, sec, sim: (object(), _FakeAuthDeviceFlow()))
    p.authorizeButtonPressed({"clientId": "abc", "useSimulator": False})
    coordinator.stop.assert_called_once()
    assert p._coordinator is None


def test_failed_device_flow_marks_devices_auth_required(monkeypatch):
    # A flow ending denied/expired leaves no coordinator for the stop-edge
    # marking to fire from — the worker itself must surface it on the devices.
    from unittest.mock import Mock
    import plugin as plugin_mod

    class _DeniedAuth(_FakeAuthDeviceFlow):
        def run_device_flow(self, should_stop=None):
            return ("denied", "user said no")

        def state(self):
            return plugin_mod.STATE_UNAUTHORIZED

    p = _plugin()
    bridge = Mock()
    p._bridges[5] = bridge
    monkeypatch.setattr(p, "_build_client", lambda cid, sec, sim: (object(), _DeniedAuth()))
    p.authorizeButtonPressed({"clientId": "abc", "useSimulator": False})
    p._auth_thread.join(timeout=2)
    bridge.mark_auth_required.assert_called_once()


def test_shutdown_aborts_api():
    # A regression here means a worker blocked at the gate keeps looping for up
    # to the full Retry-After and plugin restart hangs the Indigo host.
    from unittest.mock import Mock
    p = _plugin()
    api = Mock()
    p._api = api
    p.shutdown()
    api.abort.assert_called_once()
