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
