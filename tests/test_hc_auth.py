"""Unit tests for hc_auth.py — Device Flow, simulator, refresh, persistence."""
import json
import os
import threading
from unittest.mock import Mock

import pytest

import hc_auth
from hc_auth import (HomeConnectAuth, TokenStore, STATE_AUTHORIZED, STATE_AUTH_REQUIRED,
                     STATE_PENDING, STATE_UNAUTHORIZED, SCOPES)
from support import FakeAPI, FakeRaw, oauth_error

CLIENT_A = "client-aaaa-0001"
CLIENT_B = "client-bbbb-0002"

DEVICE_AUTH = {
    "device_code": "DEV-CODE-XYZ",
    "user_code": "ABCD-1234",
    "verification_uri": "https://verify.home-connect.com",
    "verification_uri_complete": "https://verify.home-connect.com?user_code=ABCD-1234",
    "expires_in": 300,
    "interval": 5,
}


def token_response(access="ACCESS-1", refresh="REFRESH-1", expires_in=86400, scope=SCOPES):
    return {"access_token": access, "refresh_token": refresh,
            "expires_in": expires_in, "scope": scope}


def make_auth(api, tmp_path, client_id=CLIENT_A, secret="secret-x", simulator=False, now=None):
    token_file = os.path.join(str(tmp_path), "tokens.json")
    return HomeConnectAuth(api, token_file, client_id, secret, simulator=simulator,
                           logger=Mock(), now=now or (lambda: 1_000.0), sleep=lambda s: None)


# -- Device Flow --------------------------------------------------------------

def test_device_flow_happy_path(tmp_path):
    api = FakeAPI()
    api.queue_post(DEVICE_AUTH).queue_post(token_response())
    auth = make_auth(api, tmp_path)

    info = auth.start_device_flow()
    assert info["user_code"] == "ABCD-1234"
    assert auth.state() == STATE_PENDING

    status, _ = auth.poll_device_flow()
    assert status == "success"
    assert auth.is_authorized()
    assert auth.authorization_header() == "ACCESS-1"
    assert auth.scopes() == SCOPES.split()

    # Token persisted to disk with absolute expiry.
    with open(auth._token_path, encoding="utf-8") as handle:   # pylint: disable=protected-access
        stored = json.load(handle)
    assert stored[CLIENT_A]["refresh_token"] == "REFRESH-1"
    assert stored[CLIENT_A]["expires_at"] == 1_000.0 + 86400


def test_run_device_flow_reuses_code_started_by_caller(tmp_path):
    """The Authorize button calls start_device_flow (to display the user code)
    then hands off to run_device_flow on a worker thread. run_device_flow must
    poll THAT code, not start a second flow — otherwise the user approves the
    displayed code while the plugin polls an orphaned one (jarvis 2026-08-02:
    two user codes logged ~200ms apart, auth never completed)."""
    api = FakeAPI()
    api.queue_post(DEVICE_AUTH).queue_post(token_response())
    auth = make_auth(api, tmp_path)

    info = auth.start_device_flow()
    status, _ = auth.run_device_flow()
    assert status == "success"
    device_auth_posts = [c for c in api.post_calls if c["path"].endswith("device_authorization")]
    assert len(device_auth_posts) == 1          # no second flow started
    token_posts = [c for c in api.post_calls if c["path"].endswith("token")]
    assert token_posts[0]["form"]["device_code"] == "DEV-CODE-XYZ"
    assert info["user_code"] == "ABCD-1234"


def test_device_flow_pending_then_success(tmp_path):
    api = FakeAPI()
    (api.queue_post(DEVICE_AUTH)
        .queue_post(oauth_error("authorization_pending"))
        .queue_post(token_response()))
    auth = make_auth(api, tmp_path)
    status, _ = auth.run_device_flow()
    assert status == "success"
    assert auth.is_authorized()


def test_device_flow_slow_down_increases_interval(tmp_path):
    api = FakeAPI()
    (api.queue_post(DEVICE_AUTH)
        .queue_post(oauth_error("slow_down"))
        .queue_post(token_response()))
    auth = make_auth(api, tmp_path)
    status, _ = auth.run_device_flow()
    assert status == "success"
    assert auth._device_interval == 10     # pylint: disable=protected-access


def test_device_flow_access_denied(tmp_path):
    api = FakeAPI()
    api.queue_post(DEVICE_AUTH).queue_post(oauth_error("access_denied", description="user said no"))
    auth = make_auth(api, tmp_path)
    status, detail = auth.run_device_flow()
    assert status == "denied"
    assert detail == "user said no"
    assert auth.state() == STATE_UNAUTHORIZED
    assert not auth.is_authorized()


def test_device_flow_expired_token_restarts(tmp_path):
    api = FakeAPI()
    (api.queue_post(DEVICE_AUTH)
        .queue_post(oauth_error("expired_token"))
        .queue_post(DEVICE_AUTH)
        .queue_post(token_response()))
    auth = make_auth(api, tmp_path)
    status, _ = auth.run_device_flow(max_restarts=1)
    assert status == "success"
    device_auth_posts = [c for c in api.post_calls if c["path"].endswith("device_authorization")]
    assert len(device_auth_posts) == 2     # restarted once


def test_device_flow_grant_type_urn_fallback(tmp_path):
    api = FakeAPI()
    (api.queue_post(DEVICE_AUTH)
        .queue_post(oauth_error("unsupported_grant_type"))
        .queue_post(token_response()))
    auth = make_auth(api, tmp_path)
    auth.start_device_flow()
    status, _ = auth.poll_device_flow()
    assert status == "success"
    grant_types = [c["form"].get("grant_type") for c in api.post_calls if c["path"].endswith("token")]
    assert "urn:ietf:params:oauth:grant-type:device_code" in grant_types


# -- Simulator ----------------------------------------------------------------

def test_simulator_authorization_code_grant(tmp_path):
    api = FakeAPI()
    location = "https://apiclient.home-connect.com/o2c.html?code=SIMCODE-999&state=x"
    api.queue_request(FakeRaw(302, {"Location": location}))
    api.queue_post(token_response(access="SIM-ACCESS"))
    auth = make_auth(api, tmp_path, simulator=True)
    status, _ = auth.authorize_simulator()
    assert status == "success"
    assert auth.authorization_header() == "SIM-ACCESS"
    token_post = [c for c in api.post_calls if c["path"].endswith("token")][0]
    assert token_post["form"]["grant_type"] == "authorization_code"
    assert token_post["form"]["code"] == "SIMCODE-999"
    assert token_post["form"]["redirect_uri"]   # required by the simulator


def test_simulator_no_redirect_raises(tmp_path):
    api = FakeAPI()
    api.queue_request(FakeRaw(200, {}))
    auth = make_auth(api, tmp_path, simulator=True)
    with pytest.raises(Exception):
        auth.authorize_simulator()


# -- Refresh ------------------------------------------------------------------

def _seed_token(auth, api, **kwargs):
    """Store an initial token by running one device-flow poll."""
    api.queue_post(DEVICE_AUTH).queue_post(token_response(**kwargs))
    auth.start_device_flow()
    auth.poll_device_flow()


def test_refresh_persists_new_refresh_token(tmp_path):
    api = FakeAPI()
    auth = make_auth(api, tmp_path)
    _seed_token(auth, api, access="ACCESS-1", refresh="REFRESH-1")

    api.queue_post(token_response(access="ACCESS-2", refresh="REFRESH-2"))
    assert auth.refresh_if_needed(force=True) is True
    assert auth.authorization_header() == "ACCESS-2"
    with open(auth._token_path, encoding="utf-8") as handle:   # pylint: disable=protected-access
        stored = json.load(handle)
    assert stored[CLIENT_A]["refresh_token"] == "REFRESH-2"    # new refresh token persisted


def test_refresh_429_sets_backoff_and_keeps_token(tmp_path):
    api = FakeAPI()
    auth = make_auth(api, tmp_path)
    _seed_token(auth, api)

    api.queue_post(oauth_error("too_many", status=429, retry_after=30))
    assert auth.refresh_if_needed(force=True) is False
    assert auth.state() == STATE_AUTHORIZED       # token retained
    assert auth._refresh_backoff_until == 1_000.0 + 30  # pylint: disable=protected-access


def test_refresh_invalid_grant_sets_auth_required(tmp_path):
    api = FakeAPI()
    auth = make_auth(api, tmp_path)
    _seed_token(auth, api)

    api.queue_post(oauth_error("invalid_grant", status=400))
    assert auth.refresh_if_needed(force=True) is False
    assert auth.state() == STATE_AUTH_REQUIRED


def test_auth_required_halts_all_refresh_attempts_until_reauth(tmp_path):
    """After invalid_grant, no further token requests may be sent (100/day refresh
    limit): refresh_if_needed and handle_unauthorized must short-circuit without
    HTTP until a successful re-auth resets the state."""
    api = FakeAPI()
    auth = make_auth(api, tmp_path)
    _seed_token(auth, api)

    api.queue_post(oauth_error("invalid_grant", status=400))
    assert auth.refresh_if_needed(force=True) is False
    assert auth.state() == STATE_AUTH_REQUIRED
    posts_after_failure = len(api.post_calls)

    # Minute-tick and 401-hook paths: zero network traffic while auth-required.
    for _ in range(3):
        assert auth.refresh_if_needed(force=True) is False
    assert auth.handle_unauthorized(auth.authorization_header()) is False
    assert len(api.post_calls) == posts_after_failure

    # Successful re-auth (device flow completion calls _store_token) recovers.
    auth._store_token({"access_token": "new-access", "refresh_token": "new-refresh",  # pylint: disable=protected-access
                       "expires_in": 86400})
    assert auth.state() == STATE_AUTHORIZED
    api.queue_post({"access_token": "newer", "refresh_token": "newer-r", "expires_in": 86400})
    assert auth.refresh_if_needed(force=True) is True


def test_next_refresh_due_one_hour_before_expiry(tmp_path):
    api = FakeAPI()
    auth = make_auth(api, tmp_path)
    _seed_token(auth, api, expires_in=86400)
    assert auth.next_refresh_due() == 1_000.0 + 86400 - 3600


def test_refresh_respects_min_interval(tmp_path):
    api = FakeAPI()
    auth = make_auth(api, tmp_path)
    _seed_token(auth, api)
    # Force the token past its refresh window but mark a very recent attempt.
    entry = auth._store.get(CLIENT_A)                        # pylint: disable=protected-access
    entry["expires_at"] = 1_000.0
    auth._store.set(CLIENT_A, entry)                         # pylint: disable=protected-access
    auth._last_refresh_attempt = 1_000.0                     # pylint: disable=protected-access
    assert auth.refresh_if_needed(force=False) is False
    assert not any(c["form"].get("grant_type") == "refresh_token" for c in api.post_calls)


# -- 401 handling -------------------------------------------------------------

def test_handle_unauthorized_forces_refresh(tmp_path):
    api = FakeAPI()
    auth = make_auth(api, tmp_path)
    _seed_token(auth, api, access="ACCESS-1", refresh="REFRESH-1")
    api.queue_post(token_response(access="ACCESS-2", refresh="REFRESH-2"))
    assert auth.handle_unauthorized("ACCESS-1") is True
    assert auth.authorization_header() == "ACCESS-2"


def test_handle_unauthorized_ignores_stale_token(tmp_path):
    api = FakeAPI()
    auth = make_auth(api, tmp_path)
    _seed_token(auth, api, access="ACCESS-1")
    assert auth.handle_unauthorized("OLD-TOKEN") is False
    assert not any(c["form"].get("grant_type") == "refresh_token" for c in api.post_calls)


# -- Token store persistence --------------------------------------------------

def test_token_store_keyed_per_client(tmp_path):
    token_file = os.path.join(str(tmp_path), "tokens.json")

    api_a = FakeAPI()
    auth_a = HomeConnectAuth(api_a, token_file, CLIENT_A, "s", logger=Mock(), now=lambda: 1.0)
    _seed_token(auth_a, api_a, access="A-ACCESS", refresh="A-REFRESH")

    api_b = FakeAPI()
    auth_b = HomeConnectAuth(api_b, token_file, CLIENT_B, "s", logger=Mock(), now=lambda: 1.0)
    _seed_token(auth_b, api_b, access="B-ACCESS", refresh="B-REFRESH")

    # A third agent for client A must still see A's token (B did not clobber it).
    api_c = FakeAPI()
    auth_c = HomeConnectAuth(api_c, token_file, CLIENT_A, "s", logger=Mock(), now=lambda: 1.0)
    assert auth_c.authorization_header() == "A-ACCESS"

    with open(token_file, encoding="utf-8") as handle:
        stored = json.load(handle)
    assert set(stored.keys()) == {CLIENT_A, CLIENT_B}


def test_token_file_permissions_0600(tmp_path):
    api = FakeAPI()
    auth = make_auth(api, tmp_path)
    _seed_token(auth, api)
    mode = os.stat(auth._token_path).st_mode & 0o777   # pylint: disable=protected-access
    assert mode == 0o600


# -- Concurrency / crash-safety regressions (Phase 1 review) ------------------

def test_atomic_save_does_not_corrupt_file_on_replace_failure(tmp_path, monkeypatch):
    path = os.path.join(str(tmp_path), "tokens.json")
    store = TokenStore(path, logger=Mock())
    store.set(CLIENT_A, {"access_token": "A1", "refresh_token": "R1", "expires_at": 1.0})
    with open(path, encoding="utf-8") as handle:
        assert json.load(handle)[CLIENT_A]["access_token"] == "A1"

    # os.replace fails mid-write: the real file must survive intact, no .tmp litter.
    def boom(_src, _dst):
        raise OSError("simulated crash during replace")

    monkeypatch.setattr(hc_auth.os, "replace", boom)
    store.set(CLIENT_B, {"access_token": "B1", "refresh_token": "R2", "expires_at": 1.0})  # no raise

    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
    assert data[CLIENT_A]["access_token"] == "A1"       # original untouched / valid
    assert CLIENT_B not in data                          # failed write not persisted
    leftovers = [f for f in os.listdir(str(tmp_path)) if f.endswith(".tmp")]
    assert leftovers == []                               # temp file cleaned up


def test_late_save_from_stale_instance_does_not_clobber(tmp_path):
    token_file = os.path.join(str(tmp_path), "tokens.json")

    # Instance 2 authorizes (the live one).
    api2 = FakeAPI()
    auth2 = HomeConnectAuth(api2, token_file, CLIENT_A, "s", logger=Mock(), now=lambda: 1.0)
    _seed_token(auth2, api2, access="NEW-ACCESS", refresh="NEW-REFRESH")

    # Instance 1 is superseded, then its in-flight device flow completes late.
    api1 = FakeAPI()
    auth1 = HomeConnectAuth(api1, token_file, CLIENT_A, "s", logger=Mock(), now=lambda: 1.0)
    auth1.mark_stale()
    api1.queue_post(DEVICE_AUTH).queue_post(token_response(access="STALE-ACCESS"))
    auth1.start_device_flow()
    status, _ = auth1.poll_device_flow()

    assert status == "cancelled"                         # stale instance refused to save
    assert auth2.authorization_header() == "NEW-ACCESS"  # live token survives
    with open(token_file, encoding="utf-8") as handle:
        assert json.load(handle)[CLIENT_A]["access_token"] == "NEW-ACCESS"


def test_concurrent_refresh_submits_exactly_once(tmp_path):
    api = FakeAPI()
    auth = make_auth(api, tmp_path)
    _seed_token(auth, api, access="A1", refresh="R1")
    # Only ONE refresh response is queued: if both threads submitted, the second
    # would pop an empty deque and raise.
    api.queue_post(token_response(access="A2", refresh="R2"))

    barrier = threading.Barrier(2)
    results = []
    errors = []

    def worker():
        try:
            barrier.wait()
            results.append(auth.refresh_if_needed(force=True))
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert errors == []
    refresh_posts = [c for c in api.post_calls if c["form"].get("grant_type") == "refresh_token"]
    assert len(refresh_posts) == 1                        # single-use token submitted once
    assert all(results)                                  # both callers see a fresh token
    assert auth.authorization_header() == "A2"
