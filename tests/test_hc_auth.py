"""Unit tests for hc_auth.py — Device Flow, simulator, refresh, persistence."""
import json
import os
from unittest.mock import Mock

import pytest

from hc_auth import (HomeConnectAuth, STATE_AUTHORIZED, STATE_AUTH_REQUIRED,
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
    auth._last_refresh_attempt = 1_000.0                     # pylint: disable=protected-access
    auth._tokens[CLIENT_A]["expires_at"] = 1_000.0           # pylint: disable=protected-access
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
