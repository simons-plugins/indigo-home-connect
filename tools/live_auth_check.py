#!/usr/bin/env python3
"""Live Home Connect authorization check (Phase 1 validation).

Runs the real OAuth flow from the terminal, then lists your appliances so you
can confirm the client id + auth path work end to end before wiring the plugin
into Indigo.

    python3 tools/live_auth_check.py             # production Device Flow
    python3 tools/live_auth_check.py --simulator  # simulator Code Grant

Reads ``.env`` from the repo root (``clientid = ...`` / ``clientsecret = ...``,
optionally ``simulator_clientid`` / ``simulator_clientsecret`` for ``--simulator``;
spaces around ``=`` are tolerated). Tokens are cached in a gitignored
``.live_tokens.json`` so re-runs reuse the authorization. Appliance haIds are
redacted in output.
"""
import argparse
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER_PLUGIN = os.path.join(REPO_ROOT, "Home Connect.indigoPlugin", "Contents", "Server Plugin")
sys.path.insert(0, SERVER_PLUGIN)

from hc_api import HomeConnectAPI, HomeConnectError, PROD_HOST, SIMULATOR_HOST, redact  # noqa: E402
from hc_auth import HomeConnectAuth  # noqa: E402

TOKEN_FILE = os.path.join(REPO_ROOT, ".live_tokens.json")
ENV_FILE = os.path.join(REPO_ROOT, ".env")


def load_env(path):
    creds = {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                    value = value[1:-1].strip()
                creds[key.strip().lower()] = value
    except OSError as exc:
        sys.exit(f"Could not read {path}: {exc}")
    return creds


def main():
    parser = argparse.ArgumentParser(description="Home Connect live auth check")
    parser.add_argument("--simulator", action="store_true", help="use the simulator Code Grant flow")
    args = parser.parse_args()

    creds = load_env(ENV_FILE)
    if args.simulator:
        client_id = creds.get("simulator_clientid") or creds.get("clientid")
        client_secret = creds.get("simulator_clientsecret", "")
    else:
        client_id = creds.get("clientid")
        client_secret = creds.get("clientsecret", "")
    if not client_id:
        sys.exit(f"No 'clientid' found in {ENV_FILE}")

    host = SIMULATOR_HOST if args.simulator else PROD_HOST
    api = HomeConnectAPI(host=host)
    auth = HomeConnectAuth(api, TOKEN_FILE, client_id, client_secret, simulator=args.simulator)
    api.set_token_provider(auth.authorization_header)
    api.set_unauthorized_handler(auth.handle_unauthorized)

    if not auth.is_authorized():
        try:
            if args.simulator:
                print("Requesting simulator authorization (auto-approved)…")
                auth.authorize_simulator()
            else:
                def on_prompt(info):
                    uri = info.get("verification_uri_complete") or info.get("verification_uri")
                    print("\n  Authorize this app in your browser:")
                    print(f"    {uri}")
                    print(f"    (user code: {info.get('user_code')})\n")
                    print("Waiting for authorization…")

                status, detail = auth.run_device_flow(on_prompt=on_prompt)
                if status != "success":
                    sys.exit(f"Authorization did not complete: {status} {detail or ''}")
        except HomeConnectError as exc:
            sys.exit(f"Authorization failed: {exc}")
    else:
        print("Reusing saved authorization.")

    print("Authorized. Fetching appliances…\n")
    try:
        payload = api.get_json("/api/homeappliances")
    except HomeConnectError as exc:
        sys.exit(f"Failed to list appliances: {exc}")

    appliances = (payload or {}).get("data", {}).get("homeappliances", [])
    if not appliances:
        print("No appliances returned.")
        return
    for appliance in appliances:
        name = appliance.get("name") or appliance.get("type") or "?"
        atype = appliance.get("type", "?")
        connected = appliance.get("connected")
        haid = redact(appliance.get("haId"))
        print(f"  • {name}  [{atype}]  haId={haid}  connected={connected}")


if __name__ == "__main__":
    main()
