#!/usr/bin/env python3
"""Live Home Connect event-stream check (Phase 2 validation / soak).

Opens the single global SSE stream, discovers appliances, and prints events as
they arrive so you can watch real state changes before wiring the plugin's
device layer (Phase 3). Reuses the same ``.env`` credentials and cached
``.live_tokens.json`` as ``tools/live_auth_check.py`` — run that first if you
have not authorized yet.

    python3 tools/live_events_check.py             # production stream
    python3 tools/live_events_check.py --simulator  # simulator (6 fake appliances)

All haIds are redacted in output; Ctrl-C exits cleanly. Simon runs this for the
multi-day soak; it costs one request per (re)connect plus the state re-reads, so
prefer ``--simulator`` for casual poking.
"""
import argparse
import logging
import os
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER_PLUGIN = os.path.join(REPO_ROOT, "Home Connect.indigoPlugin", "Contents", "Server Plugin")
sys.path.insert(0, SERVER_PLUGIN)

from hc_api import HomeConnectAPI, HomeConnectError, PROD_HOST, SIMULATOR_HOST, redact  # noqa: E402
from hc_auth import HomeConnectAuth  # noqa: E402
from hc_events import HomeConnectCoordinator  # noqa: E402

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


def authorize(auth, simulator):
    if auth.is_authorized():
        print("Reusing saved authorization.")
        return
    if simulator:
        print("Requesting simulator authorization (auto-approved)…")
        auth.authorize_simulator()
        return

    def on_prompt(info):
        uri = info.get("verification_uri_complete") or info.get("verification_uri")
        print("\n  Authorize this app in your browser:")
        print(f"    {uri}")
        print(f"    (user code: {info.get('user_code')})\n")
        print("Waiting for authorization…")

    status, detail = auth.run_device_flow(on_prompt=on_prompt)
    if status != "success":
        sys.exit(f"Authorization did not complete: {status} {detail or ''}")


def main():
    parser = argparse.ArgumentParser(description="Home Connect live event-stream check")
    parser.add_argument("--simulator", action="store_true", help="use the simulator host + credentials")
    parser.add_argument("--debug", action="store_true", help="verbose logging from the hc_* modules")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

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

    try:
        authorize(auth, args.simulator)
    except HomeConnectError as exc:
        sys.exit(f"Authorization failed: {exc}")

    def on_appliance(appliance):
        print(f"  ▸ appliance {appliance.name} [{appliance.type}] "
              f"haId={redact(appliance.haid)} connected={appliance.connected}")

    def on_event(event):
        haid = redact(event.haid) if event.haid else "-"
        items = event.items()
        detail = ""
        if items:
            keys = ", ".join(str(item.get("key")) for item in items[:4])
            detail = f" items=[{keys}{'…' if len(items) > 4 else ''}]"
        print(f"  [{event.event}] haId={haid}{detail}")

    coordinator = HomeConnectCoordinator(api, on_appliance=on_appliance, on_event=on_event)
    print("Opening event stream — Ctrl-C to stop…\n")
    coordinator.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping…")
    finally:
        coordinator.stop()
    print("Stopped cleanly.")


if __name__ == "__main__":
    main()
