#!/usr/bin/env python3
"""Live Home Connect CONTROL check — SIMULATOR ONLY (Phase 4 validation).

Exercises the full control path against the Home Connect *simulator* only: it
discovers the simulator dishwasher over the real SSE stream, then drives it
through select -> start -> (verify Run) -> pause -> resume -> stop using the same
``hc_control.Controller`` guard rails the plugin uses. It NEVER talks to a
production appliance: the host is hard-wired to the simulator and there is no
production code path in this tool.

    python3 tools/live_control_check.py            # runs the full sequence
    python3 tools/live_control_check.py --debug    # verbose hc_* logging

Reuses ``.env`` (``simulator_clientid``) and the cached ``.live_tokens.json``.
All haIds are redacted. Each step reports success, a local ControlRefused (with
its reason), or an API error — steps the simulator legitimately cannot perform
are reported, not forced.
"""
import argparse
import logging
import os
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER_PLUGIN = os.path.join(REPO_ROOT, "Home Connect.indigoPlugin", "Contents", "Server Plugin")
sys.path.insert(0, SERVER_PLUGIN)

from hc_api import HomeConnectAPI, HomeConnectError, SIMULATOR_HOST, redact  # noqa: E402
from hc_appliance import OPERATION_STATE  # noqa: E402
from hc_auth import HomeConnectAuth  # noqa: E402
from hc_cache import DiskCache  # noqa: E402
from hc_control import Controller, ControlRefused, POWER_STATE_KEY  # noqa: E402
from hc_events import HomeConnectCoordinator  # noqa: E402
import hc_constants as hc  # noqa: E402

TOKEN_FILE = os.path.join(REPO_ROOT, ".live_tokens.json")
CACHE_FILE = os.path.join(REPO_ROOT, ".live_capabilities.json")
ENV_FILE = os.path.join(REPO_ROOT, ".env")

DISCOVER_TIMEOUT = 25
STATE_TIMEOUT = 30


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


def wait_for(predicate, timeout, interval=1.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    return None


def op_tail(appliance):
    return hc.enum_tail(appliance.get(OPERATION_STATE))


def report(step, ok, detail=""):
    mark = "OK " if ok else "-- "
    print(f"  [{mark}] {step}{': ' + detail if detail else ''}")


def _power_cycle_off(controller, dishwasher):
    """Power the simulator dishwasher OFF so the auto-power-on path has work to do.

    Returns True (use power_on_first) if the appliance reached an off state, else
    False with a note — some simulator appliances do not honor PowerState=Off."""
    off_value = "BSH.Common.EnumType.PowerState.Off"
    print("Power-cycle: powering the dishwasher OFF…")
    try:
        controller.set_power(dishwasher, off_value)
    except (ControlRefused, HomeConnectError) as exc:
        print(f"  (could not power off — {exc}); skipping auto-power-on, running normally\n")
        return False
    went_off = wait_for(
        lambda: hc.enum_tail(dishwasher.get(POWER_STATE_KEY)) == "Off"
        or op_tail(dishwasher) == "Inactive", 15)
    if went_off:
        print(f"  dishwasher is off (power={hc.enum_tail(dishwasher.get(POWER_STATE_KEY))}, "
              f"op={op_tail(dishwasher) or 'unknown'}); Start will auto-power-on\n")
        return True
    print("  (simulator did not report the dishwasher off; running without auto-power-on)\n")
    return False


def try_step(step, fn):
    """Run a control step; report success / ControlRefused / API error uniformly."""
    try:
        fn()
        report(step, True)
        return True
    except ControlRefused as exc:
        report(step, False, f"refused locally — {exc.reason}")
    except HomeConnectError as exc:
        report(step, False, f"API error — {exc}")
    return False


def main():
    parser = argparse.ArgumentParser(description="Home Connect simulator control check")
    parser.add_argument("--debug", action="store_true", help="verbose hc_* logging")
    parser.add_argument("--power-cycle", action="store_true",
                        help="power the dishwasher off first, then exercise Start's auto-power-on")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.WARNING,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    creds = load_env(ENV_FILE)
    client_id = creds.get("simulator_clientid") or creds.get("clientid")
    client_secret = creds.get("simulator_clientsecret", "")
    if not client_id:
        sys.exit(f"No 'simulator_clientid' found in {ENV_FILE}")

    api = HomeConnectAPI(host=SIMULATOR_HOST)          # simulator host, hard-wired
    auth = HomeConnectAuth(api, TOKEN_FILE, client_id, client_secret, simulator=True)
    api.set_token_provider(auth.authorization_header)
    api.set_unauthorized_handler(auth.handle_unauthorized)

    print("Home Connect SIMULATOR control check\n" + "=" * 38)
    try:
        if not auth.is_authorized():
            print("Authorizing against the simulator (auto-approved)…")
            auth.authorize_simulator()
        else:
            print("Reusing saved simulator authorization.")
    except HomeConnectError as exc:
        sys.exit(f"Authorization failed: {exc}")

    cache = DiskCache(CACHE_FILE, "live-control")
    controller = Controller(api, cache)
    coordinator = HomeConnectCoordinator(api)
    coordinator.start()
    print("Event stream up — discovering appliances…")

    try:
        dishwasher = wait_for(
            lambda: next((a for a in coordinator.appliances() if a.type == "Dishwasher"), None),
            DISCOVER_TIMEOUT)
        if dishwasher is None:
            sys.exit("No simulator dishwasher discovered — is the simulator account set up?")
        print(f"Dishwasher: {dishwasher.name} haId={redact(dishwasher.haid)}")

        # Wait for the initial state re-read so guard rails see real values.
        wait_for(lambda: dishwasher.get(OPERATION_STATE) is not None, STATE_TIMEOUT)
        print(f"Initial operation state: {op_tail(dishwasher) or 'unknown'}, "
              f"connected={dishwasher.connected}, "
              f"remoteStart={dishwasher.get('BSH.Common.Status.RemoteControlStartAllowed')}")

        # Pick a program to drive (prefer one the appliance reports available).
        try:
            programs = controller.all_programs(dishwasher)
        except HomeConnectError as exc:
            programs = []
            print(f"  (could not read programs: {exc})")
        usable = [p for p in programs if (p.get("constraints") or {}).get("available", True)]
        program_key = (usable or programs or [{"key": "BSH.Common.Program.Favorite.001"}])[0]["key"]
        print(f"Using program: {hc.enum_tail(program_key)}\n")

        # Optional auto-power-on exercise: power the simulator dishwasher OFF, then
        # let Start Program's power_on_first path bring it back before starting.
        power_on_first = False
        if args.power_cycle:
            power_on_first = _power_cycle_off(controller, dishwasher)

        print("Control sequence:")
        try_step("select program",
                 lambda: controller.select_program(dishwasher, program_key,
                                                   power_on_first=power_on_first))
        started = try_step("start program",
                           lambda: controller.start_program(dishwasher, program_key,
                                                            power_on_first=power_on_first))

        if started:
            reached = wait_for(lambda: op_tail(dishwasher) in ("Run", "DelayedStart"), STATE_TIMEOUT)
            report("verify OperationState -> Run", bool(reached),
                   f"now {op_tail(dishwasher) or 'unknown'}")
            if try_step("pause program", lambda: controller.pause_program(dishwasher)):
                paused = wait_for(lambda: op_tail(dishwasher) == "Pause", 10)
                report("verify OperationState -> Pause", bool(paused),
                       f"now {op_tail(dishwasher) or 'unknown'}")
            if try_step("resume program", lambda: controller.resume_program(dishwasher)):
                resumed = wait_for(lambda: op_tail(dishwasher) == "Run", 10)
                report("verify OperationState -> Run", bool(resumed),
                       f"now {op_tail(dishwasher) or 'unknown'}")

        if try_step("stop program", lambda: controller.stop_program(dishwasher)):
            stopped = wait_for(lambda: op_tail(dishwasher) in ("Ready", "Finished", "Aborting"), 10)
            report("verify OperationState -> stopped", bool(stopped),
                   f"now {op_tail(dishwasher) or 'unknown'}")
        print(f"\nFinal operation state: {op_tail(dishwasher) or 'unknown'}")
    finally:
        coordinator.stop()
    print("Done.")


if __name__ == "__main__":
    main()
