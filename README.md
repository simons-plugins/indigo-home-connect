# Home Connect for Indigo

An [Indigo](https://www.indigodomo.com) plugin for **Bosch / Siemens / Neff / Gaggenau**
home appliances, via the official [Home Connect cloud API](https://api-docs.home-connect.com).

Your dishwasher, dryer, washer, oven, coffee maker and fridge-freezer become Indigo
devices with live state — operation state, selected/active program, progress, remaining
time, door, and appliance events — streamed in real time, plus control (start / stop /
pause / resume programs, power, settings, and commands such as *Open Door*).

State arrives over the Home Connect **Server-Sent Events** stream and control goes out
over REST, so appliances participate in Indigo triggers, schedules, control pages, and
Domio like any other device.

## Contents

- [Requirements](#requirements)
- [Supported appliances](#supported-appliances)
- [Setup](#setup)
  - [1. Register a Home Connect application](#1-register-a-home-connect-application)
  - [2. Authorize the plugin](#2-authorize-the-plugin)
  - [3. Create devices](#3-create-devices)
- [Device states](#device-states)
- [Actions](#actions)
- [Remote start](#remote-start-the-24-hour-rule)
- [Rate limits](#rate-limits)
- [Triggers](#triggers)
- [Troubleshooting](#troubleshooting)
- [Design notes](#design-notes)
- [Acknowledgments](#acknowledgments)
- [License](#license)

## Requirements

- **Indigo 2025.2+** (Python 3.10+).
- Appliances already **paired to a Home Connect account** in the Home Connect phone app.
- A free [Home Connect developer](https://developer.home-connect.com) account with a
  registered application set to **OAuth Device Flow** (walkthrough below).
- No bundled dependencies — Python **standard library only**.

## Supported appliances

Six appliance types get a dedicated Indigo device with type-specific states, and a
generic fallback covers everything else Home Connect exposes:

| Device type | Home Connect appliance | Programs |
|---|---|---|
| **Dishwasher** | Dishwasher | Yes |
| **Dryer** | Dryer | Yes |
| **Washer** | Washer | Yes |
| **Oven** | Oven | Yes |
| **Coffee Maker** | CoffeeMaker | Yes |
| **Fridge Freezer** | FridgeFreezer | No (settings/status/events only) |
| **Home Connect Appliance** (generic) | anything else — hob, hood, cleaning robot, washer-dryer, wine cooler, microwave, warming drawer, air conditioner… | If the appliance has them |

The generic type carries the full common state set and picks up any appliance-specific
keys as [dynamic states](#dynamic-states). Add a dedicated type on request if you have
hardware to validate it against.

> **Field-validated:** Dishwasher (Siemens) and Dryer (Bosch) are exercised end-to-end on
> real hardware. The other four are validated against the Home Connect simulator. See
> [`docs/APPLIANCES.md`](docs/APPLIANCES.md) for per-type capability notes and quirks.

## Setup

There are three steps: register an application on the Home Connect developer portal,
authorize the plugin against it, then create Indigo devices.

### 1. Register a Home Connect application

This is a one-time step on the [Home Connect developer portal](https://developer.home-connect.com).
A few details **must** be exactly right or authorization will silently fail.

1. **Create the developer account with the same email as your SingleKey ID, all
   lowercase.** The developer account email must match the SingleKey ID email you use in
   the Home Connect phone app, and it must be lowercase — a capital letter in the address
   is a common cause of "no appliances found" later.
2. **Register a new application** and set:
   - **OAuth Flow → Device Flow.** This is **immutable** — it cannot be changed after the
     application is created. If you pick the wrong flow you must **delete the application
     and create a new one**. The plugin only works with Device Flow.
   - **Redirect URI → leave blank.** Device Flow does not use one.
   - **Home Connect User Account for Testing →** your own account (the one your appliances
     are paired to).
3. **Wait for propagation.** A newly-created application takes roughly **15 minutes to an
   hour** to become usable. If you authorize immediately after creating it you will get an
   error — this is normal; wait and try again.
4. **Use a dedicated Client ID for this plugin.** Rate limits are enforced *per client +
   account* (see [Rate limits](#rate-limits)). Do **not** reuse the Client ID of another
   integration (for example a running Homebridge instance) — they would share, and starve,
   the same 1000-requests-per-day budget.

Copy the **Client ID** from the application's page. If your application defines a Client
Secret, copy that too (most Device Flow applications do not need one).

### 2. Authorize the plugin

1. Open the plugin's **Configure…** dialog (Plugins → Home Connect → Configure…).
2. Paste the **Client ID** (and Client Secret if your application has one).
3. Click **Authorize**. The dialog shows a **verification URL and a user code**, and the
   same details are written to the **Event Log**.
4. Open that URL in any browser, sign in with your SingleKey ID, and enter the code to
   grant access. The plugin polls in the background and completes automatically.
5. **The dialog's status line does not live-update.** After you approve in the browser,
   **close and re-open** the Configure… dialog to see the status change to *Authorized*.
   The authoritative confirmation is the **Event Log** line
   `Home Connect authorization successful`.

> **Simulator (no hardware):** tick **Use the Home Connect simulator** before pressing
> Authorize to run against the six preloaded simulator appliances. The simulator uses an
> auto-approved grant, so there is no browser step.

### 3. Create devices

1. **Devices → New…**, set **Type** to the matching model — *Dishwasher*, *Dryer*,
   *Washer*, *Oven*, *Coffee Maker*, *Fridge Freezer*, or the generic *Home Connect
   Appliance*.
2. Pick your appliance from the **Appliance** menu. The menu is populated from discovery,
   so **authorize first**; a specific device type lists only appliances of that kind, and
   an appliance already assigned to another device is marked *(in use)*.
3. Choose the **Treat "disconnected" as Off** policy (see
   [offline vs off](#offline-vs-off)) and save.

For a first-time install, double-click the `.indigoPlugin` bundle; see
[`docs/JARVIS-INSTALL.md`](docs/JARVIS-INSTALL.md) for install/update details.

## Device states

Every device type carries this **common state set**:

| State | Type | Meaning |
|---|---|---|
| `connected` | bool | Appliance reachable on the Home Connect cloud |
| `operationState` | string | `Ready`, `DelayedStart`, `Run`, `Pause`, `ActionRequired`, `Finished`, `Aborting`, … |
| `powerState` | string | `On`, `Off`, `Standby` (as the appliance reports/allows) |
| `doorState` | string | `Open`, `Closed`, `Locked` |
| `programActive` | string | Key tail of the running program (e.g. `Auto2`) |
| `programSelected` | string | Key tail of the staged/selected program |
| `programProgress` | integer | 0–100 % |
| `remainingTime` | integer | Seconds remaining |
| `remainingTimeFormatted` | string | `H:MM` (e.g. `1:24`) |
| `remoteControlActive` | bool | Remote control enabled on the appliance |
| `remoteStartAllowed` | bool | Remote **start** currently permitted (see [the 24 h rule](#remote-start-the-24-hour-rule)) |
| `localControlActive` | bool | Someone is operating the appliance at the panel |
| `lastEvent` | string | Most recent Home Connect event |
| `lastEventTime` | string | Timestamp of `lastEvent` |
| `status` | string | UI summary shown on the device (e.g. `Run · Eco 50 · 1:24 remaining`) |

**Type-specific additions:**

| Type | Extra states |
|---|---|
| Dishwasher | `saltNearlyEmpty`, `rinseAidNearlyEmpty` |
| Dryer | `dryingTarget` |
| Washer | `spinSpeed`, `temperature` |
| Oven | `setpointTemperature`, `currentCavityTemperature`, `preheatFinished` |
| Coffee Maker | `beanContainerEmpty`, `waterTankEmpty`, `dripTrayFull` |
| Fridge Freezer | `setpointTemperatureRefrigerator`, `setpointTemperatureFreezer`, `doorAlarmRefrigerator`, `doorAlarmFreezer`, `superModeRefrigerator`, `superModeFreezer` |
| Home Connect Appliance | common set only |

### Dynamic states

Home Connect adds and renames enum keys without notice. Any key the plugin does not map
explicitly surfaces automatically as an extra **string state** on the device (via
`getDeviceStateList`). Unknown keys therefore appear as raw states rather than crashing
the plugin — if new states show up after an appliance update, that is expected.

## Actions

All actions are device-scoped (pick the Home Connect device in the action's dialog). Every
control call is **pre-flight-checked locally** before any request is sent, so a call the
appliance would reject is refused with an actionable message instead of burning your
request budget.

| Action | What it does | Preconditions (checked locally) |
|---|---|---|
| **Start Program** | Starts a program now | Connected · Remote Control on · Remote Start allowed · not locally controlled · `operationState = Ready` |
| **Select Program** | Stages a program without starting | Connected · powered on |
| **Stop Program** | Aborts the active program | Connected · a program is active or paused |
| **Pause Program** | Pauses (if supported) | Connected · appliance exposes the Pause command |
| **Resume Program** | Resumes a paused program | Connected · appliance exposes the Resume command |
| **Send Command** | Sends a supported command (e.g. *Open Door*, *Partly Open Door*) | Connected · command is in the appliance's command list |
| **Set Power State** | On / Off / Standby | Connected · value is in the appliance's allowed power states (when known) |
| **Set Setting (advanced)** | Writes any BSH setting key (e.g. `BSH.Common.Setting.ChildLock`) | Connected · a setting key is given (the appliance validates the key/value) |

**Start / Select** dialogs offer a **Program** menu (built from the appliance's full
program list) and an optional **Option overrides** field — one `key=value` per line, e.g.
`BSH.Common.Option.StartInRelative=3600`. Values coerce to `true`/`false` (bool), whole
numbers (int), otherwise text. Leave it blank to start with the program's defaults.

Menus for programs, commands and power states are served from a 24-hour capability cache,
so opening an action dialog costs at most one request the first time that day.

## Remote start: the 24-hour rule

Home Connect only lets the cloud **start** a program while
`BSH.Common.Status.RemoteControlStartAllowed` is true, and **you can only enable it at the
appliance itself** — there is no API to turn it on. Critically, the appliance
**auto-expires this permission roughly 24 hours after you enable it.** This is a Home
Connect **platform rule, not a plugin bug**.

Consequences:

- A *Start Program* action that worked yesterday can fail today with
  *"Remote Start is not allowed…"* simply because the permission lapsed. Re-enable it on
  the appliance (usually a long-press or a "Remote start" toggle on the machine) before
  running the program.
- **Select Program**, **Stop**, **Pause/Resume**, **Set Setting** and **Set Power** do
  **not** require Remote Start — only starting a fresh program does.

The plugin exposes the live permission as the **`remoteStartAllowed`** device state so you
can build a notification for when it lapses — for example a trigger on
`remoteStartAllowed` becoming *false* that sends you a reminder to re-arm it before your
next scheduled wash.

## Rate limits

The Home Connect API budget is **per Client ID + account** and is **strict**:

- **1000 requests per day**, 50/min, 10/s (burst 20).
- **5 program starts and 5 stops per minute.**
- **10 consecutive errors in 10 minutes → a 10-minute block;** repeated violations can
  block for 24 hours or lock the account.

**What the plugin does to stay well inside it:**

- Uses **one global SSE event stream** for all appliances instead of polling — state
  changes are pushed, not fetched.
- **Caches** program/command/power capabilities for 24 hours (with stale-fallback), so
  building menus and surviving restarts costs almost nothing.
- Runs a **plugin-wide request gate** that honours the server's `Retry-After` for *every*
  request, plus **local limiters** that refuse a 6th start/stop within a minute *before*
  any request goes out.
- **Refuses locally** any control call whose preconditions aren't met, so a rejected call
  never counts toward the 10-errors block.
- Logs a running daily counter (debug) and a single **warning at 800/1000** so a runaway
  loop is visible before Home Connect blocks the client.

**What you must NOT do:**

- **Do not build triggers/schedules that poll the appliance rapidly** (e.g. an Indigo
  schedule firing a Start/Select every few seconds). The SSE stream already keeps state
  current; manual polling only spends budget and can trip the error block.
- **Do not share one Client ID** across this plugin and another integration (Homebridge,
  Home Assistant, a second Indigo server). They share the same 1000/day budget and will
  starve each other. Register a **dedicated** Client ID per client.

## Triggers

Everything surfaces as device states, so native Indigo **state-change triggers** cover the
common cases without any custom event type:

- Program finished — trigger on `operationState` changing to `Finished`, or on `lastEvent`.
- Remote start lapsed — trigger on `remoteStartAllowed` becoming *false*.
- Consumables / alarms — `saltNearlyEmpty`, `rinseAidNearlyEmpty`, `waterTankEmpty`,
  `beanContainerEmpty`, `dripTrayFull`, `doorAlarmFreezer`, `doorAlarmRefrigerator`,
  `preheatFinished`.

Duplicate appliance events (some dishwashers re-send `ProgramFinished` on reconnect) are
de-duplicated before they reach state, so a trigger fires once per real event.

## Troubleshooting

**The program menu is empty.**
Program menus are built from the appliance. If the appliance is **off**, or has **just
finished a cycle with the door still closed**, Home Connect may report only the program on
the dial (or none). The plugin works around this by listing the appliance's **full**
program set and marking entries *(not currently available)* rather than the momentary
available-list — but the appliance must be **on** and reachable. Power the appliance on
(open the door after a finished cycle) and reopen the action dialog.

**A control action fails with a "409" / conflict.**
The appliance rejected the request. Common causes and fixes:
- **RemoteControlNotActive** — enable Remote Control on the appliance.
- **RemoteControlStartAllowed** lapsed — re-enable Remote Start on the appliance (see the
  [24-hour rule](#remote-start-the-24-hour-rule)).
- **LocalControlActive** — someone is using the appliance at the panel; wait until they
  finish.
- **Door open** — close the door before starting.
The plugin's error message names the concrete next step; follow it, then retry.

**"Authorization required" appears / control stops working.**
The refresh token was revoked or went **unused for more than 60 days** (e.g. the plugin
was disabled over a long holiday). Re-open **Configure…** and press **Authorize** again to
mint fresh tokens. The Event Log carries the "authorization lost" error and the
Configure… dialog's status line shows *Authorization required*; device state stops
updating until you re-authorize.

**No appliances in the picker.**
Confirm the appliances are paired in the Home Connect **phone app**, that authorization
succeeded (Event Log), and that your developer account email exactly matches your SingleKey
ID email in **lowercase**. A just-created application may still be [propagating](#1-register-a-home-connect-application).

### Offline vs off

Some appliances report **power-off as a disconnect**, making "off" and "offline"
indistinguishable over the API. Each device therefore has a **Treat "disconnected" as
Off** checkbox:

- **On** (default) — a disconnect shows the device as *Off*. Best for appliances that drop
  off the cloud when powered down.
- **Off** — a disconnect flags a device **error** state instead, so you can alert on an
  appliance that unexpectedly dropped off the network.

### Is it me or Home Connect?

Home Connect (BSH) cloud outages produce error floods and empty reads. The plugin throttles
its own error logging and falls back to cached capabilities during an outage, but if
nothing is updating, check the community-run **unofficial status page** before digging
further: <https://homeconnect.thouky.co.uk>.

## Design notes

- **Cloud-only.** There is no local Home Connect API; everything goes through BSH's cloud.
- **One SSE stream + minimal REST**, a global `Retry-After`-honouring request gate, 24-hour
  capability cache, and local pre-flight checks — all to live within the 1000/day budget.
- **Stdlib only** — no bundled dependencies, so nothing to `pip install` on the Indigo
  server. Tokens live in a `0600` JSON file under Indigo's Preferences (not in plugin
  prefs), and tokens/appliance IDs are redacted in log output.

## Acknowledgments

The architecture of this plugin owes a great deal to
[homebridge-homeconnect](https://github.com/thoukydides/homebridge-homeconnect)
by Alexander Thoukydides (ISC licensed) — years of documented, battle-tested
handling of the Home Connect API's rate limits, event stream quirks, and
appliance edge cases informed this design. No code was copied; the plugin is an
independent Python implementation.

The author of homebridge-homeconnect also runs an unofficial
[Home Connect API status page](https://homeconnect.thouky.co.uk).

## License

MIT — see [LICENSE](./LICENSE).
</content>
</invoke>
