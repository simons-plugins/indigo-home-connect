# Changelog

All notable changes to the Home Connect plugin. Format loosely follows
[Keep a Changelog](https://keepachangelog.com); versions are `YYYY.R.P` (Indigo
convention). Versions `2026.0.x` were the phased internal build-up; **`2026.1.0`** is the
first release of the complete, user-visible feature set.

## [2026.1.4] — 2026-08-03

Red-team hardening, wave 2 — the remaining audit findings (#15–#21, plus the
cheap items from the #22 grab-bag).

### Fixed
- **Removed appliances no longer leave zombie devices (#15).** DEPAIRED — and
  an appliance vanishing from a *successful* discovery pass (haId churn) — now
  detaches the bridge and flags the device ("Not found on account", error
  state) instead of leaving it attached to a dead object showing "Off"
  forever. Re-pairing under the same haId re-attaches and clears the flag
  automatically; a new haId needs the appliance re-selected in the device
  settings (the log line says so).
- **Persistently failing reads park instead of grinding (#16).** After 8
  consecutive failed read attempts (~10 min of backed-off retries) the queue
  parks with one warning, instead of retrying every 10 minutes forever
  (~144 requests/day per stuck appliance). A CONNECTED transition, PAIRED, or
  a 6-hourly reprobe (~2-4 requests/day, so a transient cloud-side outage
  self-heals) unparks it. The cloud's connection-initialization 409 is now
  also treated as "not ready yet", not a failure.
- **Dynamic-state registration can no longer drop real states (#17).** A newly
  discovered key's value now goes in its own second batch: if registration
  fails (or Indigo hasn't rebuilt the state list yet), operationState/status
  and friends still land, and the new key retries next push.
- **Every accepted write is now watched, not just Start Program (#18).** Set
  Power State, Set Setting and Select Program get a 60 s ValueWatch: Home
  Connect can 2xx-accept a PUT and drop it appliance-side with no error — the
  watch warns with the wanted-vs-reported values if the change never arrives
  over SSE.
- **Unknown state no longer refuses controls (#19).** Right after a plugin
  restart (before the first re-read lands) Remote Control / Remote Start /
  OperationState are simply unknown; the guard rails refused with a false
  "enable Remote Control on the appliance". Unknown now passes through — the
  appliance stays authoritative and a genuine refusal comes back as a 409 with
  the actionable hint. Only a value the appliance actually *reported* can
  refuse locally.
- **SSE events are routed on the worker thread (#20).** Dispatch previously ran
  observer callbacks — ending in Indigo state writes, an IPC round-trip — on
  the reader thread; a slow Indigo server could stall reads past the 120 s
  dead-stream timeout and trigger a spurious reconnect + re-read burst. The
  reader now only parses and enqueues; the single worker preserves ordering.
  Worker-side HTTP (re-reads, discovery) *defers* when the rate-limit gate is
  closed instead of blocking, so live event routing never freezes behind a
  Retry-After, and a queue depth of 500+ logs a falling-behind warning.
- **Zombie streams are detected and renewed (#21).** The BSH-confirmed backend
  failure where keep-alives continue but events stop is invisible to the 120 s
  watchdog. When the stream has carried only keep-alives for 30 minutes *while
  a program is running* (a running appliance emits progress every few
  minutes), the stream is renewed — one counted request. The renewal takes the
  no-error STOP path, so devices do not flap Off / fire triggers during the
  ~1 s reconnect, and the post-renewal re-read normally falls outside the
  freshness window (when it doesn't, the cache was just refreshed by the read
  that made it fresh).
- **From the #22 grab-bag:** a bare SSE field name with no colon (valid per
  spec; accepted for the four real field names — any other bare token is
  treated as this API's known stream corruption) no longer restarts the
  stream; EVENT items without a timestamp always fire (the `(key, None)`
  dedupe collision silently swallowed repeats); a token-file entry with a
  missing or non-numeric `expires_at` refreshes instead of error-looping; the
  config dialog's auth-status read logs failures at debug instead of
  swallowing them. Changing the Client ID while devices are live now marks
  them "Authorization required" instead of freezing them healthy-looking
  until the new client is authorized.

## [2026.1.3] — 2026-08-03

Red-team hardening, wave 1 — fixes for the highest-severity findings of the
2026-08-03 audit (issues #8–#14; the full findings list is issues #8–#22).

### Fixed
- **Rate-limit gate is now interruptible and never blocks Indigo threads (#8).**
  A real 429 block can carry a Retry-After of minutes to 24 h. The gate wait now
  runs in 1 s slices against an abort event (plugin shutdown / client rebuild
  unblocks it immediately); control actions and dynamic menus **refuse locally**
  ("rate-limited — try again in N min") instead of sleeping; capability loads
  fall back to the stale 24 h cache when gated; and the disk cache no longer
  holds its lock across a loader's HTTP round-trip.
- **Closing the config dialog no longer kills an in-flight authorization (#9).**
  Saving with unchanged credentials is now a no-op supersession (the pending
  device-flow worker, stream and rate-limiter windows are kept); Cancel leaves
  the polling worker running, as the dialog text has always promised.
- **Losing authorization now shows on the devices (#10).** When the refresh
  token dies (revoked / 60-day idle expiry) every bridged device flips to
  ``connected=false`` / "Authorization required" with a device error state,
  instead of freezing indefinitely at its last healthy-looking state.
- **Quota-burn loops now back off (#11, #13, #14).** Stream reconnects escalate
  from the 60 s cap to 15 min after 5 consecutive failures (a failed open OR a
  stream that dies within 120 s); a 429 **without** Retry-After now applies a
  synthetic 60 s gate; transport/5xx retries wait 2 s/4 s/8 s between attempts
  (the 10-successive-errors block counts back-to-back retries); and a failing
  token refresh backs off exponentially (60 s → 1 h cap) instead of hitting the
  100/day token endpoint every supervisor tick.
- **Reconnect re-reads are suppressed within a 5-minute freshness window (#12).**
  A stream blip or a CONNECTED flap just after a completed full read no longer
  costs another 3–5 GET pass (self-powering-off dishwashers flap by design);
  PAIRED and first discovery still force a read. Abandoned passes never arm the
  window.

Review round (CodeRabbit + 4-lens agent review) — further fixes on the same
findings:
- A 429 **received mid-request** with a long Retry-After now raises to the
  caller instead of re-entering the gate wait — the pre-flight check passes
  moments before the 429 lands, and retrying would block the calling (possibly
  Indigo UI/action) thread for the whole block. Short blocks (≤5 s) are still
  waited out and retried.
- Auth-required device marking is now **level-triggered as well as
  edge-triggered**: a device created or restarted while authorization is dead
  shows "Authorization required" (not "Waiting for appliance…"), and a device
  flow that ends denied/expired/failed marks devices from the worker (the
  coordinator-stop edge never fires in that path).
- Pressing **Authorize** now stops a coordinator wired to the superseded
  client, instead of leaving its reconnect loop error-spinning against the
  aborted api (false "failing repeatedly" warnings) until the next tick.
- Token refresh **defers while the request gate is closed** — a token POST
  would otherwise block the supervisor thread (stalling reconcile) for the
  whole Retry-After.
- Requests abandoned by shutdown/supersession are tagged and logged as
  teardown (debug), never as "read failed … retrying in Ns" warnings that
  would be false twice over.
- Gate-wait log lines humanize long delays ("24.0h", not "86400.0s"); the 429
  action hint uses the server's actual Retry-After when present.

## [2026.1.2] — 2026-08-03

### Added
- **Auto power-on for Start / Select Program.** Many dishwashers power themselves off after a
  cycle and on door events, which left a scheduled *Start Program* refused as *"not ready"*. A
  new **Power on first if needed** checkbox (on both actions, on by default) powers the
  appliance on and waits — **event-driven, no polling** — for `OperationState = Ready` (25 s
  timeout) before running the guard rails and starting. The power-on `PowerState = On` PUT is a
  normal idempotent write (not a program start; the start limiter still counts once).
  - Refuses with an actionable message when power is **not remotely writable** (read-only power,
    e.g. some dryers — *"cannot be powered on remotely — press its power button"*) or the
    appliance **never becomes Ready**, and in both cases does **not** attempt the start.
  - Power-on cannot grant Remote Start (still the appliance's 24-hour rule); already-on
    appliances take the zero-extra-request happy path. `Standby` is treated as on and is
    **not** woken by this option (only `Off`/`Inactive`).

### Changed
- **Start-watch window 15 s → 60 s**, and its message now reflects uncertainty rather than
  failure (*"has not reported starting after 60s — it may still begin…"*). A real appliance
  runs pre-start water/door checks and reports state over the cloud SSE stream, so the 15 s
  window produced a false-negative "did not start" warning on a start that was actually
  succeeding (seen on Simon's first real hardware Start).

## [2026.1.0] — 2026-08-03

Phase 5 — final polish and full user documentation. First minor release: the plugin is now
feature-complete and field-validated on real hardware (Bosch dryer, Siemens dishwasher).

### Added
- **Full user documentation.** Rewritten `README.md`: supported appliances, per-type state
  tables, actions, and step-by-step sections for developer-portal registration, the
  authorize flow, remote start, rate limits, and troubleshooting.
- **`docs/APPLIANCES.md`** — per-appliance-type capability notes and known quirks, with
  field-validated vs simulator-validated status per type.
- Startup now logs a one-line **version + configured-device-count** summary at info.

### Changed
- Control-action API failures are logged with an **actionable next step** — a `409` names
  the door/remote-control/local-use checks to make, `429` says to wait and retry, `403`
  says to re-authorize; authorization errors point at the Client ID and Device Flow setting.
- A deliberate shutdown of the SSE stream now logs at **debug**, not warning (closing the
  socket under the blocked reader is expected, not a fault); genuine drops still warn and
  note that reconnect is automatic.

## [2026.0.5] — Phase 4: control actions

- `Actions.xml` + `hc_control.py`: Start / Select / Stop / Pause / Resume Program, Send
  Command (Open Door / Partly Open Door), Set Power State, Set Setting (advanced).
- **"Refuse locally before any HTTP"** guard rails: every call is pre-flight-checked against
  the cached appliance state, so a request Home Connect would reject never leaves the plugin
  (dodging the 10-consecutive-errors block).
- Local rate limiters (5 starts + 5 stops per rolling 60 s); program-start PUT marked
  `no_retry` so a lost response can never double-start; post-start operation-state watch that
  warns if a start never leaves `Ready` (door/water/tank).
- Dynamic program/command/power menus served from the 24 h capability cache; localized
  program names; full-`/programs` menu source (works around appliances that report only the
  dial's program as "available").

## [2026.0.4] — Phase 3: Indigo device layer

- Seven device types — Dishwasher, Dryer, Washer, Oven, Coffee Maker, Fridge Freezer, and a
  generic Home Connect Appliance fallback — with a shared 15-state common block plus
  type-specific states.
- `device_bridge.py`: batched `updateStatesOnServer`, a computed `status` summary
  (`Run · Eco 50 · 1:24 remaining`), dynamic string states for undocumented keys, last-event
  tracking, and the per-device off-vs-disconnected policy.
- Discovery-driven appliance picker (type-filtered, "(in use)" marked), late-attach on
  PAIRED, and a **Log Discovered Appliances** menu item.

## [2026.0.3] — Phase 2: SSE event stream + state engine

- One global Server-Sent-Events stream (`hc_events.py`): tolerant parser, infinite reconnect
  with a 120 s dead-stream timeout, keep-alive filtering, and synthetic START/STOP.
- Per-appliance state engine (`hc_appliance.py`): flat key→value cache, observer API, EVENT
  de-duplication, and a sequential reconnect re-read queue that is abandoned the moment an
  appliance disconnects (never burns budget on offline appliances).
- Settings-only appliances (fridge/freezer) skip program reads; a 401 on the stream forces
  one token refresh + single retry (no reconnect storm on a dead token).

## [2026.0.2] — Phase 1: OAuth + rate-limit-aware client

- `hc_api.py`: stdlib `http.client` transport with a plugin-wide request gate, idempotent-only
  retries (never 400/403/404/405/406/409/415), `Retry-After` handling, a daily 1000-request
  budget counter (warning at 800), and token/haId redaction.
- `hc_auth.py`: OAuth **Device Flow** (production) and Authorization Code Grant (simulator);
  per-client token store (JSON, `0600`, atomic merge-on-write); proactive refresh with
  `invalid_grant` → authorization-required detection.
- `hc_cache.py`: 24 h TTL disk cache with stale-value fallback, invalidated on version change.
- `PluginConfig.xml` authorize flow: Client ID / secret / simulator toggle, verification URL
  and user code shown in the dialog and Event Log.

## [2026.0.1] — Phase 0: scaffold

- Plugin bundle layout, `Info.plist`, CI (version-check, pytest, release), `pyproject.toml`
  lint config (netro pattern), `CLAUDE.md`, and the README skeleton.
</content>
