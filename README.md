# Home Connect for Indigo

Indigo plugin for **Bosch / Siemens / Neff / Gaggenau** appliances via the official
[Home Connect cloud API](https://api-docs.home-connect.com): live appliance state
(operation state, program, progress, remaining time, door, events) over the
Server-Sent Events stream, and control (start/stop/pause programs, settings,
commands) over REST.

> **Status: pre-release, under active development.** See
> [`docs/`](./docs/) and the workspace PRD for the build plan.

## Requirements

- Indigo 2025.2+ (Python 3.10+)
- Appliances paired to a Home Connect account via the Home Connect phone app
- A free [Home Connect developer](https://developer.home-connect.com) account
  with a registered application (OAuth flow: **Device Flow**) — full walkthrough
  to follow in the setup docs

## Design notes

- Cloud-only API (no local path exists) with strict rate limits
  (1000 requests/day per client+account) — the plugin uses a single SSE event
  channel plus minimal REST, a global request gate honouring `Retry-After`, and
  a 24 h capability cache.
- Remote start requires *Remote Control Start Allowed* on the appliance, which
  **auto-expires roughly every 24 h** by design (a Home Connect platform rule,
  not a plugin limitation). The plugin exposes it as a device state so you can
  trigger a notification when it lapses.
- No bundled dependencies — Python stdlib only.

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
