# CLAUDE.md — Home Connect

> **Part of the [Indigo workspace](../CLAUDE.md)** — see root for cross-project map, standards, and tooling.

## Project Identity

- **Name**: Home Connect
- **Type**: Indigo plugin
- **Shortcut**: `home connect`
- **GitHub**: https://github.com/simons-plugins/indigo-home-connect
- **Language**: Python 3.10+ (stdlib only — no bundled deps)

## Role in the workspace

Bosch/Siemens Home Connect appliances (dishwasher, dryer, washer, oven, coffee
maker, fridge-freezer + generic fallback) as Indigo devices, via the official
cloud API: OAuth2 Device Flow auth, one global SSE event stream for state,
minimal REST for control.

**Read the PRD first**: [`../docs/plans/PRD-indigo-home-connect.md`](../docs/plans/PRD-indigo-home-connect.md)
— it encodes the API's hard rate limits (1000 req/day!), auth model, and the
design lessons adopted from homebridge-homeconnect. Do not add polling loops or
extra REST calls without checking the request-budget rationale there.

Local dev secrets live in `.env` (gitignored, never commit): `clientid`,
`clientsecret` for the Home Connect developer application. Never log or commit
tokens; redact haIds and tokens in log output (first-4/last-8).

## Related projects

Standalone — no sibling dependencies in this workspace.

## Standards

Inherits workspace standards from [root CLAUDE.md](../CLAUDE.md#common-standards-apply-to-every-project-unless-its-claudemd-overrides). Key points:

- **Version bump per PR**: `Info.plist` `PluginVersion` (`YYYY.R.P`); `CFBundleVersion` stays `1.0.0`.
- **Testing**: pytest (netro-pattern fake `indigo` in `tests/conftest.py`); CI runs pytest on every PR.
- **Merge**: GitHub PR only, never `--admin`, never squash, wait for CI green, wait for Simon's go-ahead.
- **Before writing Indigo plugin code**: invoke `/indigo:dev`.
