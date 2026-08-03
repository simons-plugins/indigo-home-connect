# Per-appliance capability notes

A quick reference for what each device type surfaces, which actions apply, and the known
quirks per appliance family. Capabilities are detected **empirically** at runtime (the
plugin reads `PowerState.constraints.allowedvalues`, `GET /commands`, and the program list
from the appliance itself) rather than assumed from the type — so an individual model may
expose more or less than the notes below.

**Validation status:**

| Type | Status |
|---|---|
| Dishwasher | **Field-validated** — Siemens dishwasher on live hardware |
| Dryer | **Field-validated** — Bosch tumble dryer on live hardware |
| Washer | Simulator-validated |
| Oven | Simulator-validated |
| Coffee Maker | Simulator-validated |
| Fridge Freezer | Simulator-validated |
| Home Connect Appliance (generic) | Simulator-validated |

Common to **all** types: the 15 common states (see the README) and the state-scoped actions
that only need *connected* (Stop, Pause/Resume where supported, Send Command, Set Setting).
"Programs" and "Remote start" columns below note what is type-specific.

---

## Dishwasher — field-validated

- **Programs:** yes. Start / Select / Stop all apply; Start needs Remote Start.
- **Power:** typically `On` / `Off`; some models expose `Standby`. Detected from
  constraints — the Set Power State menu only lists what the appliance allows.
- **Extra states:** `saltNearlyEmpty`, `rinseAidNearlyEmpty` (from EVENT keys).
- **Known quirk — available-programs after a cycle:** just after a cycle finishes with the
  **door still closed**, the appliance's *available* program list can collapse to only the
  program on the dial (or none). The plugin builds menus from the **full** `/programs` list
  and marks entries *(not currently available)* to work around this — but if a menu still
  looks bare, open the door / power the appliance on and reopen the dialog.
- **Known quirk — duplicate `ProgramFinished`:** some dishwashers re-send `ProgramFinished`
  on reconnect. The plugin de-duplicates events by `(haId, key, timestamp)` so a trigger
  fires once per real finish.

## Dryer — field-validated

- **Programs:** yes. Start / Select / Stop apply; Start needs Remote Start.
- **Power:** **read-only `On`.** The Bosch dryer reports PowerState with only `On` in its
  allowed values, so the Set Power State menu offers no Off/Standby and the plugin refuses
  any such call locally — this is the appliance's behaviour, not a plugin limitation.
- **Extra states:** `dryingTarget` (the selected dryness level, from the program option).
- **Note:** the *available* program list on the dryer can report a single program (the one
  on the dial) while `/programs` returns the full set (observed: 1 vs 13 live) — another
  reason menus are built from the full list.

## Washer — simulator-validated

- **Programs:** yes. Start / Select / Stop apply; Start needs Remote Start.
- **Power:** typically `On` / `Off`; detected from constraints.
- **Extra states:** `spinSpeed`, `temperature` (from program options).

## Oven — simulator-validated

- **Programs:** yes. Start / Select / Stop apply; Start needs Remote Start.
- **Power:** commonly `On` / `Standby` (ovens usually sit in Standby rather than a hard
  Off); detected from constraints.
- **Commands:** may expose door commands; the Send Command menu lists only what the
  appliance actually supports.
- **Extra states:** `setpointTemperature`, `currentCavityTemperature`, `preheatFinished`.

## Coffee Maker — simulator-validated

- **Programs:** yes (beverages). Start / Select / Stop apply; Start needs Remote Start.
- **Power:** commonly `On` / `Standby`; detected from constraints.
- **Extra states:** `beanContainerEmpty`, `waterTankEmpty`, `dripTrayFull` (from EVENT
  keys) — useful for "refill" notifications.

## Fridge Freezer — simulator-validated

- **Programs:** **none.** The fridge/freezer family is settings/status/events only. The
  plugin marks the type `supports_programs = False`, so it never issues program reads
  (saving budget) and the program actions do not apply. Program-related states stay blank.
- **Power:** not meaningfully controllable — refrigeration runs continuously.
- **Extra states:** `setpointTemperatureRefrigerator`, `setpointTemperatureFreezer`,
  `doorAlarmRefrigerator`, `doorAlarmFreezer`, `superModeRefrigerator`, `superModeFreezer`.
- **Control:** use **Set Setting (advanced)** to change setpoints or toggle super/eco
  modes by their BSH setting keys.

## Home Connect Appliance (generic) — simulator-validated

- The fallback for any appliance without a dedicated type — hob, hood, cleaning robot,
  washer-dryer, wine cooler, microwave, warming drawer, air conditioner, etc.
- Carries the **common state set** only; any appliance-specific keys surface automatically
  as [dynamic string states](../README.md#dynamic-states).
- Programs, power values and commands are all detected empirically, so the applicable
  actions depend entirely on what the specific appliance exposes.

---

### How capability detection works (why the notes say "typically")

The plugin never hard-codes an appliance's abilities. On demand (and cached 24 h) it reads:

- `GET /programs` — the full program set for Start/Select menus.
- `GET /commands` — supported commands (Pause/Resume/OpenDoor/…). A `404` means "no
  commands", treated as the feature being absent, not an error.
- `GET /settings/BSH.Common.Setting.PowerState` → `constraints.allowedvalues` — the exact
  power values this unit permits (this is how the dryer's read-only `On` is discovered).

If a live read fails, the last cached value is used, so a brief cloud blip does not empty
your menus. See the PRD (`docs/plans/PRD-indigo-home-connect.md` §3.6) for the rationale.
</content>
