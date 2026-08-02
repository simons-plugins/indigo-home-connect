"""BSH key -> Indigo state mapping tables and value helpers (PRD §4).

Pure stdlib, never imports ``indigo``. Two responsibilities:

* the mapping from Home Connect's namespaced enum keys (e.g.
  ``BSH.Common.Status.OperationState``) to the flat Indigo state IDs declared in
  ``Devices.xml`` (common block + per-type additions), and the appliance-type ->
  ``deviceTypeId`` routing used by discovery and the device ConfigUI;
* the small value transforms every consumer needs — enum-tail extraction,
  ``EventPresentState`` -> bool, remaining-time formatting, and the state-ID
  sanitiser for undocumented keys that degrade to dynamic string states.

Only keys with a *known* BSH name live in the maps; everything else is tolerated
as a dynamic state, never a crash (PRD §6 "undocumented enum churn").
"""

# -- Value kinds a mapped state can carry -------------------------------------
# "enum"  -> tail of a namespaced enum value ("...OperationState.Run" -> "Run")
# "int"   -> integer (progress %, seconds)
# "num"   -> number passthrough (temperatures / setpoints)
# "bool"  -> JSON boolean (or "true"/"false" string)
# "event" -> EventPresentState value -> bool (Present/Confirmed True, Off False)

# Synthetic reachability key set by the state engine (hc_appliance.CONNECTED).
CONNECTED_KEY = "connected"

# -- Common map: BSH key -> (state_id, kind) ----------------------------------
COMMON_MAP = {
    CONNECTED_KEY: ("connected", "bool"),
    "BSH.Common.Status.OperationState": ("operationState", "enum"),
    "BSH.Common.Setting.PowerState": ("powerState", "enum"),
    "BSH.Common.Status.DoorState": ("doorState", "enum"),
    "BSH.Common.Root.ActiveProgram": ("programActive", "enum"),
    "BSH.Common.Root.SelectedProgram": ("programSelected", "enum"),
    "BSH.Common.Option.ProgramProgress": ("programProgress", "int"),
    "BSH.Common.Option.RemainingProgramTime": ("remainingTime", "int"),
    "BSH.Common.Status.RemoteControlActive": ("remoteControlActive", "bool"),
    "BSH.Common.Status.RemoteControlStartAllowed": ("remoteStartAllowed", "bool"),
    "BSH.Common.Status.LocalControlActive": ("localControlActive", "bool"),
}

# Common state IDs that exist in every type's XML but are not written by a direct
# BSH-key mapping (derived / bridge-managed).
DERIVED_STATE_IDS = ("remainingTimeFormatted", "lastEvent", "lastEventTime", "status")

# -- Per-type additions: deviceTypeId -> {BSH key: (state_id, kind)} -----------
TYPE_MAP = {
    "dishwasher": {
        "Dishcare.Dishwasher.Event.SaltNearlyEmpty": ("saltNearlyEmpty", "event"),
        "Dishcare.Dishwasher.Event.RinseAidNearlyEmpty": ("rinseAidNearlyEmpty", "event"),
    },
    "dryer": {
        "LaundryCare.Dryer.Option.DryingTarget": ("dryingTarget", "enum"),
    },
    "washer": {
        "LaundryCare.Washer.Option.SpinSpeed": ("spinSpeed", "enum"),
        "LaundryCare.Washer.Option.Temperature": ("temperature", "enum"),
    },
    "oven": {
        "Cooking.Oven.Option.SetpointTemperature": ("setpointTemperature", "num"),
        "Cooking.Oven.Status.CurrentCavityTemperature": ("currentCavityTemperature", "num"),
        "Cooking.Oven.Event.PreheatFinished": ("preheatFinished", "event"),
    },
    "coffeeMaker": {
        "ConsumerProducts.CoffeeMaker.Event.BeanContainerEmpty": ("beanContainerEmpty", "event"),
        "ConsumerProducts.CoffeeMaker.Event.WaterTankEmpty": ("waterTankEmpty", "event"),
        "ConsumerProducts.CoffeeMaker.Event.DripTrayFull": ("dripTrayFull", "event"),
    },
    "fridgeFreezer": {
        "Refrigeration.FridgeFreezer.Setting.SetpointTemperatureRefrigerator":
            ("setpointTemperatureRefrigerator", "num"),
        "Refrigeration.FridgeFreezer.Setting.SetpointTemperatureFreezer":
            ("setpointTemperatureFreezer", "num"),
        "Refrigeration.FridgeFreezer.Event.DoorAlarmFreezer": ("doorAlarmFreezer", "event"),
        "Refrigeration.FridgeFreezer.Event.DoorAlarmRefrigerator": ("doorAlarmRefrigerator", "event"),
        "Refrigeration.FridgeFreezer.Setting.SuperModeRefrigerator": ("superModeRefrigerator", "bool"),
        "Refrigeration.FridgeFreezer.Setting.SuperModeFreezer": ("superModeFreezer", "bool"),
    },
    "homeConnectAppliance": {},
}

# All device types this plugin defines (generic fallback last).
DEVICE_TYPE_IDS = ("dishwasher", "dryer", "washer", "oven", "coffeeMaker",
                   "fridgeFreezer", "homeConnectAppliance")

GENERIC_TYPE_ID = "homeConnectAppliance"

# -- Appliance type (BSH ``type``) <-> deviceTypeId ---------------------------
APPLIANCE_TYPE_TO_DEVICE_TYPE = {
    "Dishwasher": "dishwasher",
    "Dryer": "dryer",
    "Washer": "washer",
    "Oven": "oven",
    "CoffeeMaker": "coffeeMaker",
    "FridgeFreezer": "fridgeFreezer",
}

# deviceTypeId -> the BSH appliance types it accepts (None = accept all, generic).
DEVICE_TYPE_TO_APPLIANCE_TYPES = {
    device_type: (appliance_type,)
    for appliance_type, device_type in APPLIANCE_TYPE_TO_DEVICE_TYPE.items()
}
DEVICE_TYPE_TO_APPLIANCE_TYPES[GENERIC_TYPE_ID] = None

# Settings-only appliances have no programs (PRD §2). Default True for the rest,
# including the generic type — absent-program errors are swallowed in hc_events.
_NO_PROGRAM_TYPES = frozenset({"fridgeFreezer"})


def device_type_for(appliance_type):
    """Map a BSH appliance ``type`` to the deviceTypeId (generic fallback)."""
    return APPLIANCE_TYPE_TO_DEVICE_TYPE.get(appliance_type, GENERIC_TYPE_ID)


def supports_programs(device_type_id):
    """True unless the type is settings-only (fridge/freezer family)."""
    return device_type_id not in _NO_PROGRAM_TYPES


def spec_for(device_type_id, bsh_key):
    """Return ``(state_id, kind)`` for a BSH key on a device type, or ``None``.

    The per-type additions win over the common map on the (rare) chance a key is
    listed in both; in practice the two are disjoint.
    """
    type_map = TYPE_MAP.get(device_type_id, {})
    if bsh_key in type_map:
        return type_map[bsh_key]
    return COMMON_MAP.get(bsh_key)


def common_state_ids():
    """The 15 common state IDs present in every type's Devices.xml block."""
    ids = [state_id for state_id, _ in COMMON_MAP.values()]
    ids.extend(DERIVED_STATE_IDS)
    return ids


def state_ids_for(device_type_id):
    """Every declared state ID for a type (common + type-specific)."""
    ids = set(common_state_ids())
    ids.update(state_id for state_id, _ in TYPE_MAP.get(device_type_id, {}).values())
    return ids


# -- Value transforms ---------------------------------------------------------
def enum_tail(value):
    """``BSH.Common.EnumType.OperationState.Run`` -> ``Run``.

    Non-namespaced or non-string values pass through unchanged, so a bare
    ``"Closed"`` or a number is returned as-is.
    """
    if isinstance(value, str) and "." in value:
        return value.rsplit(".", 1)[-1]
    return value


# EventPresentState tails that mean "active" (PRD §4 EventPresentState handling).
_EVENT_PRESENT = frozenset({"Present", "Confirmed"})


def event_present_bool(value):
    """``EventPresentState`` value -> bool. Present/Confirmed True, else False."""
    if isinstance(value, bool):
        return value
    return enum_tail(value) in _EVENT_PRESENT


def to_bool(value):
    """Coerce a JSON boolean or ``"true"``/``"false"``/``"on"``/``"off"`` string."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "on", "yes", "1")
    return bool(value)


def to_int(value, default=0):
    """Best-effort integer coercion (used for progress % and seconds)."""
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def clamp_percent(value):
    """Clamp a progress value into 0..100."""
    return max(0, min(100, to_int(value, 0)))


def format_remaining(seconds):
    """Whole seconds -> ``"H:MM"`` (e.g. 5040 -> ``"1:24"``, 1500 -> ``"0:25"``).

    ``None`` yields ``None``; negatives clamp to ``"0:00"``. Rounds to the nearest
    minute so a 84-minute remaining reads ``1:24``, matching the PRD status
    example.
    """
    if seconds is None:
        return None
    total_minutes = max(0, int(round(to_int(seconds, 0) / 60.0)))
    return f"{total_minutes // 60}:{total_minutes % 60:02d}"


def prettify_program(name):
    """Insert a space at letter/digit boundaries: ``Eco50`` -> ``Eco 50``.

    Conservative — only the last-segment tail is prettified for the status
    summary; the raw tail is what's stored in the ``programActive`` state.
    """
    if not name:
        return name
    out = []
    prev = ""
    for char in str(name):
        if prev and prev.isalpha() and char.isdigit():
            out.append(" ")
        elif prev and prev.isdigit() and char.isalpha():
            out.append(" ")
        out.append(char)
        prev = char
    return "".join(out)


def sanitise_state_key(key):
    """MQTT/BSH-style key -> a valid Indigo state ID (camelCase ASCII alnum).

    Indigo forbids underscores, dots and non-ASCII in state IDs (field notes:
    "State ID naming rules"). Undocumented keys route through here to become
    dynamic string states rather than crashing the plugin.
    """
    parts = []
    current = []
    for char in str(key):
        if char.isascii() and char.isalnum():
            current.append(char)
        elif current:
            parts.append("".join(current))
            current = []
    if current:
        parts.append("".join(current))
    if not parts:
        return ""
    head = parts[0]
    sid = head[0].lower() + head[1:] + "".join(p[:1].upper() + p[1:] for p in parts[1:])
    if not sid[0].isalpha():
        sid = "z" + sid[0].upper() + sid[1:]
    return sid
