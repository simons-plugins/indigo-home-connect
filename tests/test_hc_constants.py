"""Unit tests for hc_constants.py — value transforms and mapping tables."""
import hc_constants as hc


# -- enum tail ----------------------------------------------------------------

def test_enum_tail_extracts_last_segment():
    assert hc.enum_tail("BSH.Common.EnumType.OperationState.Run") == "Run"
    assert hc.enum_tail("Dishcare.Dishwasher.Program.Eco50") == "Eco50"


def test_enum_tail_passes_through_plain_values():
    assert hc.enum_tail("Closed") == "Closed"
    assert hc.enum_tail(42) == 42
    assert hc.enum_tail(None) is None


# -- EventPresentState -> bool ------------------------------------------------

def test_event_present_bool():
    assert hc.event_present_bool("BSH.Common.EnumType.EventPresentState.Present") is True
    assert hc.event_present_bool("BSH.Common.EnumType.EventPresentState.Confirmed") is True
    assert hc.event_present_bool("BSH.Common.EnumType.EventPresentState.Off") is False
    assert hc.event_present_bool(True) is True
    assert hc.event_present_bool("anything else") is False


# -- remaining-time formatting ------------------------------------------------

def test_format_remaining_zero():
    assert hc.format_remaining(0) == "0:00"


def test_format_remaining_under_an_hour():
    assert hc.format_remaining(1500) == "0:25"      # 25 min


def test_format_remaining_over_an_hour():
    assert hc.format_remaining(5040) == "1:24"      # 84 min -> 1:24


def test_format_remaining_none():
    assert hc.format_remaining(None) is None


def test_format_remaining_negative_clamps():
    assert hc.format_remaining(-10) == "0:00"


# -- progress clamp -----------------------------------------------------------

def test_clamp_percent_bounds():
    assert hc.clamp_percent(-5) == 0
    assert hc.clamp_percent(50) == 50
    assert hc.clamp_percent(150) == 100


# -- prettify -----------------------------------------------------------------

def test_prettify_program_spaces_letter_digit_boundary():
    assert hc.prettify_program("Eco50") == "Eco 50"
    assert hc.prettify_program("Auto2") == "Auto 2"
    assert hc.prettify_program("Cotton") == "Cotton"


# -- type routing / supports_programs -----------------------------------------

def test_device_type_for_known_and_generic():
    assert hc.device_type_for("Dishwasher") == "dishwasher"
    assert hc.device_type_for("FridgeFreezer") == "fridgeFreezer"
    assert hc.device_type_for("Hood") == "homeConnectAppliance"


def test_supports_programs():
    assert hc.supports_programs("dishwasher") is True
    assert hc.supports_programs("fridgeFreezer") is False
    assert hc.supports_programs("homeConnectAppliance") is True


# -- spec lookup --------------------------------------------------------------

def test_spec_for_common_key():
    assert hc.spec_for("dishwasher", "BSH.Common.Status.OperationState") == ("operationState", "enum")


def test_spec_for_type_specific_key():
    assert hc.spec_for("dishwasher", "Dishcare.Dishwasher.Event.SaltNearlyEmpty") == \
        ("saltNearlyEmpty", "event")
    # A dishwasher key is not a state on the washer type.
    assert hc.spec_for("washer", "Dishcare.Dishwasher.Event.SaltNearlyEmpty") is None


def test_spec_for_unknown_key_is_none():
    assert hc.spec_for("oven", "Vendor.Totally.Undocumented") is None


# -- state-id sanitiser -------------------------------------------------------

def test_sanitise_state_key_camelcases():
    assert hc.sanitise_state_key("Vendor.Weird.Undocumented") == "vendorWeirdUndocumented"


def test_sanitise_state_key_forces_letter_start():
    sid = hc.sanitise_state_key("123.abc")
    assert sid[0].isalpha()


def test_sanitise_state_key_empty():
    assert hc.sanitise_state_key("...") == ""


# -- appliance-type filter map ------------------------------------------------

def test_generic_type_accepts_all():
    assert hc.DEVICE_TYPE_TO_APPLIANCE_TYPES["homeConnectAppliance"] is None
    assert hc.DEVICE_TYPE_TO_APPLIANCE_TYPES["oven"] == ("Oven",)
