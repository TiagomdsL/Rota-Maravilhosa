# tests/test_logic.py
import sys
import os
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from main import (
    build_statistics_query,
    build_weather_analysis_query,
    build_temporal_analysis_query,
    normalize_state,
    STATE_NAMES,
    NAME_TO_CODE,
)

# =============================================================================
# NORMALIZE STATE TESTS
# =============================================================================


def test_normalize_state_with_code():
    result = normalize_state("CA")
    assert result == "CA"


def test_normalize_state_with_code_lowercase():
    result = normalize_state("ca")
    assert result == "CA"


def test_normalize_state_with_name():
    result = normalize_state("California")
    assert result == "CA"


def test_normalize_state_with_name_lowercase():
    result = normalize_state("california")
    assert result == "CA"


def test_normalize_state_with_name_two_words():
    result = normalize_state("New York")
    assert result == "NY"


def test_normalize_state_with_whitespace():
    result = normalize_state("  Texas  ")
    assert result == "TX"


def test_normalize_state_invalid_raises_error():
    with pytest.raises(ValueError, match="Invalid state: InvalidState"):
        normalize_state("InvalidState")


def test_normalize_state_invalid_code_raises_error():
    with pytest.raises(ValueError, match="Invalid state: XX"):
        normalize_state("XX")


# =============================================================================
# STATISTICS QUERY TESTS
# =============================================================================


def test_build_statistics_query():
    query = build_statistics_query("CA", "2024-01-01", "2024-12-31")

    assert "SELECT COUNT(*) as total_accidents, AVG(Severity) as avg_severity" in query
    assert "FROM `proj1cc-493515.accidents.accidents`" in query
    assert "WHERE State = 'CA'" in query
    assert "AND Start_Time BETWEEN '2024-01-01' AND '2024-12-31'" in query


def test_build_statistics_query_different_state():
    query = build_statistics_query("TX", "2023-01-01", "2023-06-30")

    assert "WHERE State = 'TX'" in query
    assert "BETWEEN '2023-01-01' AND '2023-06-30'" in query


def test_build_statistics_query_single_day():
    query = build_statistics_query("FL", "2024-03-15", "2024-03-15")

    assert "WHERE State = 'FL'" in query
    assert "BETWEEN '2024-03-15' AND '2024-03-15'" in query


# =============================================================================
# WEATHER ANALYSIS QUERY TESTS
# =============================================================================


def test_build_weather_analysis_query_with_state():
    query = build_weather_analysis_query("CA")

    assert "SELECT Weather_Condition" in query
    assert "COUNT(*) as accident_count" in query
    assert "AVG(Severity) as avg_severity" in query
    assert "FROM `proj1cc-493515.accidents.accidents`" in query
    assert "WHERE State = 'CA'" in query
    assert "GROUP BY Weather_Condition" in query
    assert "HAVING Weather_Condition IS NOT NULL" in query
    assert "ORDER BY accident_count DESC" in query


def test_build_weather_analysis_query_without_state():
    query = build_weather_analysis_query(None)

    assert "SELECT Weather_Condition" in query
    assert "COUNT(*) as accident_count" in query
    assert "AVG(Severity) as avg_severity" in query
    assert "FROM `proj1cc-493515.accidents.accidents`" in query
    assert "WHERE" not in query
    assert "GROUP BY Weather_Condition" in query
    assert "HAVING Weather_Condition IS NOT NULL" in query
    assert "ORDER BY accident_count DESC" in query


def test_build_weather_analysis_query_different_state():
    query = build_weather_analysis_query("TX")

    assert "WHERE State = 'TX'" in query


# =============================================================================
# TEMPORAL ANALYSIS QUERY TESTS
# =============================================================================


def test_build_temporal_analysis_query_without_day():
    query = build_temporal_analysis_query("Miami", None)

    assert (
        "SELECT EXTRACT(HOUR FROM Start_Time) as hour, COUNT(*) as accident_count"
        in query
    )
    assert "FROM `proj1cc-493515.accidents.accidents`" in query
    assert "WHERE LOWER(City) = LOWER('Miami')" in query
    assert "GROUP BY hour ORDER BY hour" in query
    assert "AND FORMAT_TIMESTAMP" not in query


def test_build_temporal_analysis_query_with_day():
    query = build_temporal_analysis_query("Los Angeles", "Monday")

    assert "WHERE LOWER(City) = LOWER('Los Angeles')" in query
    assert "AND FORMAT_TIMESTAMP('%A', Start_Time) = 'Monday'" in query
    assert "GROUP BY hour ORDER BY hour" in query


def test_build_temporal_analysis_query_with_different_day():
    query = build_temporal_analysis_query("Chicago", "Friday")

    assert "WHERE LOWER(City) = LOWER('Chicago')" in query
    assert "AND FORMAT_TIMESTAMP('%A', Start_Time) = 'Friday'" in query


def test_build_temporal_analysis_query_case_insensitive_city():
    query = build_temporal_analysis_query("New York", None)

    assert "LOWER(City) = LOWER('New York')" in query


def test_build_temporal_analysis_query_with_city_having_apostrophe():
    query = build_temporal_analysis_query("St. Louis", None)

    assert "LOWER(City) = LOWER('St. Louis')" in query


# =============================================================================
# STATE NAMES DICTIONARY TESTS
# =============================================================================


def test_state_names_has_all_states():
    expected_states = [
        "AL",
        "AK",
        "AZ",
        "AR",
        "CA",
        "CO",
        "CT",
        "DE",
        "FL",
        "GA",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "VA",
        "WA",
        "WV",
        "WI",
        "WY",
    ]

    for state in expected_states:
        assert state in STATE_NAMES


def test_name_to_code_reverse_mapping():
    assert NAME_TO_CODE["California"] == "CA"
    assert NAME_TO_CODE["Texas"] == "TX"
    assert NAME_TO_CODE["New York"] == "NY"
    assert NAME_TO_CODE["Florida"] == "FL"


def test_name_to_code_completeness():
    for code, name in STATE_NAMES.items():
        assert NAME_TO_CODE[name] == code


# =============================================================================
# PARAMETRIZED TESTS
# =============================================================================


@pytest.mark.parametrize(
    "input_state,expected_code",
    [
        ("CA", "CA"),
        ("ca", "CA"),
        ("California", "CA"),
        ("california", "CA"),
        ("NY", "NY"),
        ("New York", "NY"),
        ("TX", "TX"),
        ("Texas", "TX"),
        ("FL", "FL"),
        ("Florida", "FL"),
    ],
)
def test_normalize_state_parametrized(input_state, expected_code):
    """Test multiple state normalizations with a single test."""
    assert normalize_state(input_state) == expected_code


@pytest.mark.parametrize(
    "invalid_state",
    [
        "InvalidState",
        "XX",
        "ZZ",
        "Portugal",
        "123",
        "",
    ],
)
def test_normalize_state_invalid_parametrized(invalid_state):
    """Test multiple invalid states with a single test."""
    with pytest.raises(ValueError, match=f"Invalid state: {invalid_state}"):
        normalize_state(invalid_state)
