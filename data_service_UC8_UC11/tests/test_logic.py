# tests/test_logic.py
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from main import build_hotspots_query, build_county_comparison_query

# =============================================================================
# HOTSPOTS QUERY TESTS
# =============================================================================


def test_build_hotspots_query_without_filters():
    query = build_hotspots_query(city=None, state=None, limit=10)

    assert "FROM `proj1cc-493515.accidents.accidents`" in query
    assert "GROUP BY Start_Lat, Start_Lng" in query
    assert "LIMIT 10" in query
    assert "WHERE" not in query


def test_build_hotspots_query_with_city():
    query = build_hotspots_query(city="Miami", state=None, limit=5)

    assert "City = 'Miami'" in query
    assert "LIMIT 5" in query


def test_build_hotspots_query_with_state():
    query = build_hotspots_query(city=None, state="CA", limit=15)

    assert "State = 'CA'" in query
    assert "LIMIT 15" in query


def test_build_hotspots_query_with_city_and_state():
    query = build_hotspots_query(city="Miami", state="FL", limit=20)

    assert "City = 'Miami'" in query
    assert "State = 'FL'" in query
    assert "AND" in query
    assert "LIMIT 20" in query


# =============================================================================
# COUNTY COMPARISON QUERY TESTS
# =============================================================================


def test_build_county_comparison_query():
    query = build_county_comparison_query("CA")

    assert "County" in query
    assert "COUNT(ID) as accident_count" in query
    assert "AVG(Severity) as avg_severity" in query
    assert "WHERE State = 'CA'" in query
    assert "GROUP BY County" in query
    assert "ORDER BY accident_count DESC" in query


def test_build_county_comparison_query2():
    query = build_county_comparison_query("TX")

    assert "County" in query
    assert "COUNT(ID) as accident_count" in query
    assert "AVG(Severity) as avg_severity" in query
    assert "WHERE State = 'TX'" in query
    assert "GROUP BY County" in query
    assert "ORDER BY accident_count DESC" in query
