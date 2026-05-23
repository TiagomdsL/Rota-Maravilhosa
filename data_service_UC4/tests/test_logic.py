import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from main import (
    features_query,
    weather_distribution_query,
    bounding_box_query,
)


def test_features_query():
    query = features_query(
        40.0,
        -8.0,
        10,
    )

    assert "COUNT(*) as total_accidents" in query
    assert "AVG(Severity) as avg_severity" in query
    assert "AVG(Visibility_mi_) as avg_visibility" in query
    assert "AVG(Precipitation_in_) as avg_precipitation" in query

    assert "39.99" in query
    assert "40.01" in query

    assert "-8.01" in query
    assert "-7.99" in query

    assert "EXTRACT(HOUR FROM Start_Time) = 10" in query


def test_features_query2():
    query = features_query(
        38.5,
        -9.1,
        22,
    )

    assert "38.49" in query
    assert "38.51" in query

    assert "-9.11" in query
    assert "-9.09" in query

    assert "EXTRACT(HOUR FROM Start_Time) = 22" in query


def test_weather_distribution_query():
    query = weather_distribution_query(
        40.0,
        -8.0,
        15,
    )

    assert "Weather_Condition" in query
    assert "COUNT(*) as cnt" in query
    assert "GROUP BY Weather_Condition" in query

    assert "39.99" in query
    assert "40.01" in query

    assert "-8.01" in query
    assert "-7.99" in query

    assert "EXTRACT(HOUR FROM Start_Time) = 15" in query


def test_weather_distribution_query2():
    query = weather_distribution_query(
        41.2,
        -7.5,
        3,
    )

    assert "41.19" in query
    assert "41.21" in query

    assert "-7.51" in query
    assert "-7.49" in query

    assert "EXTRACT(HOUR FROM Start_Time) = 3" in query


def test_bounding_box_query():
    query = bounding_box_query(
        39.0,
        40.0,
        -9.0,
        -8.0,
        100,
    )

    assert "Start_Lat BETWEEN 39.0 AND 40.0" in query
    assert "Start_Lng BETWEEN -9.0 AND -8.0" in query
    assert "LIMIT 100" in query

    assert "latitude" in query
    assert "longitude" in query
    assert "severity" in query


def test_bounding_box_query2():
    query = bounding_box_query(
        38.0,
        38.5,
        -9.5,
        -9.0,
        50,
    )

    assert "Start_Lat BETWEEN 38.0 AND 38.5" in query
    assert "Start_Lng BETWEEN -9.5 AND -9.0" in query
    assert "LIMIT 50" in query


def test_bounding_box_query_limit():
    query = bounding_box_query(
        0.0,
        1.0,
        0.0,
        1.0,
        999,
    )

    assert "LIMIT 999" in query
