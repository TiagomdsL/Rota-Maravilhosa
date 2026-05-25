# tests/test_logic.py

from pytest import approx
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from main import (
    normalize_probability,
    classify_severity,
    severity_prediction_query,
    risk_prediction_query,
    nearby_accidents_query,
)


def test_normalize_probability():
    result = normalize_probability(2)

    assert result == approx(0.3333, rel=1e-2)


def test_normalize_probability_limits():
    assert normalize_probability(100) <= 0.95
    assert normalize_probability(-100) >= 0.05


def test_classify_severity():
    assert classify_severity(0.1) == 1
    assert classify_severity(0.3) == 2
    assert classify_severity(0.6) == 3
    assert classify_severity(0.9) == 4


def test_severity_prediction_query():
    query = severity_prediction_query(
        11.0,
        0.2,
        "Rain",
    )

    assert "11.0 as visibility" in query
    assert "0.2 as precipitation" in query
    assert "Rain" in query
    assert "ML.PREDICT" in query
    assert "severity_model" in query


def test_severity_prediction_query2():
    query = severity_prediction_query(
        5.0,
        1.5,
        "Fog",
    )

    assert "5.0 as visibility" in query
    assert "1.5 as precipitation" in query
    assert "Fog" in query


def test_risk_prediction_query():
    query = risk_prediction_query(8)

    assert "8 as hour" in query
    assert "risk_model" in query
    assert "ML.PREDICT" in query


def test_risk_prediction_query2():
    query = risk_prediction_query(22)

    assert "22 as hour" in query
    assert "predicted_severity_score" in query


def test_nearby_accidents_query():
    query = nearby_accidents_query(
        40.0,
        -8.0,
    )

    assert "40.01" in query
    assert "39.99" in query
    assert "-7.99" in query
    assert "-8.01" in query
    assert "COUNT(*) as nearby_count" in query


def test_nearby_accidents_query2():
    query = nearby_accidents_query(
        38.5,
        -9.1,
    )

    assert "38.49" in query
    assert "38.51" in query
    assert "-9.11" in query
    assert "-9.09" in query


# test
