# tests/test_logic.py

from pytest import approx
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from main import (
    interpolate_waypoints,
    aggregate_risk,
    classify_risk,
)


def test_classify_risk():
    assert classify_risk(0.1) == "Low"
    assert classify_risk(0.3) == "Medium"
    assert classify_risk(0.8) == "High"


def test_interpolate_waypoints_count():
    waypoints = interpolate_waypoints(
        0.0,
        0.0,
        10.0,
        10.0,
    )

    assert len(waypoints) == 7


def test_interpolate_waypoints_start_end():
    waypoints = interpolate_waypoints(
        1.0,
        2.0,
        3.0,
        4.0,
    )

    assert waypoints[0]["latitude"] == approx(1.0)
    assert waypoints[0]["longitude"] == approx(2.0)

    assert waypoints[-1]["latitude"] == approx(3.0)
    assert waypoints[-1]["longitude"] == approx(4.0)


def test_interpolate_waypoints_middle():
    waypoints = interpolate_waypoints(
        0.0,
        0.0,
        6.0,
        6.0,
        n=1,
    )

    assert len(waypoints) == 3

    middle = waypoints[1]

    assert middle["latitude"] == approx(3.0)
    assert middle["longitude"] == approx(3.0)


def test_aggregate_risk_empty():
    assert aggregate_risk([]) == 0.0


def test_aggregate_risk():
    scores = [
        {
            "accident_probability": 0.4,
            "predicted_severity": 4,
        },
        {
            "accident_probability": 0.2,
            "predicted_severity": 2,
        },
    ]

    result = aggregate_risk(scores)

    expected = ((0.4 * 1.0) + (0.2 * 0.5)) / 2

    assert result == approx(expected, rel=1e-3)


def test_aggregate_risk_low_values():
    scores = [
        {
            "accident_probability": 0.1,
            "predicted_severity": 1,
        },
        {
            "accident_probability": 0.2,
            "predicted_severity": 1,
        },
    ]

    result = aggregate_risk(scores)

    assert result < 0.1


def test_interpolate_waypoints_horizontal():
    waypoints = interpolate_waypoints(
        0.0,
        0.0,
        0.0,
        10.0,
        n=2,
    )

    assert waypoints[0]["latitude"] == approx(0.0)
    assert waypoints[-1]["latitude"] == approx(0.0)
    assert waypoints[-1]["longitude"] == approx(10.0)
