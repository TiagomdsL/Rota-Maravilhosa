from pytest import approx
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from main import classify_risk, road_factor_map, ml_to_prob, ml_query


def test_classify_risk():
    assert classify_risk(0.1) == "Low"
    assert classify_risk(0.3) == "Medium"
    assert classify_risk(0.6) == "High"
    assert classify_risk(0.9) == "Critical"


def test_road_factor():
    assert road_factor_map("junction") == approx(1.3)
    assert road_factor_map("roundabout") == approx(1.25)
    assert road_factor_map("curve") == approx(1.15)
    assert road_factor_map("traffic_signal") == approx(1.1)
    assert road_factor_map("straight") == approx(0.9)
    assert road_factor_map("unknown") == approx(1.0)


def test_road_factor_case():
    assert road_factor_map("JUNCTION") == approx(1.3)
    assert road_factor_map("RoundAbout") == approx(1.25)


def test_ml_to_prob():
    r = ml_to_prob(2)
    assert r == approx(0.3333, rel=1e-2)

    assert ml_to_prob(100) <= 0.95
    assert ml_to_prob(-100) >= 0.05


def test_ml_query():
    q = ml_query(10, 40.0, -8.0, "rain")

    assert "10 as hour" in q
    assert "40.0 as latitude" in q
    assert "-8.0 as longitude" in q
    assert "rain" in q
    assert "ML.PREDICT" in q


def test_ml_query2():
    q = ml_query(15, 30.0, -8.0, "rain")

    assert "15 as hour" in q
    assert "30.0 as latitude" in q
    assert "-8.0 as longitude" in q
    assert "rain" in q
    assert "ML.PREDICT" in q
