import os
import json
import logging
import time
from typing import List

from fastapi import FastAPI, HTTPException, Query, Response, Request
from pydantic import BaseModel, Field

from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

from google.cloud import bigquery
from google.oauth2 import service_account

from tracing import setup_tracing, get_current_span


# ─────────────────────────────────────────────
# APP INIT
# ─────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Accident Prediction Service", version="1.0.0")
setup_tracing(app, "prediction-service-uc9-uc10")


PROJECT = "proj1cc-493515"
TABLE = "proj1cc-493515.accidents.accidents"
LOCATION = "US"
MODEL = "proj1cc-493515.accidents.occurrence_model2"

SERVICE_NAME = "prediction-service-uc9-uc10"


# ─────────────────────────────────────────────
# PROMETHEUS
# ─────────────────────────────────────────────

REQUEST_COUNT = Counter("requests_total", "Total requests", ["service", "endpoint"])

REQUEST_LATENCY = Histogram("request_latency_seconds", "Request latency", ["service"])

ERROR_COUNT = Counter("requests_errors_total", "Total errors", ["service"])


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.time()

    try:
        response = await call_next(request)

        REQUEST_COUNT.labels(service=SERVICE_NAME, endpoint=request.url.path).inc()

        if response.status_code >= 400:
            ERROR_COUNT.labels(service=SERVICE_NAME).inc()

        return response

    finally:
        REQUEST_LATENCY.labels(service=SERVICE_NAME).observe(time.time() - start)


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ─────────────────────────────────────────────
# BIGQUERY CLIENT
# ─────────────────────────────────────────────


def get_client():
    api_token = os.environ.get("API_TOKEN")

    if api_token:
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(api_token)
        )
        return bigquery.Client(
            credentials=credentials, project=PROJECT, location=LOCATION
        )

    return bigquery.Client(project=PROJECT, location=LOCATION)


# ─────────────────────────────────────────────
# DOMAIN LOGIC (REFATORADO)
# ─────────────────────────────────────────────


def classify_risk(prob: float) -> str:
    if prob < 0.25:
        return "Low"
    elif prob < 0.5:
        return "Medium"
    elif prob < 0.75:
        return "High"
    return "Critical"


def road_factor_map(road_topology: str) -> float:
    factors = {
        "junction": 1.3,
        "roundabout": 1.25,
        "curve": 1.15,
        "traffic_signal": 1.1,
        "straight": 0.9,
    }
    return factors.get(road_topology.lower(), 1.0)


def ml_query(hour, lat, lon, weather):
    return f"""
    SELECT predicted_severity_score as accident_probability
    FROM ML.PREDICT(
      MODEL `{MODEL}`,
      (
        SELECT
          {hour} as hour,
          {lat} as latitude,
          {lon} as longitude,
          '{weather}' as weather_condition
      )
    )
    """


def ml_to_prob(row_value: float) -> float:
    prob = (row_value - 1) / 3
    return min(0.95, max(0.05, prob))


def apply_span(span, **kwargs):
    for k, v in kwargs.items():
        span.set_attribute(k, v)


# ─────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────


class PredictRequest(BaseModel):
    latitude: float
    longitude: float
    hour: int = Field(..., ge=0, le=23)
    weather_condition: str


class PredictResponse(BaseModel):
    accident_probability: float
    risk_level: str


class SimulateRequest(BaseModel):
    latitude: float
    longitude: float
    hour: int = Field(..., ge=0, le=23)
    weather_condition: str
    road_topology: str


class SimulateResponse(BaseModel):
    probability_score: float
    predicted_severity: str
    explanation: List[str]


# ─────────────────────────────────────────────
# HEALTH
# ─────────────────────────────────────────────


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    return {"status": "ok"}


# ─────────────────────────────────────────────
# ENDPOINTS (CLEAN)
# ─────────────────────────────────────────────


@app.post("/accidents/predict-occurrence", response_model=PredictResponse)
def predict_occurrence(request: PredictRequest):
    span = get_current_span()

    apply_span(
        span,
        **{
            "business.latitude": request.latitude,
            "business.longitude": request.longitude,
            "business.hour": request.hour,
            "business.weather_condition": request.weather_condition,
        },
    )

    logger.info(
        f"Predict occurrence: lat={request.latitude}, lon={request.longitude}, hour={request.hour}"
    )

    try:
        client = get_client()
        sql = ml_query(
            request.hour, request.latitude, request.longitude, request.weather_condition
        )

        rows = list(client.query(sql).result())

        severity_score = float(rows[0]["accident_probability"])
        prob = ml_to_prob(severity_score)

        result = PredictResponse(
            accident_probability=round(prob, 4), risk_level=classify_risk(prob)
        )

        span.set_attribute("business.accident_probability", result.accident_probability)

        return result

    except Exception as e:
        span.set_attribute("error", True)
        span.set_attribute("error.message", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/accidents/simulate-risk", response_model=SimulateResponse)
def simulate_risk(request: SimulateRequest):
    span = get_current_span()

    apply_span(
        span,
        **{
            "business.latitude": request.latitude,
            "business.longitude": request.longitude,
            "business.hour": request.hour,
            "business.weather_condition": request.weather_condition,
            "business.road_topology": request.road_topology,
        },
    )

    try:
        road_factor = road_factor_map(request.road_topology)
        client = get_client()

        sql = ml_query(
            request.hour, request.latitude, request.longitude, request.weather_condition
        )

        rows = list(client.query(sql).result())

        severity_score = float(rows[0]["accident_probability"])
        base_prob = ml_to_prob(severity_score)

        final_prob = min(0.95, base_prob * road_factor)

        explanation = []

        if base_prob > 0.06:
            explanation.append(f"Hour {request.hour}:00 has elevated accident rate")

        if road_factor > 1.2:
            explanation.append(f"{request.road_topology} is a high-risk road feature")

        result = SimulateResponse(
            probability_score=round(final_prob, 4),
            predicted_severity=classify_risk(final_prob),
            explanation=explanation,
        )

        span.set_attribute("business.final_probability", result.probability_score)

        return result

    except Exception as e:
        span.set_attribute("error", True)
        span.set_attribute("error.message", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
def get_stats():
    client = get_client()
    row = list(client.query(f"SELECT COUNT(*) as total FROM `{TABLE}`").result())[0]

    return {"total_accidents": row["total"]}
