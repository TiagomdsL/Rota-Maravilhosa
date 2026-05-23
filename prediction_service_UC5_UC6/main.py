import os
import json
import logging
import time
from datetime import datetime

from fastapi import FastAPI, HTTPException, Response, Request
from pydantic import BaseModel

from prometheus_client import (
    Counter,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

from google.cloud import bigquery
from google.oauth2 import service_account

from tracing import setup_tracing, get_current_span


# ─────────────────────────────────────────────
# APP INIT
# ─────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Prediction Service", version="1.0.0")

setup_tracing(app, "prediction-service-uc5-uc6")


PROJECT = "proj1cc-493515"
TABLE = "proj1cc-493515.accidents.accidents"
LOCATION = "US"

SERVICE_NAME = "prediction-service-uc5-uc6"

DATA_SERVICE_URL = os.getenv(
    "DATA_SERVICE_URL",
    "http://localhost:8001",
)


# ─────────────────────────────────────────────
# PROMETHEUS
# ─────────────────────────────────────────────

REQUEST_COUNT = Counter(
    "requests_total",
    "Total requests",
    ["service", "endpoint"],
)

REQUEST_LATENCY = Histogram(
    "request_latency_seconds",
    "Request latency",
    ["service"],
)

ERROR_COUNT = Counter(
    "requests_errors_total",
    "Total errors",
    ["service"],
)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.time()

    try:
        response = await call_next(request)

        REQUEST_COUNT.labels(
            service=SERVICE_NAME,
            endpoint=request.url.path,
        ).inc()

        if response.status_code >= 400:
            ERROR_COUNT.labels(service=SERVICE_NAME).inc()

        return response

    finally:
        REQUEST_LATENCY.labels(service=SERVICE_NAME).observe(time.time() - start)


@app.get("/metrics")
def metrics():
    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


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
            credentials=credentials,
            project=PROJECT,
            location=LOCATION,
        )

    return bigquery.Client(
        project=PROJECT,
        location=LOCATION,
    )


# ─────────────────────────────────────────────
# DOMAIN LOGIC
# ─────────────────────────────────────────────


def apply_span(span, **kwargs):
    for key, value in kwargs.items():
        span.set_attribute(key, value)


def normalize_probability(value: float) -> float:
    prob = (value - 1) / 3
    return min(0.95, max(0.05, prob))


def classify_severity(probability: float) -> int:
    if probability < 0.25:
        return 1
    elif probability < 0.5:
        return 2
    elif probability < 0.75:
        return 3
    return 4


def severity_prediction_query(
    visibility: float,
    precipitation: float,
    weather_condition: str,
):
    return f"""
    SELECT predicted_Severity as predicted_severity
    FROM ML.PREDICT(
      MODEL `proj1cc-493515.accidents.severity_model`,
      (
        SELECT
          {visibility} as visibility,
          {precipitation} as precipitation,
          '{weather_condition}' as weather_condition
      )
    )
    """


def risk_prediction_query(hour: int):
    return f"""
    SELECT predicted_severity_score as risk_probability
    FROM ML.PREDICT(
      MODEL `proj1cc-493515.accidents.risk_model`,
      (
        SELECT {hour} as hour
      )
    )
    """


def nearby_accidents_query(latitude: float, longitude: float):
    return f"""
    SELECT COUNT(*) as nearby_count
    FROM `{TABLE}`
    WHERE
        Start_Lat BETWEEN {latitude - 0.01} AND {latitude + 0.01}
        AND Start_Lng BETWEEN {longitude - 0.01} AND {longitude + 0.01}
        AND Start_Time IS NOT NULL
    """


# ─────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────


class SeverityInput(BaseModel):
    visibility: float
    precipitation: float
    weather_condition: str


class SeverityResponse(BaseModel):
    predicted_severity: int


class RiskRequest(BaseModel):
    latitude: float
    longitude: float
    timestamp: datetime


class RiskResponse(BaseModel):
    accident_probability: float
    predicted_severity: int
    nearby_accidents_count: int


# ─────────────────────────────────────────────
# HEALTH
# ─────────────────────────────────────────────


@app.get("/health")
async def health():
    logger.info("Health check")
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    return {"status": "ok"}


# ─────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────


@app.post(
    "/predict-severity",
    response_model=SeverityResponse,
)
def predict_severity(request: SeverityInput):
    span = get_current_span()

    apply_span(
        span,
        **{
            "business.visibility": request.visibility,
            "business.precipitation": request.precipitation,
            "business.weather_condition": request.weather_condition,
        },
    )

    logger.info(
        f"Predict severity: visibility={request.visibility}, "
        f"precipitation={request.precipitation}"
    )

    try:
        client = get_client()

        sql = severity_prediction_query(
            request.visibility,
            request.precipitation,
            request.weather_condition,
        )

        rows = list(client.query(sql).result())

        predicted_severity = int(rows[0]["predicted_severity"])

        result = SeverityResponse(predicted_severity=predicted_severity)

        span.set_attribute(
            "business.predicted_severity",
            result.predicted_severity,
        )

        logger.info(f"Predicted severity: {result.predicted_severity}")

        return result

    except Exception as e:
        span.set_attribute("error", True)
        span.set_attribute("error.message", str(e))

        raise HTTPException(
            status_code=500,
            detail=str(e),
        )


@app.post(
    "/risk/score",
    response_model=RiskResponse,
)
async def calculate_risk_score(request: RiskRequest):
    span = get_current_span()

    apply_span(
        span,
        **{
            "business.latitude": request.latitude,
            "business.longitude": request.longitude,
            "business.timestamp": request.timestamp.isoformat(),
        },
    )

    logger.info(
        f"Risk score for location: " f"({request.latitude}, {request.longitude})"
    )

    try:
        client = get_client()

        sql = risk_prediction_query(request.timestamp.hour)

        rows = list(client.query(sql).result())

        raw_probability = float(rows[0]["risk_probability"])

        probability = normalize_probability(raw_probability)

        severity = classify_severity(probability)

        nearby_count = 0

        try:
            nearby_sql = nearby_accidents_query(
                request.latitude,
                request.longitude,
            )

            nearby_rows = list(client.query(nearby_sql).result())

            nearby_count = nearby_rows[0]["nearby_count"]

        except Exception as nearby_error:
            logger.warning(f"Could not fetch nearby accidents: " f"{nearby_error}")

        result = RiskResponse(
            accident_probability=round(probability, 4),
            predicted_severity=severity,
            nearby_accidents_count=nearby_count,
        )

        apply_span(
            span,
            **{
                "business.accident_probability": result.accident_probability,
                "business.predicted_severity": result.predicted_severity,
                "business.nearby_accidents": result.nearby_accidents_count,
            },
        )

        logger.info(
            f"Risk score result: "
            f"probability={result.accident_probability}, "
            f"severity={result.predicted_severity}, "
            f"nearby={result.nearby_accidents_count}"
        )

        return result

    except Exception as e:
        span.set_attribute("error", True)
        span.set_attribute("error.message", str(e))

        raise HTTPException(
            status_code=500,
            detail=str(e),
        )
