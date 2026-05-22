import os
import json
import logging
import time
from datetime import datetime
from typing import Dict, List

from fastapi import FastAPI, Response, Request, HTTPException
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

app = FastAPI(
    title="Data Service",
    version="1.0.0",
)

setup_tracing(app, "data-service-uc4")


PROJECT = "proj1cc-493515"
TABLE = "proj1cc-493515.accidents.accidents"
LOCATION = "US"

SERVICE_NAME = "data-service-uc4"


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


def features_query(
    latitude: float,
    longitude: float,
    hour: int,
):
    return f"""
    SELECT
        COUNT(*) as total_accidents,
        AVG(Severity) as avg_severity,
        AVG(Visibility_mi_) as avg_visibility,
        AVG(Precipitation_in_) as avg_precipitation
    FROM `{TABLE}`
    WHERE
        Start_Lat BETWEEN {latitude - 0.01} AND {latitude + 0.01}
        AND Start_Lng BETWEEN {longitude - 0.01} AND {longitude + 0.01}
        AND EXTRACT(HOUR FROM Start_Time) = {hour}
    """


def weather_distribution_query(
    latitude: float,
    longitude: float,
    hour: int,
):
    return f"""
    SELECT
        Weather_Condition,
        COUNT(*) as cnt
    FROM `{TABLE}`
    WHERE
        Start_Lat BETWEEN {latitude - 0.01} AND {latitude + 0.01}
        AND Start_Lng BETWEEN {longitude - 0.01} AND {longitude + 0.01}
        AND EXTRACT(HOUR FROM Start_Time) = {hour}
    GROUP BY Weather_Condition
    """


def bounding_box_query(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    limit: int,
):
    return f"""
    SELECT
        Start_Lat as latitude,
        Start_Lng as longitude,
        Severity as severity
    FROM `{TABLE}`
    WHERE
        Start_Lat BETWEEN {min_lat} AND {max_lat}
        AND Start_Lng BETWEEN {min_lon} AND {max_lon}
    LIMIT {limit}
    """


# ─────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────


class FeaturesRequest(BaseModel):
    latitude: float
    longitude: float
    timestamp: str


class FeaturesResponse(BaseModel):
    total_accidents: int
    avg_severity: float
    avg_visibility: float | None
    avg_precipitation: float | None
    weather_distribution: Dict[str, int]


class Accident(BaseModel):
    latitude: float
    longitude: float
    severity: int


class BoundingBoxResponse(BaseModel):
    accidents: List[Accident]


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
    "/features",
    response_model=FeaturesResponse,
)
def get_location_features(request: FeaturesRequest):
    span = get_current_span()

    apply_span(
        span,
        **{
            "business.latitude": request.latitude,
            "business.longitude": request.longitude,
            "business.timestamp": request.timestamp,
        },
    )

    logger.info(
        f"Features request for location: " f"({request.latitude}, {request.longitude})"
    )

    try:
        hour = datetime.fromisoformat(request.timestamp).hour

        span.set_attribute(
            "business.hour",
            hour,
        )

        client = get_client()

        sql = features_query(
            request.latitude,
            request.longitude,
            hour,
        )

        row = list(client.query(sql).result())[0]

        weather_sql = weather_distribution_query(
            request.latitude,
            request.longitude,
            hour,
        )

        weather_distribution = {
            r["Weather_Condition"]: r["cnt"]
            for r in client.query(weather_sql).result()
            if r["Weather_Condition"]
        }

        result = FeaturesResponse(
            total_accidents=row["total_accidents"],
            avg_severity=round(
                row["avg_severity"] or 0,
                2,
            ),
            avg_visibility=row["avg_visibility"],
            avg_precipitation=row["avg_precipitation"],
            weather_distribution=weather_distribution,
        )

        span.set_attribute(
            "business.total_accidents",
            result.total_accidents,
        )

        logger.info(f"Features result: " f"{result.total_accidents} accidents found")

        return result

    except Exception as e:
        span.set_attribute("error", True)
        span.set_attribute("error.message", str(e))

        raise HTTPException(
            status_code=500,
            detail=str(e),
        )


@app.get(
    "/accidents/bounding-box",
    response_model=BoundingBoxResponse,
)
def get_accidents_bounding_box(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    limit: int = 100,
):
    span = get_current_span()

    apply_span(
        span,
        **{
            "business.min_lat": min_lat,
            "business.max_lat": max_lat,
            "business.min_lon": min_lon,
            "business.max_lon": max_lon,
            "business.limit": limit,
        },
    )

    logger.info(
        f"Bounding box: "
        f"({min_lat},{min_lon}) "
        f"to ({max_lat},{max_lon}), "
        f"limit={limit}"
    )

    try:
        client = get_client()

        sql = bounding_box_query(
            min_lat,
            max_lat,
            min_lon,
            max_lon,
            limit,
        )

        rows = [dict(row) for row in client.query(sql).result()]

        logger.info(f"Bounding box returned " f"{len(rows)} accidents")

        return BoundingBoxResponse(accidents=rows)

    except Exception as e:
        span.set_attribute("error", True)
        span.set_attribute("error.message", str(e))

        raise HTTPException(
            status_code=500,
            detail=str(e),
        )
