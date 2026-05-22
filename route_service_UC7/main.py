import os
import time
import logging
from typing import List

import httpx

from fastapi import (
    FastAPI,
    HTTPException,
    Response,
    Request,
)

from pydantic import BaseModel

from prometheus_client import (
    Counter,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

from tracing import setup_tracing, get_current_span


# ─────────────────────────────────────────────
# APP INIT
# ─────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Route Service",
    version="1.0.0",
)

setup_tracing(app, "route-service-uc7")


PREDICTION_SERVICE_URL = os.getenv(
    "PREDICTION_SERVICE_URL",
    "http://localhost:8002",
)

SERVICE_NAME = "route-service-uc7"

NUM_WAYPOINTS = 5


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
# DOMAIN LOGIC
# ─────────────────────────────────────────────


def apply_span(span, **kwargs):
    for key, value in kwargs.items():
        span.set_attribute(key, value)


def interpolate_waypoints(
    origin_lat: float,
    origin_lon: float,
    destination_lat: float,
    destination_lon: float,
    n: int = NUM_WAYPOINTS,
):
    waypoints = []

    for i in range(n + 2):
        t = i / (n + 1)

        waypoints.append(
            {
                "latitude": round(
                    origin_lat + t * (destination_lat - origin_lat),
                    6,
                ),
                "longitude": round(
                    origin_lon + t * (destination_lon - origin_lon),
                    6,
                ),
            }
        )

    return waypoints


async def get_risk_for_waypoint(
    client: httpx.AsyncClient,
    latitude: float,
    longitude: float,
    timestamp: str,
):
    try:
        response = await client.post(
            f"{PREDICTION_SERVICE_URL}/risk/score",
            json={
                "latitude": latitude,
                "longitude": longitude,
                "timestamp": timestamp,
            },
        )

        response.raise_for_status()

        return response.json()

    except httpx.HTTPError:
        return {
            "accident_probability": 0.0,
            "predicted_severity": 1,
            "nearby_accidents_count": 0,
        }


def aggregate_risk(
    waypoint_scores: List[dict],
):
    if not waypoint_scores:
        return 0.0

    scores = [
        wp["accident_probability"] * (wp["predicted_severity"] / 4)
        for wp in waypoint_scores
    ]

    return round(
        sum(scores) / len(scores),
        4,
    )


def classify_risk(score: float):
    if score < 0.2:
        return "Low"
    elif score < 0.5:
        return "Medium"
    return "High"


# ─────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────


class RouteRequest(BaseModel):
    origin_lat: float
    origin_lon: float
    destination_lat: float
    destination_lon: float
    timestamp: str = "2016-02-08T08:00:00"


class WaypointRisk(BaseModel):
    latitude: float
    longitude: float
    accident_probability: float
    predicted_severity: int
    nearby_accidents_count: int


class RouteResponse(BaseModel):
    recommended_route: dict
    risk_score: float
    risk_level: str
    total_waypoints_analyzed: int


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
    "/route/analyze",
    response_model=RouteResponse,
)
async def analyze_route(request: RouteRequest):
    span = get_current_span()

    apply_span(
        span,
        **{
            "business.origin_lat": request.origin_lat,
            "business.origin_lon": request.origin_lon,
            "business.destination_lat": request.destination_lat,
            "business.destination_lon": request.destination_lon,
        },
    )

    logger.info(
        f"Route analysis from "
        f"({request.origin_lat},{request.origin_lon}) "
        f"to "
        f"({request.destination_lat},{request.destination_lon})"
    )

    try:
        waypoints = interpolate_waypoints(
            request.origin_lat,
            request.origin_lon,
            request.destination_lat,
            request.destination_lon,
        )

        span.set_attribute(
            "business.total_waypoints",
            len(waypoints),
        )

        async with httpx.AsyncClient(timeout=10.0) as client:

            scores = []

            for waypoint in waypoints:
                risk = await get_risk_for_waypoint(
                    client,
                    waypoint["latitude"],
                    waypoint["longitude"],
                    request.timestamp,
                )

                scores.append(
                    {
                        **waypoint,
                        "accident_probability": risk.get(
                            "accident_probability",
                            0.0,
                        ),
                        "predicted_severity": risk.get(
                            "predicted_severity",
                            1,
                        ),
                        "nearby_accidents_count": risk.get(
                            "nearby_accidents_count",
                            0,
                        ),
                    }
                )

        risk_score = aggregate_risk(scores)

        risk_level = classify_risk(risk_score)

        apply_span(
            span,
            **{
                "business.risk_score": risk_score,
                "business.risk_level": risk_level,
            },
        )

        logger.info(
            f"Route analysis completed: "
            f"risk_score={risk_score}, "
            f"level={risk_level}"
        )

        result = RouteResponse(
            recommended_route={
                "origin": {
                    "latitude": request.origin_lat,
                    "longitude": request.origin_lon,
                },
                "destination": {
                    "latitude": request.destination_lat,
                    "longitude": request.destination_lon,
                },
                "waypoints": scores,
            },
            risk_score=risk_score,
            risk_level=risk_level,
            total_waypoints_analyzed=len(scores),
        )

        return result

    except Exception as e:
        span.set_attribute("error", True)
        span.set_attribute("error.message", str(e))

        raise HTTPException(
            status_code=500,
            detail=str(e),
        )
