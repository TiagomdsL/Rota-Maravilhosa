# route_service/main.py
import os
import httpx
import logging
from fastapi import FastAPI, HTTPException, Query, Response, Request
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import time
from pydantic import BaseModel

from tracing import setup_tracing, get_current_span

app = FastAPI(title="Route Service")
setup_tracing(app, "route-service-uc7")

logger = logging.getLogger(__name__)

PREDICTION_SERVICE_URL = os.getenv("PREDICTION_SERVICE_URL", "http://localhost:8002")

REQUEST_COUNT = Counter(
    "requests_total",
    "Total requests",
    ["service", "endpoint"]
)

REQUEST_LATENCY = Histogram(
    "request_latency_seconds",
    "Request latency",
    ["service"]
)

ERROR_COUNT = Counter(
    "requests_errors_total",
    "Total errors",
    ["service"]
)

SERVICE_NAME = "route-service-uc7"

@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.time()
    try:
        response = await call_next(request)

        REQUEST_COUNT.labels(
            service=SERVICE_NAME,
            endpoint=request.url.path
        ).inc()

        if response.status_code >= 400:
            ERROR_COUNT.labels(service=SERVICE_NAME).inc()

        return response

    finally:
        duration = time.time() - start
        REQUEST_LATENCY.labels(service=SERVICE_NAME).observe(duration)

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

NUM_WAYPOINTS = 5

class RouteRequest(BaseModel):
    origin_lat: float
    origin_lon: float
    destination_lat: float
    destination_lon: float
    timestamp: str = "2016-02-08T08:00:00"

def interpolate_waypoints(
    origin_lat: float, origin_lon: float,
    destination_lat: float, destination_lon: float,
    n: int = NUM_WAYPOINTS
) -> list[dict]:
    waypoints = []
    for i in range(n + 2):  
        t = i / (n + 1)
        waypoints.append({
            "latitude": round(origin_lat + t * (destination_lat - origin_lat), 6),
            "longitude": round(origin_lon + t * (destination_lon - origin_lon), 6),
        })
    return waypoints

async def get_risk_for_waypoint(
    client: httpx.AsyncClient,
    lat: float,
    lon: float,
    timestamp: str
) -> dict:
    try:
        response = await client.post(
            f"{PREDICTION_SERVICE_URL}/risk/score",
            json={"latitude": lat, "longitude": lon, "timestamp": timestamp}
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError:
        return {"accident_probability": 0.0, "predicted_severity": 1, "nearby_accidents_count": 0}

def aggregate_risk(waypoint_scores: list[dict]) -> float:
    if not waypoint_scores:
        return 0.0

    scores = [
        wp["accident_probability"] * (wp["predicted_severity"] / 4)
        for wp in waypoint_scores
    ]
    return round(sum(scores) / len(scores), 4)

@app.get("/health")
async def health():
    logger.info("Health check")
    return {"gateway": "ok"}

@app.post("/route/analyze")
async def analyze_route(request: RouteRequest):
    span = get_current_span()
    span.set_attribute("business.origin_lat", request.origin_lat)
    span.set_attribute("business.origin_lon", request.origin_lon)
    span.set_attribute("business.destination_lat", request.destination_lat)
    span.set_attribute("business.destination_lon", request.destination_lon)
    
    logger.info(f"Route analysis from ({request.origin_lat},{request.origin_lon}) to ({request.destination_lat},{request.destination_lon})")
    
    waypoints = interpolate_waypoints(
        request.origin_lat, request.origin_lon,
        request.destination_lat, request.destination_lon
    )
    
    span.set_attribute("business.total_waypoints", len(waypoints))

    async with httpx.AsyncClient(timeout=10.0) as client:
        scores = []
        for i, wp in enumerate(waypoints):
            score = await get_risk_for_waypoint(
                client, wp["latitude"], wp["longitude"], request.timestamp
            )
            scores.append({
                **wp,
                "accident_probability": score.get("accident_probability", 0.0),
                "predicted_severity": score.get("predicted_severity", 1),
                "nearby_accidents_count": score.get("nearby_accidents_count", 0),
            })

    risk_score = aggregate_risk(scores)
    risk_level = _risk_label(risk_score)
    
    span.set_attribute("business.risk_score", risk_score)
    span.set_attribute("business.risk_level", risk_level)
    
    logger.info(f"Route analysis completed: risk_score={risk_score}, level={risk_level}")

    return {
        "recommended_route": {
            "origin": {"latitude": request.origin_lat, "longitude": request.origin_lon},
            "destination": {"latitude": request.destination_lat, "longitude": request.destination_lon},
            "waypoints": scores,
        },
        "risk_score": risk_score,
        "risk_level": risk_level,
        "total_waypoints_analyzed": len(scores),
    }

def _risk_label(score: float) -> str:
    if score < 0.2:
        return "Low"
    elif score < 0.5:
        return "Medium"
    else:
        return "High"

@app.get("/ready")
async def ready():
    return {"status": "ok"}