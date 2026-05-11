import os
import json
import logging
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import time
from fastapi import FastAPI, Query, Response, Request
from pydantic import BaseModel
from typing import List
from google.cloud import bigquery
from google.oauth2 import service_account

from tracing import setup_tracing, get_current_span

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = FastAPI()
setup_tracing(app, "data-service-uc4")

PROJECT = "proj1cc-493515"
TABLE = "proj1cc-493515.accidents.accidents"
LOCATION = "US"

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

SERVICE_NAME = "data-service-uc4"

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

def get_client():
    api_token = os.environ.get("API_TOKEN")
    if api_token:
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(api_token))
        return bigquery.Client(credentials=credentials, project=PROJECT, location=LOCATION)
    return bigquery.Client(project=PROJECT, location=LOCATION)

class Accident(BaseModel):
    latitude: float
    longitude: float
    severity: int

class BoundingBoxResponse(BaseModel):
    accidents: List[Accident]

@app.get("/health")
async def health():
    logger.info("Health check")
    return {"status": "ok"}

@app.get("/ready")
async def ready():
    return {"status": "ok"}

@app.post("/features")
def get_location_features(payload: dict):
    span = get_current_span()
    span.set_attribute("business.latitude", payload.get("latitude"))
    span.set_attribute("business.longitude", payload.get("longitude"))
    span.set_attribute("business.timestamp", payload.get("timestamp"))
    
    logger.info(f"Features request for location: ({payload.get('latitude')}, {payload.get('longitude')})")
    
    from datetime import datetime
    latitude = payload["latitude"]
    longitude = payload["longitude"]
    hour = datetime.fromisoformat(payload["timestamp"]).hour

    span.set_attribute("business.hour", hour)

    client = get_client()
    sql = f"""
        SELECT
            COUNT(*) as total_accidents,
            AVG(Severity) as avg_severity,
            AVG(Visibility_mi_) as avg_visibility,
            AVG(Precipitation_in_) as avg_precipitation
        FROM `{TABLE}`
        WHERE Start_Lat BETWEEN {latitude-0.01} AND {latitude+0.01}
        AND Start_Lng BETWEEN {longitude-0.01} AND {longitude+0.01}
        AND EXTRACT(HOUR FROM Start_Time) = {hour}
    """
    row = list(client.query(sql).result())[0]

    weather_sql = f"""
        SELECT Weather_Condition, COUNT(*) as cnt
        FROM `{TABLE}`
        WHERE Start_Lat BETWEEN {latitude-0.01} AND {latitude+0.01}
        AND Start_Lng BETWEEN {longitude-0.01} AND {longitude+0.01}
        AND EXTRACT(HOUR FROM Start_Time) = {hour}
        GROUP BY Weather_Condition
    """
    weather_dist = {r["Weather_Condition"]: r["cnt"]
                   for r in client.query(weather_sql).result()
                   if r["Weather_Condition"]}

    result = {
        "total_accidents": row["total_accidents"],
        "avg_severity": round(row["avg_severity"] or 0, 2),
        "avg_visibility": row["avg_visibility"],
        "avg_precipitation": row["avg_precipitation"],
        "weather_distribution": weather_dist
    }
    
    span.set_attribute("business.total_accidents", row["total_accidents"])
    logger.info(f"Features result: {row['total_accidents']} accidents found")
    
    return result

@app.get("/accidents/bounding-box", response_model=BoundingBoxResponse)
def get_accidents_bounding_box(
    min_lat: float, max_lat: float,
    min_lon: float, max_lon: float,
    limit: int = 100
):
    span = get_current_span()
    span.set_attribute("business.min_lat", min_lat)
    span.set_attribute("business.max_lat", max_lat)
    span.set_attribute("business.min_lon", min_lon)
    span.set_attribute("business.max_lon", max_lon)
    span.set_attribute("business.limit", limit)
    
    logger.info(f"Bounding box: ({min_lat},{min_lon}) to ({max_lat},{max_lon}), limit={limit}")
    
    client = get_client()
    sql = f"""
        SELECT Start_Lat as latitude, Start_Lng as longitude, Severity as severity
        FROM `{TABLE}`
        WHERE Start_Lat BETWEEN {min_lat} AND {max_lat}
        AND Start_Lng BETWEEN {min_lon} AND {max_lon}
        LIMIT {limit}
    """
    rows = [dict(r) for r in client.query(sql).result()]
    
    logger.info(f"Bounding box returned {len(rows)} accidents")
    return {"accidents": rows}