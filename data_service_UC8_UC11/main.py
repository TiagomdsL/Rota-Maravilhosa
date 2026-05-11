import os
import json
import logging
from typing import Optional
from google.cloud import bigquery
from google.oauth2 import service_account
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import time
from fastapi import FastAPI, Query, Response, Request

from tracing import setup_tracing, get_current_span

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = FastAPI()
setup_tracing(app, "data-service-uc8-uc11")

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

SERVICE_NAME = "data-service-uc8-uc11"

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

@app.get("/health")
async def health():
    logger.info("Health check")
    return {"status": "ok"}

@app.get("/ready")
async def ready():
    return {"status": "ok"}

@app.get("/hotspots")
def get_hotspots(
    city: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    limit: int = 10
):
    span = get_current_span()
    if city:
        span.set_attribute("business.city", city)
    if state:
        span.set_attribute("business.state", state)
    span.set_attribute("business.limit", limit)
    
    logger.info(f"Hotspots request: city={city}, state={state}, limit={limit}")
    
    filters = []
    if city:
        filters.append(f"City = '{city}'")
    if state:
        filters.append(f"State = '{state}'")
    where = "WHERE " + " AND ".join(filters) if filters else ""
    client = get_client()
    sql = f"""
        SELECT Start_Lat, Start_Lng, COUNT(*) as count
        FROM `{TABLE}` {where}
        GROUP BY Start_Lat, Start_Lng
        ORDER BY count DESC
        LIMIT {limit}
    """
    result = [dict(r) for r in client.query(sql).result()]
    logger.info(f"Hotspots returned {len(result)} locations")
    return result

@app.get("/county-comparison")
def county_comparison(state: str):
    span = get_current_span()
    span.set_attribute("business.state", state)
    
    logger.info(f"County comparison request for state: {state}")
    
    client = get_client()
    sql = f"""
        SELECT County, COUNT(ID) as accident_count, AVG(Severity) as avg_severity
        FROM `{TABLE}`
        WHERE State = '{state}'
        GROUP BY County
        ORDER BY accident_count DESC
    """
    result = [dict(r) for r in client.query(sql).result()]
    logger.info(f"County comparison returned {len(result)} counties")
    return result