import os
import json
import time
import logging
from typing import Optional

from fastapi import FastAPI, Query, Response, Request
from google.cloud import bigquery
from google.oauth2 import service_account
from prometheus_client import (
    Counter,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST
)

from tracing import setup_tracing, get_current_span

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

setup_tracing(app, "data-service-uc8-uc11")

PROJECT = "proj1cc-493515"
TABLE = "proj1cc-493515.accidents.accidents"
LOCATION = "US"

SERVICE_NAME = "data-service-uc8-uc11"

# =============================================================================
# PROMETHEUS METRICS
# =============================================================================

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

# =============================================================================
# MIDDLEWARE
# =============================================================================

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

# =============================================================================
# METRICS ENDPOINT
# =============================================================================

@app.get("/metrics")
def metrics():
    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )

# =============================================================================
# BIGQUERY CLIENT
# =============================================================================

def get_client():
    api_token = os.environ.get("API_TOKEN")

    if api_token:
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(api_token)
        )

        return bigquery.Client(
            credentials=credentials,
            project=PROJECT,
            location=LOCATION
        )

    return bigquery.Client(
        project=PROJECT,
        location=LOCATION
    )

# =============================================================================
# HEALTHCHECKS
# =============================================================================

@app.get("/health")
async def health():
    logger.info("Health check")
    return {"status": "ok"}

@app.get("/ready")
async def ready():
    return {"status": "ok"}

# =============================================================================
# BUSINESS LOGIC
# =============================================================================

def build_hotspots_query(
    city: Optional[str],
    state: Optional[str],
    limit: int
):
    filters = []

    if city:
        filters.append(f"City = '{city}'")

    if state:
        filters.append(f"State = '{state}'")

    where = f"WHERE {' AND '.join(filters)}" if filters else ""

    return f"""
        SELECT
            Start_Lat,
            Start_Lng,
            COUNT(*) as count
        FROM `{TABLE}`
        {where}
        GROUP BY Start_Lat, Start_Lng
        ORDER BY count DESC
        LIMIT {limit}
    """

def fetch_hotspots(
    city: Optional[str],
    state: Optional[str],
    limit: int
):
    logger.info(
        f"Fetching hotspots: city={city}, state={state}, limit={limit}"
    )

    client = get_client()

    sql = build_hotspots_query(city, state, limit)

    result = [dict(r) for r in client.query(sql).result()]

    logger.info(f"Hotspots returned {len(result)} locations")

    return result

def build_county_comparison_query(state: str):
    return f"""
        SELECT
            County,
            COUNT(ID) as accident_count,
            AVG(Severity) as avg_severity
        FROM `{TABLE}`
        WHERE State = '{state}'
        GROUP BY County
        ORDER BY accident_count DESC
    """

def fetch_county_comparison(state: str):
    logger.info(f"Fetching county comparison for state={state}")

    client = get_client()

    sql = build_county_comparison_query(state)

    result = [dict(r) for r in client.query(sql).result()]

    logger.info(f"County comparison returned {len(result)} counties")

    return result

# =============================================================================
# ENDPOINTS
# =============================================================================

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

    return fetch_hotspots(city, state, limit)

@app.get("/county-comparison")
def county_comparison(state: str):
    span = get_current_span()

    span.set_attribute("business.state", state)

    return fetch_county_comparison(state)