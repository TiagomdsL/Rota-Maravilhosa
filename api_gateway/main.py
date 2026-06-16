from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request, Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import time
from pydantic import BaseModel
import httpx
import os
import logging

from tracing import setup_tracing, get_current_span

# ─────────────────────────────
# APP INIT
# ─────────────────────────────

app = FastAPI(title="API Gateway")
setup_tracing(app, "api-gateway")
logger = logging.getLogger(__name__)

# ─────────────────────────────
# HTTP CLIENT
# ─────────────────────────────

client = httpx.AsyncClient(timeout=30.0)

DATA_SERVICE_URL = os.getenv("DATA_SERVICE_URL", "http://localhost:8001")
PREDICTION_SERVICE_URL = os.getenv("PREDICTION_SERVICE_URL", "http://localhost:8002")
ROUTE_SERVICE_URL = os.getenv("ROUTE_SERVICE_URL", "http://localhost:8003")
DATA_SERVICE_URL_UC123 = os.getenv("DATA_SERVICE_URL_UC123", "http://localhost:8004")
DATA_SERVICE_UC8_UC11_URL = os.getenv("DATA_SERVICE_UC8_UC11_URL", "http://localhost:8007")
PREDICTION_SERVICE_UC9_UC10_URL = os.getenv("PREDICTION_SERVICE_UC9_UC10_URL", "http://localhost:8008")

# ─────────────────────────────
# METRICS
# ─────────────────────────────

REQUEST_COUNT = Counter("requests_total", "Total requests", ["service", "endpoint"])
REQUEST_LATENCY = Histogram("request_latency_seconds", "Request latency", ["service"])
ERROR_COUNT = Counter("requests_errors_total", "Total errors", ["service"])
SERVICE_NAME = "api-gateway"

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

# ─────────────────────────────
# MODELS
# ─────────────────────────────

class SeverityInput(BaseModel):
    visibility: float
    precipitation: float
    weather_condition: str

class RiskRequest(BaseModel):
    latitude: float
    longitude: float
    timestamp: datetime

class RouteRequest(BaseModel):
    origin_lat: float
    origin_lon: float
    destination_lat: float
    destination_lon: float
    timestamp: str = "2016-02-08T08:00:00"

class PredictRequest(BaseModel):
    latitude: float
    longitude: float
    hour: int
    weather_condition: str

class SimulateRequest(BaseModel):
    latitude: float
    longitude: float
    hour: int
    weather_condition: str
    road_topology: str

# ─────────────────────────────
# ROUTES
# ─────────────────────────────

@app.get("/health")
async def health():
    return {"gateway": "ok"}

@app.get("/ready")
async def ready():
    return {"status": "ready"}

@app.post("/accidents/predict-severity")
async def gateway_predict_severity(input_data: SeverityInput):
    span = get_current_span()
    span.set_attribute("business.weather_condition", input_data.weather_condition)
    try:
        response = await client.post(f"{PREDICTION_SERVICE_URL}/predict-severity", json=input_data.dict())
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/accidents/bounding-box")
async def bounding_box(min_lat: float, max_lat: float, min_lon: float, max_lon: float, limit: int = 100):
    response = await client.get(f"{DATA_SERVICE_URL}/accidents/bounding-box",
        params={"min_lat": min_lat, "max_lat": max_lat, "min_lon": min_lon, "max_lon": max_lon, "limit": limit})
    return response.json()

@app.post("/risk/score")
async def gateway_risk_score(body: RiskRequest):
    response = await client.post(f"{PREDICTION_SERVICE_URL}/risk/score",
        json={"latitude": body.latitude, "longitude": body.longitude, "timestamp": body.timestamp.isoformat()})
    return response.json()

@app.post("/route/analyze")
async def gateway_route_analyze(body: RouteRequest):
    response = await client.post(f"{ROUTE_SERVICE_URL}/route/analyze", json=body.dict())
    return response.json()

@app.get("/analytics/hotspots")
async def get_hotspots(city: Optional[str] = Query(None), state: Optional[str] = Query(None), limit: int = 10):
    response = await client.get(f"{DATA_SERVICE_UC8_UC11_URL}/hotspots",
        params={"city": city, "state": state, "limit": limit})
    return response.json()

@app.get("/analytics/county-comparison")
async def county_comparison(state: str):
    response = await client.get(f"{DATA_SERVICE_UC8_UC11_URL}/county-comparison", params={"state": state})
    return response.json()

@app.post("/accidents/predict-occurrence")
async def predict_occurrence(payload: PredictRequest):
    response = await client.post(f"{PREDICTION_SERVICE_UC9_UC10_URL}/accidents/predict-occurrence", json=payload.dict())
    return response.json()

@app.post("/accidents/simulate-risk")
async def simulate_risk(payload: SimulateRequest):
    response = await client.post(f"{PREDICTION_SERVICE_UC9_UC10_URL}/accidents/simulate-risk", json=payload.dict())
    return response.json()

@app.get("/stats")
async def get_stats():
    response = await client.get(f"{PREDICTION_SERVICE_UC9_UC10_URL}/stats")
    return response.json()

@app.get("/accidents/statistics/by-state")
async def get_accident_stats_by_state(state: str, start_date: str, end_date: str):
    response = await client.get(f"{DATA_SERVICE_URL_UC123}/accidents/statistics/by-state",
        params={"state": state, "start_date": start_date, "end_date": end_date})
    return response.json()

@app.get("/accidents/weather-analysis")
async def analyze_by_weather(state: Optional[str] = None):
    response = await client.get(f"{DATA_SERVICE_URL_UC123}/accidents/weather-analysis", params={"state": state})
    return response.json()

@app.get("/accidents/temporal-analysis")
async def get_temporal_analysis(city: str, day_of_week: Optional[str] = None):
    params = {"city": city}
    if day_of_week:
        params["day_of_week"] = day_of_week
    response = await client.get(f"{DATA_SERVICE_URL_UC123}/accidents/temporal-analysis", params=params)
    return response.json()
