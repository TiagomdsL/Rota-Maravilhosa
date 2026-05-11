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

app = FastAPI(title="API Gateway")
setup_tracing(app, "api-gateway")

logger = logging.getLogger(__name__)

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

SERVICE_NAME = "api-gateway"

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

client = httpx.AsyncClient(timeout=30.0)
DATA_SERVICE_URL = os.getenv("DATA_SERVICE_URL", "http://localhost:8001")
PREDICTION_SERVICE_URL = os.getenv("PREDICTION_SERVICE_URL", "http://localhost:8002")
ROUTE_SERVICE_URL = os.getenv("ROUTE_SERVICE_URL", "http://localhost:8003")
DATA_SERVICE_URL_UC123 = os.getenv("DATA_SERVICE_URL_UC123", "http://localhost:8004")
DATA_SERVICE_UC8_UC11_URL = os.getenv("DATA_SERVICE_UC8_UC11_URL", "http://localhost:8007")
PREDICTION_SERVICE_UC9_UC10_URL = os.getenv("PREDICTION_SERVICE_UC9_UC10_URL", "http://localhost:8008")

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

async def forward_request(
    service_url: str,
    endpoint: str,
    params: dict = None
):
    url = f"{service_url}{endpoint}"

    try:
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()

    except httpx.TimeoutException:
        logger.error(f"Timeout calling {url}")
        raise HTTPException(status_code=504, detail="Service timeout")
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error from {url}: {e.response.status_code}")
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except Exception as e:
        logger.error(f"Error calling {url}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health():
    logger.info("Health check requested")
    return {"gateway": "ok"}

@app.get("/ready")
async def ready(request: Request):
    return {"status": "ready"}

@app.post("/accidents/predict-severity")
async def gateway_predict_severity(input_data: SeverityInput):
    span = get_current_span()
    span.set_attribute("business.visibility", input_data.visibility)
    span.set_attribute("business.precipitation", input_data.precipitation)
    span.set_attribute("business.weather_condition", input_data.weather_condition)
    
    logger.info(f"Predict severity request: visibility={input_data.visibility}, precipitation={input_data.precipitation}")
    
    try:
        response = await client.post(
            f"{PREDICTION_SERVICE_URL}/predict-severity",
            json=input_data.dict()
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    
@app.get("/accidents/bounding-box")
async def bounding_box(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    limit: int = 100
):
    span = get_current_span()
    span.set_attribute("business.min_lat", min_lat)
    span.set_attribute("business.max_lat", max_lat)
    span.set_attribute("business.min_lon", min_lon)
    span.set_attribute("business.max_lon", max_lon)
    span.set_attribute("business.limit", limit)
    
    logger.info(f"Bounding box request: ({min_lat},{min_lon}) to ({max_lat},{max_lon})")
    
    try:
        response = await client.get(
            f"{DATA_SERVICE_URL}/accidents/bounding-box",
            params={
                "min_lat": min_lat,
                "max_lat": max_lat,
                "min_lon": min_lon,
                "max_lon": max_lon,
                "limit": limit
            }
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/risk/score")
async def gateway_risk_score(body: RiskRequest):
    span = get_current_span()
    span.set_attribute("business.latitude", body.latitude)
    span.set_attribute("business.longitude", body.longitude)
    
    logger.info(f"Risk score request for location: ({body.latitude}, {body.longitude})")
    
    try:
        response = await client.post(
            f"{PREDICTION_SERVICE_URL}/risk/score",
            json={
                "latitude": body.latitude,
                "longitude": body.longitude,
                "timestamp": body.timestamp.isoformat(),
            }
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/route/analyze")
async def gateway_route_analyze(body: RouteRequest):
    span = get_current_span()
    span.set_attribute("business.origin_lat", body.origin_lat)
    span.set_attribute("business.origin_lon", body.origin_lon)
    span.set_attribute("business.destination_lat", body.destination_lat)
    span.set_attribute("business.destination_lon", body.destination_lon)
    
    logger.info(f"Route analysis from ({body.origin_lat},{body.origin_lon}) to ({body.destination_lat},{body.destination_lon})")
    
    try:
        response = await client.post(
            f"{ROUTE_SERVICE_URL}/route/analyze",
            json=body.dict()
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/hotspots")
async def get_hotspots(
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
    
    try:
        response = await client.get(
            f"{DATA_SERVICE_UC8_UC11_URL}/hotspots",
            params={"city": city, "state": state, "limit": limit}
        )
        response.raise_for_status()
        return response.json()

    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/county-comparison")
async def county_comparison(state: str):
    span = get_current_span()
    span.set_attribute("business.state", state)
    
    logger.info(f"County comparison request for state: {state}")
    
    try:
        response = await client.get(
            f"{DATA_SERVICE_UC8_UC11_URL}/county-comparison",
            params={"state": state}
        )
        response.raise_for_status()
        return response.json()

    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/accidents/predict-occurrence")
async def predict_occurrence(payload: PredictRequest):
    span = get_current_span()
    span.set_attribute("business.latitude", payload.latitude)
    span.set_attribute("business.longitude", payload.longitude)
    span.set_attribute("business.hour", payload.hour)
    span.set_attribute("business.weather_condition", payload.weather_condition)
    
    logger.info(f"Predict occurrence: lat={payload.latitude}, lon={payload.longitude}, hour={payload.hour}")
    
    try:
        response = await client.post(
            f"{PREDICTION_SERVICE_UC9_UC10_URL}/accidents/predict-occurrence",
            json=payload.dict()
        )
        response.raise_for_status()
        return response.json()

    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/accidents/simulate-risk")
async def simulate_risk(payload: SimulateRequest):
    span = get_current_span()
    span.set_attribute("business.latitude", payload.latitude)
    span.set_attribute("business.longitude", payload.longitude)
    span.set_attribute("business.hour", payload.hour)
    span.set_attribute("business.weather_condition", payload.weather_condition)
    span.set_attribute("business.road_topology", payload.road_topology)
    
    logger.info(f"Simulate risk: lat={payload.latitude}, hour={payload.hour}, road={payload.road_topology}")
    
    try:
        response = await client.post(
            f"{PREDICTION_SERVICE_UC9_UC10_URL}/accidents/simulate-risk",
            json=payload.dict()
        )
        response.raise_for_status()
        return response.json()

    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_stats():
    logger.info("Stats request")
    
    try:
        response = await client.get(f"{PREDICTION_SERVICE_UC9_UC10_URL}/stats")
        response.raise_for_status()
        return response.json()

    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/accidents/statistics/by-state")
async def get_accident_stats_by_state(
    state: str = Query(..., description="US state name or code"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)")
):
    span = get_current_span()
    span.set_attribute("business.state", state)
    span.set_attribute("business.start_date", start_date)
    span.set_attribute("business.end_date", end_date)
    
    logger.info(f"Statistics by state: {state} from {start_date} to {end_date}")
    
    return await forward_request(
        DATA_SERVICE_URL_UC123,
        "/accidents/statistics/by-state",
        params={"state": state, "start_date": start_date, "end_date": end_date}
    )

@app.get("/accidents/weather-analysis")
async def analyze_by_weather(
    state: Optional[str] = Query(None, description="Filter by state name or code")
):
    span = get_current_span()
    if state:
        span.set_attribute("business.state", state)
    
    logger.info(f"Weather analysis request for state: {state}")
    
    params = {"state": state} if state else {}
    return await forward_request(
        DATA_SERVICE_URL_UC123,
        "/accidents/weather-analysis",
        params=params
    )

@app.get("/accidents/temporal-analysis")
async def get_temporal_analysis(
    city: str = Query(..., description="City name"),
    day_of_week: Optional[str] = Query(None, description="Filter by day of week")
):
    span = get_current_span()
    span.set_attribute("business.city", city)
    if day_of_week:
        span.set_attribute("business.day_of_week", day_of_week)
    
    logger.info(f"Temporal analysis for city: {city}, day: {day_of_week}")
    
    params = {"city": city}
    if day_of_week:
        params["day_of_week"] = day_of_week

    return await forward_request(
        DATA_SERVICE_URL_UC123,
        "/accidents/temporal-analysis",
        params=params
    )