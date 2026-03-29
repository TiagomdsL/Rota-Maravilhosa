from datetime import datetime
from typing import Optional  
from fastapi import FastAPI, HTTPException, Query, logger
from pydantic import BaseModel
import httpx
import os

app = FastAPI(title="API Gateway")

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
    """
    Forward GET request to a microservice with error handling.
    """
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
    return {"gateway": "ok"}
 
@app.post("/accidents/predict-severity")
async def gateway_predict_severity(input_data: SeverityInput):
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
    limit : int = 100
):
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
    """
    UC6 – Proxy para o prediction_service calcular o risk score.
    """
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
    state: str = Query(..., description="US state name or code (e.g., CA, California)"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)")
):
    """
    Retrieve accident statistics by state and date range.

    Returns total number of accidents and average severity.
    """
    return await forward_request(
        DATA_SERVICE_URL_UC123,
        "/accidents/statistics/by-state",
        params={"state": state, "start_date": start_date, "end_date": end_date}
    )


@app.get("/accidents/weather-analysis")
async def analyze_by_weather(
    state: Optional[str] = Query(None, description="Filter by state name or code")
):
    """
    Analyze accidents by weather condition.

    Returns number of accidents and average severity grouped by weather condition.
    """
    params = {"state": state} if state else {}
    return await forward_request(
        DATA_SERVICE_URL_UC123,
        "/accidents/weather-analysis",
        params=params
    )


@app.get("/accidents/temporal-analysis")
async def get_temporal_analysis(
    city: str = Query(..., description="City name"),
    day_of_week: Optional[str] = Query(None, description="Filter by day of week (Monday, Tuesday, etc.)")
):
    """
    Temporal risk analysis.

    Returns accident frequency per hour of the day for a specific city.
    """
    params = {"city": city}
    if day_of_week:
        params["day_of_week"] = day_of_week

    return await forward_request(
        DATA_SERVICE_URL_UC123,
        "/accidents/temporal-analysis",
        params=params
    )

   