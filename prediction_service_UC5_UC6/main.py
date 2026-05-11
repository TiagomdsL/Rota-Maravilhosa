import os
import json
import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query, Response, Request
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import time
import httpx
from pydantic import BaseModel
from google.cloud import bigquery
from google.oauth2 import service_account

from tracing import setup_tracing, get_current_span

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = FastAPI(title="Prediction Service")
setup_tracing(app, "prediction-service-uc5-uc6")

DATA_SERVICE_URL = os.getenv("DATA_SERVICE_URL", "http://localhost:8001")

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

SERVICE_NAME = "prediction-service-uc5-uc6"

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

class SeverityInput(BaseModel):
    visibility: float
    precipitation: float
    weather_condition: str

class RiskRequest(BaseModel):
    latitude: float
    longitude: float
    timestamp: datetime

@app.get("/health")
async def health():
    logger.info("Health check")
    return {"status": "ok"}

@app.get("/ready")
async def ready():
    return {"status": "ok"}

@app.post("/predict-severity")
def predict_severity(input_data: SeverityInput):
    span = get_current_span()
    span.set_attribute("business.visibility", input_data.visibility)
    span.set_attribute("business.precipitation", input_data.precipitation)
    span.set_attribute("business.weather_condition", input_data.weather_condition)
    
    logger.info(f"Predict severity: visibility={input_data.visibility}, precip={input_data.precipitation}")
    
    client = get_client()
    sql = f"""
        SELECT AVG(Severity) as avg_severity
        FROM `{TABLE}`
        WHERE Visibility_mi_ BETWEEN {input_data.visibility-0.1} AND {input_data.visibility+0.1}
        AND Precipitation_in_ BETWEEN {input_data.precipitation-0.01} AND {input_data.precipitation+0.01}
        AND LOWER(Weather_Condition) = LOWER('{input_data.weather_condition}')
    """
    rows = list(client.query(sql).result())
    avg = rows[0]["avg_severity"] if rows else None
    if avg is None:
        global_rows = list(client.query(f"SELECT AVG(Severity) as avg_severity FROM `{TABLE}`").result())
        avg = global_rows[0]["avg_severity"] if global_rows else 2.0
    
    result = {"predicted_severity": round(avg, 2)}
    span.set_attribute("business.predicted_severity", result["predicted_severity"])
    
    logger.info(f"Predicted severity: {result['predicted_severity']}")
    return result

@app.post("/risk/score")
async def calculate_risk_score(request: RiskRequest):
    span = get_current_span()
    span.set_attribute("business.latitude", request.latitude)
    span.set_attribute("business.longitude", request.longitude)
    span.set_attribute("business.timestamp", request.timestamp.isoformat())
    
    logger.info(f"Risk score for location: ({request.latitude}, {request.longitude})")
    
    async with httpx.AsyncClient() as http_client:
        try:
            response = await http_client.post(
                f"{DATA_SERVICE_URL}/features",
                json={"latitude": request.latitude, "longitude": request.longitude,
                      "timestamp": request.timestamp.isoformat()})
            response.raise_for_status()
            features = response.json()
        except httpx.HTTPStatusError:
            features = {"total_accidents": 0, "avg_severity": 0.0,
                       "avg_visibility": None, "avg_precipitation": None, "weather_distribution": {}}
        except httpx.HTTPError as e:
            raise HTTPException(status_code=502, detail=f"Data service unavailable: {str(e)}")

    total = features["total_accidents"]
    avg_severity = features["avg_severity"]
    prob = min(total / 10, 1.0)
    if features.get("avg_visibility") and features["avg_visibility"] < 5.0:
        prob = min(prob * 1.3, 1.0)
    if features.get("avg_precipitation") and features["avg_precipitation"] > 0.1:
        prob = min(prob * 1.2, 1.0)
    severity = 1 if total == 0 or avg_severity == 0 else max(1, min(4, round(avg_severity)))
    
    result = {"accident_probability": round(prob, 4), "predicted_severity": severity,
            "nearby_accidents_count": total}
    
    span.set_attribute("business.accident_probability", result["accident_probability"])
    span.set_attribute("business.predicted_severity", result["predicted_severity"])
    
    logger.info(f"Risk score result: probability={prob}, severity={severity}")
    return result