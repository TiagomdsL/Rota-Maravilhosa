import os
import json
import logging
from fastapi import FastAPI, HTTPException, Query, Response, Request
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import time
from pydantic import BaseModel, Field
from typing import List
from google.cloud import bigquery
from google.oauth2 import service_account

from tracing import setup_tracing, get_current_span

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = FastAPI(title="Accident Prediction Service", version="1.0.0")
setup_tracing(app, "prediction-service-uc9-uc10")

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

SERVICE_NAME = "prediction-service-uc9-uc10"

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

def classify_risk(prob: float) -> str:
    if prob < 0.25: return "Low"
    elif prob < 0.5: return "Medium"
    elif prob < 0.75: return "High"
    else: return "Critical"

class PredictRequest(BaseModel):
    latitude: float
    longitude: float
    hour: int = Field(..., ge=0, le=23)
    weather_condition: str

class PredictResponse(BaseModel):
    accident_probability: float
    risk_level: str

class SimulateRequest(BaseModel):
    latitude: float
    longitude: float
    hour: int = Field(..., ge=0, le=23)
    weather_condition: str
    road_topology: str

class SimulateResponse(BaseModel):
    probability_score: float
    predicted_severity: str
    explanation: List[str]

@app.get("/health")
async def health():
    logger.info("Health check")
    return {"status": "ok"}

@app.get("/ready")
async def ready():
    return {"status": "ok"}

@app.post("/accidents/predict-occurrence", response_model=PredictResponse)
def predict_occurrence(request: PredictRequest):
    span = get_current_span()
    span.set_attribute("business.latitude", request.latitude)
    span.set_attribute("business.longitude", request.longitude)
    span.set_attribute("business.hour", request.hour)
    span.set_attribute("business.weather_condition", request.weather_condition)
    
    logger.info(f"Predict occurrence: lat={request.latitude}, lon={request.longitude}, hour={request.hour}")
    
    try:
        client = get_client()
        sql = f"""
            SELECT COUNT(*) as total,
            COUNTIF(EXTRACT(HOUR FROM Start_Time) = {request.hour}) as hour_count
            FROM `{TABLE}`
            WHERE LOWER(Weather_Condition) LIKE LOWER('%{request.weather_condition}%')
        """
        row = list(client.query(sql).result())[0]
        total = row["total"] or 1
        prob = min(0.9, max(0.01, row["hour_count"] / total))
        
        result = PredictResponse(accident_probability=round(prob, 4), risk_level=classify_risk(prob))
        span.set_attribute("business.accident_probability", result.accident_probability)
        
        logger.info(f"Prediction result: probability={prob}, level={result.risk_level}")
        return result
    except Exception as e:
        span.set_attribute("error", True)
        span.set_attribute("error.message", str(e))
        logger.error(f"Error in predict_occurrence: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/accidents/simulate-risk", response_model=SimulateResponse)
def simulate_risk(request: SimulateRequest):
    span = get_current_span()
    span.set_attribute("business.latitude", request.latitude)
    span.set_attribute("business.longitude", request.longitude)
    span.set_attribute("business.hour", request.hour)
    span.set_attribute("business.weather_condition", request.weather_condition)
    span.set_attribute("business.road_topology", request.road_topology)
    
    logger.info(f"Simulate risk: lat={request.latitude}, hour={request.hour}, road={request.road_topology}")
    
    try:
        road_factors = {'junction': 1.3, 'roundabout': 1.25, 'curve': 1.15,
                       'traffic_signal': 1.1, 'straight': 0.9}
        road_factor = road_factors.get(request.road_topology.lower(), 1.0)
        
        span.set_attribute("business.road_factor", road_factor)
        
        client = get_client()
        sql = f"""
            SELECT COUNT(*) as total,
            COUNTIF(EXTRACT(HOUR FROM Start_Time) = {request.hour}) as hour_count
            FROM `{TABLE}`
            WHERE LOWER(Weather_Condition) LIKE LOWER('%{request.weather_condition}%')
        """
        row = list(client.query(sql).result())[0]
        total = row["total"] or 1
        base_prob = min(0.9, max(0.01, row["hour_count"] / total))
        final_prob = min(0.95, base_prob * road_factor)
        
        explanation = []
        if base_prob > 0.06:
            explanation.append(f"Hour {request.hour}:00 has elevated accident rate")
        if road_factor > 1.2:
            explanation.append(f"{request.road_topology} is a high-risk road feature")
        
        result = SimulateResponse(probability_score=round(final_prob, 4),
                                 predicted_severity=classify_risk(final_prob),
                                 explanation=explanation)
        
        span.set_attribute("business.final_probability", result.probability_score)
        logger.info(f"Simulation result: probability={final_prob}, severity={result.predicted_severity}")
        
        return result
    except Exception as e:
        span.set_attribute("error", True)
        span.set_attribute("error.message", str(e))
        logger.error(f"Error in simulate_risk: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
def get_stats():
    logger.info("Stats request")
    client = get_client()
    row = list(client.query(f"SELECT COUNT(*) as total FROM `{TABLE}`").result())[0]
    return {"total_accidents": row["total"]}