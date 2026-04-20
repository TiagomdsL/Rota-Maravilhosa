import os
import json
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = FastAPI(title="Accident Prediction Service", version="1.0.0")

PROJECT = "proj1cc-493515"
TABLE = "proj1cc-493515.accidents.accidents"
LOCATION = "US"

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
    return {"status": "ok"}

@app.get("/ready")
async def ready():
    return {"status": "ok"}

@app.post("/accidents/predict-occurrence", response_model=PredictResponse)
def predict_occurrence(request: PredictRequest):
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
        return PredictResponse(accident_probability=round(prob, 4), risk_level=classify_risk(prob))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/accidents/simulate-risk", response_model=SimulateResponse)
def simulate_risk(request: SimulateRequest):
    try:
        road_factors = {'junction': 1.3, 'roundabout': 1.25, 'curve': 1.15,
                       'traffic_signal': 1.1, 'straight': 0.9}
        road_factor = road_factors.get(request.road_topology.lower(), 1.0)
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
        return SimulateResponse(probability_score=round(final_prob, 4),
                               predicted_severity=classify_risk(final_prob),
                               explanation=explanation)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
def get_stats():
    client = get_client()
    row = list(client.query(f"SELECT COUNT(*) as total FROM `{TABLE}`").result())[0]
    return {"total_accidents": row["total"]}
