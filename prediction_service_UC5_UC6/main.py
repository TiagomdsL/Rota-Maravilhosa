import os
import json
import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException
import httpx
from pydantic import BaseModel
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO)
app = FastAPI(title="Prediction Service")
DATA_SERVICE_URL = os.getenv("DATA_SERVICE_URL", "http://localhost:8001")

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
    return {"status": "ok"}

@app.get("/ready")
async def ready():
    return {"status": "ok"}

@app.post("/predict-severity")
def predict_severity(input_data: SeverityInput):
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
    return {"predicted_severity": round(avg, 2)}

@app.post("/risk/score")
async def calculate_risk_score(request: RiskRequest):
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
    return {"accident_probability": round(prob, 4), "predicted_severity": severity,
            "nearby_accidents_count": total}

