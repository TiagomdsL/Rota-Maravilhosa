import os
import json
import logging
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = FastAPI()

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

class Accident(BaseModel):
    latitude: float
    longitude: float
    severity: int

class BoundingBoxResponse(BaseModel):
    accidents: List[Accident]

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/ready")
async def ready():
    return {"status": "ok"}

@app.post("/features")
def get_location_features(payload: dict):
    from datetime import datetime
    latitude = payload["latitude"]
    longitude = payload["longitude"]
    hour = datetime.fromisoformat(payload["timestamp"]).hour

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

    return {
        "total_accidents": row["total_accidents"],
        "avg_severity": round(row["avg_severity"] or 0, 2),
        "avg_visibility": row["avg_visibility"],
        "avg_precipitation": row["avg_precipitation"],
        "weather_distribution": weather_dist
    }

@app.get("/accidents/bounding-box", response_model=BoundingBoxResponse)
def get_accidents_bounding_box(
    min_lat: float, max_lat: float,
    min_lon: float, max_lon: float,
    limit: int = 100
):
    client = get_client()
    sql = f"""
        SELECT Start_Lat as latitude, Start_Lng as longitude, Severity as severity
        FROM `{TABLE}`
        WHERE Start_Lat BETWEEN {min_lat} AND {max_lat}
        AND Start_Lng BETWEEN {min_lon} AND {max_lon}
        LIMIT {limit}
    """
    rows = [dict(r) for r in client.query(sql).result()]
    return {"accidents": rows}
