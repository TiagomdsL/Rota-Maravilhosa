import os
import json
import logging
from fastapi import FastAPI, Query
from typing import Optional
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO)
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

@app.get("/health")
async def health():
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
    return [dict(r) for r in client.query(sql).result()]

@app.get("/county-comparison")
def county_comparison(state: str):
    client = get_client()
    sql = f"""
        SELECT County, COUNT(ID) as accident_count, AVG(Severity) as avg_severity
        FROM `{TABLE}`
        WHERE State = '{state}'
        GROUP BY County
        ORDER BY accident_count DESC
    """
    return [dict(r) for r in client.query(sql).result()]
