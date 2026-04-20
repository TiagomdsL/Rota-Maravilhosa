import os
import json
import logging
from fastapi import FastAPI, HTTPException, Query
from typing import Optional
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO)
app = FastAPI(title="Data Service", version="1.0.0")

PROJECT = "proj1cc-493515"
TABLE = "proj1cc-493515.accidents.accidents"
LOCATION = "US"

STATE_NAMES = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California",
    "CO":"Colorado","CT":"Connecticut","DE":"Delaware","FL":"Florida","GA":"Georgia",
    "HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa","KS":"Kansas",
    "KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts",
    "MI":"Michigan","MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana",
    "NE":"Nebraska","NV":"Nevada","NH":"New Hampshire","NJ":"New Jersey","NM":"New Mexico",
    "NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma",
    "OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina",
    "SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont",
    "VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming"
}
NAME_TO_CODE = {v: k for k, v in STATE_NAMES.items()}

def get_client():
    api_token = os.environ.get("API_TOKEN")
    if api_token:
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(api_token))
        return bigquery.Client(credentials=credentials, project=PROJECT, location=LOCATION)
    return bigquery.Client(project=PROJECT, location=LOCATION)

def normalize_state(state: str) -> str:
    state = state.strip()
    if len(state) == 2:
        return state.upper()
    if state in NAME_TO_CODE:
        return NAME_TO_CODE[state]
    raise ValueError(f"Invalid state: {state}")

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/ready")
async def ready():
    return {"status": "ok"}

@app.get("/accidents/statistics/by-state")
async def get_statistics(
    state: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...)
):
    try:
        state_code = normalize_state(state)
        client = get_client()
        sql = f"""
            SELECT COUNT(*) as total_accidents, AVG(Severity) as avg_severity
            FROM `{TABLE}`
            WHERE State = '{state_code}'
            AND Start_Time BETWEEN '{start_date}' AND '{end_date}'
        """
        row = list(client.query(sql).result())[0]
        return {
            "state": state_code,
            "state_name": STATE_NAMES.get(state_code, state_code),
            "total_accidents": row["total_accidents"],
            "avg_severity": round(row["avg_severity"] or 0, 2)
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/accidents/weather-analysis")
async def weather_analysis(state: Optional[str] = Query(None)):
    try:
        where = f"WHERE State = '{normalize_state(state)}'" if state else ""
        client = get_client()
        sql = f"""
            SELECT Weather_Condition, COUNT(*) as accident_count, AVG(Severity) as avg_severity
            FROM `{TABLE}`
            {where}
            GROUP BY Weather_Condition
            HAVING Weather_Condition IS NOT NULL
            ORDER BY accident_count DESC
        """
        return [{"weather_condition": r["Weather_Condition"],
                 "accident_count": int(r["accident_count"]),
                 "avg_severity": round(r["avg_severity"] or 0, 2)}
                for r in client.query(sql).result()]
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/accidents/temporal-analysis")
async def temporal_analysis(
    city: str = Query(...),
    day_of_week: Optional[str] = Query(None)
):
    day_filter = f"AND FORMAT_TIMESTAMP('%A', Start_Time) = '{day_of_week}'" if day_of_week else ""
    client = get_client()
    sql = f"""
        SELECT EXTRACT(HOUR FROM Start_Time) as hour, COUNT(*) as accident_count
        FROM `{TABLE}`
        WHERE LOWER(City) = LOWER('{city}')
        {day_filter}
        GROUP BY hour ORDER BY hour
    """
    hour_map = {int(r["hour"]): int(r["accident_count"])
                for r in client.query(sql).result()}
    return [{"hour": h, "accident_count": hour_map.get(h, 0)} for h in range(24)]
