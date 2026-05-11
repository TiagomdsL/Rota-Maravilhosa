import os
import json
import logging
from fastapi import FastAPI, HTTPException, Query, Response, Request
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import time
from typing import Optional
from google.cloud import bigquery
from google.oauth2 import service_account

from tracing import setup_tracing, get_current_span

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = FastAPI(title="Data Service", version="1.0.0")
setup_tracing(app, "data-service-uc123")

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

SERVICE_NAME = "data-service-uc123"

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

def normalize_state(state: str) -> str:
    state = state.strip()
    if len(state) == 2:
        return state.upper()
    if state in NAME_TO_CODE:
        return NAME_TO_CODE[state]
    raise ValueError(f"Invalid state: {state}")

@app.get("/health")
async def health():
    logger.info("Health check")
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
    span = get_current_span()
    span.set_attribute("business.state", state)
    span.set_attribute("business.start_date", start_date)
    span.set_attribute("business.end_date", end_date)
    
    logger.info(f"Statistics request: state={state}, from={start_date}, to={end_date}")
    
    try:
        state_code = normalize_state(state)
        span.set_attribute("business.state_code", state_code)
        
        client = get_client()
        sql = f"""
            SELECT COUNT(*) as total_accidents, AVG(Severity) as avg_severity
            FROM `{TABLE}`
            WHERE State = '{state_code}'
            AND Start_Time BETWEEN '{start_date}' AND '{end_date}'
        """
        row = list(client.query(sql).result())[0]
        
        result = {
            "state": state_code,
            "state_name": STATE_NAMES.get(state_code, state_code),
            "total_accidents": row["total_accidents"],
            "avg_severity": round(row["avg_severity"] or 0, 2)
        }
        
        span.set_attribute("business.total_accidents", row["total_accidents"])
        logger.info(f"Statistics result: {result['total_accidents']} accidents, severity={result['avg_severity']}")
        
        return result
    except ValueError as e:
        span.set_attribute("error", True)
        span.set_attribute("error.message", str(e))
        logger.error(f"Validation error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/accidents/weather-analysis")
async def weather_analysis(state: Optional[str] = Query(None)):
    span = get_current_span()
    if state:
        span.set_attribute("business.state", state)
    
    logger.info(f"Weather analysis request for state: {state}")
    
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
        result = [{"weather_condition": r["Weather_Condition"],
                   "accident_count": int(r["accident_count"]),
                   "avg_severity": round(r["avg_severity"] or 0, 2)}
                  for r in client.query(sql).result()]
        
        logger.info(f"Weather analysis returned {len(result)} conditions")
        return result
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/accidents/temporal-analysis")
async def temporal_analysis(
    city: str = Query(...),
    day_of_week: Optional[str] = Query(None)
):
    span = get_current_span()
    span.set_attribute("business.city", city)
    if day_of_week:
        span.set_attribute("business.day_of_week", day_of_week)
    
    logger.info(f"Temporal analysis for city: {city}, day: {day_of_week}")
    
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
    result = [{"hour": h, "accident_count": hour_map.get(h, 0)} for h in range(24)]
    logger.info(f"Temporal analysis completed for {city}")
    return result