"""
Dataset loader - Reads from BigQuery instead of CSV
"""
import os
import json
import logging
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from typing import Optional

logger = logging.getLogger(__name__)

PROJECT = "proj1cc-493515"
TABLE = "proj1cc-493515.accidents.accidents"
LOCATION = "US"

_dataset_cache = None
_dataset_info = None

def get_bq_client():
    api_token = os.environ.get("API_TOKEN")
    if api_token:
        credentials_info = json.loads(api_token)
        client = bigquery.Client.from_service_account_info(credentials_info)
        return client
    return bigquery.Client(project=PROJECT, location=LOCATION)

def load_dataset(
    filepath: str = None,
    chunksize: int = 50000,
    use_cache: bool = True,
    max_rows: Optional[int] = 100000
) -> pd.DataFrame:
    global _dataset_cache, _dataset_info

    if use_cache and _dataset_cache is not None:
        logger.info(f"Returning cached dataset ({len(_dataset_cache)} rows)")
        return _dataset_cache

    logger.info(f"Loading dataset from BigQuery: {TABLE}")

    client = get_bq_client()
    limit = f"LIMIT {max_rows}" if max_rows else ""

    sql = f"""
        SELECT
            Start_Time, Severity, City, County, State,
            Weather_Condition, Start_Lat, Start_Lng,
            Visibility_mi_, Precipitation_in_
        FROM `{TABLE}`
        {limit}
    """

    df = client.query(sql).to_dataframe()
    logger.info(f"Loaded {len(df)} rows from BigQuery")

    # Renomear colunas
    df = df.rename(columns={
        'Start_Time': 'start_time',
        'Severity': 'severity',
        'City': 'city',
        'County': 'county',
        'State': 'state',
        'Weather_Condition': 'weather_condition',
        'Start_Lat': 'latitude',
        'Start_Lng': 'longitude',
        'Visibility_mi_': 'visibility',
        'Precipitation_in_': 'precipitation'
    })

    if 'start_time' in df.columns:
        df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    if 'weather_condition' in df.columns:
        df['weather_condition'] = df['weather_condition'].fillna('Unknown')
    if 'city' in df.columns:
        df['city'] = df['city'].fillna('Unknown')
    if 'county' in df.columns:
        df['county'] = df['county'].fillna('Unknown')

    _dataset_cache = df
    _dataset_info = {
        "rows": len(df),
        "columns": len(df.columns),
        "states": df['state'].nunique() if 'state' in df.columns else 0,
        "cities": df['city'].nunique() if 'city' in df.columns else 0,
    }

    logger.info(f"Dataset ready: {len(df)} rows")
    return df


def get_dataset() -> pd.DataFrame:
    if _dataset_cache is None:
        raise RuntimeError("Dataset not loaded")
    return _dataset_cache


def get_dataset_info() -> dict:
    if _dataset_cache is None:
        return {"status": "not_loaded"}
    return _dataset_info
