# data_service/main.py
import os
from typing import List
from fastapi import FastAPI
from pydantic import BaseModel
from dataset_service.repository import get_dataset
from dataset_service.loader import load_dataset
import pandas as pd

app = FastAPI()

dataset_path = os.path.join("dataset", "US_Accidents_March23.csv")
load_dataset(dataset_path, max_rows=500000)

class Accident(BaseModel):
    latitude: float
    longitude: float
    severity: int

class BoundingBoxResponse(BaseModel):
    accidents: List[Accident]

@app.post("/features")
def get_location_features(payload: dict):
    """
    Retorna features históricas de acidentes para PredictionService.
    Input: latitude, longitude, timestamp
    """
    df = get_dataset()
    latitude = payload["latitude"]
    longitude = payload["longitude"]
    timestamp = pd.to_datetime(payload["timestamp"])
    hour = timestamp.hour

    # Filtrar acidentes próximos (±0.01 graus ~1 km)
    nearby = df[
        (df["latitude"].between(latitude - 0.01, latitude + 0.01)) &
        (df["longitude"].between(longitude - 0.01, longitude + 0.01))
    ]
    nearby["hour"] = nearby["start_time"].dt.hour
    nearby = nearby[nearby["hour"] == hour]

    total_accidents = len(nearby)
    avg_severity = nearby["severity"].mean() if total_accidents > 0 else 0

    # Condições ambientais médias
    avg_visibility = nearby["visibility"].mean() if "visibility" in nearby else None
    avg_precipitation = nearby["precipitation"].mean() if "precipitation" in nearby else None
    weather_counts = nearby["weather_condition"].value_counts().to_dict()

    return {
        "total_accidents": total_accidents,
        "avg_severity": round(avg_severity, 2),
        "avg_visibility": avg_visibility,
        "avg_precipitation": avg_precipitation,
        "weather_distribution": weather_counts
    }

@app.get("/accidents/bounding-box", response_model=BoundingBoxResponse)
def get_accidents_bounding_box(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    limit: int = 100 
):
    df = get_dataset()

    filtered = df[
        (df["latitude"].between(min_lat, max_lat)) &
        (df["longitude"].between(min_lon, max_lon))
    ]
    result = filtered[["latitude", "longitude", "severity"]].head(limit)

    return {
        "accidents": result.to_dict(orient="records")
    }