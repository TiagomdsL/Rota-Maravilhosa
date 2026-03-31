import os
from fastapi import FastAPI, Query
import pandas as pd
from functools import lru_cache
from typing import Optional

app = FastAPI()

DATA_PATH = os.getenv("DATASET_PATH", "/app/dataset/US_Accidents_March23.csv")
_dataset_cache = None

def load_data():
    """
    Carrega o dataset com cache simples.
    A primeira chamada carrega o CSV, as subsequentes usam o cache.
    """
    global _dataset_cache
    
    if _dataset_cache is None:
        # Carrega apenas colunas necessárias para performance
        required_columns = [
            'City', 'County', 'State', 'Severity', 'ID', 
            'Start_Lat', 'Start_Lng'
        ]
        
        try:
            _dataset_cache = pd.read_csv(
                DATA_PATH,
                usecols=required_columns,
                low_memory=False,
                nrows = 500000
            )
        except ValueError:
            # Se algumas colunas não existirem, carrega todas
            _dataset_cache = pd.read_csv(DATA_PATH, low_memory=False)
    
    return _dataset_cache


@app.get("/hotspots")
def get_hotspots(
    city: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    limit: int = 10
):
    df = load_data()
    
    if city:
        df = df[df["City"] == city]
    
    if state:
        df = df[df["State"] == state]
    
    grouped = df.groupby(["Start_Lat", "Start_Lng"]).size().reset_index(name="count")
    result = grouped.sort_values("count", ascending=False).head(limit)
    
    return result.to_dict(orient="records")


@app.get("/county-comparison")
def county_comparison(state: str):
    df = load_data()
    df = df[df["State"] == state]
    
    grouped = df.groupby("County").agg(
        accident_count=("ID", "count"),
        avg_severity=("Severity", "mean")
    ).reset_index()
    
    return grouped.to_dict(orient="records")