"""
Data Service - Main FastAPI application for the 3 endpoints
"""

import sys
import os
from fastapi import FastAPI, HTTPException, Query
from typing import Optional
import logging

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from dataset_service import (
    load_dataset,
    get_dataset_info,
    get_statistics_by_state,
    analyze_by_weather,
    get_temporal_analysis
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Data Service",
    version="1.0.0",
    description="Service for querying US Accidents dataset"
)

# Dataset configuration
DATASET_PATH = os.getenv("DATASET_PATH", os.path.join(parent_dir, "dataset", "US_Accidents_March23.csv"))
SAMPLE_ROWS = os.getenv("SAMPLE_ROWS", None)  # For testing, e.g., "10000"

try:
    load_dataset(DATASET_PATH, max_rows=500000)  # ← APENAS O CAMINHO
    logger.info("Dataset loaded successfully")
except Exception as e:
    logger.error(f"Failed to load dataset: {e}")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return get_dataset_info()


@app.get("/accidents/statistics/by-state")
async def get_statistics(
    state: str = Query(..., description="US state name or code (e.g., CA, California)"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)")
):
    """
    Get accident statistics by state and date range.
    
    Returns total number of accidents and average severity.
    """
    try:
        return get_statistics_by_state(state, start_date, end_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Error in get_statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/accidents/weather-analysis")
async def weather_analysis(
    state: Optional[str] = Query(None, description="Filter by state name or code")
):
    """
    Analyze accidents by weather condition.
    
    Returns number of accidents and average severity grouped by weather condition.
    """
    try:
        return analyze_by_weather(state)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Error in weather_analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/accidents/temporal-analysis")
async def temporal_analysis(
    city: str = Query(..., description="City name"),
    day_of_week: Optional[str] = Query(None, description="Filter by day of week (Monday, Tuesday, etc.)")
):
    """
    Get accident frequency by hour of day.
    
    Returns accident count for each hour (0-23).
    """
    try:
        return get_temporal_analysis(city, day_of_week)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Error in temporal_analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))

