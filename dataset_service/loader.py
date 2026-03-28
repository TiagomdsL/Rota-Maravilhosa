"""
Dataset loader - Handles loading and caching of the dataset
"""

import pandas as pd
import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    'Start_Time', 'Severity', 'City', 'County', 'State',
    'Weather_Condition', 'Start_Lat', 'Start_Lng'
]

COLUMN_MAPPING = {
    'Start_Time': 'start_time',
    'Severity': 'severity',
    'City': 'city',
    'County': 'county',
    'State': 'state',
    'Weather_Condition': 'weather_condition',
    'Start_Lat': 'latitude',
    'Start_Lng': 'longitude'
}

_dataset_cache = None
_dataset_info = None


def load_dataset(
    filepath: str,
    chunksize: int = 50000,
    use_cache: bool = True
) -> pd.DataFrame:
    """
    Load dataset using chunks to avoid memory issues
    """
    global _dataset_cache, _dataset_info
    
    if use_cache and _dataset_cache is not None:
        logger.info(f"Returning cached dataset ({len(_dataset_cache)} rows)")
        return _dataset_cache
    
    logger.info(f"Loading dataset from {filepath}")
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Dataset not found at {filepath}")
    
    # Carregar em chunks e concatenar
    chunks = []
    total_rows = 0
    
    logger.info("Reading CSV in chunks...")
    for i, chunk in enumerate(pd.read_csv(
        filepath,
        usecols=REQUIRED_COLUMNS,
        chunksize=chunksize,
        low_memory=False
    )):
        chunks.append(chunk)
        total_rows += len(chunk)
        logger.info(f"Loaded chunk {i+1}: {len(chunk)} rows (total: {total_rows})")
    
    logger.info(f"Concatenating {len(chunks)} chunks...")
    df = pd.concat(chunks, ignore_index=True)
    
    logger.info(f"Total rows loaded: {len(df)}")
    
    # Converter Start_Time
    if 'Start_Time' in df.columns:
        df['Start_Time'] = pd.to_datetime(df['Start_Time'], errors='coerce')
    
    # Renomear colunas
    rename_map = {k: v for k, v in COLUMN_MAPPING.items() if k in df.columns}
    df = df.rename(columns=rename_map)
    
    # Limpar dados
    if 'weather_condition' in df.columns:
        df['weather_condition'] = df['weather_condition'].fillna('Unknown')
        df['weather_condition'] = df['weather_condition'].replace('', 'Unknown')
    
    if 'city' in df.columns:
        df['city'] = df['city'].fillna('Unknown')
    
    if 'county' in df.columns:
        df['county'] = df['county'].fillna('Unknown')
    
    _dataset_cache = df
    _dataset_info = {
        "rows": len(df),
        "columns": len(df.columns),
        "column_names": list(df.columns),
        "states": df['state'].nunique() if 'state' in df.columns else 0,
        "cities": df['city'].nunique() if 'city' in df.columns else 0,
        "date_range": {
            "min": df['start_time'].min().isoformat() if 'start_time' in df.columns else None,
            "max": df['start_time'].max().isoformat() if 'start_time' in df.columns else None
        }
    }
    
    logger.info(f"Dataset loaded: {len(df)} rows")
    return df


def get_dataset() -> pd.DataFrame:
    """Get current dataset"""
    if _dataset_cache is None:
        raise RuntimeError("Dataset not loaded")
    return _dataset_cache


def get_dataset_info() -> dict:
    """Get dataset information"""
    if _dataset_cache is None:
        return {"status": "not_loaded"}
    return {"status": "loaded", **(_dataset_info or {})}