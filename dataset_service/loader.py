


import pandas as pd
import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Colunas necessárias para a aplicação
REQUIRED_COLUMNS = [
    'Start_Time', 'Severity', 'City', 'County', 'State',
    'Weather_Condition', 'Lat', 'Lng'
]

# Mapeamento de colunas (caso o ficheiro tenha nomes diferentes)
COLUMN_MAPPING = {
    'Start_Time': 'start_time',
    'Severity': 'severity',
    'City': 'city',
    'County': 'county',
    'State': 'state',
    'Weather_Condition': 'weather_condition',
    'Lat': 'latitude',
    'Lng': 'longitude'
}

# Cache do dataset (carregado uma vez)
_dataset_cache = None
_dataset_info = None


def load_dataset(
    filepath: str,
    nrows: Optional[int] = None,
    use_cache: bool = True
) -> pd.DataFrame:
    """
    Load dataset from CSV file.
    
    Args:
        filepath: Path to CSV file
        nrows: Number of rows to load (None = all)
        use_cache: Use cached dataset if available
    
    Returns:
        DataFrame with loaded data
    """
    global _dataset_cache
    
    # Return cached version if available and requested
    if use_cache and _dataset_cache is not None:
        logger.info(f"Returning cached dataset ({len(_dataset_cache)} rows)")
        return _dataset_cache
    
    logger.info(f"Loading dataset from {filepath}")
    
    # Load only required columns for memory efficiency
    df = pd.read_csv(
        filepath,
        usecols=REQUIRED_COLUMNS,
        parse_dates=['Start_Time'],
        nrows=nrows,
        low_memory=False
    )
    
    # Rename columns for consistency
    df = df.rename(columns=COLUMN_MAPPING)
    
    # Convert Start_Time to datetime
    df['start_time'] = pd.to_datetime(df['start_time'])
    
    # Clean weather condition (remove empty values)
    df['weather_condition'] = df['weather_condition'].fillna('Unknown')
    df['weather_condition'] = df['weather_condition'].replace('', 'Unknown')
    
    # Clean city and county
    df['city'] = df['city'].fillna('Unknown')
    df['county'] = df['county'].fillna('Unknown')
    
    # Cache the dataset
    _dataset_cache = df
    
    logger.info(f"Dataset loaded: {len(df)} rows, {len(df.columns)} columns")
    
    return df


def get_dataset() -> pd.DataFrame:
    """
    Get the current dataset (must be loaded first).
    Raises exception if not loaded.
    """
    if _dataset_cache is None:
        raise RuntimeError("Dataset not loaded. Call load_dataset() first.")
    return _dataset_cache


def get_dataset_info() -> dict:
    """
    Get information about the loaded dataset.
    """
    global _dataset_info
    
    if _dataset_cache is None:
        return {"status": "not_loaded"}
    
    return {
        "status": "loaded",
        "rows": len(_dataset_cache),
        "columns": len(_dataset_cache.columns),
        "column_names": list(_dataset_cache.columns),
        "states": _dataset_cache['state'].nunique(),
        "cities": _dataset_cache['city'].nunique(),
        "date_range": {
            "min": _dataset_cache['start_time'].min().isoformat(),
            "max": _dataset_cache['start_time'].max().isoformat()
        }
    }


def reload_dataset(filepath: str, nrows: Optional[int] = None) -> pd.DataFrame:
    """
    Force reload of dataset (clears cache).
    """
    global _dataset_cache
    _dataset_cache = None
    return load_dataset(filepath, nrows)