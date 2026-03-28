"""
Data repository - Query functions for the 3 endpoints
"""

import pandas as pd
from typing import Optional, List, Dict, Any
import logging

from dataset_service.loader import get_dataset

logger = logging.getLogger(__name__)


# State mapping
STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming"
}
NAME_TO_CODE = {v: k for k, v in STATE_NAMES.items()}


def normalize_state(state_input: str) -> str:
    """Convert state name to code (e.g., 'California' -> 'CA')"""
    state_input = state_input.strip()
    if len(state_input) == 2:
        return state_input.upper()
    if state_input in NAME_TO_CODE:
        return NAME_TO_CODE[state_input]
    raise ValueError(f"Invalid state: {state_input}")


def get_statistics_by_state(
    state: str,
    start_date: str,
    end_date: str
) -> Dict[str, Any]:
    """
    Get accident statistics for a state and date range.
    Endpoint: /accidents/statistics/by-state
    """
    df = get_dataset()
    
    state_code = normalize_state(state)
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    filtered = df[
        (df['state'] == state_code) &
        (df['start_time'] >= start_dt) &
        (df['start_time'] <= end_dt)
    ]
    
    total = len(filtered)
    avg_severity = filtered['severity'].mean() if total > 0 else 0
    
    return {
        "state": state_code,
        "state_name": STATE_NAMES.get(state_code, state_code),
        "total_accidents": total,
        "avg_severity": round(avg_severity, 2)
    }


def analyze_by_weather(state: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Analyze accident distribution by weather condition.
    Endpoint: /accidents/weather-analysis
    """
    df = get_dataset()
    
    filtered = df.copy()
    if state:
        state_code = normalize_state(state)
        filtered = filtered[filtered['state'] == state_code]
    
    result = filtered.groupby('weather_condition').agg(
        accident_count=('severity', 'count'),
        avg_severity=('severity', 'mean')
    ).reset_index()
    
    # Filter out Unknown
    result = result[result['weather_condition'] != 'Unknown']
    result = result.sort_values('accident_count', ascending=False)
    
    return [
        {
            "weather_condition": row['weather_condition'],
            "accident_count": int(row['accident_count']),
            "avg_severity": round(row['avg_severity'], 2)
        }
        for _, row in result.iterrows()
    ]


def get_temporal_analysis(
    city: str,
    day_of_week: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Get accident frequency by hour of day.
    Endpoint: /accidents/temporal-analysis
    """
    df = get_dataset()
    
    filtered = df[df['city'].str.lower() == city.lower()].copy()
    
    if filtered.empty:
        return [{"hour": h, "accident_count": 0} for h in range(24)]
    
    filtered['hour'] = filtered['start_time'].dt.hour
    filtered['day_name'] = filtered['start_time'].dt.day_name()
    
    if day_of_week:
        filtered = filtered[filtered['day_name'].str.lower() == day_of_week.lower()]
    
    result = filtered.groupby('hour').size().reset_index(name='accident_count')
    
    # Fill missing hours
    all_hours = pd.DataFrame({'hour': range(24)})
    result = all_hours.merge(result, on='hour', how='left').fillna(0)
    result['accident_count'] = result['accident_count'].astype(int)
    
    return [
        {"hour": int(row['hour']), "accident_count": int(row['accident_count'])}
        for _, row in result.iterrows()
    ]