"""
Dataset Service - Centralized data access module
"""

from dataset_service.loader import (
    load_dataset,
    get_dataset,
    get_dataset_info
)

from dataset_service.repository import (
    get_statistics_by_state,
    analyze_by_weather,
    get_temporal_analysis
)

__all__ = [
    'load_dataset',
    'get_dataset',
    'get_dataset_info',
    'get_statistics_by_state',
    'analyze_by_weather',
    'get_temporal_analysis'
]