"""
Configuration module for the TikTok data collection pipeline.
"""

import os
from pathlib import Path
from typing import Dict, Any

# Base paths
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
DATA_DIR = Path(AIRFLOW_HOME) / 'data'
LOGS_DIR = Path(AIRFLOW_HOME) / 'logs'
VIDEOS_DIR = DATA_DIR / 'tiktok_videos'
TRANSCRIPTS_DIR = DATA_DIR / 'tiktok_transcripts'

# Create necessary directories
for directory in [DATA_DIR, LOGS_DIR, VIDEOS_DIR, TRANSCRIPTS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# API Configuration
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY')
if not RAPIDAPI_KEY:
    raise ValueError("RAPIDAPI_KEY environment variable is required")

# TikTok API Settings
TIKTOK_API_CONFIG = {
    'base_url': 'https://tiktok-api15.p.rapidapi.com',
    'headers': {
        'x-rapidapi-key': RAPIDAPI_KEY,
        'x-rapidapi-host': 'tiktok-api15.p.rapidapi.com'
    },
    'endpoints': {
        'search': '/index/Tiktok/searchVideoListByKeywords',
        'video_info': '/index/Tiktok/getVideoInfo'
    },
    'rate_limit': {
        'request_delay': 1,  # seconds between API calls
        'max_retries': 3,
        'retry_delay': 5
    }
}

# Search Settings
SEARCH_CONFIG = {
    'videos_per_request': 5,
    'max_videos_per_restaurant': 10,
    'relevance_threshold': 60,
    'min_likes': 1000,
    'max_comments': 1000
}

# Video Processing Settings
VIDEO_CONFIG = {
    'download_hd': True,
    'supported_formats': ['mp4', 'mov'],
    'max_duration_seconds': 600,
    'chunk_size': 8192
}

# Restaurant Data Settings
RESTAURANT_CONFIG = {
    'supported_formats': ['csv', 'parquet', 'feather'],
    'search_threshold': 70,
    'batch_size': 50
}

def get_config() -> Dict[str, Any]:
    """
    Get the current configuration as a dictionary.
    Useful for logging and debugging.
    """
    return {
        'paths': {
            'airflow_home': AIRFLOW_HOME,
            'data_dir': str(DATA_DIR),
            'logs_dir': str(LOGS_DIR),
            'videos_dir': str(VIDEOS_DIR),
            'transcripts_dir': str(TRANSCRIPTS_DIR)
        },
        'api': TIKTOK_API_CONFIG,
        'search': SEARCH_CONFIG,
        'video': VIDEO_CONFIG,
        'restaurant': RESTAURANT_CONFIG
    } 