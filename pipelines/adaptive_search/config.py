"""
Configuration module for the Adaptive Restaurant Search application.
-------------------------------------------

This module defines all of the tunable parameters, file paths, and environment-driven settings
used by the depth-first restaurant search.
"""

import os
from pathlib import Path
from typing import Dict, Any

# Base paths
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
DATA_DIR = Path(AIRFLOW_HOME) / 'data'
LOGS_DIR = Path(AIRFLOW_HOME) / 'logs'

# Create necessary directories
for directory in [DATA_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Search parameters
INITIAL_RADIUS = int(os.getenv('INITIAL_RADIUS', 1000))  # in meters
INITIAL_STEP = float(os.getenv('INITIAL_STEP', 0.01))  # approx 1.1km in degrees
MIN_STEP = float(os.getenv('MIN_STEP', 0.0025))  # Minimum step size for subdivision
TYPE = os.getenv('LOCATION_TYPE', 'restaurant')
HIGH_DENSITY_THRESHOLD = int(os.getenv('HIGH_DENSITY_THRESHOLD', 60))
MAX_RESULTS_PER_PAGE = int(os.getenv('MAX_RESULTS_PER_PAGE', 20))
MAX_PAGES = int(os.getenv('MAX_PAGES', 3))

# Checkpoint configuration
CKPT_DIR = DATA_DIR / 'checkpoints'
CKPT_DIR.mkdir(parents=True, exist_ok=True)
CKPT_FILE = CKPT_DIR / 'adaptive_search_checkpoint.ckpt'
CKPT_TMP_FILE = CKPT_DIR / 'adaptive_search_checkpoint.ckpt.tmp'

# Concurrency settings
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 10))
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 500))

# API configuration
API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
if not API_KEY:
    raise ValueError("GOOGLE_PLACES_API_KEY environment variable is required")

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
            'checkpoint_dir': str(CKPT_DIR)
        },
        'search': {
            'initial_radius': INITIAL_RADIUS,
            'initial_step': INITIAL_STEP,
            'min_step': MIN_STEP,
            'type': TYPE,
            'high_density_threshold': HIGH_DENSITY_THRESHOLD,
            'max_results_per_page': MAX_RESULTS_PER_PAGE,
            'max_pages': MAX_PAGES
        },
        'processing': {
            'max_workers': MAX_WORKERS,
            'chunk_size': CHUNK_SIZE
        }
    }