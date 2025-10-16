from pathlib import Path
import os

# Base paths
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
DATA_DIR = Path(AIRFLOW_HOME) / 'data'
LOGS_DIR = Path(AIRFLOW_HOME) / 'logs'

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'restaurant_data'),
    'user': os.getenv('POSTGRES_USER', 'airflow'),
    'password': os.getenv('POSTGRES_PASSWORD', 'airflow')
}

# API configurations
GOOGLE_PLACES_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
TIKTOK_API_KEY = os.getenv('TIKTOK_API_KEY')

# Service-specific configurations
ADAPTIVE_SEARCH_CONFIG = {
    'initial_radius': 1000,
    'initial_step': 100,
    'min_step': 50,
    'high_density_threshold': 5,
    'chunk_size': 20,
    'location_type': 'restaurant',
    'max_deep_dives': 1000
}

PLACE_DETAILS_CONFIG = {
    'batch_size': 50,
    'max_retries': 3,
    'retry_delay': 5
}

VIDEO_PROCESSING_CONFIG = {
    'max_videos_per_batch': 10,
    'supported_formats': ['mp4', 'mov', 'avi'],
    'max_duration_seconds': 600
}

TIKTOK_CONFIG = {
    'max_videos': 100,
    'min_likes': 1000,
    'max_comments': 1000
}

# Data loading configuration
DATA_LOAD_CONFIG = {
    'batch_size': 1000,
    'max_retries': 3,
    'retry_delay': 5
}

# Create necessary directories
for directory in [DATA_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True) 