"""
Checkpoint Management Module
------------------------------------------------------------------------------------
This module provides functionality for saving and loading search state checkpoints
during the adaptive search process for city restaurant data.

The module handles persistent storage of search progress, enabling:
- Resumption of interrupted searches
- Recovery from failures
- Tracking of search state across different cities

Key Components:
    - Comprehensive checkpoint saving for search state data
    - Checkpoint loading with error handling and logging
    - Atomic file operations to prevent data corruption
    - Utilities for flushing accumulated data to CSV files

Functions:
    save_comprehensive_checkpoint: Saves all search state data for a specific city
    save_search_state: Captures and saves the current search process state
    load_search_state: Loads previously saved checkpoint data for a city
    flush_chunk: Writes accumulated restaurant data to a CSV file

Constants:
    CKPT_FILE: Default checkpoint filename
    CKPT_TMP_FILE: Temporary file used during atomic checkpoint operations

Dependencies:
    - pickle: For serialization of checkpoint data
    - os: For file system operations
    - pandas: For data manipulation and CSV writing
    - datetime, time: For timestamp management
    - utils.logger: For operation logging

Version: 1.0.1
"""


import pickle
import os
import pandas as pd
from datetime import datetime
import time

from utils import logger

CKPT_FILE      = "adaptive_search.ckpt"
CKPT_TMP_FILE  = CKPT_FILE + ".tmp"


ts = datetime.now().strftime("%Y%m%d")

# ----------------------------------------------------------------------------------------------------------

def save_comprehensive_checkpoint(city: str, state_data: dict):
    """
    Save a comprehensive checkpoint of the current search state for a specific city.
    
    Args:
        city: Name of the city being processed
        state_data: Dictionary containing all state data to checkpoint
    """
    checkpoint_filename = f"data/checkpoints/checkpoint_{city.lower()}_{ts}.ckpt"
    tmp_filename = checkpoint_filename + ".tmp"
    
    try:
        # Write to a temporary file first to avoid corruption if the process is interrupted
        with open(tmp_filename, "wb") as f:
            pickle.dump(state_data, f)
        
        # Atomic replacement of the checkpoint file
        os.replace(tmp_filename, checkpoint_filename)
        
        logger.info(f"Saved comprehensive checkpoint", extra={
            "operation": "save_checkpoint",
            "city": city,
            "checkpoint_size_bytes": os.path.getsize(checkpoint_filename),
            "saved_components": list(state_data.keys())
        })
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {str(e)}", extra={
            "operation": "save_checkpoint",
            "city": city,
            "error": str(e)
        })
        if os.path.exists(tmp_filename):
            os.remove(tmp_filename)

# ----------------------------------------------------------------------------------------------------------

def save_search_state(
        city:str,
        initial_tiles:list, 
        high_density_stack:list, 
        processed:set, 
        seen_place_ids:set, 
        deep_count:int
        ) -> None:
    
    state_data = {
        "timestamp": time.time(),
        "city": city,
        "initial_tiles": initial_tiles,  # All tiles covering the city
        "high_density_stack": high_density_stack,  # Tiles needing further subdivision
        "processed": processed,  # Set of already processed tile keys
        "seen_place_ids": seen_place_ids,  # Set of unique place IDs found
        "deep_count": deep_count,  # Count of deep dives performed
        "version": "1.0.1"  # Version of the checkpoint format for compatibility checks
    }
    
    save_comprehensive_checkpoint(city, state_data)

# ----------------------------------------------------------------------------------------------------------

# Add directory argument to the function signature
def load_search_state(city: str, date_tag:str='') -> dict:
    """Load checkpoint data for a city if it exists."""
    checkpoint_filename = f"checkpoint_{city.lower()}{date_tag}.ckpt"
    
    if not os.path.exists(checkpoint_filename):
        logger.info(f"No checkpoint found for {city}", extra={
            "operation": "load_checkpoint",
            "city": city,
            "status": "not_found"
        })
        return None
    
    try:
        with open(checkpoint_filename, "rb") as f:
            state_data = pickle.load(f)
        
        logger.info(f"Loaded checkpoint for {city}", extra={
            "operation": "load_checkpoint",
            "city": city,
            "components": list(state_data.keys()),
            "checkpoint_age_seconds": time.time() - state_data.get("timestamp", 0),
            "status": "success"
        })
        
        return state_data
    except Exception as e:
        logger.error(f"Failed to load checkpoint: {str(e)}", extra={
            "operation": "load_checkpoint",
            "city": city,
            "error": str(e),
            "status": "error"
        })
        return None

# ----------------------------------------------------------------------------------------------------------

def flush_chunk(city: str, chunk_buffer: list) -> None:
    if not chunk_buffer:
        return  # nothing to write

    df = pd.DataFrame(chunk_buffer)

    mode = 'a' if os.path.exists(f"{city.lower()}_restaurants.csv") else 'w'
    header = (mode == 'w')
    df.to_csv(f"{city.lower()}_restaurants.csv", mode=mode, header=header, index=False)

    chunk_buffer.clear()
    return None

# ----------------------------------------------------------------------------------------------------------
