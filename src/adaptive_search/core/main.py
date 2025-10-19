#!/usr/bin/env python3
import logging
from typing import Dict, Any, Optional
from pathlib import Path

from .search import AdaptiveSearch
from ..config import (
    INITIAL_STEP,
    MIN_STEP,
    TYPE,
    HIGH_DENSITY_THRESHOLD,
    CHUNK_SIZE,
    DATA_DIR
)
from ..utils.logger import setup_logger

logger = setup_logger(__name__)

def main(
    city: str,
    initial_radius: float = 1000,
    initial_step: float = INITIAL_STEP,
    min_step: float = MIN_STEP,
    high_density_threshold: int = HIGH_DENSITY_THRESHOLD,
    chunk_size: int = CHUNK_SIZE,
    location_type: str = TYPE,
    max_deep_dives: int = 1000,
    phase: str = 'all',
    output_dir: Optional[Path] = None
) -> Dict[str, Any]:
    """
    Main entry point for the adaptive search pipeline.
    
    Args:
        city: Name of the city to search
        initial_radius: Initial search radius in meters
        initial_step: Initial step size for tile generation
        min_step: Minimum step size for tile generation
        high_density_threshold: Threshold for high-density areas
        chunk_size: Size of the chunk buffer for CSV writing
        location_type: Type of location to search for
        max_deep_dives: Maximum number of deep dives to perform
        phase: Search phase to run (all, initial, or deep)
        output_dir: Directory to save output files (defaults to DATA_DIR/city)
    
    Returns:
        Dict containing search results and metadata
    """
    try:
        # Set up output directory
        if output_dir is None:
            output_dir = DATA_DIR / city
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Starting Adaptive Search for {city}", extra={
            "city": city,
            "initial_radius": initial_radius,
            "initial_step": initial_step,
            "min_step": min_step,
            "high_density_threshold": high_density_threshold,
            "chunk_size": chunk_size,
            "location_type": location_type,
            "max_deep_dives": max_deep_dives,
            "phase": phase,
            "output_dir": str(output_dir)
        })
        
        # Initialize the AdaptiveSearch class
        search = AdaptiveSearch(
            city=city,
            initial_radius=initial_radius,
            initial_step=initial_step,
            min_step=min_step,
            high_density_threshold=high_density_threshold,
            chunk_size=chunk_size,
            location_type=location_type,
            output_dir=output_dir
        )
        
        # Set max deep dives if specified
        search.max_deep_dives = max_deep_dives
        
        # Run the appropriate phase(s)
        results = {}
        if phase == 'all':
            results = search.run()
        elif phase == 'initial':
            results = search.run_initial_scan()
        elif phase == 'deep':
            results = search.run_deep_dive()
        
        logger.info(f"Adaptive Search for {city} completed successfully")
        
        return {
            'status': 'success',
            'city': city,
            'results': results,
            'output_dir': str(output_dir)
        }
        
    except Exception as e:
        logger.error(f"Error in Adaptive Search: {str(e)}", exc_info=True)
        return {
            'status': 'error',
            'city': city,
            'error': str(e)
        }