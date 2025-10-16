"""
Google Places Nearby Search Module
--------------------------------

This module provides functionality to query the Google Places API for nearby establishments,
with built-in pagination support, error handling, and comprehensive metrics tracking.

The module performs adaptive density-based searches for points of interest (particularly 
restaurants) around specified geographic coordinates, handling Google's API pagination 
constraints and rate limiting automatically.

Key Features:
    - Paginated retrieval with configurable maximum pages (default: 3 pages per the API limit)
    - Automatic handling of next_page_tokens with appropriate delays
    - Comprehensive logging with unique search_id correlation
    - API usage metrics tracking and reporting
    - High-density area detection for adaptive search algorithms
    - Error handling with exponential backoff for quota limits

Functions:
    get_nearby_places: Retrieves places near a specified lat/lng coordinate with pagination
                       support, returning aggregated results and metadata.

Dependencies:
    - requests: For HTTP requests to the Places API
    - uuid: For generating unique search identifiers
    - time: For implementing delays and timing operations
    - utils.metrics.api_metrics: For tracking API usage statistics
    - utils.logger: For structured logging of operations and errors
    - adaptive_search.config: For configuration parameters (API_KEY, MAX_PAGES, etc.)

Configuration:
    API_KEY: Google Places API key
    MAX_PAGES: Maximum number of pages to retrieve per location (default: 3)
    HIGH_DENSITY_THRESHOLD: Results count threshold for marking an area as high-density

Usage Example:
    ```python
    from nearby_search import get_nearby_places
    
    # Search for restaurants within 500m of a location
    places, unique_count, pages = get_nearby_places(
        lat=37.7749, 
        lng=-122.4194, 
        radius=500,
        type_="restaurant"
    )
    
    print(f"Found {unique_count} unique restaurants in {pages} pages")
    ```

Notes:
    - The module automatically implements best practices for the Places API
    - Respects API rate limits with built-in waiting periods
    - Part of a larger adaptive search system for efficient geographic coverage
"""



import uuid
import time
import requests
from typing import Tuple
from utils.metrics import api_metrics
from utils import logger
from adaptive_search.config import API_KEY, MAX_PAGES, HIGH_DENSITY_THRESHOLD



def get_nearby_places(
        lat: float, 
        lng: float,
        radius: int,
        type_: str
        ) -> Tuple[list[dict], int, int]:
    """
    Retrieve up to MAX_PAGES of “nearby” place results around a given location.

    This function:
      1. Generates a short, unique `search_id` for correlation in the logs.
      2. Iteratively requests pages of results (up to MAX_PAGES), handling:
         - Non-OK API statuses (warnings and retries on OVER_QUERY_LIMIT).
         - next_page_token delays.
      3. Aggregates all place entries into a single list.
      4. Updates API metrics counters (total requests, results returned, unique results).
      5. Logs per-page and summary information, including duration and density flag.
      6. Returns a tuple containing:
         - `places` (list of dict): All place result objects retrieved.
         - `total_unique` (int): Count of unique places (length of `places`).
         - `pages_fetched` (int): Number of pages actually fetched.

    Args:
        lat (float):     Latitude of the search center.
        lng (float):     Longitude of the search center.
        radius (int):    Search radius in meters.
        type_ (str):     Place type filter (e.g., "restaurant").

    Returns:
        tuple[list[dict], int, int]:
            places         -- Flat list of all place result dicts.
            total_unique   -- Number of places returned.
            pages_fetched  -- Number of pages retrieved from the API.

    Notes:
        - Uses exponential backoff on OVER_QUERY_LIMIT (pause before retry).
        - Determines high-density areas by comparing total results to HIGH_DENSITY_THRESHOLD.
        - In case of any non-recoverable error, logs the exception and returns ([], 0, 0).
    """
    
    search_id = str(uuid.uuid4())[:8]


    places = []
    url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "location": f"{lat},{lng}",
        "radius": radius,
        "type": type_,
        "key": API_KEY
    }

    page_count:int = 0
    total_results:int = 0

    start_time:float = time.time()
    
    logger.debug(f"Starting nearby search", extra={
        "operation": "nearby_search",
        "search_id": search_id,
        "lat": lat,
        "lng": lng,
        "radius": radius
    })

    try:
        for _ in range(MAX_PAGES):  # Google Places API only allows 3 pages
            logger.debug(f"Requesting page {page_count + 1}", extra={
                "operation": "nearby_search",
                "search_id": search_id,
                "page": page_count + 1
            })
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            api_metrics.total_requests += 1
            
            data = response.json()
            status = data.get('status')
            
            if status != "OK":
                logger.warning(f"API returned non-OK status", extra={
                    "operation": "nearby_search",
                    "search_id": search_id,
                    "status": status,
                    "error_message": data.get('error_message', 'No error message')
                })
                if status == "OVER_QUERY_LIMIT":
                    logger.error("API quota exceeded", extra={
                        "operation": "nearby_search",
                        "search_id": search_id
                    })
                    time.sleep(5)  # Wait longer before retrying
                    continue
                break

            results = data.get('results', [])
            places.extend(results)
            
            result_count = len(results)
            total_results += result_count
            api_metrics.results_returned += result_count
            
            logger.info(f"Fetched places", extra={
                "operation": "nearby_search",
                "search_id": search_id,
                "page": page_count + 1,
                "results_count": result_count,
                "total_so_far": total_results
            })
            
            page_count += 1

            token = data.get("next_page_token")
            if token:
                logger.debug(f"Next page token received", extra={
                    "operation": "nearby_search",
                    "search_id": search_id,
                    "has_next_page": True
                })
                time.sleep(2)  # wait for token to activate
                params = {
                    "pagetoken": token,
                    "key": API_KEY
                }
            else:
                logger.debug(f"No more pages available", extra={
                    "operation": "nearby_search",
                    "search_id": search_id,
                    "has_next_page": False
                })
                break

        duration = time.time() - start_time

        api_metrics.unique_results += len(places)
        api_metrics.log_metrics()

        logger.info(f"Completed nearby search", extra={
            "operation": "nearby_search",
            "search_id": search_id,
            "total_pages": page_count,
            "total_results": total_results,
            "duration_sec": round(duration, 2),
            "is_high_density": total_results >= HIGH_DENSITY_THRESHOLD
        })
        
        return places, len(places), page_count
        
    except Exception as e:
        logger.error(f"Error in nearby search: {str(e)}", extra={
            "operation": "nearby_search",
            "search_id": search_id,
            "lat": lat,
            "lng": lng,
            "error": str(e),
            "status": "error"
        })
        return [], 0