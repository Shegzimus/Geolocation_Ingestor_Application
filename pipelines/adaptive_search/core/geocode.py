"""
Module for retrieving the geographic center of a city via the Google Maps Geocoding API.
---------------------------------
Logs each lookup with a unique search ID, tracks API usage metrics, and reports errors
in a structured, JSON-formatted log.

Functions:
    get_city_center(city: str) -> tuple[float, float, dict]:
        Perform a geocoding request for the given city name, log the request and outcome,
        and return the latitude, longitude, and viewport bounds.
"""

import uuid
import requests

from utils import logger
from utils.metrics import api_metrics
from config import API_KEY


def get_city_center(city: str) -> tuple[float, float, dict]:
    """
    Query Google Maps Geocoding API to find the center point of a city.

    This function:
      1. Generates a short, unique `search_id` for tracing.
      2. Logs the start of the geocode operation at INFO level.
      3. Makes an HTTP GET to the Geocoding endpoint.
      4. Increments the global API request counter.
      5. Parses the first result’s `location` (lat/lng) and `viewport`.
      6. Logs success or failure, including coordinates or error details.
      7. Returns a tuple of (latitude, longitude, viewport) on success.
      8. Raises ValueError if no results are found, or re-raises on other HTTP errors.

    Args:
        city (str): The human-readable name of the city to geocode.

    Returns:
        tuple[float, float, dict]:
            - latitude (float): Center latitude of the city.
            - longitude (float): Center longitude of the city.
            - viewport (dict): The bounding viewport from the API response.

    Raises:
        ValueError: If the API returns no results for the given city.
        requests.HTTPError: If the HTTP request fails (4xx/5xx status).
        Exception: Propagates any other unexpected exceptions.
    """    
    # Generate a unique ID for this search operation    
    search_id:str = str(uuid.uuid4())[:8]
    
    logger.info(f"Geocoding city center", extra={
        "operation": "geocode",
        "search_id": search_id,
        "city": city
    })

    geocode_url = f"https://maps.googleapis.com/maps/api/geocode/json?address={city}&key={API_KEY}"

    try:
        response = requests.get(geocode_url)
        response.raise_for_status()
        api_metrics.total_requests += 1
        
        data = response.json()['results']
        
        if not data:
            logger.error(f"City not found", extra={
                "operation": "geocode",
                "search_id": search_id,
                "city": city,
                "status": "failed"
            })
            raise ValueError("City not found")
        
        location = data[0]['geometry']['location']
        viewport = data[0]['geometry']['viewport']
        
        logger.info(f"Successfully geocoded city", extra={
            "operation": "geocode",
            "search_id": search_id,
            "city": city,
            "lat": location['lat'],
            "lng": location['lng'],
            "status": "success"
        })
        
        return location['lat'], location['lng'], viewport
        
    except Exception as e:
        logger.error(f"Error geocoding city: {str(e)}", extra={
            "operation": "geocode",
            "search_id": search_id,
            "city": city,
            "error": str(e),
            "status": "error"
        })
        raise