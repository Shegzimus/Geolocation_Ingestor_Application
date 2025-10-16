"""
Tiling Module for Adaptive Restaurant Search
--------------------------------

This module provides utilities for generating and managing geographic tiles
used in the adaptive restaurant search process. It supports creating tiles
centered at specified coordinates, subdividing tiles into quadrants, generating
an initial grid of tiles to cover a target viewport, and calculating an
approximate search radius (in meters) for a given tile size.

Functions:
  create_tile(lat, lng, size) -> tuple:
    Create a tile with its center at (lat, lng) and a square side length of 'size'.

  subdivide_tile(tile) -> list:
    Subdivide a given tile into four smaller tiles (SW, SE, NW, NE quadrants).

  generate_initial_tiles(center_lat, center_lng, viewport, initial_step) -> list:
    Generate a grid of tiles covering the provided viewport (with padding), each
    tile having side length 'initial_step'. Logs the total number of tiles and
    the geographic bounds used.

  calculate_search_radius(lat, lng, size) -> float:
    Approximate the search radius in meters from a tile's side length in degrees,
    accounting for latitude-dependent longitude scaling.
"""

from utils import logger
import math


def create_tile(lat: float, 
                lng: float, 
                size: float) -> tuple[float, float, float]:
    """Create a tile with center at lat, lng and given size."""
    return (lat, lng, size)



def subdivide_tile(tile: tuple) -> list:
    """Subdivide a tile into four smaller tiles."""
    lat, lng, size = tile
    new_size = size / 2
    half_size = new_size / 2
    
    return [
        (lat - half_size, lng - half_size, new_size),  # SW
        (lat - half_size, lng + half_size, new_size),  # SE
        (lat + half_size, lng - half_size, new_size),  # NW
        (lat + half_size, lng + half_size, new_size),  # NE
    ]


def generate_initial_tiles(
        center_lat: float, 
        center_lng: float, 
        viewport, 
        initial_step: float
        ) -> list[float, float, float]:
    
    """Create a grid of initial tiles covering the city area."""

    # Extract the bounds from viewport
    sw = viewport['southwest']
    ne = viewport['northeast']
    
    # Add padding to ensure coverage
    lat_min = sw['lat'] - 0.05
    lng_min = sw['lng'] - 0.05
    lat_max = ne['lat'] + 0.05
    lng_max = ne['lng'] + 0.05
    
    tiles = []
    lat = lat_min
    while lat <= lat_max:
        lng = lng_min
        while lng <= lng_max:
            tiles.append(create_tile(lat, lng, initial_step))
            lng += initial_step
        lat += initial_step


    # Log the generated tiles
    logger.info(f"Generated initial tiles", extra={
        "operation": "generate_tiles",
        "tile_count": len(tiles),
        "initial_step": initial_step,
        "bounds": {
            "lat_min": lat_min,
            "lat_max": lat_max,
            "lng_min": lng_min,
            "lng_max": lng_max
        }
    })

    return tiles



def calculate_search_radius(lat:float, lng:float, size:float)-> float:
    """Approximate the search radius in meters for a tile of side length 'size' in degrees.

    Converts degrees to meters by averaging latitude and longitude scales at the given latitude,
    then computes the radius corresponding to half the tile's diagonal.

    Args:
        lat (float): Latitude of the tile center.
        lng (float): Longitude of the tile center.
        size (float): Side length of the tile in degrees.

    Returns:
        float: Approximate search radius in meters.
    """
    # Convert from degrees to meters
    # This is an approximation that varies with latitude
    meters_per_degree_lat = 111111  # Approximate
    meters_per_degree_lng = 111111 * math.cos(math.radians(lat))
    
    # Average the conversion factors for a reasonable approximation
    meters_per_degree_avg = (meters_per_degree_lat + meters_per_degree_lng) / 2
    
    # Calculate radius in degrees
    radius_degrees = (size/2) * math.sqrt(2)
    
    # Convert to meters
    radius_meters = radius_degrees * meters_per_degree_avg
    
    return radius_meters



# def calculate_accurate_search_radius(lat: float, lng: float, size: float) -> float:
#     """Calculate an accurate search radius in meters for a tile of side length 'size' in degrees.
    
#     Computes the actual geodesic distance from the center to each corner of the tile
#     and returns the maximum distance as the radius.
    
#     Args:
#         lat (float): Latitude of the tile center in degrees.
#         lng (float): Longitude of the tile center in degrees.
#         size (float): Side length of the tile in degrees.
        
#     Returns:
#         float: Accurate search radius in meters.
#     """
#     # Earth's mean radius in meters
#     EARTH_RADIUS = 6371000
    
#     # Calculate the coordinates of the four corners
#     half_size = size / 2
#     corners = [
#         (lat + half_size, lng + half_size),  # Northeast
#         (lat + half_size, lng - half_size),  # Northwest
#         (lat - half_size, lng + half_size),  # Southeast
#         (lat - half_size, lng - half_size)   # Southwest
#     ]
    
#     # Calculate the distance to each corner using the Haversine formula
#     distances = []
#     for corner_lat, corner_lng in corners:
#         # Convert latitude and longitude from degrees to radians
#         lat1_rad = math.radians(lat)
#         lng1_rad = math.radians(lng)
#         lat2_rad = math.radians(corner_lat)
#         lng2_rad = math.radians(corner_lng)
        
#         # Haversine formula
#         dlat = lat2_rad - lat1_rad
#         dlng = lng2_rad - lng1_rad
#         a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlng/2)**2
#         c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
#         distance = EARTH_RADIUS * c
        
#         distances.append(distance)
    
#     # Return the maximum distance as the radius
#     return max(distances)