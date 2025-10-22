"""
LEGACY CODE

"""







import requests
import time
import pandas as pd
import pickle
import os
from dotenv import load_dotenv
import math


# -------------------------------------------------------------------------------------------------------
# Set up logging
import logging
import json
from logging.handlers import RotatingFileHandler
import uuid
from datetime import datetime

class JsonFormatter(logging.Formatter):
    def format(self, record):
        builtins = {
            "name", "msg", "args", "levelname", "levelno",
            "pathname", "filename", "module", "exc_info", "stack_info",
            "exc_text", "high_density_stack_info", "lineno", "funcName",
            "created", "msecs", "relativeCreated", "thread",
            "threadName", "processName", "process"
        }
        payload = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level":     record.levelname,
            "logger":    record.name,
            "message":   record.getMessage(),
        }
        # Pick up any extra attributes
        for key, value in record.__dict__.items():
            if key not in builtins:
                payload[key] = value
        return json.dumps(payload)

# Timestamped filename
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"logs/depth_first_restaurant_search_{ts}.log"

# Create module‐level logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = RotatingFileHandler(
    filename=log_filename,
    maxBytes=10_000_000,
    backupCount=5
)
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)

# --------------------------------------------------------------------------------------------------------

# Load environment variables

load_dotenv()
API_KEY = os.getenv("SHEGZ_MAPS_API_KEY")

# --------------------------------------------------------------------------------------------------------- 

# Constants (Entry points that can be later refactored into a config file)
INITIAL_RADIUS = 2000  # in meters
INITIAL_STEP = 0.015  # approx 1.1km in degrees
MIN_STEP = 0.0025  # Minimum step size for subdivision
TYPE = "restaurant"
HIGH_DENSITY_THRESHOLD = 60  # If we get this many results, mark as high density
MAX_RESULTS_PER_PAGE = 20  # Google Places API returns max 20 results per page
MAX_PAGES = 3  # Google Places API allows max 3 pages of results

# Checkpoint file for saving state
CKPT_FILE = "adaptive_search_checkpoint.ckpt"   # Checkpoint file for saving state
CKPT_TMP_FILE = CKPT_FILE + ".tmp"  # Temporary checkpoint file for saving state
CHUNK_SIZE = 500  # Number of records to save in each chunk

MAX_WORKERS = 8

script_start_time = datetime.now()

timestamp_str = script_start_time.strftime("%Y%m%d_%H%M%S")
# ----------------------------------------------------------------------------------------------------------

class APIMetrics:
    """Track API usage metrics"""
    def __init__(self):
        self.total_requests = 0
        self.results_returned = 0
        self.unique_results = 0
        
    def log_metrics(self):
        """Log current API metrics"""
        logger.info("API Metrics Summary", extra={
            "metrics": {
                "total_requests": self.total_requests,
                "results_returned": self.results_returned,
                "unique_results": self.unique_results,
                "efficiency_ratio": round(self.unique_results / max(1, self.results_returned), 2)
            }
        })

# Initialize API metrics tracker
api_metrics = APIMetrics()

# ----------------------------------------------------------------------------------------------------------

# Checkpoint utilities
def save_comprehensive_checkpoint(city: str, state_data: dict):
    """
    Save a comprehensive checkpoint of the current search state for a specific city.
    
    Args:
        city: Name of the city being processed
        state_data: Dictionary containing all state data to checkpoint
    """
    checkpoint_filename = f"checkpoint_{city.lower()}.ckpt"
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


def load_search_state(city: str):
    """Load checkpoint data for a city if it exists."""
    checkpoint_filename = f"checkpoint_{city.lower()}.ckpt"
    
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

def get_city_center(city: str) -> tuple[float, float, dict]:
    """Get the latitude and longitude of the city center using Google Maps Geocoding API."""
    search_id = str(uuid.uuid4())[:8]  # Generate a unique ID for this search operation
    
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

def get_nearby_places(
        lat: float, 
        lng: float, 
        radius: int,  
        type_: str
        ) -> tuple[list, int, int]:
    """Get nearby places and return both the places and the count of results."""
    
    search_id = str(uuid.uuid4())[:8]
    
    logger.info(f"Searching for places", extra={
        "operation": "nearby_search",
        "search_id": search_id,
        "lat": lat,
        "lng": lng,
        "radius": radius,
        "type": type_
    })

    places = []
    url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "location": f"{lat},{lng}",
        "radius": radius,
        "type": type_,
        "key": API_KEY
    }

    page_count = 0
    total_results = 0

    start_time = time.time()


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

# ----------------------------------------------------------------------------------------------------------

def create_tile(lat: float, 
                lng: float, 
                size: float) -> tuple[float, float, float]:
    """Create a tile with center at lat, lng and given size."""
    return (lat, lng, size)

# ----------------------------------------------------------------------------------------------------------

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

# ----------------------------------------------------------------------------------------------------------

def calculate_search_radius(lat, size):
    # Convert from degrees to meters
    # This is an approximation that varies with latitude
    meters_per_degree_lat = 111000  # Approximate
    meters_per_degree_lng = 111000 * math.cos(math.radians(lat))
    
    # Average the conversion factors for a reasonable approximation
    meters_per_degree_avg = (meters_per_degree_lat + meters_per_degree_lng) / 2
    
    # Calculate radius in degrees
    radius_degrees = (size/2) * math.sqrt(2)
    
    # Convert to meters
    radius_meters = radius_degrees * meters_per_degree_avg
    
    return radius_meters

# ----------------------------------------------------------------------------------------------------------

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

# ----------------------------------------------------------------------------------------------------------




def flush_chunk(city: str, chunk_buffer: list) -> None:
    if not chunk_buffer:
        return  # nothing to write

    df = pd.DataFrame(chunk_buffer)

    mode = 'a' if os.path.exists(f"{city.lower()}_restaurants_{ts}.csv") else 'w'
    header = (mode == 'w')
    df.to_csv(f"{city.lower()}_restaurants_{ts}.csv", mode=mode, header=header, index=False)

    chunk_buffer.clear()
    return None



# ----------------------------------------------------------------------------------------------------------
def collect_all_places_adaptive(city: str) -> None:
    """
    Collect all restaurants using an adaptive density-based search strategy.
    """


    session_id = str(uuid.uuid4())[:8]

    # Load checkpoint or init state
    state = load_search_state(city= f"{city}_20250422_200815")
    if state:  # Resume from checkpoint
        try:
            initial_tiles = state.get("initial_tiles", [])
            high_density_stack = state.get("high_density_stack", [])
            processed = state.get("processed", set())
            seen_place_ids = state.get("seen_place_ids", set())
            deep_count = state.get("deep_count", 0)
            
            # Check if we've completed initial scan already and log the outcome
            initial_scan_complete = len(initial_tiles) > 0 and all(tile in processed for tile in initial_tiles)
            
            logger.info(f"Resuming search from checkpoint", extra={
                "operation": "resume_search",
                "session_id": session_id,
                "city": city,
                "initial_scan_complete": initial_scan_complete,
                "high_density_areas": len(high_density_stack),
                "unique_places": len(seen_place_ids),
                "deep_dives_completed": deep_count
            })
            start_time = time.time()
            chunk_buffer = []

            # Initialize API metrics
            api_metrics = APIMetrics()

        except Exception as e:
            logger.error(f"Error resuming from checkpoint: {str(e)}", extra={
                "operation": "resume_search",
                "session_id": session_id,
                "city": city,
                "error": str(e),
                "status": "error"
            })
            raise

    else: 
        # Start fresh
        initial_tiles = []
        processed = set()
        high_density_stack = []
        seen_place_ids = set()

        deep_count = 0
        initial_scan_complete = False

        # Initialize API metrics
        api_metrics = APIMetrics()

        logger.info("Starting new search", extra={
            "operation": "new_search",
            "session_id": session_id,
            "city": city
            })
        
        # Initialize chunk buffer for unique results
        chunk_buffer = []

        # Record start time for peformance logging/tracking
        start_time = time.time()

        try:
            # Initialize the high_density_stack with initial tiles
            lat, lng, viewport = get_city_center(city)
            initial_tiles = generate_initial_tiles(lat, lng, viewport, INITIAL_STEP)

            logger.info(f"Beginning initial tile processing", extra={
                "operation": "adaptive_search",
                "session_id": session_id,
                "phase": "initial_scan",
                "tile_count": len(initial_tiles)
            })

            print(f"Starting with {len(initial_tiles)} initial tiles for city: {city}...")


#    CRAWL CITY
            for idx, tile in enumerate(initial_tiles):
                lat, lng, size = tile
                print(f"Processing tile {idx+1}/{len(initial_tiles)} - Lat: {lat}, Lng: {lng}")

                radius = calculate_search_radius(lat, size)

                places, count, pages = get_nearby_places(lat, lng, radius, TYPE)

                # Add unique places to results
                new_places = 0
                for place in places:
                    pid = place["place_id"]
                    if pid not in seen_place_ids:
                        chunk_buffer.append(place)
                        seen_place_ids.add(pid)
                        new_places += 1
                        api_metrics.unique_results += 1

                logger.info(f"Processed initial tile results", extra={
                    "operation": "adaptive_search",
                    "session_id": session_id,
                    "phase": "initial_scan",
                    "tile_index": idx + 1,
                    "results_count": count,
                    "new_places": new_places,
                    "total_unique": len(seen_place_ids)
                })

                # Flush chunk if it reaches the chunk size
                if new_places>0 and len(chunk_buffer)>=CHUNK_SIZE:
                    
                    # log first to capture chunk size before flush
                    logger.info(f"Flushed {len(chunk_buffer)} places to CSV", extra={
                        "operation": "flush_chunk",
                        "phase": "initial_scan",
                        "session_id": session_id,
                        "city": city,
                        "flushed_count": len(chunk_buffer)
                    })
                    
                    flush_chunk(city, chunk_buffer)
                    

                # Check if the tile is a high-density area
                if pages == 3 and count == 60 :
                    high_density_stack.append((lat, lng, size))
                    logger.info(f"Found high density area", extra={
                        "operation": "adaptive_search",
                        "session_id": session_id,
                        "phase": "initial_scan",
                        "tile": {"lat": lat, "lng": lng, "size": size},
                        "density": count
                    })

                    # Log API metrics
                if (idx + 1) % 5 == 0 or idx == len(initial_tiles) - 1:
                    api_metrics.log_metrics()
        except Exception as e:
            logger.error(f"Error during initial scan: {str(e)}", extra={
                "operation": "initial_scan",
                "session_id": session_id,
                "city": city,
                "error": str(e),
                "status": "error"
            })
            raise

        # Save initialization output as a checkpoint
        ck = save_search_state(
            city,
            initial_tiles,
            high_density_stack,
            processed,
            seen_place_ids,
            deep_count
            )

    logger.info(f"Completed initial scan", extra={
        "operation": "adaptive_search",
        "session_id": session_id,
        "phase": "initial_scan",
        "high_density_areas": len(high_density_stack),
        "unique_places": len(seen_place_ids),
        "initial_scan_time": round(time.time() - start_time, 2),
        "checkpoint": bool(ck)
    })

    if len(chunk_buffer) > 0:
        flush_chunk(city, chunk_buffer)
        logger.info(f"Flushed the final {len(chunk_buffer)} places to CSV", extra={
            "operation": "flush_chunk",
            "phase": "initial_scan",
            "session_id": session_id,
            "city": city,
            "flushed_count": len(chunk_buffer)
        })


    print("Out of", len(initial_tiles), "initial tiles,", len(high_density_stack), "high-density areas were discovered. Starting deep dive...")

    # Deep dive into high-density areas ##################################################################################
    while high_density_stack:
        lat, lng, size = high_density_stack.pop()
        deep_count += 1
        key = f"{lat:.6f},{lng:.6f},{size:.6f}"

        if key in processed:
            continue

        processed.add(key)
        radius = calculate_search_radius(lat, size)
        # search
        places, count, pages = get_nearby_places(
            lat, 
            lng, 
            radius, 
            TYPE
            )
        

        # store results
        new = 0 
        for p in places:
            pid = p['place_id']
            if pid not in seen_place_ids:
                seen_place_ids.add(pid); chunk_buffer.append(p); new+=1
        if new>0 and len(chunk_buffer)>=CHUNK_SIZE:
            flush_chunk(city, chunk_buffer)

            logging.info(f"Flushed {len(chunk_buffer)} places to CSV", extra={
                "operation": "flush_chunk",
                "phase": "deep_dive",
                "session_id": session_id,
                "city": city,
                "flushed_count": len(chunk_buffer)
            })

        # save a separate checkpoint for deep dives
        if deep_count % 3 == 0:
            save_search_state(
                city=f"{city}_deep_dive",
                initial_tiles= initial_tiles,
                high_density_stack=high_density_stack,
                processed=processed,
                seen_place_ids=seen_place_ids,
                deep_count=deep_count
                )
            
        # log API usage during the deep dive
        if len(high_density_stack) % 3 == 0:
            api_metrics.log_metrics()
            logger.info(f"API metrics logged", extra={
                "operation": "api_metrics",
                "session_id": session_id,
                "city": city,
                "current_high_density_stack_size": len(high_density_stack)
            })



        # subdivide only if above minimum search resolution and response capped
        if size > MIN_STEP and pages==3 and count==60:
            for sub in subdivide_tile((lat, lng, size)):
                high_density_stack.append(sub)
        # checkpoint periodically

    # final flush
    if len(chunk_buffer)>0:
        flush_chunk(city, chunk_buffer)
    logging.info(f"Flushed the final {len(chunk_buffer)} places to CSV", extra={
                "operation": "flush_chunk",
                "phase": "final_flush",
                "session_id": session_id,
                "city": city,
                "flushed_count": len(chunk_buffer)
            })
    
    # Remove checkpoint file when complete
    checkpoint_filename = f"checkpoint_{city.lower()}_deep_dive.ckpt"
    if os.path.exists(checkpoint_filename):
        os.remove(checkpoint_filename)
        logger.info(f"Checkpoint file removed", extra={
            "operation": "remove_checkpoint",
            "session_id": session_id,
            "city": city,
            "checkpoint_file": checkpoint_filename
        })
    end_time = time.time() - start_time

    
    logger.info("Adaptive search complete", extra={
        "city": city, 
        "total_places": len(seen_place_ids),
        "total_time": round(end_time, 2),
        "session_id": session_id,
        "status": "completed",
        "duration": round(end_time, 2)
        })
    
    # Log final API metrics
    api_metrics.log_metrics()
    logger.info(f"Final API metrics", extra={
        "operation": "final_metrics",
        "session_id": session_id,
        "city": city,
        "total_requests": api_metrics.total_requests,
        "results_returned": api_metrics.results_returned,
        "unique_results": api_metrics.unique_results
    })
    print(f"Adaptive search completed for {city} in {round(end_time, 2)} seconds")

    return None


# Run it
if __name__ == "__main__":
    city = "London"

    logger.info(f"Starting restaurant search application", extra={
        "operation": "application_start",
        "city": city,
        "version": "1.0.1"
    })

    try:
        collect_all_places_adaptive(city)

    except Exception as e:
        logger.critical(f"Application error: {str(e)}", extra={
            "operation": "application_error",
            "error": str(e)
        })
        print(f"Error: {str(e)}")

    finally:
        logger.info("Application shutting down", extra={
            "operation": "application_shutdown"})
