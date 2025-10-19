import uuid
import time
import os
from typing import Tuple, List, Dict, Set

# Spatial Modules
from geocode import get_city_center
from places import get_nearby_places
from spatial.tiles import (
    generate_initial_tiles, 
    subdivide_tile, 
    calculate_search_radius
    )

# Configuration
from config import (
    INITIAL_RADIUS, 
    INITIAL_STEP,  
    MIN_STEP, 
    TYPE,  
    HIGH_DENSITY_THRESHOLD,
    CHUNK_SIZE,
    CITY
    )

# Utilities
from utils.metrics import api_metrics
from utils.checkpoint import save_search_state, load_search_state, flush_chunk
from utils import logger

# Initialize global variables


class AdaptiveSearch:
    """
    AdaptiveSearch performs efficient spatial searches for places within a city using adaptive subdivisions.
    
    This class implements a two-phase search strategy:
    1. Initial scan - Creates a grid of tiles across the city and identifies high-density areas
    2. Deep dive - Recursively subdivides high-density areas to discover more places
    
    The algorithm optimizes API usage by focusing on areas with higher concentrations of places
    and adapting the search grid dynamically based on place density.
    
    Key features:
    - Automatic checkpoint management to resume interrupted searches
    - Efficient memory management with chunked result writing
    - API usage tracking and optimization
    - Adaptive subdivision based on density thresholds
    
    Attributes:
        city (str): City name to search within
        initial_radius (float): Initial search radius for API calls in meters
        initial_step (float): Initial tile size for the grid in degrees
        min_step (float): Minimum tile size for subdivision in degrees
        high_density_threshold (int): Threshold to identify high-density areas
        chunk_size (int): Number of places to buffer before writing to disk
        location_type (str): Type of place to search for (e.g., "restaurant")
        session_id (str): Unique identifier for this search operation
        api_metrics (APIMetrics): Tracker for API usage statistics
        
    Usage:
        search = AdaptiveSearch(
            city="Dublin",
            initial_radius=1000,
            initial_step=0.01,
            min_step=0.001,
            high_density_threshold=20,
            chunk_size=100,
            location_type="restaurant"
        )
        search.run()  # Executes both initial scan and deep dive phases
    """

    def __init__(
            self, 
            city:str = CITY, 
            initial_radius:float = INITIAL_RADIUS, 
            initial_step:float = INITIAL_STEP, 
            min_step:float = MIN_STEP, 
            high_density_threshold:int = HIGH_DENSITY_THRESHOLD,
            chunk_size:int = CHUNK_SIZE,
            location_type:str = TYPE,
            ) -> None:

        self.chunk_size:int = chunk_size                                                        # Size of the chunk buffer for CSV writing
        self.initial_radius:float = initial_radius                                              # Initial search radius
        self.initial_step:float = initial_step                                                  # Initial step size for tile generation
        self.min_step:float = min_step                                                          # Minimum step size for tile generation
        self.high_density_threshold:int = high_density_threshold                                # Threshold for high-density areas

        # Initialize instance variables
        self.city:                  str                              = city                      # City to search for
        self.location_type:         str                              = location_type             # Type of location to search for (e.g., restaurant, cafe, etc.)
        self.session_id:            str                              = str(uuid.uuid4())[:8]     # Generate a unique ID for this search operation
        self.start_time:            float                            = time.time()               # Start time for the search operation

        self.api_metrics                                             = api_metrics.APIMetrics()  # Initialize API metrics tracking   

        self.processed:             Set[str]                         = set()                     # Set to track processed tiles
        self.seen_place_ids:        Set[str]                         = set()                     # Set to track unique place IDs     
        self.new_places:            int                              = 0                         # Counter for new places found
        self.deep_count:            int                              = 0                         # Counter for deep dives performed
        self.max_deep_dives:        int                              = 1000                      # Limit to prevent too many API calls - can be adjusted or removed later
        self.initial_tiles:         List[Tuple[float, float, float]] = []                        # [longitude, latitude, size]
        self.high_density_stack:    List[Tuple[float,float,float]]   = []                        # Priority queue for high-density areas
        self.chunk_buffer:          List[Dict]                       = []                        # Buffer for chunked CSV writing



    def log_info(self, message: str, extra: dict):
        logger.info(message, extra=extra)

    def log_error(self, message: str, extra: dict):
        logger.error(message, extra=extra)

    def log_debug(self, message: str, extra: dict):
        logger.debug(message, extra=extra)

# -------------------------------------------------- Helper Functions ---------------------------------------------------------

    def check_tile_density(
            self,
            pages:int,
            lat:float, 
            lng:float, 
            size:float, 
            count:int,
            )-> None:
        if pages == 3 and count == 60:
            self.high_density_stack.append((lat, lng, size))
            self.log.info(f"Found high density area", extra={
                "operation": "adaptive_search",
                "session_id": self.session_id,
                "phase": "initial_scan",
                "tile": {"lat": lat, "lng": lng, "size": size},
                "density": count
            })

    def add_unique_row_to_final_list(self, places:List[Dict])-> None:
        for place in places:
            pid = place["place_id"]
            if pid not in self.seen_place_ids:
                self.chunk_buffer.append(place)
                self.seen_place_ids.add(pid)
                self.new_places += 1
                self.api_metrics.unique_results += 1

    def check_and_save_chunk_size(self) -> None:
        
        # Check if the chunk buffer has reached the specified size
        if self.new_places>0 and len(self.chunk_buffer)>=self.chunk_size:
            flush_chunk(self.city, self.chunk_buffer)
            self.log.info(f"Flushed {len(self.chunk_buffer)} places to CSV", extra={
                "operation": "flush_chunk",
                "phase": "initial_scan",
                "session_id": self.session_id,
                "city": self.city,
                "flushed_count": len(self.chunk_buffer)
            })

    def crawl_city(self)-> None:
    # Initialize the high_density_stack with initial tiles

        self.log.info(f"Starting adaptive search for city", extra={
            "operation": "adaptive_search",
            "session_id": self.session_id,
            "city": self.city,
            "search_params": {
                "initial_radius": self.initial_radius,
                "initial_step": self.initial_step,
                "min_step": self.min_step,
                "high_density_threshold": self.high_density_threshold
            }
        })
        print(f"Starting with {len(self.initial_tiles)} initial tiles for city: {self.city}...")

        for idx, tile in enumerate(self.initial_tiles):
            lat, lng, size = tile
            print(f"Processing tile {idx+1}/{len(self.initial_tiles)} - Lat: {lat}, Lng: {lng}")

            places, count, pages = get_nearby_places(lat, lng, self.initial_radius, self.location_type)

            self.add_unique_row_to_final_list(places=places)
            
            self.check_and_save_chunk_size() # Flushes the chunk buffer to CSV if it reaches the specified size

            self.check_tile_density(
                pages=pages,
                lat=lat,
                lng=lng,
                size=size,
                count=count
                ) # Checks the tile and appends to the high_density_stack if the density is high

            if (idx + 1) % 5 == 0 or idx == len(self.initial_tiles) - 1:
                    self.api_metrics.log_metrics()

        if len(self.chunk_buffer) > 0:
            flush_chunk(self.city, self.chunk_buffer)

        ck = save_search_state(
                self.city,
                self.initial_tiles,
                self.high_density_stack,
                self.processed,
                self.seen_place_ids,
                self.deep_count
                )
        

        self.log.info(f"Completed initial scan", extra={
            "operation": "adaptive_search",
            "session_id": self.session_id,
            "phase": "initial_scan",
            "high_density_stack": len(self.high_density_stack),
            "unique_places": len(self.seen_place_ids),
            "checkpoint": bool(ck),
            "initial_scan_time": round(time.time() - self.start_time, 2)
        })

    def run_initial_scan(self) -> None:
        """
        Run the initial scan for the city.
        This method generates initial tiles and processes them to find high-density areas.
        """
        self.log.info(f"Starting adaptive search for city", extra={
            "operation": "adaptive_search",
            "session_id": self.session_id,
            "city": self.city,
            "search_params": {
                "initial_radius": self.initial_radius,
                "initial_step": self.initial_step,
                "min_step": self.min_step,
                "high_density_threshold": self.high_density_threshold
            }
        })
        

        try:
            state = load_search_state(self.city)      #   MAKE MORE SPECIFIC TO CATCH THE DATED LAST CHECKPOINT IF EXISTS

        except Exception as e:
            self.log.error(f"Error loading checkpoint: {str(e)}", extra={
                "operation": "adaptive_search",
                "session_id": self.session_id,
                "city": self.city,
                "error": str(e),
                "status": "error"
            })
            raise

        if state:
            self.initial_tiles = state.get("initial_tiles", [])
            self.high_density_stack = state.get("high_density_stack", [])
            self.processed = state.get("processed", set())
            self.seen_place_ids = state.get("seen_place_ids", set())
            self.deep_count = state.get("deep_count", 0)
            print(f"Loaded checkpoint for {self.city} with {len(self.initial_tiles)} initial tiles and {len(self.high_density_stack)} high-density areas")

            # Check if we've completed initial scan already and log the outcome
            initial_scan_complete = len(self.initial_tiles) > 0 and all(tile in self.processed for tile in self.initial_tiles)
            
            self.log.info(f"Resuming search from checkpoint", extra={
                "operation": "resume_search",
                "session_id": self.session_id,
                "city": self.city,
                "initial_scan_complete": initial_scan_complete,
                "high_density_stack": len(self.high_density_stack),
                "unique_places": len(self.seen_place_ids),
                "deep_dives_completed": self.deep_count
            })

        else:
            try:
                center_lat, center_lng, viewport = get_city_center(self.city)
                self.initial_tiles = generate_initial_tiles(center_lat, center_lng, viewport, self.initial_step)
                
                self.log.info(f"Beginning initial tile processing", extra={
                    "operation": "adaptive_search",
                    "session_id": self.session_id,
                    "phase": "initial_scan",
                    "tile_count": len(self.initial_tiles)
                })

                print(f"Starting with {len(self.initial_tiles)} initial tiles for city: {self.city}...")
                
                self.crawl_city(self.initial_tiles)
                
            except Exception as e:
                self.log.error(f"Error during initial scan: {str(e)}", extra={
                    "operation": "adaptive_search",
                    "session_id": self.session_id,
                    "city": self.city,
                    "error": str(e),
                    "status": "error"
                })

            print(f"Out of len{self.initial_tiles}, len{self.high_density_stack} high-density areas were discovered starting deep dive...")

    def run_deep_dive(self) -> None:
        """
        Run the deep dive phase for high-density areas.
        """
            
        self.log.info(f"Beginning deep dive phase", extra={
                "operation": "adaptive_search",
                "session_id": self.session_id,
                "phase": "deep_dive",
                "high_density_stack": len(self.high_density_stack),
                "max_deep_dives": self.max_deep_dives
            })

        try: # Deep dive into high-density areas
            while len(self.high_density_stack) and self.deep_count < self.max_deep_dives:  # Limit to prevent too many API calls
                stack_element:Tuple[float,float,float] = self.high_density_stack.pop()
                lat, lng, size = stack_element


                # Skip if we've already processed this tile                     REPOSITION THIS LOGIC TO TRACK EACH PROCESSED TILE IN THE LOOP (might even remove)
                tile_key:str = f"{lat:.6f},{lng:.6f},{size:.6f}"
                if tile_key in self.processed:
                    self.log.debug(f"Skipping tile (already processed)", extra={
                        "operation": "adaptive_search",
                        "session_id": self.session_id,
                        "phase": "deep_dive",
                        "tile": {"lat": lat, "lng": lng, "size": size},
                        "skipped": True,
                        "reason": "already_processed"
                    })
                    continue
                
                else:
                    self.processed.add(tile_key)
                
                # print(f"Deep diving high-density area with {-neg_count} places at Lat: {lat}, Lng: {lng}")
                self.log.info(f"Deep diving high-density area", extra={
                    "operation": "adaptive_search",
                    "session_id": self.session_id,
                    "phase": "deep_dive",
                    "deep_dive_index": self.deep_count,
                    "tile": {"lat": lat, "lng": lng, "size": size}
                })
                
                # Adjust radius based on tile size
                adjusted_radius:float = calculate_search_radius(lat, size)
                
                # search
                places, count, pages = get_nearby_places(
                    lat, 
                    lng, 
                    adjusted_radius, 
                    self.location_type
                    )
                
                # Count the dive we just executed
                self.api_metrics.total_requests += 1
                self.deep_count += 1
                self.api_metrics.results_returned += count

                
                for p in places:
                    pid = p['place_id']
                    if pid not in self.seen_place_ids:
                        self.seen_place_ids.add(pid) 
                        self.chunk_buffer.append(p)
                        self.api_metrics.unique_results += 1

                if len(self.chunk_buffer)>=self.chunk_size:
                    flush_chunk(self.city, self.chunk_buffer)

                    self.log.info(f"Flushed {len(self.chunk_buffer)} places to CSV", extra={
                        "operation": "flush_chunk",
                        "phase": "deep_dive",
                        "session_id": self.session_id,
                        "city": self.city,
                        "flushed_count": len(self.chunk_buffer)
                    })

                # save a separate checkpoint for deep dives
                if self.deep_count % 3 == 0:
                    save_search_state(
                        city=f"{self.city}_deep_dive",
                        initial_tiles= self.initial_tiles,
                        high_density_stack=self.high_density_stack,
                        processed=self.processed,
                        seen_place_ids=self.seen_place_ids,
                        deep_count=self.deep_count
                        )
                    
                # log API usage during the deep dive
                if len(self.high_density_stack) % 3 == 0:
                    api_metrics.log_metrics()
                    self.log.info(f"API metrics logged", extra={
                        "operation": "api_metrics",
                        "session_id": self.session_id,
                        "city": self.city,
                        "current_high_density_stack_size": len(self.high_density_stack)
                    })

                if size > self.min_step and pages==3 and count==60:
                    for sub in subdivide_tile((lat, lng, size)):
                        self.high_density_stack.append(sub)

        except Exception as e:
            self.log.error(f"Error during deep dive: {str(e)}", extra={
                "operation": "adaptive_search",
                "session_id": self.session_id,
                "city": self.city,
                "error": str(e),
                "status": "error"
            })

        # final flush
        if len(self.chunk_buffer)>0:
            flush_chunk(self.city, self.chunk_buffer)
        self.log.info(f"Flushed the final {len(self.chunk_buffer)} places to CSV", extra={
                    "operation": "flush_chunk",
                    "phase": "final_flush",
                    "session_id": self.session_id,
                    "city": self.city,
                    "flushed_count": len(self.chunk_buffer)
                })
        
        # Remove checkpoint file when complete
        checkpoint_filename = f"checkpoint_{self.city.lower()}_deep_dive.ckpt"
        if os.path.exists(checkpoint_filename):
            os.remove(checkpoint_filename)
            self.log.info(f"Checkpoint file removed", extra={
                "operation": "remove_checkpoint",
                "session_id": self.session_id,
                "city": self.city,
                "checkpoint_file": checkpoint_filename
            })
        end_time = time.time() - self.start_time

        self.log.info("Adaptive search complete", extra={
            "city": self.city, 
            "total_places": len(self.seen_place_ids),
            "total_time": round(end_time, 2),
            "session_id": self.session_id,
            "status": "completed",
            "duration": round(end_time, 2)
            })

            # Log final API metrics
        api_metrics.log_metrics()
        self.log.info(f"Final API metrics", extra={
            "operation": "final_metrics",
            "session_id": self.session_id,
            "city": self.city,
            "total_requests": api_metrics.total_requests,
            "results_returned": api_metrics.results_returned,
            "unique_results": api_metrics.unique_results
        })
        print(f"Adaptive search completed for {self.city} in {round(end_time, 2)} seconds")

    def run(self) -> None:
        """
        Run the adaptive search process.
        """
        self.run_initial_scan()
        self.run_deep_dive()

