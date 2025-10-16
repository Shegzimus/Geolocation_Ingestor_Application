"""
restaurant_scraper.py

This module scrapes and saves restaurant location data for a specified city using the Google Maps Geocoding 
and Places APIs.

Overview:
- Determines the geographic center of a city.
- Tiles the surrounding area into coordinate grids (to overcome API limitations).
- Searches each tile for nearby places (default: restaurants).
- Collects and deduplicates results.
- Saves the data to a CSV file.

Global Configuration:
- `CITY`: The name of the city to search in (default: "Dublin").
- `SEARCH_TYPE`: Type of place to search for (default: "restaurant").
- `RADIUS`: Radius in meters for each tile search (default: 2000).
- `API_KEY`: Google Maps API key loaded from environment variables (`SHEGZ_MAPS_API_KEY`).

Key Functions:
- `get_city_center(city)`: Gets latitude and longitude for a given city.
- `tile_city(lat, lng)`: Generates grid points for spatial searches.
- `get_nearby_places(lat, lng, radius, type)`: Retrieves places near a point using Google Places API.
- `search_for_restaurants()`: Gathers all unique restaurant data from the city area.
- `save_locations_restaurants(save_method, data)`: Saves results to CSV or other formats.
- `run()`: Main entry point for running the full workflow.

Tiling Strategy Notes:
The Google Places API limits nearby search results to 60 per request (20 per page over 3 pages max). To collect comprehensive place data
for an entire city, the area is divided into overlapping tiles — a strategy inspired by spatial indexing methods and geospatial grid systems.
Each tile represents a coordinate center from which nearby searches are performed.

References:
- Google Places API pagination limit: https://developers.google.com/maps/documentation/places/web-service/search#PlaceSearchPaging
- Spatial indexing: https://en.wikipedia.org/wiki/Spatial_index
- Developer discussion on tiling necessity: https://stackoverflow.com/questions/32502120/why-does-google-places-api-return-only-60-results
- Geospatial tiling inspiration: https://s2geometry.io/

Note:
- Requires internet access and a valid Google Maps API key.
- Designed to be extended with more save methods and improved performance (e.g., multithreading).
- Logging to be included in later versions
"""




import requests
import time
import csv
import os
from dotenv import load_dotenv
import logging


API_KEY = os.getenv('SHEGZ_MAPS_API_KEY')
CITY = "Dublin"
SEARCH_TYPE = "restaurant"
RADIUS = 2000 


def configure():
    load_dotenv()



configure()


# ----------------------------------------------------------------------------------------------------
def get_city_center(city:str)-> tuple:
    """
    Retrieves the geographical center (latitude and longitude) of a given city using the Google Maps Geocoding API.

    Args:
        city (str): The name of the city to geocode.

    Returns:
        tuple: A tuple containing the latitude and longitude of the city's center as floats.

    Raises:
        ValueError: If the city cannot be found in the geocoding results.
        requests.HTTPError: If the HTTP request to the API fails.

    Note:
        This function requires a valid API key to be available in the global variable `API_KEY`.
    """
    
    geocode_url = f"https://maps.googleapis.com/maps/api/geocode/json?address={city}&key={API_KEY}"
    response = requests.get(geocode_url)
    response.raise_for_status()
    results = response.json()['results']
    if not results:
        raise ValueError("City not found")

    location = results[0]['geometry']['location']
    return location['lat'], location['lng']



# ----------------------------------------------------------------------------------------------------
def get_place_details():

    return None



# ----------------------------------------------------------------------------------------------------
def get_nearby_places(lat:int, lng:int, radius:int, type:str)->list:
    """
    Retrieves a list of places of a specified type near a given geographic location using the Google Places API.

    Args:
        lat (int): Latitude of the location to search near.
        lng (int): Longitude of the location to search near.
        radius (int): Search radius in meters.
        type (str): Type of place to search for (e.g., 'restaurant', 'cafe', 'museum').

    Returns:
        list: A list of dictionaries representing nearby places that match the search criteria.

    Raises:
        requests.HTTPError: If the HTTP request to the API fails.

    Note:
        This function uses the `next_page_token` mechanism provided by the Google Places API
        to retrieve additional results beyond the first page. It waits 2 seconds before fetching the next page,
        as required by the API. A valid API key must be defined in the global variable `API_KEY`.
    """
    places = []
    url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "location": f"{lat},{lng}",
        "radius": radius,
        "type": type,
        "key": API_KEY
    }
    while True:
        response = requests.get(url, params=params)
        response.raise_for_status()
        restaurants = response.json()
        places.extend(restaurants.get('results', []))
        token = restaurants.get("next_page_token")
        if token:
            time.sleep(2)  # wait for token to activate
            params = {
                "pagetoken": token,
                "key": API_KEY
            }
        else:
            break
    return places

# ----------------------------------------------------------------------------------------------------
def tile_city(center_lat:int, center_lng:int, step=0.02, radius=RADIUS)-> list:
    """
    Divides a city area into tiles (latitude and longitude points) for spatial querying.

    Args:
        center_lat (int): Latitude of the city's center.
        center_lng (int): Longitude of the city's center.
        step (float, optional): Distance in degrees between each tile point. Defaults to 0.02.
        radius (int, optional): Radius used to inform tile coverage (not directly used in logic). Defaults to RADIUS.

    Returns:
        list: A list of (latitude, longitude) tuples representing tile center points covering the area.

    Note:
        This function tiles a square area of approximately ±0.1 degrees around the city's center.
        Each tile center is spaced apart by the `step` value in both latitude and longitude directions.
    """

    lat_range = [center_lat - 0.1, center_lat + 0.1]
    lng_range = [center_lng - 0.1, center_lng + 0.1]
    tiles = []
    lat = lat_range[0]
    while lat <= lat_range[1]:
        lng = lng_range[0]
        while lng <= lng_range[1]:
            tiles.append((lat, lng))
            lng += step
        lat += step
    return tiles

# ----------------------------------------------------------------------------------------------------
def search_for_restaurants()-> list:
    """
    Searches for restaurants throughout a tiled area of a specified city using the Google Maps API.

    This function:
    - Retrieves the geographic center of the city defined by the global variable `CITY`.
    - Tiles the area around the city center into small square sections.
    - For each tile, uses the Google Places API to find nearby restaurants.
    - Deduplicates results by tracking unique place IDs.

    Returns:
        list: A list of unique restaurant place data (as dictionaries) retrieved from the API.

    Side Effects:
        - Prints progress information to the console for each tile search and overall count of results.
        - Relies on the following global variables:
            - `CITY`: Name of the city to search in.
            - `RADIUS`: Radius in meters for each nearby place search.
            - `SEARCH_TYPE`: The type of place to search for (e.g., "restaurant").
            - `API_KEY`: Your Google Maps API key (used internally by helper functions).

    Raises:
        Any exceptions encountered during individual tile searches are caught and printed,
        but do not stop the overall execution.
    """

    # REFACTOR MULTITHREADING LATER
    center_lat, center_lng = get_city_center(CITY)
    print(f"City center: {center_lat}, {center_lng}")

    tiles = tile_city(center_lat, center_lng, step=0.015)
    restaurants = []
    seen_place_ids = set()

    for i, (lat, lng) in enumerate(tiles):
        print(f"[{i+1}/{len(tiles)}] Searching around {lat},{lng}")
        try:
            places = get_nearby_places(lat, lng, RADIUS, SEARCH_TYPE)
            for place in places:
                if place['place_id'] not in seen_place_ids:
                    restaurants.append(place)
                    seen_place_ids.add(place['place_id'])
        except Exception as e:
            print(f"Error at tile {lat},{lng}: {e}")

    get_place_details()
    print(f"\n Total unique restaurants found: {len(restaurants)}")
    
    return restaurants

# ----------------------------------------------------------------------------------------------------
def save_locations_restaurants(save_method="csv", data=None) -> None:
    """
    Saves restaurant location data to a file in the specified format.

    Args:
        save_method (str, optional): The format to save the data in. Currently only "csv" is supported. Defaults to "csv".
        data (list, optional): A list of restaurant data dictionaries. If not provided, the function will call
                               `search_for_restaurants()` to retrieve data.

    Returns:
        None

    Raises:
        ValueError: If an unsupported save method is specified.

    Side Effects:
        - Writes a file (e.g., CSV) containing restaurant details: name, address, latitude, longitude, and place ID.
        - Prints a confirmation message including the filename.

    Notes:
        - The filename is generated using the global `CITY` variable (e.g., "Berlin_restaurants.csv").
        - To support additional file formats in the future, extend the `if save_method == "csv"` block.
    """

    

    restaurants = data if data is not None else search_for_restaurants()

    # ADD MORE SAVE METHODS LATER
    if save_method == "csv":
        CITY = CITY.lower()
        filename = f"{CITY}_restaurants.{save_method}"
        with open(filename, "w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["name", "address", "lat", "lng", "place_id"])
            for p in restaurants:
                writer.writerow([
                    p.get("name"),
                    p.get("vicinity"),
                    p["geometry"]["location"]["lat"],
                    p["geometry"]["location"]["lng"],
                    p["place_id"]
                ])
        print(f"Saved to {filename}")
    
    # elif save_method == "sheets":

    # elif save_method == "parquet":

    # elif save_method == "feather":

    # elif save_method == "json":

    else:
        raise ValueError(f"Unsupported save method: {save_method}")



# def run():
#     restaurants = search_for_restaurants()
#     # logging.info(f"{len(restaurants)} unique restaurants found")

#     # Save to CSV
#     save_locations_restaurants(save_method="csv", data=restaurants)


if __name__ == "__main__":
    pass