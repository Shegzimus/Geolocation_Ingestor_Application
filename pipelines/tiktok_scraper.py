"""
Module for Processing Restaurant Data and Querying TikTok Videos

This module provides utility functions for loading and processing restaurant data as well as for querying the TikTok API to retrieve and validate video content. 

It supports multiple data formats and includes functionality for:
    
    - Validating supported file formats.
    - Loading restaurant data from CSV, JSON, or Parquet files, including parsing stringified list values for restaurant types.
    - Generating search keyword columns based on restaurant names, cities, and additional fixed tokens.
    - Performing fuzzy matching between restaurant names and video titles using the TheFuzz (fuzzywuzzy) library.
    - Querying TikTok’s SearchVideoListByKeywords API endpoint to obtain video details.
    - Converting Unix timestamps to human-readable dates and constructing public TikTok URLs based on video metadata.
    - Saving pandas DataFrames into various file formats (CSV, JSON, Parquet, Feather).

Usage:
    1. Call `load_restaurants_data()` with the desired file path, city, file format, and number of rows to load the restaurant data.
    2. Generate search keywords from the loaded DataFrame using `generate_keywords_col()`.
    3. For a given restaurant, use `query_tiktok()` to search TikTok videos, which internally applies fuzzy matching via `check_fuzzy_match()`.
    4. Finally, save the processed results using `save_dataframe()`.

Important:
    - Ensure that the provided file format is one of the supported formats: "csv", "json", or "parquet".
    - The `types` column in the restaurant data is expected to be a string representing a list; it will be parsed using the `ast.literal_eval` function.
    - The TikTok API key must be set as an environment variable (RAPIDAPI_KEY) for proper querying.
    - Adjust parameters such as `video_count` and `fuzzy_threshold` in the `query_tiktok()` function as needed based on your use case.
    
Exceptions:
    - Raises appropriate exceptions (e.g., FileNotFoundError, ValueError, and pd.errors.ParserError) when file loading fails or when unsupported formats are provided.

This module is designed to be integrated into a larger data pipeline for analyzing video content relevance for restaurant listings.
"""



import pandas as pd
import os
import json
import ast
import time



# ----------------------------------------------------------------------------------------------------

def check_format(file_format: str) -> None:
    supported_formats = {"csv", "json", "parquet"}  # CALL THIS

    if file_format not in supported_formats:
        raise ValueError(f"Unsupported file format: '{file_format}'. Supported formats are: {supported_formats}")

# ----------------------------------------------------------------------------------------------------


def load_restaurants_data(
        path: str, 
        city: str, 
        file_format: str= "csv", 
        nrows: int=30
        )-> pd.DataFrame:
    """
    Load restaurant data for a given city from a file and process it into a DataFrame.

    Parameters:
        path (str): The directory path where the restaurant file is located.
        city (str): The city name used to determine the file name.
        file_format (str, optional): The format of the file ("csv", "json", or "parquet"). Default is "csv".
        nrows (int, optional): Number of rows to read from the file (only applicable for CSV and JSON). Default is 30.

    Returns:
        pd.DataFrame: The processed DataFrame with restaurant data, including a primary key "id" and a 
                      parsed "types_list" column.

    Raises:
        ValueError: If the city parameter is empty.
        FileNotFoundError: If the target file is not found.
        pd.errors.ParserError: If the file cannot be parsed correctly.
    """


    if not city or path:
        raise ValueError("You must provide a city name and a valid file path to load its restaurant data.")
    
    check_format()
    city = city.lower()
    file_format = file_format.lower()

    filename = f"{city}_restaurants.{file_format}"
    full_path = os.path.join(path, filename) if path else filename

    try:
        if file_format == "csv":
            df = pd.read_csv(full_path, nrows=nrows)
        elif file_format == "json":
            df = pd.read_json(full_path, nrows=nrows)
        elif file_format == "parquet":
            df = pd.read_parquet(full_path)
        
        df.reset_index(inplace=True)
        df.rename(columns={"index": "id"}, inplace=True)
        df['types_list'] = df['types'].apply(ast.literal_eval)
        return df
    
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {full_path}")
    except pd.errors.ParserError:
        raise pd.errors.ParserError(f"Could not parse {file_format}: {full_path}")


# ----------------------------------------------------------------------------------------------------


def generate_keywords_col(data: pd.DataFrame, city: str) -> None:
    """
    Generate search keyword columns for restaurant data.

    Parameters:
        data (pd.DataFrame): The DataFrame containing restaurant information.
        city (str): The city name to include in the search keywords.
    
    This function creates two new columns in the DataFrame:
        - 'keywords_type1': combines the restaurant's name with the city.
        - 'keywords_type2': combines the restaurant's name with the city and the literal 'food'.
    
    Prints a completion message once done.
    """
        
    data['keywords_type1'] = data.apply(
        lambda row: f"{row['name']} {city}", axis=1
    )

    data['keywords_type2'] = data.apply(
        lambda row: f"{row['name']} {city} food", axis=1
    )

    print("Search-keyword Generation Complete")

    return None


# ----------------------------------------------------------------------------------------------------

def check_fuzzy_match(restaurant_name: str, video_title: str):
    """
    Compute and return the fuzzy matching score between a restaurant's name and a video's title.

    Parameters:
        restaurant_name (str): The restaurant's name.
        video_title (str): The video's title.
    
    Returns:
        int: The fuzzy matching score (using partial_ratio) indicating similarity.
    """

    from thefuzz import fuzz
    score_title = fuzz.partial_ratio(restaurant_name.lower(), video_title.lower())
    return score_title
      

# ----------------------------------------------------------------------------------------------------




def query_tiktok(
        restaurant_name: str ,
        keyword: str,
        video_count: int=10,
        fuzzy_threshold: int=80        
        )-> list:
    """
    Query the TikTok API for videos based on a given search keyword and apply fuzzy matching to assess relevance.

    Parameters:
        restaurant_name (str): The name of the restaurant to match against video titles.
        keyword (str): The search keyword to be passed to the TikTok API.
        video_count (int, optional): The number of videos to request (default is 10).
        fuzzy_threshold (int, optional): The minimum fuzzy match score to consider a video relevant (default is 80).

    Returns:
        tuple: A tuple containing:
            - response_table (pd.DataFrame): A DataFrame with video data, fuzzy matching scores, and a public TikTok URL.
            - safe_filename (str): A version of the keyword modified for safe file naming (spaces replaced with underscores, etc.).

    Notes:
        - The API request relies on the environment variable 'RAPIDAPI_KEY' for authentication.
        - The function extracts metadata from each video, converts Unix timestamps to date and time,
          and constructs a shareable public URL.
        - If no videos are found, a placeholder entry is added.
    """
    
    import datetime
    import requests

    tiktok_api_url = "https://tiktok-api15.p.rapidapi.com/index/Tiktok/searchVideoListByKeywords"
    headers = {
        "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
        "x-rapidapi-host": "tiktok-api15.p.rapidapi.com"
    }

    querystring = {
        "keywords": keyword,
        "count": video_count,
        "cursor": "0"
    }

    response_records =[]

    try:
        response = requests.get(tiktok_api_url, headers=headers, params=querystring)
        response_json = response.json()

    except Exception as e:
        print(f"API request failed: {e}. Check params")

    # Check if the API returned a success code and contains videos
    if response_json.get("code") == 0 and response_json.get("data", {}).get("videos"):
        videos = response_json["data"]["videos"]
        # Process each returned video entry
        for video in videos:
            video_title = video.get("title", "")
            video_caption = video.get("caption", "")  # Some responses might include caption
            video_duration = video.get("video_duration", "")
            play_count = video.get("play_count", "")
            share_count = video.get("share_count", "")
            download_count = video.get("download_count", "")
            create_time_unix = video.get("create_time_unix", "")

            if create_time_unix:  # only proceed if it's not None or empty string
                try:
                    create_time_unix = int(create_time_unix)
                    dt_object = datetime.datetime.fromtimestamp(create_time_unix)
                    date_created = dt_object.date()
                    time_created = dt_object.time()
                except (ValueError, TypeError):
                    date_created = None
                    time_created = None
            else:
                date_created = None
                time_created = None

            fuzzy_score = check_fuzzy_match(restaurant_name=restaurant_name, 
                                            video_title=video_title)


            author_unique_id = video["author"]["unique_id"]
            video_id = video["video_id"]
            video['public_url'] = f"https://www.tiktok.com/@{author_unique_id}/video/{video_id}"


                # Store the record (even if fuzzy_score is low)
            response_records.append({
                "video_id": video.get("video_id"),
                "author": author_unique_id,
                "video_title": video_title,
                "video_caption": video_caption,
                "video_duration": video_duration,
                "play_count": play_count,
                "share_count": share_count,
                "download_count": download_count,
                "date_created": date_created,
                "time_created": time_created,
                "fuzzy_score": fuzzy_score,
                "public_url": video['public_url']


            })

    else:
    # In case no videos found, record a placeholder entry
        response_records.append({
            "video_id": None,
            "author": None,
            "video_title": None,
            "video_caption": None,
            "video_duration": None,
            "play_count": None,
            "share_count": None,
            "download_count": None,
            "create_time": None,
            "fuzzy_score": None,
            "public_url": None,

        })
    
    time.sleep(1)

    response_table = pd.DataFrame(response_records)
    safe_filename = keyword.replace(" ", "_").replace("/", "-")

    return response_table, safe_filename

# ----------------------------------------------------------------------------------------------------

def save_dataframe(df: pd.DataFrame, 
                  filename: str, 
                  file_format: str
                  ) -> None:
    file_format = file_format.lower()
    filepath = f"{filename}.{file_format}"

    if file_format == "csv":
        df.to_csv(filepath, index=False)
    elif file_format == "json":
        df.to_json(filepath, orient="records", lines=True)
    elif file_format == "parquet":
        df.to_parquet(filepath, index=False)
    elif file_format == "feather":
        df.to_feather(filepath)
    else:
        raise ValueError(f"Unsupported format: '{file_format}'. Choose from 'csv', 'json', 'parquet', 'feather'.")

    print(f" Data saved successfully to {filepath}")



if __name__ == "__main__":

    pass
