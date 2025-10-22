import pandas as pd
import os
import requests
import pickle
import json
import time
from collections import OrderedDict

def main():
    # Load the dataset
    df = pd.read_csv("dublin_reordered - merged_dedup.csv")
    
    place_details = []
    api_key = os.getenv("SHEGZ_MAPS_API_KEY")
    
    # Check for checkpoint
    checkpoint_file = "place_details_checkpoint.ckpt"
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "rb") as f:
            state_data = pickle.load(f)
        place_ids = state_data["place_id"]
        print(f"Resuming from checkpoint with {len(place_ids)} places remaining")
    else:
        # If no checkpoint exists, start from the beginning
        place_ids = df["place_id"].tolist()
        print(f"Starting new run with {len(place_ids)} places to process")
    
    # Create output file if it doesn't exist
    output_file = "dublin_place_details.csv"
    
    # Define all fields we want to request - in a specific order
    # This should match the expected_columns in save_results for consistency
    fields = [
        "name", "place_id", "rating", "formatted_phone_number", "dine_in",
        "delivery", "business_status", "curbside_pickup", "editorial_summary",
        "formatted_address", "international_phone_number", "price_level",
        "reservable", "reviews", "secondary_opening_hours", "serves_beer",
        "serves_breakfast", "serves_brunch", "serves_dinner", "serves_lunch",
        "serves_vegetarian_food", "serves_wine", "takeout", "types",
        "url", "user_ratings_total", "utc_offset", "vicinity", "website",
        "wheelchair_accessible_entrance"
    ]
    
    # Join fields with comma for the API request
    fields_param = ",".join(fields)
    
    # Setup for rate limiting
    requests_per_second = 10  # Adjust as needed for your API quota
    min_delay = 1.0 / requests_per_second
    
    # Process places
    while place_ids:
        start_time = time.time()
        
        # Get next place ID
        place_id = place_ids.pop(0)  # Take from front of list to maintain order
        
        # Build API request with consistent field order
        url = f"https://maps.googleapis.com/maps/api/place/details/json?fields={fields_param}&place_id={place_id}&key={api_key}"
        
        try:
            response = requests.get(url)
            
            # Print response for debugging
            if response.status_code != 200:
                print(f"Error: API returned status code {response.status_code}")
                print(f"Response content: {response.text}")
                continue
            
            data = response.json()
            
            # Check if we have a valid response
            if "result" not in data:
                print(f"Warning: No 'result' in response for place_id={place_id}")
                print(f"Response content: {json.dumps(data, indent=2)}")
                continue
            
            # Add place details to our list
            place_details.append(data["result"])
            print(f"Successfully processed place_id={place_id}")
            
            # Save to CSV periodically (every 10 places)
            if len(place_details) >= 10:
                save_results(place_details, output_file)
                place_details = []  # Clear after saving
            
            # Save checkpoint
            save_checkpoint(place_ids, checkpoint_file)
            
            # Rate limiting
            elapsed = time.time() - start_time
            if elapsed < min_delay:
                time.sleep(min_delay - elapsed)
                
        except Exception as e:
            print(f"Error processing place_id={place_id}: {e}")
            # Keep the failed ID for retry (optional)
            place_ids.append(place_id)
    
    # Save any remaining results
    if place_details:
        save_results(place_details, output_file)
    
    print("Processing complete!")


def save_results(place_details, output_file):
    """Save place details to CSV file with consistent column ordering"""
    try:
        # Convert to DataFrame
        results_df = pd.json_normalize(place_details)
        
        # Define the expected columns in the desired order
        # These match the fields you requested in the API
        expected_columns = [
            'name', 'place_id', 'rating', 'formatted_phone_number', 'dine_in',
            'delivery', 'business_status', 'curbside_pickup', 'editorial_summary.overview',
            'formatted_address', 'international_phone_number', 'price_level',
            'reservable', 'reviews', 'secondary_opening_hours', 'serves_beer',
            'serves_breakfast', 'serves_brunch', 'serves_dinner', 'serves_lunch',
            'serves_vegetarian_food', 'serves_wine', 'takeout', 'types',
            'url', 'user_ratings_total', 'utc_offset', 'vicinity', 'website',
            'wheelchair_accessible_entrance'
        ]
        
        # Handle nested fields like reviews that might be arrays
        # Get all actual columns from the results
        actual_columns = results_df.columns.tolist()
        
        # Create a standardized DataFrame with all expected columns
        # For columns that don't exist in this batch, add them with NaN values
        for col in expected_columns:
            if col not in actual_columns and '.' not in col:  # Skip complex nested fields
                results_df[col] = None
        
        # For complex nested fields like reviews, editorial_summary etc.
        # These will have varying structure so we'll keep what json_normalize produces
                
        # Check if file exists to determine mode and header
        file_exists = os.path.exists(output_file)
        mode = 'a' if file_exists else 'w'
        header = not file_exists
        
        # If file exists and we're appending, make sure column order matches
        if file_exists:
            try:
                # Read the header of existing file to get column order
                existing_header = pd.read_csv(output_file, nrows=0).columns.tolist()
                
                # Reorder current results to match existing file
                # This includes both our expected columns and any additional columns
                all_columns = [col for col in existing_header if col in results_df.columns]
                new_columns = [col for col in results_df.columns if col not in existing_header]
                
                if new_columns:  # Handle any new columns not in the original file
                    print(f"Warning: Found {len(new_columns)} new columns not in existing file")
                    # Append new columns to the end
                    results_df = results_df[all_columns + new_columns]
                else:
                    results_df = results_df[all_columns]
            except Exception as e:
                print(f"Warning: Error reading existing file header: {e}")
                # Fall back to using the current DataFrame as is
        
        # Save to CSV
        results_df.to_csv(output_file, mode=mode, header=header, index=False)
        print(f"Saved {len(place_details)} records to {output_file}")
    except Exception as e:
        print(f"Error saving results: {e}")
        # Emergency backup - save the raw data
        with open(f"place_details_backup_{int(time.time())}.json", "w") as f:
            json.dump(place_details, f)


def save_checkpoint(place_ids, checkpoint_file):
    """Save checkpoint with remaining place IDs"""
    state_data = {"place_id": place_ids}
    tmp_filename = checkpoint_file + ".tmp"
    
    try:
        # Write to temporary file first
        with open(tmp_filename, "wb") as f:
            pickle.dump(state_data, f)
        
        # Atomic replacement
        os.replace(tmp_filename, checkpoint_file)
    except Exception as e:
        print(f"Error saving checkpoint: {e}")


if __name__ == "__main__":
    main()