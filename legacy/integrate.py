import requests
import os
import pandas as pd
import ast
import time
import tempfile
from thefuzz import fuzz
from dotenv import load_dotenv

# Load your environment variables
load_dotenv()

PROJECT_ID = os.environ.get("GCP_PROJECT_ID") 

def get_video_download_url(tiktok_url:str)->str:
    """
    Get the actual download URL for a TikTok video
    """
    url:str = "https://tiktok-api15.p.rapidapi.com/index/Tiktok/getVideoInfo"
    
    querystring:dict = {"url": tiktok_url, "hd": "1"}
    
    headers = {
        "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
        "x-rapidapi-host": "tiktok-api15.p.rapidapi.com"
    }
    
    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        response_json = response.json()
        
        # Extract the download URL from the response
        # Note: You may need to adjust this based on the actual response structure
        if response_json.get("code") == 0 and response_json.get("data"):
            download_url = response_json.get("data").get("play")
            return download_url
        else:
            print(f"Failed to get download URL: {response_json.get('message', 'Unknown error')}")
            return None
    except Exception as e:
        print(f"Error getting download URL: {e}")
        return None

def download_tiktok_video(download_url, output_path):
    """
    Download a TikTok video using the direct download URL
    """
    try:
        resp = requests.get(download_url, stream=True)
        resp.raise_for_status()
        
        with open(output_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive chunks
                    f.write(chunk)
        
        print(f"Video downloaded successfully to {output_path}")
        return True
    except Exception as e:
        print(f"Error downloading video: {e}")
        return False

# Function to send video to transcription service
def transcribe_video(video_path, transcription_endpoint):
    """
    Send video to the transcription service and return the transcript
    """
    try:
        with open(video_path, 'rb') as video_file:
            files = {'file': video_file}
            response = requests.post(transcription_endpoint, files=files)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Transcription failed with status {response.status_code}: {response.text}")
            return None
    except Exception as e:
        print(f"Error during transcription request: {e}")
        return None

# Main function to process restaurants and videos
def process_restaurants_videos(csv_path:str, transcription_endpoint, limit:int=5):
    # Read restaurant data
    df = pd.read_csv(csv_path, nrows=limit)
    df.reset_index(inplace=True)
    df.rename(columns={"index": "id"}, inplace=True)
    
    # Parse types and generate search keywords
    df['types_list'] = df['types'].apply(ast.literal_eval)
    df['search_keywords'] = df.apply(
        lambda row: f"{row['name']} {row['vicinity']} food",
        axis=1
    )
    
    # API configuration
    tiktok_api_url = "https://tiktok-api15.p.rapidapi.com/index/Tiktok/searchVideoListByKeywords"
    headers = {
        "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
        "x-rapidapi-host": "tiktok-api15.p.rapidapi.com"
    }
    
    # Results container
    results = []
    
    # Process each restaurant
    for idx, row in df.iterrows():
        restaurant_id = row['id']
        restaurant_name = row['name']
        search_keywords = row['search_keywords']
        vicinity = row['vicinity']
        
        print(f"Processing restaurant {idx+1}/{len(df)}: {restaurant_name}")
        
        # Query TikTok API
        querystring = {
            "keywords": search_keywords,
            "count": "5",  # Limit to 3 videos per restaurant
            "cursor": "0"
        }
        
        try:
            response = requests.get(tiktok_api_url, headers=headers, params=querystring)
            response_json = response.json()
            
            if response_json.get("code") == 0 and response_json.get("data", {}).get("videos"):
                videos = response_json["data"]["videos"]
                
                # Process each video
                for video_idx, video in enumerate(videos):
                    # Create public URL
                    author_unique_id = video["author"]["unique_id"]
                    video_id = video["video_id"]
                    tiktok_url = f"https://www.tiktok.com/@{author_unique_id}/video/{video_id}"
                    
                    # Calculate relevance score
                    video_title = video.get("title", "")
                    video_caption = video.get("caption", "")
                    score_title = fuzz.partial_ratio(restaurant_name.lower(), video_title.lower())
                    score_caption = fuzz.partial_ratio(restaurant_name.lower(), video_caption.lower()) if video_caption else 0
                    fuzzy_score = max(score_title, score_caption)/100
                    
                    print(f"  Video {video_idx+1}: {tiktok_url} (Relevance: {fuzzy_score}%)")
                    
                    # Only process videos with decent relevance
                    if fuzzy_score >= 0.3:
                        # Get the direct download URL
                        download_url = get_video_download_url(tiktok_url)
                        
                        if download_url:
                            # Create a temporary file for the video
                            with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as temp_video:
                                # Download the video
                                if download_tiktok_video(download_url, temp_video.name):
                                    # Send for transcription
                                    print(f"  Sending video for transcription...")
                                    transcript_data = transcribe_video(temp_video.name, transcription_endpoint)
                                    
                                    if transcript_data:
                                        # Store results
                                        results.append({
                                            "restaurant_id": restaurant_id,
                                            "restaurant_name": restaurant_name,
                                            "vicinity": vicinity,
                                            "search_keywords": search_keywords,
                                            "tiktok_url": tiktok_url,
                                            "download_url": download_url,
                                            "S_title": fuzzy_score,
                                            "video_title": video_title,
                                            "video_caption": video_caption,
                                            "transcript": transcript_data
                                        })
                                        print(f"  Transcription complete and stored.")
                                    else:
                                        print(f"  Transcription failed.")
                                else:
                                    print(f"  Failed to download video.")
                                
                                # Clean up temp file
                                try:
                                    os.unlink(temp_video.name)
                                except:
                                    pass
                        else:
                            print(f"  Could not get download URL for the video.")
                    else:
                        print(f"  Video relevance below threshold, skipping.")
            else:
                print(f"  No videos found for {restaurant_name}")
        
        except Exception as e:
            print(f"Error processing {restaurant_name}: {e}")
        
        # Avoid rate limiting
        time.sleep(1)
    
    return results

# Example usage
if __name__ == "__main__":
    csv_path = "london_restaurants_20250505_151833.csv"
    # Replace with your actual Firebase function URL
    transcription_endpoint = "https://upload-and-transcribe-lhsmwvub5q-uc.a.run.app"
    
    results = process_restaurants_videos(csv_path, transcription_endpoint, limit=5)
    
    # Save results to file
    results_df = pd.DataFrame(results)
    results_df.to_csv("london_restaurant_video_transcripts_v2.csv", index=False)
    print(f"Saved results for {len(results)} videos to restaurant_video_transcripts.csv")