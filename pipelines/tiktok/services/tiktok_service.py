import os
import ast
import time
import json
import requests
import pandas as pd
import csv
from typing import List, Dict, Any, Optional, Union, Tuple, BinaryIO
from datetime import datetime
from io import BytesIO
from thefuzz import fuzz
from pathlib import Path

from ..config import (
    TIKTOK_API_CONFIG,
    SEARCH_CONFIG,
    VIDEO_CONFIG,
    VIDEOS_DIR,
    LOGS_DIR
)
from ..utils.logger import setup_logger

logger = setup_logger(__name__)

class TikTokService:
    """Service for interacting with TikTok APIs to search, get info, and download videos."""
    
    def __init__(self):
        """Initialize the TikTok service with configuration."""
        self.base_url = TIKTOK_API_CONFIG['base_url']
        self.headers = TIKTOK_API_CONFIG['headers']
        self.endpoints = TIKTOK_API_CONFIG['endpoints']
        self.rate_limit = TIKTOK_API_CONFIG['rate_limit']
        
        # Create video directory if it doesn't exist
        VIDEOS_DIR.mkdir(parents=True, exist_ok=True)
    
    def search_videos(
        self, 
        keywords: str, 
        count: int = SEARCH_CONFIG['videos_per_request'],
        cursor: str = "0"
    ) -> Tuple[List[Dict], str]:
        """
        Search for TikTok videos by keywords.
        
        Args:
            keywords: Search terms
            count: Number of videos to retrieve
            cursor: Pagination cursor
            
        Returns:
            Tuple containing (list of video data, next pagination cursor)
        """
        url = f"{self.base_url}{self.endpoints['search']}"
        querystring = {
            "keywords": keywords,
            "count": str(count),
            "cursor": cursor
        }
        
        for attempt in range(self.rate_limit['max_retries']):
            try:
                response = requests.get(
                    url, 
                    headers=self.headers, 
                    params=querystring
                )
                response.raise_for_status()
                response_json = response.json()
                
                if response_json.get("code") == 0 and response_json.get("data", {}).get("videos"):
                    videos = response_json["data"]["videos"]
                    next_cursor = response_json["data"].get("cursor", "0")
                    return videos, next_cursor
                else:
                    error_message = response_json.get("message", "Unknown error")
                    logger.error(f"TikTok search API error: {error_message}")
                    if attempt < self.rate_limit['max_retries'] - 1:
                        time.sleep(self.rate_limit['retry_delay'])
                        continue
                    return [], cursor
                    
            except Exception as e:
                logger.error(f"Error searching TikTok videos: {e}")
                if attempt < self.rate_limit['max_retries'] - 1:
                    time.sleep(self.rate_limit['retry_delay'])
                    continue
                return [], cursor
            finally:
                time.sleep(self.rate_limit['request_delay'])
    
    def get_video_download_url(self, tiktok_url: str) -> Optional[str]:
        """
        Get the actual download URL for a TikTok video.
        
        Args:
            tiktok_url: Public TikTok video URL
            
        Returns:
            Direct download URL for the video or None if unsuccessful
        """
        url = f"{self.base_url}{self.endpoints['video_info']}"
        querystring = {
            "url": tiktok_url,
            "hd": "1" if VIDEO_CONFIG['download_hd'] else "0"
        }
        
        for attempt in range(self.rate_limit['max_retries']):
            try:
                response = requests.get(
                    url, 
                    headers=self.headers, 
                    params=querystring
                )
                response.raise_for_status()
                response_json = response.json()
                
                if response_json.get("code") == 0 and response_json.get("data"):
                    download_url = response_json.get("data").get("play")
                    return download_url
                else:
                    error_message = response_json.get("message", "Unknown error")
                    logger.error(f"Failed to get download URL: {error_message}")
                    if attempt < self.rate_limit['max_retries'] - 1:
                        time.sleep(self.rate_limit['retry_delay'])
                        continue
                    return None
            except Exception as e:
                logger.error(f"Error getting download URL: {e}")
                if attempt < self.rate_limit['max_retries'] - 1:
                    time.sleep(self.rate_limit['retry_delay'])
                    continue
                return None
            finally:
                time.sleep(self.rate_limit['request_delay'])
    
    def download_video(self, download_url: str, video_id: str) -> Optional[Path]:
        """
        Download a TikTok video using the direct download URL.
        
        Args:
            download_url: Direct video download URL
            video_id: TikTok video ID for filename
            
        Returns:
            Path to downloaded video or None if unsuccessful
        """
        output_path = VIDEOS_DIR / f"{video_id}.mp4"
        
        try:
            resp = requests.get(download_url, stream=True)
            resp.raise_for_status()
            
            with open(output_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=VIDEO_CONFIG['chunk_size']):
                    if chunk:
                        f.write(chunk)
            
            logger.info(f"Video downloaded successfully to {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Error downloading video: {e}")
            return None
    
    def create_tiktok_url(self, author_unique_id: str, video_id: str) -> str:
        """
        Create a public TikTok URL from author ID and video ID.
        
        Args:
            author_unique_id: TikTok username/unique ID
            video_id: TikTok video ID
            
        Returns:
            Public TikTok URL
        """
        return f"https://www.tiktok.com/@{author_unique_id}/video/{video_id}"
        
    def process_video_for_restaurant(
        self, 
        video: Dict, 
        restaurant_name: str,
        relevance_threshold: int = SEARCH_CONFIG['relevance_threshold']
    ) -> Dict:
        """
        Process a video for a specific restaurant, checking relevance.
        
        Args:
            video: Video data from the search API
            restaurant_name: Name of the restaurant for relevance check
            relevance_threshold: Minimum relevance score (0-100) to consider
            
        Returns:
            Dictionary with processed video information including relevance score
        """
        # Create public URL
        author_unique_id = video["author"]["unique_id"]
        video_id = video["video_id"]
        tiktok_url = self.create_tiktok_url(author_unique_id, video_id)
        
        # Calculate relevance score
        video_title = video.get("title", "")
        video_caption = video.get("caption", "")
        search_keyword = video.get("search_keyword", "")
        
        score_title = fuzz.partial_ratio(restaurant_name.lower(), video_title.lower())
        score_keyword = fuzz.partial_ratio(restaurant_name.lower(), search_keyword.lower())
        score_caption = fuzz.partial_ratio(restaurant_name.lower(), video_caption.lower()) if video_caption else 0
        
        fuzzy_score = max(score_title, score_keyword * 0.9, score_caption * 0.8)
        
        result = {
            "tiktok_url": tiktok_url,
            "author_id": author_unique_id,
            "video_id": video_id,
            "title": video_title,
            "caption": video_caption,
            "relevance_score": fuzzy_score,
            "meets_threshold": fuzzy_score >= relevance_threshold,
            "likes": video.get("likes", 0),
            "comments": video.get("comments", 0),
            "shares": video.get("shares", 0),
            "created_at": video.get("created_at", "")
        }
        
        return result