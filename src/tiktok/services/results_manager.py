import os
import ast
import time
import json
import pandas as pd
import csv
from typing import List, Dict, Any, Optional, Union
from datetime import datetime




class ResultsManager:
    """Manager for collecting and storing video results for restaurants."""
    
    def __init__(self):
        """Initialize the results manager."""
        self.results = []
    
    def add_video_result(
            self,
            restaurant_id: Union[int, str],
            restaurant_name: str,
            tiktok_url: str,
            download_url: str, 
            relevance_score: int,
            video_title: str,
            video_caption: str,
            transcript: Dict[str, Any]
            ) -> None:
        """
        Add video result for a restaurant.
        
        Args:
            restaurant_id: ID of the restaurant
            restaurant_name: Name of the restaurant
            tiktok_url: TikTok video URL
            download_url: Direct download URL for the video
            relevance_score: Relevance score (0-100)
            video_title: Title of the video
            video_caption: Caption of the video
            transcript: Transcript data
        """
        # Create result entry
        result = {
            "restaurant_id": restaurant_id,
            "restaurant_name": restaurant_name,
            "tiktok_url": tiktok_url,
            "download_url": download_url,
            "relevance_score": relevance_score,
            "video_title": video_title,
            "video_caption": video_caption,
            "transcript": transcript
        }
        
        self.results.append(result)
        print(f"Added video result for restaurant {restaurant_name} (ID: {restaurant_id})")
    
    def get_all_results(self) -> List[Dict[str, Any]]:
        """
        Get all collected video results.
        
        Returns:
            List of all video results
        """
        return self.results
    
    def save_results(self, output_path: Optional[str] = None, format: str = 'csv') -> str:
        """
        Save collected results to a file.
        
        Args:
            output_path: Path to save the results. If None, a default path will be used.
            format: Format to save the results in ('csv', 'json')
            
        Returns:
            Path where the results were saved
        """
        if not self.results:
            print("Warning: No results to save")
            return ""
            
        # Generate default filename if none provided
        if not output_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if format.lower() == 'csv':
                output_path = f"restaurant_video_transcripts_{timestamp}.csv"
            elif format.lower() == 'json':
                output_path = f"restaurant_video_transcripts_{timestamp}.json"
            else:
                output_path = f"restaurant_video_transcripts_{timestamp}.csv"
                format = 'csv'
        
        try:
            # Save based on format
            if format.lower() == 'csv':
                results_df = pd.DataFrame(self.results)
                results_df.to_csv(output_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
            elif format.lower() == 'json':
                with open(output_path, 'w') as f:
                    json.dump(self.results, f, indent=2)
            else:
                print(f"Unsupported format: {format}. Defaulting to CSV.")
                results_df = pd.DataFrame(self.results)
                results_df.to_csv(output_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
                
            print(f"Saved {len(self.results)} results to {output_path}")
            return output_path
            
        except Exception as e:
            print(f"Error saving results: {e}")
            return ""

