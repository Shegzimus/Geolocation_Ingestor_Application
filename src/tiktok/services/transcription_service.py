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




class TranscriptionService:
    """Service for handling video transcription operations through external APIs."""
    
    def __init__(self, endpoint_url: str = None):
        """
        Initialize the transcription service.
        
        Args:
            endpoint_url: URL of the transcription service endpoint.
                          If None, will try to get from environment.
        """
        self.endpoint_url = endpoint_url or os.getenv("TRANSCRIPTION_ENDPOINT")
        if not self.endpoint_url:
            raise ValueError("Transcription endpoint URL is required but not provided")
        
        # Settings
        self.request_timeout = 180  # seconds to wait for transcription response
        self.retry_count = 3        # number of times to retry on failure
        self.retry_delay = 2        # seconds to wait between retries
    
    def transcribe_video(
            self,
            video_path: str,
            additional_params: Dict = None
            ) -> Optional[Dict[str, Any]]:
        """
        Send a video file to the transcription service and return the transcript.
        
        Args:
            video_path: Path to the video file to transcribe
            additional_params: Any additional parameters to send with the request
            
        Returns:
            Dictionary containing transcription results or None if unsuccessful
        """
        if not os.path.exists(video_path):
            print(f"Error: Video file not found at {video_path}")
            return None
        
        params = additional_params or {}
        
        try:
            with open(video_path, 'rb') as video_file:
                return self._send_transcription_request(video_file, params)
        except Exception as e:
            print(f"Error opening video file {video_path}: {e}")
            return None
    
    def transcribe_video_from_bytes(self, 
                                   video_bytes: bytes, 
                                   filename: str = "video.mp4",
                                   additional_params: Dict = None) -> Optional[Dict[str, Any]]:
        """
        Send video bytes to the transcription service and return the transcript.
        
        Args:
            video_bytes: Raw bytes of the video to transcribe
            filename: Filename to use in the request
            additional_params: Any additional parameters to send with the request
            
        Returns:
            Dictionary containing transcription results or None if unsuccessful
        """
        params = additional_params or {}
        file_obj = BytesIO(video_bytes)
        file_obj.name = filename
        
        try:
            return self._send_transcription_request(file_obj, params)
        except Exception as e:
            print(f"Error transcribing video from bytes: {e}")
            return None
    
    def _send_transcription_request(
            self,
            file_obj: BinaryIO,
            params: Dict) -> Optional[Dict[str, Any]]:
        """
        Internal method to send the transcription request.
        
        Args:
            file_obj: File-like object containing the video
            params: Additional parameters for the request
            
        Returns:
            Dictionary containing transcription results or None if unsuccessful
        """
        files = {'file': file_obj}
        
        for attempt in range(self.retry_count):
            try:
                response = requests.post(
                    self.endpoint_url,
                    files=files,
                    data=params,
                    timeout=self.request_timeout
                )
                
                # Check if request was successful
                if response.status_code == 200:
                    try:
                        return response.json()
                    except json.JSONDecodeError:
                        print(f"Error: Received non-JSON response: {response.text[:100]}...")
                        return None
                else:
                    print(f"Transcription request failed (attempt {attempt+1}/{self.retry_count})")
                    print(f"Status code: {response.status_code}")
                    print(f"Response: {response.text[:200]}...")
                    
                    # If we haven't exhausted our retries, wait and try again
                    if attempt < self.retry_count - 1:
                        time.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff
                    else:
                        return None
                        
            except requests.exceptions.Timeout:
                print(f"Timeout during transcription request (attempt {attempt+1}/{self.retry_count})")
                if attempt < self.retry_count - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    return None
                    
            except requests.exceptions.RequestException as e:
                print(f"Error during transcription request (attempt {attempt+1}/{self.retry_count}): {e}")
                if attempt < self.retry_count - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    return None
        
        return None
    
    def extract_transcript_text(
            self,
            transcript_data: Dict[str, Any],
            format_type: str = "plain"
            ) -> Optional[str]:
        """
        Extract plain text from transcription result.
        
        Args:
            transcript_data: The transcript data returned from the service
            format_type: Output format - "plain" or "with_timestamps"
            
        Returns:
            String containing the transcript text or None if unsuccessful
        """
        try:
            # This implementation assumes a specific transcript structure
            # Adjust according to your actual API response format
            if not transcript_data:
                return None
                
            # Example implementation - adjust based on your actual transcription service's response format
            if "transcript" in transcript_data:
                if format_type == "plain":
                    return transcript_data["transcript"]
                elif format_type == "with_timestamps" and "segments" in transcript_data:
                    segments = transcript_data["segments"]
                    formatted_transcript = []
                    for segment in segments:
                        start_time = segment.get("start", 0)
                        text = segment.get("text", "")
                        formatted_transcript.append(f"[{self._format_time(start_time)}] {text}")
                    return "\n".join(formatted_transcript)
                else:
                    return transcript_data["transcript"]
            else:
                print("Error: Transcript data does not contain expected fields")
                return None
                
        except Exception as e:
            print(f"Error extracting transcript text: {e}")
            return None
    
    def _format_time(self, seconds: Union[int, float]) -> str:
        """
        Format seconds into MM:SS format.
        
        Args:
            seconds: Time in seconds
            
        Returns:
            Formatted time string
        """
        minutes = int(seconds // 60)
        seconds = int(seconds % 60)
        return f"{minutes:02d}:{seconds:02d}"
