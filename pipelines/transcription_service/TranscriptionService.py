import os
import requests



# Pseudo code for the TranscriptionService class
# This is a simplified version of the class based on the provided code snippet
# Note: The actual implementation may vary based on the specific requirements and libraries used
# Import necessary libraries


# Service input
# [ Tiktok URL,  ]

# get video download URL
# download video

###### Process video
# declare video key
# send video to transcription service
# flatted transcription column

class TranscriptionService:
    def __init__(self):
        self.endpoint:str = "https://upload-and-transcribe-lhsmwvub5q-uc.a.run.app"
        self.tiktok_url:str = ""
        self.download_url = ""
        self.video_output_path = "opt/data/videos"
        pass

    def get_video_download_url(self)-> str | None:
        """
        Get the actual download URL for a TikTok video
        """

        url:str = "https://tiktok-api15.p.rapidapi.com/index/Tiktok/getVideoInfo"
        
        querystring:dict = {"url": self.tiktok_url, "hd": "1"}
        
        headers = {
            "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
            "x-rapidapi-host": "tiktok-api15.p.rapidapi.com"
        }
        
        try:
            response = requests.get(url, headers=headers, params=querystring)
            response.raise_for_status()
            response_json = response.json()
            
            # Extract the download URL from the response
            if response_json.get("code") == 0 and response_json.get("data"):
                self.download_url = response_json.get("data").get("play")
                return self.download_url
            else:
                print(f"Failed to get download URL: {response_json.get('message', 'Unknown error')}")
                return None
        except Exception as e:
            print(f"Error getting download URL: {e}")
            return None
        

    def download_tiktok_video(self) -> bool:
        """
        Download a TikTok video using the direct download URL
        """
        try:
            resp = requests.get(self.download_url, stream=True)
            resp.raise_for_status()
            
            with open(self.video_output_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive chunks
                        f.write(chunk)
            
            print(f"Video downloaded successfully to {self.video_output_path}")
            return True
        except Exception as e:
            print(f"Error downloading video: {e}")
            return False
    
    def transcribe_video(self) -> str | None:
        """
        Send video to the transcription service and return the transcript
        """
        try:
            with open(self.video_output_path, 'rb') as video_file:
                files = {'file': video_file}
                response = requests.post(self.endpoint, files=files)
            
            if response.status_code == 200:
                transcript = response.json().get('transcript')
                return transcript
            else:
                print(f"Error in transcription: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"Error sending video for transcription: {e}")
            return None