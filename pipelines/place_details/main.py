import requests
import os
import pandas as pd
import json




place_id:str = "ChIJwUmurTkPZ0gRep1czgc57Nk"
api_key:str = os.getenv("SHEGZ_MAPS_API_KEY")
url = f"https://maps.googleapis.com/maps/api/place/details/json?fields=name,rating,formatted_phone_number,dine_in&place_id={place_id}&key={api_key}"

response = requests.get(url)
if response.status_code == 200:
    data = response.json()
    print(json.dumps(data, indent=2))
else:
    print(f"Error: {response.status_code}")