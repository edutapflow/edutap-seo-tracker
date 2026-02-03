import requests
import base64
from config import API_LOGIN, API_PASSWORD

def find_city_code(city_name):
    url = f"https://api.dataforseo.com/v3/serp/google/locations/in"
    auth = "Basic " + base64.b64encode(f"{API_LOGIN}:{API_PASSWORD}".encode()).decode()
    headers = {'Authorization': auth}
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        locations = response.json()['tasks'][0]['result']
        for loc in locations:
            if city_name.lower() in loc['location_name'].lower():
                print(f"✅ Found it! {loc['location_name']} -> Code: {loc['location_code']}")
    else:
        print("Error fetching locations")

# Run this
find_city_code("Panchkula")
