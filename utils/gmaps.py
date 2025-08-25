import requests
import time
from math import atan2, degrees
from .keys import GOOGLE_API_KEY

# Reuse session for better performance
session = requests.Session()

# Rate limiting
last_api_call = 0
MIN_API_INTERVAL = 0.02  # 50 QPS limit = 20ms between calls

def rate_limit():
    """Enforce rate limiting between API calls"""
    global last_api_call
    current_time = time.time()
    elapsed = current_time - last_api_call
    if elapsed < MIN_API_INTERVAL:
        time.sleep(MIN_API_INTERVAL - elapsed)
    last_api_call = time.time()

def get_geocode(address):
    rate_limit()
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={GOOGLE_API_KEY}"
    res = session.get(url, timeout=30)
    res.raise_for_status()
    data = res.json()
    
    # Handle rate limiting responses
    if data.get('status') == 'OVER_QUERY_LIMIT':
        raise Exception("Google Maps API rate limit exceeded")
    
    if data['status'] != 'OK' or not data['results']:
        return None, None
    location = data['results'][0]['geometry']['location']
    return location['lat'], location['lng']


def get_metadata(lat, lng):
    rate_limit()
    url = f"https://maps.googleapis.com/maps/api/streetview/metadata?location={lat},{lng}&key={GOOGLE_API_KEY}"
    res = session.get(url, timeout=30)
    res.raise_for_status()
    data = res.json()
    
    # Handle rate limiting responses
    if data.get('status') == 'OVER_QUERY_LIMIT':
        raise Exception("Google Maps API rate limit exceeded")
    
    if data['status'] != 'OK' or 'pano_id' not in data:
        return None, None, None
    return data['location']['lat'], data['location']['lng'], data['pano_id']


def calculate_heading(lat1, lon1, lat2, lon2):
    delta_lon = lon2 - lon1
    x = atan2(
        degrees(delta_lon),
        degrees(lat2 - lat1)
    )
    heading = (degrees(x) + 360) % 360
    return round(heading)
