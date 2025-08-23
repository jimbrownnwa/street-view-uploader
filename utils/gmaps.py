import requests
from math import atan2, degrees
from .keys import GOOGLE_API_KEY



def get_geocode(address):
    print("📡 get_geocode is using requests properly")
    ...

    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={GOOGLE_API_KEY}"
    res = requests.get(url)
    res.raise_for_status()
    data = res.json()
    if data['status'] != 'OK' or not data['results']:
        return None, None
    location = data['results'][0]['geometry']['location']
    return location['lat'], location['lng']


def get_metadata(lat, lng):
    url = f"https://maps.googleapis.com/maps/api/streetview/metadata?location={lat},{lng}&key={GOOGLE_API_KEY}"
    res = requests.get(url)
    res.raise_for_status()
    data = res.json()
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
