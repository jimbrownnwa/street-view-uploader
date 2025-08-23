import math

def calculate_heading(lat1, lon1, lat2, lon2):
    """Calculate the compass bearing from point A to point B"""
    d_lon = math.radians(lon2 - lon1)
    lat1 = math.radians(lat1)
    lat2 = math.radians(lat2)

    x = math.sin(d_lon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - (
        math.sin(lat1) * math.cos(lat2) * math.cos(d_lon)
    )

    initial_heading = math.atan2(x, y)
    heading_degrees = (math.degrees(initial_heading) + 360) % 360
    return heading_degrees
