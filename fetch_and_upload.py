import requests
import cloudinary.uploader
import time

# Import helpers
from utils.sheets import update_row
from utils.keys import GOOGLE_API_KEY, CLOUDINARY_CONFIG

# Configure Cloudinary
cloudinary.config(
    cloud_name=CLOUDINARY_CONFIG["cloud_name"],
    api_key=CLOUDINARY_CONFIG["api_key"],
    api_secret=CLOUDINARY_CONFIG["api_secret"]
)

# --- Helper Functions ---

def build_address(row):
    return f"{row['address']}, {row['city']}, {row['state']} {row['zip_code']}"

def get_coords(address):
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={GOOGLE_API_KEY}"
    res = requests.get(url).json()
    if res["status"] != "OK":
        return None
    location = res["results"][0]["geometry"]["location"]
    return location["lat"], location["lng"]

def get_pano_metadata(lat, lng):
    url = f"https://maps.googleapis.com/maps/api/streetview/metadata?location={lat},{lng}&key={GOOGLE_API_KEY}"
    res = requests.get(url).json()
    if res["status"] != "OK":
        return None
    return res.get("pano_id")

def download_street_view(pano_id, heading=0):
    url = f"https://maps.googleapis.com/maps/api/streetview?size=560x430&pano={pano_id}&heading={heading}&pitch=10&fov=70&key={GOOGLE_API_KEY}"
    res = requests.get(url)
    if res.status_code != 200:
        return None
    return res.content

def upload_to_cloudinary(image_bytes, public_id=None):
    res = cloudinary.uploader.upload(image_bytes, public_id=public_id, resource_type="image")
    return res["secure_url"]

# --- Main Handler for a Row ---

def process_row(row, index, sheet):
    address = build_address(row)
    print(f"📍 Processing: {address}")

    coords = get_coords(address)
    if not coords:
        update_row(sheet, index, "", "❌ No coordinates")
        return

    pano_id = get_pano_metadata(*coords)
    if not pano_id:
        update_row(sheet, index, "", "❌ No panorama")
        return

    image = download_street_view(pano_id)
    if not image:
        update_row(sheet, index, "", "❌ Image download failed")
        return

    try:
        public_id = f"street_view/{row['zip_code']}_{index}"
        url = upload_to_cloudinary(image, public_id=public_id)
        update_row(sheet, index, url, "✅ Success")
        print(f"✅ Uploaded: {url}")
    except Exception as e:
        update_row(sheet, index, "", f"❌ Upload failed: {str(e)}")

    time.sleep(0.25)  # avoid hitting rate limits
