import requests
import cloudinary
import cloudinary.uploader
from utils.keys import CLOUDINARY_CONFIG

# ✅ CONFIGURE CLOUDINARY WITH YOUR KEYS
cloudinary.config(
    cloud_name=CLOUDINARY_CONFIG["cloud_name"],
    api_key=CLOUDINARY_CONFIG["api_key"],
    api_secret=CLOUDINARY_CONFIG["api_secret"]
)

def upload_to_cloudinary(image_url, public_id=None):
    response = requests.get(image_url, stream=True)
    if response.status_code != 200:
        raise Exception(f"Failed to download image: {response.status_code}")

    upload_options = {"resource_type": "image"}
    if public_id:
        upload_options["public_id"] = public_id

    upload_result = cloudinary.uploader.upload(response.raw, **upload_options)
    return upload_result["secure_url"]
