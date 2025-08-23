from utils.sheets import get_sheet, get_rows_to_process, update_row
from utils.gmaps import get_geocode, get_metadata, calculate_heading
from utils.cloud import upload_to_cloudinary
from utils.keys import GOOGLE_API_KEY

print("🚀 Starting batch...")

sheet = get_sheet()
rows = get_rows_to_process(sheet)

print(f"🚀 Starting batch — {len(rows)} rows to process.")

for i, row in enumerate(rows):
    try:
        # Step 1: Build full address
        full_address = f"{row['address']}, {row['city']}, {row['state']} {row['zip_code']}"
        print(f"📍 Processing: {full_address}")

        # Step 2: Get coordinates
        target_lat, target_lng = get_geocode(full_address)
        pano_lat, pano_lng, pano_id = get_metadata(target_lat, target_lng)

        if not pano_id or None in [pano_lat, pano_lng, target_lat, target_lng]:
            raise ValueError("❌ Missing required location data.")

        # Step 3: Calculate heading
        heading = calculate_heading(pano_lat, pano_lng, target_lat, target_lng)

        # Step 4: Build Google Street View image URL
        image_url = (
            f"https://maps.googleapis.com/maps/api/streetview?"
            f"size=560x430&pano={pano_id}&heading={heading}&pitch=10&fov=70"
            f"&key={GOOGLE_API_KEY}"
        )

        # Step 5: Format file name
        street = row['address'].replace(",", "").replace(".", "").strip().replace(" ", "_")
        city = row['city'].replace(",", "").replace(".", "").strip().replace(" ", "_")
        zip_code = str(row['zip_code']).strip()
        public_id = f"{street}_{city}_{zip_code}".lower()

        # Step 6: Upload to Cloudinary with correct filename
        hosted_url = upload_to_cloudinary(image_url, public_id=public_id)

        # Step 7: Write to sheet
        update_row(sheet, i, hosted_url, "✅ Success")

    except Exception as e:
        update_row(sheet, i, "", f"❌ Error: {str(e)}")
        print(f"❌ Error on row {i + 1}: {e}")

print("🎉 Batch complete.")
