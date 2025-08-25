from utils.sheets import get_sheet, get_rows_to_process, batch_update_rows
from utils.gmaps import get_metadata, get_geocode, calculate_heading
from utils.cloud import upload_to_cloudinary
from utils.keys import GOOGLE_API_KEY

import concurrent.futures
import threading
import time

# Thread-safe list to collect updates
update_results = []
update_lock = threading.Lock()

def process_row(index, row):
    try:
        full_address = f"{row['address']}, {row['city']}, {row['state']} {row['zip_code']}"
        print(f"Processing: {full_address}")

        # Get coordinates
        target_lat, target_lng = get_geocode(full_address)
        pano_lat, pano_lng, pano_id = get_metadata(target_lat, target_lng)

        if not pano_id or None in [pano_lat, pano_lng, target_lat, target_lng]:
            raise ValueError("Missing required location data.")

        heading = calculate_heading(pano_lat, pano_lng, target_lat, target_lng)

        # Build Street View image URL
        image_url = (
            f"https://maps.googleapis.com/maps/api/streetview?"
            f"size=560x430&pano={pano_id}&heading={heading}&pitch=10&fov=70"
            f"&key={GOOGLE_API_KEY}"
        )

        # Build Cloudinary filename
        street = row['address'].replace(",", "").replace(".", "").strip().replace(" ", "_")
        city = row['city'].replace(",", "").replace(".", "").strip().replace(" ", "_")
        zip_code = str(row['zip_code']).strip()
        public_id = f"{street}_{city}_{zip_code}".lower()

        # Upload to Cloudinary
        hosted_url = upload_to_cloudinary(image_url, public_id=public_id)

        # Store result for batch update
        with update_lock:
            update_results.append((index, hosted_url, "Success"))
            
    except Exception as e:
        with update_lock:
            update_results.append((index, "", f"Error: {str(e)}"))
        print(f"Error on row {index + 1}: {e}")

def main():
    global update_results
    update_results = []
    
    start_time = time.time()
    print("Starting batch...")
    
    sheet = get_sheet()
    rows_with_indices = get_rows_to_process(sheet)
    print(f"Starting batch - {len(rows_with_indices)} rows to process.")

    if not rows_with_indices:
        print("No rows to process!")
        return

    processing_start = time.time()
    # Process all rows concurrently with higher thread count
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_row, i, row) for i, row in rows_with_indices]
        concurrent.futures.wait(futures)
    
    processing_end = time.time()
    processing_time = processing_end - processing_start

    # Batch update all results to Google Sheets in single operation
    print("Updating Google Sheets...")
    update_start = time.time()
    if update_results:
        batch_update_rows(sheet, update_results)
        print(f"Updated {len(update_results)} rows in Google Sheets")
    update_end = time.time()
    update_time = update_end - update_start
    
    total_time = time.time() - start_time
    
    print("\n--- TIMING RESULTS ---")
    print(f"Processing time: {processing_time:.2f} seconds")
    print(f"Google Sheets update time: {update_time:.2f} seconds") 
    print(f"Total execution time: {total_time:.2f} seconds")
    print(f"Average per record: {total_time/len(rows_with_indices):.2f} seconds")
    print("Batch complete.")

if __name__ == "__main__":
    main()
