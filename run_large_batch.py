from utils.sheets import get_sheet, get_rows_to_process, batch_update_rows
from utils.gmaps import get_metadata, get_geocode, calculate_heading
from utils.cloud import upload_to_cloudinary
from utils.keys import GOOGLE_API_KEY

import concurrent.futures
import threading
import time
import json
from datetime import datetime

# Configuration for large batches
CHUNK_SIZE = 250
INTER_CHUNK_DELAY = 30  # seconds
MAX_WORKERS = 8  # Balanced between speed and stability
MAX_RETRIES = 1  # Single retry for transient errors only
RETRY_DELAY_BASE = 3  # Reduced delay for single retry

# Thread-safe containers
update_lock = threading.Lock()
progress_file = "batch_progress.json"

def save_progress(chunk_num, total_chunks, completed_records, failed_records):
    """Save progress to file for resumption"""
    progress = {
        "timestamp": datetime.now().isoformat(),
        "chunk_num": chunk_num,
        "total_chunks": total_chunks,
        "completed_records": completed_records,
        "failed_records": failed_records
    }
    with open(progress_file, 'w') as f:
        json.dump(progress, f, indent=2)

def load_progress():
    """Load previous progress if exists"""
    try:
        with open(progress_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return None

def process_row_with_retry(index, row, max_retries=MAX_RETRIES):
    """Process a single row with smart retry logic"""
    for attempt in range(max_retries + 1):  # +1 for initial attempt
        try:
            full_address = f"{row['address']}, {row['city']}, {row['state']} {row['zip_code']}"
            print(f"  Processing: {full_address}")

            # Get coordinates with timeout
            target_lat, target_lng = get_geocode(full_address)
            if not target_lat or not target_lng:
                raise ValueError("Geocoding failed")
            
            pano_lat, pano_lng, pano_id = get_metadata(target_lat, target_lng)
            if not pano_id or None in [pano_lat, pano_lng]:
                raise ValueError("Street View metadata not available")

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
            
            return index, hosted_url, "Success"
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # Don't retry for permanent errors
            if any(permanent_error in error_msg for permanent_error in [
                "street view metadata not available", 
                "geocoding failed", 
                "not found",
                "zero_results"
            ]):
                print(f"    Permanent error on row {index + 1}: {str(e)}")
                return index, "", f"Error: {str(e)}"
            
            # Retry only for transient errors (network, rate limits, etc.)
            if attempt < max_retries:
                delay = RETRY_DELAY_BASE
                print(f"    Retry {attempt + 1}/{max_retries} for row {index + 1} after {delay}s: {str(e)}")
                time.sleep(delay)
            else:
                print(f"    Final error on row {index + 1}: {str(e)}")
                return index, "", f"Error: {str(e)}"

def process_chunk(chunk_data, chunk_num, total_chunks):
    """Process a single chunk of records"""
    print(f"\n=== CHUNK {chunk_num}/{total_chunks} - {len(chunk_data)} records ===")
    chunk_start_time = time.time()
    
    chunk_results = []
    
    # Process chunk with thread pool
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_row_with_retry, i, row) for i, row in chunk_data]
        
        # Wait with timeout to prevent hanging
        for future in concurrent.futures.as_completed(futures, timeout=600):  # 10 minute timeout
            result = future.result()
            if result:
                chunk_results.append(result)
    
    chunk_time = time.time() - chunk_start_time
    successful = len([r for r in chunk_results if r[1]])  # Has image_url
    failed = len(chunk_results) - successful
    
    print(f"Chunk {chunk_num} completed: {successful} successful, {failed} failed ({chunk_time:.1f}s)")
    
    return chunk_results

def main():
    total_start_time = time.time()
    
    # Set up watchdog timer as backup exit mechanism
    import threading
    def emergency_exit():
        time.sleep(1800)  # 30 minutes max runtime
        print("EMERGENCY EXIT: Maximum runtime exceeded!")
        import os
        os._exit(1)
    
    watchdog = threading.Thread(target=emergency_exit, daemon=True)
    watchdog.start()
    
    print("Starting large batch processing...")
    print(f"Configuration: {CHUNK_SIZE} records/chunk, {INTER_CHUNK_DELAY}s delay, {MAX_WORKERS} threads")
    
    # Check for previous progress
    previous_progress = load_progress()
    if previous_progress:
        print(f"Previous run found: Chunk {previous_progress['chunk_num']}/{previous_progress['total_chunks']}")
        try:
            resume = input("Resume from previous run? (y/n): ").lower().strip() == 'y'
            if not resume:
                previous_progress = None
        except EOFError:
            # Auto-skip resume in automated runs
            print("Starting fresh run...")
            previous_progress = None
    
    # Get sheet and rows to process
    sheet = get_sheet()
    all_rows_with_indices = get_rows_to_process(sheet)
    
    if not all_rows_with_indices:
        print("No rows to process!")
        # Clean up progress file if no work to do
        try:
            import os
            os.remove(progress_file)
            print("Cleaned up progress file.")
        except FileNotFoundError:
            pass
        return
    
    total_records = len(all_rows_with_indices)
    total_chunks = (total_records + CHUNK_SIZE - 1) // CHUNK_SIZE
    
    print(f"Total: {total_records} records in {total_chunks} chunks")
    
    # Determine starting point
    start_chunk = 1
    all_results = []
    
    if previous_progress and previous_progress.get('chunk_num', 0) < total_chunks:
        start_chunk = previous_progress['chunk_num'] + 1
        print(f"Resuming from chunk {start_chunk}")
    elif previous_progress and previous_progress.get('chunk_num', 0) >= total_chunks:
        print("Previous run was already completed! Starting fresh.")
        previous_progress = None
        # Clean up old progress file
        try:
            import os
            os.remove(progress_file)
        except FileNotFoundError:
            pass
    
    # Process chunks
    for chunk_num in range(start_chunk, total_chunks + 1):
        # Create chunk
        start_idx = (chunk_num - 1) * CHUNK_SIZE
        end_idx = min(start_idx + CHUNK_SIZE, total_records)
        chunk_data = all_rows_with_indices[start_idx:end_idx]
        
        # Process chunk
        chunk_results = process_chunk(chunk_data, chunk_num, total_chunks)
        all_results.extend(chunk_results)
        
        # Update Google Sheets for this chunk
        if chunk_results:
            print(f"  Updating Google Sheets for chunk {chunk_num}...")
            batch_update_rows(sheet, chunk_results)
            print(f"  Updated {len(chunk_results)} rows")
        
        # Save progress
        completed = len([r for r in all_results if r[1]])
        failed = len(all_results) - completed
        save_progress(chunk_num, total_chunks, completed, failed)
        
        # Inter-chunk delay (except for last chunk)
        if chunk_num < total_chunks:
            print(f"  Waiting {INTER_CHUNK_DELAY}s before next chunk...")
            time.sleep(INTER_CHUNK_DELAY)
    
    # Final summary
    print("DEBUG: Starting final summary...")
    total_time = time.time() - total_start_time
    successful_records = len([r for r in all_results if r[1]])
    failed_records = len(all_results) - successful_records
    print("DEBUG: Calculations complete, printing results...")
    
    print("\n" + "="*60)
    print("FINAL RESULTS")
    print("="*60)
    print(f"Total records processed: {len(all_results)}")
    print(f"Successful: {successful_records}")
    print(f"Failed: {failed_records}")
    print(f"Success rate: {successful_records/len(all_results)*100:.1f}%")
    print(f"Total execution time: {total_time/60:.1f} minutes")
    print(f"Average per record: {total_time/len(all_results):.2f} seconds")
    
    # Clean up progress file after successful completion
    try:
        import os
        os.remove(progress_file)
        print("Progress file cleaned up.")
    except FileNotFoundError:
        pass
        
    print("Large batch processing complete!")
    print("Exiting program...")
    
    # Force clean exit - multiple methods to ensure termination
    import sys
    import os
    
    try:
        # Method 1: Clean exit
        sys.exit(0)
    except:
        try:
            # Method 2: Force exit
            os._exit(0)
        except:
            # Method 3: Nuclear option
            import subprocess
            subprocess.run(["taskkill", "/F", "/PID", str(os.getpid())], shell=True)

if __name__ == "__main__":
    main()