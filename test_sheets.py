from utils.sheets import get_sheet, get_rows_to_process, update_row

sheet = get_sheet()

# Step 1: Fetch rows missing image_URL
rows = get_rows_to_process(sheet)
print(f"✅ Found {len(rows)} rows to process.")

# Step 2: Print first row to confirm structure
if rows:
    print("🧪 Sample row:")
    print(rows[0])

    # Step 3: Write test data back to sheet
    update_row(sheet, 0, "https://example.com/test.jpg", "✅ Test OK")
    print("✅ Wrote test data to first row.")
else:
    print("⚠️ No rows to process.")
