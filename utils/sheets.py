from google.oauth2.service_account import Credentials
import gspread

# Define the sheet you're connecting to
SHEET_ID = "1wavqnMBfFsDLAuxC4DS6heorIyuZcyKJwCWxn0mIdbA"
SHEET_NAME = "Sheet1"  # Rename if using a different tab name

# Scopes required for Sheets + Drive
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Authenticate with service account
def get_sheet():
    creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    return sheet

# Fetch records to process (rows missing image_URL) with row indices
def get_rows_to_process(sheet):
    data = sheet.get_all_records()
    rows_with_indices = [(i, row) for i, row in enumerate(data) if not row.get("image_URL")]
    return rows_with_indices

# Batch update multiple rows at once
def batch_update_rows(sheet, updates):
    if not updates:
        return
    
    # Get column indices once
    image_url_col = sheet.find("image_URL").col
    status_col = sheet.find("Processing Status").col
    
    # Prepare batch update data
    batch_data = []
    for row_index, image_url, status in updates:
        actual_row = row_index + 2  # +2 for 0-indexing and header
        batch_data.extend([
            {"range": f"{gspread.utils.rowcol_to_a1(actual_row, image_url_col)}", "values": [[image_url]]},
            {"range": f"{gspread.utils.rowcol_to_a1(actual_row, status_col)}", "values": [[status]]}
        ])
    
    # Execute batch update
    sheet.batch_update(batch_data)

# Legacy function for individual updates (keep for compatibility)
def update_row(sheet, row_index, image_url, status="✅ Complete"):
    # +2 accounts for 0-indexing and header row
    sheet.update_cell(row_index + 2, sheet.find("image_URL").col, image_url)
    sheet.update_cell(row_index + 2, sheet.find("Processing Status").col, status)
