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

# Fetch records to process (rows missing image_URL)
def get_rows_to_process(sheet):
    data = sheet.get_all_records()
    rows = [row for row in data if not row.get("image_URL")]
    return rows

# Update image URL + status back to sheet
def update_row(sheet, row_index, image_url, status="✅ Complete"):
    # +2 accounts for 0-indexing and header row
    sheet.update_cell(row_index + 2, sheet.find("image_URL").col, image_url)
    sheet.update_cell(row_index + 2, sheet.find("Processing Status").col, status)
