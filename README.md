# Street View Image Tool

## Overview
Fetch Google Street View images for a list of addresses stored in a Google Sheet, upload them to a public host (e.g. Cloudinary), and write back the URLs.

## How to Use

1. Add your credentials in `credentials.json`
2. Set config values in `config.json`
3. Share your Google Sheet with the service account email
4. Run the script:
   ```bash
   python main.py
   ```

## Setup

Install dependencies:
```bash
pip install -r requirements.txt
```
