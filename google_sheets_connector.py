"""
Google Sheets Connector
Saves extracted property data directly to Google Sheets using gspread.
"""

import os
import json
from typing import List, Dict, Optional


def authenticate_google_sheets(credentials_file: str = 'credentials.json'):
    """
    Authenticate with Google Sheets API using service account credentials.
    
    Args:
        credentials_file: Path to credentials.json file
        
    Returns:
        Authorized gspread client
        
    Raises:
        FileNotFoundError: If credentials.json doesn't exist
        ImportError: If gspread not installed
    """
    
    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
    except ImportError:
        print("❌ Error: Required libraries not installed.")
        print("   Run: pip install gspread oauth2client")
        raise
    
    # Check if credentials file exists
    if not os.path.exists(credentials_file):
        raise FileNotFoundError(
            f"❌ credentials.json not found at '{credentials_file}'\n"
            "   Please download it from Google Cloud Console.\n"
            "   See GOOGLE_SHEETS_SETUP.md for instructions."
        )
    
    # Define the scope
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    
    try:
        # Load credentials
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            credentials_file, 
            scope
        )
        
        # Authorize and return client
        client = gspread.authorize(creds)
        
        print(f"✅ Successfully authenticated with Google Sheets API")
        return client
    
    except json.JSONDecodeError:
        raise ValueError(
            f"❌ credentials.json is invalid or corrupted.\n"
            "   Please download a fresh copy from Google Cloud Console."
        )
    except Exception as e:
        raise Exception(f"❌ Authentication failed: {str(e)}")


def open_google_sheet(spreadsheet_url: str, credentials_file: str = 'credentials.json'):
    """
    Open a Google Sheet and return the first sheet.
    
    Args:
        spreadsheet_url: Full URL of the Google Sheet
        credentials_file: Path to credentials.json
        
    Returns:
        Worksheet object (sheet1)
        
    Raises:
        ValueError: If URL is invalid or sheet not found
    """
    
    try:
        client = authenticate_google_sheets(credentials_file)
        
        # Extract sheet ID from URL
        # Expected format: https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit...
        if '/d/' not in spreadsheet_url:
            raise ValueError("Invalid Google Sheets URL format")
        
        sheet_id = spreadsheet_url.split('/d/')[1].split('/')[0]
        
        print(f"📋 Opening spreadsheet (ID: {sheet_id[:20]}...)")
        
        # Open the spreadsheet
        spreadsheet = client.open_by_key(sheet_id)
        
        # Get the first sheet
        sheet = spreadsheet.sheet1
        
        print(f"✅ Opened sheet: '{sheet.title}'")
        return sheet
    
    except FileNotFoundError as e:
        print(f"❌ {e}")
        raise
    except Exception as e:
        print(f"❌ Failed to open Google Sheet: {str(e)}")
        raise


def prepare_row(record: Dict[str, str], column_order: List[str]) -> List[str]:
    """
    Convert a record dictionary into a list row for Google Sheets.
    Handles missing values by replacing with "NA".
    
    Args:
        record: Dictionary with property data
        column_order: List of column names in desired order
        
    Returns:
        List of values matching column order
    """
    
    row = []
    
    for column in column_order:
        # Get value from record, default to "NA" if missing
        value = record.get(column, "NA")
        
        # Handle None, empty strings, and None-like values
        if value is None or str(value).strip() == "":
            value = "NA"
        
        row.append(str(value).strip())
    
    return row


def save_to_google_sheets(
    data: List[Dict[str, str]],
    spreadsheet_url: str,
    credentials_file: str = 'credentials.json',
    clear_existing: bool = False
) -> Dict[str, int]:
    """
    Save extracted property records to Google Sheets.
    
    Args:
        data: List of property records (dictionaries)
        spreadsheet_url: URL of the target Google Sheet
        credentials_file: Path to credentials.json
        clear_existing: If True, clears all data before adding (default: False)
        
    Returns:
        Dictionary with operation statistics
    """
    
    if not data:
        print("⚠️  No data to save.")
        return {"rows_added": 0, "failed": 0}
    
    # Column order in Google Sheet
    columns = [
        'Locality',
        'PropertyType',
        'Size',
        'Facing',
        'Price',
        'Furnishing',
        'ContactName',
        'ContactNumber'
    ]
    
    try:
        # Open the sheet
        sheet = open_google_sheet(spreadsheet_url, credentials_file)
        
        print(f"\n💾 Saving {len(data)} records to Google Sheets...")
        
        # Option 1: Clear existing data (if flag is set)
        if clear_existing:
            print("   Clearing existing data...")
            sheet.clear()
            # Add headers
            sheet.insert_row(columns, 1)
            start_row = 2
        else:
            # Check if headers exist, if not add them
            current_data = sheet.get_all_records()
            if len(current_data) == 0:
                # Sheet is empty, add headers
                sheet.insert_row(columns, 1)
                print("   Added headers to empty sheet")
                start_row = 2
            else:
                # Append to existing data
                start_row = len(current_data) + 2  # +2 for header row
                print(f"   Appending to existing {len(current_data)} records")
        
        # Prepare rows
        rows_to_add = []
        for record in data:
            row = prepare_row(record, columns)
            rows_to_add.append(row)
        
        # Add rows to sheet
        print(f"   Adding {len(rows_to_add)} rows...", end=" ")
        sheet.append_rows(rows_to_add, value_input_option='RAW')
        print("✓")
        
        # Display success message
        total_rows = len(sheet.get_all_records())
        print(f"\n✅ Success! Total records in sheet: {total_rows}")
        
        return {
            "rows_added": len(rows_to_add),
            "total_records": total_rows,
            "failed": 0
        }
    
    except FileNotFoundError as e:
        print(f"❌ {e}")
        return {"rows_added": 0, "failed": len(data)}
    except Exception as e:
        print(f"❌ Error saving to Google Sheets: {str(e)}")
        return {"rows_added": 0, "failed": len(data)}


def verify_sheet_connection(
    spreadsheet_url: str,
    credentials_file: str = 'credentials.json'
) -> bool:
    """
    Verify that we can connect to the Google Sheet.
    Useful for testing setup before main process.
    
    Args:
        spreadsheet_url: URL of the target Google Sheet
        credentials_file: Path to credentials.json
        
    Returns:
        True if connection successful, False otherwise
    """
    
    try:
        print("🔍 Verifying Google Sheets connection...")
        sheet = open_google_sheet(spreadsheet_url, credentials_file)
        print(f"✅ Connection verified! Sheet title: '{sheet.title}'")
        print(f"   Current records: {len(sheet.get_all_records())}")
        return True
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        return False


# ============================================================================
# OPTIONAL: Batch Operations
# ============================================================================

def append_to_google_sheets_batch(
    data: List[Dict[str, str]],
    spreadsheet_url: str,
    credentials_file: str = 'credentials.json',
    batch_size: int = 100
) -> Dict[str, int]:
    """
    Save data to Google Sheets in batches (for large datasets).
    
    Args:
        data: List of property records
        spreadsheet_url: URL of the target Google Sheet
        credentials_file: Path to credentials.json
        batch_size: Number of rows to add per batch (default: 100)
        
    Returns:
        Dictionary with operation statistics
    """
    
    total_added = 0
    total_failed = 0
    
    # Process in batches
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        print(f"\n📦 Processing batch {i//batch_size + 1}...")
        
        result = save_to_google_sheets(
            batch,
            spreadsheet_url,
            credentials_file,
            clear_existing=(i == 0)  # Clear only on first batch
        )
        
        total_added += result.get("rows_added", 0)
        total_failed += result.get("failed", 0)
    
    return {
        "total_rows_added": total_added,
        "total_failed": total_failed
    }


# ============================================================================
# ENTRY POINT (for testing)
# ============================================================================

if __name__ == "__main__":
    # Example usage
    SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1t-wRcI9XcdPu5FVpK6hAsG71pN5DhhMGXLc1ngsXPxY/edit?usp=sharing"
    
    # Test data
    sample_data = [
        {
            "Locality": "DLF Phase 5",
            "PropertyType": "Builder Floor",
            "Size": "502 sq yd",
            "Facing": "Park Facing",
            "Price": "8.25 cr",
            "Furnishing": "Semi Furnished",
            "ContactName": "Raj",
            "ContactNumber": "98XXXXXX12"
        }
    ]
    
    # Try to verify connection
    print("=" * 70)
    print("TESTING GOOGLE SHEETS CONNECTION")
    print("=" * 70)
    
    if verify_sheet_connection(SPREADSHEET_URL):
        print("\n✅ Ready to save data!")
        print("   Now integrate with property_processor.py")
    else:
        print("\n❌ Setup needed!")
        print("   See GOOGLE_SHEETS_SETUP.md for step-by-step instructions")
