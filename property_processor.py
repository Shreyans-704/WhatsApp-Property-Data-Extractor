"""
Property Message Processor
Converts messy WhatsApp-style property messages into structured CSV data.
Can export to CSV file or Google Sheets.
"""

import csv
import os
import re
from typing import List, Dict, Optional

# Optional: Import Google Sheets connector
try:
    from google_sheets_connector import save_to_google_sheets, verify_sheet_connection
    GOOGLE_SHEETS_AVAILABLE = True
except ImportError:
    GOOGLE_SHEETS_AVAILABLE = False

# ============================================================================
# RAW MESSAGE DATA
# ============================================================================

RAW_MESSAGES = [
    "DLF Phase 5, builder floor, 502 sq yd, park facing, asking 8.25 cr, semi furnished, ready to move, owner side, contact Raj 98XXXXXX12 🔥",
    "*Requirement for purchase🔥* *Emmar MGF palm drive Sec 66* Size 3221 Requirement 6 to 9 th floor unit no. 2 Budget- Market rate *Nandni jha* *9205947627- 9211902580*",
    "Plot requirements 📌 Saket squar ke near Plot purchase Krna hai Residential 1500 se 1800 sq fit Confirm plot or rate Dm kre Contact us :-7828690339",
    "*URGENT REQUIREMENT FOR MALE STAFF* *WORK - REAL ESTATE CALLING + FIELD WORK* 🎉*SALARY + INCENTIVE🎉* *LOCATION : DWARKA EXPRESS Way SECTOR 102 the satya hive* 🎉 *BHUMI NIWAS REALTY PVT LTD.*🎉 *DAVINDER SINGH* *8595366005*",
    "*URGENT REQUIREMENT* *Required: 1000 Sq yd Plot/Kothi / Built-up House (don't quote South facing)* 📍 Preferred Locations: DLF Phase 1, DLF Phase 2 Sushant Lok 1, 31, 40, Sector 15 part 2 or any other location connected with Highway. 💰 Budget: As per prevailing market price Please call with confirm and direct options. *Aman* *8448905370*",
    "*Urgent Requirement* 🚨 *M3M Antalya Hills* Configuration: 3 BHK Location: Pine Block Preferred Sizes: • 1616 sq ft • 1547 sq ft Budget: Max ₹11,000 per sq ft 💰 Token in hand 🤝 Ready to close immediately If anyone has a matching unit, we can sit on the table on an immediate basis. Call Shailesh- 9818346018",
    "🚨 URGENT REQUIREMENT – BUY (Self Use / Investment) 1️⃣Bare Shell Unit (For Self Use) • Size: 700–800 Sq. Ft. • Location: Golf Course Road & Golf Course Extension Road • Budget: ₹11,000–12,000 per Sq. Ft. • Preference: Premium Commercial ⸻ 🚨 URGENT REQUIREMENT – LEASE 2️⃣Commercial Space for Fitness Center • Size: 4000–4500 Sq. Ft. • Location: Golf Course Road & Golf Course Extension Road • Budget: Up to ₹7 Lakh • Purpose: Open Fitness Center ⸻ 📞 Contact: Dushyant Dahiya 📱 935548856",
    "🤖 *Gemini AI Pro + 2TB Storage* ✅ *Redeem Link Activation* 🔥 *Working on Existing Gmail* 🗓 *Validity: 18 Months* 📧 *Activation: On Your Email* ❌ *Original Price: ₹39500/-* 💰 *Price: ₹ Inbox* 📌 *What You'll Get:* * 🤖 *Gemini 2.5 Pro & Flash* * 💎 *1000 AI Credits Monthly* * 🎬 *VEO3 AI Video Generator* * ️ *50+ Videos/Monthly* * 🍌 *Nano Banana Access* * 🎥 *Flow AI Filmmaker* * ️ *Whisk Image-to-Video AI* * 📚 *NotebookLM for Research* * ✍️ *Gemini in Gmail, Docs* * ☁️ *2TB Google Storage* ⚠️ *Limited Time Offer* 📥 *DM Now* 📥 919310051848",
    "Urgent requirement for purchase *DLF summit* Tower A&C Unit no 1,2 Anil Sharma Mehta propmart pvt ltd 7488846863"
]

# ============================================================================
# STEP 1: FILTER PROPERTY-RELATED MESSAGES
# ============================================================================

def filter_messages(messages: List[str]) -> List[str]:
    """
    Filter out non-property messages using keyword-based approach.
    
    Logic:
    - Keep messages with property-related keywords (buy, sell, lease, rent, etc.)
    - Discard messages with job-posting keywords (staff, salary, hiring)
    - Discard messages with product-sale keywords (AI, storage, software)
    
    Args:
        messages: List of raw message strings
        
    Returns:
        List of filtered property-related messages
    """
    
    # Keywords that indicate property-related messages
    PROPERTY_KEYWORDS = [
        'bhk', 'property', 'plot', 'house', 'apartment', 'flat', 'builder',
        'floor', 'purchase', 'buy', 'sell', 'lease', 'rent', 'requirement',
        'sq ft', 'sq yd', 'facing', 'budget', 'furnished', 'location',
        'sector', 'dlf', 'phase', 'project', 'emmar', 'm3m', 'kothi',
        'commercial', 'residential', 'unit', 'tower', 'configuration'
    ]
    
    # Keywords that indicate non-property messages
    SPAM_KEYWORDS = [
        'staff', 'salary', 'incentive', 'hiring', 'job', 'vacancy',
        'ai pro', 'gemini', 'storage', 'gmail', 'activation', 'redeem'
    ]
    
    filtered = []
    
    for message in messages:
        message_lower = message.lower()
        
        # Check if it has any spam keywords (exclude immediately)
        if any(spam_word in message_lower for spam_word in SPAM_KEYWORDS):
            continue
        
        # Check if it has any property keywords (include)
        if any(prop_word in message_lower for prop_word in PROPERTY_KEYWORDS):
            filtered.append(message)
    
    return filtered


# ============================================================================
# STEP 2: EXTRACT DATA USING OPENAI API
# ============================================================================

def extract_property_data_with_api(message: str) -> List[Dict[str, str]]:
    """
    Extract structured property data using OpenAI API.
    Falls back to mock extraction if API is not configured.
    
    Args:
        message: Property message string
        
    Returns:
        List of dictionaries (each dict = one property requirement)
    """
    
    try:
        import openai
        
        # Check if API key is set
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            print("⚠️  OPENAI_API_KEY not set. Using mock extraction...")
            return extract_property_data_mock(message)
        
        # Set up OpenAI client
        client = openai.OpenAI(api_key=api_key)
        
        # Create prompt for extraction
        prompt = f"""
Extract property information from this WhatsApp message. If multiple requirements exist (e.g., BUY + LEASE), 
create separate entries for each.

For each requirement, extract:
- Locality (neighborhood/project name)
- Property Type (apartment, plot, house, commercial, etc.)
- Size (in sq ft or sq yd)
- Facing/Feature (north facing, park facing, etc.)
- Price/Budget
- Furnishing (furnished, semi-furnished, unfurnished)
- Contact Name
- Contact Number (10 digits, without spaces)

If a field is missing, return "NA".

Return as JSON array of objects. Example:
[{{"Locality": "DLF Phase 5", "PropertyType": "Builder Floor", ...}}]

Message: {message}
"""
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a real estate data extraction expert. Return only valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=500
        )
        
        # Parse response
        response_text = response.choices[0].message.content
        import json
        
        # Try to extract JSON from response
        try:
            data = json.loads(response_text)
            # Ensure it's a list
            if isinstance(data, dict):
                data = [data]
            return data if isinstance(data, list) else []
        except json.JSONDecodeError:
            print(f"⚠️  Failed to parse API response. Using mock extraction...")
            return extract_property_data_mock(message)
    
    except ImportError:
        print("⚠️  OpenAI library not installed. Run: pip install openai")
        print("     Using mock extraction instead...\n")
        return extract_property_data_mock(message)
    except Exception as e:
        print(f"⚠️  API Error: {e}. Using mock extraction...")
        return extract_property_data_mock(message)


def extract_property_data_mock(message: str) -> List[Dict[str, str]]:
    """
    Mock extraction function using regex and keyword matching.
    Works without API and good for interview explanation.
    
    Args:
        message: Property message string
        
    Returns:
        List of dictionaries with extracted fields
    """
    
    # Count how many requirements in message (simple heuristic)
    requirement_count = len(re.findall(r'requirement|buy|lease|rent|requirement for', 
                                       message.lower(), re.IGNORECASE))
    requirement_count = max(1, min(requirement_count, 3))  # Cap at 3
    
    results = []
    
    for i in range(requirement_count):
        extracted = {
            'Locality': extract_locality(message),
            'PropertyType': extract_property_type(message),
            'Size': extract_size(message),
            'Facing': extract_facing(message),
            'Price': extract_price(message),
            'Furnishing': extract_furnishing(message),
            'ContactName': extract_contact_name(message),
            'ContactNumber': extract_contact_number(message)
        }
        results.append(extracted)
    
    return results


# ============================================================================
# HELPER FUNCTIONS FOR DATA EXTRACTION
# ============================================================================

def extract_locality(text: str) -> str:
    """Extract locality/project name"""
    # Try to match common project names and neighborhoods
    projects = [
        'dlf phase [0-9]+', 'dlf summit', 'emmar mgf palm drive', 'm3m antalya hills',
        'sushant lok', 'sector [0-9]+', 'golf course road', 'saket', 'dwarka'
    ]
    
    for pattern in projects:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(0).title()
    
    return "NA"


def extract_property_type(text: str) -> str:
    """Extract property type with priority logic"""
    text_lower = text.lower()
    
    # Priority order: most specific to least specific
    if re.search(r'\bplot\b|kothi', text_lower):
        return 'Plot'
    if re.search(r'builder\s+floor', text_lower):
        return 'Builder Floor'
    if re.search(r'\d+\s*bhk|configuration', text_lower):
        return 'Apartment'
    if re.search(r'\b(?:house|built-up)\b', text_lower):
        return 'House'
    if re.search(r'commercial|fitness\s+center', text_lower):
        return 'Commercial'
    if re.search(r'apartment|flat|3\s*bhk', text_lower):
        return 'Apartment'
    if re.search(r'\bunit\b', text_lower):
        return 'Unit'
    
    return 'NA'


def extract_size(text: str) -> str:
    """Extract size with support for ranges, multiple values, plain numbers, and typos"""
    # Pattern 1: Size range with variants (e.g., "1500-1800 sq ft", "1500-1800 sq fit", "1500–1800 sq yd")
    range_pattern = r'(\d+\s*(?:-|to|se)\s*\d+\s*(?:sq\.?\s*(?:ft|yd|fit))|\d+\s*(?:sq\.?\s*(?:ft|yd|fit)))'
    match = re.search(range_pattern, text, re.IGNORECASE)

    if match:
        result = match.group(0).strip().lower()

        # fix "sq fit"
        result = re.sub(r'fit', 'ft', result)

        # convert "1500 se 1800" → "1500–1800"
        range_match = re.search(r'(\d+)\s*(?:-|to|se)\s*(\d+)\s*(sq\s*(?:ft|yd))', result)
        if range_match:
            return f"{range_match.group(1)}–{range_match.group(2)} {range_match.group(3)}"

        return result
    
    # Pattern 2: Explicit size with unit (e.g., "502 sq yd", "3221 sq ft")
    size_pattern = r'(\d+(?:[.,]\d+)*)\s*(?:sq\.?\s*(?:ft|yd|feet|yard))'
    match = re.search(size_pattern, text, re.IGNORECASE)
    if match:
        return match.group(0).strip()
    
    # Pattern 3: Multiple sizes (e.g., "1616 sq ft, 1547 sq ft")
    multi_size = re.findall(r'(\d+(?:,\d+)?\s*sq\s*ft)', text, re.IGNORECASE)
    if len(multi_size) > 1:
        return ', '.join(multi_size)
    
    # Pattern 4: Plain number after "size" keyword (e.g., "Size 3221", "size: 3221 sq ft")
    plain_pattern = r'(?:size|SIZE)[:\s]+([0-9,]+)(?:\s*(?:sq\s*ft|sq\s*fit|sq\s*yd))?'
    match = re.search(plain_pattern, text)
    if match:
        size_val = match.group(1).strip()
        if re.search(r'sq\s*(?:ft|fit|yd)', match.group(0), re.IGNORECASE):
            unit_match = re.search(r'sq\s*(?:ft|fit|yd)', match.group(0), re.IGNORECASE)
            unit = unit_match.group(0)
            # Normalize "sq fit" to "sq ft"
            unit = re.sub(r'fit\b', 'ft', unit, flags=re.IGNORECASE)
            return f'{size_val} {unit}'
        return f'{size_val} sq ft'
    
    return 'NA'


def extract_facing(text: str) -> str:
    """Extract facing/feature"""
    facings = ['north facing', 'south facing', 'east facing', 'west facing', 
               'park facing', 'premium commercial', 'pine block']
    
    for facing in facings:
        if facing.lower() in text.lower():
            return facing.title()
    
    return "NA"


def extract_price(text: str) -> str:
    """Extract price/budget with support for various formats"""
    text_lower = text.lower()
    
    # Try multiple price patterns in order of specificity
    patterns = [
        r'₹?\s*[\d.,]+\s*(?:cr|crore)',
        r'(?:₹\s*)?[\d,]+\s*(?:per\s*(?:sq\.?\s*(?:ft|yd)|sqft|sqyd))',
        r'[\d.,]+\s*(?:lakh|lac)\s*(?:per\s*sq\s*(?:ft|yd))?',
        r'market\s*(?:rate|price)',
        r'₹\s*[\d,]+(?:\s*-\s*[\d,]+)?',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            result = match.group(0).strip()
            result = re.sub(r'\s+', ' ', result)
            return result
    
    return 'NA'


def extract_furnishing(text: str) -> str:
    """Extract furnishing status"""
    if re.search(r'semi.?furnished', text, re.IGNORECASE):
        return "Semi-Furnished"
    elif re.search(r'furnished', text, re.IGNORECASE):
        return "Furnished"
    elif re.search(r'bare shell|unfurnished', text, re.IGNORECASE):
        return "Unfurnished"
    
    return "NA"


def extract_contact_name(text: str) -> str:
    """Extract contact person name and ignore invalid placeholders"""

    invalid_names = {"us", "me", "now"}

    # Pattern 0: Names inside asterisks (WhatsApp format)
    match = re.search(r'\*\s*([A-Za-z]+(?:\s+[A-Za-z]+)?)\s*\*', text)
    if match:
        name = match.group(1).strip()

        # 🔥 FIX
        name = re.sub(r'^(price|ft)\s+', '', name, flags=re.IGNORECASE)

        if name.lower() in invalid_names:
            return "NA"

        if not re.search(r'location|sector|phase|plot|building|urgent|requirement|purchase', name, re.IGNORECASE):
            return name.title()

    # Pattern 1: After contact/call keywords
    match = re.search(r'(?:contact|call|reach|by)\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)', text, re.IGNORECASE)
    if match:
        name = match.group(1).strip()

        # 🔥 FIX
        name = re.sub(r'^(price|ft)\s+', '', name, flags=re.IGNORECASE)

        if name.lower() in invalid_names:
            return "NA"

        if not re.search(r'location|sector|phase|plot|building|road', name, re.IGNORECASE):
            return name.title()

    # Pattern 2: Name before number
    match = re.search(r'\b([A-Za-z]+(?:\s+[A-Za-z]+)?)\s+(?:[-–]?\s*)?(\d{10}|\d{2}[Xx]{6}\d{2})', text)
    if match:
        name = match.group(1).strip()

        # 🔥 FIX
        name = re.sub(r'^(price|ft)\s+', '', name, flags=re.IGNORECASE)

        if name.lower() in invalid_names:
            return "NA"

        if not re.search(r'location|sector|phase|plot|building|road|requirement|urgent', name, re.IGNORECASE):
            return name.title()

    return "NA"


def extract_contact_number(text: str) -> str:
    """Extract phone numbers (handles masked, ranges, and multiple numbers)"""
    
    # Pattern 1: Exact 10-digit numbers
    exact_pattern = r'\b(\d{10})\b'
    matches = re.findall(exact_pattern, text)
    if matches:
        return ', '.join(matches)
    
    # Pattern 2: Masked numbers with proper word boundary (e.g., "98XXXXXX12", "9876XXXX10")
    masked_pattern = r'\b(\d{2}[Xx]{6}\d{2})\b'
    matches = re.findall(masked_pattern, text)
    if matches:
        return ', '.join(matches)
    
    # Pattern 3: Partially masked with numbers and X/x (e.g., "987654XXXX", "9876XXXX3210")
    partial_masked = r'\b(\d{4,6}[Xx]{2,4}\d{2,4})\b'
    matches = re.findall(partial_masked, text)
    if matches:
        return ', '.join(matches)
    
    # Pattern 4: Numbers with dashes/spaces (e.g., "9876-54-3210", "9876 54 3210")
    dash_pattern = r'(\d{4}[\s-]\d{2,3}[\s-]\d{4,5})'
    matches = re.findall(dash_pattern, text)
    if matches:
        cleaned = [re.sub(r'[\s-]', '', m) for m in matches]
        return ', '.join(cleaned)
    
    # Pattern 5: Number ranges (e.g., "9876543210-9876543211")
    range_pattern = r'(\d{10})\s*[-–]\s*(\d{10})'
    match = re.search(range_pattern, text)
    if match:
        return f'{match.group(1)}, {match.group(2)}'
    
    return 'NA'


# ============================================================================
# STEP 3: CLEAN AND VALIDATE DATA
# ============================================================================

def clean_data(records: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Clean extracted data:
    - Remove empty strings and replace with "NA"
    - Trim whitespace
    - Normalize casing (Title Case for names/localities)
    - Remove duplicates
    - Clean special characters
    
    Args:
        records: List of extracted records
        
    Returns:
        Cleaned records
    """
    
    cleaned = []
    seen = set()
    
    for record in records:
        cleaned_record = {}
        
        for key, value in record.items():
            value_str = str(value).strip() if value else ""
            
            if not value_str or value_str.lower() in ['', 'none', 'nan', 'null']:
                cleaned_record[key] = "NA"
            else:
                # Normalize: Remove extra spaces, normalize dashes
                value_str = re.sub(r'\s+', ' ', value_str)
                value_str = re.sub(r'[–\-]', '-', value_str)
                
                # Title case for names and localities
                if key in ['ContactName', 'Locality']:
                    value_str = value_str.title()
                
                cleaned_record[key] = value_str
        
        # Check for duplicates
        record_tuple = tuple(sorted(cleaned_record.items()))
        if record_tuple not in seen:
            seen.add(record_tuple)
            cleaned.append(cleaned_record)
    
    return cleaned


# ============================================================================
# STEP 4: SAVE TO CSV
# ============================================================================

def save_to_csv(records: List[Dict[str, str]], filename: str = 'output.csv') -> None:
    """
    Save extracted records to CSV file.
    
    Args:
        records: List of property records
        filename: Output CSV filename
    """
    
    if not records:
        print("No records to save.")
        return
    
    # Define CSV columns
    fieldnames = [
        'Locality',
        'PropertyType',
        'Size',
        'Facing',
        'Price',
        'Furnishing',
        'ContactName',
        'ContactNumber'
    ]
    
    # Write to CSV
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        writer.writerows(records)
    
    print(f"✅ Data saved to '{filename}' ({len(records)} records)")


# ============================================================================
# MAIN WORKFLOW
# ============================================================================

def process_messages(messages: List[str], use_api: bool = False) -> List[Dict[str, str]]:
    """
    Main workflow: Filter -> Extract -> Clean -> Return
    
    Args:
        messages: List of raw messages
        use_api: Whether to use OpenAI API (True) or mock extraction (False)
        
    Returns:
        List of structured property records
    """
    
    print("=" * 70)
    print("PROPERTY MESSAGE PROCESSOR - WORKFLOW")
    print("=" * 70)
    
    # STEP 1: Filter messages
    print(f"\n📋 STEP 1: Filtering {len(messages)} messages...")
    filtered = filter_messages(messages)
    print(f"   ✓ Found {len(filtered)} property-related messages\n")
    
    # STEP 2: Extract data
    print(f"📊 STEP 2: Extracting data from {len(filtered)} messages...")
    all_records = []
    
    for i, message in enumerate(filtered, 1):
        print(f"   Processing message {i}/{len(filtered)}...", end=" ")
        
        if use_api:
            records = extract_property_data_with_api(message)
        else:
            records = extract_property_data_mock(message)
        
        all_records.extend(records)
        print(f"✓ ({len(records)} record(s))")
    
    print(f"   ✓ Extracted {len(all_records)} total records\n")
    
    # STEP 3: Clean data
    print(f"🧹 STEP 3: Cleaning {len(all_records)} records...")
    cleaned = clean_data(all_records)
    print(f"   ✓ Cleaned data ready\n")
    
    return cleaned


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Get input from user
    print("=" * 70)
    print("PROPERTY MESSAGE PROCESSOR - USER INPUT MODE")
    print("=" * 70)
    print("\n📝 Enter property messages (type DONE when finished):\n")
    
    user_messages = []
    while True:
        try:
            user_input = input("> ").strip()
            
            # Check for exit condition
            if user_input.upper() == "DONE":
                break
            
            # Skip empty lines
            if user_input:
                user_messages.append(user_input)
        
        except KeyboardInterrupt:
            print("\n\n❌ Input cancelled by user.")
            exit(0)
        except EOFError:
            # Handle non-interactive mode
            break
    
    # Validate that we have input
    if not user_messages:
        print("\n❌ No messages provided. Exiting.")
        exit(0)
    
    print(f"\n✅ Received {len(user_messages)} message(s). Processing...\n")
    
    # Process messages (using mock extraction by default)
    # To use OpenAI API, set OPENAI_API_KEY environment variable and pass use_api=True
    
    structured_data = process_messages(user_messages, use_api=False)
    
    # Display results in terminal
    print("\n" + "=" * 70)
    print("STRUCTURED OUTPUT")
    print("=" * 70)
    
    if structured_data:
        for i, record in enumerate(structured_data, 1):
            print(f"\nRecord {i}:")
            for key, value in record.items():
                print(f"  {key:20} → {value}")
    else:
        print("\n⚠️  No properties found in input messages.")
        exit(0)
    
    # Save to Google Sheets
    print(f"\n" + "=" * 70)
    print("💾 SAVING TO GOOGLE SHEETS")
    print("=" * 70)
    
    if GOOGLE_SHEETS_AVAILABLE:
        SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1t-wRcI9XcdPu5FVpK6hAsG71pN5DhhMGXLc1ngsXPxY/edit?usp=sharing"
        
        # Check if credentials exist
        if os.path.exists('credentials.json'):
            print("\n📊 Found credentials.json - uploading to Google Sheets...")
            try:
                result = save_to_google_sheets(
                    data=structured_data,
                    spreadsheet_url=SPREADSHEET_URL,
                    credentials_file='credentials.json',
                    clear_existing=False  # Append to existing data
                )
                print(f"\n✅ SUCCESS! {len(structured_data)} records saved to Google Sheets")
                print(f"   Sheet URL: {SPREADSHEET_URL}")
            except Exception as e:
                print(f"\n❌ Error saving to Google Sheets: {str(e)}")
        else:
            print("\n❌ credentials.json not found")
            print("   To enable Google Sheets export:")
            print("   1. Go to Google Cloud Console")
            print("   2. Create a service account and download JSON key")
            print("   3. Rename to credentials.json and place in this folder")
            print("   4. Share your Google Sheet with the service account email")
    else:
        print("\n❌ gspread not installed")
        print("   Run: pip install gspread oauth2client")
    
    print("\n" + "=" * 70)
   