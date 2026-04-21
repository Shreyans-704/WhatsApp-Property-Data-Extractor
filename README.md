# Property Message Processor

## What Does This Do?

I built a tool that takes messy WhatsApp property messages and turns them into clean, structured data. Imagine you're getting tons of random real estate leads in chat—this script reads them, pulls out the important info, and organizes it so you can actually use it.

**In simple terms:** Messy text → Clean spreadsheet. Done.

---

## How It Works (4 Simple Steps)

### 1. Filter

Removes junk messages:

* Looks for property keywords (BHK, plot, apartment, budget, etc.)
* Throws out job postings, product ads, spam
* Keeps only real property messages

### 2. Extract

Pulls out important info:

* Gets: location, property type, size, facing, price, furnishing, contact name, contact number
* Uses pattern matching to find these fields
* If info is missing, marks it as "NA"

### 3. Clean

Makes data consistent:

* Removes extra spaces
* Handles empty values
* Removes duplicates

### 4. Save

Stores the data:

* Saves to Google Sheet (cloud, real-time, shareable)

---

## Fields We Extract

| Field         | Example        | What It Means                |
| ------------- | -------------- | ---------------------------- |
| Locality      | DLF Phase 5    | Which area/project           |
| PropertyType  | Builder Floor  | Apartment, plot, house, etc. |
| Size          | 502 sq yd      | How big                      |
| Facing        | Park Facing    | Direction or feature         |
| Price         | 8.25 cr        | Cost or budget               |
| Furnishing    | Semi Furnished | Move-in ready or not         |
| ContactName   | Raj            | Who to call                  |
| ContactNumber | 9876543210     | Their phone number           |

---

## Example: Before & After

### Raw Message (Messy)

```
DLF Phase 5, builder floor, 502 sq yd, park facing, asking 8.25 cr,
semi furnished, ready to move, owner side, contact Raj 98XXXXXX12
```

### Structured Output (Clean)

```
Locality        | PropertyType  | Size      | Facing      | Price   | Furnishing      | ContactName | ContactNumber
DLF Phase 5     | Builder Floor | 502 sq yd | Park Facing | 8.25 cr | Semi-Furnished  | Raj         | 98XXXXXX12
```

---

## How to Run

1. Install dependencies:

```
pip install gspread oauth2client
```

2. Get Google credentials (optional):

* Go to Google Cloud Console
* Create a service account
* Download `credentials.json`
* Place it in the project folder
* Share your Google Sheet with the service account email

3. Run the script:

```
python property_processor.py
```

4. Check output:


* Google Sheet: Auto-updated (if configured)

---

## What Could Go Wrong (Limitations)

1. Phone number formats vary:

* Some messages have masked numbers like `98XXXX12`
* Some have formats like `+91-9876-543-210`
* The script might miss these

2. Incomplete messages:

* Example: "DLF Phase 5 - interested?"
* Result: Most fields become "NA"

3. Duplicate messages:

* Exact duplicates are removed
* Near-duplicates may still exist

---

## Future Improvements

1. Smarter extraction with AI:

* Use LLMs instead of regex
* Better handling of messy/unstructured data

2. Confidence scoring:

* Assign score to extracted fields
* Flag low-confidence records

---

## Requirements

* Python 3.7+
* Libraries: gspread, oauth2client (optional)
* Google account (for Sheets integration)
* Raw WhatsApp messages

---

## Project Files

* `property_processor.py` → Main script
* `google_sheets_connector.py` → Google Sheets integration
* `output.csv` → Generated output
* `credentials.json` → API credentials
* `requirements.txt` → Dependencies
* `README.md` → Documentation

---

## Summary

A simple pipeline that converts unstructured WhatsApp property messages into clean, structured data and stores it for easy use.
