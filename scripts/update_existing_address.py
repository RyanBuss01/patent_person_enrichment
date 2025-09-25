#!/usr/bin/env python3
"""
Clone an existing person record with updated location details.
Reads JSON from stdin:
{
  "existing_id": 123,
  "city": "New City",
  "state": "CA",
  "country": "US",
  "address": "123 Main St",
  "zip": "90210"
}

The script copies every column from the referenced record, inserts a NEW row,
overrides location fields supplied in the payload, and stamps `issue_date`
with the current UTC timestamp. No existing rows are modified.
Outputs JSON like {"success": true}.
"""
import sys
import json
import os
from typing import Dict
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from database.db_manager import DatabaseConfig, DatabaseManager


def main():
    try:
        raw = sys.stdin.read() or ''
        payload: Dict = json.loads(raw) if raw.strip() else {}
    except Exception as e:
        print(json.dumps({"success": False, "error": f"Invalid JSON: {e}"}))
        return 1

    existing_id = payload.get('existing_id')
    if not existing_id:
        print(json.dumps({"success": False, "error": "existing_id is required"}))
        return 1

    cfg = DatabaseConfig.from_env()
    db = DatabaseManager(cfg)
    try:
        existing_row = db.execute_query(
            "SELECT * FROM existing_people WHERE id = %s LIMIT 1",
            (existing_id,),
            fetch_one=True
        )
    except Exception as e:
        print(json.dumps({"success": False, "error": f"Failed to load existing record: {e}"}))
        return 1

    if not existing_row:
        print(json.dumps({"success": False, "error": "Existing record not found"}))
        return 1

    # Build new record by cloning existing values
    new_record = dict(existing_row)
    # Remove primary key so a new row is inserted
    new_record.pop('id', None)

    # Columns that should take updated values if provided
    allowed = ['city', 'state', 'country', 'address', 'zip']
    for col in allowed:
        if col in payload and payload[col] is not None:
            new_record[col] = str(payload[col]).strip()

    # Always stamp with current datetime for issue_date
    new_record['issue_date'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # Drop auto-managed timestamp columns so DB can populate fresh values
    for auto_col in ('created_at', 'updated_at', 'last_updated'):  # ignore if column not present
        if auto_col in new_record:
            new_record.pop(auto_col, None)

    try:
        inserted = db.insert_batch('existing_people', [new_record], ignore_duplicates=False)
        if inserted <= 0:
            print(json.dumps({"success": False, "error": "Insert returned no rows"}))
            return 1
        print(json.dumps({"success": True}))
        return 0
    except Exception as e:
        print(json.dumps({"success": False, "error": str(e)}))
        return 1


if __name__ == '__main__':
    sys.exit(main())
