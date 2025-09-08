#!/usr/bin/env python3
"""
Update an existing person's address in SQL.
Reads JSON from stdin:
{
  "existing_id": 123,
  "city": "New City",
  "state": "CA",
  "country": "US",
  "address": "123 Main St",
  "zip": "90210"
}
Outputs a small JSON result: {"success": true}
"""
import sys
import json
import os
from typing import Dict

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

    # Only allow specific fields
    allowed = ['city', 'state', 'country', 'address', 'zip']
    updates = {k: (payload.get(k) or '').strip() for k in allowed if k in payload}
    if not updates:
        print(json.dumps({"success": False, "error": "No update fields provided"}))
        return 1

    cfg = DatabaseConfig.from_env()
    db = DatabaseManager(cfg)

    set_clauses = []
    params = []
    for col, val in updates.items():
        set_clauses.append(f"{col}=%s")
        params.append(val)
    params.append(existing_id)
    sql = f"UPDATE existing_people SET {', '.join(set_clauses)} WHERE id=%s"

    try:
        with db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, tuple(params))
            conn.commit()
        print(json.dumps({"success": True}))
        return 0
    except Exception as e:
        print(json.dumps({"success": False, "error": str(e)}))
        return 1


if __name__ == '__main__':
    sys.exit(main())

