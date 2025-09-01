#!/usr/bin/env python3
"""
Small helper to return counts from SQL for frontend status.
Outputs JSON like: {"enriched_people": 123}
"""
import json
from database.db_manager import DatabaseConfig, DatabaseManager

def main():
    cfg = DatabaseConfig.from_env()
    db = DatabaseManager(cfg)
    try:
        with db.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM enriched_people")
            except Exception:
                # Table may not exist yet
                print(json.dumps({"enriched_people": 0}))
                return
            row = cursor.fetchone()
            count = int(row[0]) if row and row[0] is not None else 0
            print(json.dumps({"enriched_people": count}))
    except Exception:
        # Connection failure or similar: fall back to 0
        print(json.dumps({"enriched_people": 0}))

if __name__ == "__main__":
    main()

