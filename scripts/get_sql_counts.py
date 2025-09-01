#!/usr/bin/env python3
"""
Small helper to return counts from SQL for frontend status.
Outputs JSON like: {"enriched_people": 123}
"""
import json
import os
from database.db_manager import DatabaseConfig, DatabaseManager

def main():
    # Support both DB_* (preferred) and SQL_* (legacy) env names
    host = os.getenv('DB_HOST') or os.getenv('SQL_HOST') or 'localhost'
    port = int(os.getenv('DB_PORT') or os.getenv('SQL_PORT') or '3306')
    database = os.getenv('DB_NAME') or os.getenv('SQL_DATABASE') or 'patent_data'
    username = os.getenv('DB_USER') or os.getenv('SQL_USER') or 'root'
    password = os.getenv('DB_PASSWORD') or os.getenv('SQL_PASSWORD') or 'password'
    engine = (os.getenv('DB_ENGINE') or 'mysql').lower()

    cfg = DatabaseConfig(host=host, port=port, database=database, username=username, password=password, engine=engine)
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
