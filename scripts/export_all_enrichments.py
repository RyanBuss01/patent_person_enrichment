#!/usr/bin/env python3
"""
Export all enriched people from SQL to CSV on stdout.
Flattens the entire JSON stored in the `enrichment_data` column, producing a
column for every nested field (dot-separated path). Arrays/objects are
JSON-stringified in a single cell.

Depends on database/db_manager.DatabaseManager available in PYTHONPATH.
"""
import sys
import os
import json
import csv
from datetime import datetime

def _flatten(obj, prefix='', out=None):
    if out is None:
        out = {}
    if obj is None:
        if prefix:
            out[prefix] = ''
        return out
    if isinstance(obj, list):
        out[prefix] = json.dumps(obj, ensure_ascii=False)
        return out
    if isinstance(obj, dict):
        if not obj and prefix:
            out[prefix] = ''
            return out
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else k
            _flatten(v, key, out)
        return out
    # Primitive
    out[prefix] = str(obj)
    return out

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

try:
    from database.db_manager import DatabaseManager, DatabaseConfig
except Exception as e:
    sys.stderr.write(f"Failed to import Database modules: {e}\n")
    sys.exit(1)


def extract_row(row):
    """Flatten a single DB row, including enrichment_data JSON."""
    flat = {}
    # Include common top-level DB columns (stringified)
    for k in ['id','first_name','last_name','city','state','country','patent_number','person_type','api_cost','enriched_at','created_at','updated_at']:
        if k in row:
            v = row.get(k)
            flat[k] = '' if v is None else str(v)
    # Parse and flatten enrichment_data
    ed_raw = row.get('enrichment_data')
    try:
        ed = json.loads(ed_raw) if isinstance(ed_raw, (str, bytes)) else (ed_raw or {})
    except Exception:
        ed = {}
    flat_ed = _flatten(ed, prefix='enrichment_data')
    flat.update(flat_ed)
    return flat


def main():
    cfg = DatabaseConfig.from_env()
    db = DatabaseManager(cfg)
    # stream rows
    query = "SELECT * FROM enriched_people ORDER BY enriched_at DESC"
    rows = db.execute_query(query)

    # Build all rows in memory to compute a comprehensive header set
    flat_rows = []
    header_set = set()
    for row in rows:
        out = extract_row(row)
        flat_rows.append(out)
        for k in out.keys():
            header_set.add(k)

    headers = sorted(header_set)
    writer = csv.DictWriter(sys.stdout, fieldnames=headers)
    writer.writeheader()
    for out in flat_rows:
        # Fill missing keys with ''
        normalized = {h: out.get(h, '') for h in headers}
        writer.writerow(normalized)


if __name__ == '__main__':
    main()
