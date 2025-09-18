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
    # Treat booleans as empty in CSV export
    if isinstance(obj, bool):
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
    val = '' if str(obj).strip().lower() in {'nan', 'none', 'null'} else str(obj)
    out[prefix] = val
    return out

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

try:
    from database.db_manager import DatabaseManager, DatabaseConfig
except Exception as e:
    sys.stderr.write(f"Failed to import Database modules: {e}\n")
    sys.exit(1)


def extract_row(row):
    """Flatten a single DB row, including enrichment_data JSON and joined existing_people fields."""
    flat = {}
    # Include common top-level DB columns (stringified)
    base_cols = [
        'id','first_name','last_name','city','state','country','patent_number','person_type',
        'api_cost','enriched_at','created_at','updated_at'
    ]
    # Newly added fields from existing_people (if joined)
    extra_cols = [
        'issue_id','new_issue_rec_num','inventor_id','patent_no','title','issue_date',
        'bar_code','mod_user','mail_to_assignee','mail_to_name','mail_to_add1'
    ]
    for k in base_cols + extra_cols:
        if k in row:
            v = row.get(k)
            # Normalize booleans and dates to strings
            if isinstance(v, bool):
                v = '1' if v else ''
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

    # Dynamically discover existing_people columns to avoid 1054 errors
    col_rows = db.execute_query('SHOW COLUMNS FROM existing_people') or []
    cols = [r.get('Field') or r.get('COLUMN_NAME') or r.get('field') for r in col_rows if isinstance(r, dict)]
    def pick(want, alts=None):
        alts = alts or []
        if want in cols:
            return want
        for a in alts:
            if a in cols:
                return a
        return None
    mapping = {
        'issue_id': pick('issue_id'),
        'new_issue_rec_num': pick('new_issue_rec_num', ['issue_rec_num','rec_num']),
        'inventor_id': pick('inventor_id'),
        'patent_no': pick('patent_no', ['patent_number','patent_num']),
        'title': pick('title', ['patent_title','invention_title']),
        'issue_date': pick('issue_date', ['date','patent_date']),
        'bar_code': pick('bar_code', ['barcode']),
        'mod_user': pick('mod_user', ['modified_by','last_modified_by']),
        'mail_to_assignee': pick('mail_to_assignee', ['assignee','assign_name']),
        'mail_to_name': pick('mail_to_name'),
        'mail_to_add1': pick('mail_to_add1', ['address','addr1','mail_to_add_1'])
    }
    select_parts = []
    for alias, col in mapping.items():
        if not col:
            continue
        select_parts.append(f"ex.{col} AS {alias}" if col != alias else f"ex.{col}")
    select_clause = (', ' + ', '.join(select_parts)) if select_parts else ''

    # Stream rows: include additional fields from existing_people via LEFT JOIN
    query = (
        f"SELECT ep.*{select_clause} "
        "FROM enriched_people ep "
        "LEFT JOIN existing_people ex ON ep.first_name = ex.first_name AND ep.last_name = ex.last_name "
        "AND IFNULL(ep.city,'') = IFNULL(ex.city,'') AND IFNULL(ep.state,'') = IFNULL(ex.state,'') "
        "ORDER BY ep.enriched_at DESC"
    )
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

    # Simplify headers to last dotted segment with collision-safe suffixes
    def simplify_headers(cols):
        mapping = {}
        counts = {}
        display = []
        for c in cols:
            base = c.split('.')[-1]
            n = counts.get(base, 0) + 1
            counts[base] = n
            name = base if n == 1 else f"{base}_{n}"
            mapping[c] = name
            display.append(name)
        return mapping, display

    mapping, display_headers = simplify_headers(headers)

    writer = csv.DictWriter(sys.stdout, fieldnames=display_headers)
    writer.writeheader()
    for out in flat_rows:
        # Remap to display headers, fill missing keys with ''
        normalized = {mapping[h]: out.get(h, '') for h in headers}
        writer.writerow(normalized)


if __name__ == '__main__':
    main()
