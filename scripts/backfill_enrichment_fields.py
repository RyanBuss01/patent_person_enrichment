#!/usr/bin/env python3
"""
Backfill selected existing_people fields into enriched_people.enrichment_data JSON
to ensure formatted CSV exports have stable values even when records are skipped.

Fields backfilled (if present in existing_people or existing_people_new):
  - inventor_id, mod_user, title, patent_no, mail_to_add1/mail_to_zip (or fallback: address/zip)

Usage:
  python3 scripts/backfill_enrichment_fields.py           # apply updates
  python3 scripts/backfill_enrichment_fields.py --dry-run # report only

Relies on database/db_manager DatabaseManager to connect via env.
"""
import json
import sys
from typing import Dict, Any, List
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from database.db_manager import DatabaseManager, DatabaseConfig


def get_columns(db: DatabaseManager, table: str) -> List[str]:
    try:
        rows = db.execute_query(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",
            (db.config.database, table)
        )
        return [r.get('COLUMN_NAME') if isinstance(r, dict) else r[0] for r in rows]
    except Exception:
        return []


def choose_field(cols: List[str], preferred: str, fallback: str = None) -> str:
    if preferred in cols:
        return preferred
    if fallback and fallback in cols:
        return fallback
    return None


def fetch_existing_record(db: DatabaseManager, cols: List[str], first: str, last: str, city: str, state: str) -> Dict[str, Any]:
    fields = {
        'inventor_id': choose_field(cols, 'inventor_id'),
        'mod_user': choose_field(cols, 'mod_user'),
        'title': choose_field(cols, 'title'),
        'patent_no': choose_field(cols, 'patent_no'),
        'mail_to_add1': choose_field(cols, 'mail_to_add1', 'address'),
        'mail_to_zip': choose_field(cols, 'mail_to_zip', 'zip'),
    }
    select_parts = []
    for key, col in fields.items():
        if col:
            if key != col:
                select_parts.append(f"{col} AS {key}")
            else:
                select_parts.append(col)
    if not select_parts:
        return {}
    select_clause = ", ".join(select_parts)
    query = (
        f"SELECT {select_clause} FROM existing_people "
        "WHERE first_name=%s AND last_name=%s AND IFNULL(city,'')=%s AND IFNULL(state,'')=%s LIMIT 1"
    )
    params = (first, last, city or '', state or '')
    rows = db.execute_query(query, params)
    if not rows:
        # retry ignore city
        query2 = (
            f"SELECT {select_clause} FROM existing_people "
            "WHERE first_name=%s AND last_name=%s AND IFNULL(state,'')=%s LIMIT 1"
        )
        rows = db.execute_query(query2, (first, last, state or ''))
    if rows:
        return rows[0] if isinstance(rows[0], dict) else {}
    return {}


def main():
    dry_run = '--dry-run' in sys.argv
    cfg = DatabaseConfig.from_env()
    db = DatabaseManager(cfg)
    cols = get_columns(db, 'existing_people')
    if not cols:
        print('Could not read existing_people columns; aborting.')
        sys.exit(1)

    rows = db.execute_query("SELECT id, first_name, last_name, city, state, enrichment_data FROM enriched_people")
    total = len(rows)
    updated = 0
    skipped = 0
    missing = 0
    for r in rows:
        try:
            ed_raw = r.get('enrichment_data') if isinstance(r, dict) else None
            ed = json.loads(ed_raw) if isinstance(ed_raw, str) else (ed_raw or {})
            ex = ed.get('existing_record') or {}
            if ex and any(str(v or '').strip() for v in ex.values()):
                skipped += 1
                continue
            first = (r.get('first_name') or '').strip()
            last = (r.get('last_name') or '').strip()
            city = (r.get('city') or '').strip()
            state = (r.get('state') or '').strip()
            extra = fetch_existing_record(db, cols, first, last, city, state)
            if not any(str(v or '').strip() for v in extra.values()):
                missing += 1
                continue
            ed['existing_record'] = extra
            if dry_run:
                updated += 1
                continue
            db.execute_query(
                "UPDATE enriched_people SET enrichment_data=%s WHERE id=%s",
                (json.dumps(ed, ensure_ascii=False), r.get('id'))
            )
            updated += 1
        except Exception as e:
            # continue on errors
            continue
    report = {
        'total_rows': total,
        'already_had_existing_record': skipped,
        'updated': updated if not dry_run else f"{updated} (dry-run)",
        'no_extra_found': missing,
    }
    out_dir = Path('output') / 'logs'
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / 'backfill_report.json').open('w') as f:
        json.dump(report, f, indent=2)
    print('Backfill report:', report)


if __name__ == '__main__':
    main()

