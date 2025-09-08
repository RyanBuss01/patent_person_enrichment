#!/usr/bin/env python3
"""
Backfill enrichment_data.pdl_data with full PeopleDataLabs records.

Why: Older rows may contain boolean presence flags (e.g., emails: true) from
identify responses. This script re-fetches full person records and updates the
stored JSON so exports show actual values instead of booleans.

Usage:
  python scripts/backfill_enrichment_records.py [--limit 100]

Env:
  - DB_* for database connection
  - PEOPLEDATALABS_API_KEY for PDL API
"""
import os
import json
import argparse
from typing import Any, Dict

from database.db_manager import DatabaseManager, DatabaseConfig

try:
    from peopledatalabs import PDLPY
except Exception as e:
    raise SystemExit(f"PeopleDataLabs SDK not available: {e}")


def needs_backfill(pdl_data: Dict[str, Any]) -> bool:
    if not isinstance(pdl_data, dict):
        return True
    # Heuristic: if commonly list/object fields are boolean, we need backfill
    suspicious_keys = [
        'emails', 'phone_numbers', 'profiles', 'education', 'experience',
        'location_geo', 'location_name', 'location_names', 'location_locality',
        'location_metro', 'location_region', 'location_country'
    ]
    for k in suspicious_keys:
        v = pdl_data.get(k)
        if isinstance(v, bool):
            return True
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=100, help='Max rows to backfill')
    args = ap.parse_args()

    api_key = os.getenv('PEOPLEDATALABS_API_KEY')
    if not api_key or api_key == 'YOUR_PDL_API_KEY':
        raise SystemExit('PEOPLEDATALABS_API_KEY is required for backfill')

    cfg = DatabaseConfig.from_env()
    db = DatabaseManager(cfg)
    client = PDLPY(api_key=api_key)

    select_sql = (
        "SELECT id, enrichment_data FROM enriched_people "
        "ORDER BY enriched_at DESC LIMIT %s"
    )
    rows = db.execute_query(select_sql, (args.limit,))
    updated = 0
    skipped = 0
    errors = 0

    for row in rows:
        rid = row.get('id')
        ed_raw = row.get('enrichment_data')
        try:
            ed = json.loads(ed_raw) if isinstance(ed_raw, (str, bytes)) else (ed_raw or {})
        except Exception:
            ed = {}
        enriched = ed.get('enrichment_result') or {}
        enriched_data = enriched.get('enriched_data') or ed.get('enriched_data') or {}
        pdl_data = enriched_data.get('pdl_data') or {}

        if not needs_backfill(pdl_data):
            skipped += 1
            continue

        # Try to retrieve full record
        full_data = None
        match_id = pdl_data.get('id') or pdl_data.get('pdl_id')
        try:
            if match_id:
                resp = client.person.retrieve(id=match_id)
                js = resp.json()
                if js.get('status') == 200 and js.get('data'):
                    full_data = js['data']
        except Exception:
            pass

        # If no id, fallback to using original_person data for enrichment
        if full_data is None:
            original = ed.get('original_person') or enriched_data.get('original_data') or {}
            params = {}
            if original.get('first_name'):
                params['first_name'] = str(original.get('first_name')).strip()
            if original.get('last_name'):
                params['last_name'] = str(original.get('last_name')).strip()
            loc_parts = []
            for key in ('city', 'state', 'country'):
                val = original.get(key)
                if val:
                    loc_parts.append(str(val).strip())
            if loc_parts:
                params['location'] = ', '.join(loc_parts)
            try:
                if params:
                    resp = client.person.enrichment(**params)
                    js = resp.json()
                    if js.get('status') == 200 and js.get('data'):
                        full_data = js['data']
            except Exception:
                pass

        if full_data is None:
            errors += 1
            continue

        # Update JSON structure in-place
        enriched_data['pdl_data'] = full_data
        # Prefer storing under enrichment_result.enriched_data for consistency
        if ed.get('enrichment_result'):
            ed['enrichment_result']['enriched_data'] = enriched_data
        else:
            ed['enriched_data'] = enriched_data
        # Keep a marker
        meta = ed.get('enrichment_metadata') or {}
        meta['backfill_method'] = 'retrieve' if match_id else 'enrichment'
        ed['enrichment_metadata'] = meta

        new_json = json.dumps(ed)
        update_sql = "UPDATE enriched_people SET enrichment_data=%s WHERE id=%s"
        try:
            with db.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(update_sql, (new_json, rid))
                conn.commit()
            updated += 1
        except Exception:
            errors += 1
            continue

    print(f"Backfill complete: updated={updated}, skipped={skipped}, errors={errors}")


if __name__ == '__main__':
    main()

