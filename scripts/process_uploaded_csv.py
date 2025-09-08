#!/usr/bin/env python3
"""
Reads CSV content from stdin and writes normalized patents JSON to output/downloaded_patents.json.
CSV columns expected (case-insensitive):
  Patent Number, Abstract, issue_date, Inventor, Name, Address 1, Address 2, Address 3, City, State, Zip, Country
Abstract is ignored. Any field may be empty.
Multiple rows for same patent number will aggregate inventors.
"""
import sys
import os
import json
import csv
from pathlib import Path

def norm_header(h):
    return (h or '').strip().lower()

def parse_name(name):
    if not name:
        return '', ''
    # Expecting "Last, First"; fallback to split by space
    if ',' in name:
        last, first = name.split(',', 1)
        return first.strip(), last.strip()
    parts = name.strip().split()
    if len(parts) == 1:
        return '', parts[0]
    return parts[0], ' '.join(parts[1:])

def clean_patent(num):
    if not num:
        return ''
    s = str(num).strip().upper()
    # remove non-digits
    import re
    s = re.sub(r'[^0-9]', '', s)
    return s

def main():
    data = sys.stdin.read()
    if not data.strip():
        print('No CSV data provided', file=sys.stderr)
        sys.exit(2)

    # Try to sniff dialect safely
    from io import StringIO
    sio = StringIO(data)
    try:
        sample = sio.read(2048)
        sio.seek(0)
        dialect = csv.Sniffer().sniff(sample)
    except Exception:
        sio.seek(0)
        dialect = csv.excel
    reader = csv.DictReader(sio, dialect=dialect)

    # Map headers (case-insensitive, tolerate variations)
    patents = {}
    for row in reader:
        # Normalize keys for safety
        row_l = { norm_header(k): (v or '').strip() for k, v in row.items() }
        pn = clean_patent(row_l.get('patent number') or row_l.get('patent_number') or row_l.get('number'))
        if not pn:
            # skip rows without patent number
            continue
        issue_date = row_l.get('issue_date') or row_l.get('date') or ''
        inventor_name = row_l.get('inventor, name') or row_l.get('inventor name') or row_l.get('inventor') or ''
        a1 = row_l.get('address 1') or row_l.get('address1') or ''
        a2 = row_l.get('address 2') or row_l.get('address2') or ''
        a3 = row_l.get('address 3') or row_l.get('address3') or ''
        city = row_l.get('city') or ''
        state = row_l.get('state') or ''
        zipc = row_l.get('zip') or row_l.get('zipcode') or ''
        country = row_l.get('country') or ''

        first, last = parse_name(inventor_name)
        inv = {
            'first_name': first,
            'last_name': last,
            'address1': a1,
            'address2': a2,
            'address3': a3,
            'city': city,
            'state': state,
            'zip': zipc,
            'country': country or 'US',
            'person_type': 'inventor'
        }

        if pn not in patents:
            patents[pn] = {
                'patent_number': pn,
                'patent_title': '',
                'patent_date': issue_date,
                'inventors': [inv],
                'assignees': []
            }
        else:
            patents[pn]['inventors'].append(inv)

    output_dir = Path(os.getenv('OUTPUT_DIR', 'output'))
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / 'downloaded_patents.json'
    with out_file.open('w') as f:
        json.dump(list(patents.values()), f, indent=2)

    # Also write a simple download_results.json summary
    results = {
        'success': True,
        'source': 'uploaded_csv',
        'patents_downloaded': len(patents),
        'output_files': {
            'json': str(out_file)
        }
    }
    with (output_dir / 'download_results.json').open('w') as f:
        json.dump(results, f, indent=2)

    print(f"Processed {len(patents)} patents from uploaded CSV")

if __name__ == '__main__':
    main()

