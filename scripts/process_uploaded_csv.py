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
from io import StringIO
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
    import re
    # keep alphanumerics so design or plant prefixes (e.g., D, PP) survive
    s = re.sub(r'[^A-Z0-9]', '', s)
    return s


def first_nonempty(row, *keys):
    for key in keys:
        val = row.get(key)
        if val:
            return val
    return ''

def main():
    data = sys.stdin.read()
    if not data.strip():
        print('No CSV data provided', file=sys.stderr)
        sys.exit(2)

    # Try to sniff dialect safely. Use newline='' to avoid universal newline
    # translation issues when DictReader iterates.
    sio = StringIO(data, newline='')
    try:
        sample = sio.read(2048)
        sio.seek(0)
        dialect = csv.Sniffer().sniff(sample)
    except Exception:
        sio.seek(0)
        dialect = csv.excel
    reader = csv.DictReader(sio, dialect=dialect, skipinitialspace=True)

    # Map headers (case-insensitive, tolerate variations)
    patents = {}
    rows_total = 0
    for row in reader:
        rows_total += 1
        # Normalize keys for safety
        row_l = { norm_header(k): (v or '').strip() for k, v in row.items() }
        pn = clean_patent(first_nonempty(row_l,
                                         'patent number',
                                         'patent_number',
                                         'number',
                                         'patentno',
                                         'patent_no'))
        if not pn:
            # skip rows without patent number
            continue
        issue_date = first_nonempty(row_l, 'issue_date', 'date')
        inventor_name = first_nonempty(row_l, 'inventor, name', 'inventor name', 'inventor')
        inventor_first = first_nonempty(row_l, 'inventor_first', 'first_name')
        inventor_last = first_nonempty(row_l, 'inventor_last', 'last_name')

        if inventor_first or inventor_last:
            first = inventor_first
            last = inventor_last
        else:
            first, last = parse_name(inventor_name)

        a1 = first_nonempty(row_l, 'address 1', 'address1', 'mail_to_add1')
        a2 = first_nonempty(row_l, 'address 2', 'address2', 'mail_to_add2')
        a3 = first_nonempty(row_l, 'address 3', 'address3', 'mail_to_add3')
        city = first_nonempty(row_l, 'city', 'mail_to_city')
        state = first_nonempty(row_l, 'state', 'mail_to_state')
        zipc = first_nonempty(row_l, 'zip', 'zipcode', 'mail_to_zip')
        country = first_nonempty(row_l, 'country', 'mail_to_country')

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
                'patent_title': first_nonempty(row_l, 'patent title', 'title'),
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

    print(f"Processed {len(patents)} patents from uploaded CSV (rows read: {rows_total})")

if __name__ == '__main__':
    main()
