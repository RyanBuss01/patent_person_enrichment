#!/usr/bin/env python3
"""
Reads XLSX content from stdin and writes normalized patents JSON to output/downloaded_patents.json.
Expected columns (case-insensitive):
  Patent Number, Abstract (ignored), issue_date, Inventor, Name, Address 1, Address 2, Address 3, City, State, Zip, Country
Any may be empty. Aggregates inventors by patent.
"""
import sys
import os
import json
from io import BytesIO
from pathlib import Path
import pandas as pd

def norm(h):
    return (str(h) if h is not None else '').strip().lower()

def parse_name(name):
    if not name or not str(name).strip():
        return '', ''
    s = str(name).strip()
    if ',' in s:
        last, first = s.split(',', 1)
        return first.strip(), last.strip()
    parts = s.split()
    if len(parts) == 1:
        return '', parts[0]
    return parts[0], ' '.join(parts[1:])

def clean_patent(num):
    if num is None:
        return ''
    import re
    s = re.sub(r'[^0-9]', '', str(num).strip().upper())
    return s

def main():
    data = sys.stdin.buffer.read()
    if not data:
        print('No XLSX data provided', file=sys.stderr)
        sys.exit(2)
    df = pd.read_excel(BytesIO(data), dtype=str)
    # Normalize columns
    cols = { c: norm(c) for c in df.columns }
    df.rename(columns=cols, inplace=True)

    def pick(row, *names):
        for n in names:
            if n in row and str(row[n]).strip():
                return str(row[n]).strip()
        return ''

    patents = {}
    for _, r in df.iterrows():
        row = r.to_dict()
        pn = clean_patent(pick(row, 'patent number', 'patent_number', 'number'))
        if not pn:
            continue
        issue_date = pick(row, 'issue_date', 'date')
        inventor_name = pick(row, 'inventor, name', 'inventor name', 'inventor')
        a1 = pick(row, 'address 1', 'address1')
        a2 = pick(row, 'address 2', 'address2')
        a3 = pick(row, 'address 3', 'address3')
        city = pick(row, 'city')
        state = pick(row, 'state')
        zipc = pick(row, 'zip', 'zipcode')
        country = pick(row, 'country') or 'US'

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
            'country': country,
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

    results = {
        'success': True,
        'source': 'uploaded_xlsx',
        'patents_downloaded': len(patents),
        'output_files': { 'json': str(out_file) }
    }
    with (output_dir / 'download_results.json').open('w') as f:
        json.dump(results, f, indent=2)

    print(f"Processed {len(patents)} patents from uploaded XLSX")

if __name__ == '__main__':
    main()

