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
    s = re.sub(r'[^A-Z0-9]', '', str(num).strip().upper())
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

    def first_nonempty(row, *names):
        for name in names:
            if name in row:
                value = row[name]
                if value and str(value).strip():
                    return str(value).strip()
        return ''

    patents = {}
    for _, r in df.iterrows():
        row = r.to_dict()
        pn = clean_patent(first_nonempty(row,
                                         'patent number',
                                         'patent_number',
                                         'number',
                                         'patentno',
                                         'patent_no'))
        if not pn:
            continue
        issue_date = first_nonempty(row, 'issue_date', 'date')
        inventor_name = first_nonempty(row, 'inventor, name', 'inventor name', 'inventor')
        inventor_first = first_nonempty(row, 'inventor_first', 'first_name')
        inventor_last = first_nonempty(row, 'inventor_last', 'last_name')

        if inventor_first or inventor_last:
            first = inventor_first
            last = inventor_last
        else:
            first, last = parse_name(inventor_name)

        a1 = first_nonempty(row, 'address 1', 'address1', 'mail_to_add1')
        a2 = first_nonempty(row, 'address 2', 'address2', 'mail_to_add2')
        a3 = first_nonempty(row, 'address 3', 'address3', 'mail_to_add3')
        city = first_nonempty(row, 'city', 'mail_to_city')
        state = first_nonempty(row, 'state', 'mail_to_state')
        zipc = first_nonempty(row, 'zip', 'zipcode', 'mail_to_zip')
        country = first_nonempty(row, 'country', 'mail_to_country') or 'US'

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
                'patent_title': first_nonempty(row, 'patent title', 'title'),
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
