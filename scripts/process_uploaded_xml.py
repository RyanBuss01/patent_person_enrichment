#!/usr/bin/env python3
"""
Reads XML content from stdin and writes normalized patents JSON to output/downloaded_patents.json.
Designed for weekly USPTO ipg*.xml files that may contain multiple XML docs.

Output structure matches Step 0 downloader so downstream steps work unchanged.
"""
import sys
import os
import json
from pathlib import Path
import xml.etree.ElementTree as ET


def get_text(elem, xpath):
    try:
        found = elem.find(xpath)
        return (found.text or '').strip() if (found is not None and found.text) else ''
    except Exception:
        return ''


def extract_one(root):
    patent_number = get_text(root, './/document-id/doc-number')
    patent_date = get_text(root, './/document-id/date')
    patent_title = get_text(root, './/invention-title')
    if not patent_number:
        return None

    # inventors
    inv_nodes = (root.findall('.//parties/inventors/inventor') or
                 root.findall('.//us-parties/inventors/inventor') or
                 root.findall('.//applicants/applicant[@app-type="applicant-inventor"]'))
    inventors = []
    for n in inv_nodes:
        first = get_text(n, './/first-name')
        last = get_text(n, './/last-name')
        city = get_text(n, './/city')
        state = get_text(n, './/state')
        country = get_text(n, './/country')
        if first or last:
            inventors.append({
                'first_name': first,
                'last_name': last,
                'city': city,
                'state': state,
                'country': country
            })

    # assignees
    ass_nodes = (root.findall('.//parties/assignees/assignee') or
                 root.findall('.//us-parties/assignees/assignee'))
    assignees = []
    for a in ass_nodes:
        org = get_text(a, './/orgname')
        first = get_text(a, './/first-name')
        last = get_text(a, './/last-name')
        city = get_text(a, './/city')
        state = get_text(a, './/state')
        country = get_text(a, './/country')
        if org or first or last:
            assignees.append({
                'organization': org,
                'first_name': first,
                'last_name': last,
                'city': city,
                'state': state,
                'country': country,
                'type': 'organization' if org else 'individual'
            })

    return {
        'patent_number': patent_number.strip(),
        'patent_date': patent_date.strip(),
        'patent_title': patent_title.strip(),
        'inventors': inventors,
        'assignees': assignees
    }


def main():
    data = sys.stdin.buffer.read()
    if not data:
        print('No XML data provided', file=sys.stderr)
        sys.exit(2)

    text = data.decode('utf-8', errors='ignore')
    parts = text.split('<?xml')
    patents = []
    for i, part in enumerate(parts):
        if not part.strip():
            continue
        xml = '<?xml' + part if not part.startswith('<?xml') else part
        try:
            root = ET.fromstring(xml)
            p = extract_one(root)
            if p:
                patents.append(p)
        except Exception:
            # ignore malformed chunk
            continue

    out_dir = Path(os.getenv('OUTPUT_DIR', 'output'))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_json = out_dir / 'downloaded_patents.json'
    with out_json.open('w') as f:
        json.dump(patents, f, indent=2)

    results = {
        'success': True,
        'source': 'uploaded_xml',
        'patents_downloaded': len(patents),
        'output_files': { 'json': str(out_json) }
    }
    with (out_dir / 'download_results.json').open('w') as f:
        json.dump(results, f, indent=2)

    print(f"Processed {len(patents)} patents from uploaded XML")


if __name__ == '__main__':
    main()

