#!/usr/bin/env python3
# =============================================================================
# simple_xml_processor.py
# Process XML files directly without Access databases
# =============================================================================
import os
import json
import pandas as pd
import xml.etree.ElementTree as ET
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_xml_files(xml_folder="USPC_Download", output_folder="output"):
    """Process XML files and extract patent data directly"""
    
    xml_path = Path(xml_folder)
    output_path = Path(output_folder)
    output_path.mkdir(exist_ok=True)
    
    if not xml_path.exists():
        logger.error(f"XML folder not found: {xml_folder}")
        return
    
    # Get all XML/TXT files
    xml_files = list(xml_path.glob("*.txt")) + list(xml_path.glob("*.xml"))
    
    if not xml_files:
        logger.warning(f"No XML files found in {xml_folder}")
        return
    
    logger.info(f"Found {len(xml_files)} files to process")
    
    all_patents = []
    
    # Process first 3 files for testing
    for xml_file in xml_files[:3]:
        logger.info(f"Processing: {xml_file.name}")
        patents = extract_patents_from_file(xml_file)
        all_patents.extend(patents)
        logger.info(f"Extracted {len(patents)} patents from {xml_file.name}")
    
    if all_patents:
        # Save as JSON
        json_file = output_path / "extracted_patents.json"
        with open(json_file, 'w') as f:
            json.dump(all_patents, f, indent=2, default=str)
        
        # Save as CSV
        csv_file = output_path / "extracted_patents.csv"
        df = pd.json_normalize(all_patents)
        # Simplify headers to last dotted segment with collision-safe suffixes
        def _simplify_headers(cols):
            mapping = {}
            counts = {}
            simple = []
            for c in cols:
                base = str(c).split('.')[-1]
                n = counts.get(base, 0) + 1
                counts[base] = n
                name = base if n == 1 else f"{base}_{n}"
                mapping[c] = name
                simple.append(name)
            return mapping, simple
        _, simple_cols = _simplify_headers(list(df.columns))
        df.columns = simple_cols
        df.to_csv(csv_file, index=False)
        
        logger.info(f"✅ Processed {len(all_patents)} total patents")
        logger.info(f"📄 Saved to: {json_file}")
        logger.info(f"📄 Saved to: {csv_file}")
        
        return all_patents
    else:
        logger.warning("No patents extracted!")
        return []

def extract_patents_from_file(file_path):
    """Extract patents from a single XML file"""
    patents = []
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Split on XML declarations to handle multiple documents
        xml_docs = content.split('<?xml version="1.0" encoding="UTF-8"?>')
        
        for i, doc in enumerate(xml_docs[1:]):  # Skip first empty split
            try:
                # Add back XML declaration
                full_doc = '<?xml version="1.0" encoding="UTF-8"?>' + doc
                
                # Parse XML
                root = ET.fromstring(full_doc)
                
                # Extract patent data
                patent = extract_patent_data(root)
                if patent:
                    patents.append(patent)
                    
            except ET.XMLSyntaxError:
                continue
            except Exception as e:
                logger.debug(f"Error parsing document {i}: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
    
    return patents

def extract_patent_data(root):
    """Extract patent data from XML root element"""
    try:
        # Basic patent info
        patent_number = get_xml_text(root, './/document-id/doc-number')
        patent_date = get_xml_text(root, './/document-id/date')
        patent_title = get_xml_text(root, './/invention-title')
        
        if not patent_number:
            return None
        
        # Extract inventors
        inventors = []
        inventor_elements = (root.findall('.//parties/inventors/inventor') or 
                           root.findall('.//us-parties/inventors/inventor') or
                           root.findall('.//applicants/applicant[@app-type="applicant-inventor"]'))
        
        for inventor in inventor_elements:
            inventor_data = {
                'first_name': get_xml_text(inventor, './/first-name'),
                'last_name': get_xml_text(inventor, './/last-name'),
                'city': get_xml_text(inventor, './/city'),
                'state': get_xml_text(inventor, './/state'),
                'country': get_xml_text(inventor, './/country'),
                'address': get_xml_text(inventor, './/address-1')
            }
            
            # Only add if we have a name
            if inventor_data['first_name'] or inventor_data['last_name']:
                inventors.append(inventor_data)
        
        # Extract assignees
        assignees = []
        assignee_elements = (root.findall('.//parties/assignees/assignee') or
                           root.findall('.//us-parties/assignees/assignee'))
        
        for assignee in assignee_elements:
            assignee_data = {
                'organization': get_xml_text(assignee, './/orgname'),
                'first_name': get_xml_text(assignee, './/first-name'),
                'last_name': get_xml_text(assignee, './/last-name'),
                'city': get_xml_text(assignee, './/city'),
                'state': get_xml_text(assignee, './/state'),
                'country': get_xml_text(assignee, './/country')
            }
            
            if assignee_data['organization'] or assignee_data['first_name'] or assignee_data['last_name']:
                assignees.append(assignee_data)
        
        return {
            'patent_number': patent_number,
            'patent_date': patent_date,
            'patent_title': patent_title,
            'inventors': inventors,
            'assignees': assignees,
            'total_people': len(inventors) + len([a for a in assignees if a.get('first_name')])
        }
        
    except Exception as e:
        logger.debug(f"Error extracting patent data: {e}")
        return None

def get_xml_text(element, xpath):
    """Safely extract text from XML element"""
    try:
        found = element.find(xpath)
        return found.text.strip() if found is not None and found.text else None
    except:
        return None

if __name__ == "__main__":
    print("🚀 Processing XML Patent Files")
    patents = process_xml_files()
    
    if patents:
        print(f"\n📊 Summary:")
        print(f"Total patents: {len(patents)}")
        total_people = sum(p.get('total_people', 0) for p in patents)
        print(f"Total people: {total_people}")
        print(f"Files saved in 'output/' folder")
    else:
        print("❌ No patents were processed")
