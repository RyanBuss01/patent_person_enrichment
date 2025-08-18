#!/usr/bin/env python3
# =============================================================================
# diagnostic_matching.py - Debug the matching algorithm
# =============================================================================
import pandas as pd
import logging
from pathlib import Path
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug_matching_algorithm():
    """Debug what's going wrong with the matching"""
    
    print("🔍 DIAGNOSTIC: DEBUGGING MATCHING ALGORITHM")
    print("=" * 60)
    
    # Step 1: Sample the data to understand the format
    debug_csv_data_format()
    
    # Step 2: Test person key generation
    debug_person_key_generation()
    
    # Step 3: Check patent column issues
    debug_patent_columns()
    
    # Step 4: Compare sample XML vs CSV data
    debug_sample_matching()

def debug_csv_data_format():
    """Debug the actual format of CSV data"""
    print("\n📊 STEP 1: CSV DATA FORMAT ANALYSIS")
    print("-" * 40)
    
    csv_folder = Path("converted_databases/csv")
    
    # Check New_Issue file format
    new_issue_file = csv_folder / "uspc_new_issue_New_Issue.csv"
    if new_issue_file.exists():
        print(f"\n📄 NEW_ISSUE FILE ANALYSIS:")
        df = pd.read_csv(new_issue_file, nrows=1000)
        
        # Show inventor_status distribution
        status_counts = df['inventor_status'].value_counts()
        print(f"   📊 Inventor Status Distribution:")
        for status, count in status_counts.items():
            print(f"      {status}: {count}")
        
        # Show successful matches
        successful = df[df['inventor_status'].isin(['Found Inventor Valid', 'Matched by Operator'])]
        print(f"   ✅ Successful matches in sample: {len(successful)}/1000 ({len(successful)/10:.1f}%)")
        
        # Show sample successful record
        if len(successful) > 0:
            sample = successful.iloc[0]
            print(f"   📋 Sample successful record:")
            print(f"      Name: {sample.get('inventor_first', '')} {sample.get('inventor_last', '')}")
            print(f"      Location: {sample.get('inventor_city', '')}, {sample.get('inventor_state', '')}")
            print(f"      Status: {sample.get('inventor_status', '')}")
            print(f"      Inventor ID: {sample.get('inventor_id', '')}")
            print(f"      Patent: {sample.get('patent_num', '')}")
    
    # Check Master Inventor file format
    master_file = csv_folder / "uspc_patent_data_Inventor.csv"
    if master_file.exists():
        print(f"\n📄 MASTER INVENTOR FILE ANALYSIS:")
        df = pd.read_csv(master_file, nrows=1000)
        print(f"   📊 Columns: {list(df.columns)}")
        
        # Show sample record
        if len(df) > 0:
            sample = df.iloc[0]
            print(f"   📋 Sample master inventor:")
            print(f"      ID: {sample.get('inventor_id', '')}")
            print(f"      Name: {sample.get('inventor_first', '')} {sample.get('inventor_last', '')}")
            print(f"      Location: {sample.get('inventor_city', '')}, {sample.get('inventor_state', '')}")

def debug_person_key_generation():
    """Debug person key generation logic"""
    print("\n🔑 STEP 2: PERSON KEY GENERATION DEBUG")
    print("-" * 40)
    
    # Test cases from your logs
    test_cases = [
        {
            'first_name': 'Pin-Yang',
            'last_name': 'Chang', 
            'city': 'New Taipei',
            'state': None,
            'country': 'Taiwan'
        },
        {
            'first_name': 'Karen',
            'last_name': 'Valentin',
            'city': 'Birmingham', 
            'state': 'AL',
            'country': 'US'
        },
        {
            'first_name': 'Jay',
            'last_name': 'Welford',
            'city': 'West Bloomfield',
            'state': 'MI',
            'country': 'US'
        }
    ]
    
    for i, person in enumerate(test_cases, 1):
        print(f"\n   Test Case {i}: {person['first_name']} {person['last_name']}")
        
        # Current algorithm
        key = create_access_db_person_key(
            person['first_name'],
            person['last_name'], 
            person.get('city', ''),
            person.get('state', '')
        )
        print(f"      Current key: '{key}'")
        
        # Alternative algorithms to test
        alt_key1 = f"{person['first_name'].lower()}|{person['last_name'].lower()}"
        alt_key2 = f"{person['first_name'].lower()}|{person['last_name'].lower()}|{person.get('state', '').lower()}"
        alt_key3 = f"{person['first_name'].lower()}|{person['last_name'].lower()}|{person.get('city', '').lower()}|{person.get('state', '').lower()}"
        
        print(f"      Alt key 1 (name only): '{alt_key1}'")
        print(f"      Alt key 2 (name + state): '{alt_key2}'")
        print(f"      Alt key 3 (name + city + state): '{alt_key3}'")

def debug_patent_columns():
    """Debug why patent columns aren't being found"""
    print("\n📋 STEP 3: PATENT COLUMN DEBUG")
    print("-" * 40)
    
    csv_folder = Path("converted_databases/csv")
    patent_files = [
        'patent_table_Patents.csv',
        'PatentHistorical_PatentsHistorical.csv'
    ]
    
    for filename in patent_files:
        file_path = csv_folder / filename
        if file_path.exists():
            print(f"\n📄 {filename}:")
            df = pd.read_csv(file_path, nrows=10)
            print(f"   📊 Columns: {list(df.columns)}")
            print(f"   📋 Sample data:")
            print(df.head(3).to_string())
        else:
            print(f"\n❌ {filename}: File not found")

def debug_sample_matching():
    """Debug a sample matching case"""
    print("\n🎯 STEP 4: SAMPLE MATCHING DEBUG")
    print("-" * 40)
    
    # Load sample XML data
    try:
        with open('output/extracted_patents.json', 'r') as f:
            xml_patents = json.load(f)
        
        if xml_patents:
            sample_patent = xml_patents[0]
            print(f"\n📋 Sample XML Patent:")
            print(f"   Number: {sample_patent.get('patent_number', '')}")
            print(f"   Title: {sample_patent.get('patent_title', '')}")
            
            if sample_patent.get('inventors'):
                inventor = sample_patent['inventors'][0]
                print(f"   Sample Inventor:")
                print(f"      Name: {inventor.get('first_name', '')} {inventor.get('last_name', '')}")
                print(f"      Location: {inventor.get('city', '')}, {inventor.get('state', '')}")
                
                # Generate person key
                xml_key = create_access_db_person_key(
                    inventor.get('first_name', ''),
                    inventor.get('last_name', ''),
                    inventor.get('city', ''),
                    inventor.get('state', '')
                )
                print(f"      Generated key: '{xml_key}'")
    
    except Exception as e:
        print(f"   ❌ Error loading XML data: {e}")
    
    # Load sample CSV data and look for potential matches
    csv_folder = Path("converted_databases/csv")
    new_issue_file = csv_folder / "uspc_new_issue_New_Issue.csv"
    
    if new_issue_file.exists():
        print(f"\n📊 Sample CSV matches:")
        df = pd.read_csv(new_issue_file, nrows=10000)  # Larger sample
        
        successful = df[df['inventor_status'].isin(['Found Inventor Valid', 'Matched by Operator'])]
        
        if len(successful) > 0:
            for i, (_, row) in enumerate(successful.head(5).iterrows()):
                csv_key = create_access_db_person_key(
                    row.get('inventor_first', ''),
                    row.get('inventor_last', ''),
                    row.get('inventor_city', ''),
                    row.get('inventor_state', '')
                )
                print(f"   CSV {i+1}: {row.get('inventor_first', '')} {row.get('inventor_last', '')} -> '{csv_key}'")

def create_access_db_person_key(first_name: str, last_name: str, city: str = '', state: str = ''):
    """Replicate the current person key logic"""
    first = clean_name(first_name)
    last = clean_name(last_name)
    
    if not first or not last:
        return None
    
    state_clean = clean_state(state)
    return f"{first.lower()}|{last.lower()}|{state_clean.lower()}"

def clean_name(name):
    """Clean name field"""
    if pd.isna(name) or not str(name).strip() or str(name).lower() in ['nan', 'none', 'null', '']:
        return ""
    
    cleaned = str(name).strip().title()
    
    # Remove common suffixes
    suffixes = [' Jr', ' Sr', ' II', ' III', ' Jr.', ' Sr.']
    for suffix in suffixes:
        if cleaned.endswith(suffix):
            cleaned = cleaned[:-len(suffix)].strip()
    
    return cleaned

def clean_state(state):
    """Clean state field"""
    if pd.isna(state) or not str(state).strip() or str(state).lower() in ['nan', 'none', 'null', '']:
        return ""
    
    state_str = str(state).strip().upper()
    
    # State mapping
    state_mapping = {
        'CALIFORNIA': 'CA', 'NEW YORK': 'NY', 'TEXAS': 'TX', 'FLORIDA': 'FL',
        'ILLINOIS': 'IL', 'PENNSYLVANIA': 'PA', 'OHIO': 'OH', 'GEORGIA': 'GA',
        'NORTH CAROLINA': 'NC', 'MICHIGAN': 'MI', 'NEW JERSEY': 'NJ', 'VIRGINIA': 'VA',
        'WASHINGTON': 'WA', 'ARIZONA': 'AZ', 'MASSACHUSETTS': 'MA', 'TENNESSEE': 'TN',
        'INDIANA': 'IN', 'MISSOURI': 'MO', 'MARYLAND': 'MD', 'WISCONSIN': 'WI',
        'COLORADO': 'CO', 'MINNESOTA': 'MN', 'SOUTH CAROLINA': 'SC', 'ALABAMA': 'AL',
        'LOUISIANA': 'LA', 'KENTUCKY': 'KY', 'OREGON': 'OR', 'OKLAHOMA': 'OK',
        'CONNECTICUT': 'CT', 'UTAH': 'UT', 'IOWA': 'IA', 'NEVADA': 'NV'
    }
    
    return state_mapping.get(state_str, state_str)

if __name__ == "__main__":
    debug_matching_algorithm()