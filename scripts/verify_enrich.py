#!/usr/bin/env python3
"""
Script to match CSV rows against existing_people SQL table
Reads from CSV and checks enrichment data for address matches
Supports 4 modes: 'pdl', 'pdl-full', 'zaba', 'zaba-full'
"""

import os
import csv
import json
import logging
import mysql.connector
from dotenv import load_dotenv
import re

# =============================================================================
# MODE CONFIGURATION - Change this to one of: 'pdl', 'pdl-full', 'zaba', 'zaba-full'
# =============================================================================
MODE = 'pdl-full'

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler(f'../address_matching_results_{MODE}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def connect_db():
    """Simple database connection"""
    return mysql.connector.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', '3306')),
        database=os.getenv('DB_NAME', 'patent_data'),
        user=os.getenv('DB_USER', 'root'),
        password=os.getenv('DB_PASSWORD', 'password'),
        charset='utf8mb4'
    )

def remove_suffixes(name):
    """Remove generational suffixes from a name"""
    if not name:
        return ''
    
    suffixes = ['jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv', 'v']
    cleaned_name = name.strip()
    
    for suffix in suffixes:
        cleaned_name = re.sub(r',\s*' + re.escape(suffix) + r'\b', '', cleaned_name, flags=re.IGNORECASE)
        cleaned_name = re.sub(r'\s+' + re.escape(suffix) + r'\b', '', cleaned_name, flags=re.IGNORECASE)
    
    cleaned_name = re.sub(r',\s*$', '', cleaned_name.strip())
    return cleaned_name

def clean_first_name(full_name):
    """Extract just the first name"""
    name = remove_suffixes(full_name)
    parts = name.split()
    return parts[0].strip() if parts else ''

def clean_last_name(full_name):
    """Extract just the last name"""
    name = remove_suffixes(full_name)
    parts = name.split()
    return parts[-1].strip() if len(parts) >= 2 else name.strip()

def normalize_address(address):
    """Normalize address for matching"""
    if not address:
        return ''
    
    addr = address.strip().lower()
    abbreviations = {
        'place': 'pl', 'street': 'st', 'avenue': 'ave', 'boulevard': 'blvd',
        'drive': 'dr', 'road': 'rd', 'lane': 'ln', 'court': 'ct'
    }
    
    for full, abbrev in abbreviations.items():
        addr = re.sub(r'\b' + full + r'\b', abbrev, addr)
        addr = re.sub(r'\b' + abbrev + r'\b', abbrev, addr)
    
    return ' '.join(addr.split())

def extract_core_address(address):
    """Extract core address without apt/suite info"""
    if not address:
        return ''
    
    addr = normalize_address(address)
    apt_patterns = [
        r'\s+apt\s+\w+.*$', r'\s+apartment\s+\w+.*$', r'\s+unit\s+\w+.*$',
        r'\s+ste\s+\w+.*$', r'\s+suite\s+\w+.*$', r'\s+#\w+.*$'
    ]
    
    for pattern in apt_patterns:
        addr = re.sub(pattern, '', addr, flags=re.IGNORECASE)
    
    return ' '.join(addr.split())

def extract_addresses_from_pdl_json(enrichment_data):
    """Extract all street addresses from PDL enrichment JSON"""
    addresses = []
    
    try:
        if not enrichment_data:
            return addresses
            
        data = json.loads(enrichment_data)
        
        # Get street addresses from enrichment data
        enrichment = data.get('enrichment_result', {}).get('api_raw', {}).get('enrichment', {}).get('data', {})
        street_addresses = enrichment.get('street_addresses', [])
        
        for addr_obj in street_addresses:
            street_addr = addr_obj.get('street_address', '')
            if street_addr:
                addresses.append(street_addr.strip())
        
        # Also check existing_record
        existing_record = data.get('existing_record', {})
        mail_addr = existing_record.get('mail_to_add1', '')
        if mail_addr:
            addresses.append(mail_addr.strip())
            
    except (json.JSONDecodeError, KeyError):
        pass
    
    return addresses

def extract_addresses_from_zaba_json(zaba_data):
    """Extract addresses from Zaba JSON data"""
    addresses = []
    
    try:
        if not zaba_data:
            return addresses
            
        data = json.loads(zaba_data)
        
        # Check for mail_to_add1 in the JSON structure
        mail_addr = data.get('mail_to_add1', '')
        if mail_addr:
            addresses.append(mail_addr.strip())
            
        # You may need to adjust this based on the actual structure of your zaba_data JSON
        # Add other address extraction logic here if needed
            
    except (json.JSONDecodeError, KeyError):
        pass
    
    return addresses

def get_data_batch_from_db(cursor, name_pairs, mode):
    """Get data from enriched_people table in batches based on mode"""
    
    # Determine which column to query and what condition to use
    if mode in ['pdl', 'pdl-full']:
        data_column = 'enrichment_data'
    else:  # zaba or zaba-full
        data_column = 'zaba_data'
    
    # Determine the condition based on full vs non-full mode
    if mode in ['pdl-full', 'zaba-full']:
        # Full mode: get data where the column is not null
        condition = f"AND {data_column} IS NOT NULL AND {data_column} != ''"
    else:
        # Regular mode: get data where the column is not null (same condition for now)
        # You might want to adjust this if "full" means something else
        condition = f"AND {data_column} IS NOT NULL AND {data_column} != ''"
    
    # Build the WHERE clause for multiple name pairs
    # Handle middle initials by matching just the first part of first_name
    where_conditions = []
    params = []
    
    for first_name, last_name in name_pairs:
        # For first name: match if database first_name starts with our first_name
        # This handles cases like CSV "Jeffrey" matching DB "Jeffrey J. Moore"
        where_conditions.append("""(
            LOWER(TRIM(first_name)) LIKE LOWER(TRIM(CONCAT(%s, '%%'))) AND 
            LOWER(TRIM(last_name)) = LOWER(TRIM(%s))
        )""")
        params.extend([first_name, last_name])
    
    where_clause = " OR ".join(where_conditions)
    
    query = f"""
    SELECT first_name, last_name, {data_column} FROM enriched_people 
    WHERE ({where_clause})
    {condition}
    """
    
    logger.info(f"  - Executing batch query for {len(name_pairs)} names...")
    logger.info(f"  - Sample names: {name_pairs[:3]}...")
    
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    logger.info(f"  - Found {len(results)} records with {data_column} data")
    
    # Debug: Show some sample results if found
    if results and len(results) > 0:
        logger.info(f"  - Sample result names: {[(r['first_name'], r['last_name']) for r in results[:3]]}")
    
    # Create a lookup dictionary for easy access
    # Use the CSV names as keys, not the DB names (which may have middle initials)
    data_lookup = {}
    for result in results:
        db_first = result['first_name'].strip()
        db_last = result['last_name'].strip()
        
        # Find which CSV name pair this matches
        for csv_first, csv_last in name_pairs:
            if (db_first.lower().startswith(csv_first.lower()) and 
                db_last.lower() == csv_last.lower()):
                key = (csv_first.lower(), csv_last.lower())
                data_lookup[key] = result[data_column]
                break
    
    return data_lookup

def get_existing_people_batch(cursor, name_pairs):
    """Get existing_people records with matching names in batch"""
    
    # Build the WHERE clause for multiple name pairs
    where_conditions = []
    params = []
    
    for first_name, last_name in name_pairs:
        where_conditions.append("(LOWER(TRIM(first_name)) = LOWER(TRIM(%s)) AND LOWER(TRIM(last_name)) = LOWER(TRIM(%s)))")
        params.extend([first_name, last_name])
    
    where_clause = " OR ".join(where_conditions)
    
    query = f"""
    SELECT first_name, last_name, address FROM existing_people 
    WHERE ({where_clause})
    AND address IS NOT NULL AND address != ''
    """
    
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    # Group results by name
    address_lookup = {}
    for result in results:
        first_clean = clean_first_name(result['first_name'])
        last_clean = clean_last_name(result['last_name'])
        key = (first_clean.lower(), last_clean.lower())
        
        if key not in address_lookup:
            address_lookup[key] = []
        address_lookup[key].append(result['address'])
    
    return address_lookup

def get_all_enriched_records(cursor, mode):
    """Get ALL records from enriched_people table for full modes"""
    
    # Determine which column to query
    if mode == 'pdl-full':
        data_column = 'enrichment_data'
    else:  # zaba-full
        data_column = 'zaba_data'
    
    query = f"""
    SELECT first_name, last_name, {data_column} FROM enriched_people 
    WHERE {data_column} IS NOT NULL AND {data_column} != ''
    """
    
    logger.info(f"  - Pulling ALL records with {data_column} data from database...")
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    logger.info(f"  - Found {len(results)} total records with {data_column} data")
    
    return results

def process_full_mode(cursor, mode):
    """Process all enriched records for full modes (ignore CSV)"""
    
    # Get all enriched records
    enriched_records = get_all_enriched_records(cursor, mode)
    
    # Get all existing people records
    logger.info("  - Getting ALL existing_people records...")
    existing_query = """
    SELECT first_name, last_name, address FROM existing_people 
    WHERE address IS NOT NULL AND address != ''
    """
    cursor.execute(existing_query)
    existing_records = cursor.fetchall()
    logger.info(f"  - Found {len(existing_records)} existing_people records with addresses")
    
    # Build lookup for existing people by name
    existing_lookup = {}
    for record in existing_records:
        first_clean = clean_first_name(record['first_name'])
        last_clean = clean_last_name(record['last_name'])
        key = (first_clean.lower(), last_clean.lower())
        
        if key not in existing_lookup:
            existing_lookup[key] = []
        existing_lookup[key].append(record['address'])
    
    matches_found = 0
    mismatched_addresses = 0
    new_addresses = 0
    total_processed = 0
    
    # Process each enriched record
    for enriched in enriched_records:
        total_processed += 1
        
        # Clean the enriched record names
        enriched_first = clean_first_name(enriched['first_name'])
        enriched_last = clean_last_name(enriched['last_name'])
        
        if not enriched_first or not enriched_last:
            continue
            
        logger.info(f"Record {total_processed}: Processing {enriched_first} {enriched_last} (mode: {mode})")
        
        # Extract addresses from enrichment data
        if mode == 'pdl-full':
            enriched_addresses = extract_addresses_from_pdl_json(enriched['enrichment_data'])
        else:  # zaba-full
            enriched_addresses = extract_addresses_from_zaba_json(enriched['zaba_data'])
        
        if not enriched_addresses:
            logger.info(f"  - NO ADDRESSES in {mode} data")
            continue
        
        logger.info(f"  - Found {len(enriched_addresses)} addresses in {mode} data")
        
        # Look up existing addresses for this name
        key = (enriched_first.lower(), enriched_last.lower())
        existing_addresses = existing_lookup.get(key, [])
        
        if not existing_addresses:
            logger.info(f"  - NEW ADDRESS (not in existing_people)")
            new_addresses += 1
            continue
        
        logger.info(f"  - Found {len(existing_addresses)} existing addresses")
        
        # Check for address match
        has_match = find_address_match(enriched_addresses, existing_addresses)
        
        if has_match:
            matches_found += 1
            logger.info(f"  - ✓ ADDRESS MATCH FOUND!")
        else:
            mismatched_addresses += 1
            logger.info(f"  - × ADDRESS MISMATCH")
    
    return matches_found, mismatched_addresses, new_addresses, total_processed
    """Process a batch of CSV rows"""
    
    # Extract name pairs from the batch
    name_pairs = []
    valid_rows = []
    
    for row_num, row in batch_rows:
        # Get names from CSV
        full_first = (row.get('inventor_first', row.get('first_name', ''))).strip()
        full_last = (row.get('inventor_last', row.get('last_name', ''))).strip()
        
        first_name = clean_first_name(full_first)
        last_name = clean_last_name(full_last)
        
        if first_name and last_name:
            name_pairs.append((first_name, last_name))
            valid_rows.append((row_num, row, first_name, last_name))
    
    if not name_pairs:
        return 0  # No valid rows to process
    
    logger.info(f"Processing batch of {len(name_pairs)} valid name pairs...")
    
    # Get enrichment/zaba data for all names in batch
    data_lookup = get_data_batch_from_db(cursor, name_pairs, mode)
    
    # Get existing people data for all names in batch
    existing_lookup = get_existing_people_batch(cursor, name_pairs)
    
    matches_in_batch = 0
    
    # Process each row in the batch
    for row_num, row, first_name, last_name in valid_rows:
        logger.info(f"Row {row_num}: Processing {first_name} {last_name} (mode: {MODE})")
        
        # Look up data for this name
        key = (first_name.lower(), last_name.lower())
        data_json = data_lookup.get(key)
        
        if not data_json:
            logger.info(f"  - NO DATA in database for mode '{MODE}'")
            continue
        
        logger.info(f"  - Found data, extracting addresses...")
        
        # Extract addresses based on mode
        if mode in ['pdl', 'pdl-full']:
            enriched_addresses = extract_addresses_from_pdl_json(data_json)
        else:  # zaba or zaba-full
            enriched_addresses = extract_addresses_from_zaba_json(data_json)
        
        if not enriched_addresses:
            logger.info(f"  - NO ADDRESSES in {mode} JSON")
            continue
        
        logger.info(f"  - Found {len(enriched_addresses)} addresses in {mode} data")
        
        # Look up existing addresses for this name
        existing_addresses = existing_lookup.get(key, [])
        
        if not existing_addresses:
            logger.info(f"  - NO NAME MATCHES in existing_people")
            continue
        
        logger.info(f"  - Found {len(existing_addresses)} existing addresses")
        
        # Check for address match
        has_match = find_address_match(enriched_addresses, existing_addresses)
        
        if has_match:
            matches_in_batch += 1
            logger.info(f"  - ✓ ADDRESS MATCH FOUND!")
        else:
            logger.info(f"  - × NO ADDRESS MATCH")
    
    return matches_in_batch
    """Check if any addresses match"""
    enriched_cores = [extract_core_address(addr) for addr in enriched_addresses if addr]
    existing_cores = [extract_core_address(addr) for addr in existing_addresses if addr]
    
    for enr_core in enriched_cores:
        if enr_core and enr_core in existing_cores:
            return True
    
    return False

def find_address_match(enriched_addresses, existing_addresses):
    """Check if any addresses match"""
    enriched_cores = [extract_core_address(addr) for addr in enriched_addresses if addr]
    existing_cores = [extract_core_address(addr) for addr in existing_addresses if addr]
    
    for enr_core in enriched_cores:
        if enr_core and enr_core in existing_cores:
            return True
    
    return False

def diagnose_db_issue(cursor, mode):
    """Debug function to check database state"""
    
    # Determine which column to check
    if mode in ['pdl', 'pdl-full']:
        data_column = 'enrichment_data'
    else:  # zaba or zaba-full
        data_column = 'zaba_data'
    
    logger.info(f"\n=== DATABASE DIAGNOSTICS FOR {mode.upper()} MODE ===")
    
    # Check total records in enriched_people
    cursor.execute("SELECT COUNT(*) as total FROM enriched_people")
    total = cursor.fetchone()['total']
    logger.info(f"Total records in enriched_people: {total}")
    
    # Check records with the target data column
    cursor.execute(f"SELECT COUNT(*) as count FROM enriched_people WHERE {data_column} IS NOT NULL AND {data_column} != ''")
    with_data = cursor.fetchone()['count']
    logger.info(f"Records with {data_column}: {with_data}")
    
    # Sample some names from the table
    cursor.execute(f"SELECT first_name, last_name FROM enriched_people WHERE {data_column} IS NOT NULL AND {data_column} != '' LIMIT 5")
    samples = cursor.fetchall()
    logger.info(f"Sample names with {data_column}:")
    for sample in samples:
        logger.info(f"  - {sample['first_name']} {sample['last_name']}")
    
    logger.info("=== END DIAGNOSTICS ===\n")

def main():
    """Main function with batching for CSV modes and full processing for full modes"""
    csv_file = '../output/new_and_existing_enrichments.csv'
    batch_size = 100
    
    logger.info(f"Starting address matching in '{MODE}' mode for: {csv_file}")
    
    # Validate mode
    valid_modes = ['pdl', 'pdl-full', 'zaba', 'zaba-full']
    if MODE not in valid_modes:
        logger.error(f"Invalid mode '{MODE}'. Must be one of: {valid_modes}")
        return
    
    conn = connect_db()
    cursor = conn.cursor(dictionary=True)
    
    # Run diagnostics to check database state
    diagnose_db_issue(cursor, MODE)
    
    matches_found = 0
    mismatched_addresses = 0
    new_addresses = 0
    total_processed = 0
    
    try:
        # Handle full modes differently - ignore CSV, process all database records
        if MODE in ['pdl-full', 'zaba-full']:
            logger.info(f"Running in FULL mode - processing ALL database records, ignoring CSV")
            matches_found, mismatched_addresses, new_addresses, total_processed = process_full_mode(cursor, MODE)
            
        else:
            # Handle CSV-based modes with batching
            logger.info(f"Running in CSV mode - using batch size: {batch_size}")
            batch_num = 0
            
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                batch = []
                row_num = 0
                
                for row in reader:
                    row_num += 1
                    batch.append((row_num, row))
                    
                    # Process batch when it reaches batch_size
                    if len(batch) >= batch_size:
                        batch_num += 1
                        logger.info(f"\n--- Processing Batch {batch_num} (rows {batch[0][0]}-{batch[-1][0]}) ---")
                        
                        matches_in_batch, mismatches_in_batch, new_in_batch = process_batch(batch, cursor, MODE)
                        matches_found += matches_in_batch
                        mismatched_addresses += mismatches_in_batch
                        new_addresses += new_in_batch
                        
                        # Count valid rows in batch (those with names)
                        valid_in_batch = sum(1 for _, row in batch 
                                           if clean_first_name(row.get('inventor_first', row.get('first_name', ''))) 
                                           and clean_last_name(row.get('inventor_last', row.get('last_name', ''))))
                        
                        total_processed += valid_in_batch
                        
                        logger.info(f"Batch {batch_num} complete: {matches_in_batch} matches, {mismatches_in_batch} mismatches, {new_in_batch} new")
                        
                        batch = []  # Reset batch
                
                # Process remaining rows in final batch
                if batch:
                    batch_num += 1
                    logger.info(f"\n--- Processing Final Batch {batch_num} (rows {batch[0][0]}-{batch[-1][0]}) ---")
                    
                    matches_in_batch, mismatches_in_batch, new_in_batch = process_batch(batch, cursor, MODE)
                    matches_found += matches_in_batch
                    mismatched_addresses += mismatches_in_batch
                    new_addresses += new_in_batch
                    
                    # Count valid rows in final batch
                    valid_in_batch = sum(1 for _, row in batch 
                                       if clean_first_name(row.get('inventor_first', row.get('first_name', ''))) 
                                       and clean_last_name(row.get('inventor_last', row.get('last_name', ''))))
                    
                    total_processed += valid_in_batch
                    
                    logger.info(f"Final Batch {batch_num} complete: {matches_in_batch} matches, {mismatches_in_batch} mismatches, {new_in_batch} new")
                
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    
    finally:
        cursor.close()
        conn.close()
    
    # Calculate match rate (matches vs mismatches only, excluding new addresses)
    comparable_records = matches_found + mismatched_addresses
    match_rate = (matches_found / comparable_records * 100) if comparable_records > 0 else 0.0
    
    # Final summary in the requested format
    logger.info("\n" + "=" * 50)
    logger.info(f"MODE: {MODE}")
    if MODE in ['pdl-full', 'zaba-full']:
        logger.info("PROCESSING: ALL database records (CSV ignored)")
    else:
        logger.info("PROCESSING: CSV-based matching")
    logger.info(f"TOTAL PROCESSED: {total_processed}")
    logger.info(f"NEW ADDRESSES: {new_addresses}")
    logger.info(f"MISMATCHED ADDRESSES: {mismatched_addresses}")
    logger.info(f"MATCHES FOUND: {matches_found}")
    logger.info(f"MATCH RATE: {match_rate:.1f}%")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()