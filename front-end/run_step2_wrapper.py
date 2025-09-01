#!/usr/bin/env python3
"""
Step 2 Wrapper: Data Enrichment (moved up from Step 3)
Uses existing enrich.py runner with duplicate prevention and test mode
"""
import sys
import os
import json
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Add the parent directory to sys.path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from runners.enrich import run_sql_data_enrichment as run_enrichment

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config(test_mode=False):
    """Load enrichment configuration like main.py"""
    return {
        'PEOPLEDATALABS_API_KEY': os.getenv('PEOPLEDATALABS_API_KEY', "YOUR_PDL_API_KEY"),
        'XML_FILE_PATH': "ipg250812.xml",
        'OUTPUT_DIR': os.getenv('OUTPUT_DIR', 'output'),
        'OUTPUT_CSV': "output/enriched_patents.csv",
        'OUTPUT_JSON': "output/enriched_patents.json",
        'ENRICH_ONLY_NEW_PEOPLE': os.getenv('ENRICH_ONLY_NEW_PEOPLE', 'true').lower() == 'true',
        'MAX_ENRICHMENT_COST': 2 if test_mode else int(os.getenv('MAX_ENRICHMENT_COST', '1000')),
        'TEST_MODE': test_mode
    }

def load_existing_enrichment():
    """Load existing enrichment data to avoid duplicates"""
    enriched_file = 'output/enriched_patents.json'
    enriched_people = set()
    if os.path.exists(enriched_file):
        try:
            with open(enriched_file, 'r') as f:
                enriched_data = json.load(f)
            for person in enriched_data:
                original_data = person.get('enriched_data', {}).get('original_data', {})
                first_name = (original_data.get('first_name') or '').strip().lower()
                last_name = (original_data.get('last_name') or '').strip().lower()
                city = (original_data.get('city') or '').strip().lower()
                state = (original_data.get('state') or '').strip().lower()
                patent_number = (person.get('patent_number') or '').strip()
                if first_name or last_name:
                    person_id = f"{first_name}_{last_name}_{city}_{state}_{patent_number}"
                    enriched_people.add(person_id)
            logger.info(f"Found {len(enriched_people)} already enriched people")
            return enriched_people, enriched_data
        except Exception as e:
            logger.warning(f"Error loading existing enrichment data: {e}")
    return enriched_people, []

def filter_already_enriched_people(people_data, already_enriched):
    if not already_enriched:
        return people_data
    filtered_people = []
    skipped_count = 0
    for person in people_data:
        first_name = (person.get('first_name') or '').strip().lower()
        last_name = (person.get('last_name') or '').strip().lower()
        city = (person.get('city') or '').strip().lower()
        state = (person.get('state') or '').strip().lower()
        patent_number = (person.get('patent_number') or '').strip()
        if not first_name and not last_name:
            skipped_count += 1
            continue
        person_id = f"{first_name}_{last_name}_{city}_{state}_{patent_number}"
        if person_id not in already_enriched:
            filtered_people.append(person)
        else:
            skipped_count += 1
    if skipped_count > 0:
        logger.info(f"Skipped {skipped_count} people (duplicates or missing names)")
    return filtered_people
def load_people_for_enrichment(config):
    step1_people_file = 'output/new_people_for_enrichment.json'
    if os.path.exists(step1_people_file):
        try:
            with open(step1_people_file, 'r') as f:
                people_data = json.load(f)
            logger.info(f"Loaded {len(people_data)} people from Step 1 results")
            already_enriched, existing_data = load_existing_enrichment()
            filtered_people = filter_already_enriched_people(people_data, already_enriched)
            if len(filtered_people) < len(people_data):
                logger.info(f"After filtering duplicates: {len(filtered_people)} people remain")
            return filtered_people, existing_data
        except Exception as e:
            logger.error(f"Error loading people from Step 1: {e}")
    logger.warning("No Step 1 people data found, will fall back to XML parsing")
    return [], []

def main():
    """Run Step 2: Data Enrichment"""
    # Support test mode via env or CLI flag (compat)
    test_mode = os.getenv('STEP2_TEST_MODE', '').lower() == 'true' or ('--test' in sys.argv)
    print("🚀 STARTING STEP 2: DATA ENRICHMENT" + (" (TEST MODE)" if test_mode else ""))
    print("=" * 60)
    
    config = load_config(test_mode)
    os.makedirs(config['OUTPUT_DIR'], exist_ok=True)
    
    try:
        people_to_enrich, existing_enriched_data = load_people_for_enrichment(config)
        if people_to_enrich:
            config['new_people_data'] = people_to_enrich
            logger.info(f"Will enrich {len(people_to_enrich)} people")
        
        # Backup existing enriched data if present
        backup_existing_data = None
        if os.path.exists(config['OUTPUT_JSON']):
            with open(config['OUTPUT_JSON'], 'r') as f:
                backup_existing_data = json.load(f)
            logger.info(f"Backing up {len(backup_existing_data)} existing enriched records")
        
        # Run enrichment
        logger.info("Starting data enrichment...")
        print("💎 Enriching patent inventor and assignee data")
        if config.get('PEOPLEDATALABS_API_KEY') == 'YOUR_PDL_API_KEY':
            print("⚠️  Using mock data - configure PEOPLEDATALABS_API_KEY for real enrichment")
        result = run_enrichment(config)
        
        # Merge with existing if needed
        if result.get('success') and result.get('enriched_data'):
            new_enriched_data = result['enriched_data']
            if backup_existing_data:
                combined_data = backup_existing_data + new_enriched_data
                with open(config['OUTPUT_JSON'], 'w') as f:
                    json.dump(combined_data, f, indent=2, default=str)
                _export_combined_to_csv(combined_data, config['OUTPUT_CSV'])
                result['total_enriched_records'] = len(combined_data)
                result['new_records_added'] = len(new_enriched_data)
                result['existing_records'] = len(backup_existing_data)
            else:
                result['total_enriched_records'] = len(new_enriched_data)
                result['new_records_added'] = len(new_enriched_data)
                result['existing_records'] = 0
        
        # Save results meta for frontend
        results_file = os.path.join(config['OUTPUT_DIR'], 'enrichment_results.json')
        with open(results_file, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        
        if result.get('success'):
            print("\n✅ STEP 2 COMPLETED SUCCESSFULLY!")
            print("=" * 60)
            print("📊 ENRICHMENT SUMMARY:")
            print(f"   👥 People processed this run: {result.get('total_people', 0):,}")
            print(f"   ✅ Successfully enriched this run: {result.get('enriched_count', 0):,}")
            print(f"   📈 Enrichment rate: {result.get('enrichment_rate', 0):.1f}%")
            if result.get('api_calls_saved'):
                print(f"   💰 API calls saved by deduplication: {result.get('api_calls_saved', 0):,}")
                print(f"   💵 Estimated cost savings: {result.get('estimated_cost_savings', '$0.00')}")
            print(f"   💸 API cost for this run: {result.get('actual_api_cost', '$0.00')}")
            if result.get('total_enriched_records'):
                print(f"   📚 Total enriched records: {result.get('total_enriched_records', 0):,}")
                print(f"   🆕 New records added: {result.get('new_records_added', 0):,}")
                print(f"   📂 Existing records: {result.get('existing_records', 0):,}")
            print("\n📁 OUTPUT FILES:")
            if os.path.exists(config['OUTPUT_CSV']):
                file_size = os.path.getsize(config['OUTPUT_CSV']) / 1024
                print(f"   📄 {config['OUTPUT_CSV']} ({file_size:.1f} KB)")
            if os.path.exists(config['OUTPUT_JSON']):
                file_size = os.path.getsize(config['OUTPUT_JSON']) / 1024
                print(f"   📄 {config['OUTPUT_JSON']} ({file_size:.1f} KB)")
        else:
            print(f"\n❌ STEP 2 FAILED: {result.get('error')}")
            return 1
    except Exception as e:
        logger.error(f"Step 2 failed with error: {e}")
        print(f"\n❌ STEP 2 FAILED: {e}")
        return 1
    return 0

def _safe_join_list(data):
    if not data:
        return ''
    if isinstance(data, list):
        return ', '.join([item.get('address', str(item)) if isinstance(item, dict) and 'address' in item
                         else item.get('number', str(item)) if isinstance(item, dict) and 'number' in item  
                         else str(item) for item in data])
    return str(data)

def _export_combined_to_csv(enriched_data, filename):
    import pandas as pd
    if not enriched_data:
        logger.warning("No enriched data to export")
        return
    rows = []
    for data in enriched_data:
        pdl_data = data.get('enriched_data', {}).get('pdl_data', {})
        original_data = data.get('enriched_data', {}).get('original_data', {})
        row = {
            'patent_number': data.get('patent_number'),
            'patent_title': data.get('patent_title'),
            'original_name': data.get('original_name'),
            'person_type': data.get('enriched_data', {}).get('person_type'),
            'match_score': data.get('match_score'),
            'api_method': data.get('enriched_data', {}).get('api_method'),
            'original_first_name': original_data.get('first_name'),
            'original_last_name': original_data.get('last_name'),
            'original_city': original_data.get('city'),
            'original_state': original_data.get('state'),
            'original_country': original_data.get('country'),
            'enriched_full_name': pdl_data.get('full_name'),
            'enriched_first_name': pdl_data.get('first_name'),
            'enriched_last_name': pdl_data.get('last_name'),
            'enriched_emails': _safe_join_list(pdl_data.get('emails')),
            'enriched_phone_numbers': _safe_join_list(pdl_data.get('phone_numbers')),
            'enriched_linkedin_url': pdl_data.get('linkedin_url'),
            'enriched_current_title': pdl_data.get('job_title'),
            'enriched_current_company': pdl_data.get('job_company_name'),
            'enriched_city': pdl_data.get('location_locality'),
            'enriched_state': pdl_data.get('location_region'),
            'enriched_country': pdl_data.get('location_country'),
            'enriched_industry': pdl_data.get('industry'),
        }
        rows.append(row)
    pd.DataFrame(rows).to_csv(filename, index=False)
    logger.info(f"Exported {len(rows)} combined records to {filename}")

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
