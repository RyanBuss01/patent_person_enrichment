#!/usr/bin/env python3
"""
Step 2 Wrapper: Data Enrichment
Routes to either PDL or ZabaSearch enrichment, then generates CSVs
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

from runners.run_pdl_enrich import run_pdl_enrichment
from runners.run_zaba_enrich import run_zaba_enrichment
from runners.csv_builder import generate_all_csvs

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config(test_mode=False, express_mode=False, use_zaba=False):
    """Load enrichment configuration"""
    config = {
        'PEOPLEDATALABS_API_KEY': os.getenv('PEOPLEDATALABS_API_KEY', "YOUR_PDL_API_KEY"),
        'XML_FILE_PATH': "ipg250812.xml",
        'OUTPUT_DIR': os.getenv('OUTPUT_DIR', 'output'),
        'OUTPUT_CSV': "output/enriched_patents.csv",
        'OUTPUT_JSON': "output/enriched_patents.json",
        'ENRICH_ONLY_NEW_PEOPLE': os.getenv('ENRICH_ONLY_NEW_PEOPLE', 'true').lower() == 'true',
        'MAX_ENRICHMENT_COST': 10 if test_mode else int(os.getenv('MAX_ENRICHMENT_COST', '1000')),
        'TEST_MODE': test_mode,
        'EXPRESS_MODE': express_mode,
        'USE_ZABA': use_zaba
    }
    return config

def load_people_for_enrichment():
    """Load people from Step 1 results"""
    step1_people_file = 'output/new_people_for_enrichment.json'
    
    if not os.path.exists(step1_people_file):
        logger.warning("No Step 1 people data found")
        return []
    
    try:
        with open(step1_people_file, 'r') as f:
            people_data = json.load(f)
        
        logger.info(f"Loaded {len(people_data)} people from Step 1 results")
        print(f"STEP 2: Loaded {len(people_data)} people from Step 1")
        return people_data
        
    except Exception as e:
        logger.error(f"Error loading people from Step 1: {e}")
        return []

def main():
    """Run Step 2: Data Enrichment"""
    # Parse command line arguments
    test_mode = os.getenv('STEP2_TEST_MODE', '').lower() == 'true' or ('--test' in sys.argv)
    express_mode = os.getenv('STEP2_EXPRESS_MODE', '').lower() == 'true' or ('--express' in sys.argv)
    rebuild_only = ('--rebuild' in sys.argv)
    use_zaba = ('--zaba' in sys.argv)
    
    method_name = "ZabaSearch Web Scraping" if use_zaba else "PeopleDataLabs API"
    
    print("🚀 STARTING STEP 2: DATA ENRICHMENT" + 
          f" ({method_name})" + 
          (" (TEST MODE)" if test_mode else "") + 
          (" [EXPRESS]" if express_mode else "") +
          (" [REBUILD ONLY]" if rebuild_only else ""))
    print("=" * 60)
    
    config = load_config(test_mode, express_mode, use_zaba)
    run_started_at = datetime.utcnow()
    config['RUN_STARTED_AT'] = run_started_at.isoformat()
    os.makedirs(config['OUTPUT_DIR'], exist_ok=True)

    # Load already-enriched people filtered during Step 1 so CSVs & progress include them
    filtered_existing_path = Path(config['OUTPUT_DIR']) / 'existing_filtered_enriched_people.json'
    already_enriched_people = []
    if filtered_existing_path.exists():
        try:
            with filtered_existing_path.open('r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    already_enriched_people = data
        except Exception as exc:
            logger.warning(f"Could not load existing filtered enriched people: {exc}")
    config['already_enriched_people'] = already_enriched_people
    if already_enriched_people:
        print(f"STEP 2: Loaded {len(already_enriched_people)} already-enriched people from Step 1")

    if rebuild_only:
        try:
            if use_zaba:
                print("🔄 ZabaSearch CSV rebuild mode: generating CSVs from existing ZabaSearch data...")
            else:
                print("🔄 Rebuild-only mode: pulling all enriched people from SQL (no API calls)...")
            
            # Generate CSVs from existing database data
            csv_result = generate_all_csvs(config)
            
            if csv_result.get('success'):
                print(f"\n✅ {method_name.upper()} REBUILD COMPLETED SUCCESSFULLY!")
                print("=" * 60)
                print("📁 OUTPUT FILES:")
                for file_path, stats in csv_result.get('files_generated', {}).items():
                    if os.path.exists(file_path):
                        file_size = os.path.getsize(file_path) / 1024
                        records = stats.get('records_written', 0)
                        print(f"   📄 {file_path} ({file_size:.1f} KB, {records:,} records)")
                return 0
            else:
                print(f"\n❌ {method_name.upper()} REBUILD FAILED: {csv_result.get('error')}")
                return 1
                
        except Exception as e:
            logger.error(f"{method_name} rebuild failed: {e}")
            print(f"\n❌ STEP 2 {method_name.upper()} REBUILD FAILED: {e}")
            return 1
    try:
        # Load people to enrich
        people_to_enrich = load_people_for_enrichment()
        if not people_to_enrich:
            print("No people to enrich. Generating CSVs from existing data...")
            csv_result = generate_all_csvs(config)
            return 0 if csv_result.get('success') else 1
        
        config['new_people_data'] = people_to_enrich
        
        # Run enrichment based on method
        if use_zaba:
            logger.info("Starting ZabaSearch web scraping enrichment...")
            print("🕸️ Enriching patent inventor data via ZabaSearch web scraping")
            result = run_zaba_enrichment(config)
        else:
            logger.info("Starting PeopleDataLabs API enrichment...")
            print("💎 Enriching patent inventor and assignee data via PeopleDataLabs API")
            result = run_pdl_enrichment(config)
        
        # Generate CSVs after enrichment
        if result.get('success'):
            logger.info("Enrichment completed successfully, generating CSVs...")
            config['enrichment_result'] = result
            csv_result = generate_all_csvs(config)
            config.pop('enrichment_result', None)
            
            # Merge results
            result.update({
                'csv_generation': csv_result,
                'files_generated': csv_result.get('files_generated', {})
            })
        
        # Save results metadata
        results_file = os.path.join(config['OUTPUT_DIR'], 'enrichment_results.json')
        with open(results_file, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        
        if result.get('success'):
            print("\n✅ STEP 2 COMPLETED SUCCESSFULLY!")
            print("=" * 60)
            print("📊 ENRICHMENT SUMMARY:")
            print(f"   🔧 Method: {method_name}")
            print(f"   👥 People processed this run: {result.get('total_people', 0):,}")
            print(f"   ✅ Successfully enriched this run: {result.get('enriched_count', 0):,}")
            print(f"   📈 Enrichment rate: {result.get('enrichment_rate', 0):.1f}%")
            
            if result.get('already_enriched_count') is not None:
                print(f"   🔁 Duplicates skipped: {result.get('already_enriched_count', 0):,}")
            
            # Cost reporting based on method
            if use_zaba:
                print(f"   💰 Scraping cost for this run: $0.00 (Free web scraping)")
                if result.get('failed_count'):
                    print(f"   ❌ Failed scrapes: {result.get('failed_count', 0):,}")
            else:
                if result.get('api_calls_saved'):
                    print(f"   💰 API calls saved by deduplication: {result.get('api_calls_saved', 0):,}")
                    print(f"   💵 Estimated cost savings: {result.get('estimated_cost_savings', '$0.00')}")
                print(f"   💸 API cost for this run: {result.get('actual_api_cost', '$0.00')}")
            
            print(f"   📚 Total enriched records: {result.get('total_enriched_records', 0):,}")

            print("\n📁 OUTPUT FILES:")
            for file_path, stats in result.get('files_generated', {}).items():
                if os.path.exists(file_path):
                    file_size = os.path.getsize(file_path) / 1024
                    records = stats.get('records_written', 0)
                    print(f"   📄 {file_path} ({file_size:.1f} KB, {records:,} records)")
                    
        else:
            print(f"\n❌ STEP 2 FAILED: {result.get('error')}")
            return 1
            
    except Exception as e:
        logger.error(f"Step 2 failed with error: {e}")
        print(f"\n❌ STEP 2 FAILED: {e}")
        return 1
    
    return 0
    
if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
