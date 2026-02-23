#!/usr/bin/env python3
"""
Business Step 2 Wrapper: PDL Company Enrichment
Enriches extracted trademark data using PeopleDataLabs Company API,
then generates CSV exports for CRM/Zapier import.
"""
import sys
import os
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Add the parent directory to sys.path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from runners.run_company_enrich import run_company_enrichment

# Load environment variables
load_dotenv()

# Configure logging
BIZ_OUTPUT_DIR = 'output/business'
os.makedirs(BIZ_OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(BIZ_OUTPUT_DIR, 'biz_step2_progress.log'), mode='w')
    ]
)
logger = logging.getLogger(__name__)


def write_progress_update(stage, details=""):
    """Write progress updates that the server can read."""
    progress_info = {
        'timestamp': datetime.now().isoformat(),
        'stage': stage,
        'details': details
    }

    if details:
        print(f"PROGRESS: {stage} - {details}")
    else:
        print(f"PROGRESS: {stage}")
    sys.stdout.flush()

    try:
        with open(os.path.join(BIZ_OUTPUT_DIR, 'biz_step2_progress.json'), 'w') as f:
            json.dump(progress_info, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not write progress file: {e}")


def parse_search_fields():
    """Parse --search-fields argument from command line."""
    for i, arg in enumerate(sys.argv):
        if arg == '--search-fields' and i + 1 < len(sys.argv):
            return sys.argv[i + 1].split(',')
    return ['name', 'location']  # Default fields


def load_trademarks():
    """Load trademark data from Step 1 results."""
    trademarks_file = os.path.join(BIZ_OUTPUT_DIR, 'extracted_trademarks.json')

    if not os.path.exists(trademarks_file):
        logger.warning("No Step 1 trademark data found")
        return []

    try:
        with open(trademarks_file, 'r') as f:
            trademarks = json.load(f)
        logger.info(f"Loaded {len(trademarks)} trademarks from Step 1 results")
        return trademarks
    except Exception as e:
        logger.error(f"Error loading trademarks from Step 1: {e}")
        return []


def main():
    """Run Business Step 2: PDL Company Enrichment"""
    test_mode = '--test' in sys.argv
    search_fields = parse_search_fields()

    method_name = "PeopleDataLabs Company API"

    print("🚀 STARTING BUSINESS STEP 2: PDL COMPANY ENRICHMENT" +
          (" (TEST MODE - 5 records)" if test_mode else ""))
    print("=" * 60)
    print(f"🔍 Search fields: {', '.join(search_fields)}")

    start_time = time.time()
    write_progress_update("Initializing", "Loading configuration and trademark data")

    api_key = os.getenv('PEOPLEDATALABS_API_KEY', 'YOUR_PDL_API_KEY')
    if not api_key or api_key == 'YOUR_PDL_API_KEY':
        print(f"❌ PEOPLEDATALABS_API_KEY is not set. Cannot enrich without a valid API key.")
        write_progress_update("Error", "PEOPLEDATALABS_API_KEY is not set")
        return 1
    print(f"Using PDL API key: {api_key[:10]}...")

    config = {
        'PEOPLEDATALABS_API_KEY': api_key,
        'OUTPUT_DIR': BIZ_OUTPUT_DIR,
        'TEST_MODE': test_mode,
        'SEARCH_FIELDS': search_fields,
    }

    try:
        # Load trademarks from Step 1
        trademarks = load_trademarks()
        if not trademarks:
            print("❌ No trademark data found. Run Business Step 1 first.")
            write_progress_update("Error", "No trademark data available")
            return 1

        print(f"📋 Loaded {len(trademarks)} trademarks for enrichment")
        if test_mode:
            print(f"🧪 TEST MODE: Will only process first 5 records")

        config['trademark_data'] = trademarks

        # Run enrichment
        write_progress_update("Enriching", f"Processing {len(trademarks)} companies via {method_name}")
        print(f"💎 Enriching trademark data via {method_name}")

        result = run_company_enrichment(config)

        if not result.get('success'):
            error_msg = result.get('error', 'Enrichment failed')
            write_progress_update("Error", error_msg)
            print(f"\n❌ BUSINESS STEP 2 FAILED: {error_msg}")
            return 1

        # Save last output for persistence
        elapsed_time = time.time() - start_time

        print("\n✅ BUSINESS STEP 2 COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"📊 ENRICHMENT SUMMARY:")
        print(f"   🔧 Method: {method_name}")
        print(f"   🔍 Search fields: {', '.join(search_fields)}")
        skipped = result.get('skipped_dedup', 0)
        total_new = result.get('total_companies', 0)
        print(f"   🏢 New companies processed: {total_new:,}")
        print(f"   ✅ Successfully enriched: {result.get('enriched_count', 0):,}")
        print(f"      - via Enrichment API: {result.get('enrich_matches', 0):,}")
        print(f"      - via Search fallback: {result.get('search_matches', 0):,}")
        no_match = result.get('failed_count', 0)
        if no_match:
            print(f"   ⚠️  No match found: {no_match:,}")
        if skipped:
            print(f"   🔁 Already enriched (skipped): {skipped:,}")
        print(f"   📈 Enrichment rate: {result.get('enrichment_rate', 0):.1f}%")
        print(f"   💸 API calls: {result.get('api_calls', 0):,}")
        print(f"   💰 Estimated cost: {result.get('estimated_cost', '$0.00')}")
        print(f"   ⏱️  Total time: {elapsed_time:.1f}s")

        if test_mode:
            print(f"   🧪 TEST MODE: Only processed first 5 records")

        print(f"\n📁 OUTPUT FILES:")
        for file_path, stats in result.get('files_generated', {}).items():
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path) / 1024
                records = stats.get('records_written', 0)
                print(f"   📄 {file_path} ({file_size:.1f} KB, {records:,} records)")

        write_progress_update("Complete",
                              f"Enriched {result.get('enriched_count', 0)}/{result.get('total_companies', 0)} companies")
        print(f"\n🎉 BUSINESS STEP 2 COMPLETE!")
        logger.info("Business Step 2 wrapper completed successfully")

    except Exception as e:
        error_msg = f"Business Step 2 failed with error: {e}"
        logger.error(error_msg)
        write_progress_update("Error", error_msg)
        print(f"\n❌ BUSINESS STEP 2 FAILED: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
