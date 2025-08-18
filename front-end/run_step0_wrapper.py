#!/usr/bin/env python3
"""
Step 0 Wrapper: Uses existing integrate_existing_data.py runner
Lightweight wrapper to run Step 0 for the frontend
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

from runners.integrate_existing_data import run_existing_data_integration

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration exactly like main.py does"""
    return {
        'ACCESS_DB_PATH': os.getenv('ACCESS_DB_PATH', "patent_system/Database.mdb"),
        'USPC_DOWNLOAD_PATH': os.getenv('USPC_DOWNLOAD_PATH', "USPC_Download"),
        'CSV_DATABASE_FOLDER': "converted_databases/csv",
        'USE_EXISTING_DATA': os.getenv('USE_EXISTING_DATA', 'true').lower() == 'true',
        'ENRICH_ONLY_NEW_PEOPLE': os.getenv('ENRICH_ONLY_NEW_PEOPLE', 'true').lower() == 'true',
        'MAX_ENRICHMENT_COST': int(os.getenv('MAX_ENRICHMENT_COST', '1000')),
        'OUTPUT_DIR': os.getenv('OUTPUT_DIR', 'output'),
    }

def main():
    """Run Step 0 using existing runner"""
    print("🚀 STARTING STEP 0: INTEGRATE EXISTING DATA")
    print("=" * 60)
    
    # Load configuration (same as main.py)
    config = load_config()
    
    # Create output directory
    os.makedirs(config['OUTPUT_DIR'], exist_ok=True)
    
    try:
        # Run the integration using existing runner
        logger.info("Starting existing data integration...")
        result = run_existing_data_integration(config)
        
        # Save results to JSON file for frontend
        results_file = os.path.join(config['OUTPUT_DIR'], 'integration_results.json')
        with open(results_file, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        
        # Print summary exactly like main.py
        if result.get('success'):
            print("\n✅ STEP 0 COMPLETED SUCCESSFULLY!")
            print("=" * 60)
            print(f"📊 INTEGRATION SUMMARY:")
            print(f"   🗃️  Existing patents in DB: {result.get('existing_patents_count', 0):,}")
            print(f"   👥 Existing people in DB: {result.get('existing_people_count', 0):,}")
            print(f"   🆕 New patents found: {result.get('new_patents_count', 0):,}")
            print(f"   🆕 New people found: {result.get('new_people_count', 0):,}")
            print(f"   🔁 Duplicate patents avoided: {result.get('duplicate_patents_count', 0):,}")
            print(f"   🔁 Duplicate people avoided: {result.get('duplicate_people_count', 0):,}")
            
            # Calculate API cost savings
            total_xml_people = result.get('total_xml_people', 0)
            new_people = result.get('new_people_count', 0)
            if total_xml_people > 0:
                saved_api_calls = total_xml_people - new_people
                estimated_savings = saved_api_calls * 0.03
                print(f"\n💰 COST SAVINGS:")
                print(f"   📉 API calls avoided: {saved_api_calls:,}")
                print(f"   💵 Estimated cost saved: ${estimated_savings:.2f}")
                print(f"   💸 Cost for new people: ${new_people * 0.03:.2f}")
            
            print(f"\n📁 OUTPUT FILES:")
            if result.get('new_patents_count', 0) > 0:
                print(f"   📋 New patents: output/filtered_new_patents.json")
                print(f"   👥 New people: output/new_people_for_enrichment.json")
            print(f"   📊 Integration results: output/integration_results.json")
            
        else:
            print(f"\n❌ STEP 0 FAILED: {result.get('error')}")
            return 1
        
    except Exception as e:
        logger.error(f"Step 0 failed with error: {e}")
        print(f"\n❌ STEP 0 FAILED: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)