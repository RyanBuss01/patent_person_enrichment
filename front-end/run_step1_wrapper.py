#!/usr/bin/env python3
"""
Step 1 Wrapper: Uses existing extract_patents.py runner
Lightweight wrapper to run Step 1 for the frontend
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

from runners.extract_patents import run_patent_extraction

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
        'PATENTSVIEW_API_KEY': os.getenv('PATENTSVIEW_API_KEY', "YOUR_API_KEY"),  # Changed default
        'EXTRACT_BY_DATE': os.getenv('EXTRACT_BY_DATE', 'true').lower() == 'true',
        'DAYS_BACK': int(os.getenv('DAYS_BACK', '7')),
        'CPC_CODES': ['H04', 'G06'],  # Technology areas (electronics, computing)
        'MAX_RESULTS': int(os.getenv('MAX_RESULTS', '1000')),
        'OUTPUT_DIR': os.getenv('OUTPUT_DIR', 'output'),
    }

def check_if_step1_needed():
    """Check if Step 1 is needed by looking at Step 0 results"""
    try:
        results_file = 'output/integration_results.json'
        if os.path.exists(results_file):
            with open(results_file, 'r') as f:
                step0_results = json.load(f)
            
            if step0_results.get('success') and step0_results.get('new_patents_count', 0) > 0:
                print("ℹ️  Step 0 found XML patent data - Step 1 extraction not needed")
                print(f"   Found {step0_results.get('new_patents_count', 0)} new patents from XML files")
                print("   Skipping API extraction and using XML data instead.")
                
                # Create a mock successful result since we don't need API data
                return False, {
                    'success': True,
                    'total_patents': step0_results.get('new_patents_count', 0),
                    'source': 'xml_files',
                    'skipped_api': True,
                    'message': 'Used XML data from Step 0 instead of API extraction'
                }
        
        print("No Step 0 data found, proceeding with API extraction...")
        return True, None
        
    except Exception as e:
        logger.warning(f"Could not check Step 0 results: {e}")
        print("Could not check Step 0 results, proceeding with API extraction...")
        return True, None

def main():
    """Run Step 1 using existing runner"""
    print("🚀 STARTING STEP 1: EXTRACT PATENTS FROM USPTO API")
    print("=" * 60)
    
    # Check if this step is needed
    should_run, mock_result = check_if_step1_needed()
    
    if not should_run:
        # Save the mock result and return success
        config = load_config()
        os.makedirs(config['OUTPUT_DIR'], exist_ok=True)
        
        results_file = os.path.join(config['OUTPUT_DIR'], 'extraction_results.json')
        with open(results_file, 'w') as f:
            json.dump(mock_result, f, indent=2, default=str)
        
        print("\n✅ STEP 1 COMPLETED (SKIPPED - USING XML DATA)!")
        print("=" * 60)
        print(f"📊 DATA SOURCE SUMMARY:")
        print(f"   📋 Patents available from XML: {mock_result.get('total_patents', 0):,}")
        print(f"   📄 Source: XML files from Step 0")
        print(f"   🚫 API extraction skipped (not needed)")
        
        print(f"\n📁 DATA LOCATION:")
        print(f"   📋 Patent data: output/filtered_new_patents.json (from Step 0)")
        print(f"   👥 People data: output/new_people_for_enrichment.json (from Step 0)")
        
        print(f"\n🔄 NEXT STEP:")
        print(f"   Run Step 2 (Data Enrichment) to add contact information")
        
        return 0
    
    # Load configuration (same as main.py)
    config = load_config()
    
    # Create output directory
    os.makedirs(config['OUTPUT_DIR'], exist_ok=True)
    
    try:
        # Run the extraction using existing runner
        logger.info("Starting patent extraction from USPTO API...")
        print(f"📡 Extracting patents from last {config['DAYS_BACK']} days")
        print(f"📊 Maximum results: {config['MAX_RESULTS']:,}")
        
        result = run_patent_extraction(config)
        
        # Save results to JSON file for frontend
        results_file = os.path.join(config['OUTPUT_DIR'], 'extraction_results.json')
        with open(results_file, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        
        # Print summary exactly like main.py
        if result.get('success'):
            print("\n✅ STEP 1 COMPLETED SUCCESSFULLY!")
            print("=" * 60)
            print(f"📊 EXTRACTION SUMMARY:")
            print(f"   📋 Total patents extracted: {result.get('total_patents', 0):,}")
            
            api_info = result.get('api_info', {})
            if api_info:
                print(f"   📡 API: {api_info.get('api_version', 'Unknown')}")
                print(f"   📅 Date range: {api_info.get('date_range', 'Unknown')}")
            
            print(f"\n📁 OUTPUT FILES:")
            for output_file in result.get('output_files', []):
                if os.path.exists(output_file):
                    file_size = os.path.getsize(output_file) / 1024  # KB
                    print(f"   📄 {output_file} ({file_size:.1f} KB)")
            
            print(f"\n🔄 NEXT STEP:")
            print(f"   Run Step 2 (Data Enrichment) to add contact information")
            
        else:
            print(f"\n❌ STEP 1 FAILED: {result.get('error')}")
            
            # Show helpful troubleshooting info
            if 'troubleshooting' in result:
                print(f"\n🔧 TROUBLESHOOTING:")
                for key, value in result['troubleshooting'].items():
                    print(f"   • {key.replace('_', ' ').title()}: {value}")
            
            if 'help' in result:
                print(f"\n💡 HELP: {result['help']}")
            
            # Suggest using XML data instead
            step0_count = check_step0_patent_count()
            if step0_count > 0:
                print(f"\n💡 ALTERNATIVE:")
                print(f"   Since Step 0 found {step0_count:,} patents from XML,")
                print(f"   you can skip Step 1 and proceed directly to Step 2 (Enrichment)")
            
            return 1
        
    except Exception as e:
        logger.error(f"Step 1 failed with error: {e}")
        print(f"\n❌ STEP 1 FAILED: {e}")
        print(f"\n💡 SUGGESTION:")
        print(f"   Since Step 0 found XML data, you can skip Step 1")
        print(f"   and proceed directly to Step 2 (Enrichment)")
        return 1
    
    return 0

def check_step0_patent_count():
    """Helper to get patent count from Step 0"""
    try:
        with open('output/integration_results.json', 'r') as f:
            step0_results = json.load(f)
        return step0_results.get('new_patents_count', 0)
    except:
        return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)