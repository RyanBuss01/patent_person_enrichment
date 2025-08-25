#!/usr/bin/env python3
"""
Step 0 Wrapper: Patent Download from PatentsView API
Enhanced wrapper with progress reporting for frontend polling
Provides real-time updates during patent download process
"""
import sys
import os
import json
import logging
import time
import argparse
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Add the parent directory to sys.path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from runners.download_patents import run_patent_download

# Load environment variables
load_dotenv()

# Configure logging with detailed output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('output/step0_download_progress.log', mode='w')
    ]
)
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration exactly like other steps do"""
    return {
        'PATENTSVIEW_API_KEY': os.getenv('PATENTSVIEW_API_KEY', "oq371zFI.BjeAbayJsdHdvEgbei0vskz5bTK3KM1S"),
        'OUTPUT_DIR': os.getenv('OUTPUT_DIR', 'output'),
        'MAX_RESULTS': int(os.getenv('MAX_RESULTS', '1000')),
        'DAYS_BACK': int(os.getenv('DAYS_BACK', '7'))
    }

def write_progress_update(stage, details=""):
    """Write progress updates that the server can read"""
    progress_info = {
        'timestamp': datetime.now().isoformat(),
        'stage': stage,
        'details': details
    }
    
    # Print for immediate server capture
    print(f"PROGRESS: {stage} - {details}")
    sys.stdout.flush()
    
    # Also write to a progress file for persistence
    try:
        with open('output/step0_download_progress.json', 'w') as f:
            json.dump(progress_info, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not write progress file: {e}")

def main():
    """Run Step 0: Download Patents from PatentsView API using the runner"""
    parser = argparse.ArgumentParser(description='Download patents from PatentsView API')
    parser.add_argument('--mode', choices=['smart', 'manual'], default='smart',
                        help='Download mode: smart (automatic) or manual (date range)')
    parser.add_argument('--days-back', type=int, default=7,
                        help='Days to look back in smart mode (default: 7)')
    parser.add_argument('--start-date', type=str,
                        help='Start date for manual mode (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str,
                        help='End date for manual mode (YYYY-MM-DD)')
    parser.add_argument('--max-results', type=int, default=1000,
                        help='Maximum number of patents to download (default: 1000)')
    
    args = parser.parse_args()
    
    print("🚀 STARTING STEP 0: DOWNLOAD PATENTS FROM PATENTSVIEW API")
    print("=" * 60)
    
    # Load configuration (same as main.py)
    config = load_config()
    
    # Create output directory
    os.makedirs(config['OUTPUT_DIR'], exist_ok=True)
    
    # Initialize progress tracking
    start_time = time.time()
    write_progress_update("Initializing", "Loading configuration and preparing download")
    
    try:
        # Update config with command line arguments
        config.update({
            'mode': args.mode,
            'days_back': args.days_back,
            'start_date': args.start_date,
            'end_date': args.end_date,
            'max_results': args.max_results
        })
        
        # Validate manual mode parameters
        if args.mode == 'manual' and (not args.start_date or not args.end_date):
            error_msg = "Manual mode requires --start-date and --end-date parameters"
            logger.error(error_msg)
            write_progress_update("Configuration error", error_msg)
            print(f"\n❌ STEP 0 FAILED: {error_msg}")
            return 1
        
        # Stage 1: Setup and initialization
        logger.info("Starting patent download...")
        write_progress_update("Starting download", f"Using {args.mode} mode with max {args.max_results} results")
        print("⏳ Downloading patents... this may take several minutes depending on the number of results")
        
        # Stage 2: Run the download with progress monitoring
        logger.info("Download process starting...")
        write_progress_update("Connecting to API", "Initializing PatentsView API connection")
        
        # This is where the long-running process happens
        result = run_patent_download(config)
        
        # Stage 3: Check results
        if not result or not result.get('success'):
            error_msg = result.get('error') if result else 'No result returned'
            logger.error(f"Download did not complete successfully: {error_msg}")
            write_progress_update("Download failed", error_msg)
            print(f"\n❌ STEP 0 FAILED: {error_msg}")
            return 1
        
        # Stage 4: Save results
        write_progress_update("Saving results", "Writing download results to output files")
        logger.info("Download completed, results already saved by runner...")
        
        # Stage 5: Generate summary and analysis
        write_progress_update("Generating summary", "Analyzing results and computing statistics")
        logger.info("Results saved, generating summary...")
        
        # Print summary exactly like other steps
        elapsed_time = time.time() - start_time
        print("\n✅ STEP 0 COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"📊 DOWNLOAD SUMMARY:")
        print(f"   🔧 Mode: {result.get('mode', 'unknown')}")
        
        if result.get('mode') == 'smart':
            print(f"   📅 Days back: {config.get('days_back', 'unknown')}")
        elif result.get('mode') == 'manual':
            print(f"   📅 Date range: {config.get('start_date')} to {config.get('end_date')}")
            
        print(f"   📋 Patents downloaded: {result.get('patents_downloaded', 0):,}")
        print(f"   🌐 API requests made: {result.get('api_requests_made', 'unknown')}")
        print(f"   ⏱️  Total download time: {elapsed_time/60:.1f} minutes")
        
        # Stage 6: Show sample patents
        if result.get('patents_downloaded', 0) > 0:
            # Try to read sample patents from the file
            try:
                with open(os.path.join(config['OUTPUT_DIR'], 'downloaded_patents.json'), 'r') as f:
                    patents_data = json.load(f)
                    
                print(f"\n📋 SAMPLE PATENTS:")
                for i, patent in enumerate(patents_data[:3]):
                    title = patent.get('patent_title', 'Unknown')
                    title_truncated = title[:60] + "..." if len(title) > 60 else title
                    print(f"   {i+1}. {patent.get('patent_number', 'Unknown')} - {title_truncated}")
                    
                    if patent.get('inventors'):
                        inventor_names = []
                        for inv in patent['inventors'][:2]:  # Show first 2 inventors
                            name = f"{inv.get('first_name', '')} {inv.get('last_name', '')}".strip()
                            if name:
                                inventor_names.append(name)
                        if inventor_names:
                            print(f"      👨‍🔬 Inventors: {', '.join(inventor_names)}")
                            
            except Exception as e:
                logger.warning(f"Could not read sample patents: {e}")
        
        # Stage 7: Cost analysis and file info
        write_progress_update("Computing costs", "Calculating API usage and cost estimates")
        api_requests = result.get('api_requests_made', 0)
        patents_count = result.get('patents_downloaded', 0)
        
        if api_requests > 0:
            print(f"\n💰 API USAGE:")
            print(f"   📊 Total API requests: {api_requests}")
            print(f"   ⚡ Average patents per request: {patents_count / api_requests:.1f}")
            print(f"   🕒 Rate: {patents_count / (elapsed_time / 60):.1f} patents/minute")
        
        print(f"\n📁 OUTPUT FILES:")
        output_files = result.get('output_files', {})
        if output_files.get('json'):
            print(f"   📋 Patents JSON: {output_files['json']}")
        if output_files.get('csv'):
            print(f"   📄 Patents CSV: {output_files['csv']}")
        print(f"   📊 Download results: {config['OUTPUT_DIR']}/download_results.json")
        
        # Final completion message
        write_progress_update("Complete", f"Successfully downloaded {patents_count} patents in {elapsed_time/60:.1f} minutes")
        print(f"\n🎉 STEP 0 DOWNLOAD COMPLETE!")
        logger.info("Step 0 wrapper completed successfully")
        
    except Exception as e:
        error_msg = f"Step 0 failed with error: {e}"
        logger.error(error_msg)
        write_progress_update("Error", error_msg)
        print(f"\n❌ STEP 0 FAILED: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)