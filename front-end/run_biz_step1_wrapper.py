#!/usr/bin/env python3
"""
Business Step 1 Wrapper: Trademark XML Upload & Extract
Parses uploaded USPTO Trademark Assignment XML, extracts assignee (business) data,
and filters to US addresses.
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
        logging.FileHandler(os.path.join(BIZ_OUTPUT_DIR, 'biz_step1_progress.log'), mode='w')
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
        with open(os.path.join(BIZ_OUTPUT_DIR, 'biz_step1_progress.json'), 'w') as f:
            json.dump(progress_info, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not write progress file: {e}")



def main():
    """Run Business Step 1: Upload & Extract Trademarks"""
    parser = argparse.ArgumentParser(description='Extract trademark assignee data from USPTO XML')
    parser.add_argument('--mode', choices=['upload', 'download'], default='upload',
                        help='Mode: upload (process existing XML) or download (fetch from USPTO)')
    parser.add_argument('--xml-path', type=str, default=None,
                        help='Path to uploaded XML file (required for upload mode)')
    parser.add_argument('--days-back', type=int, default=7,
                        help='Number of days back to download (download mode only)')

    args = parser.parse_args()

    print("STARTING BUSINESS STEP 1: UPLOAD & EXTRACT TRADEMARKS")
    print("=" * 60)

    start_time = time.time()
    write_progress_update("Initializing", "Loading configuration")

    try:
        if args.mode == 'download':
            # Auto-download mode: fetch XMLs from USPTO then process
            write_progress_update("Downloading", f"Fetching trademark XMLs from USPTO ({args.days_back} days back)")
            print(f"Auto-download mode: fetching last {args.days_back} days from USPTO...")

            from runners.download_trademarks import run_trademark_download
            download_config = {
                'OUTPUT_DIR': BIZ_OUTPUT_DIR,
                'days_back': args.days_back,
                'max_files': args.days_back,
            }
            download_result = run_trademark_download(download_config)

            if not download_result.get('success'):
                error_msg = download_result.get('error', 'Download failed')
                write_progress_update("Error", error_msg)
                print(f"\nBUSINESS STEP 1 FAILED: {error_msg}")
                return 1

            xml_path = download_result['output_files']['xml']
            print(f"Downloaded {download_result['trademarks_downloaded']} files, combined XML at: {xml_path}")
        else:
            # Upload mode: use provided XML path
            xml_path = args.xml_path
            if not xml_path or not os.path.exists(xml_path):
                error_msg = f"XML file not found: {xml_path}"
                write_progress_update("Error", error_msg)
                print(f"\nBUSINESS STEP 1 FAILED: {error_msg}")
                return 1
            print(f"Using uploaded XML file: {xml_path}")

        # Parse the XML
        write_progress_update("Parsing XML", "Extracting trademark assignee data from XML")
        print("Parsing trademark assignment XML data...")

        from classes.trademark_xml_parser import TrademarkXMLParser
        tm_parser = TrademarkXMLParser(xml_path)
        trademarks = tm_parser.parse_xml_file()

        if not trademarks:
            write_progress_update("Error", "No trademark assignee records found in XML")
            print("\nBUSINESS STEP 1 FAILED: No trademark assignee records found in XML file")
            return 1

        print(f"Found {len(trademarks)} total assignee records")

        # Filter US only
        write_progress_update("Filtering", "Removing non-US addresses")
        us_trademarks = tm_parser.filter_us_only(trademarks)
        foreign_count = len(trademarks) - len(us_trademarks)
        print(f"US addresses: {len(us_trademarks)} (filtered {foreign_count} foreign)")

        # Deduplicate within this batch (same name appearing multiple times)
        write_progress_update("Deduplicating", "Removing duplicate names within batch")
        unique_trademarks = tm_parser.deduplicate(us_trademarks)
        batch_dup_count = len(us_trademarks) - len(unique_trademarks)
        print(f"Unique trademarks: {len(unique_trademarks)} (removed {batch_dup_count} batch duplicates)")

        # Save results
        write_progress_update("Saving results", "Writing extracted trademark data")

        # Save extracted trademarks JSON
        trademarks_json_path = os.path.join(BIZ_OUTPUT_DIR, 'extracted_trademarks.json')
        with open(trademarks_json_path, 'w') as f:
            json.dump(unique_trademarks, f, indent=2)

        # Save extracted trademarks CSV
        trademarks_csv_path = os.path.join(BIZ_OUTPUT_DIR, 'extracted_trademarks.csv')
        if unique_trademarks:
            import csv
            with open(trademarks_csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=unique_trademarks[0].keys())
                writer.writeheader()
                writer.writerows(unique_trademarks)

        # Generate summary
        elapsed_time = time.time() - start_time

        print("\nBUSINESS STEP 1 COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"EXTRACTION SUMMARY:")
        print(f"   Total records parsed: {len(trademarks):,}")
        print(f"   US addresses kept: {len(us_trademarks):,}")
        print(f"   Foreign filtered out: {foreign_count:,}")
        print(f"   Batch duplicates removed: {batch_dup_count:,}")
        print(f"   Final trademarks: {len(unique_trademarks):,}")
        print(f"   Total time: {elapsed_time:.1f}s")

        if unique_trademarks:
            print(f"\nSAMPLE TRADEMARKS:")
            for i, tm in enumerate(unique_trademarks[:3]):
                print(f"   {i+1}. #{tm.get('trademark_number', 'N/A')} - {tm.get('contact_name', 'N/A')}")
                print(f"      {tm.get('city', '')}, {tm.get('state', '')} {tm.get('zip_code', '')}")
                if tm.get('legal_entity_type'):
                    print(f"      Entity: {tm.get('legal_entity_type', '')}")

        print(f"\nOUTPUT FILES:")
        print(f"   {trademarks_json_path}")
        print(f"   {trademarks_csv_path}")

        write_progress_update("Complete",
                              f"Extracted {len(unique_trademarks)} US trademarks in {elapsed_time:.1f}s")
        print(f"\nBUSINESS STEP 1 COMPLETE!")
        logger.info("Business Step 1 wrapper completed successfully")

    except Exception as e:
        error_msg = f"Business Step 1 failed with error: {e}"
        logger.error(error_msg)
        write_progress_update("Error", error_msg)
        print(f"\nBUSINESS STEP 1 FAILED: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
