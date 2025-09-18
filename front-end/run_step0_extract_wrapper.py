#!/usr/bin/env python3
"""
Alternate Step 0 Wrapper: Run legacy extract_patents runner
Emits progress output compatible with frontend polling and writes standardized
output files (downloaded_patents.json, download_results.json).
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

# Ensure project root import
sys.path.append(str(Path(__file__).parent.parent))

from runners.extract_patents import run_patent_extraction

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('output/step0_extract_progress.log', mode='w')
    ]
)
logger = logging.getLogger(__name__)


def load_config():
    return {
        'PATENTSVIEW_API_KEY': os.getenv('PATENTSVIEW_API_KEY', ''),
        'OUTPUT_DIR': os.getenv('OUTPUT_DIR', 'output'),
        'MAX_RESULTS': int(os.getenv('MAX_RESULTS', '1000')),
        'DAYS_BACK': int(os.getenv('DAYS_BACK', '7'))
    }


def progress(stage, details=""):
    msg = f"PROGRESS: {stage}{(' - ' + details) if details else ''}"
    print(msg)
    sys.stdout.flush()
    try:
        with open('output/step0_extract_progress.json', 'w') as f:
            json.dump({'timestamp': datetime.now().isoformat(), 'stage': stage, 'details': details}, f, indent=2)
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser(description='Alternate Step 0 extractor')
    parser.add_argument('--days-back', type=int, default=7)
    parser.add_argument('--max-results', type=int, default=1000)
    args = parser.parse_args()

    cfg = load_config()
    cfg.update({
        'DAYS_BACK': args.days_back,
        'MAX_RESULTS': args.max_results
    })

    os.makedirs(cfg['OUTPUT_DIR'], exist_ok=True)
    start = time.time()
    print("🚀 STARTING STEP 0 (Alternate): Extract Patents")
    progress('Initializing', 'Preparing configuration')

    try:
        progress('Extracting', f"days_back={cfg['DAYS_BACK']}, max_results={cfg['MAX_RESULTS']}")
        result = run_patent_extraction(cfg)
        if not result or not result.get('success'):
            err = (result or {}).get('error', 'unknown error')
            progress('Failed', err)
            print(f"\n❌ STEP 0 (Alternate) FAILED: {err}")
            return 1

        elapsed = time.time() - start
        count = result.get('total_patents', 0)
        progress('Complete', f"Extracted {count} patents in {elapsed/60:.1f} minutes")

        print("\n✅ STEP 0 (Alternate) COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("📊 SUMMARY:")
        print(f"   📋 Patents extracted: {count:,}")
        print(f"   ⏱️  Time: {elapsed/60:.1f} minutes")
        ofiles = result.get('output_files') or []
        if ofiles:
            for p in ofiles:
                print(f"   📁 {p}")
        return 0

    except Exception as e:
        logger.exception("Alternate Step 0 failed")
        progress('Error', str(e))
        print(f"\n❌ STEP 0 (Alternate) FAILED: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())

