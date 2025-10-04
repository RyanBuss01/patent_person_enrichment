#!/usr/bin/env python3
"""
Step 1 Wrapper: Enhanced with progress reporting for frontend polling
Provides real-time updates during long-running data integration process
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

# Add the project root to sys.path so we can import our modules
try:
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))
except Exception:
    pass

from runners.integrate_existing_data import run_existing_data_integration

# Load environment variables
load_dotenv()

# Ensure output directory exists for logs and artifacts
try:
    Path('output').mkdir(exist_ok=True)
except Exception:
    pass

# Configure logging with more detailed output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('output/step1_progress.log', mode='w')
    ]
)
logger = logging.getLogger(__name__)

def parse_cli_args():
    parser = argparse.ArgumentParser(description='Run Step 1 integration wrapper')
    parser.add_argument('--dev-mode', action='store_true', help='Enable dev mode filtering by issue date cutoff')
    parser.add_argument('--issue-date', dest='issue_date', help='Issue date cutoff (ISO datetime) for dev mode filtering')
    parser.add_argument('--skip-enrichment-filter', action='store_true', help='Skip filtering out already enriched people')
    return parser.parse_args()


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
        'DEDUP_NEW_PEOPLE': os.getenv('DEDUP_NEW_PEOPLE', 'true').lower() == 'true',
        'SKIP_ALREADY_ENRICHED_FILTER': os.getenv('SKIP_ALREADY_ENRICHED_FILTER', 'false').lower() == 'true',
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
        with open('output/step1_progress.json', 'w') as f:
            json.dump(progress_info, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not write progress file: {e}")

def _log_field_presence_step1_from_files():
    try:
        existing_file = Path('output') / 'existing_people_in_db.json'
        if not existing_file.exists():
            return
        with existing_file.open('r') as f:
            data = json.load(f)
        fields = ['patent_no','title','mail_to_add1','mail_to_zip','mod_user','inventor_id']
        stats = { 'total': len(data) }
        for field in fields:
            stats[field] = sum(1 for p in data if str(p.get(field, '')).strip() != '')
        print(f"STEP1 DIAG: existing_people_in_db fields -> {stats}")
        log_dir = Path('output') / 'logs'
        log_dir.mkdir(parents=True, exist_ok=True)
        with (log_dir / 'step1_field_presence.json').open('w') as out:
            json.dump({ 'stats': stats, 'generated_at': datetime.now().isoformat() }, out, indent=2)
    except Exception as e:
        logger.warning(f"Could not compute step1 field presence diagnostics: {e}")

def analyze_and_log_match_scores():
    """Fixed version: Analyze match scores from BOTH output files"""
    try:
        write_progress_update("Analyzing match scores", "Computing match statistics from all processed people")
        
        # Read BOTH files to get complete picture
        enrichment_file = 'output/new_people_for_enrichment.json'
        existing_file = 'output/existing_people_found.json'
        
        all_people = []
        
        # Load people who will be enriched (scores <25)
        if os.path.exists(enrichment_file):
            with open(enrichment_file, 'r') as f:
                enrichment_people = json.load(f)
                all_people.extend(enrichment_people)
                print(f"   📊 Loaded {len(enrichment_people):,} people from enrichment file")
        else:
            print(f"   ❓ No enrichment file found")
            enrichment_people = []
        
        # Load people who were flagged as existing (scores ≥25)  
        if os.path.exists(existing_file):
            with open(existing_file, 'r') as f:
                existing_people = json.load(f)
                all_people.extend(existing_people)
                print(f"   📊 Loaded {len(existing_people):,} people from existing file")
        else:
            print(f"   ❓ No existing people file found")
            existing_people = []
        
        if not all_people:
            print("   ❓ No people files found for score analysis")
            return
        
        print(f"   📊 Total people analyzed: {len(all_people):,}")
        
        # Count people in different score ranges
        score_ranges = {
            'no_score': 0,      # No matching attempted or score = 0
            '1-9': 0,          # Very low confidence matches  
            '10-19': 0,        # Low confidence - needs review
            '20-24': 0,        # Medium confidence - needs review
            '25-49': 0,        # High confidence matches - considered existing
            '50-74': 0,        # Very high confidence matches - considered existing
            '75-89': 0,        # Near certain matches - considered existing
            '90-100': 0,       # Exact matches - considered existing
        }
        
        needs_review_count = 0
        existing_count = 0
        
        for person in all_people:
            score = person.get('match_score', 0)
            match_status = person.get('match_status', '')
            
            if score == 0 or score is None:
                score_ranges['no_score'] += 1
            elif 1 <= score <= 9:
                score_ranges['1-9'] += 1
            elif 10 <= score <= 19:
                score_ranges['10-19'] += 1
                if match_status == 'needs_review':
                    needs_review_count += 1
            elif 20 <= score <= 24:
                score_ranges['20-24'] += 1
                if match_status == 'needs_review':
                    needs_review_count += 1
            elif 25 <= score <= 49:
                score_ranges['25-49'] += 1
                existing_count += 1
            elif 50 <= score <= 74:
                score_ranges['50-74'] += 1
                existing_count += 1
            elif 75 <= score <= 89:
                score_ranges['75-89'] += 1
                existing_count += 1
            elif 90 <= score <= 100:
                score_ranges['90-100'] += 1
                existing_count += 1
        
        print(f"\n🎯 COMPLETE MATCH SCORE BREAKDOWN:")
        print(f"   ❓ No Score/Score 0: {score_ranges['no_score']:,}")
        print(f"   📊 Score 1-9 (Very Low): {score_ranges['1-9']:,}")
        print(f"   🔍 Score 10-19 (Needs Review): {score_ranges['10-19']:,}")
        print(f"   🔍 Score 20-24 (Needs Review): {score_ranges['20-24']:,}")
        print(f"   ✅ Score 25-49 (High Conf): {score_ranges['25-49']:,}")
        print(f"   ✅ Score 50-74 (Very High): {score_ranges['50-74']:,}")
        print(f"   ✅ Score 75-89 (Near Certain): {score_ranges['75-89']:,}")
        print(f"   ✅ Score 90-100 (Exact): {score_ranges['90-100']:,}")
        
        print(f"\n📊 PROCESSING DECISIONS:")
        print(f"   🆕 Will be enriched: {len(enrichment_people):,}")
        print(f"   ✅ Flagged as existing: {existing_count:,}")
        print(f"   🔍 Need manual review: {needs_review_count:,}")
        
        if needs_review_count > 0:
            print(f"\n⚠️  MANUAL REVIEW NEEDED:")
            print(f"   🔍 {needs_review_count:,} potential matches need verification")
            print(f"   💡 Look for 'Review Potential Matches' button in Step 1")
            write_progress_update("Match analysis complete", f"{needs_review_count} matches need manual review")
        else:
            print(f"\n✅ NO MANUAL REVIEW NEEDED")
            print(f"   🎯 All matches have clear confidence scores")
            write_progress_update("Match analysis complete", "No manual review needed")
        
        # Verify our numbers add up
        total_analyzed = sum(score_ranges.values())
        print(f"\n🔢 VERIFICATION:")
        print(f"   Total analyzed: {total_analyzed:,}")
        print(f"   Should equal: {len(all_people):,}")
        print(f"   ✅ Match: {total_analyzed == len(all_people)}")
        
    except Exception as e:
        print(f"   ❌ Error analyzing match scores: {e}")
        write_progress_update("Match analysis error", f"Error: {e}")

def analyze_inventor_distribution():
    """Post-run diagnostics: inventor-per-patent distribution and duplicates"""
    try:
        write_progress_update("Post-diagnostics", "Computing inventor distribution and duplicates")
        patents_file = 'output/filtered_new_patents.json'
        people_file = 'output/new_people_for_enrichment.json'
        if not (os.path.exists(patents_file) and os.path.exists(people_file)):
            print("   ❓ Skipping diagnostics: output files not found")
            return
        import json
        with open(patents_file, 'r') as f:
            patents = json.load(f)
        with open(people_file, 'r') as f:
            people = json.load(f)
        # Inventors per patent distribution
        inv_counts = {}
        for p in patents:
            c = len([i for i in p.get('inventors', [])])
            inv_counts[c] = inv_counts.get(c, 0) + 1
        total_patents = len(patents)
        total_people = len(people)
        avg_inv = (total_people / total_patents) if total_patents else 0
        one_inv = inv_counts.get(1, 0)
        two_inv = inv_counts.get(2, 0)
        three_plus = sum(v for k, v in inv_counts.items() if k >= 3)
        # Duplicate people (within new list) by name+city+state
        def key(p):
            return (
                (p.get('first_name') or '').strip().lower(),
                (p.get('last_name') or '').strip().lower(),
                (p.get('city') or '').strip().lower(),
                (p.get('state') or '').strip().lower(),
            )
        seen = set()
        for p in people:
            seen.add(key(p))
        unique_people = len(seen)
        dups = total_people - unique_people
        print("\n📈 INVENTOR DISTRIBUTION (US patents only):")
        print(f"   Patents analyzed: {total_patents:,}")
        print(f"   Inventors found: {total_people:,}")
        print(f"   Avg inventors per patent: {avg_inv:.2f}")
        print(f"   Patents with 1 inventor: {one_inv:,}")
        print(f"   Patents with 2 inventors: {two_inv:,}")
        print(f"   Patents with 3+ inventors: {three_plus:,}")
        print("\n👥 NEW PEOPLE DEDUP (name+city+state):")
        print(f"   Unique people: {unique_people:,}")
        print(f"   Duplicate entries: {dups:,} (same person across multiple patents)")
    except Exception as e:
        print(f"   ❌ Error computing inventor distribution: {e}")

def print_filtering_summary(result):
    """Print US patent filtering summary"""
    us_filter = result.get('us_filter_result', {})
    if not us_filter:
        return
    
    print(f"📊 FILTERING SUMMARY:")
    print(f"   📋 Total XML patents processed: {result.get('original_patents_count', 0):,}")
    print(f"   🇺🇸 US patents kept: {result.get('us_patents_count', 0):,}")
    print(f"   🌍 Foreign patents filtered out: {result.get('foreign_patents_count', 0):,}")
    print(f"   📈 US retention rate: {us_filter.get('us_retention_rate', 'N/A')}")
    print()

def main():
    """Run Step 1 using existing runner with enhanced progress reporting"""
    print("🚀 STARTING STEP 1: INTEGRATE EXISTING DATA")
    print("=" * 60)
    
    args = parse_cli_args()

    # Load configuration (same as main.py)
    config = load_config()

    if args.skip_enrichment_filter:
        config['SKIP_ALREADY_ENRICHED_FILTER'] = True
        print("INTEGRATION ONLY: Skipping already-enriched filter to mirror legacy behavior")
    else:
        config['SKIP_ALREADY_ENRICHED_FILTER'] = bool(config.get('SKIP_ALREADY_ENRICHED_FILTER'))

    if args.dev_mode:
        config['DEV_MODE'] = True
        if args.issue_date:
            config['DEV_ISSUE_CUTOFF'] = args.issue_date
            print(f"DEV MODE: Filtering existing SQL people using issue date cutoff {args.issue_date}")
        else:
            print("DEV MODE: Enabled without explicit cutoff — defaulting to current timestamp")
            config['DEV_ISSUE_CUTOFF'] = datetime.now().isoformat(timespec='minutes')
    else:
        config['DEV_MODE'] = False

    # Create output directory
    os.makedirs(config['OUTPUT_DIR'], exist_ok=True)
    
    # Initialize progress tracking
    start_time = time.time()
    write_progress_update("Initializing", "Loading configuration and preparing directories")
    
    try:
        # Stage 1: Setup and initialization
        logger.info("Starting existing data integration...")
        write_progress_update("Starting integration", "Initializing data processing pipeline")
        print("⏳ Processing patents... this may take several minutes for large datasets")
        
        # Stage 2: Run the integration with progress monitoring
        logger.info("Integration process starting...")
        write_progress_update("Processing XML files", "Reading and parsing patent XML data")
        
        # This is where the long-running process happens
        result = run_existing_data_integration(config)
        
        # Stage 3: Check results
        if not result or not result.get('success'):
            error_msg = result.get('error') if result else 'No result returned'
            logger.error(f"Integration did not complete successfully: {error_msg}")
            write_progress_update("Integration failed", error_msg)
            print(f"\n❌ STEP 1 FAILED: {error_msg}")
            return 1
        
        # Stage 4: Save results
        write_progress_update("Saving results", "Writing integration results to output files")
        logger.info("Integration completed, saving results...")
        
        # Save results to JSON file for frontend
        results_file = os.path.join(config['OUTPUT_DIR'], 'integration_results.json')
        with open(results_file, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        
        # Stage 5: Generate summary and analysis
        write_progress_update("Generating summary", "Analyzing results and computing statistics")
        logger.info("Results saved, generating summary...")
        
        # Print summary exactly like main.py
        elapsed_time = time.time() - start_time
        print("\n✅ STEP 1 COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
        # NEW: Print filtering summary first
        print_filtering_summary(result)
        
        def safe_format_number(value, default=0):
            """Safely format a number with commas, handling string values"""
            try:
                return f"{int(value):,}"
            except (ValueError, TypeError):
                return str(value)

        print(f"📊 INTEGRATION SUMMARY:")
        print(f"   🗃️  Existing patents in DB: {safe_format_number(result.get('existing_patents_count', 0))}")
        print(f"   👥 Existing people in DB: {safe_format_number(result.get('existing_people_count', 0))}")
        print(f"   🆕 New patents found: {safe_format_number(result.get('new_patents_count', 0))}")
        print(f"   🆕 New people found: {safe_format_number(result.get('new_people_count', 0))}")

        if result.get('dedup_new_people_removed') is not None:
            print(f"   🔁 Duplicates removed (new people): {result.get('dedup_new_people_removed', 0):,}")
        print(f"   🔁 Duplicate patents avoided: {result.get('duplicate_patents_count', 0):,}")
        print(f"   🔁 Duplicate people avoided: {result.get('duplicate_people_count', 0):,}")
        print(f"   ⏱️  Total processing time: {elapsed_time/60:.1f} minutes")
        
        # Stage 6: Match score analysis
        analyze_and_log_match_scores()
        _log_field_presence_step1_from_files()
        analyze_inventor_distribution()
        
        # Stage 7: Cost analysis
        write_progress_update("Computing cost savings", "Calculating API cost savings from duplicate detection")
        total_xml_people = result.get('total_xml_people', 0)
        new_people = result.get('new_people_count', 0)
        if total_xml_people > 0:
            saved_api_calls = total_xml_people - new_people
            estimated_savings = saved_api_calls * 0.1
            print(f"\n💰 COST SAVINGS:")
            print(f"   📉 API calls avoided: {saved_api_calls:,}")
            print(f"   💵 Estimated cost saved: ${estimated_savings:.2f}")
            print(f"   💸 Cost for new people: ${new_people * 0.1:.2f}")
        
        print(f"\n📁 OUTPUT FILES:")
        if result.get('new_patents_count', 0) > 0:
            print(f"   📋 New patents: output/filtered_new_patents.json")
            print(f"   👥 New people: output/new_people_for_enrichment.json")
        print(f"   📊 Integration results: output/integration_results.json")
        
        # Final completion message
        write_progress_update("Complete", f"Successfully processed {result.get('new_patents_count', 0)} new patents and {result.get('new_people_count', 0)} new people in {elapsed_time/60:.1f} minutes")
        print(f"\n🎉 STEP 1 PROCESSING COMPLETE!")
        logger.info("Step 1 wrapper completed successfully")
        
    except Exception as e:
        error_msg = f"Step 1 failed with error: {e}"
        logger.error(error_msg)
        write_progress_update("Error", error_msg)
        print(f"\n❌ STEP 1 FAILED: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
