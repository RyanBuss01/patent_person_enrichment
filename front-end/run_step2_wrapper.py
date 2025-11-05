#!/usr/bin/env python3
"""
Step 2 Wrapper: Data Enrichment
Routes to either PDL or ZabaSearch enrichment, then generates CSVs
"""
import sys
import os
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv

# Add the parent directory to sys.path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from runners.run_pdl_enrich import run_pdl_enrichment
from runners.run_zaba_enrich import run_zaba_enrichment
from runners.csv_builder import generate_all_csvs, generate_all_and_current_csvs

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def _dev_enrich_all_usa_requested() -> bool:
    """Detect whether the last Step 1 run was the Dev 'Enrich All USA' shortcut."""
    integration_path = Path('output/integration_results.json')
    message_markers: List[str] = []

    try:
        text = Path('output/last_step1_output.txt').read_text(encoding='utf-8', errors='ignore')
        message_markers.append(text)
    except FileNotFoundError:
        message_markers.append('')
    except Exception:
        message_markers.append('')

    integration_data: Dict[str, Any] = {}
    try:
        with integration_path.open('r', encoding='utf-8') as f:
            integration_data = json.load(f)
    except FileNotFoundError:
        integration_data = {}
    except Exception:
        logger.debug("Could not parse integration_results.json for dev detection", exc_info=True)
        integration_data = {}

    mode = str(integration_data.get('mode') or '').lower()
    summary = str(integration_data.get('message') or '')
    if mode == 'dev_enrich_all_usa' or integration_data.get('dev_enrich_all_usa'):
        return True

    combined_messages = ' '.join(m for m in message_markers if m)
    if 'dev enrich all usa' in combined_messages.lower():
        return True
    if 'dev enrich all usa' in summary.lower():
        return True

    return False


def _is_usa_country(raw: Any) -> bool:
    """Case-insensitive country helper that mirrors the Step 1 JS logic."""
    if not raw:
        return False
    s = str(raw).strip().upper()
    s = re.sub(r'[\\.,]', '', s)
    if not s:
        return False
    if s in {'US', 'USA', 'UNITED STATES', 'UNITED STATES OF AMERICA'}:
        return True
    compact = re.sub(r'\\s+', '', s)
    if compact in {'US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA'}:
        return True
    return 'UNITED STATES' in s


def _normalize_person(source: Dict[str, Any], type_: str, patent_number: str,
                      patent_title: str, patent_date: str, counter: List[int]) -> Dict[str, Any]:
    """Normalize inventor/assignee payload similar to the Dev Step 1 route."""
    if not source:
        return {}

    first = str(source.get('first_name') or source.get('firstName') or
                source.get('inventor_name_first') or source.get('name_first') or
                source.get('given_name') or '').strip()
    last = str(source.get('last_name') or source.get('lastName') or
               source.get('inventor_name_last') or source.get('name_last') or
               source.get('surname') or '').strip()
    organization = str(source.get('organization') or source.get('org_name') or
                       source.get('assignee_organization') or source.get('company') or
                       source.get('name') or '').strip()

    if not first and not last and organization and type_ == 'assignee':
        last = organization

    if not first and not last and not organization:
        return {}

    city = str(source.get('city') or source.get('city_name') or source.get('city_or_town') or '').strip()
    state = str(source.get('state') or source.get('state_code') or source.get('state_abbr') or '').strip()
    country = str(source.get('country') or source.get('country_code') or '').strip()
    address = str(source.get('address') or source.get('mail_to_add1') or
                  source.get('address1') or source.get('street') or '').strip()
    postal = str(source.get('zip') or source.get('postal_code') or source.get('mail_to_zip') or '').strip()

    if not _is_usa_country(country):
        return {}

    record = {
        'first_name': first,
        'last_name': last,
        'city': city,
        'state': state,
        'country': country,
        'address': address or None,
        'postal_code': postal or None,
        'patent_number': patent_number,
        'patent_title': patent_title,
        'patent_date': patent_date,
        'person_type': type_,
        'person_id': f"{patent_number or 'unknown'}_{type_}_{counter[0]}",
        'match_status': 'dev_enrich_all_usa',
        'match_score': 0,
        'associated_patents': [patent_number] if patent_number else [],
        'associated_patent_count': 1 if patent_number else 0,
        'dev_enrich_all_usa': True,
        'verification_needed': False,
        'potential_matches': []
    }

    if organization:
        record['organization'] = organization

    for field in ('email', 'phone', 'raw_name'):
        if source.get(field):
            record[field] = source[field]

    counter[0] += 1
    return record


def _rebuild_dev_enrich_all_usa_people(existing_count: int = 0) -> List[Dict[str, Any]]:
    """Fallback: rebuild Dev Enrich All USA payload if Step 1 JSON is missing/empty."""
    if not _dev_enrich_all_usa_requested():
        return []

    downloaded_path = Path('output/downloaded_patents.json')
    if not downloaded_path.exists():
        logger.warning("Dev Enrich All USA fallback requested but downloaded_patents.json is missing")
        return []

    try:
        with downloaded_path.open('r', encoding='utf-8') as f:
            downloaded_patents = json.load(f)
    except Exception as exc:
        logger.error(f"Failed to parse downloaded_patents.json for Dev Enrich All USA fallback: {exc}")
        return []

    if not isinstance(downloaded_patents, list):
        logger.warning("Dev Enrich All USA fallback aborted: downloaded_patents.json is not a list")
        return []

    new_people: List[Dict[str, Any]] = []
    new_patents: List[Dict[str, Any]] = []
    counter = [0]

    for index, raw_patent in enumerate(downloaded_patents):
        if not isinstance(raw_patent, dict):
            continue

        patent_number = str(
            raw_patent.get('patent_number') or raw_patent.get('patentNumber') or
            raw_patent.get('patent_id') or raw_patent.get('number') or
            f'unknown_{index}'
        ).strip()
        patent_title = str(
            raw_patent.get('patent_title') or raw_patent.get('title') or
            raw_patent.get('patentTitle') or ''
        )
        patent_date = str(
            raw_patent.get('patent_date') or raw_patent.get('issue_date') or
            raw_patent.get('date') or ''
        )

        inventors = raw_patent.get('inventors') or raw_patent.get('inventor_list') or []
        assignees = raw_patent.get('assignees') or raw_patent.get('assignee_list') or []

        people_for_patent: List[Dict[str, Any]] = []

        for inventor in inventors if isinstance(inventors, list) else []:
            normalized = _normalize_person(
                inventor, 'inventor', patent_number, patent_title, patent_date, counter
            )
            if normalized:
                people_for_patent.append(normalized)

        for assignee in assignees if isinstance(assignees, list) else []:
            normalized = _normalize_person(
                assignee, 'assignee', patent_number, patent_title, patent_date, counter
            )
            if normalized:
                people_for_patent.append(normalized)

        if people_for_patent:
            new_patents.append({
                'patent_number': patent_number,
                'patent_title': patent_title,
                'patent_date': patent_date,
                'inventors': inventors if isinstance(inventors, list) else [],
                'assignees': assignees if isinstance(assignees, list) else []
            })
            new_people.extend(people_for_patent)

    if not new_people:
        logger.warning("Dev Enrich All USA fallback produced 0 people – skipping overwrite")
        return []

    if existing_count and len(new_people) <= existing_count:
        # Nothing gained by overwriting with the same or smaller dataset
        return []

    output_dir = Path('output')
    output_dir.mkdir(parents=True, exist_ok=True)

    people_path = output_dir / 'new_people_for_enrichment.json'
    patents_path = output_dir / 'filtered_new_patents.json'
    existing_people_path = output_dir / 'existing_people_found.json'
    moved_path = output_dir / 'same_name_diff_address.json'
    integration_path = output_dir / 'integration_results.json'

    with people_path.open('w', encoding='utf-8') as f:
        json.dump(new_people, f, indent=2)

    with patents_path.open('w', encoding='utf-8') as f:
        json.dump(new_patents, f, indent=2)

    # Reset verification-related files for dev runs
    with existing_people_path.open('w', encoding='utf-8') as f:
        json.dump([], f)
    with moved_path.open('w', encoding='utf-8') as f:
        json.dump([], f)

    integration_data: Dict[str, Any] = {}
    try:
        if integration_path.exists():
            with integration_path.open('r', encoding='utf-8') as f:
                parsed = json.load(f)
                if isinstance(parsed, dict):
                    integration_data = parsed
    except Exception:
        logger.debug("Could not read existing integration_results.json during fallback", exc_info=True)
        integration_data = {}

    integration_data.update({
        'success': True,
        'mode': 'dev_enrich_all_usa',
        'dev_enrich_all_usa': True,
        'message': (
            f"Dev Enrich All USA reconstructed: queued {len(new_people):,} people "
            f"from {len(new_patents):,} USA patents."
        ),
        'new_people_count': len(new_people),
        'new_patents_count': len(new_patents),
        'verification_completed': True,
        'processed_at': datetime.utcnow().isoformat()
    })

    with integration_path.open('w', encoding='utf-8') as f:
        json.dump(integration_data, f, indent=2, default=str)

    logger.info(
        "Rebuilt Dev Enrich All USA dataset with %s people and %s patents",
        len(new_people), len(new_patents)
    )
    print(
        f"STEP 2: Reconstructed {len(new_people):,} people and {len(new_patents):,} patents "
        "from Dev Enrich All USA selection"
    )
    print(f"STEP 2: Loaded {len(new_people):,} people from Step 1 (Dev Enrich All USA fallback)")

    return new_people

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

    people_data: List[Dict[str, Any]] = []
    if os.path.exists(step1_people_file):
        try:
            with open(step1_people_file, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
            if isinstance(loaded, list):
                people_data = loaded
        except Exception as exc:
            logger.error(f"Error loading people from Step 1: {exc}")
            people_data = []

    if people_data:
        logger.info(f"Loaded {len(people_data)} people from Step 1 results")
        print(f"STEP 2: Loaded {len(people_data)} people from Step 1")
        return people_data

    rebuilt = _rebuild_dev_enrich_all_usa_people(existing_count=len(people_data))
    if rebuilt:
        return rebuilt

    if os.path.exists(step1_people_file):
        logger.warning("Step 2 found new_people_for_enrichment.json but it is empty")
    else:
        logger.warning("No Step 1 people data found")

    return people_data

def main():
    """Run Step 2: Data Enrichment"""
    # Parse command line arguments
    test_mode = os.getenv('STEP2_TEST_MODE', '').lower() == 'true' or ('--test' in sys.argv)
    express_mode = os.getenv('STEP2_EXPRESS_MODE', '').lower() == 'true' or ('--express' in sys.argv)
    rebuild_only = ('--rebuild' in sys.argv)
    use_zaba = ('--zaba' in sys.argv)
    generate_all_current_only = ('--generate-all-current' in sys.argv)

    method_name = "ZabaSearch Web Scraping" if use_zaba else "PeopleDataLabs API"

    print("🚀 STARTING STEP 2: DATA ENRICHMENT" +
          f" ({method_name})" +
          (" (TEST MODE)" if test_mode else "") +
          (" [EXPRESS]" if express_mode else "") +
          (" [REBUILD ONLY]" if rebuild_only else "") +
          (" [ALL & CURRENT ONLY]" if generate_all_current_only else ""))
    print("=" * 60)

    config = load_config(test_mode, express_mode, use_zaba)
    # Expose express flag to downstream helpers (CSV builder) via env
    os.environ['STEP2_EXPRESS_MODE'] = 'true' if express_mode else 'false'
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

    if generate_all_current_only:
        try:
            print("🔄 Generating 'all' and 'current' base CSVs only (this may take several minutes)...")
            csv_result = generate_all_and_current_csvs(config)

            if csv_result.get('success'):
                print(f"\n✅ ALL & CURRENT CSV GENERATION COMPLETED SUCCESSFULLY!")
                print("=" * 60)
                print("📁 OUTPUT FILES:")
                for file_path, stats in csv_result.get('files_generated', {}).items():
                    if os.path.exists(file_path):
                        file_size = os.path.getsize(file_path) / 1024
                        records = stats.get('records_written', 0)
                        print(f"   📄 {file_path} ({file_size:.1f} KB, {records:,} records)")
                return 0
            else:
                print(f"\n❌ ALL & CURRENT CSV GENERATION FAILED: {csv_result.get('error')}")
                return 1

        except Exception as e:
            logger.error(f"All & Current CSV generation failed: {e}")
            print(f"\n❌ ALL & CURRENT CSV GENERATION FAILED: {e}")
            return 1

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
