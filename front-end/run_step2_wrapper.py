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
from runners.enrich import load_existing_enriched_people
from database.db_manager import DatabaseManager, DatabaseConfig
from database.db_manager import DatabaseManager, DatabaseConfig

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def _person_signature_from_enriched(item: dict) -> str:
    try:
        ed = item.get('enriched_data') or (item.get('enrichment_result', {}).get('enriched_data') if isinstance(item.get('enrichment_result'), dict) else {}) or {}
        original = ed.get('original_data') or ed.get('original_person') or item.get('original_person') or {}
        first = str(original.get('first_name', '')).strip().lower()
        last = str(original.get('last_name', '')).strip().lower()
        city = str(original.get('city', '')).strip().lower()
        state = str(original.get('state', '')).strip().lower()
        patent = str(original.get('patent_number', item.get('patent_number', ''))).strip()
        return f"{first}_{last}_{city}_{state}_{patent}"
    except Exception:
        return ''

def _log_field_backfill_presence(enriched_list):
    try:
        total = len(enriched_list or [])
        have_existing = 0
        fields = ['patent_no','mail_to_add1','mail_to_zip','title','inventor_id','mod_user']
        counts = { f: 0 for f in fields }
        for it in (enriched_list or []):
            ed = it.get('enriched_data') or {}
            ex = ed.get('existing_record') or {}
            if ex:
                have_existing += 1
            for f in fields:
                val = it.get(f) or ex.get(f)
                if str(val or '').strip() != '':
                    counts[f] += 1
        print(f"STEP2 DIAG: enriched snapshot existing_record presence: {have_existing}/{total} rows carry backfill")
        print(f"STEP2 DIAG: field availability counts: {counts}")
        out_dir = Path('output') / 'logs'
        out_dir.mkdir(parents=True, exist_ok=True)
        with (out_dir / 'step2_field_backfill_presence.json').open('w') as f:
            json.dump({ 'total': total, 'have_existing_record': have_existing, 'counts': counts, 'generated_at': datetime.now().isoformat() }, f, indent=2)
    except Exception as e:
        print(f"STEP2 DIAG: failed to write field backfill diagnostics: {e}")

def load_config(test_mode=False, express_mode=False):
    """Load enrichment configuration like main.py"""
    return {
        'PEOPLEDATALABS_API_KEY': os.getenv('PEOPLEDATALABS_API_KEY', "YOUR_PDL_API_KEY"),
        'XML_FILE_PATH': "ipg250812.xml",
        'OUTPUT_DIR': os.getenv('OUTPUT_DIR', 'output'),
        'OUTPUT_CSV': "output/enriched_patents.csv",
        'OUTPUT_JSON': "output/enriched_patents.json",
        'ENRICH_ONLY_NEW_PEOPLE': os.getenv('ENRICH_ONLY_NEW_PEOPLE', 'true').lower() == 'true',
        # Allow up to 10 test enrichments (approximate $1 if $0.10/call)
        'MAX_ENRICHMENT_COST': 10 if test_mode else int(os.getenv('MAX_ENRICHMENT_COST', '1000')),
        'TEST_MODE': test_mode,
        'EXPRESS_MODE': express_mode
    }

def load_existing_enrichment():
    """Load existing enrichment data to avoid duplicates (JSON + DB fallback)."""
    enriched_file = 'output/enriched_patents.json'
    enriched_people = set()
    enriched_data = []

    # 1) Load from local snapshot if present
    if os.path.exists(enriched_file) and os.path.getsize(enriched_file) > 0:
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
                    enriched_people.add(f"{first_name}_{last_name}_{city}_{state}_{patent_number}")
            logger.info(f"Found {len(enriched_people)} already enriched people from JSON snapshot")
        except Exception as e:
            logger.warning(f"Error loading existing enrichment JSON: {e}")

    # 2) Also load signatures from SQL (authoritative), in case JSON is missing/outdated
    try:
        db = DatabaseManager(DatabaseConfig.from_env())
        rows = db.execute_query("SELECT first_name, last_name, city, state, patent_number FROM enriched_people") or []
        added = 0
        for r in rows:
            fn = (r.get('first_name') or '').strip().lower()
            ln = (r.get('last_name') or '').strip().lower()
            ct = (r.get('city') or '').strip().lower()
            st = (r.get('state') or '').strip().lower()
            pn = (r.get('patent_number') or '').strip()
            if fn or ln:
                sig = f"{fn}_{ln}_{ct}_{st}_{pn}"
                if sig not in enriched_people:
                    enriched_people.add(sig)
                    added += 1
        if added:
            logger.info(f"Augmented with {added} signatures from DB (enriched_people)")
    except Exception as e:
        logger.warning(f"Could not load signatures from DB: {e}")

    return enriched_people, enriched_data

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
            loaded_count = len(people_data)
            logger.info(f"Loaded {loaded_count} people from Step 1 results")
            print(f"STEP 2: Loaded {loaded_count} people from Step 1")
            already_enriched, existing_data = load_existing_enrichment()
            filtered_people = filter_already_enriched_people(people_data, already_enriched)
            remaining = len(filtered_people)
            removed_as_already = loaded_count - remaining
            # Persist pre-filter count on config for later summary output
            try:
                config['PRE_FILTER_ALREADY_ENRICHED_COUNT'] = int(removed_as_already)
            except Exception:
                pass
            # Log explicit count of already-enriched filtered out
            logger.info(f"Filtered out {removed_as_already} as already enriched; {remaining} remaining for this run")
            print(f"STEP 2: Filtered out {removed_as_already} already enriched; {remaining} remaining")
            return filtered_people, existing_data
        except Exception as e:
            logger.error(f"Error loading people from Step 1: {e}")
    logger.warning("No Step 1 people data found, will fall back to XML parsing")
    return [], []

def _augment_formatted_fields(records):
    """Ensure patent_no, title, mail_to_add1, mail_to_zip are present on each record.
    Pull from enrichment JSON original data, then from SQL existing_people(_new) for address/zip/title if missing.
    """
    try:
        db = DatabaseManager(DatabaseConfig.from_env())
    except Exception:
        db = None
    cache = {}
    # Discover available columns once per table to avoid selecting non-existent columns
    cols_existing = []
    cols_new = []
    if db:
        try:
            rows = db.execute_query('SHOW COLUMNS FROM existing_people') or []
            cols_existing = [r.get('Field') or r.get('COLUMN_NAME') or r.get('field') for r in rows if isinstance(r, dict)]
        except Exception:
            cols_existing = []
        try:
            rows2 = db.execute_query('SHOW COLUMNS FROM existing_people_new') or []
            cols_new = [r.get('Field') or r.get('COLUMN_NAME') or r.get('field') for r in rows2 if isinstance(r, dict)]
        except Exception:
            cols_new = []
    def pick(cols, want, alts=None):
        alts = alts or []
        if want in cols:
            return want
        for a in alts:
            if a in cols:
                return a
        return None
    def build_select(cols):
        mapping = {
            'inventor_id': pick(cols, 'inventor_id'),
            'mod_user': pick(cols, 'mod_user', ['modified_by','last_modified_by','user']),
            'title': pick(cols, 'title', ['patent_title','invention_title']),
            'patent_no': pick(cols, 'patent_no', ['patent_number','patent_num']),
            'mail_to_add1': pick(cols, 'mail_to_add1', ['address','addr1','mail_to_add_1']),
            'mail_to_zip': pick(cols, 'mail_to_zip', ['zip','postal_code'])
        }
        parts = []
        for alias, col in mapping.items():
            if not col:
                continue
            parts.append(col if col == alias else f"{col} AS {alias}")
        # Prefer rows that have values — build ORDER BY dynamically
        order = []
        for k in ['patent_no','title','mail_to_add1','mail_to_zip']:
            col = mapping.get(k)
            if col:
                order.append(f"({col} IS NOT NULL AND {col} <> '') DESC")
        order_clause = (" ORDER BY " + ", ".join(order)) if order else ''
        return { 'select': ', '.join(parts), 'order': order_clause }
    def key(first,last,city,state):
        return (str(first or '').strip().lower(), str(last or '').strip().lower(), str(city or '').strip().lower(), str(state or '').strip().lower())
    def lookup_sql(first,last,city,state):
        if not db:
            return {}
        k = key(first,last,city,state)
        if k in cache:
            return cache[k]
        extra = {}
        try:
            sel = build_select(cols_existing)
            if sel['select']:
                q = (
                    f"SELECT {sel['select']} FROM existing_people "
                    "WHERE first_name=%s AND last_name=%s AND IFNULL(city,'')=%s AND IFNULL(state,'')=%s"
                    f"{sel['order']} LIMIT 1"
                )
                rows = db.execute_query(q, (first,last,city or '',state or ''))
            else:
                rows = []
            if not rows:
                sel2 = build_select(cols_new)
                if sel2['select']:
                    q2 = (
                        f"SELECT {sel2['select']} FROM existing_people_new "
                        "WHERE first_name=%s AND last_name=%s AND IFNULL(city,'')=%s AND IFNULL(state,'')=%s"
                        f"{sel2['order']} LIMIT 1"
                    )
                    rows = db.execute_query(q2, (first,last,city or '',state or ''))
            if rows:
                r = rows[0]
                # Normalize address/zip
                extra = {
                    'patent_no': (r.get('patent_no') if isinstance(r, dict) else None) or '',
                    'title': (r.get('title') if isinstance(r, dict) else None) or '',
                    'mail_to_add1': ((r.get('mail_to_add1') if isinstance(r, dict) else None) or (r.get('address') if isinstance(r, dict) else None) or ''),
                    'mail_to_zip': ((r.get('mail_to_zip') if isinstance(r, dict) else None) or (r.get('zip') if isinstance(r, dict) else None) or ''),
                    'inventor_id': (r.get('inventor_id') if isinstance(r, dict) else None) or '',
                    'mod_user': (r.get('mod_user') if isinstance(r, dict) else None) or ''
                }
        except Exception:
            extra = {}
        cache[k] = extra
        return extra

    for it in records or []:
        ed = it.get('enriched_data') or {}
        original = ed.get('original_data') or ed.get('original_person') or {}
        # Base patent fields from original
        it['patent_no'] = it.get('patent_no') or original.get('patent_number') or it.get('patent_number') or ''
        it['title'] = it.get('title') or it.get('patent_title') or original.get('patent_title') or ''
        # Use existing_record if present
        exrec = ed.get('existing_record') or {}
        mail1 = it.get('mail_to_add1') or exrec.get('mail_to_add1')
        zipc = it.get('mail_to_zip') or exrec.get('mail_to_zip')
        if not (mail1 and str(mail1).strip()) or not (zipc and str(zipc).strip()) or not it.get('title'):
            # Lookup from SQL if needed
            first = original.get('first_name') or it.get('first_name')
            last = original.get('last_name') or it.get('last_name')
            city = (original.get('city') or it.get('city') or '').strip()
            state = (original.get('state') or it.get('state') or '').strip()
            extra = lookup_sql(first,last,city,state)
            if extra:
                it['mail_to_add1'] = it.get('mail_to_add1') or extra.get('mail_to_add1')
                it['mail_to_zip'] = it.get('mail_to_zip') or extra.get('mail_to_zip')
                it['title'] = it.get('title') or extra.get('title')
                it['patent_no'] = it.get('patent_no') or extra.get('patent_no')
                # Also embed back into JSON for persistence
                exrec.update({k:v for k,v in extra.items() if v})
                ed['existing_record'] = exrec
                it['enriched_data'] = ed
        # If title still empty but we have a patent number, try downloaded_patents table
        if (not it.get('title')) and (it.get('patent_no') or it.get('patent_number')) and db:
            try:
                pn = (it.get('patent_no') or it.get('patent_number') or '').strip()
                row = db.execute_query(
                    "SELECT patent_title FROM downloaded_patents WHERE patent_number=%s LIMIT 1",
                    (pn,), fetch_one=True
                )
                if row and (row.get('patent_title') or '').strip():
                    it['title'] = row.get('patent_title')
            except Exception:
                pass
    return records

def _read_json_safe(path_str, default=None, label='file'):
    """Read JSON safely. If the file is corrupt, log and return default instead of failing.
    Also preserves the corrupt file by renaming it with a .bad suffix so it doesn't break future runs.
    """
    try:
        p = Path(path_str)
        if not p.exists():
            return default
        with p.open('r') as f:
            return json.load(f)
    except Exception as e:
        print(f"WARNING: Could not read {label} at {path_str}: {e}")
        try:
            bad = p.with_suffix(p.suffix + '.bad')
            p.rename(bad)
            print(f"INFO: Renamed corrupt {label} to {bad}")
        except Exception:
            pass
        return default

def main():
    """Run Step 2: Data Enrichment"""
    # Support test mode via env or CLI flag (compat)
    test_mode = os.getenv('STEP2_TEST_MODE', '').lower() == 'true' or ('--test' in sys.argv)
    express_mode = os.getenv('STEP2_EXPRESS_MODE', '').lower() == 'true' or ('--express' in sys.argv)
    rebuild_only = ('--rebuild' in sys.argv)
    print("🚀 STARTING STEP 2: DATA ENRICHMENT" + (" (TEST MODE)" if test_mode else "") + (" [EXPRESS]" if express_mode else ""))
    print("=" * 60)
    
    config = load_config(test_mode, express_mode)
    os.makedirs(config['OUTPUT_DIR'], exist_ok=True)

    if rebuild_only:
        try:
            print("🔄 Rebuild-only mode: pulling all enriched people from SQL (no API calls)...")
            # Load the authoritative enriched set from SQL
            full_enriched = load_existing_enriched_people() or []
            print(f"   Loaded {len(full_enriched)} records from SQL")

            # Backfill address/zip into SQL JSON for reliability
            try:
                _backfill_mail_fields_in_sql()
            except Exception as e:
                logger.warning(f"Address/ZIP backfill skipped during rebuild: {e}")

            # Write snapshot JSON/CSV using SQL-derived enriched set (deduped)
            with open(config['OUTPUT_JSON'], 'w') as f:
                json.dump(_dedupe_items(full_enriched), f, indent=2, default=str)
            _export_combined_to_csv(_dedupe_items(full_enriched), config['OUTPUT_CSV'])

            # Build CURRENT scope from existing overlays and write CSVs
            try:
                scoped = _build_current_scope(config['OUTPUT_DIR'])
            except Exception:
                scoped = []

            # Generate CURRENT directly from SQL
            _generate_current_csvs_from_sql(config['OUTPUT_DIR'])
        
            # ADD THIS: Generate contact CSVs
            contact_stats = _generate_contact_csvs_from_sql(config['OUTPUT_DIR'])
            
            # Define expected output file paths for printing
            cur_csv = os.path.join(config['OUTPUT_DIR'], 'current_enrichments.csv')
            cur_fmt_csv = os.path.join(config['OUTPUT_DIR'], 'current_enrichments_formatted.csv')
            contact_cur_csv = os.path.join(config['OUTPUT_DIR'], 'contact_current.csv')  # ADD THIS
            
            # NEW CSVs are empty for rebuild-only
            new_csv = os.path.join(config['OUTPUT_DIR'], 'new_enrichments.csv')
            _write_regular_csv(new_csv, [])
            new_fmt_csv = os.path.join(config['OUTPUT_DIR'], 'new_enrichments_formatted.csv')
            _write_formatted_csv(new_fmt_csv, [])
            contact_new_csv = os.path.join(config['OUTPUT_DIR'], 'contact_new.csv')  # ADD THIS
            _write_contact_csv(contact_new_csv, [])  # ADD THIS

            print("\n✅ REBUILD COMPLETED SUCCESSFULLY!")
            print("=" * 60)
            print("📁 OUTPUT FILES:")
            # UPDATE THIS: Include contact CSVs in the file list
            for p in [config['OUTPUT_CSV'], config['OUTPUT_JSON'], cur_csv, cur_fmt_csv, contact_cur_csv, new_csv, new_fmt_csv, contact_new_csv]:
                try:
                    if os.path.exists(p):
                        sz = os.path.getsize(p) / 1024
                        print(f"   📄 {p} ({sz:.1f} KB)")
                except Exception:
                    pass
            return 0
        except Exception as e:
            logger.error(f"Rebuild-only failed: {e}")
            print(f"\n❌ STEP 2 REBUILD FAILED: {e}")
            return 1
    
    try:
        people_to_enrich, existing_enriched_data = load_people_for_enrichment(config)
        if people_to_enrich:
            config['new_people_data'] = people_to_enrich
            logger.info(f"Will enrich {len(people_to_enrich)} people")
        
        # Backup existing enriched data if present (tolerate malformed JSON)
        backup_existing_data = _read_json_safe(config['OUTPUT_JSON'], default=[], label='existing enriched snapshot')
        if backup_existing_data:
            logger.info(f"Backing up {len(backup_existing_data)} existing enriched records")
        
        # Run enrichment
        logger.info("Starting data enrichment...")
        print("💎 Enriching patent inventor and assignee data")
        result = run_enrichment(config)
        
        # Merge with existing if needed
        if result.get('success'):
            # Full enriched snapshot from SQL (authoritative)
            full_enriched = result.get('enriched_data') or []
            new_enriched_data = result.get('newly_enriched_data') or []
            matched_existing = result.get('matched_existing') or []

            # Performance safeguard: avoid per-record SQL lookups for the entire snapshot.
            # Only augment the current run's records; server-side export hydrates fields as needed.
            try:
                current_cycle = (new_enriched_data or []) + (matched_existing or [])
                if current_cycle:
                    # Log diagnostics for current-cycle only to avoid confusion
                    _log_field_backfill_presence(current_cycle)
                    _augment_formatted_fields(current_cycle)
                    logger.info(f"Augmented formatted fields for current cycle only: {len(current_cycle)} records")
                else:
                    logger.info("No current-cycle records to augment; skipping snapshot-wide augmentation")
            except Exception as e:
                logger.warning(f"Skipping augmentation due to error: {e}")
            # Backfill address/zip into SQL JSON for all enriched rows
            try:
                _backfill_mail_fields_in_sql()
            except Exception as e:
                logger.warning(f"Address/ZIP backfill skipped due to error: {e}")

            # Always write local JSON/CSV snapshot from full enriched set (deduped)
            with open(config['OUTPUT_JSON'], 'w') as f:
                json.dump(_dedupe_items(full_enriched), f, indent=2, default=str)
            _export_combined_to_csv(_dedupe_items(full_enriched), config['OUTPUT_CSV'])

            # Save per-run files
            with open(os.path.join(config['OUTPUT_DIR'], 'enriched_patents_new_this_run.json'), 'w') as f:
                json.dump(new_enriched_data, f, indent=2, default=str)
            with open(os.path.join(config['OUTPUT_DIR'], 'current_cycle_enriched.json'), 'w') as f:
                json.dump((new_enriched_data or []) + (matched_existing or []), f, indent=2, default=str)

            # Write additional CSVs: NEW and CURRENT (regular + formatted)
            try:
                # NEW regular
                new_csv = os.path.join(config['OUTPUT_DIR'], 'new_enrichments.csv')
                removed_new_regular = _write_regular_csv(new_csv, _dedupe_items(new_enriched_data))
                print(f"   📄 {new_csv} (prebuilt)")

                # NEW formatted
                new_fmt_csv = os.path.join(config['OUTPUT_DIR'], 'new_enrichments_formatted.csv')
                removed_new_formatted = _write_formatted_csv(new_fmt_csv, _dedupe_items(new_enriched_data))
                print(f"   📄 {new_fmt_csv} (prebuilt)")

                 # NEW contact
                contact_new_csv = os.path.join(config['OUTPUT_DIR'], 'contact_new.csv')
                removed_new_contact = _write_contact_csv(contact_new_csv, _dedupe_items(new_enriched_data))
                print(f"   📄 {contact_new_csv} (prebuilt)")

                # Log filtered counts for NEW
                total_new = len(new_enriched_data or [])
                removed_new = max(removed_new_regular, removed_new_formatted)
                if removed_new:
                    print(f"   ⚠️ Filtered out {removed_new}/{total_new} new rows lacking full address/zip")
            except Exception as e:
                logger.warning(f"Could not write NEW CSVs: {e}")

            try:
                # Build CURRENT directly from SQL (includes all enriched rows; names/city/state from SQL)
                _backfill_mail_fields_in_sql()
                cur_stats = _generate_current_csvs_from_sql(config['OUTPUT_DIR'])
                print(f"   📄 {os.path.join(config['OUTPUT_DIR'], 'current_enrichments.csv')} (prebuilt from SQL)")
                print(f"   📄 {os.path.join(config['OUTPUT_DIR'], 'current_enrichments_formatted.csv')} (prebuilt from SQL)")
                contact_stats = _generate_contact_csvs_from_sql(config['OUTPUT_DIR'])
                removed_cur = max(cur_stats.get('removed_regular', 0), cur_stats.get('removed_formatted', 0))
                if removed_cur:
                    print(f"   ⚠️ Filtered out {removed_cur}/{cur_stats.get('total', 0)} current rows lacking full address/zip")
            except Exception as e:
                logger.warning(f"Could not write CURRENT CSVs from SQL: {e}")

            # Update result counts based on authoritative full set
            result['total_enriched_records'] = len(full_enriched)
            result['new_records_added'] = len(new_enriched_data)
            result['existing_records'] = max(0, len(full_enriched) - len(new_enriched_data))

            # Also persist a list of enrichments that were already in SQL before this run
            try:
                new_sigs = set(_person_signature_from_enriched(it) for it in new_enriched_data)
                preexisting = [it for it in full_enriched if _person_signature_from_enriched(it) not in new_sigs]
                pre_path = os.path.join(config['OUTPUT_DIR'], 'existing_enrichments_in_db.json')
                with open(pre_path, 'w') as f:
                    json.dump(preexisting, f, indent=2, default=str)
                print(f"PROGRESS: Saved existing enrichments JSON - count={len(preexisting):,}")
            except Exception as e:
                logger.warning(f"Could not write existing_enrichments_in_db.json: {e}")
        
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
            # Include the pre-filtered already-enriched count captured before the run
            pre_filtered = config.get('PRE_FILTER_ALREADY_ENRICHED_COUNT')
            if isinstance(pre_filtered, int):
                print(f"   🔁 Already enriched filtered before run: {pre_filtered:,}")
            # Additional skip details
            if result.get('already_enriched_count') is not None:
                print(f"   🔁 Duplicates skipped: {result.get('already_enriched_count', 0):,}")
            if result.get('skipped_failed_count') is not None:
                print(f"   🚫 Previously failed skipped: {result.get('skipped_failed_count', 0):,}")
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
            # Also list the prebuilt CSVs if present
            for extra in ('new_enrichments.csv','new_enrichments_formatted.csv','contact_new.csv','current_enrichments.csv','current_enrichments_formatted.csv','contact_current.csv'):
                p = os.path.join(config['OUTPUT_DIR'], extra)
                if os.path.exists(p):
                    sz = os.path.getsize(p) / 1024
                    print(f"   📄 {p} ({sz:.1f} KB)")
                    
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

    # Flatten nested dicts, stringify arrays/objects into JSON strings
    def _flatten(obj, prefix='', out=None):
        if out is None:
            out = {}
        if obj is None:
            if prefix:
                out[prefix] = ''
            return out
        # Treat booleans as empty (PDL sometimes returns presence booleans)
        if isinstance(obj, bool):
            if prefix:
                out[prefix] = ''
            return out
        if isinstance(obj, list):
            out[prefix] = json.dumps(obj, ensure_ascii=False)
            return out
        if isinstance(obj, dict):
            if not obj and prefix:
                out[prefix] = ''
                return out
            for k, v in obj.items():
                key = f"{prefix}.{k}" if prefix else k
                _flatten(v, key, out)
            return out
        # Primitive
        val = '' if str(obj).strip().lower() in {'nan', 'none', 'null'} else str(obj)
        out[prefix] = val
        return out

    rows = []
    headers = set()
    for rec in enriched_data:
        flat = _flatten(rec)
        rows.append(flat)
        for k in flat.keys():
            headers.add(k)

    headers = sorted(headers)
    # Build DataFrame with all headers, fill missing keys with ''
    normalized = [{h: r.get(h, '') for h in headers} for r in rows]
    df = pd.DataFrame(normalized, columns=headers)
    df.fillna('', inplace=True)

    # Simplify column names to last dotted segment, deduplicating with suffixes
    def _simplify_headers(cols):
        mapping = {}
        counts = {}
        simple_cols = []
        for c in cols:
            base = c.split('.')[-1]
            n = counts.get(base, 0) + 1
            counts[base] = n
            name = base if n == 1 else f"{base}_{n}"
            mapping[c] = name
            simple_cols.append(name)
        return mapping, simple_cols

    _, simple_cols = _simplify_headers(headers)
    df.columns = simple_cols

    df.to_csv(filename, index=False)
    logger.info(f"Exported {len(rows)} combined records to {filename} with {len(headers)} columns")


def _backfill_mail_fields_in_sql(limit: int = None):
    """Update enriched_people.enrichment_data JSON to include mail_to_add1 and mail_to_zip
    in existing_record for rows that are missing them, deriving from pdl_data when possible.
    """
    cfg = DatabaseConfig.from_env()
    db = DatabaseManager(cfg)
    rows = db.execute_query("SELECT id, enrichment_data FROM enriched_people") or []
    updated = 0
    def _first_non_empty(*vals):
        for v in vals:
            s = '' if v is None else str(v).strip()
            if s != '':
                return s
        return ''
    def _pick_street(pdl: dict) -> str:
        if not isinstance(pdl, dict):
            return ''
        x = _first_non_empty(
            pdl.get('job_company_location_street_address'),
            pdl.get('location_street_address')
        )
        if x:
            return x
        try:
            sa = pdl.get('street_addresses')
            if isinstance(sa, list) and sa:
                first = sa[0] or {}
                return _first_non_empty(first.get('street_address'), first.get('formatted_address'))
        except Exception:
            pass
        try:
            exp = pdl.get('experience')
            if isinstance(exp, list) and exp:
                primary = next((e for e in exp if e and e.get('is_primary')), None)
                if primary:
                    loc = ((primary.get('company') or {}).get('location') or {})
                    v = _first_non_empty(loc.get('street_address'), loc.get('address_line_2'))
                    if v:
                        return v
                for e in exp:
                    try:
                        loc = ((e.get('company') or {}).get('location') or {})
                        v = _first_non_empty(loc.get('street_address'), loc.get('address_line_2'))
                        if v:
                            return v
                    except Exception:
                        continue
        except Exception:
            pass
        return ''
    def _pick_zip(pdl: dict) -> str:
        if not isinstance(pdl, dict):
            return ''
        x = _first_non_empty(
            pdl.get('job_company_location_postal_code'),
            pdl.get('location_postal_code')
        )
        if x:
            return x
        try:
            sa = pdl.get('street_addresses')
            if isinstance(sa, list) and sa:
                first = sa[0] or {}
                return _first_non_empty(first.get('postal_code'))
        except Exception:
            pass
        try:
            exp = pdl.get('experience')
            if isinstance(exp, list) and exp:
                primary = next((e for e in exp if e and e.get('is_primary')), None)
                if primary:
                    loc = ((primary.get('company') or {}).get('location') or {})
                    v = _first_non_empty(loc.get('postal_code'))
                    if v:
                        return v
                for e in exp:
                    try:
                        loc = ((e.get('company') or {}).get('location') or {})
                        v = _first_non_empty(loc.get('postal_code'))
                        if v:
                            return v
                    except Exception:
                        continue
        except Exception:
            pass
        return ''
    for i, r in enumerate(rows):
        if limit and i >= limit:
            break
        try:
            raw = r.get('enrichment_data') if isinstance(r, dict) else None
            ed = json.loads(raw) if isinstance(raw, (str, bytes)) else (raw or {})
        except Exception:
            ed = {}
        enriched = ed.get('enrichment_result') or {}
        enriched_data = enriched.get('enriched_data') or ed.get('enriched_data') or {}
        pdl = enriched_data.get('pdl_data') or {}
        existing = ed.get('existing_record') or {}
        need_street = not bool(str(existing.get('mail_to_add1') or '').strip())
        need_zip = not bool(str(existing.get('mail_to_zip') or '').strip())
        if not (need_street or need_zip):
            continue
        street = _pick_street(pdl) if need_street else existing.get('mail_to_add1')
        zc = _pick_zip(pdl) if need_zip else existing.get('mail_to_zip')
        if not (street or zc):
            continue
        new_existing = dict(existing)
        if need_street:
            new_existing['mail_to_add1'] = street
        if need_zip:
            new_existing['mail_to_zip'] = zc
        ed['existing_record'] = new_existing
        try:
            db.execute_query("UPDATE enriched_people SET enrichment_data=%s WHERE id=%s", (json.dumps(ed, ensure_ascii=False), r.get('id')))
            updated += 1
        except Exception:
            continue
    logger.info(f"Backfilled mail_to_add1/mail_to_zip into {updated} enriched_people rows")


# ---------------- Additional exports: new/current (regular and formatted) ---------------- #
def _sanitize_for_csv(val):
    if val is None:
        return ''
    if isinstance(val, bool):
        return ''
    s = str(val).strip()
    return '' if s.lower() in {'nan', 'null', 'none', 'true', 'false'} else s


def _first_non_empty(*vals):
    for v in vals:
        s = _sanitize_for_csv(v)
        if s != '':
            return s
    return ''


def _get_ed_roots(item):
    has_sql_root = bool(item and item.get('enrichment_result') and item['enrichment_result'].get('enriched_data'))
    ed = item['enrichment_result']['enriched_data'] if has_sql_root else (item.get('enriched_data') or {})
    original = ed.get('original_data') or ed.get('original_person') or item.get('original_person') or {}
    pdl = ed.get('pdl_data') or ({})
    existing = ed.get('existing_record') or {}
    return ed, original, pdl, existing


def _pick_pdl_street(pdl):
    if not pdl:
        return ''
    # Per request: only use the company street address field
    s = _first_non_empty(pdl.get('job_company_location_street_address'))
    if s:
        return s
    return ''


def _pick_pdl_zip(pdl):
    if not pdl:
        return ''
    # Per request: only use the company postal code field
    z = _first_non_empty(pdl.get('job_company_location_postal_code'))
    if z:
        return z
    return ''


FORMATTED_HEADERS = [
  'issue_id','new_issue_rec_num','inventor_id','patent_no','title','issue_date',
  'mail_to_assignee','mail_to_name','mail_to_add1','mail_to_add2','mail_to_add3',
  'mail_to_city','mail_to_state','mail_to_zip','mail_to_country','mail_to_send_key',
  'inventor_first','inventor_last','mod_user','bar_code','inventor_contact'
]


def _build_formatted_row(item: dict) -> dict:
    ed, original, pdl, existing = _get_ed_roots(item)
    
    # email - try PDL data first, then existing record
    email = ''
    try:
        emails = pdl.get('emails')
        if isinstance(emails, list) and emails:
            e0 = emails[0]
            if isinstance(e0, str):
                email = e0
            elif isinstance(e0, dict):
                email = e0.get('address') or e0.get('email') or ''
    except Exception:
        pass
    
    # If no email from PDL, try existing record
    if not email:
        email = _first_non_empty(existing.get('email'), item.get('email'))
    
    # Prefer PDL company street address first, then existing record address
    street = _first_non_empty(
        _pick_pdl_street(pdl), 
        item.get('mail_to_add1'), 
        existing.get('mail_to_add1'),
        existing.get('address')  # Add the new address field from existing_people table
    )
    
    line2 = _first_non_empty(pdl.get('job_company_location_address_line_2'), pdl.get('location_address_line_2'))
    city = _first_non_empty(pdl.get('job_company_location_locality'), pdl.get('location_locality'), original.get('city'), item.get('city'))
    
    # Preserve state from existing/original; do not override with PDL
    state = _first_non_empty(item.get('mail_to_state'), existing.get('mail_to_state'), original.get('state'), item.get('state'))
    
    # Prefer PDL company postal code first, then existing record
    zip_code = _first_non_empty(_pick_pdl_zip(pdl), item.get('mail_to_zip'), existing.get('mail_to_zip'))
    
    country = _first_non_empty(pdl.get('job_company_location_country'), pdl.get('location_country'), original.get('country'), item.get('country'))
    first = _first_non_empty(original.get('first_name'), item.get('first_name'))
    last = _first_non_empty(original.get('last_name'), item.get('last_name'))

    formatted = {
        # UPDATED: Map the new fields from existing_people table
        'issue_id': _first_non_empty(item.get('issue_id'), existing.get('issue_id')),
        'new_issue_rec_num': _first_non_empty(item.get('new_issue_rec_num'), existing.get('new_issue_rec_num')),
        'inventor_id': _first_non_empty(item.get('inventor_id'), existing.get('inventor_id')),
        'patent_no': _first_non_empty(item.get('patent_no'), existing.get('patent_no'), original.get('patent_number'), item.get('patent_number')),
        'title': _first_non_empty(item.get('patent_title'), item.get('title'), existing.get('title'), original.get('patent_title')),
        'issue_date': _first_non_empty(item.get('issue_date'), existing.get('issue_date')),
        'mail_to_assignee': _first_non_empty(item.get('mail_to_assignee'), existing.get('mail_to_assignee')),
        'mail_to_name': _sanitize_for_csv(_first_non_empty(item.get('mail_to_name'), f"{first} {last}".strip())),
        'mail_to_add1': street,
        'mail_to_add2': line2,
        'mail_to_add3': item.get('mail_to_add3') or '',
        'mail_to_city': _first_non_empty(item.get('mail_to_city'), city),
        'mail_to_state': state,
        'mail_to_zip': _first_non_empty(item.get('mail_to_zip'), existing.get('mail_to_zip'), zip_code),
        'mail_to_country': _first_non_empty(item.get('mail_to_country'), country),
        'mail_to_send_key': item.get('mail_to_send_key') or '',
        'inventor_first': first,
        'inventor_last': last,
        'mod_user': _first_non_empty(item.get('mod_user'), existing.get('mod_user')),  # UPDATED: Use existing record
        'bar_code': _first_non_empty(item.get('bar_code'), existing.get('bar_code')),
        'inventor_contact': 0
    }
    
    # Ensure all expected headers are present
    for h in FORMATTED_HEADERS:
        if h not in formatted:
            formatted[h] = ''
    
    return formatted

def _flatten_for_csv(records: list):
    # Reuse the combined flattener
    def _flatten(obj, prefix='', out=None):
        if out is None:
            out = {}
        if obj is None:
            if prefix:
                out[prefix] = ''
            return out
        if isinstance(obj, bool):
            if prefix:
                out[prefix] = ''
            return out
        if isinstance(obj, list):
            out[prefix] = json.dumps(obj, ensure_ascii=False)
            return out
        if isinstance(obj, dict):
            if not obj and prefix:
                out[prefix] = ''
                return out
            for k, v in obj.items():
                key = f"{prefix}.{k}" if prefix else k
                _flatten(v, key, out)
            return out
        val = '' if str(obj).strip().lower() in {'nan', 'none', 'null'} else str(obj)
        out[prefix] = val
        return out

    headers = set()
    rows = []
    for rec in records or []:
        flat = _flatten(rec)
        rows.append(flat)
        headers.update(flat.keys())
    headers = sorted(headers)
    normalized = [{h: r.get(h, '') for h in headers} for r in rows]
    return headers, normalized


def _simplify_headers(headers):
    mapping = {}
    counts = {}
    simple_cols = []
    for c in headers:
        base = c.split('.')[-1]
        n = counts.get(base, 0) + 1
        counts[base] = n
        name = base if n == 1 else f"{base}_{n}"
        mapping[c] = name
        simple_cols.append(name)
    return mapping, simple_cols


def _write_regular_csv(path, records):
    import pandas as pd
    # Helper to detect boolean-like address in either formatted row or underlying JSON existing_record
    def _has_boolean_address(it, fmt=None):
        try:
            if fmt is None:
                fmt = _build_formatted_row(it)
            addr = (fmt.get('mail_to_add1') or '').strip().lower()
            zc = (fmt.get('mail_to_zip') or '').strip().lower()
            if addr in {'true','false'} or zc in {'true','false'}:
                return True
            # Inspect underlying JSON as well
            _, __, ___, existing = _get_ed_roots(it)
            eaddr = (existing.get('mail_to_add1') or '').strip().lower() if isinstance(existing, dict) else ''
            ezip = (existing.get('mail_to_zip') or '').strip().lower() if isinstance(existing, dict) else ''
            return (eaddr in {'true','false'}) or (ezip in {'true','false'})
        except Exception:
            return False
    kept = []
    removed = 0
    for it in (records or []):
        fmt = _build_formatted_row(it)
        if _has_boolean_address(it, fmt):
            removed += 1
            continue
        kept.append(it)
    headers, rows = _flatten_for_csv(kept)
    mapping, simple_cols = _simplify_headers(headers)
    df_rows = [{mapping[h]: r.get(h, '') for h in headers} for r in rows]
    pd.DataFrame(df_rows, columns=simple_cols).to_csv(path, index=False)
    return removed


def _write_formatted_csv(path, records):
    import csv
    rows_all = [_build_formatted_row(r) for r in (records or [])]
    rows = []
    removed = 0
    for idx, r in enumerate(rows_all):
        try:
            addr = (r.get('mail_to_add1') or '').strip().lower()
            zc = (r.get('mail_to_zip') or '').strip().lower()
            hasBoolInRow = addr in {'true','false'} or zc in {'true','false'}
            # Also inspect underlying JSON existing_record
            it = (records or [])[idx] if idx < len(records or []) else None
            _, __, ___, existing = _get_ed_roots(it or {})
            eaddr = (existing.get('mail_to_add1') or '').strip().lower() if isinstance(existing, dict) else ''
            ezip = (existing.get('mail_to_zip') or '').strip().lower() if isinstance(existing, dict) else ''
            hasBoolInJson = (eaddr in {'true','false'}) or (ezip in {'true','false'})
            if hasBoolInRow or hasBoolInJson:
                removed += 1
                continue
            rows.append(r)
        except Exception:
            rows.append(r)
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=FORMATTED_HEADERS)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return removed


# ---------------- Dedup helpers ---------------- #
def _sig_simple(item: dict) -> str:
    try:
        # Prefer original data when present
        ed = item.get('enriched_data') or (item.get('enrichment_result', {}).get('enriched_data') if isinstance(item.get('enrichment_result'), dict) else {}) or {}
        original = ed.get('original_data') or ed.get('original_person') or item.get('original_person') or {}
        def norm(v):
            return ('' if v is None else str(v)).strip().lower()
        return '|'.join([
            norm(original.get('first_name') or item.get('first_name')),
            norm(original.get('last_name') or item.get('last_name')),
            norm(original.get('city') or item.get('city')),
            norm(original.get('state') or item.get('state'))
        ])
    except Exception:
        return ''


def _has_enriched(it: dict) -> bool:
    ed = (it.get('enriched_data') or (it.get('enrichment_result', {}).get('enriched_data') if isinstance(it.get('enrichment_result'), dict) else {}))
    return bool(ed)


def _dedupe_items(items: list) -> list:
    by_sig = {}
    for it in items or []:
        s = _sig_simple(it)
        if not s:
            continue
        if s not in by_sig or (_has_enriched(it) and not _has_enriched(by_sig[s])):
            by_sig[s] = it
    return list(by_sig.values())


def _fetch_all_enriched_rows_from_sql() -> list:
    cfg = DatabaseConfig.from_env()
    db = DatabaseManager(cfg)
    
    # Discover existing_people columns safely
    col_rows = db.execute_query('SHOW COLUMNS FROM existing_people') or []
    cols = [r.get('Field') or r.get('COLUMN_NAME') or r.get('field') for r in col_rows if isinstance(r, dict)]
    
    def pick(want, alts=None):
        alts = alts or []
        if want in cols:
            return want
        for a in alts:
            if a in cols:
                return a
        return None
    
    # UPDATED: Include the new fields in the mapping
    mapping = {
        'issue_id': pick('issue_id'),
        'new_issue_rec_num': pick('new_issue_rec_num', ['issue_rec_num','rec_num']),
        'inventor_id': pick('inventor_id'),
        'patent_no': pick('patent_no', ['patent_number','patent_num']),
        'title': pick('title', ['patent_title','invention_title']),
        'issue_date': pick('issue_date', ['date','patent_date']),
        'bar_code': pick('bar_code', ['barcode']),
        'mod_user': pick('mod_user', ['modified_by','last_modified_by']),
        'mail_to_assignee': pick('mail_to_assignee', ['assignee','assign_name']),
        'mail_to_name': pick('mail_to_name'),
        'mail_to_add1': pick('mail_to_add1', ['address','addr1','mail_to_add_1']),
        'mail_to_zip': pick('mail_to_zip', ['zip']),
        # UPDATED: Add the new fields
        'address': pick('address', ['addr1', 'mail_to_add1']),  # Fallback to mail_to_add1 if address doesn't exist
        'email': pick('email', ['email_address', 'email_addr'])
    }
    
    select_parts = []
    for alias, col in mapping.items():
        if not col:
            continue
        select_parts.append(f"ex.{col} AS {alias}")
    
    select_clause = (', ' + ', '.join(select_parts)) if select_parts else ''
    
    query = (
        f"SELECT ep.*{select_clause} "
        "FROM enriched_people ep "
        "LEFT JOIN existing_people ex ON ep.first_name = ex.first_name AND ep.last_name = ex.last_name "
        "AND IFNULL(ep.city,'') = IFNULL(ex.city,'') AND IFNULL(ep.state,'') = IFNULL(ex.state,'') "
        "ORDER BY ep.enriched_at DESC"
    )
    
    return db.execute_query(query) or []

def _build_items_for_formatted_from_sql_rows(rows: list) -> list:
    items = []
    for row in rows:
        try:
            ed_raw = row.get('enrichment_data') if isinstance(row, dict) else None
            ed = json.loads(ed_raw) if isinstance(ed_raw, (str, bytes)) else (ed_raw or {})
        except Exception:
            ed = {}
        
        enr = ed.get('enrichment_result') or {}
        ed_root = enr.get('enriched_data') or {}
        
        # UPDATED: Merge existing_record from SQL join into ed_root.existing_record
        # Include all the new fields from the database
        existing_sql = {}
        for k in ('issue_id','new_issue_rec_num','inventor_id','patent_no','title','issue_date','bar_code','mod_user','mail_to_assignee','mail_to_name','mail_to_add1','mail_to_zip','address','email'):
            if k in row:
                existing_sql[k] = row.get(k)
        
        ex_rec = ed_root.get('existing_record') or ed.get('existing_record') or {}
        merged_existing = { 
            **ex_rec, 
            **{k: v for k, v in existing_sql.items() if (v is not None and str(v).strip() != '')} 
        }
        
        ed_root['existing_record'] = merged_existing
        enr['enriched_data'] = ed_root
        
        item = {
            'first_name': row.get('first_name'),
            'last_name': row.get('last_name'),
            'city': row.get('city'),
            'state': row.get('state'),
            'patent_number': row.get('patent_number'),
            'enrichment_result': enr,
            'original_person': ed.get('original_person') or {},
            # UPDATED: Also include the new fields at the top level for easier access
            'issue_id': row.get('issue_id'),
            'inventor_id': row.get('inventor_id'),
            'mod_user': row.get('mod_user'),
            'address': row.get('address'),
            'email': row.get('email')
        }
        items.append(item)
    
    return _dedupe_items(items)

def _generate_current_csvs_from_sql(output_dir: str):
    print(f"\n=== GENERATING CURRENT CSVS FROM SQL (WITH DETAILED LOGGING) ===")
    
    # Step 1: Fetch SQL enriched rows
    print(f"STEP 1: Fetching enriched people from SQL...")
    rows = _fetch_all_enriched_rows_from_sql()
    print(f"  Raw SQL rows fetched: {len(rows):,}")
    
    # Log SQL table stats
    try:
        db = DatabaseManager(DatabaseConfig.from_env())
        sql_stats = db.execute_query("SELECT COUNT(*) as total FROM enriched_people") or []
        total_sql = sql_stats[0].get('total', 0) if sql_stats else 0
        print(f"  Total enriched_people in SQL: {total_sql:,}")
        
        # Check for address backfill
        addr_stats = db.execute_query("""
            SELECT 
                SUM(CASE WHEN enrichment_data LIKE '%mail_to_add1%' THEN 1 ELSE 0 END) as has_addr,
                SUM(CASE WHEN enrichment_data LIKE '%mail_to_zip%' THEN 1 ELSE 0 END) as has_zip
            FROM enriched_people
        """) or []
        if addr_stats:
            print(f"  Records with mail_to_add1 in JSON: {addr_stats[0].get('has_addr', 0):,}")
            print(f"  Records with mail_to_zip in JSON: {addr_stats[0].get('has_zip', 0):,}")
    except Exception as e:
        print(f"  Error checking SQL stats: {e}")
    
    # Step 2: Build items from SQL rows
    print(f"STEP 2: Building items from SQL rows...")
    items_sql = _build_items_for_formatted_from_sql_rows(rows)
    print(f"  Items built from SQL: {len(items_sql):,}")
    
    # Check enrichment status
    enriched_count = sum(1 for item in items_sql if item.get('enriched_data', {}).get('pdl_data'))
    print(f"  Items with PDL enrichment data: {enriched_count:,}")
    
    # Step 3: Load Step 1 existing people
    print(f"STEP 3: Loading Step 1 existing people...")
    try:
        scope_items = _build_current_scope(output_dir, include_step1=True)
        print(f"  Step 1 scope items loaded: {len(scope_items):,}")
        
        # Analyze Step 1 items
        step1_existing = 0
        step1_moved = 0
        step1_auto_matched = 0
        for item in scope_items:
            match_reason = item.get('match_reason', '')
            match_status = item.get('match_status', '')
            if match_reason in ['auto_matched', 'moved']:
                step1_existing += 1
                if match_reason == 'moved':
                    step1_moved += 1
                elif match_reason == 'auto_matched':
                    step1_auto_matched += 1
        
        print(f"  Step 1 existing people: {step1_existing:,}")
        print(f"    Auto-matched: {step1_auto_matched:,}")
        print(f"    Moved (same name, diff address): {step1_moved:,}")
        
    except Exception as e:
        print(f"  Error loading Step 1 scope: {e}")
        scope_items = []
    
    # Step 4: Union and analyze before dedup
    print(f"STEP 4: Union and deduplication...")
    all_items = (items_sql or []) + (scope_items or [])
    print(f"  Combined before dedup: {len(all_items):,}")
    print(f"    From SQL: {len(items_sql):,}")
    print(f"    From Step 1: {len(scope_items):,}")
    
    # Analyze sources before dedup
    source_count = {}
    for item in all_items:
        source = 'unknown'
        if 'enrichment_result' in item:
            source = 'sql_enriched'
        elif item.get('match_reason'):
            source = f"step1_{item.get('match_reason')}"
        elif item.get('match_status'):
            source = f"step1_{item.get('match_status')}"
        source_count[source] = source_count.get(source, 0) + 1
    
    print(f"  Source breakdown before dedup:")
    for source, count in sorted(source_count.items()):
        print(f"    {source}: {count:,}")
    
    # Deduplication
    items = _dedupe_items(all_items)
    dedup_removed = len(all_items) - len(items)
    print(f"  After deduplication: {len(items):,} (removed {dedup_removed:,})")
    
    # Analyze what survived dedup
    survived_sources = {}
    for item in items:
        source = 'unknown'
        if 'enrichment_result' in item:
            source = 'sql_enriched'
        elif item.get('match_reason'):
            source = f"step1_{item.get('match_reason')}"
        elif item.get('match_status'):
            source = f"step1_{item.get('match_status')}"
        survived_sources[source] = survived_sources.get(source, 0) + 1
    
    print(f"  Source breakdown after dedup:")
    for source, count in sorted(survived_sources.items()):
        print(f"    {source}: {count:,}")
    
    # Step 5: Address filtering analysis
    print(f"STEP 5: Address filtering analysis...")
    has_address = 0
    has_zip = 0
    has_both_valid = 0
    boolean_issues = 0
    empty_issues = 0
    
    for item in items:
        try:
            formatted = _build_formatted_row(item)
            addr = (formatted.get('mail_to_add1') or '').strip().lower()
            zip_code = (formatted.get('mail_to_zip') or '').strip().lower()
            
            if addr and addr not in {'true', 'false'}:
                has_address += 1
            if zip_code and zip_code not in {'true', 'false'}:
                has_zip += 1
            if addr and zip_code and addr not in {'true', 'false'} and zip_code not in {'true', 'false'}:
                has_both_valid += 1
            if addr in {'true', 'false'} or zip_code in {'true', 'false'}:
                boolean_issues += 1
            if not addr and not zip_code:
                empty_issues += 1
        except Exception:
            pass
    
    print(f"  Items with valid address: {has_address:,}")
    print(f"  Items with valid ZIP: {has_zip:,}")
    print(f"  Items with both valid addr+ZIP: {has_both_valid:,}")
    print(f"  Items with boolean address/ZIP: {boolean_issues:,}")
    print(f"  Items with empty address+ZIP: {empty_issues:,}")
    print(f"  WILL BE FILTERED OUT: {len(items) - has_both_valid:,}")
    
    # Step 6: Write CSVs with detailed filtering logs
    print(f"STEP 6: Writing CSV files...")
    
    # Current regular CSV
    cur_csv = os.path.join(output_dir, 'current_enrichments.csv')
    print(f"  Writing regular CSV: {cur_csv}")
    removed_regular = _write_regular_csv(cur_csv, items)
    kept_regular = len(items) - removed_regular
    print(f"    Regular CSV: {kept_regular:,} kept, {removed_regular:,} filtered")
    
    # Current formatted CSV  
    cur_fmt_csv = os.path.join(output_dir, 'current_enrichments_formatted.csv')
    print(f"  Writing formatted CSV: {cur_fmt_csv}")
    removed_formatted = _write_formatted_csv(cur_fmt_csv, items)
    kept_formatted = len(items) - removed_formatted
    print(f"    Formatted CSV: {kept_formatted:,} kept, {removed_formatted:,} filtered")
    
    print(f"=== CURRENT CSV GENERATION COMPLETE ===")
    
    return {
        'removed_regular': removed_regular,
        'removed_formatted': removed_formatted,
        'total': len(items),
        'kept': len(items) - max(removed_regular, removed_formatted)
    }

def _person_sig_for_scope(item):
    # Same signature as server: name+city+state from original data
    ed, original, _, _ = _get_ed_roots(item or {})
    def norm(v):
        return ('' if v is None else str(v)).strip().lower()
    return '|'.join([
        norm(original.get('first_name') or item.get('first_name')),
        norm(original.get('last_name') or item.get('last_name')),
        norm(original.get('city') or item.get('city')),
        norm(original.get('state') or item.get('state'))
    ])

def _build_current_scope(output_dir, include_step1=True):
    # current scope = current_cycle (newly_enriched + matched_existing) plus Step 1 existing (optional),
    # deduped by signature, overlaid with enriched snapshot.
    try:
        with open(os.path.join(output_dir, 'current_cycle_enriched.json'), 'r') as f:
            current = json.load(f)
    except Exception:
        current = []
    # Step1 existing
    step1 = []
    if include_step1:
        for name in ('existing_people_in_db.json', 'existing_people_found.json'):
            p = os.path.join(output_dir, name)
            if os.path.exists(p):
                try:
                    with open(p, 'r') as f:
                        step1 = json.load(f)
                        break
                except Exception:
                    step1 = []

    # full snapshot for overlay
    try:
        with open(os.path.join(output_dir, 'enriched_patents.json'), 'r') as f:
            full = json.load(f)
    except Exception:
        full = []

    # Dedup current by signature
    seen = set()
    current_dedup = []
    for it in current:
        s = _person_sig_for_scope(it)
        if not s or s in seen:
            continue
        seen.add(s)
        current_dedup.append(it)

    # Merge with Step1 and dedupe again preferring enriched entries
    def _has_enriched(it):
        ed = (it.get('enriched_data') or (it.get('enrichment_result', {}).get('enriched_data') if isinstance(it.get('enrichment_result'), dict) else {}))
        return bool(ed)
    by_sig = {}
    for arr in (current_dedup, (step1 if isinstance(step1, list) else [])):
        for it in arr:
            s = _person_sig_for_scope(it)
            if not s:
                continue
            if s not in by_sig or (_has_enriched(it) and not _has_enriched(by_sig[s])):
                by_sig[s] = it
    merged = list(by_sig.values())

    # Enriched overlay map
    enriched_map = {}
    for rec in (full or []):
        s = _person_sig_for_scope(rec)
        if s:
            enriched_map[s] = rec

    current_overlaid = [enriched_map.get(_person_sig_for_scope(it), it) for it in merged]
    return current_overlaid


# ---------------- Contact CSV generation ---------------- #
CONTACT_HEADERS = ['first_name', 'last_name', 'email', 'address', 'zip', 'state']

def _build_contact_row(item: dict) -> dict:
    """Build a contact row with just the essential contact fields"""
    ed, original, pdl, existing = _get_ed_roots(item)
    
    # Extract email from PDL data or existing record
    email = ''
    try:
        emails = pdl.get('emails')
        if isinstance(emails, list) and emails:
            e0 = emails[0]
            if isinstance(e0, str):
                email = e0
            elif isinstance(e0, dict):
                email = e0.get('address') or e0.get('email') or ''
    except Exception:
        pass
    
    # If no email from PDL, try existing record
    if not email:
        email = _first_non_empty(existing.get('email'), item.get('email'))
    
    # Get address - prefer PDL company address, then existing record
    address = _first_non_empty(
        _pick_pdl_street(pdl), 
        item.get('mail_to_add1'), 
        existing.get('mail_to_add1'),
        existing.get('address')
    )
    
    # Get ZIP code - prefer PDL company postal code, then existing record
    zip_code = _first_non_empty(
        _pick_pdl_zip(pdl), 
        item.get('mail_to_zip'), 
        existing.get('mail_to_zip')
    )
    
    # Get state - preserve from existing/original; do not override with PDL
    state = _first_non_empty(
        item.get('mail_to_state'), 
        existing.get('mail_to_state'), 
        original.get('state'), 
        item.get('state')
    )
    
    # Get names
    first_name = _first_non_empty(original.get('first_name'), item.get('first_name'))
    last_name = _first_non_empty(original.get('last_name'), item.get('last_name'))
    
    contact_row = {
        'first_name': first_name,
        'last_name': last_name, 
        'email': email,
        'address': address,
        'zip': zip_code,
        'state': state
    }
    
    return contact_row

def _write_contact_csv(path, records, csv_type='contact'):
    """Write contact CSV with just essential contact fields"""
    import csv
    
    # Build contact rows from all records
    contact_rows_all = [_build_contact_row(r) for r in (records or [])]
    
    # Filter out rows with boolean address/zip issues or missing essential data
    contact_rows = []
    removed = 0
    
    for idx, row in enumerate(contact_rows_all):
        try:
            # Check for boolean issues
            addr = (row.get('address') or '').strip().lower()
            zip_code = (row.get('zip') or '').strip().lower()
            email = (row.get('email') or '').strip()
            first_name = (row.get('first_name') or '').strip()
            last_name = (row.get('last_name') or '').strip()
            
            # Skip if boolean values in address/zip
            if addr in {'true', 'false'} or zip_code in {'true', 'false'}:
                removed += 1
                continue
                
            # Skip if missing essential contact info (name + at least one contact method)
            if not (first_name or last_name):
                removed += 1
                continue
                
            # Keep if we have at least email OR (address + zip)
            has_email = bool(email and '@' in email)
            has_address = bool(addr and zip_code)
            
            if not (has_email or has_address):
                removed += 1
                continue
                
            contact_rows.append(row)
            
        except Exception:
            removed += 1
            continue
    
    # Write CSV
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CONTACT_HEADERS)
        writer.writeheader()
        for row in contact_rows:
            # Ensure all headers are present
            clean_row = {h: row.get(h, '') for h in CONTACT_HEADERS}
            writer.writerow(clean_row)
    
    logger.info(f"Wrote {len(contact_rows)} contact records to {path} (filtered out {removed})")
    return removed

def _generate_contact_csvs_from_sql(output_dir: str):
    """Generate contact CSVs for current data from SQL"""
    print(f"STEP 7: Generating contact CSVs...")
    
    # Use the same data source as current CSVs
    rows = _fetch_all_enriched_rows_from_sql()
    items_sql = _build_items_for_formatted_from_sql_rows(rows)
    
    # Load Step 1 existing people
    try:
        scope_items = _build_current_scope(output_dir, include_step1=True)
    except Exception:
        scope_items = []
    
    # Combine and dedupe (same as current CSV logic)
    all_items = (items_sql or []) + (scope_items or [])
    items = _dedupe_items(all_items)
    
    # Write contact CSV for current data
    contact_current_csv = os.path.join(output_dir, 'contact_current.csv')
    removed_current = _write_contact_csv(contact_current_csv, items, 'current')
    
    print(f"  📄 {contact_current_csv} ({len(items) - removed_current:,} kept, {removed_current:,} filtered)")
    
    return {
        'removed_current': removed_current,
        'total_current': len(items),
        'kept_current': len(items) - removed_current
    }

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
