# =============================================================================
# runners/enrich.py - SIMPLE AND FAST
# Step 3: Data Enrichment using PeopleDataLabs API
# =============================================================================
import logging
import os
import json
import time
import pandas as pd
from typing import Dict, Any, List, Tuple, Optional
from pathlib import Path
from classes.people_data_labs_enricher import PeopleDataLabsEnricher
from database.db_manager import DatabaseManager, DatabaseConfig


def _person_signature(person: Dict[str, Any]) -> str:
    """Build a stable signature for a person used for matching/skipping."""
    first_name = (person.get('first_name') or '').strip().lower()
    last_name = (person.get('last_name') or '').strip().lower()
    city = (person.get('city') or '').strip().lower()
    state = (person.get('state') or '').strip().lower()
    patent_number = (person.get('patent_number') or '').strip()
    return f"{first_name}_{last_name}_{city}_{state}_{patent_number}"


def _ensure_failed_table(conn, engine: str):
    """Ensure failed_enrichments table exists with a reasonable schema."""
    cursor = conn.cursor()
    if engine == 'sqlite':
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS failed_enrichments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT,
                last_name TEXT,
                city TEXT,
                state TEXT,
                country TEXT,
                patent_number TEXT,
                person_type TEXT,
                failure_reason TEXT,
                failure_code TEXT,
                attempt_count INTEGER DEFAULT 1,
                last_attempt_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                raw_person TEXT,
                context TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(first_name,last_name,city,state,patent_number,person_type)
            )
            """
        )
    else:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS failed_enrichments (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                city VARCHAR(100),
                state VARCHAR(50),
                country VARCHAR(100),
                patent_number VARCHAR(50),
                person_type VARCHAR(50),
                failure_reason TEXT,
                failure_code VARCHAR(100),
                attempt_count INT DEFAULT 1,
                last_attempt_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                raw_person JSON,
                context JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uniq_failed_person (first_name,last_name,city,state,patent_number,person_type),
                INDEX idx_person (last_name, first_name, state, city)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
    try:
        conn.commit()
    except Exception:
        pass


def _record_failed_enrichment(cursor, engine: str, person: Dict[str, Any], reason: str, failure_code: Optional[str] = None):
    """Insert or update a failed enrichment record."""
    # Normalize person fields
    first_name = (person.get('first_name') or '').strip()
    last_name = (person.get('last_name') or '').strip()
    city = (person.get('city') or '').strip()
    state = (person.get('state') or '').strip()
    country = (person.get('country') or 'US').strip()
    patent_number = (person.get('patent_number') or '').strip()
    person_type = (person.get('person_type') or 'inventor').strip()

    if engine == 'sqlite':
        query = (
            "INSERT INTO failed_enrichments (first_name,last_name,city,state,country,patent_number,person_type,failure_reason,failure_code,raw_person,context) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(first_name,last_name,city,state,patent_number,person_type) DO UPDATE SET "
            "attempt_count = attempt_count + 1, last_attempt_at = CURRENT_TIMESTAMP, failure_reason=excluded.failure_reason, failure_code=excluded.failure_code"
        )
        params = (
            first_name, last_name, city, state, country, patent_number, person_type,
            reason, failure_code or '', json.dumps(person), json.dumps({'stage': 'enrichment'})
        )
    else:
        query = (
            "INSERT INTO failed_enrichments "
            "(first_name,last_name,city,state,country,patent_number,person_type,failure_reason,failure_code,raw_person,context) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE attempt_count=attempt_count+1, last_attempt_at=CURRENT_TIMESTAMP, failure_reason=VALUES(failure_reason), failure_code=VALUES(failure_code)"
        )
        params = (
            first_name, last_name, city, state, country, patent_number, person_type,
            reason, failure_code or '', json.dumps(person), json.dumps({'stage': 'enrichment'})
        )
    cursor.execute(query, params)


def _load_failed_signatures(db_config: DatabaseConfig) -> set:
    """Load signatures for people who previously failed to enrich."""
    try:
        db_manager = DatabaseManager(db_config)
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            # Check table existence first to avoid noisy errors in context manager
            exists = False
            try:
                if db_config.engine == 'sqlite':
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='failed_enrichments'")
                    exists = bool(cursor.fetchone())
                else:
                    cursor.execute(
                        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
                        (db_config.database, 'failed_enrichments')
                    )
                    row = cursor.fetchone()
                    exists = bool(row and (row[0] if not isinstance(row, dict) else list(row.values())[0]))
            except Exception:
                exists = False
            if not exists:
                return set()
            cursor.execute(
                "SELECT first_name, last_name, city, state, patent_number, person_type FROM failed_enrichments"
            )
            rows = cursor.fetchall() or []
            failed = set()
            for r in rows:
                if isinstance(r, dict):
                    person = {
                        'first_name': r.get('first_name'), 'last_name': r.get('last_name'),
                        'city': r.get('city'), 'state': r.get('state'),
                        'patent_number': r.get('patent_number')
                    }
                else:
                    first_name, last_name, city, state, patent_number, _ptype = r
                    person = {
                        'first_name': first_name, 'last_name': last_name,
                        'city': city, 'state': state, 'patent_number': patent_number
                    }
                failed.add(_person_signature(person))
            return failed
    except Exception:
        return set()

logger = logging.getLogger(__name__)

def run_sql_data_enrichment(config: Dict[str, Any]) -> Dict[str, Any]:
    """Simple, fast enrichment process"""
    
    try:
        print("STEP 1: Loading existing enriched people from database...")
        
        # Load ALL existing enriched people into memory once
        existing_enriched = load_existing_enriched_people()
        print(f"Loaded {len(existing_enriched)} existing enriched people")
        
        print("STEP 2: Loading people to enrich...")
        
        # Load people to enrich
        people_to_enrich = load_people_to_enrich(config)
        if not people_to_enrich:
            return {
                'success': True,
                'message': 'No people to enrich',
                'total_people': 0,
                'enriched_count': 0,
                'enriched_data': [],
                'actual_api_cost': '$0.00'
            }
        
        print(f"Found {len(people_to_enrich)} people to potentially enrich")
        
        print("STEP 3: Checking for duplicates...")
        
        # Get the 19 already-enriched people from config (set by load_people_for_enrichment)
        already_enriched_from_step1 = config.get('already_enriched_people', [])
        
        # Convert already-enriched Step 1 people to enriched records by pulling from SQL
        matched_existing_for_this_run = []
        if already_enriched_from_step1:
            print(f"Processing {len(already_enriched_from_step1)} already-enriched people from Step 1...")
            db_config = DatabaseConfig.from_env()
            db_manager = DatabaseManager(db_config)
            
            for person in already_enriched_from_step1:
                first_name = (person.get('first_name') or '').strip()
                last_name = (person.get('last_name') or '').strip()
                city = (person.get('city') or '').strip()
                state = (person.get('state') or '').strip()
                
                if not first_name or not last_name:
                    continue
                    
                # Query SQL to get their enriched data
                query = """
                SELECT * FROM enriched_people 
                WHERE LOWER(TRIM(first_name)) = LOWER(%s) 
                AND LOWER(TRIM(last_name)) = LOWER(%s)
                AND LOWER(TRIM(IFNULL(city,''))) = LOWER(%s)
                AND LOWER(TRIM(IFNULL(state,''))) = LOWER(%s)
                LIMIT 1
                """
                
                rows = db_manager.execute_query(query, (first_name, last_name, city, state))
                if rows:
                    # Build enriched record from SQL data
                    row = rows[0]
                    try:
                        ed_raw = row.get('enrichment_data')
                        ed = json.loads(ed_raw) if ed_raw else {}
                    except Exception:
                        ed = {}
                    
                    enriched_record = {
                        'original_name': f"{first_name} {last_name}",
                        'patent_number': row.get('patent_number', ''),
                        'patent_title': ed.get('original_person', {}).get('patent_title', ''),
                        'match_score': ed.get('enrichment_result', {}).get('match_score', 0),
                        'enriched_data': ed,
                        'enriched_at': row.get('enriched_at')
                    }
                    matched_existing_for_this_run.append(enriched_record)
        
        # Filter out already enriched people (FAST in-memory check)
        new_people_to_enrich = []
        skipped_count = 0
        skipped_failed_count = 0
        skipped_duplicate_count = 0
        
        # Express mode: skip previously failed enrichments
        express_mode = bool(config.get('EXPRESS_MODE'))
        failed_set = set()
        if express_mode:
            print("Express mode enabled: loading failed enrichments to skip...")
            failed_set = _load_failed_signatures(DatabaseConfig.from_env())
            print(f"Loaded {len(failed_set)} failed signatures to skip in express mode")
        
        for person in people_to_enrich:
            if express_mode and _person_signature(person) in failed_set:
                skipped_failed_count += 1
                skipped_count += 1
                continue
            
            # Check if this person is already enriched
            match = find_existing_enriched_match(person, existing_enriched)
            if match is not None:
                skipped_duplicate_count += 1
                skipped_count += 1
                # Note: We don't add to matched_existing here because these weren't from Step 1
            else:
                new_people_to_enrich.append(person)

        # Detailed summary of filtering
        print(
            "After duplicate check: "
            f"{len(new_people_to_enrich)} new people, "
            f"{skipped_duplicate_count} duplicates, "
            f"{skipped_failed_count} skipped (previously failed), "
            f"{skipped_count} total skipped"
        )
        
        # Combine all enriched data for return - existing + newly enriched + matched from step 1
        if existing_enriched and matched_existing_for_this_run:
            all_enriched_data = existing_enriched + matched_existing_for_this_run
        elif existing_enriched:
            all_enriched_data = existing_enriched
        elif matched_existing_for_this_run:
            all_enriched_data = matched_existing_for_this_run
        else:
            all_enriched_data = []
        
        if not new_people_to_enrich:
            return {
                'success': True,
                'message': 'All people already enriched or failed',
                'total_people': len(people_to_enrich) + len(already_enriched_from_step1),
                'enriched_count': 0,
                'enriched_data': all_enriched_data,
                'newly_enriched_data': [],
                'matched_existing': matched_existing_for_this_run,  # The 19 people from Step 1
                'actual_api_cost': '$0.00',
                'api_calls_saved': len(people_to_enrich) + len(already_enriched_from_step1),
                'already_enriched_count': skipped_duplicate_count,
                'skipped_failed_count': skipped_failed_count
            }
        
        # Limit for test mode (hard cap to 5 people)
        if bool(config.get('TEST_MODE')) and len(new_people_to_enrich) > 5:
            new_people_to_enrich = new_people_to_enrich[:5]
            print(f"TEST MODE: Limited to {len(new_people_to_enrich)} people")
        
        print(f"STEP 4: Enriching {len(new_people_to_enrich)} people...")

        # Initialize simple live progress (works with UI poller)
        progress_path = Path(config.get('OUTPUT_DIR', 'output')) / 'step2_progress.json'
        try:
            with open(progress_path, 'w') as pf:
                json.dump({
                    'step': 2,
                    'total': int(len(new_people_to_enrich) + skipped_count + len(already_enriched_from_step1)),
                    'processed': 0,
                    'newly_enriched': 0,
                    'already_enriched': int(skipped_count + len(already_enriched_from_step1)),
                    'stage': 'starting_enrichment',
                    'started_at': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }, pf)
        except Exception:
            pass

        # Enrich the new people in a single pass (original faster flow)
        newly_enriched = enrich_people_batch(new_people_to_enrich, config, progress={
            'path': str(progress_path),
            'total': int(len(new_people_to_enrich) + skipped_count + len(already_enriched_from_step1)),
            'skipped': int(skipped_count + len(already_enriched_from_step1))
        })
        
        # Note: Each enrichment is now saved to SQL inside the loop
        print(f"STEP 5: Saved {len(newly_enriched)} enrichments during processing")
        
        # Combine all enriched data for return - existing + newly enriched + matched from step 1
        if existing_enriched and newly_enriched and matched_existing_for_this_run:
            all_enriched_data = existing_enriched + newly_enriched + matched_existing_for_this_run
        elif existing_enriched and newly_enriched:
            all_enriched_data = existing_enriched + newly_enriched
        elif existing_enriched and matched_existing_for_this_run:
            all_enriched_data = existing_enriched + matched_existing_for_this_run
        elif newly_enriched and matched_existing_for_this_run:
            all_enriched_data = newly_enriched + matched_existing_for_this_run
        elif existing_enriched:
            all_enriched_data = existing_enriched
        elif newly_enriched:
            all_enriched_data = newly_enriched
        elif matched_existing_for_this_run:
            all_enriched_data = matched_existing_for_this_run
        else:
            all_enriched_data = []
        
        result = {
            'success': True,
            'total_people': len(people_to_enrich) + len(already_enriched_from_step1),
            'enriched_count': len(newly_enriched),
            'enrichment_rate': len(newly_enriched) / len(new_people_to_enrich) * 100 if new_people_to_enrich else 0,
            # Full snapshot from SQL + this run (for reporting)
            'enriched_data': all_enriched_data,
            # Only records created in this run (for local file append semantics)
            'newly_enriched_data': newly_enriched,
            # Existing matches from this run (the 19 people from Step 1)
            'matched_existing': matched_existing_for_this_run,
            'actual_api_cost': f"${len(newly_enriched) * 0.03:.2f}",
            'api_calls_saved': skipped_count + len(already_enriched_from_step1),
            'already_enriched_count': skipped_duplicate_count,
            'skipped_failed_count': skipped_failed_count,
            'failed_count': len(new_people_to_enrich) - len(newly_enriched)
        }
        
        print(f"STEP 6: Function completed successfully!")
        print(f"  Returning {len(all_enriched_data)} total enriched records")
        print(f"  Newly enriched: {len(newly_enriched)}")
        print(f"  Matched existing from Step 1: {len(matched_existing_for_this_run)}")
        print(f"  Result keys: {list(result.keys())}")
        
        return result
        
    except Exception as e:
        logger.error(f"Enrichment failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'total_people': 0,
            'enriched_count': 0,
            'enriched_data': [],
            'actual_api_cost': '$0.00'
        }
    
def load_existing_enriched_people() -> List[Dict[str, Any]]:
    """Load all existing enriched people from database in one query.
    Be tolerant of schema differences (e.g., missing mail_to_add1) by probing columns and building a safe SELECT.
    """
    try:
        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)

        # Discover available columns on existing_people
        cols = []
        try:
            col_rows = db_manager.execute_query("SHOW COLUMNS FROM existing_people") or []
            cols = [r.get('Field') or r.get('COLUMN_NAME') or r.get('field') for r in col_rows if isinstance(r, dict)]
        except Exception:
            cols = []

        def _pick(want: str, alts: List[str] = None) -> str:
            alts = alts or []
            if want in cols:
                return want
            for a in alts:
                if a in cols:
                    return a
            return ''

        # Map to aliases expected by downstream code
        mapping = {
            'issue_id': _pick('issue_id'),
            'new_issue_rec_num': _pick('new_issue_rec_num', ['issue_rec_num','rec_num']),
            'inventor_id': _pick('inventor_id'),
            'patent_no': _pick('patent_no', ['patent_number','patent_num']),
            'title': _pick('title', ['patent_title','invention_title']),
            'issue_date': _pick('issue_date', ['date','patent_date']),
            'bar_code': _pick('bar_code', ['barcode']),
            'mod_user': _pick('mod_user', ['modified_by','last_modified_by']),
            'mail_to_assignee': _pick('mail_to_assignee', ['assignee','assign_name']),
            'mail_to_name': _pick('mail_to_name'),
            'mail_to_add1': _pick('mail_to_add1', ['address','addr1','mail_to_add_1'])
        }

        select_parts = []
        for alias, col in mapping.items():
            if not col:
                continue
            if col == alias:
                select_parts.append(f"ex.{col}")
            else:
                select_parts.append(f"ex.{col} AS {alias}")
        select_clause = ', '.join(select_parts) if select_parts else ''

        query = (
            f"SELECT ep.*{(', ' + select_clause) if select_clause else ''} "
            "FROM enriched_people ep "
            "LEFT JOIN existing_people ex ON ep.first_name = ex.first_name AND ep.last_name = ex.last_name "
            "AND IFNULL(ep.city,'') = IFNULL(ex.city,'') AND IFNULL(ep.state,'') = IFNULL(ex.state,'') "
            "ORDER BY ep.enriched_at DESC"
        )
        results = db_manager.execute_query(query)
        
        enriched_data = []
        parse_errors: List[Tuple[Any, str]] = []
        for row in results:
            try:
                # Parse JSON data
                enrichment_data = json.loads(row.get('enrichment_data', '{}'))
                
                # Convert to standard format
                enriched_record = {
                    'original_name': f"{row.get('first_name', '')} {row.get('last_name', '')}".strip(),
                    'patent_number': row.get('patent_number', ''),
                    'patent_title': enrichment_data.get('original_person', {}).get('patent_title', ''),
                    'match_score': enrichment_data.get('enrichment_result', {}).get('match_score', 0),
                    'enriched_data': enrichment_data,
                    'enriched_at': row.get('enriched_at'),
                    # propagate selected existing_people fields when available
                    'issue_id': row.get('issue_id'),
                    'new_issue_rec_num': row.get('new_issue_rec_num'),
                    'inventor_id': row.get('inventor_id'),
                    'patent_no': row.get('patent_no'),
                    'title': row.get('title'),
                    'issue_date': row.get('issue_date'),
                    'bar_code': row.get('bar_code'),
                    'mod_user': row.get('mod_user'),
                    'mail_to_assignee': row.get('mail_to_assignee'),
                    'mail_to_name': row.get('mail_to_name'),
                    'mail_to_add1': row.get('mail_to_add1') or row.get('address')
                }
                enriched_data.append(enriched_record)
                
            except Exception as e:
                parse_errors.append((row.get('id'), str(e)))
                continue

        if parse_errors:
            total_errors = len(parse_errors)
            unique_rows = len({row_id for row_id, _ in parse_errors})
            sample_row, sample_error = parse_errors[0]
            logger.warning(
                "Encountered %d parse errors across %d rows while loading enrichment data; "
                "first example row %s: %s",
                total_errors,
                unique_rows,
                sample_row,
                sample_error,
            )
        
        # Fallback: if DB has no enriched rows, try local snapshot to preserve duplicate protection
        if not enriched_data:
            try:
                snap_path = Path('output') / 'enriched_patents.json'
                if snap_path.exists() and snap_path.stat().st_size > 0:
                    with open(snap_path, 'r') as f:
                        local_data = json.load(f)
                    if isinstance(local_data, list):
                        return local_data
            except Exception:
                pass
        return enriched_data
        
    except Exception as e:
        logger.warning(f"Error loading existing enriched people: {e}")
        return []


def load_people_to_enrich(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Load people who need enrichment"""
    people = []
    
    # Try from config first
    if config.get('new_people_data'):
        people = config['new_people_data']
        print(f"Using {len(people)} people from config")
        return people
    
    # Try from file
    people_file = Path(config.get('OUTPUT_DIR', 'output')) / 'new_people_for_enrichment.json'
    if people_file.exists():
        with open(people_file, 'r') as f:
            people = json.load(f)
        print(f"Loaded {len(people)} people from file")
    
    return people


def is_already_enriched(person: Dict[str, Any], existing_enriched: List[Dict[str, Any]]) -> bool:
    """Check if person is already enriched - FAST in-memory lookup"""
    
    first_name = (person.get('first_name') or '').strip().lower()
    last_name = (person.get('last_name') or '').strip().lower() 
    city = (person.get('city') or '').strip().lower()
    state = (person.get('state') or '').strip().lower()
    
    if not first_name and not last_name:
        return False
    
    # Check against existing enriched data
    for existing in existing_enriched:
        existing_data = existing.get('enriched_data', {}).get('original_person', {})
        
        existing_first = (existing_data.get('first_name') or '').strip().lower()
        existing_last = (existing_data.get('last_name') or '').strip().lower()
        existing_city = (existing_data.get('city') or '').strip().lower()
        existing_state = (existing_data.get('state') or '').strip().lower()
        
        # Exact match (highest confidence)
        if (first_name and last_name and city and state and
            first_name == existing_first and last_name == existing_last and
            city == existing_city and state == existing_state):
            return True
        
        # State match (medium confidence)
        if (first_name and last_name and state and
            first_name == existing_first and last_name == existing_last and
            state == existing_state):
            return True
        
        # First initial match (lower confidence)
        if (first_name and last_name and state and existing_first and
            first_name[0] == existing_first[0] and last_name == existing_last and
            state == existing_state):
            return True
    
    return False


def find_existing_enriched_match(person: Dict[str, Any], existing_enriched: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return the existing enriched record if already enriched, else None."""
    first_name = (person.get('first_name') or '').strip().lower()
    last_name = (person.get('last_name') or '').strip().lower()
    city = (person.get('city') or '').strip().lower()
    state = (person.get('state') or '').strip().lower()
    if not first_name and not last_name:
        return None
    for existing in existing_enriched:
        existing_data = existing.get('enriched_data', {}).get('original_person', {})
        existing_first = (existing_data.get('first_name') or '').strip().lower()
        existing_last = (existing_data.get('last_name') or '').strip().lower()
        existing_city = (existing_data.get('city') or '').strip().lower()
        existing_state = (existing_data.get('state') or '').strip().lower()
        if (first_name and last_name and city and state and
            first_name == existing_first and last_name == existing_last and
            city == existing_city and state == existing_state):
            return existing
        if (first_name and last_name and state and
            first_name == existing_first and last_name == existing_last and
            state == existing_state):
            return existing
        if (first_name and last_name and state and existing_first and
            first_name[0] == existing_first[0] and last_name == existing_last and
            state == existing_state):
            return existing
    return None


def enrich_people_batch(people: List[Dict[str, Any]], config: Dict[str, Any], progress: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Enrich a batch of people"""
    
    api_key = config.get('PEOPLEDATALABS_API_KEY')
    enriched_results = []
    
    # Initialize enricher – require a valid API key (no mock paths)
    if not api_key or api_key == 'YOUR_PDL_API_KEY':
        raise RuntimeError("PEOPLEDATALABS_API_KEY is missing. Mock enrichment is disabled.")
    print(f"Using real API with key: {api_key[:10]}...")
    try:
        enricher = PeopleDataLabsEnricher(api_key)
    except Exception as e:
        raise RuntimeError(f"Failed to initialize PDL enricher: {e}")
    
    # Prepare database connection for per-record saves
    db_manager = None
    conn_ctx = None
    conn = None
    cursor = None
    try:
        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)
        # Open a single connection for the loop
        conn_ctx = db_manager.get_connection()
        conn = conn_ctx.__enter__()
        cursor = conn.cursor()
        try:
            _ensure_failed_table(conn, db_config.engine)
        except Exception:
            pass
    except Exception as e:
        logger.warning(f"Could not open DB connection for per-record saves: {e}")
        db_manager = None
        conn = None
        cursor = None

    # Single-iteration flow only (bulk disabled)

    # Progress helpers
    processed_counter = 0
    new_added_counter = 0
    def write_progress_safely():
        if not progress:
            return
        try:
            payload = {
                'step': 2,
                'total': int(progress.get('total', len(people))),
                'processed': int(progress.get('skipped', 0)) + processed_counter,
                'newly_enriched': new_added_counter,
                'already_enriched': int(progress.get('skipped', 0)),
                'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ')
            }
            with open(progress.get('path'), 'w') as pf:
                json.dump(payload, pf)
        except Exception:
            pass

    for i, person in enumerate(people):
        # Secondary safety: enforce test mode cap inside the loop
        if bool(config.get('TEST_MODE')) and i >= 5:
            break
        progress = i + 1
        total = len(people)
        person_name = f"{person.get('first_name', '')} {person.get('last_name', '')}"
        
        print(f"ENRICHING {progress}/{total}: {person_name}")
        print(f"  Person data: first_name='{person.get('first_name')}', last_name='{person.get('last_name')}', city='{person.get('city')}', state='{person.get('state')}'")
        
        try:
            # Secondary guard: check DB directly for existing enrichment to avoid duplicate API calls
            try:
                if cursor is not None and conn is not None:
                    fn = (person.get('first_name') or '').strip()
                    ln = (person.get('last_name') or '').strip()
                    st = (person.get('state') or '').strip()
                    ct = (person.get('city') or '').strip()
                    pn = (person.get('patent_number') or '').strip()
                    # Try with city + patent when available
                    params = [fn, ln, st, ct]
                    query = (
                        "SELECT 1 FROM enriched_people WHERE first_name=%s AND last_name=%s "
                        "AND IFNULL(state,'')=%s AND IFNULL(city,'')=%s"
                    )
                    if pn:
                        query += " AND IFNULL(patent_number,'')=%s"
                        params.append(pn)
                    query += " LIMIT 1"
                    cursor.execute(query, tuple(params))
                    hit = cursor.fetchone()
                    if not hit and ct:
                        # Retry ignoring city
                        params2 = [fn, ln, st]
                        query2 = (
                            "SELECT 1 FROM enriched_people WHERE first_name=%s AND last_name=%s "
                            "AND IFNULL(state,'')=%s"
                        )
                        if pn:
                            query2 += " AND IFNULL(patent_number,'')=%s"
                            params2.append(pn)
                        query2 += " LIMIT 1"
                        cursor.execute(query2, tuple(params2))
                        hit = cursor.fetchone()
                    if hit:
                        print("  Skipping: already enriched in DB")
                        processed_counter += 1
                        write_progress_safely()
                        continue
            except Exception:
                # On any DB check error, proceed to API path
                pass
            # Real API path only – clean person data
            clean_person = {
                'first_name': str(person.get('first_name', '')).strip(),
                'last_name': str(person.get('last_name', '')).strip(),
                'city': str(person.get('city', '')).strip(),
                'state': str(person.get('state', '')).strip(),
                'country': str(person.get('country', 'US')).strip(),
                'patent_number': str(person.get('patent_number', '')),
                'patent_title': str(person.get('patent_title', '')),
                'person_type': str(person.get('person_type', 'inventor'))
            }
            
            result = enricher.enrich_people_list([clean_person])
            enrichment_result = result[0] if (result and len(result) > 0) else None
            
            # Verbose per-person debug in TEST MODE
            try:
                if bool(config.get('TEST_MODE')):
                    def _bool_presence(pdl: Dict[str, Any]) -> bool:
                        try:
                            if not isinstance(pdl, dict):
                                return False
                            keys = [
                                'location_street_address','location_postal_code',
                                'job_company_location_street_address','job_company_location_postal_code',
                                'street_addresses'
                            ]
                            for k in keys:
                                v = pdl.get(k)
                                if isinstance(v, bool):
                                    return True
                            return False
                        except Exception:
                            return False
                    if enrichment_result is None:
                        print("  DEBUG: No enrichment result (None)")
                    else:
                        ed = enrichment_result.get('enriched_data', {})
                        pdl = ed.get('pdl_data', {})
                        method = ed.get('api_method', 'unknown')
                        api_raw = enrichment_result.get('api_raw', {}) or {}
                        likelihood = None
                        matches = None
                        best_score = None
                        if isinstance(api_raw.get('enrichment'), dict):
                            likelihood = api_raw.get('enrichment', {}).get('likelihood')
                        if isinstance(api_raw.get('identify'), dict):
                            try:
                                matches = len(api_raw.get('identify', {}).get('matches') or [])
                                if matches:
                                    best_score = (api_raw.get('identify', {}).get('matches')[0] or {}).get('match_score')
                            except Exception:
                                pass
                        presence = _bool_presence(pdl)
                        print(f"  DEBUG: Method={method} Likelihood={likelihood} IdentifyMatches={matches} BestScore={best_score} PresenceAddr={presence}")
            except Exception:
                pass

            if enrichment_result is not None:
                enriched_results.append(enrichment_result)
                new_added_counter += 1
                # Save immediately to SQL per record if possible
                try:
                    if cursor is not None and conn is not None:
                        _save_single_enrichment(cursor, enrichment_result)
                        conn.commit()
                        if bool(config.get('TEST_MODE')):
                            print("  DEBUG: Saved enrichment to SQL")
                except Exception as e:
                    logger.error(f"  Error saving enrichment for {person_name}: {e}")
                    if bool(config.get('TEST_MODE')):
                        print(f"  DEBUG: Save error: {e}")
            else:
                # Record failure (no enrichment result)
                try:
                    if cursor is not None and conn is not None:
                        # Use cleaned person when available
                        _record_failed_enrichment(cursor, db_config.engine if 'db_config' in locals() else 'mysql', clean_person, 'not_found', None)
                        conn.commit()
                        if bool(config.get('TEST_MODE')):
                            print("  DEBUG: Recorded failed enrichment in failed_enrichments")
                except Exception as e:
                    logger.warning(f"  Could not record failed enrichment for {person_name}: {e}")
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            # Record exception as failed enrichment
            try:
                if cursor is not None and conn is not None:
                    _record_failed_enrichment(cursor, db_config.engine if 'db_config' in locals() else 'mysql', person, f'exception: {str(e)}', None)
                    conn.commit()
            except Exception:
                pass
        
        # Small delay to be nice to API
        time.sleep(0.1)
        processed_counter += 1
        write_progress_safely()
    
    # Clean up DB connection context manager
    try:
        if conn_ctx is not None:
            # Exit the context manager if we manually entered it
            conn_ctx.__exit__(None, None, None)
    except Exception:
        pass

    print(f"Enriched {len(enriched_results)} out of {len(people)} people")
    return enriched_results


def _save_single_enrichment(cursor, result: Dict[str, Any]):
    """Save a single enrichment result using an existing cursor."""
    # Extract data
    original_data = result.get('enriched_data', {}).get('original_data', {})
    if not original_data:
        # Support alternate key name from API variations
        original_data = result.get('enriched_data', {}).get('original_person', {})
    # Optionally backfill key fields from existing_people for formatted exports
    existing_record = {}
    try:
        fn = (original_data.get('first_name') or '').strip()
        ln = (original_data.get('last_name') or '').strip()
        ct = (original_data.get('city') or '').strip()
        st = (original_data.get('state') or '').strip()
        # Pull a few columns we need for formatted CSV; support fallbacks for address/zip via SQL
        select_cols = (
            "inventor_id, mod_user, title, patent_no, mail_to_add1, mail_to_zip, address, zip"
        )
        def _normalize_row(r):
            if not r:
                return {}
            if isinstance(r, dict):
                return {
                    'inventor_id': r.get('inventor_id'),
                    'mod_user': r.get('mod_user'),
                    'title': r.get('title'),
                    'patent_no': r.get('patent_no'),
                    # prefer explicit mail_to_add1/zip, fallback to address/zip
                    'mail_to_add1': (r.get('mail_to_add1') or r.get('address') or ''),
                    'mail_to_zip': (r.get('mail_to_zip') or r.get('zip') or '')
                }
            cols = ['inventor_id','mod_user','title','patent_no','mail_to_add1','mail_to_zip','address','zip']
            out = { c: (r[i] if i < len(r) else None) for i, c in enumerate(cols) }
            out['mail_to_add1'] = out.get('mail_to_add1') or out.get('address') or ''
            out['mail_to_zip'] = out.get('mail_to_zip') or out.get('zip') or ''
            return { k: out.get(k) for k in ['inventor_id','mod_user','title','patent_no','mail_to_add1','mail_to_zip'] }

        # Try existing_people (exact match including city)
        q1 = (
            f"SELECT {select_cols} FROM existing_people "
            "WHERE first_name=%s AND last_name=%s AND IFNULL(city,'')=%s AND IFNULL(state,'')=%s LIMIT 1"
        )
        cursor.execute(q1, (fn, ln, ct, st))
        row = cursor.fetchone()
        if not row:
            # Try existing_people_new (exact)
            q1b = (
                f"SELECT {select_cols} FROM existing_people_new "
                "WHERE first_name=%s AND last_name=%s AND IFNULL(city,'')=%s AND IFNULL(state,'')=%s LIMIT 1"
            )
            try:
                cursor.execute(q1b, (fn, ln, ct, st))
                row = cursor.fetchone()
            except Exception:
                row = None
        if not row:
            # Fallback: existing_people ignoring city
            q2 = (
                f"SELECT {select_cols} FROM existing_people "
                "WHERE first_name=%s AND last_name=%s AND IFNULL(state,'')=%s LIMIT 1"
            )
            cursor.execute(q2, (fn, ln, st))
            row = cursor.fetchone()
        if not row:
            # Fallback: existing_people_new ignoring city
            q2b = (
                f"SELECT {select_cols} FROM existing_people_new "
                "WHERE first_name=%s AND last_name=%s AND IFNULL(state,'')=%s LIMIT 1"
            )
            try:
                cursor.execute(q2b, (fn, ln, st))
                row = cursor.fetchone()
            except Exception:
                row = None

        existing_record = _normalize_row(row)
    except Exception:
        # Non-fatal: enrichment proceeds even if backfill fails
        existing_record = {}

    # Derive mail_to_add1 and mail_to_zip from PDL when available
    def _pick_pdl_street(pdl: Dict[str, Any]) -> str:
        if not isinstance(pdl, dict):
            return ''
        vals = [
            pdl.get('job_company_location_street_address'),
            pdl.get('location_street_address')
        ]
        for v in vals:
            if v and str(v).strip():
                return str(v).strip()
        try:
            sa = (pdl.get('street_addresses') or [])
            if isinstance(sa, list) and sa:
                first = sa[0] or {}
                v = first.get('street_address') or first.get('formatted_address')
                if v and str(v).strip():
                    return str(v).strip()
        except Exception:
            pass
        # Try company experience location
        try:
            exp = pdl.get('experience')
            if isinstance(exp, list) and exp:
                # Prefer primary, then scan all experiences for any street_address
                primary = next((e for e in exp if e and e.get('is_primary')), None)
                if primary:
                    loc = ((primary.get('company') or {}).get('location') or {})
                    v = loc.get('street_address') or loc.get('address_line_2')
                    if v and str(v).strip():
                        return str(v).strip()
                # Scan all entries for the first with a street_address
                for e in exp:
                    try:
                        loc = ((e.get('company') or {}).get('location') or {})
                        v = loc.get('street_address') or loc.get('address_line_2')
                        if v and str(v).strip():
                            return str(v).strip()
                    except Exception:
                        continue
        except Exception:
            pass
        return ''

    def _pick_pdl_zip(pdl: Dict[str, Any]) -> str:
        if not isinstance(pdl, dict):
            return ''
        vals = [
            pdl.get('job_company_location_postal_code'),
            pdl.get('location_postal_code')
        ]
        for v in vals:
            if v and str(v).strip():
                return str(v).strip()
        try:
            sa = (pdl.get('street_addresses') or [])
            if isinstance(sa, list) and sa:
                first = sa[0] or {}
                v = first.get('postal_code')
                if v and str(v).strip():
                    return str(v).strip()
        except Exception:
            pass
        # Try company experience location
        try:
            exp = pdl.get('experience')
            if isinstance(exp, list) and exp:
                # Prefer primary, then scan all experiences for any postal_code
                primary = next((e for e in exp if e and e.get('is_primary')), None)
                if primary:
                    loc = ((primary.get('company') or {}).get('location') or {})
                    v = loc.get('postal_code')
                    if v and str(v).strip():
                        return str(v).strip()
                for e in exp:
                    try:
                        loc = ((e.get('company') or {}).get('location') or {})
                        v = loc.get('postal_code')
                        if v and str(v).strip():
                            return str(v).strip()
                    except Exception:
                        continue
        except Exception:
            pass
        return ''

    pdl_data = (result.get('enriched_data') or {}).get('pdl_data') or {}
    pdl_street = _pick_pdl_street(pdl_data)
    pdl_zip = _pick_pdl_zip(pdl_data)

    enrichment_data = {
        "original_person": original_data,
        "enrichment_result": result,
        "enrichment_metadata": {
            "enriched_at": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "api_cost": 0.03
        },
        # Persist selected existing_people fields for reliable formatted exports later
        "existing_record": {
            **(existing_record or {}),
            # Fill address fields from PDL where not already present
            "mail_to_add1": (existing_record or {}).get('mail_to_add1') or pdl_street or '',
            "mail_to_zip": (existing_record or {}).get('mail_to_zip') or pdl_zip or ''
        }
    }
    insert_query = """
        INSERT INTO enriched_people (
            first_name, last_name, city, state, country,
            patent_number, person_type, enrichment_data, api_cost
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        (original_data.get('first_name') or '').strip(),
        (original_data.get('last_name') or '').strip(),
        (original_data.get('city') or '').strip(),
        (original_data.get('state') or '').strip(),
        (original_data.get('country') or 'US').strip(),
        original_data.get('patent_number', ''),
        original_data.get('person_type', 'inventor'),
        json.dumps(enrichment_data),
        0.03
    )
    cursor.execute(insert_query, params)
    # Optional debug logging
    try:
        if os.environ.get('ENRICH_DEBUG', 'false').lower() == 'true':
            filled = [k for k,v in (existing_record or {}).items() if str(v or '').strip() != '']
            if filled:
                print(f"ENRICH DEBUG: backfilled existing_record fields -> {filled}")
    except Exception:
        pass


def save_enrichments_to_database(enriched_results: List[Dict[str, Any]]):
    """Save new enrichments to database"""
    try:
        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)
        
        for result in enriched_results:
            try:
                # Extract data
                original_data = result.get('enriched_data', {}).get('original_data', {})
                
                # Build enrichment data JSON
                enrichment_data = {
                    "original_person": original_data,
                    "enrichment_result": result,
                    "enrichment_metadata": {
                        "enriched_at": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        "api_cost": 0.03
                    }
                }
                
                # Insert query
                insert_query = """
                INSERT INTO enriched_people (
                    first_name, last_name, city, state, country,
                    patent_number, person_type, enrichment_data, api_cost
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                params = (
                    (original_data.get('first_name') or '').strip(),
                    (original_data.get('last_name') or '').strip(),
                    (original_data.get('city') or '').strip(),
                    (original_data.get('state') or '').strip(),
                    (original_data.get('country') or 'US').strip(),
                    original_data.get('patent_number', ''),
                    original_data.get('person_type', 'inventor'),
                    json.dumps(enrichment_data),
                    0.03
                )
                
                with db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(insert_query, params)
                    conn.commit()
                
                print(f"  Saved: {result.get('original_name', 'Unknown')}")
                
            except Exception as e:
                logger.error(f"Error saving enrichment: {e}")
                continue
        
        print(f"Saved {len(enriched_results)} enrichments to database")
        
    except Exception as e:
        logger.error(f"Error saving to database: {e}")
