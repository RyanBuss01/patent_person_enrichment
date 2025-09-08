# =============================================================================
# runners/enrich.py - SIMPLE AND FAST
# Step 3: Data Enrichment using PeopleDataLabs API
# =============================================================================
import logging
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
        
        # Filter out already enriched people (FAST in-memory check)
        new_people_to_enrich = []
        skipped_count = 0
        skipped_failed_count = 0
        skipped_duplicate_count = 0
        matched_existing_for_this_run: List[Dict[str, Any]] = []
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
            match = find_existing_enriched_match(person, existing_enriched)
            if match is not None:
                skipped_duplicate_count += 1
                skipped_count += 1
                matched_existing_for_this_run.append(match)
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
        
        if not new_people_to_enrich:
            return {
                'success': True,
                'message': 'All people already enriched',
                'total_people': len(people_to_enrich),
                'enriched_count': 0,
                'enriched_data': existing_enriched,
                'matched_existing': matched_existing_for_this_run,
                'actual_api_cost': '$0.00',
                'api_calls_saved': len(people_to_enrich),
                'already_enriched_count': skipped_duplicate_count,
                'skipped_failed_count': skipped_failed_count
            }
        
        # Limit for test mode (hard cap to 2 people)
        if bool(config.get('TEST_MODE')) and len(new_people_to_enrich) > 2:
            new_people_to_enrich = new_people_to_enrich[:2]
            print(f"TEST MODE: Limited to {len(new_people_to_enrich)} people")
        
        print(f"STEP 4: Enriching {len(new_people_to_enrich)} people...")

        # Initialize simple live progress (works with UI poller)
        progress_path = Path(config.get('OUTPUT_DIR', 'output')) / 'step2_progress.json'
        try:
            with open(progress_path, 'w') as pf:
                json.dump({
                    'step': 2,
                    'total': int(len(new_people_to_enrich) + skipped_count),
                    'processed': 0,
                    'newly_enriched': 0,
                    'already_enriched': int(skipped_count),
                    'stage': 'starting_enrichment',
                    'started_at': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }, pf)
        except Exception:
            pass

        # Enrich the new people in a single pass (original faster flow)
        newly_enriched = enrich_people_batch(new_people_to_enrich, config, progress={
            'path': str(progress_path),
            'total': int(len(new_people_to_enrich) + skipped_count),
            'skipped': int(skipped_count)
        })
        
        # Note: Each enrichment is now saved to SQL inside the loop
        print(f"STEP 5: Saved {len(newly_enriched)} enrichments during processing")
        
        # Combine all enriched data for return - fix the join error here
        if existing_enriched and newly_enriched:
            all_enriched_data = existing_enriched + newly_enriched
        elif existing_enriched:
            all_enriched_data = existing_enriched
        elif newly_enriched:
            all_enriched_data = newly_enriched
        else:
            all_enriched_data = []
        
        result = {
            'success': True,
            'total_people': len(people_to_enrich),
            'enriched_count': len(newly_enriched),
            'enrichment_rate': len(newly_enriched) / len(new_people_to_enrich) * 100 if new_people_to_enrich else 0,
            # Full snapshot from SQL + this run (for reporting)
            'enriched_data': all_enriched_data,
            # Only records created in this run (for local file append semantics)
            'newly_enriched_data': newly_enriched,
            # Existing matches from this run (came from SQL)
            'matched_existing': matched_existing_for_this_run,
            'actual_api_cost': f"${len(newly_enriched) * 0.03:.2f}",
            'api_calls_saved': skipped_count,
            'already_enriched_count': skipped_duplicate_count,
            'skipped_failed_count': skipped_failed_count,
            'failed_count': len(new_people_to_enrich) - len(newly_enriched)
        }
        
        print(f"STEP 6: Function completed successfully!")
        print(f"  Returning {len(all_enriched_data)} total enriched records")
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
    """Load all existing enriched people from database in one query"""
    try:
        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)
        
        query = "SELECT * FROM enriched_people ORDER BY enriched_at DESC"
        results = db_manager.execute_query(query)
        
        enriched_data = []
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
                    'enriched_at': row.get('enriched_at')
                }
                enriched_data.append(enriched_record)
                
            except Exception as e:
                logger.warning(f"Error parsing enriched row {row.get('id')}: {e}")
                continue
        
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
        if (first_name and last_name and state and
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
        if (first_name and last_name and state and
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
        if bool(config.get('TEST_MODE')) and i >= 2:
            break
        progress = i + 1
        total = len(people)
        person_name = f"{person.get('first_name', '')} {person.get('last_name', '')}"
        
        print(f"ENRICHING {progress}/{total}: {person_name}")
        print(f"  Person data: first_name='{person.get('first_name')}', last_name='{person.get('last_name')}', city='{person.get('city')}', state='{person.get('state')}'")
        
        try:
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
            
            if enrichment_result is not None:
                enriched_results.append(enrichment_result)
                new_added_counter += 1
                # Save immediately to SQL per record if possible
                try:
                    if cursor is not None and conn is not None:
                        _save_single_enrichment(cursor, enrichment_result)
                        conn.commit()
                except Exception as e:
                    logger.error(f"  Error saving enrichment for {person_name}: {e}")
            else:
                # Record failure (no enrichment result)
                try:
                    if cursor is not None and conn is not None:
                        # Use cleaned person when available
                        _record_failed_enrichment(cursor, db_config.engine if 'db_config' in locals() else 'mysql', clean_person, 'not_found', None)
                        conn.commit()
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
    enrichment_data = {
        "original_person": original_data,
        "enrichment_result": result,
        "enrichment_metadata": {
            "enriched_at": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "api_cost": 0.03
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
