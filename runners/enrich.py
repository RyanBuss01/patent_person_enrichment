# =============================================================================
# runners/enrich.py - SIMPLE AND FAST
# Step 3: Data Enrichment using PeopleDataLabs API
# =============================================================================
import logging
import os
import json
import time
import pandas as pd
from typing import Dict, Any, List, Tuple, Optional, Set, Iterable
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
        total_steps = 6
        output_dir = Path(config.get('OUTPUT_DIR', 'output'))
        stage_path = output_dir / 'step2_stage.json'

        try:
            stage_path.unlink()
        except FileNotFoundError:
            pass
        except Exception:
            pass

        def _set_stage(step_number: int, label: str, extra: Optional[Dict[str, Any]] = None, log: bool = True) -> None:
            payload = {
                'current_step': int(step_number),
                'total_steps': int(total_steps),
                'stage_label': label,
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ')
            }
            if extra:
                payload.update(extra)
            try:
                with stage_path.open('w') as sf:
                    json.dump(payload, sf)
            except Exception:
                pass
            if log:
                print(f"STEP {step_number}/{total_steps}: {label}")

        _set_stage(1, "Loading people to enrich")
        people_to_enrich = load_people_to_enrich(config)
        if not people_to_enrich:
            _set_stage(total_steps, "Completed (no people to enrich)")
            return {
                'success': True,
                'message': 'No people to enrich',
                'total_people': 0,
                'enriched_count': 0,
                'enriched_data': [],
                'actual_api_cost': '$0.00'
            }

        print(f"Found {len(people_to_enrich)} people to potentially enrich")

        _set_stage(2, "Preparing existing enrichment lookup")
        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)
        lookup = EnrichedPeopleLookup(db_manager)

        # Already enriched people that Step 1 filtered out (we want to carry them forward)
        already_enriched_from_step1 = config.get('already_enriched_people', [])
        lookup.prefetch_people(already_enriched_from_step1)
        matched_existing_ids: List[int] = []
        matched_signatures: set = set()
        if already_enriched_from_step1:
            total_existing = len(already_enriched_from_step1)
            print(f"Processing {total_existing} already-enriched people from Step 1...")
            for idx, person in enumerate(already_enriched_from_step1, start=1):
                match_id = lookup.find_matching_id(person)
                if match_id:
                    sig = _person_signature(person)
                    if sig not in matched_signatures:
                        matched_existing_ids.append(match_id)
                        matched_signatures.add(sig)
                if idx % 25 == 0 or idx == total_existing:
                    print(
                        f"PROGRESS: Matching Step1 existing {idx}/{total_existing}"
                    )
        matched_existing_for_this_run = lookup.get_records_by_ids(matched_existing_ids)

        _set_stage(3, "Checking for duplicates")

        new_people_to_enrich: List[Dict[str, Any]] = []
        skipped_count = 0
        skipped_failed_count = 0
        skipped_duplicate_count = 0

        express_mode = bool(config.get('EXPRESS_MODE'))
        failed_set = set()
        if express_mode:
            print("Express mode enabled: loading failed enrichments to skip...")
            failed_set = _load_failed_signatures(db_config)
            print(f"Loaded {len(failed_set)} failed signatures to skip in express mode")

        lookup.prefetch_people(people_to_enrich)

        total_people_to_enrich = len(people_to_enrich)
        for idx, person in enumerate(people_to_enrich, start=1):
            if express_mode and _person_signature(person) in failed_set:
                skipped_failed_count += 1
                skipped_count += 1
                continue

            if lookup.find_best_match(person, require_record=False):
                skipped_duplicate_count += 1
                skipped_count += 1
            else:
                new_people_to_enrich.append(person)

            if idx % 50 == 0 or idx == total_people_to_enrich:
                print(
                    f"PROGRESS: Duplicate screening {idx}/{total_people_to_enrich}"
                )

        existing_enriched_records = lookup.get_records_by_ids(matched_existing_ids)
        print(f"Loaded {len(existing_enriched_records)} matched existing records for reuse")

        print(
            "After duplicate check: "
            f"{len(new_people_to_enrich)} new people, "
            f"{skipped_duplicate_count} duplicates, "
            f"{skipped_failed_count} skipped (previously failed), "
            f"{skipped_count} total skipped"
        )

        all_enriched_data: List[Dict[str, Any]] = []
        if existing_enriched_records:
            all_enriched_data.extend(existing_enriched_records)
        if matched_existing_for_this_run:
            # Avoid duplicates if records already in all_enriched_data
            existing_sigs = {_record_signature(rec) for rec in all_enriched_data}
            for rec in matched_existing_for_this_run:
                sig = _record_signature(rec)
                if sig not in existing_sigs:
                    all_enriched_data.append(rec)
                    existing_sigs.add(sig)

        if not new_people_to_enrich:
            return {
                'success': True,
                'message': 'All people already enriched or failed',
                'total_people': len(people_to_enrich) + len(already_enriched_from_step1),
                'enriched_count': 0,
                'enriched_data': all_enriched_data,
                'newly_enriched_data': [],
                'matched_existing': matched_existing_for_this_run,
                'actual_api_cost': '$0.00',
                'api_calls_saved': len(people_to_enrich) + len(already_enriched_from_step1),
                'already_enriched_count': skipped_duplicate_count,
                'skipped_failed_count': skipped_failed_count
            }
        
        # Limit for test mode (hard cap to 5 people)
        if bool(config.get('TEST_MODE')) and len(new_people_to_enrich) > 5:
            new_people_to_enrich = new_people_to_enrich[:5]
            print(f"TEST MODE: Limited to {len(new_people_to_enrich)} people")
        
        enrich_label = f"Enriching {len(new_people_to_enrich):,} people"
        _set_stage(4, enrich_label)
        print(f"STEP 4/{total_steps}: {enrich_label}")
        print(f"PROGRESS: Enrichment queue ready ({len(new_people_to_enrich)}/{len(people_to_enrich) + len(already_enriched_from_step1)})")

        # Initialize simple live progress (works with UI poller)
        progress_path = Path(config.get('OUTPUT_DIR', 'output')) / 'step2_progress.json'
        try:
            with open(progress_path, 'w') as pf:
                payload = {
                    'step': 2,
                    'total': int(len(new_people_to_enrich) + skipped_count + len(already_enriched_from_step1)),
                    'processed': 0,
                    'newly_enriched': 0,
                    'already_enriched': int(skipped_count + len(already_enriched_from_step1)),
                    'stage': enrich_label,
                    'stage_label': enrich_label,
                    'current_step': 4,
                    'total_steps': total_steps,
                    'started_at': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
                json.dump(payload, pf)
        except Exception:
            pass

        # Enrich the new people in a single pass (original faster flow)
        config['_existing_signatures'] = lookup.get_signature_snapshot()

        newly_enriched = enrich_people_batch(new_people_to_enrich, config, progress={
            'path': str(progress_path),
            'total': int(len(new_people_to_enrich) + skipped_count + len(already_enriched_from_step1)),
            'skipped': int(skipped_count + len(already_enriched_from_step1)),
            'stage_label': enrich_label,
            'current_step': 4,
            'total_steps': total_steps
        })
        
        # Note: Each enrichment is now saved to SQL inside the loop
        _set_stage(5, "Saving enrichment results")
        print(f"STEP 5/{total_steps}: Saved {len(newly_enriched)} enrichments during processing")
        if newly_enriched:
            print(f"PROGRESS: Enrichment saved ({len(newly_enriched)}/{len(new_people_to_enrich)})")

        combined_enriched: List[Dict[str, Any]] = list(all_enriched_data)
        existing_sigs = {_record_signature(rec) for rec in combined_enriched}

        for rec in newly_enriched:
            sig = _record_signature(rec)
            if sig not in existing_sigs:
                combined_enriched.append(rec)
                existing_sigs.add(sig)

        result = {
            'success': True,
            'total_people': len(people_to_enrich) + len(already_enriched_from_step1),
            'enriched_count': len(newly_enriched),
            'enrichment_rate': len(newly_enriched) / len(new_people_to_enrich) * 100 if new_people_to_enrich else 0,
            # Full snapshot from SQL + this run (for reporting)
            'enriched_data': combined_enriched,
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
        
        _set_stage(6, "Finalizing results", extra={'result_summary': {'enriched': len(newly_enriched)}}, log=False)
        print(f"STEP 6/{total_steps}: Function completed successfully!")
        print(f"  Returning {len(combined_enriched)} total enriched records")
        print(f"  Newly enriched: {len(newly_enriched)}")
        print(f"  Matched existing from Step 1: {len(matched_existing_for_this_run)}")
        print(f"  Result keys: {list(result.keys())}")
        
        _set_stage(total_steps, "Completed", extra={'result_summary': {'enriched': len(newly_enriched)}}, log=False)
        config.pop('_existing_signatures', None)
        return result
        
    except Exception as e:
        logger.error(f"Enrichment failed: {e}")
        try:
            payload = {
                'current_step': total_steps,
                'total_steps': total_steps,
                'stage_label': 'Failed',
                'error': str(e),
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ')
            }
            output_dir = Path(config.get('OUTPUT_DIR', 'output'))
            stage_path = output_dir / 'step2_stage.json'
            with stage_path.open('w') as sf:
                json.dump(payload, sf)
        except Exception:
            pass
        config.pop('_existing_signatures', None)
        return {
            'success': False,
            'error': str(e),
            'total_people': 0,
            'enriched_count': 0,
            'enriched_data': [],
            'actual_api_cost': '$0.00'
        }
    
def _normalize_value(value: Any) -> str:
    return (value or '').strip().lower()


def _record_signature(record: Dict[str, Any]) -> str:
    return '|'.join([
        _normalize_value(record.get('first_name')),
        _normalize_value(record.get('last_name')),
        _normalize_value(record.get('city')),
        _normalize_value(record.get('state')),
        (record.get('patent_number') or record.get('patent_no') or '').strip()
    ])


class EnrichedPeopleLookup:
    """Lazy loader that fetches only necessary enriched_people rows."""

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self._signature_to_id: Dict[str, int] = {}
        self._last_state_index: Dict[Tuple[str, str], Set[int]] = {}
        self._id_cache: Dict[int, Dict[str, Any]] = {}
        self._id_stub: Dict[int, Dict[str, Any]] = {}
        self._select_clause, self._mapping = self._discover_existing_people_columns()
        self._base_select_sql = (
            f"SELECT ep.*{(', ' + self._select_clause) if self._select_clause else ''} "
            "FROM enriched_people ep "
            "LEFT JOIN existing_people ex ON "
            "LOWER(TRIM(ep.first_name)) = LOWER(TRIM(ex.first_name)) "
            "AND LOWER(TRIM(ep.last_name)) = LOWER(TRIM(ex.last_name)) "
            "AND LOWER(TRIM(IFNULL(ep.city,''))) = LOWER(TRIM(IFNULL(ex.city,''))) "
            "AND LOWER(TRIM(IFNULL(ep.state,''))) = LOWER(TRIM(IFNULL(ex.state,''))) "
        )
        self._query_last_only = (
            self._base_select_sql +
            "WHERE LOWER(TRIM(ep.last_name)) = %s"
        )

    def _discover_existing_people_columns(self) -> Tuple[str, Dict[str, str]]:
        cols: List[str] = []
        try:
            col_rows = self.db.execute_query("SHOW COLUMNS FROM existing_people") or []
            cols = [
                (row.get('Field') or row.get('COLUMN_NAME') or row.get('field'))
                for row in col_rows if isinstance(row, dict)
            ]
        except Exception:
            cols = []

        def pick(primary: str, aliases: Optional[List[str]] = None) -> str:
            aliases = aliases or []
            if primary in cols:
                return primary
            for candidate in aliases:
                if candidate in cols:
                    return candidate
            return ''

        mapping = {
            'issue_id': pick('issue_id'),
            'new_issue_rec_num': pick('new_issue_rec_num', ['issue_rec_num', 'rec_num']),
            'inventor_id': pick('inventor_id'),
            'patent_no': pick('patent_no', ['patent_number', 'patent_num']),
            'title': pick('title', ['patent_title', 'invention_title']),
            'issue_date': pick('issue_date', ['date', 'patent_date']),
            'bar_code': pick('bar_code', ['barcode']),
            'mod_user': pick('mod_user', ['modified_by', 'last_modified_by']),
            'mail_to_assignee': pick('mail_to_assignee', ['assignee', 'assign_name']),
            'mail_to_name': pick('mail_to_name'),
            'mail_to_add1': pick('mail_to_add1', ['address', 'addr1', 'mail_to_add_1'])
        }

        select_parts = []
        for alias, column in mapping.items():
            if not column:
                continue
            if alias == column:
                select_parts.append(f"ex.{column}")
            else:
                select_parts.append(f"ex.{column} AS {alias}")

        return ', '.join(select_parts), mapping

    def _convert_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        try:
            enrichment_data = json.loads(row.get('enrichment_data', '{}'))
        except Exception:
            enrichment_data = {}

        first_name = row.get('first_name') or ''
        last_name = row.get('last_name') or ''
        city = row.get('city') or ''
        state = row.get('state') or ''
        patent_number = row.get('patent_number') or row.get('patent_no') or ''

        record = {
            'original_name': f"{first_name} {last_name}".strip(),
            'patent_number': patent_number,
            'patent_title': enrichment_data.get('original_person', {}).get('patent_title', ''),
            'match_score': enrichment_data.get('enrichment_result', {}).get('match_score', 0),
            'enriched_data': enrichment_data,
            'enriched_at': row.get('enriched_at'),
            'first_name': first_name,
            'last_name': last_name,
            'city': city,
            'state': state,
        }
        try:
            record_id = row.get('id')
        except AttributeError:
            record_id = None
        if record_id is None and isinstance(row, (list, tuple)) and len(row) > 0:
            record_id = row[0]
        if record_id is not None:
            record['id'] = record_id

        for alias in self._mapping:
            if alias in row and alias not in record:
                record[alias] = row.get(alias)

        if 'mail_to_add1' not in record:
            record['mail_to_add1'] = row.get('address')

        return record

    def _store_records(self, records: List[Dict[str, Any]]) -> None:
        for rec in records:
            sig = _record_signature(rec)
            if sig not in self._all_records:
                self._all_records[sig] = rec

    def prefetch_people(self, people: List[Dict[str, Any]]) -> None:
        if not people:
            return

        combos_by_state: Dict[str, Set[str]] = {}
        for person in people:
            raw_last = (person.get('last_name') or '').strip()
            normalized = _normalize_value(raw_last)
            if not normalized:
                continue
            state = _normalize_value(person.get('state'))
            combos_by_state.setdefault(state, set()).add(normalized)

        if not combos_by_state:
            return

        names_chunk_size = 80
        total_chunks = sum((len(names) + names_chunk_size - 1) // names_chunk_size for names in combos_by_state.values())
        processed_chunks = 0

        for state_value, last_names in combos_by_state.items():
            names_list = sorted(last_names)
            for idx in range(0, len(names_list), names_chunk_size):
                chunk_last_names = names_list[idx:idx + names_chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk_last_names))
                query = (
                    "SELECT id, first_name, last_name, city, state, patent_number "
                    "FROM enriched_people "
                    "WHERE LOWER(TRIM(IFNULL(state,''))) = %s "
                    f"AND LOWER(TRIM(last_name)) IN ({placeholders})"
                )
                params: List[Any] = [state_value] + list(chunk_last_names)
                try:
                    rows = self.db.execute_query(query, tuple(params)) or []
                except Exception as exc:
                    logger.warning(
                        "Prefetch chunk error (state='%s', names~%s): %s",
                        state_value or '', len(chunk_last_names), exc
                    )
                    rows = []

                for row in rows:
                    try:
                        row_id = row.get('id') if isinstance(row, dict) else row[0]
                    except Exception:
                        row_id = None
                    if not row_id:
                        continue

                    first = (row.get('first_name') if isinstance(row, dict) else row[1]) or ''
                    last = (row.get('last_name') if isinstance(row, dict) else row[2]) or ''
                    city = (row.get('city') if isinstance(row, dict) else row[3]) or ''
                    state = (row.get('state') if isinstance(row, dict) else row[4]) or ''
                    patent = (row.get('patent_number') if isinstance(row, dict) else row[5]) or ''

                    record_stub = {
                        'first_name': first,
                        'last_name': last,
                        'city': city,
                        'state': state,
                        'patent_number': patent,
                        'first_norm': _normalize_value(first),
                        'last_norm': _normalize_value(last),
                        'city_norm': _normalize_value(city),
                        'state_norm': _normalize_value(state)
                    }
                    signature = _record_signature(record_stub)
                    self._signature_to_id[signature] = row_id
                    self._id_stub[row_id] = record_stub

                    norm_last = _normalize_value(last)
                    norm_state = _normalize_value(state)
                    combo_key = (norm_last, norm_state)
                    self._last_state_index.setdefault(combo_key, set()).add(row_id)
                    # fallback bucket (any state)
                    self._last_state_index.setdefault((norm_last, ''), set()).add(row_id)

                processed_chunks += 1
                label_state = state_value if state_value else 'blank'
                print(
                    f"PROGRESS: Prefetch chunk {processed_chunks}/{total_chunks} "
                    f"(state='{label_state}', last_names={len(chunk_last_names)})"
                )

    def _load_record(self, record_id: int) -> Optional[Dict[str, Any]]:
        if record_id in self._id_cache:
            return self._id_cache[record_id]
        try:
            row = self.db.execute_query(
                self._base_select_sql + "WHERE ep.id = %s LIMIT 1",
                (record_id,),
                fetch_one=True
            )
        except Exception:
            row = None
        if not row:
            self._id_cache[record_id] = None
            return None
        record = self._convert_row(row)
        self._id_cache[record_id] = record
        return record

    def get_records_by_ids(self, ids: Iterable[int]) -> List[Dict[str, Any]]:
        unique_ids: List[int] = []
        seen: Set[int] = set()
        for record_id in ids:
            if record_id and record_id not in seen:
                seen.add(record_id)
                unique_ids.append(record_id)

        results: List[Dict[str, Any]] = []
        missing: List[int] = []
        for record_id in unique_ids:
            record = self._id_cache.get(record_id)
            if record and record.get('enriched_data'):
                results.append(record)
            else:
                missing.append(record_id)

        chunk_size = 200
        for idx in range(0, len(missing), chunk_size):
            chunk = missing[idx:idx + chunk_size]
            if not chunk:
                continue
            placeholders = ', '.join(['%s'] * len(chunk))
            query = self._base_select_sql + f"WHERE ep.id IN ({placeholders})"
            try:
                rows = self.db.execute_query(query, tuple(chunk)) or []
            except Exception as exc:
                logger.warning(f"Bulk load failed for ids {chunk[:5]}...: {exc}")
                rows = []
            for row in rows:
                try:
                    rec_id = row.get('id')
                except AttributeError:
                    rec_id = None
                if rec_id is None and isinstance(row, (list, tuple)) and len(row) > 0:
                    rec_id = row[0]
                record = self._convert_row(row)
                if rec_id is None:
                    continue
                self._id_cache[rec_id] = record

        for record_id in unique_ids:
            record = self._id_cache.get(record_id)
            if record:
                results.append(record)
        return results

    def find_matching_id(self, person: Dict[str, Any]) -> Optional[int]:
        last_name_norm = _normalize_value(person.get('last_name'))
        if not last_name_norm:
            return None

        state_norm = _normalize_value(person.get('state'))
        signature = _person_signature(person)
        sig_id = self._signature_to_id.get(signature)
        if sig_id:
            return sig_id

        candidate_ids = list(self._last_state_index.get((last_name_norm, state_norm), []))
        if state_norm and not candidate_ids:
            candidate_ids = list(self._last_state_index.get((last_name_norm, ''), []))
        if not candidate_ids:
            return None

        first_norm = _normalize_value(person.get('first_name'))
        city_norm = _normalize_value(person.get('city'))

        for cid in candidate_ids:
            stub = self._id_stub.get(cid) or {}
            if not stub:
                continue
            cand_first = stub.get('first_norm')
            cand_last = stub.get('last_norm')
            cand_city = stub.get('city_norm')
            cand_state = stub.get('state_norm')

            if first_norm and last_name_norm and city_norm and state_norm and (
                cand_first == first_norm and cand_last == last_name_norm and cand_city == city_norm and cand_state == state_norm
            ):
                return cid

            if first_norm and last_name_norm and state_norm and (
                cand_first == first_norm and cand_last == last_name_norm and cand_state == state_norm
            ):
                return cid

            if first_norm and last_name_norm and state_norm and cand_first and (
                cand_first[:1] == first_norm[:1] and cand_last == last_name_norm and cand_state == state_norm
            ):
                return cid

        return None

    def find_best_match(self, person: Dict[str, Any], require_record: bool = True):
        match_id = self.find_matching_id(person)
        if match_id is None:
            return None if require_record else False
        if not require_record:
            return True
        return self._load_record(match_id)

    def get_all_records(self) -> List[Dict[str, Any]]:
        return self.get_records_by_ids(self._signature_to_id.values())

    def get_signature_snapshot(self) -> Set[str]:
        return set(self._signature_to_id.keys())


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
            stage_label = progress.get('stage_label')
            if stage_label:
                payload['stage'] = stage_label
                payload['stage_label'] = stage_label
            if progress.get('current_step') is not None:
                payload['current_step'] = int(progress.get('current_step'))
            if progress.get('total_steps') is not None:
                payload['total_steps'] = int(progress.get('total_steps'))
            with open(progress.get('path'), 'w') as pf:
                json.dump(payload, pf)
        except Exception:
            pass

    existing_signatures = set(config.get('_existing_signatures') or [])
    commit_interval = 50
    pending_commits = 0

    for i, person in enumerate(people):
        # Secondary safety: enforce test mode cap inside the loop
        if bool(config.get('TEST_MODE')) and i >= 5:
            break
        current_index = i + 1
        total = len(people)
        person_name = f"{person.get('first_name', '')} {person.get('last_name', '')}"

        print(f"ENRICHING {current_index}/{total}: {person_name}")
        print(f"PROGRESS: Enriching {current_index}/{total}")
        print(f"  Person data: first_name='{person.get('first_name')}', last_name='{person.get('last_name')}', city='{person.get('city')}', state='{person.get('state')}'")
        
        signature = _person_signature(person)
        if signature in existing_signatures:
            print("  Skipping: already enriched (cached signature)")
            processed_counter += 1
            write_progress_safely()
            continue

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
                        pending_commits += 1
                        if pending_commits >= commit_interval:
                            conn.commit()
                            pending_commits = 0
                        if bool(config.get('TEST_MODE')):
                            print("  DEBUG: Saved enrichment to SQL")
                except Exception as e:
                    logger.error(f"  Error saving enrichment for {person_name}: {e}")
                    if bool(config.get('TEST_MODE')):
                        print(f"  DEBUG: Save error: {e}")
                existing_signatures.add(signature)
            else:
                # Record failure (no enrichment result)
                try:
                    if cursor is not None and conn is not None:
                        # Use cleaned person when available
                        _record_failed_enrichment(cursor, db_config.engine if 'db_config' in locals() else 'mysql', clean_person, 'not_found', None)
                        pending_commits += 1
                        if pending_commits >= commit_interval:
                            conn.commit()
                            pending_commits = 0
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
                    pending_commits += 1
                    if pending_commits >= commit_interval:
                        conn.commit()
                        pending_commits = 0
            except Exception:
                pass
        
        # Small delay to be nice to API
        time.sleep(0.1)
        processed_counter += 1
        write_progress_safely()
    
    # Clean up DB connection context manager
    try:
        if conn is not None and pending_commits and cursor is not None:
            conn.commit()
    except Exception:
        pass

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

    snapshot_existing = dict(existing_record or {})
    street_fallback = (original_data.get('mail_to_add1')
                       or original_data.get('mail_to_address')
                       or original_data.get('mail_to_add_1')
                       or '')
    zip_fallback = (original_data.get('mail_to_zip') or '')
    if not snapshot_existing.get('mail_to_add1') and street_fallback:
        snapshot_existing['mail_to_add1'] = street_fallback.strip()
    if not snapshot_existing.get('mail_to_zip') and zip_fallback:
        snapshot_existing['mail_to_zip'] = zip_fallback.strip()

    enrichment_data = {
        "original_person": original_data,
        "enrichment_result": result,
        "enrichment_metadata": {
            "enriched_at": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "api_cost": 0.03
        },
        # Persist selected existing_people fields for reliable formatted exports later
        "existing_record": snapshot_existing
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
