# =============================================================================
# runners/integrate_existing_data.py
# Step 1: Compare XML data to existing SQL or CSV data
# BATCH SQL APPROACH: Query database per batch instead of loading subset into memory
# ENHANCED: Added US Patent filtering before database comparison
# FIXED: Proper US filtering sequence to prevent foreign patents in people counting
# FIXED: Query full 8M database instead of 50K subset
# =============================================================================

import logging
import os
import json
import time
from typing import Dict, Any, List, Tuple, Optional
from pathlib import Path
from datetime import datetime, date
import uuid
import re
from collections import Counter

from classes.simple_xml_processor import process_xml_files

logger = logging.getLogger(__name__)

class BatchSQLQueryIntegrator:
    """Query SQL database per batch instead of loading subset into memory"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.existing_patents = set()
        self.dev_mode = bool(config.get('DEV_MODE'))
        self.dev_issue_cutoff = None
        self.dev_issue_cutoff_raw = config.get('DEV_ISSUE_CUTOFF')
        self.skip_enriched_filter = bool(config.get('SKIP_ALREADY_ENRICHED_FILTER'))
        self._enriched_person_signatures: Optional[set] = None
        if self.dev_mode:
            if self.dev_issue_cutoff_raw:
                self.dev_issue_cutoff = self._parse_issue_date_value(self.dev_issue_cutoff_raw)
                if not self.dev_issue_cutoff:
                    logger.warning(f"Dev mode cutoff '{self.dev_issue_cutoff_raw}' is invalid; disabling dev filter")
                    self.dev_mode = False
                else:
                    logger.info(
                        "Dev mode enabled. Ignoring SQL people with issue_date after %s",
                        self.dev_issue_cutoff.isoformat()
                    )
                    try:
                        print(
                            f"PROGRESS: Dev mode filter - ignoring SQL people newer than {self.dev_issue_cutoff.date().isoformat()}"
                        )
                    except Exception:
                        pass
            else:
                logger.warning("Dev mode requested without issue date cutoff; disabling dev filter")
                self.dev_mode = False
        
        # Database connection setup
        try:
            from database.db_manager import DatabaseManager, DatabaseConfig, ExistingDataDAO
            
            db_config = DatabaseConfig.from_env()
            self.db_manager = DatabaseManager(db_config)
            self.existing_dao = ExistingDataDAO(self.db_manager)
            
            if self.db_manager.test_connection():
                logger.info(f"Connected to SQL database: {db_config.host}:{db_config.port}/{db_config.database}")
                self.use_sql = True
            else:
                logger.warning("SQL database connection failed, will try CSV fallback")
                self.use_sql = False
                
        except ImportError as e:
            logger.warning(f"SQL database modules not available: {e}")
            self.use_sql = False
        except Exception as e:
            logger.warning(f"Database connection failed: {e}, will try CSV fallback")
            self.use_sql = False
    
    def filter_us_patents_only(self, xml_patents: List[Dict]) -> Dict[str, Any]:
        """Filter to keep only US patents before database comparison"""
        logger.info(f"Filtering {len(xml_patents)} patents to US patents only...")
        print(f"PROGRESS: Filtering patents to US only - Starting with {len(xml_patents):,} patents")
        
        us_patents = []
        foreign_patents = []
        
        start_time = time.time()
        
        for i, patent in enumerate(xml_patents):
            # Progress reporting for large datasets
            if i > 0 and i % 1000 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                eta_seconds = (len(xml_patents) - i) / rate if rate > 0 else 0
                print(f"PROGRESS: US Filter - {i:,}/{len(xml_patents):,} ({i/len(xml_patents)*100:.1f}%) - ETA: {eta_seconds/60:.1f}min")
            
            if self._is_us_patent(patent):
                us_patents.append(patent)
            else:
                foreign_patents.append({
                    'patent_number': patent.get('patent_number'),
                    'country': patent.get('country_code', ''),
                    'inventors': [{'country': inv.get('country', 'Unknown')} for inv in patent.get('inventors', [])],
                    'title': patent.get('patent_title', '')[:100] + '...' if patent.get('patent_title') else '',
                    'reason': 'Non-US patent'
                })
        
        elapsed = time.time() - start_time
        
        logger.info(f"US patent filtering complete in {elapsed:.1f}s:")
        logger.info(f"  - US patents: {len(us_patents):,}")
        logger.info(f"  - Foreign patents filtered out: {len(foreign_patents):,}")
        
        print(f"PROGRESS: US Filter Complete - {len(us_patents):,} US patents, {len(foreign_patents):,} foreign patents removed")
        
        # Save foreign patents log for reference
        if foreign_patents and self.config.get('OUTPUT_DIR'):
            foreign_patents_file = Path(self.config['OUTPUT_DIR']) / 'filtered_foreign_patents.json'
            try:
                with open(foreign_patents_file, 'w') as f:
                    json.dump(foreign_patents[:1000], f, indent=2)  # Limit to first 1000 for file size
                logger.info(f"Saved foreign patents log to {foreign_patents_file}")
            except Exception as e:
                logger.warning(f"Could not save foreign patents log: {e}")
        
        return {
            'us_patents': us_patents,
            'foreign_patents_count': len(foreign_patents),
            'foreign_patents_sample': foreign_patents[:10],  # First 10 for immediate review
            'filtering_time_seconds': elapsed,
            'total_xml_patents': len(xml_patents),
            'us_retention_rate': f"{(len(us_patents)/len(xml_patents)*100):.1f}%"
        }
    
    def _is_us_patent(self, patent: Dict[str, Any]) -> bool:
        """Keep patents where ALL inventors are US by country OR US state."""

        inventors = patent.get('inventors', [])
        if not inventors:
            return False

        US_STATE_CODES = {
            'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
            'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
            'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC','PR'
        }

        for inventor in inventors:
            c = (inventor.get('country') or '').strip().upper()
            s = (inventor.get('state') or '').strip().upper()
            is_us = False
            if c in {'US', 'USA', 'UNITED STATES', 'UNITED STATES OF AMERICA'}:
                is_us = True
            elif s in US_STATE_CODES:
                is_us = True
            if not is_us:
                return False

        return True

    def _parse_issue_date_value(self, value: Any) -> Optional[datetime]:
        """Parse issue_date values from config or SQL rows into datetime objects"""
        if value is None or value == '' or str(value).lower() == 'none':
            return None

        if isinstance(value, datetime):
            return value

        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())

        text = str(value).strip()
        if not text:
            return None

        # Try ISO parsing first (handles YYYY-MM-DD, YYYY-MM-DDTHH:MM, etc.)
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            pass

        # Fallback to common formats
        known_formats = (
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%m/%d/%Y',
            '%m/%d/%Y %H:%M',
            '%Y%m%d'
        )
        for fmt in known_formats:
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue

        return None

    def _apply_dev_issue_date_filter(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter SQL rows when dev mode is active to ignore recent issue_date entries"""
        if not rows or not self.dev_mode or not self.dev_issue_cutoff:
            return rows

        grouped: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        for row in rows:
            key = (
                row.get('first_name'),
                row.get('last_name'),
                row.get('city'),
                row.get('state'),
                row.get('address'),
                row.get('zip')
            )
            data = grouped.setdefault(key, {'rows': [], 'issue_dates': []})
            data['rows'].append(row)
            issue_dt = self._parse_issue_date_value(row.get('issue_date'))
            if issue_dt:
                data['issue_dates'].append(issue_dt)

        filtered_rows: List[Dict[str, Any]] = []
        removed_rows = 0
        removed_groups = 0

        for group in grouped.values():
            if group['issue_dates']:
                earliest = min(group['issue_dates'])
                if earliest and earliest <= self.dev_issue_cutoff:
                    filtered_rows.extend(group['rows'])
                else:
                    removed_rows += len(group['rows'])
                    removed_groups += 1
            else:
                filtered_rows.extend(group['rows'])

        if removed_rows:
            logger.info(
                "Dev mode filtered out %s SQL row(s) across %s group(s) newer than cutoff %s",
                removed_rows,
                removed_groups,
                self.dev_issue_cutoff.isoformat()
            )
            try:
                print(
                    f"PROGRESS: Dev mode filter removed {removed_groups} group(s) newer than cutoff"
                )
            except Exception:
                pass

        return filtered_rows
    
    def load_existing_patents_only(self) -> Dict[str, Any]:
        """Load only patents (not people) into memory - patents are smaller dataset"""
        if not self.use_sql:
            return {'success': False, 'error': 'SQL not available'}
            
        start_time = time.time()
        logger.info("Loading existing patents from SQL database...")
        print("PROGRESS: Loading existing patents from SQL...")
        
        try:
            # Only load patents into memory (much smaller than people)
            self.existing_patents = self.existing_dao.load_existing_patents()
            
            elapsed = time.time() - start_time
            logger.info(f"Patents loaded in {elapsed:.1f}s - {len(self.existing_patents):,} patents")
            print(f"PROGRESS: Patents loaded - {len(self.existing_patents):,} existing patents in memory")
            
            return {
                'success': True,
                'existing_patents_count': len(self.existing_patents),
                'source': 'sql_database',
                'loading_time_seconds': elapsed
            }
            
        except Exception as e:
            logger.error(f"Error loading patents from SQL: {e}")
            return {'success': False, 'error': str(e)}
    
    def load_existing_data_from_csv(self) -> Dict[str, Any]:
        """Fallback: Load from CSV files if SQL not available"""
        csv_folder = self.config.get('CSV_DATABASE_FOLDER', 'converted_databases/csv')
        csv_path = Path(csv_folder)
        
        if not csv_path.exists():
            return {'success': False, 'error': f'CSV folder not found: {csv_folder}'}

        if self.dev_mode and self.dev_issue_cutoff:
            logger.warning("Dev mode issue_date filtering is not applied when using CSV fallback; all records will be considered")
            try:
                print("PROGRESS: Dev mode filter unavailable for CSV fallback — including all existing records")
            except Exception:
                pass

        start_time = time.time()
        logger.info(f"Loading existing data from CSV files: {csv_folder}")
        print(f"PROGRESS: Loading CSV files from {csv_folder} (SQL fallback)")
        
        try:
            import pandas as pd
            
            csv_files = list(csv_path.glob("*.csv"))
            if not csv_files:
                return {'success': False, 'error': 'No CSV files found'}
            
            logger.info(f"Found {len(csv_files)} CSV files")
            print(f"PROGRESS: Processing {len(csv_files)} CSV files...")
            
            # For CSV fallback, we still need to implement the old approach since CSV doesn't support SQL queries
            existing_people = []
            
            # Limit files for memory management
            MAX_FILES = 20
            files_to_process = csv_files[:MAX_FILES]
            
            for i, csv_file in enumerate(files_to_process):
                logger.info(f"Loading {csv_file.name} ({i+1}/{len(files_to_process)})")
                print(f"PROGRESS: Loading file {i+1}/{len(files_to_process)}: {csv_file.name}")
                
                try:
                    # Load with row limit to prevent memory issues
                    df = pd.read_csv(csv_file, encoding='utf-8', low_memory=False, nrows=10000)
                    self._extract_from_dataframe(df, csv_file.name, existing_people)
                except Exception as e:
                    logger.error(f"Error loading {csv_file}: {e}")
                    continue
            
            elapsed = time.time() - start_time
            logger.info(f"CSV data loaded in {elapsed:.1f}s - {len(self.existing_patents):,} patents, {len(existing_people):,} people")
            print(f"PROGRESS: CSV data loaded in {elapsed:.1f}s - Ready for processing")
            
            return {
                'success': True,
                'existing_patents_count': len(self.existing_patents),
                'existing_people_count': len(existing_people),
                'source': 'csv_files',
                'files_processed': len(files_to_process),
                'loading_time_seconds': elapsed,
                'existing_people_data': existing_people  # Pass the data for CSV mode
            }
            
        except Exception as e:
            logger.error(f"Error loading from CSV: {e}")
            return {'success': False, 'error': str(e)}
    
    def _extract_from_dataframe(self, df, filename: str, existing_people: List):
        """Extract patents and people from CSV dataframe"""
        # Patent extraction
        patent_columns = ['patent_number', 'patent_id', 'publication_number', 'doc_number', 'number']
        patent_col = None
        
        for col in df.columns:
            if any(pcol.lower() in col.lower() for pcol in patent_columns):
                patent_col = col
                break
        
        if patent_col:
            for patent_num in df[patent_col].dropna().astype(str):
                clean_patent = self._clean_patent_number(patent_num)
                if clean_patent:
                    self.existing_patents.add(clean_patent)
        
        # People extraction
        first_name_col = self._find_column(df.columns, ['first_name', 'firstname', 'fname', 'inventor_first'])
        last_name_col = self._find_column(df.columns, ['last_name', 'lastname', 'lname', 'inventor_last'])
        
        if first_name_col and last_name_col:
            city_col = self._find_column(df.columns, ['city', 'inventor_city', 'location_city'])
            state_col = self._find_column(df.columns, ['state', 'inventor_state', 'location_state'])
            
            for _, row in df.iterrows():
                first_name = self._clean_string(row.get(first_name_col, '')).lower()
                last_name = self._clean_string(row.get(last_name_col, '')).lower()
                
                if first_name or last_name:
                    existing_people.append({
                        'first_name': first_name,
                        'last_name': last_name,
                        'city': self._clean_string(row.get(city_col, '')).lower() if city_col else '',
                        'state': self._clean_string(row.get(state_col, '')).lower() if state_col else '',
                        'country': '',
                        'source': filename
                    })
    
    def _find_column(self, columns: List[str], patterns: List[str]) -> Optional[str]:
        """Find column matching patterns"""
        for col in columns:
            if any(pattern.lower() in col.lower() for pattern in patterns):
                return col
        return None
    
   

    def find_people_matches_batch_sql(self, people_batch: List[Dict], batch_index: int, total_batches: int) -> Dict[str, List[Tuple[Dict, int]]]:
        """Query SQL database for matches for entire batch of people - FIXED to be more selective"""
        if not self.use_sql or not people_batch:
            return {}
        
        batch_matches = {}
        batch_start_time = time.time()
        batch_label = f"Batch {batch_index}/{total_batches}" if total_batches else f"Batch {batch_index}"
        
        logger.info(f"{batch_label}: Starting SELECTIVE SQL batch matching for {len(people_batch)} people")
        print(f"PROGRESS: {batch_label} — SELECTIVE SQL - Processing {len(people_batch)} people")
        
        try:
            # FIXED: Use selective query instead of broad lastname search
            query_start = time.time()

            dev_cutoff = self.dev_issue_cutoff if self.dev_mode else None

            print(f"DEBUG: Dev mode is {'ON' if self.dev_mode else 'OFF'}, cutoff: {dev_cutoff}")
            
            # Call the NEW more selective method
            all_db_people = self.existing_dao.find_people_by_batch_selective(people_batch, dev_cutoff_datetime=dev_cutoff)


            # Enhanced debug code for multiple people:
            debug_people = ['kenneth mott', 'christopher bell', 'david benedetti']
            for debug_name in debug_people:
                name_parts = debug_name.split()
                if len(name_parts) == 2:
                    debug_first, debug_last = name_parts
                    debug_results = [p for p in all_db_people if 
                                p.get('first_name', '').lower().strip() == debug_first and 
                                p.get('last_name', '').lower().strip() == debug_last]
                    if debug_results:
                        logger.info(f"DEBUG: SQL returned {len(debug_results)} {debug_name.title()} records:")
                        for sp in debug_results:
                            logger.info(f"  SQL: '{sp.get('first_name')}' '{sp.get('last_name')}' '{sp.get('city')}' '{sp.get('state')}'")
                    else:
                        logger.info(f"DEBUG: SQL returned NO {debug_name.title()} records")
            
            # Apply dev mode filtering in Python (as requested) 
            original_db_count = len(all_db_people)
            if self.dev_mode and self.dev_issue_cutoff:
                all_db_people = self._apply_dev_issue_date_filter(all_db_people)
                filtered_count = original_db_count - len(all_db_people)
                if filtered_count > 0:
                    logger.info(f"Dev mode filtered out {filtered_count} records in Python")
                    print(f"PROGRESS: {batch_label} — Dev mode filtered {filtered_count} records")
            
            query_time = time.time() - query_start
            
            logger.info(f"{batch_label}: Selective query returned {len(all_db_people)} records in {query_time:.3f}s")
            print(f"PROGRESS: {batch_label} — SQL Complete - {len(all_db_people)} records in {query_time:.3f}s")
            
            # Group DB results by lastname for efficient matching
            db_people_by_lastname = {}
            for db_person in all_db_people:
                db_last_name = self._clean_lastname_for_matching(db_person.get('last_name', '')).lower()
                if db_last_name not in db_people_by_lastname:
                    db_people_by_lastname[db_last_name] = []
                db_people_by_lastname[db_last_name].append(db_person)
            
            # Debug: Check if our problem people are in the grouped data
            for debug_name in debug_people:
                debug_last = debug_name.split()[1]
                if debug_last in db_people_by_lastname:
                    count = len(db_people_by_lastname[debug_last])
                    logger.info(f"DEBUG: {debug_last.title()} found in grouped data - {count} records available for matching")
                else:
                    logger.info(f"DEBUG: {debug_last.title()} NOT found in grouped data")
            
            # Match each person in batch against relevant DB results
            total_comparisons = 0
            total_matches_found = 0
            debug_successful_matches = []
            debug_failed_matches = []
            
            for i, person in enumerate(people_batch):
                person_key = f"batch_{i}"
                batch_matches[person_key] = []
                
                # Clean target person data
                target_first = self._clean_name_for_matching(person.get('first_name', ''))
                target_last = self._clean_lastname_for_matching(person.get('last_name', '')).lower()
                target_city = self._clean_string(person.get('city', '')).lower()
                target_state = self._clean_string(person.get('state', '')).lower()
                
                if not target_last:
                    continue
                
                # Get DB people with matching last name
                relevant_db_people = db_people_by_lastname.get(target_last, [])
                
                # Debug specific people in the matching phase
                debug_person_full = f"{target_first} {target_last}".strip()
                if debug_person_full in debug_people:
                    logger.info(f"DEBUG MATCHING: Processing '{target_first}' '{target_last}' '{target_city}' '{target_state}' - Found {len(relevant_db_people)} DB candidates")
                    for j, db_person in enumerate(relevant_db_people):
                        logger.info(f"  Candidate {j+1}: '{db_person.get('first_name')}' '{db_person.get('last_name')}' '{db_person.get('city')}' '{db_person.get('state')}'")
                
                person_matches = 0
                best_score_for_person = 0
                best_match_for_person = None
                
                # Score each match
                for db_person in relevant_db_people:
                    total_comparisons += 1
                    
                    score = self._calculate_simple_match_score(
                        target_first, target_last, target_city, target_state,
                        db_person.get('first_name', ''),
                        db_person.get('last_name', ''),
                        db_person.get('city', ''),
                        db_person.get('state', '')
                    )
                    
                    # Debug scoring for specific people
                    if debug_person_full in debug_people:
                        logger.info(f"DEBUG SCORING: '{target_first}' '{target_last}' vs '{db_person.get('first_name')}' '{db_person.get('last_name')}' = Score: {score}")
                    
                    if score > best_score_for_person:
                        best_score_for_person = score
                        best_match_for_person = db_person
                    
                    if score > 0:
                        batch_matches[person_key].append((db_person, score))
                        person_matches += 1
                        total_matches_found += 1
                
                # Debug final results for specific people
                if debug_person_full in debug_people:
                    logger.info(f"DEBUG FINAL: '{target_first}' '{target_last}' - Best score: {best_score_for_person}, Total matches: {person_matches}")
                
                # Debug logging for successful and failed matches
                if best_score_for_person >= 25:  # Successful match
                    if len(debug_successful_matches) < 10:
                        debug_successful_matches.append({
                            'target': f"'{target_first}' '{target_last}' '{target_city}' '{target_state}'",
                            'score': best_score_for_person
                        })
                else:  # Failed match
                    if len(debug_failed_matches) < 20:
                        debug_failed_matches.append({
                            'target': f"'{target_first}' '{target_last}' '{target_city}' '{target_state}'",
                            'score': best_score_for_person,
                            'candidates': len(relevant_db_people)
                        })
                
                # Sort and limit results
                batch_matches[person_key].sort(key=lambda x: x[1], reverse=True)
                batch_matches[person_key] = batch_matches[person_key][:10]
            
            batch_time = time.time() - batch_start_time
            
            # Summary logging with sample matches and failures  
            logger.info(f"BATCH SUMMARY - Total comparisons: {total_comparisons}, Found {total_matches_found} matches")
            
            if debug_successful_matches:
                logger.info(f"SUCCESSFUL MATCHES ({len(debug_successful_matches)}/10 shown):")
                for match in debug_successful_matches:
                    logger.info(f"  ✅ {match['target']} → Score: {match['score']}")
            
            if debug_failed_matches:
                logger.info(f"FAILED MATCHES ({len(debug_failed_matches)}/20 shown):")
                for match in debug_failed_matches:
                    logger.info(f"  ❌ {match['target']} → Best Score: {match['score']} (from {match['candidates']} candidates)")
            
            logger.info(f"{batch_label}: Complete - {total_comparisons} comparisons, {total_matches_found} matches in {batch_time:.2f}s")
            print(f"PROGRESS: {batch_label} — Complete - {total_matches_found} matches in {batch_time:.2f}s")
            
        except Exception as e:
            logger.error(f"Error in selective batch SQL query: {e}")
            print(f"PROGRESS: {batch_label} — ERROR: {e}")
        
        return batch_matches

    def _clean_string(self, value) -> str:
        """Helper method to clean string values"""
        if not value or str(value).lower() in ['nan', 'none', 'null', '']:
            return ''
        return str(value).strip()

    def find_person_matches_csv(self, target_person: Dict[str, str], existing_people: List[Dict]) -> List[Tuple[Dict, int]]:
        """CSV fallback: in-memory person matching using VBA-style scoring"""
        matches = []
        
        target_first = self._clean_string(target_person.get('first_name', '')).lower()
        target_last = self._clean_string(target_person.get('last_name', '')).lower()
        target_city = self._clean_string(target_person.get('city', '')).lower()
        target_state = self._clean_string(target_person.get('state', '')).lower()
        
        if not target_last:  # Need at least last name
            return matches
        
        # Fast in-memory scan with early termination
        matches_found = 0
        max_matches = 10
        
        for existing_person in existing_people:
            if matches_found >= max_matches:
                break
            
            # Quick elimination: last name must match
            if target_last != existing_person.get('last_name', ''):
                continue
            
            score = self._calculate_simple_match_score(
                target_first, target_last, target_city, target_state,
                existing_person.get('first_name', ''),
                existing_person.get('last_name', ''),
                existing_person.get('city', ''),
                existing_person.get('state', '')
            )
            
            if score > 0:
                matches.append((existing_person, score))
                matches_found += 1
        
        return sorted(matches, key=lambda x: x[1], reverse=True)
    
    def filter_new_xml_data_batch_sql(self, us_patents_only: List[Dict], load_result: Dict = None) -> Dict[str, Any]:
        """
        Process patents in batches, querying SQL database for each batch
        This eliminates the 50,000 person memory limit issue
        """
        logger.info(f"Processing {len(us_patents_only)} US patents with batch SQL queries")
        print(f"PROGRESS: Starting batch SQL processing - {len(us_patents_only):,} US patents")
        
        # Check if we're using SQL or CSV mode
        using_sql = self.use_sql and load_result and load_result.get('source') == 'sql_database'
        existing_people_csv = load_result.get('existing_people_data', []) if load_result else []
        
        # Configuration
        BATCH_SIZE = 1000  # Smaller batches for SQL query efficiency
        AUTO_MATCH_THRESHOLD = 25
        REVIEW_THRESHOLD = 10
        
        # Results
        new_patents = []
        new_people = []
        existing_people_found = []
        duplicate_patents = 0
        total_xml_people = 0
        processed_people = 0
        existing_enriched_filtered: List[Dict[str, Any]] = []
        
        # Statistics
        match_statistics = {
            'score_50_perfect': 0, 'score_25_moved': 0, 'score_15_initial': 0,
            'score_10_limited': 0, 'score_6_initial_exact': 0, 'score_3_initial_state': 0,
            'no_match': 0, 'auto_matched': 0, 'needs_review': 0, 'definitely_new': 0,
            'sql_queries_executed': 0, 'total_db_people_checked': 0
        }
        
        start_time = time.time()
        total_batches = (len(us_patents_only) + BATCH_SIZE - 1) // BATCH_SIZE
        
        logger.info(f"Using {'SQL batch queries' if using_sql else 'CSV in-memory matching'} for people comparison")
        print(f"PROGRESS: Using {'SQL' if using_sql else 'CSV'} mode for people matching")
        
        for batch_num in range(total_batches):
            batch_start_time = time.time()
            start_idx = batch_num * BATCH_SIZE
            end_idx = min((batch_num + 1) * BATCH_SIZE, len(us_patents_only))
            batch_patents = us_patents_only[start_idx:end_idx]
            
            # Collect all people from this batch for single SQL query
            batch_people = []
            batch_people_metadata = []  # Track which patent/role each person belongs to
            
            logger.info(f"Processing batch {batch_num + 1}/{total_batches}: {len(batch_patents)} patents")
            print(f"PROGRESS: Batch {batch_num + 1} - Collecting people from {len(batch_patents)} patents")
            
            for patent in batch_patents:
                patent_number = self._clean_patent_number(patent.get('patent_number', ''))
                is_new_patent = patent_number not in self.existing_patents
                
                if is_new_patent:
                    new_patents.append(patent)
                    
                    # Collect inventors
                    for i, inventor in enumerate(patent.get('inventors', [])):
                        total_xml_people += 1
                        batch_people.append(inventor)
                        batch_people_metadata.append({
                            'patent_number': patent_number,
                            'patent_title': patent.get('patent_title'),
                            'person_type': 'inventor',
                            'person_index': i,
                            'batch_index': len(batch_people) - 1
                        })
                    
                    # Collect assignees with names
                    for i, assignee in enumerate(patent.get('assignees', [])):
                        if assignee.get('first_name') or assignee.get('last_name'):
                            total_xml_people += 1
                            batch_people.append(assignee)
                            batch_people_metadata.append({
                                'patent_number': patent_number,
                                'patent_title': patent.get('patent_title'),
                                'person_type': 'assignee',
                                'person_index': i,
                                'batch_index': len(batch_people) - 1
                            })
                else:
                    duplicate_patents += 1
                    # Still count people for statistics
                    for inventor in patent.get('inventors', []):
                        total_xml_people += 1
                    for assignee in patent.get('assignees', []):
                        if assignee.get('first_name') or assignee.get('last_name'):
                            total_xml_people += 1
            
            # Execute batch matching (SQL or CSV)
            if batch_people:
                logger.info(f"Batch {batch_num + 1}: Starting matching for {len(batch_people)} people")
                print(f"PROGRESS: Batch {batch_num + 1} - Starting matching for {len(batch_people)} people")
                
                batch_matching_start = time.time()
                
                if using_sql:
                    batch_matches = self.find_people_matches_batch_sql(batch_people, batch_num + 1, total_batches)
                    match_statistics['sql_queries_executed'] += len(set(
                        self._clean_string(p.get('last_name', '')).lower() 
                        for p in batch_people if p.get('last_name')
                    ))
                else:
                    # CSV fallback - match each person individually
                    batch_matches = {}
                    for i, person in enumerate(batch_people):
                        person_key = f"batch_{i}"
                        matches = self.find_person_matches_csv(person, existing_people_csv)
                        batch_matches[person_key] = matches
                
                batch_matching_time = time.time() - batch_matching_start
                logger.info(f"Batch {batch_num + 1}: Matching completed in {batch_matching_time:.2f}s")
                print(f"PROGRESS: Batch {batch_num + 1} - Matching completed in {batch_matching_time:.2f}s")
                
                # Process matches for each person
                logger.info(f"Batch {batch_num + 1}: Processing {len(batch_people_metadata)} match results")
                print(f"PROGRESS: Batch {batch_num + 1} - Processing {len(batch_people_metadata)} match results")
                
                decisions_made = {'existing': 0, 'review': 0, 'new': 0}
                for metadata in batch_people_metadata:
                    batch_index = metadata['batch_index']
                    person = batch_people[batch_index]
                    person_key = f"batch_{batch_index}"
                    
                    matches = batch_matches.get(person_key, [])
                    best_score = matches[0][1] if matches else 0
                    processed_people += 1
                    
                    # Update statistics
                    self._update_match_statistics(match_statistics, best_score)
                    match_statistics['total_db_people_checked'] += len(matches)
                    
                    # Process based on score thresholds
                    if best_score >= AUTO_MATCH_THRESHOLD:
                        match_statistics['auto_matched'] += 1
                        decisions_made['existing'] += 1
                        existing_people_found.append({
                            **person,
                            'match_score': best_score,
                            'match_reason': 'auto_matched',
                            'patent_number': metadata['patent_number'],
                            'person_type': metadata['person_type'],
                            'patent_title': metadata['patent_title'],
                            # Ensure these fields are always present, even if null
                            'address': person.get('address'),
                            'email': person.get('email'), 
                            'issue_id': person.get('issue_id'),
                            'inventor_id': person.get('inventor_id'),
                            'mod_user': person.get('mod_user')
                        })
                        
                    elif best_score >= REVIEW_THRESHOLD:
                        match_statistics['needs_review'] += 1
                        decisions_made['review'] += 1
                        new_people.append({
                            **person,
                            'patent_number': metadata['patent_number'],
                            'patent_title': metadata['patent_title'],
                            'person_type': metadata['person_type'],
                            'person_id': f"{metadata['patent_number']}_{metadata['person_type']}_{processed_people}",
                            'match_score': best_score,
                            'match_status': 'needs_review',
                            'verification_needed': True,
                            'potential_matches': matches[:5]
                        })
                    else:
                        match_statistics['definitely_new'] += 1
                        decisions_made['new'] += 1
                        new_people.append({
                            **person,
                            'patent_number': metadata['patent_number'],
                            'patent_title': metadata['patent_title'],
                            'person_type': metadata['person_type'],
                            'person_id': f"{metadata['patent_number']}_{metadata['person_type']}_{processed_people}",
                            'match_score': best_score,
                            'match_status': 'new'
                        })
                
                logger.info(f"Batch {batch_num + 1}: Decisions - {decisions_made['existing']} existing, {decisions_made['review']} review, {decisions_made['new']} new")
                print(f"PROGRESS: Batch {batch_num + 1} - Decisions: {decisions_made['existing']} existing, {decisions_made['review']} review, {decisions_made['new']} new")
            
            # Progress reporting
            batch_time = time.time() - batch_start_time
            elapsed = time.time() - start_time
            patents_processed = end_idx
            rate = patents_processed / elapsed if elapsed > 0 else 0
            eta_minutes = ((len(us_patents_only) - patents_processed) / rate / 60) if rate > 0 else 0
            
            progress_msg = f"Batch {batch_num + 1}/{total_batches} - Patent {patents_processed:,}/{len(us_patents_only):,} ({patents_processed/len(us_patents_only)*100:.1f}%) - People: {processed_people:,}"
            if using_sql:
                progress_msg += f" - SQL Queries: {match_statistics['sql_queries_executed']}"
            progress_msg += f" - Rate: {rate:.1f}/sec - ETA: {eta_minutes:.1f}min"
            
            logger.info(progress_msg)
            print(f"PROGRESS: {progress_msg}")
        
        # Optional deduplication
        dedup_removed = 0
        if self.config.get('DEDUP_NEW_PEOPLE', True) and new_people:
            before = len(new_people)
            new_people = self._dedup_new_people(new_people)
            dedup_removed = before - len(new_people)

        if self.skip_enriched_filter:
            if new_people:
                logger.info("Skipping already-enriched filter per configuration (integration-only run)")
                print("PROGRESS: Skipping already-enriched filter (integration-only run)")
        elif new_people:
            new_people, existing_enriched_filtered = self._filter_already_enriched_people(new_people)
            if existing_enriched_filtered:
                logger.info(
                    f"Filtered {len(existing_enriched_filtered):,} already-enriched people before Step 2 hand-off"
                )
                print(
                    f"PROGRESS: Removed {len(existing_enriched_filtered):,} already-enriched people prior to Step 2"
                )

        total_elapsed = time.time() - start_time
        logger.info(f"Batch processing complete in {total_elapsed/60:.1f} minutes")
        print(f"PROGRESS: COMPLETE - {len(new_patents):,} new patents, {len(new_people):,} people in {total_elapsed/60:.1f} minutes")

        data_source = 'sql_database_batch_queries' if using_sql else 'csv_files'

        return {
            'new_patents': new_patents,
            'new_people': new_people,
            'existing_people_found': existing_people_found,
            'existing_enriched_people_filtered': existing_enriched_filtered,
            'duplicate_patents_count': duplicate_patents,
            'duplicate_people_count': len(existing_people_found),
            'total_original_patents': len(us_patents_only),
            'total_original_people': total_xml_people,
            'match_statistics': match_statistics,
            'processing_time_minutes': total_elapsed / 60,
            'data_source': data_source,
            'dedup_new_people_removed': dedup_removed
        }

    def _person_signature(self, person: Dict[str, Any]) -> str:
        """Normalize person fields into a stable signature for enrichment lookups."""
        first = self._clean_string(person.get('first_name', '')).lower()
        last = self._clean_string(person.get('last_name', '')).lower()
        city = self._clean_string(person.get('city', '')).lower()
        state = self._clean_string(person.get('state', '')).lower()
        return '|'.join([first, last, city, state])

    def _load_enriched_person_signatures(self) -> set:
        """Load signatures for people already enriched (cached per run)."""
        if self._enriched_person_signatures is not None:
            return self._enriched_person_signatures

        if not self.use_sql:
            self._enriched_person_signatures = set()
            return self._enriched_person_signatures

        try:
            query = (
                "SELECT LOWER(TRIM(first_name)) AS first_name, "
                "LOWER(TRIM(last_name)) AS last_name, "
                "LOWER(TRIM(COALESCE(city,''))) AS city, "
                "LOWER(TRIM(COALESCE(state,''))) AS state "
                "FROM enriched_people "
                "WHERE enrichment_data IS NOT NULL"
            )
            rows = self.db_manager.execute_query(query)
            signatures = set()
            for row in rows:
                first = (row.get('first_name') or '').strip()
                last = (row.get('last_name') or '').strip()
                city = (row.get('city') or '').strip()
                state = (row.get('state') or '').strip()
                sig = '|'.join([first, last, city, state])
                if sig.strip('|'):
                    signatures.add(sig)
            self._enriched_person_signatures = signatures
            logger.info(f"Loaded {len(signatures):,} already-enriched signatures for Step 1 filtering")
        except Exception as exc:
            logger.warning(f"Could not load enriched_people signatures: {exc}")
            self._enriched_person_signatures = set()
        return self._enriched_person_signatures

    def _filter_already_enriched_people(self, people: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Split people into enrichment vs already-enriched buckets."""
        if not people:
            return [], []

        signatures = self._load_enriched_person_signatures()
        if not signatures:
            return people, []

        keep: List[Dict[str, Any]] = []
        filtered: List[Dict[str, Any]] = []
        for person in people:
            sig = self._person_signature(person)
            if sig and sig in signatures:
                filtered_person = dict(person)
                filtered_person.setdefault('match_status', 'already_enriched')
                filtered_person['skip_reason'] = 'already_enriched'
                filtered.append(filtered_person)
            else:
                keep.append(person)
        return keep, filtered

    def _clean_name_for_matching(self, name: str) -> str:
        """Clean name for matching - remove ALL middle initials/names, keep only first name"""
        if not name:
            return ''
        
        # Convert to lowercase and strip whitespace
        cleaned = str(name).lower().strip()
        
        # DEBUG: Log name cleaning for troubleshooting
        original = cleaned
        
        # Remove everything after the first name - be aggressive
        # "Adam C." -> "adam"
        # "Thomas Durkee" -> "thomas" 
        # "Nicholas John" -> "nicholas"
        # "Roy S." -> "roy"
        parts = cleaned.split()
        if parts:
            cleaned = parts[0]  # Keep only the first word
        
        # # DEBUG: Log name cleaning result
        # if original != cleaned:
        #     logger.info(f"NAME CLEANING: '{original}' → '{cleaned}'")
        
        return cleaned

    def _clean_lastname_for_matching(self, lastname: str) -> str:
        """Clean lastname by removing common suffixes for better matching"""
        if not lastname:
            return ''
        
        cleaned = self._clean_string(lastname).lower().strip()
        
        # Remove common suffixes (order matters - longer suffixes first)
        suffixes_to_remove = [
            ', jr.', ', jr', ' jr.', ' jr',
            ', sr.', ', sr', ' sr.', ' sr', 
            ', ii', ', iii', ', iv', ', v',
            ' ii', ' iii', ' iv', ' v'
        ]
        
        for suffix in suffixes_to_remove:
            if cleaned.endswith(suffix):
                cleaned = cleaned[:-len(suffix)].strip()
                break
        
        return cleaned
    
    def _calculate_simple_match_score(self, 
                                target_first: str, target_last: str, 
                                target_city: str, target_state: str,
                                existing_first: str, existing_last: str, 
                                existing_city: str, existing_state: str) -> int:
        """Simplified match scoring - just compare 4 fields case-insensitive"""
        
        # Clean all fields for comparison
        t_first = self._clean_name_for_matching(target_first)
        t_last = self._clean_lastname_for_matching(target_last)
        t_city = self._clean_string(target_city).lower()
        t_state = self._clean_string(target_state).lower()
        
        e_first = self._clean_name_for_matching(existing_first)
        e_last = self._clean_lastname_for_matching(existing_last)
        e_city = self._clean_string(existing_city).lower()
        e_state = self._clean_string(existing_state).lower()
        
        # Must have matching last name at minimum
        if not t_last or not e_last or t_last != e_last:
            return 0
        
        # Simple scoring based on how many fields match
        score = 0
        
        # Last name matches (required to get here)
        score += 10
        
        # First name comparison
        if t_first and e_first:
            if t_first == e_first:
                score += 40  # Exact first name match
            elif t_first and e_first and t_first[0] == e_first[0]:
                score += 10  # First initial match
        
        # Location matching
        if t_state and e_state and t_state == e_state:
            score += 20  # Same state
            
            if t_city and e_city and t_city == e_city:
                score += 20  # Same city too
        
        return score
    
    def _update_match_statistics(self, stats: Dict, score: int):
        """Update match statistics"""
        if score >= 50:
            stats['score_50_perfect'] += 1
        elif score >= 25:
            stats['score_25_moved'] += 1
        elif score >= 15:
            stats['score_15_initial'] += 1
        elif score >= 10:
            stats['score_10_limited'] += 1
        elif score >= 6:
            stats['score_6_initial_exact'] += 1
        elif score >= 3:
            stats['score_3_initial_state'] += 1
        else:
            stats['no_match'] += 1
    
    def _dedup_new_people(self, people: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Deduplicate new people by name+city+state+person_type; aggregate patent numbers."""
        seen: Dict[Tuple[str, str, str, str, str], Dict[str, Any]] = {}
        for p in people:
            fn = self._clean_string(p.get('first_name', '')).lower()
            ln = self._clean_string(p.get('last_name', '')).lower()
            city = self._clean_string(p.get('city', '')).lower()
            state = self._clean_string(p.get('state', '')).lower()
            ptype = self._clean_string(p.get('person_type', '')).lower()
            key = (fn, ln, city, state, ptype)

            patent_no = p.get('patent_number')
            if key in seen:
                # Aggregate patent numbers
                assoc = seen[key].setdefault('associated_patents', set())
                if patent_no:
                    assoc.add(patent_no)
                # Optionally keep highest match score
                prev_score = seen[key].get('match_score', 0)
                cur_score = p.get('match_score', 0)
                if cur_score > prev_score:
                    seen[key]['match_score'] = cur_score
                    seen[key]['match_status'] = p.get('match_status')
                    # If the newer record has candidate matches, prefer them
                    if 'potential_matches' in p:
                        seen[key]['potential_matches'] = p.get('potential_matches')
            else:
                rec = dict(p)
                rec['associated_patents'] = set()
                if patent_no:
                    rec['associated_patents'].add(patent_no)
                seen[key] = rec

        # Convert sets to sorted lists and add counts
        deduped = []
        for rec in seen.values():
            assoc = sorted(list(rec.get('associated_patents', set())))
            rec['associated_patents'] = assoc
            rec['associated_patent_count'] = len(assoc)
            deduped.append(rec)
        return deduped
    
    def cleanup_memory(self):
        """Clean up memory after processing"""
        logger.info("Cleaning up memory...")
        self.existing_patents.clear()
        print("PROGRESS: Memory cleaned up")
    
    def _clean_string(self, value) -> str:
        """Clean string values"""
        if not value or str(value).lower() in ['nan', 'none', 'null', '']:
            return ''
        return str(value).strip()
    
    def _clean_patent_number(self, patent_num: str) -> str:
        """Clean patent number"""
        if not patent_num or str(patent_num).lower() in ['nan', 'none', '', 'null']:
            return ''
        
        clean_num = str(patent_num).strip().upper()
        # Remove common prefixes and design/plant/reissue prefixes
        clean_num = re.sub(r'^(US|USPTO|US-)', '', clean_num)
        clean_num = re.sub(r'^(D|PP|RE|H|T)', '', clean_num)
        # Drop all non-digits
        clean_num = re.sub(r'[^0-9]', '', clean_num)
        # Remove leading zeros
        clean_num = clean_num.lstrip('0')
        
        return clean_num if clean_num and clean_num.isdigit() else ''


# =============================================================================
# MAIN RUNNER FUNCTION - UPDATED TO USE BATCH SQL APPROACH
# =============================================================================

def run_existing_data_integration(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main runner function with batch SQL query approach instead of memory subset
    """
    try:
        # Step 1: Load patents from Step 0 output if present; otherwise process XML files
        xml_folder = config.get('USPC_DOWNLOAD_PATH', 'USPC_Download')
        output_folder = config.get('OUTPUT_DIR', 'output')

        step0_patents_path = Path(output_folder) / 'downloaded_patents.json'
        xml_patents = None
        input_source = 'xml_files'

        if step0_patents_path.exists():
            try:
                with open(step0_patents_path, 'r') as f:
                    xml_patents = json.load(f)
                input_source = 'step0_downloaded'
                logger.info(f"Loaded {len(xml_patents) if isinstance(xml_patents, list) else 0} patents from Step 0 output: {step0_patents_path}")
                print(f"PROGRESS: Using patents from Step 0 output ({step0_patents_path})")
            except Exception as e:
                logger.warning(f"Failed to read {step0_patents_path}: {e}. Falling back to XML files.")

        if not xml_patents:
            from classes.simple_xml_processor import process_xml_files
            logger.info("Processing XML patent files (fallback)...")
            xml_patents = process_xml_files(xml_folder, output_folder)
            input_source = 'xml_files'
        
        if not xml_patents:
            return {
                'success': False,
                'error': 'No patents found in XML files',
                'existing_patents_count': 0,
                'new_patents_count': 0,
                'duplicate_patents_count': 0
            }
        
        # Step 2: Filter to US patents FIRST
        integrator = BatchSQLQueryIntegrator(config)
        
        us_filter_result = integrator.filter_us_patents_only(xml_patents)
        us_patents_only = us_filter_result['us_patents']
        
        logger.info(f"US patent filtering: {len(us_patents_only):,} US patents from {len(xml_patents):,} total (source={input_source})")
        print(f"PROGRESS: US filtering complete - {len(us_patents_only):,} US patents will be processed")
        
        if not us_patents_only:
            return {
                'success': False,
                'error': 'No US patents found after filtering',
                'original_patents_count': len(xml_patents),
                'foreign_patents_count': us_filter_result['foreign_patents_count'],
                'us_patents_count': 0,
                'existing_patents_count': 0,
                'new_patents_count': 0,
                'duplicate_patents_count': 0,
                'us_filter_result': us_filter_result
            }
        
        # Step 3: Load existing data (patents only for SQL, or CSV fallback)
        load_result = integrator.load_existing_patents_only()
        if not load_result['success']:
            logger.info("SQL loading failed, trying CSV fallback...")
            print("PROGRESS: SQL failed, trying CSV fallback...")
            load_result = integrator.load_existing_data_from_csv()
            
            if not load_result['success']:
                # Handle no existing data case...
                logger.warning("No existing data available, treating all US patents as new")
                
                new_people_data = []
                total_xml_people = 0
                
                for patent in us_patents_only:
                    for inventor in patent.get('inventors', []):
                        new_people_data.append({
                            **inventor,
                            'patent_number': patent.get('patent_number'),
                            'patent_title': patent.get('patent_title'),
                            'person_type': 'inventor',
                            'person_id': f"{patent.get('patent_number')}_inventor_{total_xml_people}",
                            'match_status': 'new_no_data'
                        })
                        total_xml_people += 1
                    
                    for assignee in patent.get('assignees', []):
                        if assignee.get('first_name') or assignee.get('last_name'):
                            new_people_data.append({
                                **assignee,
                                'patent_number': patent.get('patent_number'),
                                'patent_title': patent.get('patent_title'),
                                'person_type': 'assignee',
                                'person_id': f"{patent.get('patent_number')}_assignee_{total_xml_people}",
                                'match_status': 'new_no_data'
                            })
                            total_xml_people += 1
                
                return {
                    'success': True,
                    'original_patents_count': len(xml_patents),
                    'foreign_patents_count': us_filter_result['foreign_patents_count'],
                    'us_patents_count': len(us_patents_only),
                    'existing_patents_count': 0,
                    'existing_people_count': 0,
                    'new_patents_count': len(us_patents_only),
                    'new_people_count': len(new_people_data),
                    'duplicate_patents_count': 0,
                    'duplicate_people_count': 0,
                    'total_xml_patents': len(us_patents_only),
                    'total_xml_people': total_xml_people,
                    'us_filter_result': us_filter_result,
                    'warning': 'No existing data found - all US patents treated as new'
                }
        
        # Step 4: Filter US patents using batch SQL queries (or CSV fallback)
        filter_result = integrator.filter_new_xml_data_batch_sql(us_patents_only, load_result)
        
        # Step 5: Clean up memory
        integrator.cleanup_memory()
        
        # Step 6: Save results
        os.makedirs(output_folder, exist_ok=True)
        
        if filter_result['new_patents']:
            with open(Path(output_folder) / 'filtered_new_patents.json', 'w') as f:
                json.dump(filter_result['new_patents'], f, indent=2, default=str)
            
            with open(Path(output_folder) / 'new_people_for_enrichment.json', 'w') as f:
                json.dump(filter_result['new_people'], f, indent=2, default=str)
            
            # Save existing people found
            existing_people_raw = filter_result['existing_people_found']
            
            with open(Path(output_folder) / 'existing_people_found.json', 'w') as f:
                json.dump(existing_people_raw, f, indent=2, default=str)

            with open(Path(output_folder) / 'existing_people_in_db.json', 'w') as f:
                json.dump(existing_people_raw, f, indent=2, default=str)
            
            logger.info(f"Saved results to {output_folder}")

        # Persist filtered already-enriched people for downstream steps
        filtered_enriched_path = Path(output_folder) / 'existing_filtered_enriched_people.json'
        with open(filtered_enriched_path, 'w') as f:
            json.dump(filter_result.get('existing_enriched_people_filtered', []), f, indent=2, default=str)
        logger.info(
            f"Saved {len(filter_result.get('existing_enriched_people_filtered', [])):,} already-enriched people to {filtered_enriched_path}"
        )

        # Create result
        result = {
            'success': True,
            'original_patents_count': len(xml_patents),
            'foreign_patents_count': us_filter_result['foreign_patents_count'],
            'us_patents_count': len(us_patents_only),
            'us_retention_rate': us_filter_result['us_retention_rate'],
            'existing_patents_count': load_result['existing_patents_count'],
            'existing_people_count': load_result.get('existing_people_count', 'FULL_DATABASE'),  # Indicate full DB access
            'new_patents_count': len(filter_result['new_patents']),
            'new_people_count': len(filter_result['new_people']),
            'duplicate_patents_count': filter_result['duplicate_patents_count'],
            'duplicate_people_count': len(filter_result['existing_people_found']),
            'filtered_existing_enriched_people_count': len(filter_result.get('existing_enriched_people_filtered', [])),
            'total_xml_patents': filter_result['total_original_patents'],
            'total_xml_people': filter_result['total_original_people'],
            'match_statistics': filter_result['match_statistics'],
            'processing_time_minutes': filter_result['processing_time_minutes'],
            'data_source': filter_result['data_source'],
            'us_filter_result': us_filter_result,
            'new_people_data': filter_result['new_people'],
            'existing_enriched_people_filtered': filter_result.get('existing_enriched_people_filtered', []),
            'dedup_new_people_removed': filter_result.get('dedup_new_people_removed', 0),
            'input_source': input_source
        }
        
        # Log comprehensive summary
        _log_comprehensive_summary(result)
        
        return result
        
    except Exception as e:
        logger.error(f"Error in batch SQL integration: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'success': False,
            'error': str(e),
            'existing_patents_count': 0,
            'new_patents_count': 0,
            'duplicate_patents_count': 0
        }

def _log_comprehensive_summary(result: Dict[str, Any]):
    """Log comprehensive summary including filtering statistics"""
    logger.info("\nCOMPREHENSIVE BATCH SQL INTEGRATION SUMMARY:")
    logger.info("=" * 70)
    
    # US Filtering Summary
    us_filter = result.get('us_filter_result', {})
    if us_filter:
        logger.info(f"FILTERING SUMMARY:")
        logger.info(f"   Total XML patents processed: {result['original_patents_count']:,}")
        logger.info(f"   US patents kept: {result['us_patents_count']:,}")
        logger.info(f"   Foreign patents filtered out: {result['foreign_patents_count']:,}")
        logger.info(f"   US retention rate: {us_filter.get('us_retention_rate', 'N/A')}")
        logger.info("")
    
    # Integration Summary
    logger.info(f"BATCH SQL INTEGRATION SUMMARY:")
    logger.info(f"   Existing patents in DB: {result['existing_patents_count']:,}")
    logger.info(f"   People comparison method: {result.get('data_source', 'unknown')}")
    if result.get('data_source') == 'sql_database_batch_queries':
        logger.info(f"   ✅ Using FULL DATABASE (not 50K subset)")
    else:
        logger.info(f"   ⚠️  Using CSV fallback")
    logger.info(f"   New patents found: {result['new_patents_count']:,}")
    logger.info(f"   New people found: {result['new_people_count']:,}")
    logger.info(f"   Duplicate patents avoided: {result['duplicate_patents_count']:,}")
    logger.info(f"   Duplicate people avoided: {result['duplicate_people_count']:,}")
    filtered_existing = result.get('filtered_existing_enriched_people_count', 0)
    if filtered_existing:
        logger.info(f"   Already-enriched filtered pre-Step 2: {filtered_existing:,}")
    logger.info(f"   Total processing time: {result.get('processing_time_minutes', 0):.1f} minutes")
    
    # Match Statistics
    match_stats = result.get('match_statistics', {})
    if match_stats:
        logger.info(f"\nMATCH SCORE BREAKDOWN:")
        logger.info(f"   SQL queries executed: {match_stats.get('sql_queries_executed', 0):,}")
        logger.info(f"   Total DB people checked: {match_stats.get('total_db_people_checked', 0):,}")
        logger.info(f"   Auto-matched (≥25): {match_stats.get('auto_matched', 0):,}")
        logger.info(f"   Needs review (10-24): {match_stats.get('needs_review', 0):,}")
        logger.info(f"   Definitely new (<10): {match_stats.get('definitely_new', 0):,}")
    
    logger.info("=" * 70)
