# =============================================================================
# runners/integrate_existing_data.py
# Step 1: Compare XML data to existing SQL or CSV data
# HYBRID: Load from SQL into memory for fast CSV-style matching
# Replaces the slow SQL approach with fast in-memory processing
# ENHANCED: Added US Patent filtering before database comparison
# FIXED: Proper US filtering sequence to prevent foreign patents in people counting
# =============================================================================

import logging
import os
import json
import time
from typing import Dict, Any, List, Tuple, Optional
from pathlib import Path
from datetime import datetime
import uuid
import re

from classes.simple_xml_processor import process_xml_files

logger = logging.getLogger(__name__)

class HybridSQLMemoryIntegrator:
    """Load from SQL database but process in memory for speed"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.existing_patents = set()
        self.existing_people = []  # List for fast in-memory matching
        self.loaded_sql_data = False
        
        # Try to connect to database
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
        """SIMPLE: Only keep patents where ALL inventors have country = 'US'"""
        
        # Check all inventors - ALL must be explicitly US
        inventors = patent.get('inventors', [])
        if not inventors:
            return False  # No inventors, can't determine origin
        
        for inventor in inventors:
            inventor_country = inventor.get('country')
            # STRICT: Must be exactly 'US', reject everything else including None/null
            if not inventor_country or str(inventor_country).upper() != 'US':
                return False
        
        # Only keep if ALL inventors are explicitly 'US'
        return True
    
    def load_existing_data_from_sql(self) -> Dict[str, Any]:
        """Load all data from SQL into memory for fast processing"""
        if not self.use_sql:
            return {'success': False, 'error': 'SQL not available'}
            
        start_time = time.time()
        logger.info("Loading existing data from SQL database into memory...")
        print("PROGRESS: Loading SQL database into memory for fast processing...")
        
        try:
            # Load patents into set for O(1) lookup
            print("PROGRESS: Loading existing patents from SQL...")
            self.existing_patents = self.existing_dao.load_existing_patents()
            logger.info(f"Loaded {len(self.existing_patents):,} existing patents")
            
            # Load people with reasonable limit to prevent memory issues
            people_limit = self.config.get('EXISTING_PEOPLE_LIMIT', 50000)
            print(f"PROGRESS: Loading up to {people_limit:,} existing people from SQL...")
            
            sql_people = self.existing_dao.load_existing_people(limit=people_limit)
            
            # Convert to fast lookup format (lowercase for matching)
            self.existing_people = []
            for person in sql_people:
                self.existing_people.append({
                    'first_name': self._clean_string(person.get('first_name', '')).lower(),
                    'last_name': self._clean_string(person.get('last_name', '')).lower(),
                    'city': self._clean_string(person.get('city', '')).lower(),
                    'state': self._clean_string(person.get('state', '')).lower(),
                    'country': self._clean_string(person.get('country', '')).lower(),
                    'source': 'sql_database',
                    'original_record': person  # Keep original for reference
                })
            
            elapsed = time.time() - start_time
            logger.info(f"SQL data loaded in {elapsed:.1f}s - {len(self.existing_patents):,} patents, {len(self.existing_people):,} people")
            print(f"PROGRESS: SQL data loaded in {elapsed:.1f}s - Ready for fast processing")
            
            self.loaded_sql_data = True
            
            return {
                'success': True,
                'existing_patents_count': len(self.existing_patents),
                'existing_people_count': len(self.existing_people),
                'source': 'sql_database',
                'loading_time_seconds': elapsed
            }
            
        except Exception as e:
            logger.error(f"Error loading from SQL: {e}")
            return {'success': False, 'error': str(e)}
    
    def load_existing_data_from_csv(self) -> Dict[str, Any]:
        """Fallback: Load from CSV files if SQL not available"""
        csv_folder = self.config.get('CSV_DATABASE_FOLDER', 'converted_databases/csv')
        csv_path = Path(csv_folder)
        
        if not csv_path.exists():
            return {'success': False, 'error': f'CSV folder not found: {csv_folder}'}
        
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
            
            # Limit files for memory management
            MAX_FILES = 20
            files_to_process = csv_files[:MAX_FILES]
            
            for i, csv_file in enumerate(files_to_process):
                logger.info(f"Loading {csv_file.name} ({i+1}/{len(files_to_process)})")
                print(f"PROGRESS: Loading file {i+1}/{len(files_to_process)}: {csv_file.name}")
                
                try:
                    # Load with row limit to prevent memory issues
                    df = pd.read_csv(csv_file, encoding='utf-8', low_memory=False, nrows=10000)
                    self._extract_from_dataframe(df, csv_file.name)
                except Exception as e:
                    logger.error(f"Error loading {csv_file}: {e}")
                    continue
            
            elapsed = time.time() - start_time
            logger.info(f"CSV data loaded in {elapsed:.1f}s - {len(self.existing_patents):,} patents, {len(self.existing_people):,} people")
            print(f"PROGRESS: CSV data loaded in {elapsed:.1f}s - Ready for processing")
            
            return {
                'success': True,
                'existing_patents_count': len(self.existing_patents),
                'existing_people_count': len(self.existing_people),
                'source': 'csv_files',
                'files_processed': len(files_to_process),
                'loading_time_seconds': elapsed
            }
            
        except Exception as e:
            logger.error(f"Error loading from CSV: {e}")
            return {'success': False, 'error': str(e)}
    
    def _extract_from_dataframe(self, df, filename: str):
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
                    self.existing_people.append({
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
    
    def find_person_matches_fast(self, target_person: Dict[str, str]) -> List[Tuple[Dict, int]]:
        """Fast in-memory person matching using VBA-style scoring"""
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
        
        for existing_person in self.existing_people:
            if matches_found >= max_matches:
                break
            
            # Quick elimination: last name must match
            if target_last != existing_person.get('last_name', ''):
                continue
            
            score = self._calculate_vba_match_score(
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
    
    def _calculate_vba_match_score(self, 
                                target_first: str, target_last: str, 
                                target_city: str, target_state: str,
                                existing_first: str, existing_last: str, 
                                existing_city: str, existing_state: str) -> int:
        """VBA-style match scoring - same as your original logic"""
        
        if not target_last or not existing_last or target_last != existing_last:
            return 0
        
        # Boolean flags for readability
        names_match = target_first == existing_first
        cities_match = target_city and existing_city and target_city == existing_city
        states_match = target_state and existing_state and target_state == existing_state
        cities_different = target_city and existing_city and target_city != existing_city
        first_initial_match = (target_first and existing_first and 
                             target_first[0] == existing_first[0])
        
        # VBA-style scoring (from your original comparison code)
        if names_match and cities_match and states_match:
            return 50  # Perfect match
        
        if names_match and states_match and not cities_different:
            return 45  # Same name/state, no city conflict
        
        if names_match and states_match and cities_different:
            return 40  # Moved cities
        
        if names_match and (states_match or not target_state or not existing_state):
            return 35  # Name match, limited location
        
        if names_match and states_match:
            return 25  # Original VBA "moved" score
        
        if first_initial_match and states_match and cities_different:
            return 15  # First initial match, different cities
        
        if names_match:
            return 12  # Name match, no location
        
        if first_initial_match and cities_match and states_match:
            return 6  # First initial, exact location
        
        if first_initial_match and states_match:
            return 3  # First initial, state only
        
        return 0
    
    def filter_new_xml_data_fast(self, us_patents_only: List[Dict]) -> Dict[str, Any]:
        """
        FIXED: Fast filtering with in-memory matching - ONLY processes US patents
        This function should only receive US patents, ensuring people count matches patent count
        """
        logger.info(f"Fast filtering of {len(us_patents_only)} US patents using in-memory data")
        print(f"PROGRESS: Starting fast patent processing - {len(us_patents_only):,} US patents only")
        
        # Verify we're only getting US patents
        non_us_count = 0
        for patent in us_patents_only:
            for inventor in patent.get('inventors', []):
                if inventor.get('country') != 'US':
                    non_us_count += 1
        
        if non_us_count > 0:
            logger.warning(f"WARNING: Found {non_us_count} non-US inventors in supposedly US-only patents!")
            print(f"WARNING: {non_us_count} non-US inventors found - filtering may have failed!")
        
        # Thresholds from VBA analysis
        AUTO_MATCH_THRESHOLD = 25
        REVIEW_THRESHOLD = 10
        
        # Results
        new_patents = []
        new_people = []
        existing_people_found = []
        duplicate_patents = 0
        total_xml_people = 0
        processed_people = 0
        
        # Statistics
        match_statistics = {
            'score_50_perfect': 0, 'score_25_moved': 0, 'score_15_initial': 0,
            'score_10_limited': 0, 'score_6_initial_exact': 0, 'score_3_initial_state': 0,
            'no_match': 0, 'auto_matched': 0, 'needs_review': 0, 'definitely_new': 0
        }
        
        start_time = time.time()
        
        # Process in batches for better progress reporting
        BATCH_SIZE = 100
        total_batches = (len(us_patents_only) + BATCH_SIZE - 1) // BATCH_SIZE
        
        for batch_num in range(total_batches):
            batch_start_time = time.time()
            start_idx = batch_num * BATCH_SIZE
            end_idx = min((batch_num + 1) * BATCH_SIZE, len(us_patents_only))
            batch_patents = us_patents_only[start_idx:end_idx]
            
            for patent in batch_patents:
                patent_number = self._clean_patent_number(patent.get('patent_number', ''))
                is_new_patent = patent_number not in self.existing_patents
                
                if is_new_patent:
                    new_patents.append(patent)
                    
                    # Process inventors (should all be US)
                    for inventor in patent.get('inventors', []):
                        total_xml_people += 1
                        processed_people += 1
                        
                        matches = self.find_person_matches_fast(inventor)
                        best_score = matches[0][1] if matches else 0
                        
                        self._update_match_statistics(match_statistics, best_score)
                        
                        if best_score >= AUTO_MATCH_THRESHOLD:
                            match_statistics['auto_matched'] += 1
                            existing_people_found.append({
                                **inventor,
                                'match_score': best_score,
                                'match_reason': 'auto_matched'
                            })
                        elif best_score >= REVIEW_THRESHOLD:
                            match_statistics['needs_review'] += 1
                            new_people.append({
                                **inventor,
                                'patent_number': patent_number,
                                'patent_title': patent.get('patent_title'),
                                'person_type': 'inventor',
                                'person_id': f"{patent_number}_inventor_{processed_people}",
                                'match_score': best_score,
                                'match_status': 'needs_review',
                                'verification_needed': True,
                                # Include top candidate matches for frontend review
                                'potential_matches': matches[:5]
                            })
                        else:
                            match_statistics['definitely_new'] += 1
                            new_people.append({
                                **inventor,
                                'patent_number': patent_number,
                                'patent_title': patent.get('patent_title'),
                                'person_type': 'inventor',
                                'person_id': f"{patent_number}_inventor_{processed_people}",
                                'match_score': best_score,
                                'match_status': 'new'
                            })
                    
                    # Process assignees with names (should also be US)
                    for assignee in patent.get('assignees', []):
                        if assignee.get('first_name') or assignee.get('last_name'):
                            total_xml_people += 1
                            processed_people += 1
                            
                            matches = self.find_person_matches_fast(assignee)
                            best_score = matches[0][1] if matches else 0
                            
                            if best_score >= AUTO_MATCH_THRESHOLD:
                                match_statistics['auto_matched'] += 1
                                existing_people_found.append({
                                    **assignee,
                                    'match_score': best_score,
                                    'match_reason': 'auto_matched'
                                })
                            elif best_score >= REVIEW_THRESHOLD:
                                match_statistics['needs_review'] += 1
                                new_people.append({
                                    **assignee,
                                    'patent_number': patent_number,
                                    'patent_title': patent.get('patent_title'),
                                    'person_type': 'assignee',
                                    'person_id': f"{patent_number}_assignee_{processed_people}",
                                    'match_score': best_score,
                                    'match_status': 'needs_review',
                                    'verification_needed': True,
                                    # Include top candidate matches for frontend review
                                    'potential_matches': matches[:5]
                                })
                            else:
                                match_statistics['definitely_new'] += 1
                                new_people.append({
                                    **assignee,
                                    'patent_number': patent_number,
                                    'patent_title': patent.get('patent_title'),
                                    'person_type': 'assignee',
                                    'person_id': f"{patent_number}_assignee_{processed_people}",
                                    'match_score': best_score,
                                    'match_status': 'new'
                                })
                else:
                    duplicate_patents += 1
                    # Count people for statistics even from duplicate patents
                    for inventor in patent.get('inventors', []):
                        total_xml_people += 1
                    for assignee in patent.get('assignees', []):
                        if assignee.get('first_name') or assignee.get('last_name'):
                            total_xml_people += 1
            
            # Batch progress
            batch_time = time.time() - batch_start_time
            elapsed = time.time() - start_time
            patents_processed = end_idx
            rate = patents_processed / elapsed if elapsed > 0 else 0
            eta_minutes = ((len(us_patents_only) - patents_processed) / rate / 60) if rate > 0 else 0
            
            progress_msg = f"Batch {batch_num + 1}/{total_batches} - Patent {patents_processed:,}/{len(us_patents_only):,} ({patents_processed/len(us_patents_only)*100:.1f}%) - People: {processed_people:,} - Rate: {rate:.1f}/sec - ETA: {eta_minutes:.1f}min"
            logger.info(progress_msg)
            print(f"PROGRESS: {progress_msg}")
        
        # Optional deduplication of new people across patents
        dedup_removed = 0
        if self.config.get('DEDUP_NEW_PEOPLE', True) and new_people:
            before = len(new_people)
            new_people = self._dedup_new_people(new_people)
            dedup_removed = before - len(new_people)
            print(f"PROGRESS: Deduplicated new people - removed {dedup_removed:,}, remaining {len(new_people):,}")

        total_elapsed = time.time() - start_time
        logger.info(f"Fast processing complete in {total_elapsed/60:.1f} minutes")
        print(f"PROGRESS: COMPLETE - {len(new_patents):,} new patents, {len(new_people):,} people in {total_elapsed/60:.1f} minutes")
        
        return {
            'new_patents': new_patents,
            'new_people': new_people,
            'existing_people_found': existing_people_found,
            'duplicate_patents_count': duplicate_patents,
            'duplicate_people_count': len(existing_people_found),
            'total_original_patents': len(us_patents_only),  # FIXED: Only US patents
            'total_original_people': total_xml_people,
            'match_statistics': match_statistics,
            'processing_time_minutes': total_elapsed / 60,
            'data_source': 'sql_database' if self.loaded_sql_data else 'csv_files',
            'dedup_new_people_removed': dedup_removed
        }

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
        self.existing_people.clear()
        print("PROGRESS: Memory cleaned up")
    
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
# MAIN RUNNER FUNCTION - FIXED SEQUENCE TO ENSURE PROPER US FILTERING
# =============================================================================

def run_existing_data_integration(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    FIXED: Main runner function with proper US patent filtering sequence
    The bug was that people counting was happening before/alongside filtering
    """
    try:
        # Step 1: Process XML files (all patents)
        xml_folder = config.get('USPC_DOWNLOAD_PATH', 'USPC_Download')
        output_folder = config.get('OUTPUT_DIR', 'output')
        
        logger.info("Processing XML patent files...")
        xml_patents = process_xml_files(xml_folder, output_folder)
        
        if not xml_patents:
            return {
                'success': False,
                'error': 'No patents found in XML files',
                'existing_patents_count': 0,
                'new_patents_count': 0,
                'duplicate_patents_count': 0
            }
        
        # Step 2: Filter to US patents FIRST, before any people processing
        integrator = HybridSQLMemoryIntegrator(config)
        
        us_filter_result = integrator.filter_us_patents_only(xml_patents)
        us_patents_only = us_filter_result['us_patents']  # CRITICAL: Only pass US patents forward
        
        logger.info(f"US patent filtering: {len(us_patents_only):,} US patents from {len(xml_patents):,} total")
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
        
        # Step 3: Load existing data (SQL first, CSV fallback)
        load_result = integrator.load_existing_data_from_sql()
        if not load_result['success']:
            logger.info("SQL loading failed, trying CSV fallback...")
            print("PROGRESS: SQL failed, trying CSV fallback...")
            load_result = integrator.load_existing_data_from_csv()
            
            if not load_result['success']:
                # No existing data available - treat everything as new
                logger.warning("No existing data available, treating all US patents as new")
                
                new_people_data = []
                total_xml_people = 0
                
                for patent in us_patents_only:  # FIXED: Use us_patents_only, not xml_patents
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
                    'total_xml_patents': len(us_patents_only),  # FIXED: Count US patents only
                    'total_xml_people': total_xml_people,
                    'us_filter_result': us_filter_result,
                    'warning': 'No existing data found - all US patents treated as new'
                }
        
        # Step 4: Filter US patents using fast in-memory matching
        # CRITICAL: Pass us_patents_only, not xml_patents
        filter_result = integrator.filter_new_xml_data_fast(us_patents_only)
        
        # Step 5: Clean up memory
        integrator.cleanup_memory()
        
        # Step 6: Save results to JSON files
        os.makedirs(output_folder, exist_ok=True)
        
        if filter_result['new_patents']:
            with open(Path(output_folder) / 'filtered_new_patents.json', 'w') as f:
                json.dump(filter_result['new_patents'], f, indent=2, default=str)
            
            with open(Path(output_folder) / 'new_people_for_enrichment.json', 'w') as f:
                json.dump(filter_result['new_people'], f, indent=2, default=str)
            
            with open(Path(output_folder) / 'existing_people_found.json', 'w') as f:
                json.dump(filter_result['existing_people_found'], f, indent=2, default=str)
            
            logger.info(f"Saved results to {output_folder}")
        
        # Create comprehensive result with filtering summary
        result = {
            'success': True,
            'original_patents_count': len(xml_patents),
            'foreign_patents_count': us_filter_result['foreign_patents_count'],
            'us_patents_count': len(us_patents_only),
            'us_retention_rate': us_filter_result['us_retention_rate'],
            'existing_patents_count': load_result['existing_patents_count'],
            'existing_people_count': load_result['existing_people_count'],
            'new_patents_count': len(filter_result['new_patents']),
            'new_people_count': len(filter_result['new_people']),
            'duplicate_patents_count': filter_result['duplicate_patents_count'],
            'duplicate_people_count': filter_result['duplicate_people_count'],
            'total_xml_patents': filter_result['total_original_patents'],  # Should now be US patents only
            'total_xml_people': filter_result['total_original_people'],    # Should now be US people only
            'match_statistics': filter_result['match_statistics'],
            'processing_time_minutes': filter_result['processing_time_minutes'],
            'data_source': filter_result['data_source'],
            'us_filter_result': us_filter_result,
            'new_people_data': filter_result['new_people'],  # For enrichment step
            'dedup_new_people_removed': filter_result.get('dedup_new_people_removed', 0)
        }
        
        # Log comprehensive summary
        _log_comprehensive_summary(result)
        
        return result
        
    except Exception as e:
        logger.error(f"Error in hybrid integration: {e}")
        return {
            'success': False,
            'error': str(e),
            'existing_patents_count': 0,
            'new_patents_count': 0,
            'duplicate_patents_count': 0
        }


def _log_comprehensive_summary(result: Dict[str, Any]):
    """Log comprehensive summary including filtering statistics"""
    logger.info("\nCOMPREHENSIVE INTEGRATION SUMMARY:")
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
    logger.info(f"INTEGRATION SUMMARY:")
    logger.info(f"   Existing patents in DB: {result['existing_patents_count']:,}")
    logger.info(f"   Existing people in DB: {result['existing_people_count']:,}")
    logger.info(f"   New patents found: {result['new_patents_count']:,}")
    logger.info(f"   New people found: {result['new_people_count']:,}")
    logger.info(f"   Duplicate patents avoided: {result['duplicate_patents_count']:,}")
    logger.info(f"   Duplicate people avoided: {result['duplicate_people_count']:,}")
    logger.info(f"   Total processing time: {result.get('processing_time_minutes', 0):.1f} minutes")
    
    # Match Statistics
    match_stats = result.get('match_statistics', {})
    if match_stats:
        logger.info(f"\nMATCH SCORE BREAKDOWN:")
        logger.info(f"   No Score/Score 0: {match_stats.get('no_match', 0):,}")
        logger.info(f"   Score 1-9 (Very Low): {match_stats.get('score_3_initial_state', 0) + match_stats.get('score_6_initial_exact', 0):,}")
        logger.info(f"   Score 10-19 (Needs Review): {match_stats.get('score_10_limited', 0) + match_stats.get('score_15_initial', 0):,}")
        logger.info(f"   Score 25-49 (High Conf): {match_stats.get('score_25_moved', 0):,}")
        logger.info(f"   Score 50-74 (Very High): {match_stats.get('score_50_perfect', 0):,}")
    
    # Cost Savings
    if result.get('duplicate_people_count', 0) > 0:
        api_calls_saved = result['duplicate_people_count']
        cost_saved = api_calls_saved * 0.03
        cost_for_new = result['new_people_count'] * 0.03
        
        logger.info(f"\nCOST SAVINGS:")
        logger.info(f"   API calls avoided: {api_calls_saved:,}")
        logger.info(f"   Estimated cost saved: ${cost_saved:.2f}")
        logger.info(f"   Cost for new people: ${cost_for_new:.2f}")
    
    logger.info("=" * 70)
