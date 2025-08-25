# =============================================================================
# runners/integrate_existing_data.py
# Enhanced Step 1: Integrate XML data with existing CSV databases using VBA-style matching
# =============================================================================
import pandas as pd
import logging
import os
from pathlib import Path
from typing import Dict, Any, List, Tuple
from classes.simple_xml_processor import process_xml_files
import json
import time

logger = logging.getLogger(__name__)

class EnhancedCSVDatabaseIntegrator:
    """Enhanced integrator with VBA-style multi-tiered matching"""
    
    def __init__(self, csv_database_folder: str):
        self.csv_database_folder = Path(csv_database_folder)
        self.existing_patents = set()
        # Store people as structured records instead of simple strings
        self.existing_people = []  # List of dicts with name/location info
        self.loaded_databases = []
        
    def load_existing_databases(self) -> Dict[str, Any]:
        """Load all CSV databases and create lookup sets"""
        logger.info(f"Loading existing databases from {self.csv_database_folder}")
        
        if not self.csv_database_folder.exists():
            logger.warning(f"CSV database folder not found: {self.csv_database_folder}")
            return {'success': False, 'error': 'CSV database folder not found'}
        
        csv_files = list(self.csv_database_folder.glob("*.csv"))
        if not csv_files:
            logger.warning(f"No CSV files found in {self.csv_database_folder}")
            return {'success': False, 'error': 'No CSV files found'}
        
        logger.info(f"Found {len(csv_files)} CSV files to process")
        
        total_patents = 0
        total_people = 0
        
        # Process the first 10 files to avoid memory issues while testing
        for csv_file in csv_files[:10]:
            try:
                logger.info(f"Loading {csv_file.name}")
                # Read with low_memory=False to handle mixed types
                df = pd.read_csv(csv_file, encoding='utf-8', low_memory=False, nrows=10000)
                
                # Extract patents and people based on expected column names
                patents_found, people_found = self._extract_from_dataframe(df, csv_file.name)
                total_patents += patents_found
                total_people += people_found
                
                self.loaded_databases.append({
                    'file': csv_file.name,
                    'patents': patents_found,
                    'people': people_found,
                    'columns': list(df.columns)
                })
                
            except Exception as e:
                logger.error(f"Error loading {csv_file}: {e}")
                continue
        
        logger.info(f"Loaded {len(self.loaded_databases)} databases")
        logger.info(f"Total existing patents: {len(self.existing_patents)}")
        logger.info(f"Total existing people: {len(self.existing_people)}")
        
        return {
            'success': True,
            'databases_loaded': len(self.loaded_databases),
            'existing_patents_count': len(self.existing_patents),
            'existing_people_count': len(self.existing_people),
            'database_details': self.loaded_databases
        }
    
    def _extract_from_dataframe(self, df: pd.DataFrame, filename: str) -> Tuple[int, int]:
        """Extract patent numbers and people from a dataframe"""
        patents_count = 0
        people_count = 0
        
        logger.debug(f"Processing {filename} with columns: {list(df.columns)[:10]}")
        
        # ========== PATENT EXTRACTION (unchanged) ==========
        patent_columns = ['patent_number', 'patent_id', 'publication_number', 'doc_number', 'number', 'patentno', 'patent', 'patent_num']
        patent_col = None
        
        # Find patent column (case insensitive)
        for col in df.columns:
            if any(pcol.lower() in col.lower() for pcol in patent_columns):
                patent_col = col
                logger.debug(f"Found patent column: {patent_col}")
                break
        
        if patent_col:
            # Add patent numbers to set (remove any formatting)
            patent_numbers = df[patent_col].dropna().astype(str)
            for patent_num in patent_numbers:
                clean_patent = self._clean_patent_number(patent_num)
                if clean_patent:
                    self.existing_patents.add(clean_patent)
                    patents_count += 1
        
        # ========== ENHANCED PEOPLE EXTRACTION ==========
        # Find name columns (case insensitive)
        first_name_col = self._find_column(df.columns, 
            ['first_name', 'firstname', 'fname', 'first', 'inventor_first', 'inventor_name_first'])
        last_name_col = self._find_column(df.columns, 
            ['last_name', 'lastname', 'lname', 'last', 'inventor_last', 'inventor_name_last'])
        
        if first_name_col and last_name_col:
            logger.debug(f"Found name columns: {first_name_col}, {last_name_col}")
            
            # Find location columns
            city_col = self._find_column(df.columns, 
                ['city', 'inventor_city', 'location_city'])
            state_col = self._find_column(df.columns, 
                ['state', 'inventor_state', 'location_state'])
            country_col = self._find_column(df.columns, 
                ['country', 'inventor_country', 'location_country'])
            
            for _, row in df.iterrows():
                first_name = self._clean_string(row.get(first_name_col))
                last_name = self._clean_string(row.get(last_name_col))
                
                if first_name or last_name:  # At least one name component
                    person_record = {
                        'first_name': first_name,
                        'last_name': last_name,
                        'city': self._clean_string(row.get(city_col, '')) if city_col else '',
                        'state': self._clean_string(row.get(state_col, '')) if state_col else '',
                        'country': self._clean_string(row.get(country_col, '')) if country_col else '',
                        'source_file': filename
                    }
                    self.existing_people.append(person_record)
                    people_count += 1
        
        logger.info(f"{filename}: Found {patents_count} patents, {people_count} people")
        return patents_count, people_count
    
    def _find_column(self, columns: List[str], patterns: List[str]) -> str:
        """Find column that matches any of the patterns (case insensitive)"""
        for col in columns:
            col_lower = col.lower()
            if any(pattern.lower() in col_lower for pattern in patterns):
                return col
        return None
    
    def _clean_string(self, value) -> str:
        """Clean and normalize string values"""
        if not value or str(value).lower() in ['nan', 'none', 'null', '']:
            return ''
        return str(value).strip()
    
    def _clean_patent_number(self, patent_num: str) -> str:
        """Clean and standardize patent number format"""
        if not patent_num or str(patent_num).lower() in ['nan', 'none', '', 'null']:
            return None
        
        # Remove common prefixes and clean
        clean_num = str(patent_num).strip().upper()
        clean_num = clean_num.replace('US', '').replace('USPTO', '')
        clean_num = clean_num.replace(',', '').replace(' ', '').replace('-', '')
        
        # Remove leading zeros but keep the number
        clean_num = clean_num.lstrip('0')
        
        return clean_num if clean_num and clean_num.isdigit() else None
    
    def find_person_matches(self, target_person: Dict[str, str]) -> List[Tuple[Dict, int]]:
        """
        Find matches for a person using VBA-style scoring logic
        Returns list of (matching_person, score) tuples sorted by score
        """
        matches = []
        
        target_first = self._clean_string(target_person.get('first_name', '')).lower()
        target_last = self._clean_string(target_person.get('last_name', '')).lower()
        target_city = self._clean_string(target_person.get('city', '')).lower()
        target_state = self._clean_string(target_person.get('state', '')).lower()
        
        if not target_first and not target_last:
            return matches
        
        for existing_person in self.existing_people:
            existing_first = self._clean_string(existing_person.get('first_name', '')).lower()
            existing_last = self._clean_string(existing_person.get('last_name', '')).lower()
            existing_city = self._clean_string(existing_person.get('city', '')).lower()
            existing_state = self._clean_string(existing_person.get('state', '')).lower()
            
            score = self._calculate_person_match_score(
                target_first, target_last, target_city, target_state,
                existing_first, existing_last, existing_city, existing_state
            )
            
            if score > 0:
                matches.append((existing_person, score))
        
        # Sort by score (highest first)
        matches.sort(key=lambda x: x[1], reverse=True)
        return matches
    
    def _calculate_person_match_score(self, 
                                target_first: str, target_last: str, 
                                target_city: str, target_state: str,
                                existing_first: str, existing_last: str, 
                                existing_city: str, existing_state: str) -> int:
        """
        Enhanced match score calculation with better name matching
        Handles cases like "Duncan Robert Kerr" vs "Duncan Kerr"
        
        Scores based on the original patent processing system:
        - 50: Perfect match (names + location match)
        - 25: Same name, same state, different city  
        - 15: First initial + last name, same state, different city
        - 10: Same name, no location or limited location data
        - 6: First initial + last name, same city and state
        - 3: First initial + last name, same state only
        """
        
        # Skip if we don't have enough data to match
        if not target_last or not existing_last:
            return 0
        
        # Enhanced name matching - handle middle names and variations
        def names_match_closely(name1: str, name2: str) -> bool:
            """Check if names match accounting for middle names/initials"""
            if not name1 or not name2:
                return False
            
            # Exact match
            if name1 == name2:
                return True
            
            # One name is contained in the other (e.g., "Duncan" in "Duncan Robert")
            name1_words = name1.split()
            name2_words = name2.split()
            
            # Check if first words match (most common case)
            if name1_words[0] == name2_words[0]:
                return True
            
            # Check if either name is a subset of the other
            if name1 in name2 or name2 in name1:
                return True
            
            return False
        
        def first_initials_match(name1: str, name2: str) -> bool:
            """Check if first initials match"""
            if not name1 or not name2:
                return False
            return name1[0].upper() == name2[0].upper()
        
        # Determine name match levels
        first_names_match = names_match_closely(target_first, existing_first)
        last_names_match = target_last == existing_last
        first_initials_match_bool = first_initials_match(target_first, existing_first)
        
        # Location matching
        cities_match = target_city and existing_city and target_city == existing_city
        states_match = target_state and existing_state and target_state == existing_state
        cities_different = target_city and existing_city and target_city != existing_city
        
        # Enhanced scoring logic
        
        # SCORE 50: Perfect match - names match closely + exact location
        if (first_names_match and last_names_match and cities_match and states_match):
            return 50
        
        # SCORE 45: Very close match - names match closely + same state (no city conflict)
        if (first_names_match and last_names_match and states_match and not cities_different):
            return 45
        
        # SCORE 40: Close match - names match closely + same state + different cities
        if (first_names_match and last_names_match and states_match and cities_different):
            return 40
        
        # SCORE 35: Names match closely but limited location data
        if (first_names_match and last_names_match and (states_match or not target_state or not existing_state)):
            return 35
        
        # SCORE 25: Original logic - same full names, same state, different city
        if (target_first and existing_first and 
            target_first == existing_first and 
            target_last == existing_last and
            states_match and cities_different):
            return 25
        
        # SCORE 20: Enhanced - first name contained in other + last name + location
        if (first_names_match and last_names_match and cities_match and states_match):
            return 20  # This should have been caught by score 50, but just in case
        
        # SCORE 15: First initial + last name, same state, different city
        if (first_initials_match_bool and last_names_match and states_match and cities_different):
            return 15
        
        # SCORE 12: Enhanced - names match closely but no location data
        if (first_names_match and last_names_match):
            return 12
        
        # SCORE 10: Original logic - same full names, limited location data
        if (target_first and existing_first and 
            target_first == existing_first and 
            target_last == existing_last):
            
            # If states match but no city data, or missing location entirely
            if states_match:
                return 10
            
            # Names match but no reliable location data
            if not target_state or not existing_state:
                return 8
        
        # SCORE 6: First initial + last name with same city and state
        if (first_initials_match_bool and last_names_match and cities_match and states_match):
            return 6
        
        # SCORE 3: First initial + last name with same state only
        if (first_initials_match_bool and last_names_match and states_match):
            return 3
        
        return 0

    def filter_new_xml_data(self, xml_patents: List[Dict]) -> Dict[str, Any]:
        """
        Enhanced filtering using multi-tiered matching with configurable thresholds
        WITH COMPREHENSIVE PROGRESS REPORTING for frontend polling
        """
        logger.info(f"Enhanced filtering of {len(xml_patents)} XML patents against existing data")
        
        # Process ALL patents (no limit)
        logger.info(f"Processing ALL {len(xml_patents)} patents (no limit)")
        
        # Configurable thresholds based on VBA analysis
        AUTO_MATCH_THRESHOLD = 25  # Auto-consider as existing person
        REVIEW_THRESHOLD = 10      # Flag for potential review
        
        new_patents = []
        new_people = []
        existing_people_found = []  # People flagged as existing
        duplicate_patents = 0
        total_xml_people = 0
        processed_patents = 0
        processed_people = 0
        
        # Detailed match statistics
        match_statistics = {
            'score_50_perfect': 0,     # Perfect matches
            'score_25_moved': 0,       # Person moved cities
            'score_15_initial': 0,     # First initial matches
            'score_10_limited': 0,     # Limited location data
            'score_6_initial_exact': 0, # First initial, exact location
            'score_3_initial_state': 0, # First initial, state only
            'no_match': 0,             # No matches found
            'auto_matched': 0,         # Above auto-match threshold
            'needs_review': 0,         # Between thresholds
            'definitely_new': 0        # Below review threshold
        }
        
        # ENHANCED PROGRESS TRACKING
        start_time = time.time()
        last_log_time = start_time
        last_major_log_time = start_time
        
        # Progress milestones for more granular reporting
        progress_milestones = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95]
        completed_milestones = set()
        
        for patent in xml_patents:
            processed_patents += 1
            current_time = time.time()
            
            # MILESTONE PROGRESS REPORTING
            progress_percent = processed_patents / len(xml_patents)
            for milestone in progress_milestones:
                if progress_percent >= milestone and milestone not in completed_milestones:
                    elapsed = current_time - start_time
                    rate = processed_patents / elapsed if elapsed > 0 else 0
                    eta_seconds = (len(xml_patents) - processed_patents) / rate if rate > 0 else 0
                    eta_minutes = eta_seconds / 60
                    
                    milestone_msg = f"{milestone*100:.0f}% complete - Patent {processed_patents:,}/{len(xml_patents):,} - People: {processed_people:,} - ETA: {eta_minutes:.1f}min"
                    print(f"PROGRESS: {milestone_msg}")
                    logger.info(f"MILESTONE: {milestone_msg}")
                    completed_milestones.add(milestone)
                    break
            
            # FREQUENT PROGRESS LOGGING: Every 25 patents or every 15 seconds
            if (processed_patents % 25 == 0) or (current_time - last_log_time > 15):
                elapsed = current_time - start_time
                rate = processed_patents / elapsed if elapsed > 0 else 0
                eta_seconds = (len(xml_patents) - processed_patents) / rate if rate > 0 else 0
                eta_minutes = eta_seconds / 60
                
                progress_msg = f"Patent {processed_patents:,}/{len(xml_patents):,} ({progress_percent*100:.1f}%) - People: {processed_people:,} - Rate: {rate:.1f}/sec - ETA: {eta_minutes:.1f}min"
                print(f"PROGRESS: {progress_msg}")
                last_log_time = current_time
            
            # MAJOR PROGRESS LOGGING: Every 100 patents or every 60 seconds  
            if (processed_patents % 100 == 0) or (current_time - last_major_log_time > 60):
                elapsed = current_time - start_time
                logger.info(f"MAJOR PROGRESS: {processed_patents:,}/{len(xml_patents):,} patents processed "
                        f"({progress_percent*100:.1f}%) in {elapsed/60:.1f} minutes - "
                        f"People processed: {processed_people:,} - "
                        f"New people found: {len(new_people):,} - "
                        f"Existing matches: {len(existing_people_found):,}")
                last_major_log_time = current_time
            
            patent_number = self._clean_patent_number(patent.get('patent_number', ''))
            is_new_patent = patent_number not in self.existing_patents
            
            if is_new_patent:
                new_patents.append(patent)
                
                # Check each person in the patent using enhanced matching
                patent_new_people = []
                
                # Process inventors
                for inventor in patent.get('inventors', []):
                    total_xml_people += 1
                    processed_people += 1
                    
                    # DETAILED PEOPLE PROGRESS: Every 50 people
                    if processed_people % 50 == 0:
                        print(f"PROGRESS: Person matching - {processed_people:,} people processed")
                    
                    matches = self.find_person_matches(inventor)
                    best_score = matches[0][1] if matches else 0
                    
                    # Update detailed statistics
                    if best_score >= 50:
                        match_statistics['score_50_perfect'] += 1
                    elif best_score >= 25:
                        match_statistics['score_25_moved'] += 1
                    elif best_score >= 15:
                        match_statistics['score_15_initial'] += 1
                    elif best_score >= 10:
                        match_statistics['score_10_limited'] += 1
                    elif best_score >= 6:
                        match_statistics['score_6_initial_exact'] += 1
                    elif best_score >= 3:
                        match_statistics['score_3_initial_state'] += 1
                    else:
                        match_statistics['no_match'] += 1
                    
                    # Apply thresholds for decision making
                    if best_score >= AUTO_MATCH_THRESHOLD:
                        # Very likely existing person - don't enrich
                        match_statistics['auto_matched'] += 1
                        existing_people_found.append({
                            **inventor,
                            'match_score': best_score,
                            'match_reason': 'auto_matched',
                            'best_match': matches[0][0] if matches else None
                        })
                    elif best_score >= REVIEW_THRESHOLD:
                        # Possible existing person - flag for review but still enrich
                        match_statistics['needs_review'] += 1
                        new_people.append({
                            **inventor,
                            'patent_number': patent_number,
                            'patent_title': patent.get('patent_title'),
                            'person_type': 'inventor',
                            'person_id': self._create_person_identifier_simple(inventor),
                            'match_score': best_score,
                            'match_status': 'needs_review',
                            'potential_matches': matches[:3],  # Top 3 matches for review
                            'verification_needed': True  # Flag to help frontend identify these
                        })
                        patent_new_people.append(inventor)
                    else:
                        # Definitely new person
                        match_statistics['definitely_new'] += 1
                        new_people.append({
                            **inventor,
                            'patent_number': patent_number,
                            'patent_title': patent.get('patent_title'),
                            'person_type': 'inventor',
                            'person_id': self._create_person_identifier_simple(inventor),
                            'match_score': best_score,
                            'match_status': 'new'
                        })
                        patent_new_people.append(inventor)
                
                # Process assignees (individual ones only) - same progress logic
                for assignee in patent.get('assignees', []):
                    if assignee.get('first_name') or assignee.get('last_name'):
                        total_xml_people += 1
                        processed_people += 1
                        
                        # DETAILED PEOPLE PROGRESS: Every 50 people
                        if processed_people % 50 == 0:
                            print(f"PROGRESS: Person matching - {processed_people:,} people processed")
                        
                        matches = self.find_person_matches(assignee)
                        best_score = matches[0][1] if matches else 0
                        
                        # Same logic as inventors but for assignees
                        if best_score >= AUTO_MATCH_THRESHOLD:
                            match_statistics['auto_matched'] += 1
                            existing_people_found.append({
                                **assignee,
                                'match_score': best_score,
                                'match_reason': 'auto_matched',
                                'best_match': matches[0][0] if matches else None
                            })
                        elif best_score >= REVIEW_THRESHOLD:
                            match_statistics['needs_review'] += 1
                            new_people.append({
                                **assignee,
                                'patent_number': patent_number,
                                'patent_title': patent.get('patent_title'),
                                'person_type': 'assignee',
                                'person_id': self._create_person_identifier_simple(assignee),
                                'match_score': best_score,
                                'match_status': 'needs_review',
                                'potential_matches': matches[:3],
                                'verification_needed': True
                            })
                            patent_new_people.append(assignee)
                        else:
                            match_statistics['definitely_new'] += 1
                            new_people.append({
                                **assignee,
                                'patent_number': patent_number,
                                'patent_title': patent.get('patent_title'),
                                'person_type': 'assignee',
                                'person_id': self._create_person_identifier_simple(assignee),
                                'match_score': best_score,
                                'match_status': 'new'
                            })
                            patent_new_people.append(assignee)
                
                # Update patent with only new people who need enrichment
                patent['inventors'] = [inv for inv in patent.get('inventors', []) 
                                    if inv in patent_new_people]
                patent['assignees'] = [ass for ass in patent.get('assignees', []) 
                                    if ass in patent_new_people]
            else:
                duplicate_patents += 1
                # Still count people for statistics even for existing patents
                for inventor in patent.get('inventors', []):
                    total_xml_people += 1
                for assignee in patent.get('assignees', []):
                    if assignee.get('first_name') or assignee.get('last_name'):
                        total_xml_people += 1
        
        # FINAL COMPLETION LOG
        final_time = time.time()
        total_elapsed = final_time - start_time
        print(f"PROGRESS: COMPLETED - Processed {processed_patents:,} patents and {processed_people:,} people in {total_elapsed/60:.1f} minutes")
        logger.info(f"COMPLETED: Processed {processed_patents:,} patents and {processed_people:,} people in {total_elapsed/60:.1f} minutes")
        
        # Calculate overall statistics
        total_people_checked = sum([
            match_statistics['score_50_perfect'], match_statistics['score_25_moved'],
            match_statistics['score_15_initial'], match_statistics['score_10_limited'],
            match_statistics['score_6_initial_exact'], match_statistics['score_3_initial_state'],
            match_statistics['no_match']
        ])
        
        total_existing_found = (match_statistics['auto_matched'] + 
                            match_statistics['needs_review'])
        match_percentage = (total_existing_found / total_people_checked * 100) if total_people_checked > 0 else 0
        
        logger.info(f"Enhanced filter results:")
        logger.info(f"  📋 New patents: {len(new_patents)}")
        logger.info(f"  🔄 Duplicate patents: {duplicate_patents}")
        logger.info(f"  👥 People needing enrichment: {len(new_people)}")
        logger.info(f"  ✅ Existing people found: {len(existing_people_found)} ({match_percentage:.1f}%)")
        logger.info(f"  📊 Match distribution:")
        logger.info(f"     Perfect matches (50): {match_statistics['score_50_perfect']}")
        logger.info(f"     Moved cities (25): {match_statistics['score_25_moved']}")
        logger.info(f"     Initial matches (15): {match_statistics['score_15_initial']}")
        logger.info(f"     Limited data (10): {match_statistics['score_10_limited']}")
        logger.info(f"     No matches: {match_statistics['no_match']}")
        
        return {
            'new_patents': new_patents,
            'new_people': new_people,
            'existing_people_found': existing_people_found,
            'duplicate_patents_count': duplicate_patents,
            'duplicate_people_count': len(existing_people_found),
            'total_original_patents': len(xml_patents),
            'total_original_people': total_xml_people,
            'match_statistics': match_statistics,
            'match_percentage': match_percentage,
            'processing_time_minutes': total_elapsed / 60,
            'thresholds_used': {
                'auto_match_threshold': AUTO_MATCH_THRESHOLD,
                'review_threshold': REVIEW_THRESHOLD
            }
        }
    
    def _create_person_identifier_simple(self, person: Dict) -> str:
        """Simple identifier creation for new people"""
        first = str(person.get('first_name', '')).strip().lower()
        last = str(person.get('last_name', '')).strip().lower()
        city = str(person.get('city', '')).strip().lower()
        state = str(person.get('state', '')).strip().lower()
        
        return '_'.join(filter(None, [first, last, city, state]))

def run_existing_data_integration(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run the existing data integration process with enhanced matching
    
    Args:
        config: Dictionary containing configuration parameters
        
    Returns:
        Dictionary containing results and statistics
    """
    try:
        # Step 1: Process XML files if not already done
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
        
        # Step 2: Load existing CSV databases with enhanced capabilities
        csv_folder = config.get('CSV_DATABASE_FOLDER', 'converted_databases/csv')
        logger.info(f"Loading existing databases from {csv_folder}")
        
        integrator = EnhancedCSVDatabaseIntegrator(csv_folder)
        
        load_result = integrator.load_existing_databases()
        if not load_result['success']:
            logger.warning("Could not load existing databases, treating all XML data as new")
            
            # Convert XML patents to people list for enrichment (fallback mode)
            new_people_data = []
            total_xml_people = 0
            
            for patent in xml_patents:
                # Add inventors
                for inventor in patent.get('inventors', []):
                    if inventor.get('first_name') or inventor.get('last_name'):
                        person_id = integrator._create_person_identifier_simple(inventor)
                        new_people_data.append({
                            **inventor,
                            'patent_number': patent.get('patent_number'),
                            'patent_title': patent.get('patent_title'),
                            'person_type': 'inventor',
                            'person_id': person_id,
                            'match_status': 'new_no_database'
                        })
                        total_xml_people += 1
                
                # Add assignees (individuals only)
                for assignee in patent.get('assignees', []):
                    if assignee.get('first_name') or assignee.get('last_name'):
                        person_id = integrator._create_person_identifier_simple(assignee)
                        new_people_data.append({
                            **assignee,
                            'patent_number': patent.get('patent_number'),
                            'patent_title': patent.get('patent_title'),
                            'person_type': 'assignee',
                            'person_id': person_id,
                            'match_status': 'new_no_database'
                        })
                        total_xml_people += 1
            
            return {
                'success': True,
                'existing_patents_count': 0,
                'existing_people_count': 0,
                'new_patents_count': len(xml_patents),
                'new_people_count': len(new_people_data),
                'duplicate_patents_count': 0,
                'duplicate_people_count': 0,
                'total_xml_patents': len(xml_patents),
                'total_xml_people': total_xml_people,
                'new_patents_data': xml_patents,
                'new_people_data': new_people_data,
                'databases_loaded': 0,
                'warning': load_result['error'],
                'match_percentage': 0
            }
        
        # Step 3: Filter XML data using enhanced matching
        filter_result = integrator.filter_new_xml_data(xml_patents)
        
        # Step 4: Save filtered results
        os.makedirs(output_folder, exist_ok=True)
        
        if filter_result['new_patents']:
            # Save new patents only (those needing processing)
            filtered_json = Path(output_folder) / 'filtered_new_patents.json'
            with open(filtered_json, 'w') as f:
                json.dump(filter_result['new_patents'], f, indent=2, default=str)
            
            # Save new people for enrichment
            new_people_json = Path(output_folder) / 'new_people_for_enrichment.json'
            with open(new_people_json, 'w') as f:
                json.dump(filter_result['new_people'], f, indent=2, default=str)
            
            # Save existing people found (for reference)
            existing_people_json = Path(output_folder) / 'existing_people_found.json'
            with open(existing_people_json, 'w') as f:
                json.dump(filter_result['existing_people_found'], f, indent=2, default=str)
            
            logger.info(f"Saved {len(filter_result['new_patents'])} new patents to {filtered_json}")
            logger.info(f"Saved {len(filter_result['new_people'])} people for enrichment to {new_people_json}")
            logger.info(f"Saved {len(filter_result['existing_people_found'])} existing people to {existing_people_json}")
        
        return {
            'success': True,
            'existing_patents_count': load_result['existing_patents_count'],
            'existing_people_count': load_result['existing_people_count'],
            'new_patents_count': len(filter_result['new_patents']),
            'new_people_count': len(filter_result['new_people']),
            'duplicate_patents_count': filter_result['duplicate_patents_count'],
            'duplicate_people_count': filter_result['duplicate_people_count'],
            'total_xml_patents': filter_result['total_original_patents'],
            'total_xml_people': filter_result['total_original_people'],
            'new_patents_data': filter_result['new_patents'],
            'new_people_data': filter_result['new_people'],
            'existing_people_found': filter_result['existing_people_found'],
            'databases_loaded': load_result['databases_loaded'],
            'match_statistics': filter_result['match_statistics'],
            'match_percentage': filter_result['match_percentage'],
            'thresholds_used': filter_result['thresholds_used']
        }
        
    except Exception as e:
        logger.error(f"Error in existing data integration: {e}")
        return {
            'success': False,
            'error': str(e),
            'existing_patents_count': 0,
            'new_patents_count': 0,
            'duplicate_patents_count': 0
        }