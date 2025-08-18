# =============================================================================
# runners/integrate_existing_data.py
# Enhanced Step 0: Integrate XML data with existing CSV databases
# =============================================================================
import pandas as pd
import logging
import os
from pathlib import Path
from typing import Dict, Any, List, Set, Tuple
from classes.simple_xml_processor import process_xml_files
import json

logger = logging.getLogger(__name__)

class CSVDatabaseIntegrator:
    """Compare XML patent data against existing CSV databases"""
    
    def __init__(self, csv_database_folder: str):
        self.csv_database_folder = Path(csv_database_folder)
        self.existing_patents = set()
        self.existing_people = set()
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
        
        # Common patent number column names (case insensitive)
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
        
        # Look for name columns (case insensitive)
        first_name_col = None
        last_name_col = None
        
        for col in df.columns:
            col_lower = col.lower()
            if ('first' in col_lower and 'name' in col_lower) or col_lower in ['firstname', 'fname', 'first', 'inventor_first']:
                first_name_col = col
            elif ('last' in col_lower and 'name' in col_lower) or col_lower in ['lastname', 'lname', 'last', 'inventor_last']:
                last_name_col = col
        
        # Create person identifiers from names and locations
        if first_name_col and last_name_col:
            logger.debug(f"Found name columns: {first_name_col}, {last_name_col}")
            
            # Find location columns
            city_col = None
            state_col = None
            country_col = None
            
            for col in df.columns:
                col_lower = col.lower()
                if 'city' in col_lower:
                    city_col = col
                elif 'state' in col_lower:
                    state_col = col
                elif 'country' in col_lower:
                    country_col = col
            
            for _, row in df.iterrows():
                person_id = self._create_person_identifier(
                    first_name=row.get(first_name_col),
                    last_name=row.get(last_name_col),
                    city=row.get(city_col, '') if city_col else '',
                    state=row.get(state_col, '') if state_col else '',
                    country=row.get(country_col, '') if country_col else ''
                )
                if person_id:
                    self.existing_people.add(person_id)
                    people_count += 1
        
        logger.info(f"{filename}: Found {patents_count} patents, {people_count} people")
        return patents_count, people_count
    
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
    
    def _create_person_identifier(self, first_name: str, last_name: str, 
                                city: str = '', state: str = '', country: str = '') -> str:
        """Create a unique identifier for a person"""
        if not first_name and not last_name:
            return None
        
        # Normalize names
        first = str(first_name or '').strip().lower()
        last = str(last_name or '').strip().lower()
        
        if not first and not last:
            return None
        
        # Skip invalid entries
        if first.lower() in ['nan', 'none', 'null'] and last.lower() in ['nan', 'none', 'null']:
            return None
        
        # Create identifier: "firstname_lastname_city_state"
        identifier_parts = [first, last]
        
        if city and str(city).lower() not in ['nan', 'none', '', 'null']:
            identifier_parts.append(str(city).strip().lower())
        
        if state and str(state).lower() not in ['nan', 'none', '', 'null']:
            identifier_parts.append(str(state).strip().lower())
        
        return '_'.join(filter(None, identifier_parts))
    
    def filter_new_xml_data(self, xml_patents: List[Dict]) -> Dict[str, Any]:
        """Filter XML patent data to only include new records"""
        logger.info(f"Filtering {len(xml_patents)} XML patents against existing data")
        
        new_patents = []
        new_people = []
        duplicate_patents = 0
        duplicate_people = 0
        total_xml_people = 0
        
        for patent in xml_patents:
            patent_number = self._clean_patent_number(patent.get('patent_number', ''))
            is_new_patent = patent_number not in self.existing_patents
            
            if is_new_patent:
                new_patents.append(patent)
                
                # Check each person in the patent
                patent_new_people = []
                
                # Check inventors
                for inventor in patent.get('inventors', []):
                    total_xml_people += 1
                    person_id = self._create_person_identifier(
                        first_name=inventor.get('first_name'),
                        last_name=inventor.get('last_name'),
                        city=inventor.get('city'),
                        state=inventor.get('state'),
                        country=inventor.get('country')
                    )
                    
                    if person_id and person_id not in self.existing_people:
                        new_people.append({
                            **inventor,
                            'patent_number': patent_number,
                            'patent_title': patent.get('patent_title'),
                            'person_type': 'inventor',
                            'person_id': person_id
                        })
                        patent_new_people.append(inventor)
                    else:
                        duplicate_people += 1
                
                # Check assignees (individual ones only)
                for assignee in patent.get('assignees', []):
                    if assignee.get('first_name') or assignee.get('last_name'):
                        total_xml_people += 1
                        person_id = self._create_person_identifier(
                            first_name=assignee.get('first_name'),
                            last_name=assignee.get('last_name'),
                            city=assignee.get('city'),
                            state=assignee.get('state'),
                            country=assignee.get('country')
                        )
                        
                        if person_id and person_id not in self.existing_people:
                            new_people.append({
                                **assignee,
                                'patent_number': patent_number,
                                'patent_title': patent.get('patent_title'),
                                'person_type': 'assignee',
                                'person_id': person_id
                            })
                            patent_new_people.append(assignee)
                        else:
                            duplicate_people += 1
                
                # Update patent with only new people
                patent['inventors'] = [inv for inv in patent.get('inventors', []) 
                                     if inv in patent_new_people]
                patent['assignees'] = [ass for ass in patent.get('assignees', []) 
                                     if ass in patent_new_people]
                
            else:
                duplicate_patents += 1
                # Still count people for statistics
                for inventor in patent.get('inventors', []):
                    total_xml_people += 1
                for assignee in patent.get('assignees', []):
                    if assignee.get('first_name') or assignee.get('last_name'):
                        total_xml_people += 1
        
        logger.info(f"Filter results:")
        logger.info(f"  New patents: {len(new_patents)}")
        logger.info(f"  Duplicate patents: {duplicate_patents}")
        logger.info(f"  New people: {len(new_people)}")
        logger.info(f"  Duplicate people: {duplicate_people}")
        
        return {
            'new_patents': new_patents,
            'new_people': new_people,
            'duplicate_patents_count': duplicate_patents,
            'duplicate_people_count': duplicate_people,
            'total_original_patents': len(xml_patents),
            'total_original_people': total_xml_people
        }

def run_existing_data_integration(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run the existing data integration process
    
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
        
        # Step 2: Load existing CSV databases
        csv_folder = config.get('CSV_DATABASE_FOLDER', 'converted_databases/csv')
        logger.info(f"Loading existing databases from {csv_folder}")
        
        integrator = CSVDatabaseIntegrator(csv_folder)
        
        load_result = integrator.load_existing_databases()
        if not load_result['success']:
            logger.warning("Could not load existing databases, treating all XML data as new")
            
            # Convert XML patents to people list for enrichment
            new_people_data = []
            total_xml_people = 0
            
            for patent in xml_patents:
                # Add inventors
                for inventor in patent.get('inventors', []):
                    if inventor.get('first_name') or inventor.get('last_name'):
                        person_id = integrator._create_person_identifier(
                            inventor.get('first_name'),
                            inventor.get('last_name'),
                            inventor.get('city'),
                            inventor.get('state'),
                            inventor.get('country')
                        )
                        new_people_data.append({
                            **inventor,
                            'patent_number': patent.get('patent_number'),
                            'patent_title': patent.get('patent_title'),
                            'person_type': 'inventor',
                            'person_id': person_id
                        })
                        total_xml_people += 1
                
                # Add assignees (individuals only)
                for assignee in patent.get('assignees', []):
                    if assignee.get('first_name') or assignee.get('last_name'):
                        person_id = integrator._create_person_identifier(
                            assignee.get('first_name'),
                            assignee.get('last_name'),
                            assignee.get('city'),
                            assignee.get('state'),
                            assignee.get('country')
                        )
                        new_people_data.append({
                            **assignee,
                            'patent_number': patent.get('patent_number'),
                            'patent_title': patent.get('patent_title'),
                            'person_type': 'assignee',
                            'person_id': person_id
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
                'warning': load_result['error']
            }
        
        # Step 3: Filter XML data to only new records
        filter_result = integrator.filter_new_xml_data(xml_patents)
        
        # Step 4: Save filtered results
        if filter_result['new_patents']:
            # Save new patents only
            filtered_json = Path(output_folder) / 'filtered_new_patents.json'
            with open(filtered_json, 'w') as f:
                json.dump(filter_result['new_patents'], f, indent=2, default=str)
            
            # Save new people for enrichment
            new_people_json = Path(output_folder) / 'new_people_for_enrichment.json'
            with open(new_people_json, 'w') as f:
                json.dump(filter_result['new_people'], f, indent=2, default=str)
            
            logger.info(f"Saved {len(filter_result['new_patents'])} new patents to {filtered_json}")
            logger.info(f"Saved {len(filter_result['new_people'])} new people to {new_people_json}")
        
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
            'databases_loaded': load_result['databases_loaded']
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