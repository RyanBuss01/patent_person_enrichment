# =============================================================================
# runners/enrich.py - SIMPLE AND FAST
# =============================================================================
import logging
import json
import time
import pandas as pd
from typing import Dict, Any, List
from pathlib import Path
from classes.people_data_labs_enricher import PeopleDataLabsEnricher
from database.db_manager import DatabaseManager, DatabaseConfig

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
        
        for person in people_to_enrich:
            if is_already_enriched(person, existing_enriched):
                skipped_count += 1
            else:
                new_people_to_enrich.append(person)
        
        print(f"After duplicate check: {len(new_people_to_enrich)} new people, {skipped_count} already enriched")
        
        if not new_people_to_enrich:
            return {
                'success': True,
                'message': 'All people already enriched',
                'total_people': len(people_to_enrich),
                'enriched_count': 0,
                'enriched_data': existing_enriched,
                'actual_api_cost': '$0.00',
                'api_calls_saved': len(people_to_enrich)
            }
        
        # Limit for test mode
        if config.get('TEST_MODE') and len(new_people_to_enrich) > 2:
            new_people_to_enrich = new_people_to_enrich[:2]
            print(f"TEST MODE: Limited to {len(new_people_to_enrich)} people")
        
        print(f"STEP 4: Enriching {len(new_people_to_enrich)} people...")
        
        # Enrich the new people
        newly_enriched = enrich_people_batch(new_people_to_enrich, config)
        
        print(f"STEP 5: Saving {len(newly_enriched)} new enrichments to database...")
        
        # Save new enrichments to database
        if newly_enriched:
            save_enrichments_to_database(newly_enriched)
        
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
            'enriched_data': all_enriched_data,
            'actual_api_cost': f"${len(newly_enriched) * 0.03:.2f}",
            'api_calls_saved': skipped_count,
            'already_enriched_count': skipped_count,
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


def enrich_people_batch(people: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Enrich a batch of people"""
    
    api_key = config.get('PEOPLEDATALABS_API_KEY')
    enriched_results = []
    
    # Initialize enricher
    if not api_key or api_key == 'YOUR_PDL_API_KEY':
        print("Using mock enrichment (no API key)")
        use_mock = True
    else:
        print(f"Using real API with key: {api_key[:10]}...")
        try:
            enricher = PeopleDataLabsEnricher(api_key)
            use_mock = False
        except Exception as e:
            print(f"Failed to initialize enricher: {e}")
            use_mock = True
    
    for i, person in enumerate(people):
        progress = i + 1
        total = len(people)
        person_name = f"{person.get('first_name', '')} {person.get('last_name', '')}"
        
        print(f"ENRICHING {progress}/{total}: {person_name}")
        print(f"  Person data: first_name='{person.get('first_name')}', last_name='{person.get('last_name')}', city='{person.get('city')}', state='{person.get('state')}'")
        
        try:
            if not use_mock:
                # Clean person data to avoid the join error
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
                
                print(f"  Calling API with clean data: {clean_person}")
                result = enricher.enrich_people_list([clean_person])
                
                if result and len(result) > 0:
                    enrichment_result = result[0]
                    print(f"  API SUCCESS: Got enrichment data")
                else:
                    print(f"  API returned empty result for {person_name} (likely not in PDL database)")
                    # Fallback to mock for testing when API returns empty
                    if config.get('TEST_MODE'):
                        print(f"  TEST MODE: Using mock data instead")
                        enrichment_result = {
                            'original_name': person_name,
                            'patent_number': person.get('patent_number', ''),
                            'patent_title': person.get('patent_title', ''),
                            'match_score': 0.5,  # Lower score for mock
                            'enriched_data': {
                                'person_type': person.get('person_type', 'inventor'),
                                'original_data': clean_person,
                                'pdl_data': {
                                    'full_name': person_name,
                                    'emails': [{'address': f"test.{person_name.lower().replace(' ', '.')}@example.com"}],
                                    'linkedin_url': f'https://linkedin.com/in/{person_name.lower().replace(" ", "")}',
                                    'job_title': 'Inventor',
                                    'job_company_name': 'Unknown Company',
                                    'note': 'Mock data - person not found in PeopleDataLabs'
                                },
                                'api_method': 'mock_fallback'
                            }
                        }
                    else:
                        continue
            else:
                # Mock enrichment - always works
                enrichment_result = {
                    'original_name': person_name,
                    'patent_number': person.get('patent_number', ''),
                    'patent_title': person.get('patent_title', ''),
                    'match_score': 1.0,
                    'enriched_data': {
                        'person_type': person.get('person_type', 'inventor'),
                        'original_data': person,
                        'pdl_data': {
                            'full_name': person_name,
                            'emails': [{'address': f"mock@example.com"}],
                            'linkedin_url': 'https://linkedin.com/in/mockuser',
                            'job_title': 'Software Engineer',
                            'job_company_name': 'Mock Company'
                        },
                        'api_method': 'mock'
                    }
                }
                print(f"  MOCK SUCCESS: Created mock enrichment")
            
            enriched_results.append(enrichment_result)
            print(f"  SUCCESS: Enriched {person_name}")
            
        except Exception as e:
            print(f"  ERROR: Failed to enrich {person_name}: {e}")
            print(f"  Error type: {type(e)}")
            import traceback
            traceback.print_exc()
            continue
        
        # Small delay to be nice to API
        if not use_mock:
            time.sleep(0.1)
    
    print(f"Enriched {len(enriched_results)} out of {len(people)} people")
    return enriched_results


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