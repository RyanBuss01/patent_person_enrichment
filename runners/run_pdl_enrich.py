# =============================================================================
# runners/run_pdl_enrich.py - PeopleDataLabs API Enrichment Only
# Handles only PDL enrichments, completely separate from ZabaSearch
# =============================================================================

import logging
import os
import json
import time
from typing import Dict, Any, List, Set, Tuple
from pathlib import Path
import sys

# Add the parent directory to sys.path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from database.db_manager import DatabaseManager, DatabaseConfig

logger = logging.getLogger(__name__)

def _person_signature(person: Dict[str, Any]) -> str:
    """Build a stable signature for a person used for matching/skipping."""
    first_name = (person.get('first_name') or '').strip().lower()
    last_name = (person.get('last_name') or '').strip().lower()
    city = (person.get('city') or '').strip().lower()
    state = (person.get('state') or '').strip().lower()
    patent_number = (person.get('patent_number') or '').strip()
    return f"{first_name}_{last_name}_{city}_{state}_{patent_number}"

def _normalize_value(value: Any) -> str:
    return str(value).strip().lower() if value is not None else ''


def check_existing_pdl_enrichments(people_to_enrich: List[Dict[str, Any]]) -> tuple:
    """Check which people already have PDL enrichments (bulk lookup)."""
    if not people_to_enrich:
        return [], 0

    logger.info("Checking database for existing PDL enrichments (bulk lookup)...")
    print("Checking for existing PDL enrichments (bulk lookup)...")

    try:
        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)

        # Collect lookup keys for people we plan to enrich
        lookup_keys: Set[Tuple[str, str, str, str]] = set()
        lookup_states: Set[str] = set()
        for person in people_to_enrich:
            first = _normalize_value(person.get('first_name'))
            last = _normalize_value(person.get('last_name'))
            city = _normalize_value(person.get('city'))
            state = _normalize_value(person.get('state'))
            lookup_keys.add((first, last, city, state))
            lookup_states.add(state)

        if not lookup_keys:
            return people_to_enrich, 0

        # Pull existing enriched people in bulk grouped by state to limit scan size
        existing_keys: Set[Tuple[str, str, str, str]] = set()
        state_list = sorted(lookup_states)
        # Ensure we always include empty-string state in chunks if present
        chunk_size = 25
        for i in range(0, len(state_list), chunk_size) or [0]:
            chunk = state_list[i:i + chunk_size]
            if not chunk:
                chunk = ['']
            placeholders = ','.join(['%s'] * len(chunk))
            query = f"""
                SELECT LOWER(TRIM(first_name)) AS first_name,
                       LOWER(TRIM(last_name)) AS last_name,
                       LOWER(TRIM(IFNULL(city,''))) AS city,
                       LOWER(TRIM(IFNULL(state,''))) AS state
                FROM enriched_people
                WHERE enrichment_data IS NOT NULL
                  AND LOWER(TRIM(IFNULL(state,''))) IN ({placeholders})
            """
            rows = db_manager.execute_query(query, tuple(chunk))
            for row in rows or []:
                key = (
                    row.get('first_name') or '',
                    row.get('last_name') or '',
                    row.get('city') or '',
                    row.get('state') or ''
                )
                existing_keys.add(key)

        logger.info("Existing PDL enriched lookup size: %s", len(existing_keys))

        new_people_to_enrich: List[Dict[str, Any]] = []
        skipped_count = 0
        skip_preview: List[str] = []

        for person in people_to_enrich:
            key = (
                _normalize_value(person.get('first_name')),
                _normalize_value(person.get('last_name')),
                _normalize_value(person.get('city')),
                _normalize_value(person.get('state'))
            )
            if key in existing_keys:
                skipped_count += 1
                if len(skip_preview) < 10:
                    name = f"{person.get('first_name', '').strip()} {person.get('last_name', '').strip()}".strip()
                    skip_preview.append(name or _person_signature(person))
            else:
                new_people_to_enrich.append(person)

        if skipped_count:
            logger.info("Skipping %s already-enriched people via bulk lookup", skipped_count)
            print(f"Skipping {skipped_count} already enriched people")
        else:
            logger.info("No existing PDL enrichments matched the incoming people")

        # Optionally print the first few skips for visibility
        preview_limit = 10
        if skip_preview:
            for name in skip_preview:
                print(f"SKIP: {name} (already enriched)")

        return new_people_to_enrich, skipped_count

    except Exception as e:
        logger.error(f"Error checking existing PDL enrichments: {e}")
        return people_to_enrich, 0

def load_existing_pdl_enriched() -> List[Dict[str, Any]]:
    """Load existing PDL enriched people from database (enrichment_data IS NOT NULL)"""
    try:
        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)
        
        # Query for records with enrichment_data (PDL data)
        query = """
        SELECT * FROM enriched_people 
        WHERE enrichment_data IS NOT NULL 
        ORDER BY enriched_at DESC
        """
        
        results = db_manager.execute_query(query)
        
        enriched_data = []
        for row in results:
            try:
                # Parse PDL enrichment data
                enrichment_data = json.loads(row.get('enrichment_data', '{}'))
                
                enriched_record = {
                    'original_name': f"{row.get('first_name', '')} {row.get('last_name', '')}".strip(),
                    'patent_number': row.get('patent_number', ''),
                    'enriched_data': enrichment_data,
                    'enriched_at': row.get('enriched_at'),
                    # Extract some key fields for compatibility
                    'api_cost': row.get('api_cost', 0.0)
                }
                enriched_data.append(enriched_record)
                
            except Exception as e:
                logger.warning(f"Error parsing PDL row {row.get('id')}: {e}")
                continue
        
        return enriched_data
        
    except Exception as e:
        logger.warning(f"Error loading existing PDL enriched people: {e}")
        return []

def save_pdl_enrichment(person: Dict[str, Any], pdl_result: Dict[str, Any]):
    """Save PDL enrichment to database (enrichment_data column only)"""
    person_name = f"{person.get('first_name', '')} {person.get('last_name', '')}"
    
    try:
        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)
        
        # Build enrichment data structure for PDL
        enrichment_data = {
            "original_person": {
                'first_name': person.get('first_name', ''),
                'last_name': person.get('last_name', ''),
                'city': person.get('city', ''),
                'state': person.get('state', ''),
                'country': person.get('country', 'US'),
                'patent_number': person.get('patent_number', ''),
                'patent_title': person.get('patent_title', ''),
                'person_type': person.get('person_type', 'inventor')
            },
            "enrichment_result": pdl_result,
            "enriched_at": time.strftime('%Y-%m-%dT%H:%M:%SZ')
        }
        
        # Check if record exists and update, or insert new
        check_query = """
        SELECT id FROM enriched_people 
        WHERE LOWER(TRIM(first_name)) = LOWER(%s) 
        AND LOWER(TRIM(last_name)) = LOWER(%s)
        AND LOWER(TRIM(IFNULL(city,''))) = LOWER(%s)
        AND LOWER(TRIM(IFNULL(state,''))) = LOWER(%s)
        LIMIT 1
        """
        
        params = (
            person.get('first_name', '').strip(),
            person.get('last_name', '').strip(),
            person.get('city', '').strip(),
            person.get('state', '').strip()
        )
        
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(check_query, params)
            existing = cursor.fetchone()
            
            api_cost = pdl_result.get('api_cost', 0.0)
            
            if existing:
                # Update existing record with PDL data
                update_query = """
                UPDATE enriched_people 
                SET enrichment_data = %s, api_cost = %s 
                WHERE id = %s
                """
                cursor.execute(update_query, (
                    json.dumps(enrichment_data),
                    api_cost,
                    existing[0] if isinstance(existing, tuple) else existing.get('id')
                ))
                conn.commit()
                print(f"SQL UPDATE: {person_name} - PDL data saved")
                logger.info(f"UPDATED SQL: {person_name} - PDL data saved to existing record")
            else:
                # Insert new record with PDL data
                insert_query = """
                INSERT INTO enriched_people (
                    first_name, last_name, city, state, country,
                    patent_number, person_type, enrichment_data, api_cost
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.execute(insert_query, (
                    person.get('first_name', '').strip(),
                    person.get('last_name', '').strip(),
                    person.get('city', '').strip(),
                    person.get('state', '').strip(),
                    person.get('country', 'US').strip(),
                    person.get('patent_number', ''),
                    person.get('person_type', 'inventor'),
                    json.dumps(enrichment_data),
                    api_cost
                ))
                conn.commit()
                print(f"SQL INSERT: {person_name} - New PDL record created")
                logger.info(f"INSERTED SQL: {person_name} - New PDL enrichment record created")
            
            # Log key data for verification
            if pdl_result.get('enriched_data'):
                pdl_data = pdl_result['enriched_data'].get('pdl_data', {})
                emails = pdl_data.get('emails', [])
                if emails:
                    print(f"  Email: {emails[0] if isinstance(emails[0], str) else emails[0].get('address', 'N/A')}")
                
                job_company = pdl_data.get('job_company_location_street_address', '')
                if job_company:
                    print(f"  Company Address: {job_company}")
            
    except Exception as e:
        print(f"SQL SAVE FAILED: {person_name} - Error: {e}")
        logger.error(f"SQL SAVE FAILED: {person_name} - Error: {e}")
        raise

def run_pdl_enrichment(config: Dict[str, Any]) -> Dict[str, Any]:
    """Run PeopleDataLabs enrichment process"""
    
    try:
        logger.info("Starting PeopleDataLabs enrichment process...")
        print("Starting PeopleDataLabs API enrichment...")
        
        # Load people to enrich
        people_to_enrich = config.get('new_people_data', [])
        
        if not people_to_enrich:
            return {
                'success': True,
                'message': 'No people to enrich with PeopleDataLabs',
                'total_people': 0,
                'enriched_count': 0,
                'enriched_data': [],
                'newly_enriched_data': [],
                'actual_api_cost': '$0.00'
            }
        
        logger.info(f"Found {len(people_to_enrich)} people to potentially enrich")
        print(f"Loaded {len(people_to_enrich)} people for PDL enrichment")
        
        # Check for existing PDL enrichments
        new_people_to_enrich, skipped_count = check_existing_pdl_enrichments(people_to_enrich)
        
        logger.info(f"After filtering PDL duplicates: {len(new_people_to_enrich)} new people, {skipped_count} already enriched")
        print(f"Will enrich {len(new_people_to_enrich)} new people")
        
        # Test mode limit
        if config.get('TEST_MODE') and len(new_people_to_enrich) > 5:
            new_people_to_enrich = new_people_to_enrich[:5]
            logger.info(f"TEST MODE: Limited to {len(new_people_to_enrich)} people")
            print(f"TEST MODE: Limited to {len(new_people_to_enrich)} people")
        
        if not new_people_to_enrich:
            # Load existing enriched data for return
            existing_enriched = load_existing_pdl_enriched()
            return {
                'success': True,
                'message': 'All people already enriched with PeopleDataLabs',
                'total_people': len(people_to_enrich),
                'enriched_count': 0,
                'enriched_data': existing_enriched,
                'newly_enriched_data': [],
                'actual_api_cost': '$0.00',
                'already_enriched_count': skipped_count
            }
        
        # Initialize progress tracking
        progress_path = Path(config.get('OUTPUT_DIR', 'output')) / 'step2_pdl_progress.json'
        try:
            with open(progress_path, 'w') as pf:
                json.dump({
                    'step': 2,
                    'method': 'peopledatalabs',
                    'total': len(new_people_to_enrich) + skipped_count,
                    'processed': 0,
                    'newly_enriched': 0,
                    'already_enriched': skipped_count,
                    'stage': 'starting_pdl_enrichment',
                    'started_at': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }, pf)
        except Exception:
            pass
        
        # Import and use the actual PDL enrichment logic from the existing enrich.py
        # This is a placeholder - you'll need to move the actual PDL enrichment code here
        from runners.enrich import run_sql_data_enrichment
        
        # Call the existing PDL enrichment with filtered people
        pdl_config = dict(config)
        pdl_config['new_people_data'] = new_people_to_enrich
        
        result = run_sql_data_enrichment(pdl_config)
        
        # Update progress
        try:
            with open(progress_path, 'w') as pf:
                json.dump({
                    'step': 2,
                    'method': 'peopledatalabs',
                    'total': len(people_to_enrich),
                    'processed': len(people_to_enrich),
                    'newly_enriched': result.get('enriched_count', 0),
                    'already_enriched': skipped_count,
                    'stage': 'completed',
                    'completed_at': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }, pf)
        except Exception:
            pass
        
        print(f"\nPeopleDataLabs enrichment completed!")
        print(f"   Successfully enriched: {result.get('enriched_count', 0)}")
        print(f"   Already enriched: {skipped_count}")
        
        # Add skipped count to result
        result['already_enriched_count'] = skipped_count
        result['method'] = 'peopledatalabs'
        
        return result
        
    except Exception as e:
        logger.error(f"PeopleDataLabs enrichment failed: {e}")
        print(f"PeopleDataLabs enrichment failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'total_people': 0,
            'enriched_count': 0,
            'enriched_data': [],
            'actual_api_cost': '$0.00',
            'method': 'peopledatalabs'
        }
