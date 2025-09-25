# =============================================================================
# runners/run_zaba_enrich.py - ZabaSearch Web Scraping Enrichment Only
# Handles only ZabaSearch enrichments, completely separate from PDL
# =============================================================================

import logging
import os
import json
import time
import subprocess
from typing import Dict, Any, List, Optional
from pathlib import Path
import sys


def _format_progress_message(saved: int, total_to_enrich: int, processed: int, failed: int) -> str:
    """Build a human-friendly progress message."""
    if total_to_enrich > 0:
        base = f"{saved}/{total_to_enrich} - people enriched"
    else:
        base = "0/0 - people enriched"
    if processed or failed:
        return f"{base} (processed:{processed}, failed:{failed})"
    return base


def _write_zaba_progress(progress_path: Path, *, total_candidates: int, total_to_enrich: int,
                          processed: int, saved: int, skipped: int, failed: int, stage: str = 'processing') -> str:
    """Persist Zaba progress for polling endpoints."""
    message = _format_progress_message(saved, total_to_enrich, processed, failed)
    progress_payload = {
        'step': 2,
        'method': 'zabasearch',
        'stage': stage,
        'total_candidates': total_candidates,
        'total_to_enrich': total_to_enrich,
        'processed': processed,
        'newly_enriched': saved,
        'already_enriched': skipped,
        'failed': failed,
        'message': message,
        'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ')
    }
    if stage == 'starting':
        progress_payload['started_at'] = progress_payload['updated_at']

    try:
        progress_path.parent.mkdir(parents=True, exist_ok=True)
        with open(progress_path, 'w') as pf:
            json.dump(progress_payload, pf, indent=2)
    except Exception:
        # Progress reporting is best effort; do not interrupt enrichment
        pass

    return message

# Add the parent directory to sys.path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from database.db_manager import DatabaseManager, DatabaseConfig

logger = logging.getLogger(__name__)

class ZabaSearchEnricher:
    """ZabaSearch web scraper that calls the working standalone script"""
    
    def __init__(self):
        # Path to the working scrape script
        self.script_path = Path(__file__).parent.parent / 'scripts' / 'scrape.py'
        
        if not self.script_path.exists():
            raise RuntimeError(f"ZabaSearch script not found at {self.script_path}")
        logger.info(f"Using ZabaSearch script at: {self.script_path}")
        
        # Try multiple Python paths in order of preference
        python_candidates = [
            str(Path(__file__).parent.parent / 'zaba_venv' / 'bin' / 'python3'),  # Local zaba_venv
            '/Users/ryanbussert/Desktop/Work/National_Engravers/scraping/venv/bin/python3',  # Original working venv
            'python3'  # System fallback
        ]
        
        self.working_python = None
        for candidate in python_candidates:
            if Path(candidate).exists() if candidate.startswith('/') else True:
                self.working_python = candidate
                logger.info(f"Using Python: {candidate}")
                break
        
        if not self.working_python:
            self.working_python = 'python3'
            logger.warning(f"No working Python found, using system python3")
    
    def scrape_person(self, person: Dict[str, Any], max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """Scrape ZabaSearch for a single person using the working script"""
        first_name = person.get('first_name', '').strip()
        last_name = person.get('last_name', '').strip()
        city = person.get('city', '').strip()
        state = person.get('state', '').strip()
        
        if not first_name or not last_name or not state:
            logger.warning(f"Missing required fields for {first_name} {last_name}")
            return None
        
        logger.info(f"Scraping ZabaSearch for {first_name} {last_name} ({city}, {state})")
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    wait_time = 2 ** attempt
                    logger.info(f"Retry {attempt + 1} for {first_name} {last_name} after {wait_time}s delay")
                    time.sleep(wait_time)
                
                # Build the command to call the working script using the working venv Python
                cmd = [
                    self.working_python, str(self.script_path),
                    '--first', first_name,
                    '--last', last_name,
                    '--state', state,
                    '--city', city,
                    '--json'
                ]
                
                working_dir = str(Path(__file__).parent.parent)
                
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=60,  # 60 second timeout
                    cwd=working_dir
                )
                
                if result.returncode == 0:
                    # Parse the JSON output - Handle multi-line output
                    try:
                        if result.stdout.strip():
                            # Split output into lines and find the JSON line
                            lines = result.stdout.strip().split('\n')
                            json_line = None
                            
                            # Look for the line that starts with '{' (JSON)
                            for line in lines:
                                line = line.strip()
                                if line.startswith('{'):
                                    json_line = line
                                    break
                            
                            if json_line:
                                zaba_data = json.loads(json_line)
                                
                                # Clean up address format and reorder fields
                                zaba_data = self._clean_zaba_data(zaba_data)
                                
                                logger.info(f"Successfully scraped {first_name} {last_name} on attempt {attempt + 1}")
                                return zaba_data
                            else:
                                if attempt < max_retries - 1:
                                    continue
                                else:
                                    return None
                        else:
                            if attempt < max_retries - 1:
                                continue
                            else:
                                return None
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse JSON output for {first_name} {last_name}: {e}")
                        if attempt < max_retries - 1:
                            continue
                        else:
                            return None
                else:
                    # Script failed
                    if attempt < max_retries - 1:
                        continue
                    else:
                        return None
                        
            except subprocess.TimeoutExpired:
                if attempt < max_retries - 1:
                    continue
                else:
                    return None
            except Exception as e:
                logger.error(f"Subprocess error for {first_name} {last_name} (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    continue
                else:
                    return None
        
        return None

    def _clean_zaba_data(self, zaba_data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean up ZabaSearch data - fix address format and reorder fields"""
        if not isinstance(zaba_data, dict):
            return zaba_data
        
        # Get the current mail_to_add1 field
        mail_to_add1 = zaba_data.get('mail_to_add1', '')
        
        # Remove state from the address (e.g., "301 Reservoir RD Lunenburg, Massachusetts" -> "301 Reservoir RD Lunenburg")
        if mail_to_add1:
            # Split by comma and take everything except the last part (which should be state)
            parts = mail_to_add1.split(',')
            if len(parts) > 1:
                # Remove the last part (state) and rejoin
                cleaned_address = ','.join(parts[:-1]).strip()
            else:
                cleaned_address = mail_to_add1.strip()
        else:
            cleaned_address = ''
        
        # Create new ordered dictionary with zip and mail_to_add1 at the front
        cleaned_data = {}
        
        # Put zip and mail_to_add1 first
        cleaned_data['zip'] = zaba_data.get('zip', '')
        cleaned_data['mail_to_add1'] = cleaned_address
        
        # Add all other fields
        for key, value in zaba_data.items():
            if key not in ['zip', 'mail_to_add1']:
                cleaned_data[key] = value
        
        return cleaned_data

def _person_signature(person: Dict[str, Any]) -> str:
    """Build a stable signature for a person used for matching/skipping."""
    first_name = (person.get('first_name') or '').strip().lower()
    last_name = (person.get('last_name') or '').strip().lower()
    city = (person.get('city') or '').strip().lower()
    state = (person.get('state') or '').strip().lower()
    patent_number = (person.get('patent_number') or '').strip()
    return f"{first_name}_{last_name}_{city}_{state}_{patent_number}"

def check_existing_zaba_enrichments(people_to_enrich: List[Dict[str, Any]]) -> tuple:
    """Check which people already have ZabaSearch enrichments (zaba_data IS NOT NULL)"""
    if not people_to_enrich:
        return [], []
    
    logger.info("Checking database for existing ZabaSearch enrichments...")
    print("Checking for existing ZabaSearch enrichments...")
    
    try:
        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)
        
        # Create a set to store people who already have ZabaSearch data
        already_enriched_people = set()
        
        # Check each person individually for ZabaSearch data (zaba_data IS NOT NULL)
        for person in people_to_enrich:
            check_query = """
            SELECT id FROM enriched_people 
            WHERE LOWER(TRIM(first_name)) = LOWER(%s) 
            AND LOWER(TRIM(last_name)) = LOWER(%s)
            AND LOWER(TRIM(IFNULL(city,''))) = LOWER(%s)
            AND LOWER(TRIM(IFNULL(state,''))) = LOWER(%s)
            AND zaba_data IS NOT NULL
            LIMIT 1
            """
            
            check_params = (
                person.get('first_name', '').strip(),
                person.get('last_name', '').strip(),
                person.get('city', '').strip(),
                person.get('state', '').strip()
            )
            
            try:
                existing_check = db_manager.execute_query(check_query, check_params)
                if existing_check:
                    person_sig = _person_signature(person)
                    already_enriched_people.add(person_sig)
            except Exception as e:
                logger.warning(f"Could not check ZabaSearch status for {person.get('first_name')} {person.get('last_name')}: {e}")
        
        logger.info(f"Found {len(already_enriched_people)} people already enriched with ZabaSearch")
        print(f"Skipping {len(already_enriched_people)} already enriched people")
        
        # Filter out people already enriched with ZabaSearch
        new_people_to_enrich = []
        skipped_count = 0
        
        for person in people_to_enrich:
            person_sig = _person_signature(person)
            if person_sig in already_enriched_people:
                skipped_count += 1
                person_name = f"{person.get('first_name', '')} {person.get('last_name', '')}"
                logger.debug(f"Skipping {person_name} - already ZabaSearch enriched")
                print(f"SKIP: {person_name} (already enriched)")
            else:
                new_people_to_enrich.append(person)
        
        return new_people_to_enrich, skipped_count
        
    except Exception as e:
        logger.error(f"Error checking existing ZabaSearch enrichments: {e}")
        return people_to_enrich, 0

def load_existing_zaba_enriched() -> List[Dict[str, Any]]:
    """Load existing ZabaSearch enriched people from database (zaba_data IS NOT NULL)"""
    try:
        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)
        
        # Query for records with zaba_data
        query = """
        SELECT * FROM enriched_people 
        WHERE zaba_data IS NOT NULL 
        ORDER BY enriched_at DESC
        """
        
        results = db_manager.execute_query(query)
        
        enriched_data = []
        for row in results:
            try:
                # Parse ZabaSearch data
                zaba_data = json.loads(row.get('zaba_data', '{}'))
                
                enriched_record = {
                    'original_name': f"{row.get('first_name', '')} {row.get('last_name', '')}".strip(),
                    'patent_number': row.get('patent_number', ''),
                    'zaba_data': zaba_data,
                    'enriched_at': row.get('enriched_at'),
                    'mail_to_add1': zaba_data.get('mail_to_add1', ''),
                    'zip': zaba_data.get('zip', ''),
                    'email': (zaba_data.get('data', {}).get('email_addresses', []) or [''])[0]
                }
                enriched_data.append(enriched_record)
                
            except Exception as e:
                logger.warning(f"Error parsing ZabaSearch row {row.get('id')}: {e}")
                continue
        
        return enriched_data
        
    except Exception as e:
        logger.warning(f"Error loading existing ZabaSearch enriched people: {e}")
        return []

def save_zaba_enrichment(person: Dict[str, Any], zaba_result: Dict[str, Any]):
    """Save ZabaSearch enrichment to database (zaba_data column only)"""
    person_name = f"{person.get('first_name', '')} {person.get('last_name', '')}"
    
    try:
        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)
        
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
            
            if existing:
                # Update existing record with ONLY ZabaSearch data (no enrichment_data)
                update_query = """
                UPDATE enriched_people 
                SET zaba_data = %s 
                WHERE id = %s
                """
                cursor.execute(update_query, (
                    json.dumps(zaba_result),
                    existing[0] if isinstance(existing, tuple) else existing.get('id')
                ))
                conn.commit()
                print(f"SQL UPDATE: {person_name} - ZabaSearch data saved")
                logger.info(f"UPDATED SQL: {person_name} - ZabaSearch data saved to existing record")
            else:
                # Insert new record with ONLY ZabaSearch data (enrichment_data stays NULL)
                insert_query = """
                INSERT INTO enriched_people (
                    first_name, last_name, city, state, country,
                    patent_number, person_type, zaba_data, api_cost
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
                    json.dumps(zaba_result),
                    0.0  # Free scraping
                ))
                conn.commit()
                print(f"SQL INSERT: {person_name} - New ZabaSearch record created")
                logger.info(f"INSERTED SQL: {person_name} - New ZabaSearch enrichment record created")
            
            # Log the data being saved for verification
            print(f"  Address: {zaba_result.get('mail_to_add1', 'N/A')}")
            print(f"  ZIP: {zaba_result.get('zip', 'N/A')}")
            phones = zaba_result.get('data', {}).get('phone_numbers', [])
            emails = zaba_result.get('data', {}).get('email_addresses', [])
            if phones:
                print(f"  Phone: {phones[0]}")
            if emails:
                print(f"  Email: {emails[0]}")
            
    except Exception as e:
        print(f"SQL SAVE FAILED: {person_name} - Error: {e}")
        logger.error(f"SQL SAVE FAILED: {person_name} - Error: {e}")
        raise

def run_zaba_enrichment(config: Dict[str, Any]) -> Dict[str, Any]:
    """Run ZabaSearch enrichment process"""
    
    try:
        logger.info("Starting ZabaSearch enrichment process...")
        print("Starting ZabaSearch web scraping enrichment...")
        
        # Load people to enrich
        people_to_enrich = config.get('new_people_data', [])
        
        if not people_to_enrich:
            return {
                'success': True,
                'message': 'No people to enrich with ZabaSearch',
                'total_people': 0,
                'enriched_count': 0,
                'enriched_data': [],
                'newly_enriched_data': [],
                'actual_scrape_cost': '$0.00'
            }
        
        logger.info(f"Found {len(people_to_enrich)} people to potentially enrich")
        print(f"Loaded {len(people_to_enrich)} people for ZabaSearch enrichment")
        
        # Check for existing ZabaSearch enrichments
        new_people_to_enrich, skipped_count = check_existing_zaba_enrichments(people_to_enrich)
        
        logger.info(f"After filtering ZabaSearch duplicates: {len(new_people_to_enrich)} new people, {skipped_count} already enriched")
        print(f"Will enrich {len(new_people_to_enrich)} new people")
        
        # Test mode limit
        if config.get('TEST_MODE') and len(new_people_to_enrich) > 5:
            new_people_to_enrich = new_people_to_enrich[:5]
            logger.info(f"TEST MODE: Limited to {len(new_people_to_enrich)} people")
            print(f"TEST MODE: Limited to {len(new_people_to_enrich)} people")
        
        if not new_people_to_enrich:
            # Load existing enriched data for return
            progress_path = Path(config.get('OUTPUT_DIR', 'output')) / 'step2_zaba_progress.json'
            completion_message = _write_zaba_progress(
                progress_path,
                total_candidates=len(people_to_enrich),
                total_to_enrich=len(new_people_to_enrich),
                processed=0,
                saved=0,
                skipped=skipped_count,
                failed=0,
                stage='completed'
            )
            print(f"PROGRESS: {completion_message}")
            sys.stdout.flush()
            existing_enriched = load_existing_zaba_enriched()
            return {
                'success': True,
                'message': 'All people already enriched with ZabaSearch',
                'total_people': len(people_to_enrich),
                'enriched_count': 0,
                'enriched_data': existing_enriched,
                'newly_enriched_data': [],
                'actual_scrape_cost': '$0.00',
                'already_enriched_count': skipped_count
            }
        
        # Initialize progress tracking
        progress_path = Path(config.get('OUTPUT_DIR', 'output')) / 'step2_zaba_progress.json'
        total_candidates = len(new_people_to_enrich) + skipped_count
        total_to_enrich = len(new_people_to_enrich)
        start_message = _write_zaba_progress(
            progress_path,
            total_candidates=total_candidates,
            total_to_enrich=total_to_enrich,
            processed=0,
            saved=0,
            skipped=skipped_count,
            failed=0,
            stage='starting'
        )
        print(f"PROGRESS: {start_message}")
        sys.stdout.flush()
        
        # Enrich people with ZabaSearch - save immediately after each success
        enricher = ZabaSearchEnricher()
        newly_enriched = []
        processed = 0
        failed_count = 0
        saved_count = 0
        
        print(f"\nStarting ZabaSearch scraping for {len(new_people_to_enrich)} people...")
        print("=" * 60)
        
        for person in new_people_to_enrich:
            processed += 1
            person_name = f"{person.get('first_name', '')} {person.get('last_name', '')}"
            logger.info(f"Scraping {processed}/{len(new_people_to_enrich)}: {person_name}")
            print(f"\n[{processed}/{len(new_people_to_enrich)}] Scraping: {person_name}")

            try:
                # Add delay between requests to be respectful
                if processed > 1:
                    print("Waiting 5 seconds...")
                    time.sleep(5)  # 10 second delay between requests
                
                # Double-check this person isn't already in SQL (race condition protection)
                person_sig = _person_signature(person)
                check_query = """
                SELECT id FROM enriched_people 
                WHERE LOWER(TRIM(first_name)) = LOWER(%s) 
                AND LOWER(TRIM(last_name)) = LOWER(%s)
                AND LOWER(TRIM(IFNULL(city,''))) = LOWER(%s)
                AND LOWER(TRIM(IFNULL(state,''))) = LOWER(%s)
                AND zaba_data IS NOT NULL
                LIMIT 1
                """
                
                check_params = (
                    person.get('first_name', '').strip(),
                    person.get('last_name', '').strip(),
                    person.get('city', '').strip(),
                    person.get('state', '').strip()
                )
                
                db_config = DatabaseConfig.from_env()
                db_manager = DatabaseManager(db_config)
                existing_check = db_manager.execute_query(check_query, check_params)
                if existing_check:
                    logger.info(f"Person {person_name} already enriched since start, skipping")
                    print(f"SKIP: {person_name} (already enriched during this run)")
                    skipped_count += 1
                    progress_message = _write_zaba_progress(
                        progress_path,
                        total_candidates=total_candidates,
                        total_to_enrich=total_to_enrich,
                        processed=processed,
                        saved=saved_count,
                        skipped=skipped_count,
                        failed=failed_count
                    )
                    print(f"PROGRESS: {progress_message}")
                    sys.stdout.flush()
                    continue

                # Scrape the person
                zaba_result = enricher.scrape_person(person, max_retries=3)

                if zaba_result:
                    print(f"SCRAPED SUCCESS: {person_name}")
                    print(f"  Address: {zaba_result.get('mail_to_add1', 'N/A')}")
                    print(f"  ZIP: {zaba_result.get('zip', 'N/A')}")
                    
                    # Save to database IMMEDIATELY
                    try:
                        save_zaba_enrichment(person, zaba_result)
                        saved_count += 1
                        logger.info(f"Successfully scraped and saved {person_name}")
                        
                        # Build enriched record for return
                        enriched_record = {
                            'original_name': person_name,
                            'patent_number': person.get('patent_number', ''),
                            'zaba_data': zaba_result,
                            'enriched_at': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                            'mail_to_add1': zaba_result.get('mail_to_add1', ''),
                            'zip': zaba_result.get('zip', ''),
                            'email': (zaba_result.get('data', {}).get('email_addresses', []) or [''])[0]
                        }
                        newly_enriched.append(enriched_record)
                        
                    except Exception as save_error:
                        logger.error(f"Failed to save {person_name} to database: {save_error}")
                        print(f"SAVE FAILED: {person_name} - {save_error}")
                        failed_count += 1
                        
                else:
                    failed_count += 1
                    logger.warning(f"No ZabaSearch data found for {person_name}")
                    print(f"NO DATA FOUND: {person_name}")

            except Exception as e:
                failed_count += 1
                logger.error(f"Error enriching {person_name}: {e}")
                print(f"ERROR: {person_name} - {e}")
                continue

            # Update progress after each attempt
            progress_message = _write_zaba_progress(
                progress_path,
                total_candidates=total_candidates,
                total_to_enrich=total_to_enrich,
                processed=processed,
                saved=saved_count,
                skipped=skipped_count,
                failed=failed_count
            )
            print(f"PROGRESS: {progress_message}")
            sys.stdout.flush()

        print("\n" + "=" * 60)
        print(f"ZabaSearch enrichment completed!")
        print(f"   Successfully scraped & saved: {saved_count}")
        print(f"   Failed: {failed_count}")
        print(f"   Already enriched: {skipped_count}")

        # Load all enriched data for return (including pre-existing)
        all_enriched_data = load_existing_zaba_enriched()

        result = {
            'success': True,
            'total_people': len(people_to_enrich),
            'enriched_count': saved_count,
            'enrichment_rate': saved_count / len(new_people_to_enrich) * 100 if new_people_to_enrich else 0,
            'enriched_data': all_enriched_data,
            'newly_enriched_data': newly_enriched,
            'total_enriched_records': len(all_enriched_data),
            'actual_scrape_cost': '$0.00',  # Free scraping
            'already_enriched_count': skipped_count,
            'failed_count': failed_count,
            'method': 'zabasearch'
        }

        final_message = _write_zaba_progress(
            progress_path,
            total_candidates=total_candidates,
            total_to_enrich=total_to_enrich,
            processed=processed,
            saved=saved_count,
            skipped=skipped_count,
            failed=failed_count,
            stage='completed'
        )
        print(f"PROGRESS: {final_message}")
        sys.stdout.flush()
        
        logger.info(f"ZabaSearch enrichment completed: {saved_count} newly enriched, {failed_count} failed, {skipped_count} skipped")
        return result
        
    except Exception as e:
        logger.error(f"ZabaSearch enrichment failed: {e}")
        print(f"ZabaSearch enrichment failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'total_people': 0,
            'enriched_count': 0,
            'enriched_data': [],
            'actual_scrape_cost': '$0.00',
            'method': 'zabasearch'
        }# =============================================================================
# runners/run_zaba_enrich.py - ZabaSearch Web Scraping Enrichment
# Alternative enrichment process using ZabaSearch instead of PeopleDataLabs
# Calls the working standalone scrape script via subprocess
# =============================================================================

import logging
import os
import json
import time
import subprocess
from typing import Dict, Any, List, Optional
from pathlib import Path
import sys

# Add the parent directory to sys.path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from database.db_manager import DatabaseManager, DatabaseConfig

logger = logging.getLogger(__name__)

class ZabaSearchEnricher:
    """ZabaSearch web scraper that calls the working standalone script"""
    
    def __init__(self):
        # Path to the working scrape script
        self.script_path = Path(__file__).parent.parent / 'scripts' / 'scrape.py'
        
        if not self.script_path.exists():
            raise RuntimeError(f"ZabaSearch script not found at {self.script_path}")
        logger.info(f"Using ZabaSearch script at: {self.script_path}")
        
        # Try multiple Python paths in order of preference
        python_candidates = [
            str(Path(__file__).parent.parent / 'zaba_venv' / 'bin' / 'python3'),  # Local zaba_venv
            '/Users/ryanbussert/Desktop/Work/National_Engravers/scraping/venv/bin/python3',  # Original working venv
            'python3'  # System fallback
        ]
        
        self.working_python = None
        for candidate in python_candidates:
            if Path(candidate).exists() if candidate.startswith('/') else True:
                self.working_python = candidate
                logger.info(f"Using Python: {candidate}")
                break
        
        if not self.working_python:
            self.working_python = 'python3'
            logger.warning(f"No working Python found, using system python3")
    
    def scrape_person(self, person: Dict[str, Any], max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """Scrape ZabaSearch for a single person using the working script"""
        first_name = person.get('first_name', '').strip()
        last_name = person.get('last_name', '').strip()
        city = person.get('city', '').strip()
        state = person.get('state', '').strip()
        
        if not first_name or not last_name or not state:
            logger.warning(f"Missing required fields for {first_name} {last_name}")
            return None
        
        logger.info(f"Scraping ZabaSearch for {first_name} {last_name} ({city}, {state})")
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    wait_time = 2 ** attempt
                    logger.info(f"Retry {attempt + 1} for {first_name} {last_name} after {wait_time}s delay")
                    time.sleep(wait_time)
                
                # Build the command to call the working script using the working venv Python
                cmd = [
                    self.working_python, str(self.script_path),
                    '--first', first_name,
                    '--last', last_name,
                    '--state', state,
                    '--city', city,
                    '--json'
                ]
                
                # Log the exact command being run with working directory
                working_dir = str(Path(__file__).parent.parent)
                cmd_str = ' '.join(cmd)
                # logger.info(f"EXACT COMMAND TO RUN MANUALLY:")
                # logger.info(f"cd {working_dir}")
                # logger.info(f"{cmd_str}")
                # logger.info(f"Full command with absolute paths: {cmd}")
                
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=60,  # 60 second timeout
                    cwd=working_dir
                )
                
                logger.info(f"Command completed with return code: {result.returncode}")
                if result.stdout:
                    logger.info(f"STDOUT: {result.stdout[:500]}...")  # First 500 chars
                if result.stderr:
                    logger.info(f"STDERR: {result.stderr}")
                
                if result.returncode == 0:
                    # Parse the JSON output - Handle multi-line output
                    try:
                        if result.stdout.strip():
                            # Split output into lines and find the JSON line
                            lines = result.stdout.strip().split('\n')
                            json_line = None
                            
                            # Look for the line that starts with '{' (JSON)
                            for line in lines:
                                line = line.strip()
                                if line.startswith('{'):
                                    json_line = line
                                    break
                            
                            if json_line:
                                zaba_data = json.loads(json_line)
                                
                                # FIX: Clean up address format and reorder fields
                                zaba_data = self._clean_zaba_data(zaba_data)
                                
                                logger.info(f"Successfully scraped {first_name} {last_name} on attempt {attempt + 1}")
                                logger.debug(f"Parsed data keys: {list(zaba_data.keys())}")
                                return zaba_data
                            else:
                                logger.error(f"No JSON line found in output for {first_name} {last_name}")
                                logger.error(f"Full stdout: {repr(result.stdout)}")
                                if attempt < max_retries - 1:
                                    continue
                                else:
                                    return None
                        else:
                            logger.error(f"Empty output from script for {first_name} {last_name}")
                            if attempt < max_retries - 1:
                                continue
                            else:
                                return None
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse JSON output for {first_name} {last_name}: {e}")
                        logger.error(f"Raw stdout: {repr(result.stdout)}")
                        if attempt < max_retries - 1:
                            continue
                        else:
                            return None
                else:
                    # Script failed
                    logger.error(f"Script failed for {first_name} {last_name} (attempt {attempt + 1})")
                    logger.error(f"Return code: {result.returncode}")
                    logger.error(f"STDERR: {result.stderr}")
                    logger.error(f"STDOUT: {result.stdout}")
                    
                    if attempt < max_retries - 1:
                        continue
                    else:
                        return None
                        
            except subprocess.TimeoutExpired:
                logger.error(f"Script timeout for {first_name} {last_name} (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    continue
                else:
                    return None
            except Exception as e:
                logger.error(f"Subprocess error for {first_name} {last_name} (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    continue
                else:
                    return None
        
        return None

    def _clean_zaba_data(self, zaba_data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean up ZabaSearch data - fix address format and reorder fields"""
        if not isinstance(zaba_data, dict):
            return zaba_data
        
        # Get the current mail_to_add1 field
        mail_to_add1 = zaba_data.get('mail_to_add1', '')
        
        # Remove state from the address (e.g., "301 Reservoir RD Lunenburg, Massachusetts" -> "301 Reservoir RD Lunenburg")
        if mail_to_add1:
            # Split by comma and take everything except the last part (which should be state)
            parts = mail_to_add1.split(',')
            if len(parts) > 1:
                # Remove the last part (state) and rejoin
                cleaned_address = ','.join(parts[:-1]).strip()
            else:
                cleaned_address = mail_to_add1.strip()
        else:
            cleaned_address = ''
        
        # Create new ordered dictionary with zip and mail_to_add1 at the front
        cleaned_data = {}
        
        # Put zip and mail_to_add1 first
        cleaned_data['zip'] = zaba_data.get('zip', '')
        cleaned_data['mail_to_add1'] = cleaned_address
        
        # Add all other fields
        for key, value in zaba_data.items():
            if key not in ['zip', 'mail_to_add1']:
                cleaned_data[key] = value
        
        return cleaned_data

def _person_signature(person: Dict[str, Any]) -> str:
    """Build a stable signature for a person used for matching/skipping."""
    first_name = (person.get('first_name') or '').strip().lower()
    last_name = (person.get('last_name') or '').strip().lower()
    city = (person.get('city') or '').strip().lower()
    state = (person.get('state') or '').strip().lower()
    patent_number = (person.get('patent_number') or '').strip()
    return f"{first_name}_{last_name}_{city}_{state}_{patent_number}"

def load_existing_zaba_enriched() -> List[Dict[str, Any]]:
    """Load existing ZabaSearch enriched people from database"""
    try:
        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)
        
        # Query for records with zaba_data
        query = """
        SELECT * FROM enriched_people 
        WHERE zaba_data IS NOT NULL 
        ORDER BY enriched_at DESC
        """
        
        results = db_manager.execute_query(query)
        
        enriched_data = []
        for row in results:
            try:
                # Parse ZabaSearch data
                zaba_data = json.loads(row.get('zaba_data', '{}'))
                
                enriched_record = {
                    'original_name': f"{row.get('first_name', '')} {row.get('last_name', '')}".strip(),
                    'patent_number': row.get('patent_number', ''),
                    'enriched_data': {
                        'original_person': {
                            'first_name': row.get('first_name'),
                            'last_name': row.get('last_name'),
                            'city': row.get('city'),
                            'state': row.get('state'),
                            'country': row.get('country'),
                            'patent_number': row.get('patent_number')
                        },
                        'zaba_data': zaba_data
                    },
                    'enriched_at': row.get('enriched_at'),
                    'mail_to_add1': zaba_data.get('mail_to_add1', ''),
                    'zip': zaba_data.get('zip', ''),
                    'email': zaba_data.get('data', {}).get('email_addresses', [{}])[0] if zaba_data.get('data', {}).get('email_addresses') else ''
                }
                enriched_data.append(enriched_record)
                
            except Exception as e:
                logger.warning(f"Error parsing ZabaSearch row {row.get('id')}: {e}")
                continue
        
        return enriched_data
        
    except Exception as e:
        logger.warning(f"Error loading existing ZabaSearch enriched people: {e}")
        return []

def save_zaba_enrichment(person: Dict[str, Any], zaba_result: Dict[str, Any]):
    """Save ZabaSearch enrichment to database with detailed logging"""
    person_name = f"{person.get('first_name', '')} {person.get('last_name', '')}"
    
    try:
        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)
        
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
            
            if existing:
                # Update existing record with ONLY ZabaSearch data (no enrichment_data)
                update_query = """
                UPDATE enriched_people 
                SET zaba_data = %s 
                WHERE id = %s
                """
                cursor.execute(update_query, (
                    json.dumps(zaba_result),
                    existing[0] if isinstance(existing, tuple) else existing.get('id')
                ))
                conn.commit()
                print(f"SQL UPDATE: {person_name} - ZabaSearch data saved")
                logger.info(f"UPDATED SQL: {person_name} - ZabaSearch data saved to existing record")
            else:
                # Insert new record with ONLY ZabaSearch data (enrichment_data stays NULL)
                insert_query = """
                INSERT INTO enriched_people (
                    first_name, last_name, city, state, country,
                    patent_number, person_type, zaba_data, api_cost
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
                    json.dumps(zaba_result),
                    0.0  # Free scraping
                ))
                conn.commit()
                print(f"SQL INSERT: {person_name} - New ZabaSearch record created")
                logger.info(f"INSERTED SQL: {person_name} - New ZabaSearch enrichment record created")
            
            # Log the data being saved for verification
            print(f"  Address: {zaba_result.get('mail_to_add1', 'N/A')}")
            print(f"  ZIP: {zaba_result.get('zip', 'N/A')}")
            phones = zaba_result.get('data', {}).get('phone_numbers', [])
            emails = zaba_result.get('data', {}).get('email_addresses', [])
            if phones:
                print(f"  Phone: {phones[0]}")
            if emails:
                print(f"  Email: {emails[0]}")
            
    except Exception as e:
        print(f"SQL SAVE FAILED: {person_name} - Error: {e}")
        logger.error(f"SQL SAVE FAILED: {person_name} - Error: {e}")
        raise

def run_zaba_enrichment_legacy(config: Dict[str, Any]) -> Dict[str, Any]:
    """Legacy ZabaSearch enrichment process retained for reference."""
    
    try:
        logger.info("Starting ZabaSearch enrichment process...")
        print("Starting ZabaSearch web scraping enrichment...")
        
        # Load people to enrich
        people_to_enrich = []
        people_file = Path(config.get('OUTPUT_DIR', 'output')) / 'new_people_for_enrichment.json'
        
        if config.get('new_people_data'):
            people_to_enrich = config['new_people_data']
        elif people_file.exists():
            with open(people_file, 'r') as f:
                people_to_enrich = json.load(f)
        
        if not people_to_enrich:
            return {
                'success': True,
                'message': 'No people to enrich with ZabaSearch',
                'total_people': 0,
                'enriched_count': 0,
                'enriched_data': [],
                'newly_enriched_data': [],
                'actual_scrape_cost': '$0.00'
            }
        
        logger.info(f"Found {len(people_to_enrich)} people to potentially enrich")
        print(f"Loaded {len(people_to_enrich)} people for ZabaSearch enrichment")
        
        # Get database connection for efficient checking and saving
        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)
        
        # Check which people already have ZabaSearch data in SQL (direct check)
        logger.info("Checking database for existing ZabaSearch enrichments...")
        print("Checking for existing ZabaSearch data...")
        
        # Create a set to store people who already have ZabaSearch data
        already_enriched_people = set()
        
        # Check each person individually for ZabaSearch data
        for person in people_to_enrich:
            check_query = """
            SELECT id FROM enriched_people 
            WHERE LOWER(TRIM(first_name)) = LOWER(%s) 
            AND LOWER(TRIM(last_name)) = LOWER(%s)
            AND LOWER(TRIM(IFNULL(city,''))) = LOWER(%s)
            AND LOWER(TRIM(IFNULL(state,''))) = LOWER(%s)
            AND zaba_data IS NOT NULL
            LIMIT 1
            """
            
            check_params = (
                person.get('first_name', '').strip(),
                person.get('last_name', '').strip(),
                person.get('city', '').strip(),
                person.get('state', '').strip()
            )
            
            try:
                existing_check = db_manager.execute_query(check_query, check_params)
                if existing_check:
                    person_sig = _person_signature(person)
                    already_enriched_people.add(person_sig)
            except Exception as e:
                logger.warning(f"Could not check ZabaSearch status for {person.get('first_name')} {person.get('last_name')}: {e}")
        
        logger.info(f"Found {len(already_enriched_people)} people already enriched with ZabaSearch")
        print(f"Skipping {len(already_enriched_people)} already enriched people")
        
        # Filter out people already enriched with ZabaSearch
        new_people_to_enrich = []
        skipped_count = 0
        
        for person in people_to_enrich:
            person_sig = _person_signature(person)
            if person_sig in already_enriched_people:
                skipped_count += 1
                person_name = f"{person.get('first_name', '')} {person.get('last_name', '')}"
                logger.debug(f"Skipping {person_name} - already ZabaSearch enriched")
            else:
                new_people_to_enrich.append(person)
        
        logger.info(f"After filtering ZabaSearch duplicates: {len(new_people_to_enrich)} new people, {skipped_count} already enriched")
        print(f"Will enrich {len(new_people_to_enrich)} new people")
        
        # Test mode limit
        if config.get('TEST_MODE') and len(new_people_to_enrich) > 5:
            new_people_to_enrich = new_people_to_enrich[:5]
            logger.info(f"TEST MODE: Limited to {len(new_people_to_enrich)} people")
            print(f"TEST MODE: Limited to {len(new_people_to_enrich)} people")
        
        if not new_people_to_enrich:
            # Load existing enriched data for return
            existing_enriched = load_existing_zaba_enriched()
            return {
                'success': True,
                'message': 'All people already enriched with ZabaSearch',
                'total_people': len(people_to_enrich),
                'enriched_count': 0,
                'enriched_data': existing_enriched,
                'newly_enriched_data': [],
                'actual_scrape_cost': '$0.00',
                'already_enriched_count': skipped_count
            }
        
        # Initialize progress tracking
        progress_path = Path(config.get('OUTPUT_DIR', 'output')) / 'step2_zaba_progress.json'
        try:
            with open(progress_path, 'w') as pf:
                json.dump({
                    'step': 2,
                    'method': 'zabasearch',
                    'total': len(new_people_to_enrich) + skipped_count,
                    'processed': 0,
                    'newly_enriched': 0,
                    'already_enriched': skipped_count,
                    'stage': 'starting_zaba_enrichment',
                    'started_at': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }, pf)
        except Exception:
            pass
        
        # Enrich people with ZabaSearch - save immediately after each success
        enricher = ZabaSearchEnricher()
        newly_enriched = []
        processed = 0
        failed_count = 0
        saved_count = 0
        
        print(f"\nStarting ZabaSearch scraping for {len(new_people_to_enrich)} people...")
        print("=" * 60)
        
        for person in new_people_to_enrich:
            processed += 1
            person_name = f"{person.get('first_name', '')} {person.get('last_name', '')}"
            logger.info(f"Scraping {processed}/{len(new_people_to_enrich)}: {person_name}")
            print(f"\n[{processed}/{len(new_people_to_enrich)}] Scraping: {person_name}")
            
            try:
                # Add delay between requests to be respectful
                if processed > 1:
                    print("Waiting 5 seconds...")
                    time.sleep(5)  # 5 second delay between requests
                
                # Double-check this person isn't already in SQL (race condition protection)
                person_sig = _person_signature(person)
                check_query = """
                SELECT id FROM enriched_people 
                WHERE LOWER(TRIM(first_name)) = LOWER(%s) 
                AND LOWER(TRIM(last_name)) = LOWER(%s)
                AND LOWER(TRIM(IFNULL(city,''))) = LOWER(%s)
                AND LOWER(TRIM(IFNULL(state,''))) = LOWER(%s)
                AND zaba_data IS NOT NULL
                LIMIT 1
                """
                
                check_params = (
                    person.get('first_name', '').strip(),
                    person.get('last_name', '').strip(),
                    person.get('city', '').strip(),
                    person.get('state', '').strip()
                )
                
                existing_check = db_manager.execute_query(check_query, check_params)
                if existing_check:
                    logger.info(f"Person {person_name} already enriched since start, skipping")
                    print(f"SKIP: {person_name} (already enriched during this run)")
                    skipped_count += 1
                    continue
                
                # Scrape the person
                zaba_result = enricher.scrape_person(person, max_retries=3)
                
                if zaba_result:
                    print(f"SCRAPED SUCCESS: {person_name}")
                    print(f"  Address: {zaba_result.get('mail_to_add1', 'N/A')}")
                    print(f"  ZIP: {zaba_result.get('zip', 'N/A')}")
                    
                    # Save to database IMMEDIATELY
                    try:
                        save_zaba_enrichment(person, zaba_result)
                        saved_count += 1
                        logger.info(f"Successfully scraped and saved {person_name}")
                        
                        # Build enriched record for return
                        enriched_record = {
                            'original_name': person_name,
                            'patent_number': person.get('patent_number', ''),
                            'enriched_data': {
                                'original_person': person,
                                'zaba_data': zaba_result
                            },
                            'enriched_at': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                            'mail_to_add1': zaba_result.get('mail_to_add1', ''),
                            'zip': zaba_result.get('zip', ''),
                            'email': zaba_result.get('data', {}).get('email_addresses', [{}])[0] if zaba_result.get('data', {}).get('email_addresses') else ''
                        }
                        newly_enriched.append(enriched_record)
                        
                    except Exception as save_error:
                        logger.error(f"Failed to save {person_name} to database: {save_error}")
                        print(f"SAVE FAILED: {person_name} - {save_error}")
                        failed_count += 1
                        
                else:
                    failed_count += 1
                    logger.warning(f"No ZabaSearch data found for {person_name}")
                    print(f"NO DATA FOUND: {person_name}")
                
            except Exception as e:
                failed_count += 1
                logger.error(f"Error enriching {person_name}: {e}")
                print(f"ERROR: {person_name} - {e}")
                continue
            
            # Update progress
            try:
                with open(progress_path, 'w') as pf:
                    json.dump({
                        'step': 2,
                        'method': 'zabasearch',
                        'total': len(new_people_to_enrich) + skipped_count,
                        'processed': processed + skipped_count,
                        'newly_enriched': saved_count,
                        'already_enriched': skipped_count,
                        'failed': failed_count,
                        'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                    }, pf)
            except Exception:
                pass
        
        print("\n" + "=" * 60)
        print(f"ZabaSearch enrichment completed!")
        print(f"   Successfully scraped & saved: {saved_count}")
        print(f"   Failed: {failed_count}")
        print(f"   Already enriched: {skipped_count}")
        
        # Load all enriched data for return (including pre-existing)
        all_enriched_data = load_existing_zaba_enriched()
        
        result = {
            'success': True,
            'total_people': len(people_to_enrich),
            'enriched_count': saved_count,
            'enrichment_rate': saved_count / len(new_people_to_enrich) * 100 if new_people_to_enrich else 0,
            'enriched_data': all_enriched_data,
            'newly_enriched_data': newly_enriched,
            'actual_scrape_cost': '$0.00',  # Free scraping
            'already_enriched_count': skipped_count,
            'failed_count': failed_count,
            'method': 'zabasearch'
        }
        
        logger.info(f"ZabaSearch enrichment completed: {saved_count} newly enriched, {failed_count} failed, {skipped_count} skipped")
        return result
        
    except Exception as e:
        logger.error(f"ZabaSearch enrichment failed: {e}")
        print(f"ZabaSearch enrichment failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'total_people': 0,
            'enriched_count': 0,
            'enriched_data': [],
            'actual_scrape_cost': '$0.00',
            'method': 'zabasearch'
        }
