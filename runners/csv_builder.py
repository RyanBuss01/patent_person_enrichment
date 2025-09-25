# =============================================================================
# runners/csv_builder.py - CSV Generation from Database
# Generates all CSV files from database data, handles both PDL and ZabaSearch
# =============================================================================

import logging
import os
import json
import csv
import shutil
from typing import Dict, Any, List
from pathlib import Path
from datetime import datetime
import sys

# Add the parent directory to sys.path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from database.db_manager import DatabaseManager, DatabaseConfig

logger = logging.getLogger(__name__)


def _parse_timestamp(value):
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith('Z'):
        text = text[:-1]
    candidates = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
    ]
    for fmt in candidates:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _zaba_signature(first_name: str, last_name: str, city: str, state: str, patent_number: str) -> str:
    parts = [
        (first_name or '').strip().lower(),
        (last_name or '').strip().lower(),
        (city or '').strip().lower(),
        (state or '').strip().lower(),
        (patent_number or '').strip()
    ]
    return '|'.join(parts)


def _extract_zaba_contact_info(zaba_data: dict) -> dict:
    emails = []
    phones = []
    try:
        emails = zaba_data.get('data', {}).get('email_addresses', []) or []
    except AttributeError:
        emails = []
    try:
        phones = zaba_data.get('data', {}).get('phone_numbers', []) or []
    except AttributeError:
        phones = []
    return {
        'email': emails[0] if emails else '',
        'phone': phones[0] if phones else ''
    }


SIMPLE_ZABA_HEADERS = [
    'first_name', 'last_name', 'city', 'state',
    'patent_number', 'mail_to_add1', 'mail_to_zip',
    'email', 'phone', 'enriched_at', 'source'
]

# CSV Headers
FORMATTED_HEADERS = [
    'issue_id', 'new_issue_rec_num', 'inventor_id', 'patent_no', 'title', 'issue_date',
    'mail_to_assignee', 'mail_to_name', 'mail_to_add1', 'mail_to_add2', 'mail_to_add3',
    'mail_to_city', 'mail_to_state', 'mail_to_zip', 'mail_to_country', 'mail_to_send_key',
    'inventor_first', 'inventor_last', 'mod_user', 'bar_code', 'inventor_contact'
]

CONTACT_HEADERS = ['first_name', 'last_name', 'email', 'address', 'zip', 'state']

def _sanitize_for_csv(val):
    """Sanitize values for CSV output"""
    if val is None:
        return ''
    if isinstance(val, bool):
        return ''
    s = str(val).strip()
    return '' if s.lower() in {'nan', 'null', 'none', 'true', 'false'} else s

def _first_non_empty(*vals):
    """Return first non-empty value from arguments"""
    for v in vals:
        s = _sanitize_for_csv(v)
        if s != '':
            return s
    return ''

def get_pdl_enriched_data():
    """Get PDL enriched data from database (enrichment_data IS NOT NULL)"""
    try:
        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)
        
        query = """
        SELECT * FROM enriched_people 
        WHERE enrichment_data IS NOT NULL 
        ORDER BY enriched_at DESC
        """
        
        results = db_manager.execute_query(query) or []
        
        items = []
        for row in results:
            try:
                # Parse PDL enrichment data
                enrichment_raw = row.get('enrichment_data')
                enrichment_data = json.loads(enrichment_raw) if enrichment_raw else {}
                
                item = {
                    'first_name': row.get('first_name'),
                    'last_name': row.get('last_name'),
                    'city': row.get('city'),
                    'state': row.get('state'),
                    'patent_number': row.get('patent_number'),
                    'enrichment_result': enrichment_data.get('enrichment_result', {}),
                    'original_person': enrichment_data.get('original_person', {}),
                    'enriched_at': row.get('enriched_at'),
                    'api_cost': row.get('api_cost', 0.0)
                }
                items.append(item)
                
            except Exception as e:
                logger.warning(f"Error parsing PDL row {row.get('id')}: {e}")
                continue
        
        return items
        
    except Exception as e:
        logger.warning(f"Error loading PDL data for CSV: {e}")
        return []

def get_zaba_enriched_data():
    """Get ZabaSearch enriched data from database (zaba_data IS NOT NULL)"""
    try:
        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)
        
        query = """
        SELECT * FROM enriched_people 
        WHERE zaba_data IS NOT NULL 
        ORDER BY enriched_at DESC
        """
        
        results = db_manager.execute_query(query) or []
        
        items = []
        for row in results:
            try:
                # Parse ZabaSearch data
                zaba_raw = row.get('zaba_data')
                zaba_data = json.loads(zaba_raw) if zaba_raw else {}
                
                item = {
                    'first_name': row.get('first_name'),
                    'last_name': row.get('last_name'),
                    'city': row.get('city'),
                    'state': row.get('state'),
                    'patent_number': row.get('patent_number'),
                    'zaba_data': zaba_data,
                    'enriched_at': row.get('enriched_at'),
                    'api_cost': 0.0  # ZabaSearch is free
                }
                items.append(item)
                
            except Exception as e:
                logger.warning(f"Error parsing ZabaSearch row {row.get('id')}: {e}")
                continue
        
        return items
        
    except Exception as e:
        logger.warning(f"Error loading ZabaSearch data for CSV: {e}")
        return []


def _normalize_zaba_record_from_result(item: dict) -> dict:
    zaba_data = item.get('zaba_data') or {}
    search_params = zaba_data.get('search_parameters') or {}
    first = search_params.get('first_name') or ''
    last = search_params.get('last_name') or ''
    city = search_params.get('city') or ''
    state = search_params.get('state') or ''
    normalized = {
        'first_name': first,
        'last_name': last,
        'city': city,
        'state': state,
        'patent_number': item.get('patent_number', ''),
        'zaba_data': zaba_data,
        'enriched_at': item.get('enriched_at'),
        'api_cost': 0.0
    }
    return normalized


def build_pdl_formatted_row(item: dict) -> dict:
    """Build formatted row from PDL data"""
    # Extract PDL enrichment data
    enrichment_result = item.get('enrichment_result', {})
    enriched_data = enrichment_result.get('enriched_data', {})
    original = item.get('original_person', {})
    pdl_data = enriched_data.get('pdl_data', {})
    
    # Extract key fields
    first = _first_non_empty(original.get('first_name'), item.get('first_name'))
    last = _first_non_empty(original.get('last_name'), item.get('last_name'))
    city = _first_non_empty(original.get('city'), item.get('city'))
    state = _first_non_empty(original.get('state'), item.get('state'))
    
    # Extract email from PDL data
    email = ''
    try:
        emails = pdl_data.get('emails', [])
        if emails:
            e0 = emails[0]
            if isinstance(e0, str):
                email = e0
            elif isinstance(e0, dict):
                email = e0.get('address', '')
    except Exception:
        pass
    
    # Extract address from PDL data
    street = _first_non_empty(pdl_data.get('job_company_location_street_address'))
    zip_code = _first_non_empty(pdl_data.get('job_company_location_postal_code'))
    
    formatted = {
        'issue_id': '',
        'new_issue_rec_num': '',
        'inventor_id': '',
        'patent_no': _first_non_empty(original.get('patent_number'), item.get('patent_number')),
        'title': _first_non_empty(original.get('patent_title')),
        'issue_date': '',
        'mail_to_assignee': '',
        'mail_to_name': f"{first} {last}".strip(),
        'mail_to_add1': street,
        'mail_to_add2': '',
        'mail_to_add3': '',
        'mail_to_city': city,
        'mail_to_state': state,
        'mail_to_zip': zip_code,
        'mail_to_country': _first_non_empty(original.get('country'), 'US'),
        'mail_to_send_key': '',
        'inventor_first': first,
        'inventor_last': last,
        'mod_user': '',
        'bar_code': '',
        'inventor_contact': 0
    }
    
    # Ensure all headers are present
    for h in FORMATTED_HEADERS:
        if h not in formatted:
            formatted[h] = ''
    
    return formatted

def build_zaba_formatted_row(item: dict) -> dict:
    """Build formatted row from ZabaSearch data"""
    zaba_data = item.get('zaba_data', {})
    
    first = item.get('first_name', '')
    last = item.get('last_name', '')
    city = item.get('city', '')
    state = item.get('state', '')
    
    # Extract email from ZabaSearch data
    email = ''
    try:
        emails = zaba_data.get('data', {}).get('email_addresses', [])
        if emails:
            email = emails[0]
    except Exception:
        pass
    
    # Extract address from ZabaSearch data (already cleaned by scraper)
    street = zaba_data.get('mail_to_add1', '')
    zip_code = zaba_data.get('zip', '')
    
    formatted = {
        'issue_id': '',
        'new_issue_rec_num': '',
        'inventor_id': '',
        'patent_no': item.get('patent_number', ''),
        'title': '',
        'issue_date': '',
        'mail_to_assignee': '',
        'mail_to_name': f"{first} {last}".strip(),
        'mail_to_add1': street,
        'mail_to_add2': '',
        'mail_to_add3': '',
        'mail_to_city': city,
        'mail_to_state': state,
        'mail_to_zip': zip_code,
        'mail_to_country': 'US',
        'mail_to_send_key': '',
        'inventor_first': first,
        'inventor_last': last,
        'mod_user': '',
        'bar_code': '',
        'inventor_contact': 0
    }
    
    # Ensure all headers are present
    for h in FORMATTED_HEADERS:
        if h not in formatted:
            formatted[h] = ''
    
    return formatted

def build_contact_row(item: dict, data_type: str) -> dict:
    """Build contact row from either PDL or ZabaSearch data"""
    first = item.get('first_name', '')
    last = item.get('last_name', '')
    state = item.get('state', '')
    
    email = ''
    address = ''
    zip_code = ''
    
    if data_type == 'pdl':
        # Extract from PDL data
        enrichment_result = item.get('enrichment_result', {})
        enriched_data = enrichment_result.get('enriched_data', {})
        pdl_data = enriched_data.get('pdl_data', {})
        
        try:
            emails = pdl_data.get('emails', [])
            if emails:
                e0 = emails[0]
                if isinstance(e0, str):
                    email = e0
                elif isinstance(e0, dict):
                    email = e0.get('address', '')
        except Exception:
            pass
        
        address = _first_non_empty(pdl_data.get('job_company_location_street_address'))
        zip_code = _first_non_empty(pdl_data.get('job_company_location_postal_code'))
        
    elif data_type == 'zaba':
        # Extract from ZabaSearch data
        zaba_data = item.get('zaba_data', {})
        
        try:
            emails = zaba_data.get('data', {}).get('email_addresses', [])
            if emails:
                email = emails[0]
        except Exception:
            pass
        
        address = zaba_data.get('mail_to_add1', '')
        zip_code = zaba_data.get('zip', '')
    
    contact_row = {
        'first_name': first,
        'last_name': last,
        'email': email,
        'address': address,
        'zip': zip_code,
        'state': state
    }
    
    return contact_row


def write_simple_zaba_csv(path: str, records: List[dict]) -> None:
    """Write a simplified CSV export for ZabaSearch records."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=SIMPLE_ZABA_HEADERS)
        writer.writeheader()
        for record in records:
            zaba_data = record.get('zaba_data', {})
            params = zaba_data.get('search_parameters', {})
            info = _extract_zaba_contact_info(zaba_data)
            row = {
                'first_name': _first_non_empty(record.get('first_name'), params.get('first_name')),
                'last_name': _first_non_empty(record.get('last_name'), params.get('last_name')),
                'city': _first_non_empty(record.get('city'), params.get('city')),
                'state': _first_non_empty(record.get('state'), params.get('state')),
                'patent_number': record.get('patent_number', ''),
                'mail_to_add1': zaba_data.get('mail_to_add1', ''),
                'mail_to_zip': zaba_data.get('zip', ''),
                'email': info['email'],
                'phone': info['phone'],
                'enriched_at': record.get('enriched_at', ''),
                'source': 'zabasearch'
            }
            writer.writerow(row)
    logger.info(f"Wrote {len(records)} ZabaSearch simplified rows to {path}")

def write_formatted_csv(path: str, records: List[dict], data_type: str) -> int:
    """Write formatted CSV file"""
    if data_type == 'pdl':
        rows_all = [build_pdl_formatted_row(r) for r in records]
    elif data_type == 'zaba':
        rows_all = [build_zaba_formatted_row(r) for r in records]
    else:
        logger.error(f"Unknown data type: {data_type}")
        return 0
    
    # Filter out rows with boolean address/zip issues
    rows = []
    removed = 0
    for row in rows_all:
        addr = (row.get('mail_to_add1') or '').strip().lower()
        zip_code = (row.get('mail_to_zip') or '').strip().lower()
        
        # Skip if boolean values
        if addr in {'true', 'false'} or zip_code in {'true', 'false'}:
            removed += 1
            continue
        
        # Skip if missing both address and zip
        if not addr and not zip_code:
            removed += 1
            continue
            
        rows.append(row)
    
    # Write CSV
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=FORMATTED_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    
    logger.info(f"Wrote {len(rows)} {data_type.upper()} formatted records to {path} (filtered {removed})")
    return removed

def write_contact_csv(path: str, records: List[dict], data_type: str) -> int:
    """Write contact CSV file"""
    contact_rows_all = [build_contact_row(r, data_type) for r in records]
    
    # Filter out rows with boolean address/zip issues or missing essential data
    contact_rows = []
    removed = 0
    
    for row in contact_rows_all:
        addr = (row.get('address') or '').strip().lower()
        zip_code = (row.get('zip') or '').strip().lower()
        email = (row.get('email') or '').strip()
        first_name = (row.get('first_name') or '').strip()
        last_name = (row.get('last_name') or '').strip()
        
        # Skip if boolean values in address/zip
        if addr in {'true', 'false'} or zip_code in {'true', 'false'}:
            removed += 1
            continue
        
        # Skip if missing essential contact info (name + at least one contact method)
        if not (first_name or last_name):
            removed += 1
            continue
        
        # Keep if we have at least email OR (address + zip)
        has_email = bool(email and '@' in email)
        has_address = bool(addr and zip_code)
        
        if not (has_email or has_address):
            removed += 1
            continue
        
        contact_rows.append(row)
    
    # Write CSV
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CONTACT_HEADERS)
        writer.writeheader()
        for row in contact_rows:
            writer.writerow(row)
    
    logger.info(f"Wrote {len(contact_rows)} {data_type.upper()} contact records to {path} (filtered {removed})")
    return removed

def write_combined_json(path: str, records: List[dict]) -> None:
    """Write combined JSON file"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(records, f, indent=2, default=str)
    
    logger.info(f"Wrote {len(records)} records to {path}")

def generate_all_csvs(config: Dict[str, Any]) -> Dict[str, Any]:
    """Generate all CSV files from database data"""
    try:
        output_dir = config.get('OUTPUT_DIR', 'output')
        use_zaba = config.get('USE_ZABA', False)
        
        files_generated = {}
        
        if use_zaba:
            logger.info("Generating ZabaSearch CSVs...")
            print("📊 Generating ZabaSearch CSVs from database...")

            run_started_at = _parse_timestamp(config.get('RUN_STARTED_AT'))
            enrichment_result = config.get('enrichment_result') or {}

            zaba_items = get_zaba_enriched_data()
            print(f"  Loaded {len(zaba_items)} ZabaSearch records from database")

            zaba_lookup = {}
            for row in zaba_items:
                sig = _zaba_signature(
                    row.get('first_name'),
                    row.get('last_name'),
                    row.get('city'),
                    row.get('state'),
                    row.get('patent_number')
                )
                if sig:
                    zaba_lookup[sig] = row

            new_items: List[dict] = []
            seen_new = set()

            raw_new_items = enrichment_result.get('newly_enriched_data') or []
            for new_item in raw_new_items:
                zaba_data = new_item.get('zaba_data') or {}
                params = zaba_data.get('search_parameters') or {}
                sig = _zaba_signature(
                    params.get('first_name'),
                    params.get('last_name'),
                    params.get('city'),
                    params.get('state'),
                    new_item.get('patent_number')
                )
                if sig and sig in zaba_lookup:
                    new_items.append(zaba_lookup[sig])
                    seen_new.add(sig)
                else:
                    normalized = _normalize_zaba_record_from_result(new_item)
                    new_items.append(normalized)
                    seen_new.add(_zaba_signature(
                        normalized.get('first_name'),
                        normalized.get('last_name'),
                        normalized.get('city'),
                        normalized.get('state'),
                        normalized.get('patent_number')
                    ))

            if run_started_at:
                for row in zaba_items:
                    sig = _zaba_signature(
                        row.get('first_name'),
                        row.get('last_name'),
                        row.get('city'),
                        row.get('state'),
                        row.get('patent_number')
                    )
                    if sig in seen_new:
                        continue
                    enriched_at = _parse_timestamp(row.get('enriched_at'))
                    if enriched_at and enriched_at >= run_started_at:
                        new_items.append(row)
                        seen_new.add(sig)

            if zaba_items:
                current_formatted_path = os.path.join(output_dir, 'current_enrichments_formatted.csv')
                removed_current = write_formatted_csv(current_formatted_path, zaba_items, 'zaba')
                files_generated[current_formatted_path] = {
                    'records_written': len(zaba_items) - removed_current,
                    'records_filtered': removed_current,
                    'data_type': 'current_formatted'
                }
                print(f"  📄 {current_formatted_path} ({len(zaba_items) - removed_current:,} records)")

                contact_current_path = os.path.join(output_dir, 'contact_current.csv')
                removed_contact_current = write_contact_csv(contact_current_path, zaba_items, 'zaba')
                files_generated[contact_current_path] = {
                    'records_written': len(zaba_items) - removed_contact_current,
                    'records_filtered': removed_contact_current,
                    'data_type': 'contact_current'
                }
                print(f"  📄 {contact_current_path} ({len(zaba_items) - removed_contact_current:,} records)")

                enriched_json_path = config.get('OUTPUT_JSON', os.path.join(output_dir, 'enriched_patents.json'))
                write_combined_json(enriched_json_path, zaba_items)
                files_generated[enriched_json_path] = {
                    'records_written': len(zaba_items),
                    'records_filtered': 0,
                    'data_type': 'enriched_json'
                }
                print(f"  📄 {enriched_json_path} ({len(zaba_items):,} records)")

                enriched_csv_path = config.get('OUTPUT_CSV', os.path.join(output_dir, 'enriched_patents.csv'))
                write_simple_zaba_csv(enriched_csv_path, zaba_items)
                files_generated[enriched_csv_path] = {
                    'records_written': len(zaba_items),
                    'records_filtered': 0,
                    'data_type': 'enriched_csv_simple'
                }
                print(f"  📄 {enriched_csv_path} ({len(zaba_items):,} records)")

                combined_csv_path = os.path.join(output_dir, 'new_and_existing_enrichments.csv')
                write_simple_zaba_csv(combined_csv_path, zaba_items)
                files_generated[combined_csv_path] = {
                    'records_written': len(zaba_items),
                    'records_filtered': 0,
                    'data_type': 'combined_simple'
                }

                legacy_formatted = os.path.join(output_dir, 'enrichments_formatted_zaba.csv')
                try:
                    shutil.copyfile(current_formatted_path, legacy_formatted)
                    files_generated[legacy_formatted] = {
                        'records_written': len(zaba_items) - removed_current,
                        'records_filtered': removed_current,
                        'data_type': 'zaba_formatted_alias',
                        'alias_of': current_formatted_path
                    }
                except Exception:
                    logger.debug("Could not create legacy Zaba formatted CSV alias")

                legacy_contact = os.path.join(output_dir, 'contacts_zaba.csv')
                try:
                    shutil.copyfile(contact_current_path, legacy_contact)
                    files_generated[legacy_contact] = {
                        'records_written': len(zaba_items) - removed_contact_current,
                        'records_filtered': removed_contact_current,
                        'data_type': 'zaba_contact_alias',
                        'alias_of': contact_current_path
                    }
                except Exception:
                    logger.debug("Could not create legacy Zaba contact CSV alias")

            else:
                print("  ⚠️ No ZabaSearch data found in database")

            new_formatted_path = os.path.join(output_dir, 'new_enrichments_formatted.csv')
            removed_new_formatted = write_formatted_csv(new_formatted_path, new_items, 'zaba')
            files_generated[new_formatted_path] = {
                'records_written': len(new_items) - removed_new_formatted,
                'records_filtered': removed_new_formatted,
                'data_type': 'new_formatted'
            }
            print(f"  📄 {new_formatted_path} ({len(new_items) - removed_new_formatted:,} records)")

            contact_new_path = os.path.join(output_dir, 'contact_new.csv')
            removed_contact_new = write_contact_csv(contact_new_path, new_items, 'zaba')
            files_generated[contact_new_path] = {
                'records_written': len(new_items) - removed_contact_new,
                'records_filtered': removed_contact_new,
                'data_type': 'contact_new'
            }
            print(f"  📄 {contact_new_path} ({len(new_items) - removed_contact_new:,} records)")

            new_enriched_csv_path = os.path.join(output_dir, 'new_enrichments.csv')
            write_simple_zaba_csv(new_enriched_csv_path, new_items)
            files_generated[new_enriched_csv_path] = {
                'records_written': len(new_items),
                'records_filtered': 0,
                'data_type': 'new_enriched_csv_simple'
            }

            legacy_new_formatted = os.path.join(output_dir, 'new_enrichments_formatted_zaba.csv')
            try:
                shutil.copyfile(new_formatted_path, legacy_new_formatted)
                files_generated[legacy_new_formatted] = {
                    'records_written': len(new_items) - removed_new_formatted,
                    'records_filtered': removed_new_formatted,
                    'data_type': 'new_formatted_alias',
                    'alias_of': new_formatted_path
                }
            except Exception:
                logger.debug("Could not create legacy Zaba new formatted CSV alias")

        else:
            # Your existing PDL CSV generation code...
            pass
        
        print(f"\n✅ CSV generation completed using {('ZabaSearch' if use_zaba else 'PeopleDataLabs')} data!")
        
        return {
            'success': True,
            'files_generated': files_generated,
            'method': 'zabasearch' if use_zaba else 'peopledatalabs'
        }
        
    except Exception as e:
        logger.error(f"CSV generation failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'files_generated': {}
        }
