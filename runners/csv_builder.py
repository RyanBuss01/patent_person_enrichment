# =============================================================================
# runners/csv_builder.py - CSV Generation from Database
# Generates all CSV files from database data, handles both PDL and ZabaSearch
# =============================================================================

import logging
import os
import json
import csv
import shutil
from typing import Dict, Any, List, Tuple, Set
from pathlib import Path
from datetime import datetime
from decimal import Decimal
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


def _person_signature_values(first_name: Any, last_name: Any, city: Any, state: Any, patent_number: Any) -> str:
    """Signature helper that mirrors enrichment duplicate checks."""
    parts = [
        (first_name or '').strip().lower() if isinstance(first_name, str) else str(first_name or '').strip().lower(),
        (last_name or '').strip().lower() if isinstance(last_name, str) else str(last_name or '').strip().lower(),
        (city or '').strip().lower() if isinstance(city, str) else str(city or '').strip().lower(),
        (state or '').strip().lower() if isinstance(state, str) else str(state or '').strip().lower(),
        (patent_number or '').strip() if isinstance(patent_number, str) else str(patent_number or '').strip()
    ]
    return '|'.join(parts)


def _load_signatures_from_csv(path: str) -> List[str]:
    """Read a CSV export from this module and return signatures for each data row."""
    signatures: List[str] = []
    if not os.path.exists(path):
        return signatures

    try:
        with open(path, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                sig = _person_signature_values(
                    row.get('first_name'),
                    row.get('last_name'),
                    row.get('city'),
                    row.get('state'),
                    row.get('patent_number') or row.get('patent_no')
                )
                if sig:
                    signatures.append(sig)
    except Exception as exc:
        logger.debug(f"Failed to read signatures from {path}: {exc}")

    return signatures


def _person_signature_from_dict(data: Dict[str, Any]) -> str:
    if not isinstance(data, dict):
        return ''
    return _person_signature_values(
        data.get('first_name'),
        data.get('last_name'),
        data.get('city'),
        data.get('state'),
        data.get('patent_number') or data.get('patent_no')
    )


def _stringify_value(value: Any) -> str:
    if value is None:
        return ''
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, 'f')
    if isinstance(value, bool):
        return 'true' if value else 'false'
    return str(value)


def _normalize_sql_row(row: Dict[str, Any], extra: Dict[str, Any] = None) -> Dict[str, Any]:
    normalized = {}
    for key, value in (row or {}).items():
        normalized[key] = _stringify_value(value)
    if extra:
        for key, value in extra.items():
            normalized[key] = _stringify_value(value)
    return normalized


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


def _unique_preserve_order(values: List[str]) -> List[str]:
    """Remove duplicates while preserving order (case-insensitive comparison)."""
    seen = set()
    unique = []
    for raw in values:
        value = _sanitize_for_csv(raw)
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(value)
    return unique


def _write_rows_to_csv(path: str, rows: List[Dict[str, Any]], preferred_order: List[str] = None) -> None:
    """Write rows to CSV ensuring consistent header ordering."""
    preferred_order = preferred_order or []
    header_set: Set[str] = set()
    for row in rows:
        header_set.update(row.keys())

    headers: List[str] = []
    for key in preferred_order:
        if key not in headers:
            headers.append(key)
            header_set.discard(key)
    headers.extend(sorted(h for h in header_set if h not in headers))
    if not headers:
        headers = list(preferred_order)

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({h: row.get(h, '') for h in headers})


def _normalize_pdl_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure PDL records expose consistent top-level fields."""
    if not item:
        return {}

    normalized = dict(item)

    enriched_blob = normalized.get('enriched_data') or normalized.get('enrichment_data') or {}
    if isinstance(enriched_blob, str):
        try:
            enriched_blob = json.loads(enriched_blob)
        except Exception:
            enriched_blob = {}
    normalized['enriched_data'] = enriched_blob if isinstance(enriched_blob, dict) else {}

    original_person = normalized.get('original_person')
    if not isinstance(original_person, dict):
        original_person = normalized['enriched_data'].get('original_person', {})
    normalized['original_person'] = original_person if isinstance(original_person, dict) else {}

    enrichment_result = normalized.get('enrichment_result')
    if not isinstance(enrichment_result, dict):
        enrichment_result = normalized['enriched_data'].get('enrichment_result', {})
    normalized['enrichment_result'] = enrichment_result if isinstance(enrichment_result, dict) else {}

    normalized['first_name'] = _first_non_empty(
        normalized.get('first_name'),
        normalized['original_person'].get('first_name')
    )
    normalized['last_name'] = _first_non_empty(
        normalized.get('last_name'),
        normalized['original_person'].get('last_name')
    )
    normalized['city'] = _first_non_empty(
        normalized.get('city'),
        normalized['original_person'].get('city')
    )
    normalized['state'] = _first_non_empty(
        normalized.get('state'),
        normalized['original_person'].get('state')
    )
    normalized['patent_number'] = _first_non_empty(
        normalized.get('patent_number'),
        normalized['original_person'].get('patent_number')
    )

    return normalized


def _extract_pdl_payload(item: Dict[str, Any]) -> Dict[str, Any]:
    """Return the PDL payload dict regardless of nesting differences."""
    if not item:
        return {}

    enriched_blob = item.get('enriched_data')
    if isinstance(enriched_blob, dict):
        pdl_data = enriched_blob.get('pdl_data')
        if isinstance(pdl_data, dict) and pdl_data:
            return pdl_data

    enrichment_result = item.get('enrichment_result')
    if isinstance(enrichment_result, dict):
        inner = enrichment_result.get('enriched_data')
        if isinstance(inner, dict):
            pdl_data = inner.get('pdl_data')
            if isinstance(pdl_data, dict) and pdl_data:
                return pdl_data
        api_raw = enrichment_result.get('api_raw')
        if isinstance(api_raw, dict):
            enrichment = api_raw.get('enrichment')
            if isinstance(enrichment, dict):
                data = enrichment.get('data')
                if isinstance(data, dict):
                    return data

    pdl_data = item.get('pdl_data')
    if isinstance(pdl_data, dict):
        return pdl_data

    return {}


def _collect_pdl_emails(pdl_data: Dict[str, Any]) -> List[Tuple[str, str]]:
    """Collect unique email addresses with their labels."""
    collected: List[Tuple[str, str]] = []

    def _append_email(address: Any, label: str):
        email = _sanitize_for_csv(address)
        if not email:
            return
        collected.append((email, label))

    for entry in pdl_data.get('emails', []) or []:
        if isinstance(entry, str):
            _append_email(entry, 'other')
        elif isinstance(entry, dict):
            _append_email(entry.get('address'), entry.get('type') or 'other')

    _append_email(pdl_data.get('work_email'), 'work')

    for entry in pdl_data.get('personal_emails', []) or []:
        _append_email(entry, 'personal')

    _append_email(pdl_data.get('recommended_personal_email'), 'personal')

    unique: List[Tuple[str, str]] = []
    seen = set()
    for email, label in collected:
        key = email.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append((email, label))
    return unique


def _format_address(street: str = '', locality: str = '', region: str = '', postal: str = '', country: str = '') -> str:
    parts = []
    street_clean = _sanitize_for_csv(street)
    if street_clean:
        parts.append(street_clean)

    locality_clean = _sanitize_for_csv(locality)
    region_clean = _sanitize_for_csv(region)
    locality_parts = [p for p in [locality_clean, region_clean] if p]
    if locality_parts:
        parts.append(', '.join(locality_parts))

    postal_clean = _sanitize_for_csv(postal)
    if postal_clean:
        parts.append(postal_clean)

    country_clean = _sanitize_for_csv(country)
    if country_clean and country_clean.lower() not in {'us', 'usa', 'united states'}:
        parts.append(country_clean)

    return ', '.join(parts)


def _collect_pdl_addresses(item: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """Collect personal and work addresses from a normalized PDL record."""
    personal: List[str] = []
    work: List[str] = []

    pdl_data = _extract_pdl_payload(item)
    if not isinstance(pdl_data, dict):
        pdl_data = {}

    def _add_personal(street='', locality='', region='', postal='', country=''):
        addr = _format_address(street, locality, region, postal, country)
        if addr:
            personal.append(addr)

    def _add_work(street='', locality='', region='', postal='', country=''):
        addr = _format_address(street, locality, region, postal, country)
        if addr:
            work.append(addr)

    # Primary personal location
    _add_personal(
        street=pdl_data.get('location_street_address') or pdl_data.get('location_name'),
        locality=pdl_data.get('location_locality'),
        region=pdl_data.get('location_region'),
        postal=pdl_data.get('location_postal_code'),
        country=pdl_data.get('location_country')
    )

    # Job/company location (work)
    _add_work(
        street=pdl_data.get('job_company_location_street_address') or pdl_data.get('job_company_location_name'),
        locality=pdl_data.get('job_company_location_locality'),
        region=pdl_data.get('job_company_location_region'),
        postal=pdl_data.get('job_company_location_postal_code'),
        country=pdl_data.get('job_company_location_country')
    )

    # Additional street addresses list
    for entry in pdl_data.get('street_addresses', []) or []:
        if not isinstance(entry, dict):
            continue
        target = work if (entry.get('type') or '').lower() in {'work', 'work_address', 'business'} else personal
        addr = _format_address(
            street=entry.get('street_address'),
            locality=entry.get('locality'),
            region=entry.get('region'),
            postal=entry.get('postal_code'),
            country=entry.get('country')
        )
        if addr:
            target.append(addr)

    # Existing record (mailing) as personal fallback
    existing_record = item.get('enriched_data', {}).get('existing_record', {})
    if isinstance(existing_record, dict):
        addr = _format_address(
            street=existing_record.get('mail_to_add1'),
            locality='',
            region='',
            postal=existing_record.get('mail_to_zip'),
            country=''
        )
        if addr:
            personal.append(addr)

    return _unique_preserve_order(personal), _unique_preserve_order(work)


def _extract_signatures_from_enriched_items(items: List[Dict[str, Any]]) -> Set[str]:
    signatures: Set[str] = set()
    if not items:
        return signatures

    for item in items:
        if not isinstance(item, dict):
            continue

        def _lookup(container: Dict[str, Any], key: str) -> Any:
            return container.get(key) if isinstance(container, dict) else None

        original = _lookup(item, 'original_person') or _lookup(item, 'original_data')
        enriched = _lookup(item, 'enriched_data') or _lookup(item, 'enrichment_data')
        enriched_original = _lookup(enriched, 'original_person') or _lookup(enriched, 'original_data')

        first = item.get('first_name') or _lookup(original, 'first_name') or _lookup(enriched_original, 'first_name')
        last = item.get('last_name') or _lookup(original, 'last_name') or _lookup(enriched_original, 'last_name')
        city = item.get('city') or _lookup(original, 'city') or _lookup(enriched_original, 'city')
        state = item.get('state') or _lookup(original, 'state') or _lookup(enriched_original, 'state')
        patent = item.get('patent_number') or _lookup(original, 'patent_number') or _lookup(enriched_original, 'patent_number')

        sig = _person_signature_values(first, last, city, state, patent)
        if sig.strip('|'):
            signatures.add(sig)

    return signatures

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
    """Build contact row from either PDL or ZabaSearch data."""
    if data_type == 'pdl':
        normalized = _normalize_pdl_item(item)
        pdl_data = _extract_pdl_payload(normalized)
        emails_with_labels = _collect_pdl_emails(pdl_data)
        return {
            'first_name': normalized.get('first_name', ''),
            'last_name': normalized.get('last_name', ''),
            'city': normalized.get('city', ''),
            'state': normalized.get('state', ''),
            'emails': emails_with_labels
        }

    if data_type == 'zaba':
        zaba_data = item.get('zaba_data', {}) or {}
        params = zaba_data.get('search_parameters', {}) or {}
        emails = []
        try:
            for entry in zaba_data.get('data', {}).get('email_addresses', []) or []:
                emails.append((_sanitize_for_csv(entry), 'personal'))
        except Exception:
            pass
        return {
            'first_name': _first_non_empty(item.get('first_name'), params.get('first_name')),
            'last_name': _first_non_empty(item.get('last_name'), params.get('last_name')),
            'city': _first_non_empty(item.get('city'), params.get('city')),
            'state': _first_non_empty(item.get('state'), params.get('state')),
            'emails': [(email, label) for email, label in emails if email]
        }

    logger.error(f"Unknown data type for build_contact_row: {data_type}")
    return {
        'first_name': _sanitize_for_csv(item.get('first_name')),
        'last_name': _sanitize_for_csv(item.get('last_name')),
        'city': _sanitize_for_csv(item.get('city')),
        'state': _sanitize_for_csv(item.get('state')),
        'emails': []
    }


def build_address_row(item: dict, data_type: str) -> dict:
    """Build address row separating personal and work addresses."""
    if data_type == 'pdl':
        normalized = _normalize_pdl_item(item)
        personal, work = _collect_pdl_addresses(normalized)
        return {
            'first_name': normalized.get('first_name', ''),
            'last_name': normalized.get('last_name', ''),
            'city': normalized.get('city', ''),
            'state': normalized.get('state', ''),
            'personal_addresses': personal,
            'work_addresses': work
        }

    if data_type == 'zaba':
        zaba_data = item.get('zaba_data', {}) or {}
        params = zaba_data.get('search_parameters', {}) or {}
        personal_addresses = []

        current_addr = _sanitize_for_csv((zaba_data.get('data', {}) or {}).get('addresses', {}).get('current'))
        if current_addr:
            personal_addresses.append(current_addr)

        try:
            for addr in (zaba_data.get('data', {}) or {}).get('addresses', {}).get('past', []) or []:
                clean = _sanitize_for_csv(addr)
                if clean:
                    personal_addresses.append(clean)
        except Exception:
            pass

        personal_addresses = _unique_preserve_order(personal_addresses)

        return {
            'first_name': _first_non_empty(item.get('first_name'), params.get('first_name')),
            'last_name': _first_non_empty(item.get('last_name'), params.get('last_name')),
            'city': _first_non_empty(item.get('city'), params.get('city')),
            'state': _first_non_empty(item.get('state'), params.get('state')),
            'personal_addresses': personal_addresses,
            'work_addresses': []
        }

    logger.error(f"Unknown data type for build_address_row: {data_type}")
    return {
        'first_name': _sanitize_for_csv(item.get('first_name')),
        'last_name': _sanitize_for_csv(item.get('last_name')),
        'city': _sanitize_for_csv(item.get('city')),
        'state': _sanitize_for_csv(item.get('state')),
        'personal_addresses': [],
        'work_addresses': []
    }


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


def generate_full_csv_exports(config: Dict[str, Any], db_manager: DatabaseManager, output_dir: str, files_generated: Dict[str, Dict[str, Any]]) -> None:
    """Generate the full CSV exports (current/new/new+existing/all) directly from SQL tables."""
    try:
        enriched_rows = db_manager.execute_query(
            "SELECT * FROM enriched_people ORDER BY enriched_at DESC"
        ) or []
    except Exception as exc:
        logger.warning(f"Unable to read enriched_people table for full exports: {exc}")
        return

    try:
        existing_people_rows = db_manager.execute_query("SELECT * FROM existing_people") or []
    except Exception:
        existing_people_rows = []

    run_started_at = _parse_timestamp(config.get('RUN_STARTED_AT'))
    enrichment_result = config.get('enrichment_result') or {}

    new_signatures = _extract_signatures_from_enriched_items(enrichment_result.get('newly_enriched_data'))
    scope_signatures = set()
    for collection in (
        config.get('new_people_data') or [],
        config.get('already_enriched_people') or [],
    ):
        for person in collection:
            sig = _person_signature_from_dict(person)
            if sig:
                scope_signatures.add(sig)
    scope_signatures.update(_extract_signatures_from_enriched_items(enrichment_result.get('matched_existing')))

    enriched_items: List[Tuple[Dict[str, Any], str, datetime]] = []
    enriched_scope_signatures: Set[str] = set()
    all_enriched_signatures: Set[str] = set()

    for row in enriched_rows:
        sig = _person_signature_values(
            row.get('first_name'),
            row.get('last_name'),
            row.get('city'),
            row.get('state'),
            row.get('patent_number') or row.get('patent_no')
        )
        ts = _parse_timestamp(row.get('enriched_at')) or _parse_timestamp(row.get('created_at')) or _parse_timestamp(row.get('updated_at'))
        enriched_items.append((row, sig, ts))
        all_enriched_signatures.add(sig)

    if not new_signatures and run_started_at:
        for _, sig, ts in enriched_items:
            if ts and ts >= run_started_at:
                new_signatures.add(sig)

    existing_items: List[Tuple[Dict[str, Any], str]] = []
    for row in existing_people_rows:
        sig = _person_signature_values(
            row.get('first_name'),
            row.get('last_name'),
            row.get('city'),
            row.get('state'),
            row.get('patent_no') or row.get('patent_number')
        )
        existing_items.append((row, sig))

    if not scope_signatures:
        scope_signatures = set(all_enriched_signatures)

    preferred_columns = ['record_scope', 'source_table', 'id', 'first_name', 'last_name', 'city', 'state', 'patent_number']

    # NEW rows
    new_rows_prepared: List[Dict[str, Any]] = []
    for row, sig, _ in enriched_items:
        if sig and sig in new_signatures:
            new_rows_prepared.append(_normalize_sql_row(row, {'source_table': 'enriched_people', 'record_scope': 'new'}))

    new_path = os.path.join(output_dir, 'new_enrichments.csv')
    _write_rows_to_csv(new_path, new_rows_prepared, preferred_columns)
    files_generated[new_path] = {
        'records_written': len(new_rows_prepared),
        'records_filtered': 0,
        'data_type': 'full_new'
    }
    print(f"  📄 {new_path} ({len(new_rows_prepared):,} records)")

    # NEW & EXISTING scope rows (limited to scope signatures when provided)
    new_and_existing_rows: List[Dict[str, Any]] = []
    for row, sig, _ in enriched_items:
        if not sig:
            continue
        if scope_signatures and sig not in scope_signatures:
            continue
        record_scope = 'new' if sig in new_signatures else 'existing'
        prepared = _normalize_sql_row(row, {'source_table': 'enriched_people', 'record_scope': record_scope})
        new_and_existing_rows.append(prepared)
        enriched_scope_signatures.add(sig)

    new_existing_path = os.path.join(output_dir, 'new_and_existing_enrichments.csv')
    _write_rows_to_csv(new_existing_path, new_and_existing_rows, preferred_columns)
    files_generated[new_existing_path] = {
        'records_written': len(new_and_existing_rows),
        'records_filtered': 0,
        'data_type': 'full_new_and_existing'
    }
    print(f"  📄 {new_existing_path} ({len(new_and_existing_rows):,} records)")

    # CURRENT rows = new & existing scope rows + existing_people rows that were filtered out
    current_rows: List[Dict[str, Any]] = list(new_and_existing_rows)
    if scope_signatures:
        for row, sig in existing_items:
            if not sig or sig not in scope_signatures:
                continue
            prepared = _normalize_sql_row(row, {'source_table': 'existing_people', 'record_scope': 'existing_table'})
            current_rows.append(prepared)

    current_path = os.path.join(output_dir, 'current_enrichments.csv')
    _write_rows_to_csv(current_path, current_rows, preferred_columns)
    files_generated[current_path] = {
        'records_written': len(current_rows),
        'records_filtered': 0,
        'data_type': 'full_current'
    }
    print(f"  📄 {current_path} ({len(current_rows):,} records)")

    # ALL rows = full enriched_people table
    all_rows_prepared: List[Dict[str, Any]] = []
    for row, sig, _ in enriched_items:
        record_scope = 'new' if sig in new_signatures else 'existing'
        all_rows_prepared.append(_normalize_sql_row(row, {'source_table': 'enriched_people', 'record_scope': record_scope}))

    all_path = os.path.join(output_dir, 'all_enrichments.csv')
    _write_rows_to_csv(all_path, all_rows_prepared, preferred_columns)
    files_generated[all_path] = {
        'records_written': len(all_rows_prepared),
        'records_filtered': 0,
        'data_type': 'full_all'
    }
    print(f"  📄 {all_path} ({len(all_rows_prepared):,} records)")

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
    contact_structs = [build_contact_row(r, data_type) for r in records]

    filtered_rows = []
    removed = 0
    max_emails = 0

    for row in contact_structs:
        first_name = _sanitize_for_csv(row.get('first_name'))
        last_name = _sanitize_for_csv(row.get('last_name'))
        if not first_name and not last_name:
            removed += 1
            continue

        email_entries = []
        for entry in row.get('emails') or []:
            if isinstance(entry, (list, tuple)) and entry:
                candidate = _sanitize_for_csv(entry[0])
            else:
                candidate = _sanitize_for_csv(entry)
            if candidate and '@' in candidate:
                email_entries.append(candidate)

        email_entries = _unique_preserve_order(email_entries)
        if not email_entries:
            removed += 1
            continue

        max_emails = max(max_emails, len(email_entries))
        filtered_rows.append({
            'first_name': first_name,
            'last_name': last_name,
            'city': _sanitize_for_csv(row.get('city')),
            'state': _sanitize_for_csv(row.get('state')),
            'emails': email_entries
        })

    if max_emails == 0:
        max_emails = 1

    email_headers = [f'email_{i + 1}' for i in range(max_emails)]
    headers = ['first_name', 'last_name', 'city', 'state'] + email_headers

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in filtered_rows:
            output_row = {
                'first_name': row['first_name'],
                'last_name': row['last_name'],
                'city': row['city'],
                'state': row['state']
            }
            for idx, email in enumerate(row['emails'], start=1):
                output_row[f'email_{idx}'] = email
            writer.writerow(output_row)

    logger.info(
        f"Wrote {len(filtered_rows)} {data_type.upper()} contact records to {path} "
        f"(filtered {removed}, max_emails={max_emails})"
    )
    return removed


def write_address_csv(path: str, records: List[dict], data_type: str) -> int:
    """Write address CSV file with personal/work columns."""
    address_structs = [build_address_row(r, data_type) for r in records]

    filtered_rows = []
    removed = 0
    max_personal = 0
    max_work = 0

    for row in address_structs:
        first_name = _sanitize_for_csv(row.get('first_name'))
        last_name = _sanitize_for_csv(row.get('last_name'))
        if not first_name and not last_name:
            removed += 1
            continue

        personal = _unique_preserve_order(row.get('personal_addresses') or [])
        work = _unique_preserve_order(row.get('work_addresses') or [])

        if not personal and not work:
            removed += 1
            continue

        max_personal = max(max_personal, len(personal))
        max_work = max(max_work, len(work))

        filtered_rows.append({
            'first_name': first_name,
            'last_name': last_name,
            'city': _sanitize_for_csv(row.get('city')),
            'state': _sanitize_for_csv(row.get('state')),
            'personal_addresses': personal,
            'work_addresses': work
        })

    personal_headers = [f'personal_address_{i + 1}' for i in range(max_personal)]
    work_headers = [f'work_address_{i + 1}' for i in range(max_work)]

    headers = ['first_name', 'last_name', 'city', 'state']
    headers.extend(personal_headers)
    headers.extend(work_headers)

    if len(headers) == 4:  # No address columns derived, add at least one personal column
        headers.append('personal_address_1')

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in filtered_rows:
            output_row = {
                'first_name': row['first_name'],
                'last_name': row['last_name'],
                'city': row['city'],
                'state': row['state']
            }

            for idx, address in enumerate(row['personal_addresses'], start=1):
                output_row[f'personal_address_{idx}'] = address

            for idx, address in enumerate(row['work_addresses'], start=1):
                output_row[f'work_address_{idx}'] = address

            writer.writerow(output_row)

    logger.info(
        f"Wrote {len(filtered_rows)} {data_type.upper()} address records to {path} "
        f"(filtered {removed}, max_personal={max_personal}, max_work={max_work})"
    )
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
        test_mode = bool(config.get('TEST_MODE'))

        files_generated = {}

        if test_mode:
            return _generate_test_mode_csvs(config, output_dir, use_zaba, files_generated)

        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)
        
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

                contact_current_addresses_path = os.path.join(output_dir, 'contact_current_addresses.csv')
                removed_contact_current_addresses = write_address_csv(contact_current_addresses_path, zaba_items, 'zaba')
                files_generated[contact_current_addresses_path] = {
                    'records_written': len(zaba_items) - removed_contact_current_addresses,
                    'records_filtered': removed_contact_current_addresses,
                    'data_type': 'contact_current_addresses'
                }
                print(
                    f"  📄 {contact_current_addresses_path} "
                    f"({len(zaba_items) - removed_contact_current_addresses:,} records)"
                )

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

            contact_new_addresses_path = os.path.join(output_dir, 'contact_new_addresses.csv')
            removed_contact_new_addresses = write_address_csv(contact_new_addresses_path, new_items, 'zaba')
            files_generated[contact_new_addresses_path] = {
                'records_written': len(new_items) - removed_contact_new_addresses,
                'records_filtered': removed_contact_new_addresses,
                'data_type': 'contact_new_addresses'
            }
            print(
                f"  📄 {contact_new_addresses_path} "
                f"({len(new_items) - removed_contact_new_addresses:,} records)"
            )

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
            logger.info("Generating PeopleDataLabs CSVs...")
            print("📊 Generating PeopleDataLabs CSVs from database...")

            enrichment_result = config.get('enrichment_result') or {}
            pdl_items = get_pdl_enriched_data()
            if not pdl_items:
                # Fallback to in-memory result when database read returns nothing (e.g., rebuild-only)
                pdl_items = enrichment_result.get('enriched_data') or []

            pdl_lookup: Dict[str, Dict[str, Any]] = {}
            for item in pdl_items:
                sig = _person_signature_values(
                    item.get('first_name'),
                    item.get('last_name'),
                    item.get('city'),
                    item.get('state'),
                    item.get('patent_number')
                )
                if sig and sig not in pdl_lookup:
                    pdl_lookup[sig] = item

            new_items = enrichment_result.get('newly_enriched_data') or []

            if pdl_items:
                contact_current_path = os.path.join(output_dir, 'contact_current.csv')
                removed_contact_current = write_contact_csv(contact_current_path, pdl_items, 'pdl')
                files_generated[contact_current_path] = {
                    'records_written': len(pdl_items) - removed_contact_current,
                    'records_filtered': removed_contact_current,
                    'data_type': 'contact_current'
                }
                print(f"  📄 {contact_current_path} ({len(pdl_items) - removed_contact_current:,} records)")

                contact_current_addresses_path = os.path.join(output_dir, 'contact_current_addresses.csv')
                removed_contact_current_addresses = write_address_csv(contact_current_addresses_path, pdl_items, 'pdl')
                files_generated[contact_current_addresses_path] = {
                    'records_written': len(pdl_items) - removed_contact_current_addresses,
                    'records_filtered': removed_contact_current_addresses,
                    'data_type': 'contact_current_addresses'
                }
                print(
                    f"  📄 {contact_current_addresses_path} "
                    f"({len(pdl_items) - removed_contact_current_addresses:,} records)"
                )
            else:
                print("  ⚠️ No PeopleDataLabs data found in database")

            if new_items:
                contact_new_path = os.path.join(output_dir, 'contact_new.csv')
                removed_contact_new = write_contact_csv(contact_new_path, new_items, 'pdl')
                files_generated[contact_new_path] = {
                    'records_written': len(new_items) - removed_contact_new,
                    'records_filtered': removed_contact_new,
                    'data_type': 'contact_new'
                }
                print(f"  📄 {contact_new_path} ({len(new_items) - removed_contact_new:,} records)")

                contact_new_addresses_path = os.path.join(output_dir, 'contact_new_addresses.csv')
                removed_contact_new_addresses = write_address_csv(contact_new_addresses_path, new_items, 'pdl')
                files_generated[contact_new_addresses_path] = {
                    'records_written': len(new_items) - removed_contact_new_addresses,
                    'records_filtered': removed_contact_new_addresses,
                    'data_type': 'contact_new_addresses'
                }
                print(
                    f"  📄 {contact_new_addresses_path} "
                    f"({len(new_items) - removed_contact_new_addresses:,} records)"
                )
            else:
                if pdl_items:
                    print("  ℹ️ No newly enriched PeopleDataLabs records for this run")

        generate_full_csv_exports(config, db_manager, output_dir, files_generated)

        if not use_zaba:
            # Build formatted CSVs for PeopleDataLabs data post-export
            new_csv_path = os.path.join(output_dir, 'new_enrichments.csv')
            new_existing_csv_path = os.path.join(output_dir, 'new_and_existing_enrichments.csv')
            current_csv_path = os.path.join(output_dir, 'current_enrichments.csv')

            new_formatted_path = os.path.join(output_dir, 'new_enrichments_formatted.csv')
            new_existing_formatted_path = os.path.join(output_dir, 'new_and_existing_enrichments_formatted.csv')
            current_formatted_path = os.path.join(output_dir, 'current_enrichments_formatted.csv')

            def _select_records(signatures: List[str]) -> List[Dict[str, Any]]:
                records: List[Dict[str, Any]] = []
                seen: Set[str] = set()
                missing = 0
                for sig in signatures:
                    if sig in seen:
                        continue
                    item = pdl_lookup.get(sig)
                    if item:
                        records.append(item)
                        seen.add(sig)
                    else:
                        missing += 1
                if missing:
                    logger.debug(
                        f"Missing {missing} PDL formatted records out of {len(signatures)} entries for {signatures[:1]}..."
                    )
                return records

            new_records = _select_records(_load_signatures_from_csv(new_csv_path))
            removed_new_formatted = write_formatted_csv(new_formatted_path, new_records, 'pdl')
            files_generated[new_formatted_path] = {
                'records_written': len(new_records) - removed_new_formatted,
                'records_filtered': removed_new_formatted,
                'data_type': 'new_formatted'
            }
            print(f"  📄 {new_formatted_path} ({len(new_records) - removed_new_formatted:,} records)")

            new_existing_records = _select_records(_load_signatures_from_csv(new_existing_csv_path))
            removed_new_existing_formatted = write_formatted_csv(
                new_existing_formatted_path,
                new_existing_records,
                'pdl'
            )
            files_generated[new_existing_formatted_path] = {
                'records_written': len(new_existing_records) - removed_new_existing_formatted,
                'records_filtered': removed_new_existing_formatted,
                'data_type': 'formatted_new_and_existing'
            }
            print(
                f"  📄 {new_existing_formatted_path} "
                f"({len(new_existing_records) - removed_new_existing_formatted:,} records)"
            )

            current_records = _select_records(_load_signatures_from_csv(current_csv_path))
            removed_current_formatted = write_formatted_csv(current_formatted_path, current_records, 'pdl')
            files_generated[current_formatted_path] = {
                'records_written': len(current_records) - removed_current_formatted,
                'records_filtered': removed_current_formatted,
                'data_type': 'current_formatted'
            }
            print(
                f"  📄 {current_formatted_path} "
                f"({len(current_records) - removed_current_formatted:,} records)"
            )

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


def _generate_test_mode_csvs(
    config: Dict[str, Any],
    output_dir: str,
    use_zaba: bool,
    files_generated: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    """Generate limited CSV outputs for test-mode enrichment runs."""

    enrichment_result = config.get('enrichment_result') or {}
    new_items = enrichment_result.get('newly_enriched_data') or []
    matched_existing = enrichment_result.get('matched_existing') or []
    current_cycle_records = list(new_items) + list(matched_existing)

    current_cycle_path = os.path.join(output_dir, 'current_cycle_enriched.json')
    write_combined_json(current_cycle_path, current_cycle_records)
    files_generated[current_cycle_path] = {
        'records_written': len(current_cycle_records),
        'records_filtered': 0,
        'data_type': 'current_cycle_snapshot'
    }

    preferred_columns = ['record_scope', 'source_table', 'id', 'first_name', 'last_name', 'city', 'state', 'patent_number']

    new_signatures = _extract_signatures_from_enriched_items(new_items)

    step1_existing_records = _load_json_list(os.path.join(output_dir, 'existing_people_in_db.json'))
    if not step1_existing_records:
        step1_existing_records = _load_json_list(os.path.join(output_dir, 'existing_people_found.json'))

    formatted_new_records: List[Dict[str, Any]] = []
    formatted_new_existing_records: List[Dict[str, Any]] = []
    formatted_current_records: List[Dict[str, Any]] = []

    new_rows_for_csv: List[Dict[str, Any]] = []
    new_and_existing_rows: List[Dict[str, Any]] = []
    current_rows: List[Dict[str, Any]] = []

    new_signatures_seen: Set[str] = set()
    new_existing_signatures_seen: Set[str] = set()
    current_signatures_seen: Set[str] = set()

    for item in current_cycle_records:
        original = (item.get('enriched_data') or {}).get('original_person') or {}
        sig = _person_signature_values(
            original.get('first_name'),
            original.get('last_name'),
            original.get('city'),
            original.get('state'),
            original.get('patent_number')
        )
        if not sig:
            continue

        record_scope = 'new' if sig in new_signatures else 'existing'
        formatted_item = _build_formatted_item_from_current_cycle(item, use_zaba)
        csv_row = _build_csv_row_from_current_cycle(item, record_scope, 'current_cycle', use_zaba)

        if sig not in new_existing_signatures_seen:
            formatted_new_existing_records.append(formatted_item)
            new_and_existing_rows.append(csv_row)
            new_existing_signatures_seen.add(sig)

        if record_scope == 'new' and sig not in new_signatures_seen:
            formatted_new_records.append(formatted_item)
            new_rows_for_csv.append(csv_row)
            new_signatures_seen.add(sig)

        if sig not in current_signatures_seen:
            formatted_current_records.append(formatted_item)
            current_rows.append(csv_row)
            current_signatures_seen.add(sig)

    for record in step1_existing_records:
        sig = _person_signature_values(
            record.get('first_name'),
            record.get('last_name'),
            record.get('city'),
            record.get('state'),
            record.get('patent_number') or record.get('patent_no')
        )
        if not sig or sig in current_signatures_seen:
            continue
        formatted_item = _build_formatted_item_from_step1(record, use_zaba)
        csv_row = _build_csv_row_from_step1(record, use_zaba)
        formatted_current_records.append(formatted_item)
        current_rows.append(csv_row)
        current_signatures_seen.add(sig)

    new_path = os.path.join(output_dir, 'new_enrichments.csv')
    _write_rows_to_csv(new_path, new_rows_for_csv, preferred_columns)
    files_generated[new_path] = {
        'records_written': len(new_rows_for_csv),
        'records_filtered': 0,
        'data_type': 'full_new'
    }

    new_formatted_path = os.path.join(output_dir, 'new_enrichments_formatted.csv')
    removed_new_formatted = write_formatted_csv(
        new_formatted_path,
        formatted_new_records,
        'zaba' if use_zaba else 'pdl'
    )
    files_generated[new_formatted_path] = {
        'records_written': len(formatted_new_records) - removed_new_formatted,
        'records_filtered': removed_new_formatted,
        'data_type': 'new_formatted'
    }

    new_existing_path = os.path.join(output_dir, 'new_and_existing_enrichments.csv')
    _write_rows_to_csv(new_existing_path, new_and_existing_rows, preferred_columns)
    files_generated[new_existing_path] = {
        'records_written': len(new_and_existing_rows),
        'records_filtered': 0,
        'data_type': 'full_new_and_existing'
    }

    new_existing_formatted_path = os.path.join(output_dir, 'new_and_existing_enrichments_formatted.csv')
    removed_new_existing_formatted = write_formatted_csv(
        new_existing_formatted_path,
        formatted_new_existing_records,
        'zaba' if use_zaba else 'pdl'
    )
    files_generated[new_existing_formatted_path] = {
        'records_written': len(formatted_new_existing_records) - removed_new_existing_formatted,
        'records_filtered': removed_new_existing_formatted,
        'data_type': 'formatted_new_and_existing'
    }

    current_path = os.path.join(output_dir, 'current_enrichments.csv')
    _write_rows_to_csv(current_path, current_rows, preferred_columns)
    files_generated[current_path] = {
        'records_written': len(current_rows),
        'records_filtered': 0,
        'data_type': 'full_current'
    }

    current_formatted_path = os.path.join(output_dir, 'current_enrichments_formatted.csv')
    removed_current_formatted = write_formatted_csv(
        current_formatted_path,
        formatted_current_records,
        'zaba' if use_zaba else 'pdl'
    )
    files_generated[current_formatted_path] = {
        'records_written': len(formatted_current_records) - removed_current_formatted,
        'records_filtered': removed_current_formatted,
        'data_type': 'current_formatted'
    }

    # Contact CSVs - limit to the current cycle records so they match the test subset
    contact_current_path = os.path.join(output_dir, 'contact_current.csv')
    removed_contact_current = write_contact_csv(contact_current_path, current_cycle_records, 'zaba' if use_zaba else 'pdl')
    files_generated[contact_current_path] = {
        'records_written': len(current_cycle_records) - removed_contact_current,
        'records_filtered': removed_contact_current,
        'data_type': 'contact_current'
    }

    contact_current_addresses_path = os.path.join(output_dir, 'contact_current_addresses.csv')
    removed_contact_current_addresses = write_address_csv(contact_current_addresses_path, current_cycle_records, 'zaba' if use_zaba else 'pdl')
    files_generated[contact_current_addresses_path] = {
        'records_written': len(current_cycle_records) - removed_contact_current_addresses,
        'records_filtered': removed_contact_current_addresses,
        'data_type': 'contact_current_addresses'
    }

    contact_new_path = os.path.join(output_dir, 'contact_new.csv')
    removed_contact_new = write_contact_csv(contact_new_path, new_items, 'zaba' if use_zaba else 'pdl')
    files_generated[contact_new_path] = {
        'records_written': len(new_items) - removed_contact_new,
        'records_filtered': removed_contact_new,
        'data_type': 'contact_new'
    }

    contact_new_addresses_path = os.path.join(output_dir, 'contact_new_addresses.csv')
    removed_contact_new_addresses = write_address_csv(contact_new_addresses_path, new_items, 'zaba' if use_zaba else 'pdl')
    files_generated[contact_new_addresses_path] = {
        'records_written': len(new_items) - removed_contact_new_addresses,
        'records_filtered': removed_contact_new_addresses,
        'data_type': 'contact_new_addresses'
    }

    return {
        'success': True,
        'files_generated': files_generated,
        'method': 'zabasearch' if use_zaba else 'peopledatalabs',
        'test_mode': True
    }
