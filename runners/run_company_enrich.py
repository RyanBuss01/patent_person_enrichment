# =============================================================================
# runners/run_company_enrich.py
# Run PDL Company enrichment on extracted trademark data
# =============================================================================
import json
import os
import csv
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQL table creation (MySQL)
# ---------------------------------------------------------------------------
ENRICHED_COMPANIES_DDL = """
CREATE TABLE IF NOT EXISTS enriched_companies (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    company_name VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(50),
    country VARCHAR(100),
    trademark_number VARCHAR(50),
    legal_entity_type VARCHAR(100),
    enrichment_data JSON,
    api_cost DECIMAL(10,2) DEFAULT 0.00,
    enriched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_enriched_company (company_name, city, state),
    INDEX idx_company_lookup (company_name, state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

FAILED_COMPANY_DDL = """
CREATE TABLE IF NOT EXISTS failed_company_enrichments (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    company_name VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100),
    trademark_number VARCHAR(50),
    legal_entity_type VARCHAR(100),
    failure_reason TEXT,
    failure_code VARCHAR(100),
    attempt_count INT DEFAULT 1,
    last_attempt_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    raw_trademark JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_failed_company (company_name, city, state),
    INDEX idx_company (company_name, state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def _ensure_company_tables(conn):
    """Create enriched_companies and failed_company_enrichments tables if they don't exist."""
    cursor = conn.cursor()
    try:
        cursor.execute(ENRICHED_COMPANIES_DDL)
        cursor.execute(FAILED_COMPANY_DDL)
        conn.commit()
        logger.info("Ensured enriched_companies and failed_company_enrichments tables exist")
    except Exception as e:
        logger.warning(f"Could not create company tables (may already exist): {e}")
    finally:
        cursor.close()


def _save_enriched_company(cursor, result: Dict):
    """Save a single enriched company to the database."""
    original = result.get('enriched_data', {}).get('original_data', {})
    pdl_data = result.get('enriched_data', {}).get('pdl_data', {})

    enrichment_data = {
        "original_trademark": original,
        "enrichment_result": {
            "pdl_data": pdl_data,
            "api_method": result.get('enriched_data', {}).get('api_method', ''),
        },
        "enrichment_metadata": {
            "enriched_at": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "api_cost": 0.03,
            "search_fields_used": result.get('enriched_data', {}).get('search_fields_used', []),
        }
    }

    query = """
        INSERT INTO enriched_companies (
            company_name, city, state, country, trademark_number,
            legal_entity_type, enrichment_data, api_cost
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            trademark_number = VALUES(trademark_number),
            enrichment_data = VALUES(enrichment_data),
            api_cost = VALUES(api_cost),
            updated_at = CURRENT_TIMESTAMP
    """
    params = (
        (original.get('contact_name') or '').strip(),
        (original.get('city') or '').strip(),
        (original.get('state') or '').strip(),
        (original.get('country') or 'US').strip(),
        (original.get('trademark_number') or '').strip(),
        (original.get('legal_entity_type') or '').strip(),
        json.dumps(enrichment_data, default=str),
        0.03
    )
    cursor.execute(query, params)


def _save_failed_company(cursor, failed: Dict):
    """Save a single failed enrichment to the database."""
    tm = failed.get('trademark', {})

    query = """
        INSERT INTO failed_company_enrichments (
            company_name, city, state, country, trademark_number,
            legal_entity_type, failure_reason, failure_code, raw_trademark
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            attempt_count = attempt_count + 1,
            last_attempt_at = CURRENT_TIMESTAMP,
            failure_reason = VALUES(failure_reason),
            failure_code = VALUES(failure_code)
    """
    params = (
        (tm.get('contact_name') or '').strip(),
        (tm.get('city') or '').strip(),
        (tm.get('state') or '').strip(),
        (tm.get('country') or 'US').strip(),
        (tm.get('trademark_number') or '').strip(),
        (tm.get('legal_entity_type') or '').strip(),
        failed.get('reason', ''),
        failed.get('failure_code', ''),
        json.dumps(tm, default=str)
    )
    cursor.execute(query, params)


def _load_already_enriched(cursor) -> set:
    """Load set of (company_name, city, state) already enriched from DB."""
    try:
        cursor.execute("SELECT LOWER(TRIM(company_name)), LOWER(TRIM(IFNULL(city,''))), LOWER(TRIM(IFNULL(state,''))) FROM enriched_companies")
        return {(r[0], r[1], r[2]) for r in cursor.fetchall()}
    except Exception as e:
        logger.warning(f"Could not load already-enriched companies: {e}")
        return set()


def _save_results_to_db(config: Dict, enriched_results: List[Dict], failed_results: List[Dict]):
    """Save enrichment results to the database."""
    try:
        from database.db_manager import DatabaseConfig, DatabaseManager
        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)

        with db_manager.get_connection() as conn:
            _ensure_company_tables(conn)
            cursor = conn.cursor()
            commit_interval = 50
            pending = 0

            # Save enriched results
            for result in enriched_results:
                try:
                    _save_enriched_company(cursor, result)
                    pending += 1
                    if pending >= commit_interval:
                        conn.commit()
                        pending = 0
                except Exception as e:
                    logger.warning(f"Error saving enriched company: {e}")

            # Save failed results
            for failed in failed_results:
                try:
                    _save_failed_company(cursor, failed)
                    pending += 1
                    if pending >= commit_interval:
                        conn.commit()
                        pending = 0
                except Exception as e:
                    logger.warning(f"Error saving failed company: {e}")

            # Final commit
            if pending > 0:
                conn.commit()

            cursor.close()
            logger.info(f"Saved {len(enriched_results)} enriched + {len(failed_results)} failed to database")
            print(f"PROGRESS: Saved {len(enriched_results)} enriched + {len(failed_results)} failed to SQL database")

    except Exception as e:
        logger.error(f"Database save failed (results still saved to JSON/CSV): {e}")
        print(f"PROGRESS: Warning - database save failed: {e}")


def run_company_enrichment(config: Dict) -> Dict:
    """Run PDL Company enrichment on trademark data.

    Steps:
    1. Load trademark data from output/business/extracted_trademarks.json
    2. Enrich using CompanyEnricher with user-selected search fields
    3. Save results to output/business/enriched_companies.json
    4. Generate formatted CSVs

    Returns: { success, total_companies, enriched_count, enrichment_rate, files_generated }
    """
    from classes.company_enricher import CompanyEnricher

    output_dir = config.get('OUTPUT_DIR', 'output/business')
    api_key = config.get('PEOPLEDATALABS_API_KEY', '')
    test_mode = config.get('TEST_MODE', False)
    search_fields = config.get('SEARCH_FIELDS', ['name', 'location'])

    os.makedirs(output_dir, exist_ok=True)

    # Load trademark data
    trademarks = config.get('trademark_data')
    if not trademarks:
        trademarks_file = os.path.join(output_dir, 'extracted_trademarks.json')
        if os.path.exists(trademarks_file):
            with open(trademarks_file, 'r') as f:
                trademarks = json.load(f)
        else:
            return {'success': False, 'error': 'No trademark data found. Run Step 1 first.'}

    if not trademarks:
        return {'success': False, 'error': 'Trademark data is empty.'}

    logger.info(f"Starting company enrichment for {len(trademarks)} trademarks")
    print(f"PROGRESS: Starting company enrichment - {len(trademarks)} trademarks loaded")

    # Load already-enriched companies from DB to skip (dedup)
    already_enriched = set()
    try:
        from database.db_manager import DatabaseConfig, DatabaseManager
        db_config = DatabaseConfig.from_env()
        db_manager = DatabaseManager(db_config)
        with db_manager.get_connection() as conn:
            _ensure_company_tables(conn)
            cursor = conn.cursor()
            already_enriched = _load_already_enriched(cursor)
            cursor.close()
        if already_enriched:
            logger.info(f"Loaded {len(already_enriched)} already-enriched companies from DB for dedup")
            print(f"PROGRESS: Found {len(already_enriched)} already-enriched companies in DB")
    except Exception as e:
        logger.warning(f"Could not load DB dedup data (proceeding without): {e}")

    # Filter out already-enriched trademarks
    skipped_dedup = 0
    if already_enriched:
        original_count = len(trademarks)
        trademarks = [
            tm for tm in trademarks
            if (
                (tm.get('contact_name') or '').strip().lower(),
                (tm.get('city') or '').strip().lower(),
                (tm.get('state') or '').strip().lower()
            ) not in already_enriched
        ]
        skipped_dedup = original_count - len(trademarks)
        if skipped_dedup > 0:
            logger.info(f"Skipped {skipped_dedup} already-enriched companies (DB dedup)")
            print(f"PROGRESS: Skipped {skipped_dedup} already-enriched companies")

    if not trademarks:
        return {
            'success': True,
            'total_companies': 0,
            'enriched_count': 0,
            'failed_count': 0,
            'enrichment_rate': 0,
            'api_calls': 0,
            'estimated_cost': '$0.00',
            'enrich_matches': 0,
            'search_matches': 0,
            'search_fields_used': search_fields,
            'test_mode': test_mode,
            'enriched_results': [],
            'failed_results': [],
            'skipped_dedup': skipped_dedup
        }

    # Initialize enricher
    enricher = CompanyEnricher(api_key=api_key)

    # Run enrichment
    result = enricher.enrich_trademark_list(
        trademarks=trademarks,
        search_fields=search_fields,
        test_mode=test_mode
    )

    if not result.get('success'):
        return result

    enriched_results = result.get('enriched_results', [])

    # Save enriched results JSON
    enriched_json_path = os.path.join(output_dir, 'enriched_companies.json')
    with open(enriched_json_path, 'w') as f:
        json.dump(enriched_results, f, indent=2, default=str)
    logger.info(f"Saved {len(enriched_results)} enriched companies to {enriched_json_path}")

    # Save failed results for debugging
    failed_results = result.get('failed_results', [])
    if failed_results:
        failed_json_path = os.path.join(output_dir, 'failed_enrichments.json')
        with open(failed_json_path, 'w') as f:
            json.dump(failed_results, f, indent=2, default=str)

    # Generate CSVs
    print(f"PROGRESS: Generating CSV exports")
    files_generated = _generate_csvs(enriched_results, output_dir)

    # Save to SQL database
    print(f"PROGRESS: Saving to SQL database")
    _save_results_to_db(config, enriched_results, failed_results)

    # Save enrichment metadata
    results_meta = {
        'success': True,
        'timestamp': datetime.utcnow().isoformat(),
        'total_companies': result.get('total_companies', 0),
        'enriched_count': result.get('enriched_count', 0),
        'failed_count': result.get('failed_count', 0),
        'enrichment_rate': result.get('enrichment_rate', 0),
        'api_calls': result.get('api_calls', 0),
        'estimated_cost': result.get('estimated_cost', '$0.00'),
        'search_fields_used': search_fields,
        'test_mode': test_mode,
        'files_generated': {k: {'records_written': v} for k, v in files_generated.items()}
    }

    results_meta_path = os.path.join(output_dir, 'enrichment_results.json')
    with open(results_meta_path, 'w') as f:
        json.dump(results_meta, f, indent=2, default=str)

    result['files_generated'] = {k: {'records_written': v} for k, v in files_generated.items()}
    result['skipped_dedup'] = skipped_dedup
    return result


def _generate_csvs(enriched_results: List[Dict], output_dir: str) -> Dict:
    """Generate all CSV formats from enriched company data."""
    files = {}

    # 1. Full enrichment CSV
    full_csv_path = os.path.join(output_dir, 'enriched_companies.csv')
    count = _write_full_csv(enriched_results, full_csv_path)
    files[full_csv_path] = count

    # 2. Raw enrichment CSV (mirrors DB table rows 1:1)
    raw_csv_path = os.path.join(output_dir, 'enriched_companies_raw.csv')
    count = _write_raw_csv(enriched_results, raw_csv_path)
    files[raw_csv_path] = count

    return files


def _write_full_csv(enriched_results: List[Dict], filepath: str) -> int:
    """Write full enrichment CSV with all available data."""
    if not enriched_results:
        return 0

    headers = [
        'trademark_number', 'original_name', 'match_score',
        # Original data
        'original_address_1', 'original_address_2', 'original_city',
        'original_state', 'original_zip', 'original_country',
        'original_legal_entity_type', 'all_serial_numbers', 'all_registration_numbers',
        # Enriched company data
        'company_name', 'company_display_name', 'company_size',
        'company_industry', 'company_website', 'company_linkedin_url',
        'company_founded', 'company_type', 'company_ticker',
        'company_location_locality', 'company_location_region',
        'company_location_country', 'company_location_street_address',
        'company_location_postal_code',
        'company_employee_count', 'company_phone',
        'company_description'
    ]

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
        writer.writeheader()

        for result in enriched_results:
            original = result.get('enriched_data', {}).get('original_data', {})
            pdl = result.get('enriched_data', {}).get('pdl_data', {})

            row = {
                'trademark_number': result.get('trademark_number', ''),
                'original_name': result.get('original_name', ''),
                'match_score': result.get('match_score', ''),
                # Original
                'original_address_1': original.get('address_1', ''),
                'original_address_2': original.get('address_2', ''),
                'original_city': original.get('city', ''),
                'original_state': original.get('state', ''),
                'original_zip': original.get('zip_code', ''),
                'original_country': original.get('country', ''),
                'original_legal_entity_type': original.get('legal_entity_type', ''),
                'all_serial_numbers': original.get('all_serial_numbers', ''),
                'all_registration_numbers': original.get('all_registration_numbers', ''),
                # Enriched
                'company_name': pdl.get('name', ''),
                'company_display_name': pdl.get('display_name', ''),
                'company_size': pdl.get('size', ''),
                'company_industry': pdl.get('industry', ''),
                'company_website': pdl.get('website', ''),
                'company_linkedin_url': pdl.get('linkedin_url', ''),
                'company_founded': pdl.get('founded', ''),
                'company_type': pdl.get('type', ''),
                'company_ticker': pdl.get('ticker', ''),
                'company_location_locality': pdl.get('location', {}).get('locality', '') if isinstance(pdl.get('location'), dict) else '',
                'company_location_region': pdl.get('location', {}).get('region', '') if isinstance(pdl.get('location'), dict) else '',
                'company_location_country': pdl.get('location', {}).get('country', '') if isinstance(pdl.get('location'), dict) else '',
                'company_location_street_address': pdl.get('location', {}).get('street_address', '') if isinstance(pdl.get('location'), dict) else '',
                'company_location_postal_code': pdl.get('location', {}).get('postal_code', '') if isinstance(pdl.get('location'), dict) else '',
                'company_employee_count': pdl.get('employee_count', ''),
                'company_phone': pdl.get('phone', ''),
                'company_description': (pdl.get('summary', '') or '')[:500]
            }
            writer.writerow(row)

    logger.info(f"Wrote {len(enriched_results)} records to {filepath}")
    return len(enriched_results)


def _write_raw_csv(enriched_results: List[Dict], filepath: str) -> int:
    """Write raw enrichment CSV that mirrors the enriched_companies DB table 1:1."""
    if not enriched_results:
        return 0

    headers = [
        'company_name', 'city', 'state', 'country', 'trademark_number',
        'legal_entity_type', 'enrichment_data', 'api_cost', 'enriched_at'
    ]

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
        writer.writeheader()

        for result in enriched_results:
            original = result.get('enriched_data', {}).get('original_data', {})
            pdl_data = result.get('enriched_data', {}).get('pdl_data', {})

            enrichment_data = {
                "original_trademark": original,
                "enrichment_result": {
                    "pdl_data": pdl_data,
                    "api_method": result.get('enriched_data', {}).get('api_method', ''),
                },
                "enrichment_metadata": {
                    "enriched_at": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "api_cost": 0.03,
                    "search_fields_used": result.get('enriched_data', {}).get('search_fields_used', []),
                }
            }

            row = {
                'company_name': (original.get('contact_name') or '').strip(),
                'city': (original.get('city') or '').strip(),
                'state': (original.get('state') or '').strip(),
                'country': (original.get('country') or 'US').strip(),
                'trademark_number': (original.get('trademark_number') or '').strip(),
                'legal_entity_type': (original.get('legal_entity_type') or '').strip(),
                'enrichment_data': json.dumps(enrichment_data, default=str),
                'api_cost': '0.03',
                'enriched_at': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            }
            writer.writerow(row)

    logger.info(f"Wrote {len(enriched_results)} raw records to {filepath}")
    return len(enriched_results)
