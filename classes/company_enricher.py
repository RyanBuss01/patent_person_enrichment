import time
import logging
import json
import os
from typing import Dict, List, Optional
from peopledatalabs import PDLPY

logger = logging.getLogger(__name__)


class CompanyEnricher:
    """Enrich trademark/business data using PeopleDataLabs Company API.

    Uses a two-step strategy:
    1. Company Enrichment API (direct one-to-one match, more precise)
    2. Company Search API fallback (Elasticsearch query, more flexible)

    The Enrichment API returns company data at the TOP LEVEL of the response.
    The Search API returns company data in a 'data' array.
    """

    def __init__(self, api_key: str, rate_limit_delay: float = 0.1):
        self.client = PDLPY(api_key=api_key)
        self.api_key = api_key
        self.rate_limit_delay = rate_limit_delay

    def enrich_company(self, params: Dict) -> Optional[Dict]:
        """Single company enrichment via GET /v5/company/enrich.

        params can include: name, website, ticker, profile, location,
        street_address, locality, region, country, postal_code, min_likelihood

        Returns the full API response dict on success (company fields at top level),
        or None on failure/no match.
        """
        try:
            clean_params = {k: v for k, v in params.items() if v not in (None, '', [])}
            if not clean_params:
                return None

            logger.debug(f"Company enrich params: {clean_params}")

            logger.info(f"Calling {self.client.company.ENRICHMENT_URL if hasattr(self.client.company, 'ENRICHMENT_URL') else 'company/enrich'} with params: {json.dumps({k: ('***' if k == 'api_key' else v) for k, v in clean_params.items()}, indent=2)}")

            response = self.client.company.enrichment(**clean_params)

            if response.status_code == 200:
                data = response.json()
                logger.info(f"Company enrich match: {data.get('name', 'unknown')} (likelihood={data.get('likelihood', '?')})")
                return data
            else:
                logger.info(f"Company enrich no match: HTTP {response.status_code} - {response.text[:200] if hasattr(response, 'text') else ''}")
                return None

        except Exception as e:
            logger.warning(f"Company enrichment error: {e}")
            return None

    def search_company_fallback(self, trademark: Dict, search_fields: List[str]) -> Optional[Dict]:
        """Fallback: use Company Search API when Enrichment API doesn't match.

        Builds an Elasticsearch query from the trademark data using 'match'
        for name (fuzzy) and 'term' for location fields (exact).

        Returns the best matching company profile dict, or None.
        """
        try:
            must_clauses = []

            name = (trademark.get('contact_name') or '').strip()
            if name and 'name' in search_fields:
                # Use match for fuzzy name matching (handles "COMPASS, INC." -> "compass")
                must_clauses.append({"match": {"name": name.lower()}})

            if 'location' in search_fields:
                city = (trademark.get('city') or '').strip().lower()
                state = (trademark.get('state') or '').strip().lower()
                if city:
                    must_clauses.append({"term": {"location.locality": city}})
                if state:
                    must_clauses.append({"term": {"location.region": state}})

            if not must_clauses:
                return None

            es_query = {
                "query": {
                    "bool": {
                        "must": must_clauses
                    }
                }
            }

            logger.info(f"Calling {self.client.company.SEARCH_URL if hasattr(self.client.company, 'SEARCH_URL') else 'company/search'} with params: {json.dumps({'size': 1, 'query': es_query}, indent=2)}")

            response = self.client.company.search(query=es_query, size=1)
            data = response.json()

            if data.get('status') == 200 and data.get('data'):
                company = data['data'][0]
                logger.info(f"Company search fallback match: {company.get('name', 'unknown')}")
                return company
            logger.info(f"Company search fallback no match: status={data.get('status')}, total={data.get('total', 0)}, error={data.get('error', 'none')}")
            return None

        except Exception as e:
            logger.warning(f"Company search fallback error: {e}")
            return None

    def search_companies(self, query=None, sql: str = None, size: int = 10) -> List[Dict]:
        """Company search via the PDLPY SDK.

        Supports Elasticsearch query object or SQL string.
        """
        try:
            params = {'size': size}
            if sql:
                params['sql'] = sql
            elif query:
                params['query'] = query
            else:
                return []

            response = self.client.company.search(**params)
            if response.status_code == 200:
                data = response.json()
                return data.get('data', [])
            return []

        except Exception as e:
            logger.warning(f"Company search error: {e}")
            return []

    def enrich_trademark_list(self, trademarks: List[Dict], search_fields: List[str],
                               test_mode: bool = False) -> Dict:
        """Main entry point: enrich a list of trademark records.

        Strategy per record:
        1. Try Company Enrichment API (direct match)
        2. If no match, try Company Search API (Elasticsearch fallback)
        3. If still no match, record as failed

        Args:
            trademarks: List of trademark dicts from TrademarkXMLParser
            search_fields: User-selected fields to search by (name, location, website, ticker, profile)
            test_mode: If True, only process first 5 records

        Returns:
            Dict with success, enriched_results, stats, etc.
        """
        if test_mode:
            trademarks = trademarks[:5]
            logger.info(f"TEST MODE: Processing only {len(trademarks)} records")

        total = len(trademarks)
        enriched_results = []
        failed_results = []
        api_calls = 0
        enrich_matches = 0
        search_matches = 0

        logger.info(f"Starting company enrichment for {total} trademarks using fields: {search_fields}")
        print(f"PROGRESS: Starting enrichment - {total} companies to process")

        for i, tm in enumerate(trademarks):
            try:
                # Build params from selected search fields
                params = self._build_company_params(tm, search_fields)
                if not params:
                    failed_results.append({
                        'trademark': tm,
                        'reason': 'Insufficient data for search',
                        'failure_code': 'no_params'
                    })
                    continue

                company_name = tm.get('contact_name', 'Unknown')

                # Step 1: Try Company Enrichment API (direct match)
                api_calls += 1
                result = self.enrich_company(params)
                api_method = 'company_enrichment'

                # enrich_company() returns dict on success, None on failure
                # (status check is done inside enrich_company via response.status_code)
                if result:
                    enrich_matches += 1
                    enriched_results.append(self._build_enriched_record(
                        tm, result, search_fields, params, api_method
                    ))
                    logger.info(f"  [ENRICH] '{company_name}' -> '{result.get('display_name') or result.get('name', '')}' (likelihood={result.get('likelihood', 0)})")
                else:
                    # Step 2: Fallback to Company Search API
                    api_calls += 1
                    search_result = self.search_company_fallback(tm, search_fields)
                    api_method = 'company_search'

                    if search_result:
                        search_matches += 1
                        enriched_results.append(self._build_enriched_record(
                            tm, search_result, search_fields, params, api_method
                        ))
                        logger.info(f"  [SEARCH] '{company_name}' -> '{search_result.get('display_name') or search_result.get('name', '')}' (via search fallback)")
                    else:
                        failed_results.append({
                            'trademark': tm,
                            'reason': 'No match found (tried enrichment + search)',
                            'failure_code': 'no_match'
                        })

                # Progress reporting
                if (i + 1) % 5 == 0 or (i + 1) == total:
                    pct = ((i + 1) / total) * 100
                    matched = len(enriched_results)
                    print(f"PROGRESS: Enriching companies - {i + 1}/{total} ({pct:.0f}%) - {matched} matched so far")

                # Rate limiting
                time.sleep(self.rate_limit_delay)

            except Exception as e:
                logger.warning(f"Error enriching company '{tm.get('contact_name', '')}': {e}")
                failed_results.append({
                    'trademark': tm,
                    'reason': str(e),
                    'failure_code': 'error'
                })

        enriched_count = len(enriched_results)
        enrichment_rate = (enriched_count / total * 100) if total > 0 else 0
        cost_per_call = 0.03  # Estimated PDL cost per credit
        estimated_cost = api_calls * cost_per_call

        result = {
            'success': True,
            'total_companies': total,
            'enriched_count': enriched_count,
            'failed_count': len(failed_results),
            'enrichment_rate': enrichment_rate,
            'api_calls': api_calls,
            'estimated_cost': f"${estimated_cost:.2f}",
            'enrich_matches': enrich_matches,
            'search_matches': search_matches,
            'search_fields_used': search_fields,
            'test_mode': test_mode,
            'enriched_results': enriched_results,
            'failed_results': failed_results
        }

        logger.info(
            f"Enrichment complete: {enriched_count}/{total} enriched ({enrichment_rate:.1f}%), "
            f"enrich API: {enrich_matches}, search fallback: {search_matches}, "
            f"{api_calls} API calls, est. cost ${estimated_cost:.2f}"
        )
        return result

    def _build_enriched_record(self, trademark: Dict, pdl_data: Dict,
                                search_fields: List[str], params: Dict,
                                api_method: str) -> Dict:
        """Build a standardized enriched result record."""
        return {
            'original_name': trademark.get('contact_name', ''),
            'trademark_number': trademark.get('trademark_number', ''),
            'match_score': pdl_data.get('likelihood', 0),
            'enriched_data': {
                'original_data': trademark,
                'pdl_data': pdl_data,
                'api_method': api_method,
                'search_fields_used': search_fields,
                'params_used': params
            }
        }

    def _build_company_params(self, trademark: Dict, search_fields: List[str]) -> Dict:
        """Map trademark data to PDL company enrichment parameters
        based on user-selected search fields."""
        params = {}

        if 'name' in search_fields:
            name = (trademark.get('contact_name') or '').strip()
            if name:
                params['name'] = name

        if 'location' in search_fields:
            # Build location string from address components
            loc_parts = []
            city = (trademark.get('city') or '').strip()
            state = (trademark.get('state') or '').strip()
            country = (trademark.get('country') or '').strip()
            if city:
                loc_parts.append(city)
            if state:
                loc_parts.append(state)
            if country and country not in ('US', 'USA'):
                loc_parts.append(country)
            if loc_parts:
                params['location'] = ', '.join(loc_parts)

            # Also pass structured location fields
            if city:
                params['locality'] = city
            if state:
                params['region'] = state
            zip_code = (trademark.get('zip_code') or '').strip()
            if zip_code:
                params['postal_code'] = zip_code

            # Street address
            addr1 = (trademark.get('address_1') or '').strip()
            addr2 = (trademark.get('address_2') or '').strip()
            street = ', '.join(filter(None, [addr1, addr2]))
            if street:
                params['street_address'] = street

        if 'website' in search_fields:
            website = (trademark.get('website') or '').strip()
            if website:
                params['website'] = website

        if 'ticker' in search_fields:
            ticker = (trademark.get('ticker') or '').strip()
            if ticker:
                params['ticker'] = ticker

        if 'profile' in search_fields:
            profile = (trademark.get('linkedin') or '').strip()
            if profile:
                params['profile'] = profile

        return params
