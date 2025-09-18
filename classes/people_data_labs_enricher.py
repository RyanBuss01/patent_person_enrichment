# =============================================================================
# classes/people_data_labs_enricher.py - ENHANCED FOR ACCESS DB INTEGRATION
# =============================================================================
import time
import logging
import json
import os
import traceback
from typing import Dict, List, Optional
from peopledatalabs import PDLPY
from urllib import request as _urllib_request
from urllib.error import URLError, HTTPError
from .data_models import PatentData, EnrichedData

logger = logging.getLogger(__name__)

class PeopleDataLabsEnricher:
    """Enrich patent data using PeopleDataLabs API"""
    
    def __init__(self, api_key: str, rate_limit_delay: float = 0.1):
        self.client = PDLPY(api_key=api_key)
        self.api_key = api_key
        self.rate_limit_delay = rate_limit_delay
        self.enriched_data = []
    
    def enrich_patent_data(self, patents: List[PatentData]) -> List[EnrichedData]:
        """Enrich all patents with PeopleDataLabs data"""
        logger.info(f"Starting enrichment for {len(patents)} patents")
        
        for patent in patents:
            # Enrich inventors
            for inventor in patent.inventors:
                enriched = self._enrich_person(inventor, patent, "inventor")
                if enriched:
                    self.enriched_data.append(enriched)
                    
                time.sleep(self.rate_limit_delay)  # Rate limiting
            
            # Enrich assignees (if they're individuals, not organizations)
            for assignee in patent.assignees:
                if assignee.get('first_name') or assignee.get('last_name'):
                    enriched = self._enrich_person(assignee, patent, "assignee")
                    if enriched:
                        self.enriched_data.append(enriched)
                        
                    time.sleep(self.rate_limit_delay)  # Rate limiting
        
        logger.info(f"Enrichment complete. Found {len(self.enriched_data)} enriched records")
        return self.enriched_data
    
    def enrich_people_list(self, people_list: List[Dict]) -> List[Dict]:
        """
        ENHANCED: Enrich a list of people from Access DB style integration
        This method handles the new format from enhanced integration
        """
        logger.info(f"Starting enrichment for {len(people_list)} people using Access DB format")
        
        enriched_results = []
        
        for person in people_list:
            try:
                # Build normalized params (strip middle initials/suffixes, clean punctuation)
                params = self._build_params(person)
                
                # Skip if insufficient data (need at least a first or last name)
                if not (params.get('first_name') or params.get('last_name')):
                    continue
                
                # Try enrichment
                enriched_person_data = self._enrich_single_person_new_format(person, params)
                if enriched_person_data:
                    enriched_results.append(enriched_person_data)
                
                # Rate limiting
                time.sleep(self.rate_limit_delay)
                
            except Exception as e:
                logger.warning(
                    f"Error enriching person {person.get('first_name', '')} {person.get('last_name', '')}: {e}\n{traceback.format_exc()}"
                )
                continue
        
        logger.info(f"Enrichment completed. {len(enriched_results)} people successfully enriched")
        return enriched_results

    # Utility: build standard PDL params for a person
    def _build_params(self, person: Dict) -> Dict:
        def _clean(s: Optional[str]) -> str:
            return '' if s is None else str(s).strip()

        raw_first = _clean(person.get('first_name'))
        raw_last = _clean(person.get('last_name'))
        city = _clean(person.get('city'))
        state = _clean(person.get('state'))
        country = _clean(person.get('country')) or 'US'

        # DEBUG: Print raw values
        print(f"DEBUG RAW: first='{raw_first}', last='{raw_last}', city='{city}', state='{state}'")

        # Use names as-is (no aggressive stripping)
        first = raw_first
        last = raw_last

        # DEBUG: Print processed values
        print(f"DEBUG PROCESSED: first='{first}', last='{last}'")

        params: Dict[str, str] = {}
        if first:
            params['first_name'] = first
        if last:
            params['last_name'] = last

        loc_parts = []
        if city: loc_parts.append(city)
        if state: loc_parts.append(state)
        if country: loc_parts.append(country)
        if loc_parts:
            params['location'] = ', '.join(loc_parts)

        # DEBUG: Print final params
        print(f"DEBUG FINAL PARAMS: {params}")

        return params
   
    def bulk_enrich_people(self, people_list: List[Dict], include_if_matched: bool = True) -> List[Dict]:
        """Use PeopleDataLabs bulk enrichment to speed up processing.

        Returns a list of enriched records in the same structure used by
        enrich_people_list/_enrich_single_person_new_format.
        """
        if not people_list:
            return []
        try:
            requests = []
            for idx, person in enumerate(people_list):
                params = self._build_params(person)
                if not params:
                    continue
                requests.append({
                    'metadata': { 'idx': idx },
                    'params': params
                })
            if not requests:
                return []
            payload = {
                # Do not set a strict "required" to avoid over-filtering
                'include_if_matched': True if include_if_matched else False,
                'requests': requests
            }

            # Prefer direct HTTP to ensure we hit /v5/person/bulk (not any preview path)
            results, api_raw = self._http_person_bulk(payload)
            enriched_results: List[Dict] = []
            for i, r in enumerate(results or []):
                try:
                    if r and r.get('status') == 200 and r.get('data'):
                        # Map response to original person by position
                        person = people_list[i] if i < len(people_list) else {}
                        enriched_results.append({
                            'original_name': f"{person.get('first_name', '')} {person.get('last_name', '')}".strip(),
                            'patent_number': person.get('patent_number', ''),
                            'patent_title': person.get('patent_title', ''),
                            'match_score': 1.0,
                            'enriched_data': {
                                'person_type': person.get('person_type', 'inventor'),
                                'original_data': person,
                                'pdl_data': r['data'],
                                'api_method': 'bulk'
                            },
                            'api_raw': {
                                'bulk': api_raw  # keep the whole array for traceability
                            }
                        })
                except Exception:
                    continue
            return enriched_results
        except Exception as e:
            logger.warning(f"Bulk enrichment failed, falling back to single requests: {e}")
            return []
    
    def _enrich_single_person_new_format(self, person: Dict, params: Dict) -> Optional[Dict]:
        """Enrich a single person in the new Access DB format.
        Try enrichment first (returns full record), then fallback to identify+retrieve.
        """
        try:
            last_data = None
            last_er = None
            last_meta = None
            require_identify = False
            # Prefer Person Enrichment first (typically faster and returns full data)
            try:
                # Use direct HTTP to ensure the non-preview enrich endpoint
                er = self._http_person_enrich(params)
                if er.get('status') == 200 and er.get('data'):
                    data = er.get('data') or {}
                    # If enrichment returned presence-booleans for key fields, try a follow-up retrieve
                    try:
                        presence_keys = [
                            'location_street_address','location_postal_code','street_addresses',
                            'job_company_location_street_address','job_company_location_postal_code'
                        ]
                        looks_like_presence = any(isinstance(data.get(k), bool) for k in presence_keys)
                        if looks_like_presence:
                            rid = data.get('id') or data.get('pdl_id')
                            if rid:
                                full_resp = self.client.person.retrieve(id=rid)
                                full_js = full_resp.json()
                                if full_js.get('status') == 200 and full_js.get('data'):
                                    er['data'] = full_js['data']
                                    data = er['data']
                    except Exception:
                        pass

                    # Decide whether to attempt identify as a second pass (keep enrichment as fallback)
                    try:
                        lk = er.get('likelihood')
                    except Exception:
                        lk = None
                    still_presence = any(isinstance((data or {}).get(k), bool) for k in ['location_street_address','location_postal_code'])
                    min_lk_env = os.getenv('ENRICH_MIN_LIKELIHOOD')
                    min_lk_val = None
                    if min_lk_env:
                        try:
                            min_lk_val = int(min_lk_env)
                        except Exception:
                            min_lk_val = None
                    require_identify = bool(still_presence or (min_lk_val is not None and isinstance(lk, int) and lk < min_lk_val))

                    # Prepare debug meta (no secrets)
                    debug_meta = {
                        'endpoint': '/v5/person/enrich',
                        'params_used': {k: v for k, v in params.items() if k.lower() != 'api_key'}
                    }
                    last_data = data
                    last_er = er
                    last_meta = debug_meta
                    if not require_identify:
                        return {
                            'original_name': f"{person.get('first_name', '')} {person.get('last_name', '')}".strip(),
                            'patent_number': person.get('patent_number', ''),
                            'patent_title': person.get('patent_title', ''),
                            'match_score': 1.0,
                            'enriched_data': {
                                'person_type': person.get('person_type', ''),
                                'original_data': person,
                                'pdl_data': data,
                                'api_method': 'enrichment'
                            },
                            'api_raw': {
                                'enrichment': er,
                                'meta': debug_meta
                            }
                        }
            except Exception:
                # Continue to Identify path below
                pass

            # Fallback to Person Identify + Retrieve for complete data
            # Identify path: choose best by score + location and retrieve to ensure full data
            response = self.client.person.identify(**params)
            result = response.json()
            if result.get('status') == 200 and result.get('matches'):
                matches = result['matches'] or []
                # Heuristic: prefer highest match_score with matching location to provided city/state
                def _norm(v):
                    return ('' if v is None else str(v)).strip().lower()
                want_city = _norm(person.get('city'))
                want_state = _norm(person.get('state'))

                def _loc_match(m):
                    d = m.get('data') or m
                    # PDL identify may surface locality/region at top-level or within 'data'
                    city = _norm(d.get('location_locality') or d.get('locality'))
                    state = _norm(d.get('location_region') or d.get('region'))
                    score = 0
                    if want_state and state == want_state:
                        score += 2
                    if want_city and city == want_city:
                        score += 3
                    # Bonus if both present in one of the location_names strings
                    try:
                        names = d.get('location_names') or []
                        if isinstance(names, list):
                            for n in names:
                                ns = _norm(n)
                                if want_state and want_state in ns:
                                    score += 1
                                if want_city and want_city in ns:
                                    score += 1
                    except Exception:
                        pass
                    return score

                def _emails_profiles_bonus(m):
                    d = m.get('data') or m
                    bonus = 0
                    if d.get('emails') and not isinstance(d.get('emails'), bool):
                        bonus += 1
                    if d.get('profiles') and isinstance(d.get('profiles'), list) and d.get('profiles'):
                        bonus += 1
                    return bonus

                def _score(m):
                    ms = m.get('match_score', 0.0) or 0.0
                    return (float(ms) * 10.0) + _loc_match(m) + _emails_profiles_bonus(m)

                best_match = max(matches, key=_score)
                pdl_data = best_match
                api_method = 'identify'
                try:
                    match_id = best_match.get('id') or best_match.get('pdl_id')
                    if match_id:
                        resp_full = self.client.person.retrieve(id=match_id)
                        full_json = resp_full.json()
                        if full_json.get('status') == 200 and full_json.get('data'):
                            pdl_data = full_json['data']
                            api_method = 'identify+retrieve'
                except Exception:
                    pass

                return {
                    'original_name': f"{person.get('first_name', '')} {person.get('last_name', '')}".strip(),
                    'patent_number': person.get('patent_number', ''),
                    'patent_title': person.get('patent_title', ''),
                    'match_score': best_match.get('match_score', 0.0),
                    'enriched_data': {
                        'person_type': person.get('person_type', ''),
                        'original_data': person,
                        'pdl_data': pdl_data,
                        'api_method': api_method
                    },
                    'api_raw': {
                        'identify': result,
                        **({'retrieve': full_json} if 'full_json' in locals() else {})
                    }
                }

            # If Identify failed to produce a usable match, fall back to the Enrichment payload (even if presence-only)
            if last_data is not None and last_er is not None:
                return {
                    'original_name': f"{person.get('first_name', '')} {person.get('last_name', '')}".strip(),
                    'patent_number': person.get('patent_number', ''),
                    'patent_title': person.get('patent_title', ''),
                    'match_score': 1.0,
                    'enriched_data': {
                        'person_type': person.get('person_type', ''),
                        'original_data': person,
                        'pdl_data': last_data,
                        'api_method': 'enrichment'
                    },
                    'api_raw': {
                        'enrichment': last_er,
                        'meta': last_meta or {
                            'endpoint': '/v5/person/enrich',
                            'params_used': {k: v for k, v in params.items() if k.lower() != 'api_key'}
                        }
                    }
                }
        except Exception as e:
            logger.warning(f"API error for {person.get('first_name', '')} {person.get('last_name', '')}: {e}")
        return None
    
    def _enrich_person(self, person_data: Dict, patent: PatentData, person_type: str) -> Optional[EnrichedData]:
        """Enrich a single person using PeopleDataLabs with real field values (no presence-only)."""
        try:
            # --- Build params ---
            params = {}
            if person_data.get('first_name'):
                params['first_name'] = person_data['first_name']
            if person_data.get('last_name'):
                params['last_name'] = person_data['last_name']

            loc_parts = []
            if person_data.get('city'):    loc_parts.append(person_data['city'])
            if person_data.get('state'):   loc_parts.append(person_data['state'])
            if person_data.get('country'): loc_parts.append(person_data['country'])
            if loc_parts:
                params['location'] = ', '.join(loc_parts)

            # Need at least a name to proceed
            if not params.get('first_name') and not params.get('last_name'):
                return None

            original_name = f"{person_data.get('first_name', '')} {person_data.get('last_name', '')}".strip()

            # --- 1) Try ENRICH first (returns full data if matched) ---
            try:
                result = self._http_person_enrich(params)  # correct var
                if result.get("status") == 200 and result.get("data"):
                    return EnrichedData(
                        original_name=original_name,
                        patent_number=patent.patent_number,
                        patent_title=patent.patent_title,
                        enriched_data={
                            'person_type': person_type,
                            'original_data': person_data,
                            'pdl_data': result['data'],   # full data (not presence)
                            'api_method': 'enrichment'
                        },
                        match_score=1.0  # Enrich doesn't return a score
                    )
            except Exception as e:
                logger.warning(f"Person Enrichment failed for {person_data}: {e}")

            # --- 2) Fallback: IDENTIFY → RETRIEVE (to convert presence → full data) ---
            try:
                response = self.client.person.identify(**params)
                id_json = response.json()

                if id_json.get('status') == 200 and id_json.get('matches'):
                    best_match = id_json['matches'][0]
                    best_data = best_match.get('data', {})  # presence blob
                    match_id = (
                        best_data.get('id') or
                        best_data.get('pdl_id') or
                        best_match.get('id') or
                        best_match.get('pdl_id')
                    )

                    if match_id:
                        resp_full = self.client.person.retrieve(id=match_id)
                        full_json = resp_full.json()
                        if full_json.get('status') == 200 and full_json.get('data'):
                            return EnrichedData(
                                original_name=original_name,
                                patent_number=patent.patent_number,
                                patent_title=patent.patent_title,
                                enriched_data={
                                    'person_type': person_type,
                                    'original_data': person_data,
                                    'pdl_data': full_json['data'],  # full values here
                                    'api_method': 'identify+retrieve'
                                },
                                match_score=best_match.get('match_score', 0.0)
                            )

                    # If we got here, Identify succeeded but Retrieve didn’t → do NOT return presence.
                    logger.info(f"Identify returned presence only (no retrievable id) for {original_name}; skipping presence.")
            except Exception as e:
                logger.warning(f"Person Identify/Retrieve failed for {person_data}: {e}")

        except Exception as e:
            logger.error(f"Error enriching person {person_data}: {e}")

        return None

    # --- Internal HTTP helpers to force correct endpoints ---
    def _http_person_enrich(self, params: Dict, allow_required_env: bool = True) -> Dict:
        """
        POST https://api.peopledatalabs.com/v5/person/enrich
        - Logs the exact request (minus the key)
        - Optionally passes ENRICH_REQUIRED and ENRICH_MIN_LIKELIHOOD from env
        - If a 404 is returned and 'required' is present, retries once WITHOUT 'required'
        """
        import copy
        url = 'https://api.peopledatalabs.com/v5/person/enrich'

        def _do_post(payload: Dict) -> Dict:
            data = json.dumps(payload).encode('utf-8')
            req = _urllib_request.Request(url, data=data, method='POST')
            req.add_header('Accept', 'application/json')
            req.add_header('Content-Type', 'application/json')
            req.add_header('X-API-Key', self.api_key)
            
            # DEBUG: Log the exact request
            print(f"DEBUG API REQUEST: {json.dumps(payload, indent=2)}")
            print(f"DEBUG API KEY: {self.api_key[:10]}...")
            
            try:
                with _urllib_request.urlopen(req, timeout=30) as resp:
                    body = resp.read().decode('utf-8') if resp else ''
                    result = json.loads(body) if body else {}
                    print(f"DEBUG API RESPONSE: status={result.get('status')}, likelihood={result.get('likelihood')}")
                    return result
            except HTTPError as he:
                try:
                    body = he.read().decode('utf-8')
                    result = json.loads(body)
                    print(f"DEBUG API ERROR: {he.code} - {result}")
                    return result
                except Exception:
                    print(f"DEBUG API ERROR: {he.code} - {str(he)}")
                    return {'status': he.code, 'error': str(he)}
            except URLError as ue:
                print(f"DEBUG NETWORK ERROR: {ue}")
                # surface network errors
                raise RuntimeError(f"PDL enrich HTTP error: {ue}")

        # ---- normalize + env knobs (do NOT eval anything locally) ----
        normalized = {k: v for k, v in params.items() if v not in (None, '')}

        # Only pass 'required' if explicitly allowed (and log it!)
        if allow_required_env:
            req_expr = os.getenv('ENRICH_REQUIRED')
            if req_expr and 'required' not in normalized:
                normalized['required'] = req_expr

        min_lk = os.getenv('ENRICH_MIN_LIKELIHOOD')
        if min_lk and 'min_likelihood' not in normalized:
            try:
                normalized['min_likelihood'] = int(min_lk)
            except Exception:
                pass

        if os.getenv('ENRICH_DEBUG', '0') == '1':
            safe = copy.deepcopy(normalized)
            # log request
            logger.info(f"[PDL ENRICH →] {safe}")

        # ---- first attempt ----
        res = _do_post(normalized)

        if os.getenv('ENRICH_DEBUG', '0') == '1':
            logger.info(f"[PDL ENRICH ←] status={res.get('status')} likelihood={res.get('likelihood')} "
                        f"has_data={'data' in res and bool(res.get('data'))} "
                        f"error={res.get('error')}")

        # ---- auto-retry if 404 and required present ----
        if res.get('status') == 404 and 'required' in normalized:
            retry_payload = {k: v for k, v in normalized.items() if k != 'required'}
            if os.getenv('ENRICH_DEBUG', '0') == '1':
                logger.info("[PDL ENRICH RETRY] Removing 'required' and retrying once")
                logger.info(f"[PDL ENRICH →] {retry_payload}")
            res2 = _do_post(retry_payload)
            if os.getenv('ENRICH_DEBUG', '0') == '1':
                logger.info(f"[PDL ENRICH ←] status={res2.get('status')} likelihood={res2.get('likelihood')} "
                            f"has_data={'data' in res2 and bool(res2.get('data'))} "
                            f"error={res2.get('error')}")
            return res2

        return res

    def _http_person_bulk(self, payload: Dict) -> (List[Dict], Dict):
        """Call PDL /v5/person/bulk directly. Returns (results_array, raw_json)."""
        url = 'https://api.peopledatalabs.com/v5/person/bulk'
        data = json.dumps(payload).encode('utf-8')
        req = _urllib_request.Request(url, data=data, method='POST')
        req.add_header('Content-Type', 'application/json')
        req.add_header('X-API-Key', self.api_key)
        try:
            with _urllib_request.urlopen(req, timeout=60) as resp:
                body = resp.read().decode('utf-8')
                js = json.loads(body) if body else []
                # Ensure array for results; keep raw for api_raw
                arr = js if isinstance(js, list) else []
                return arr, js
        except HTTPError as he:
            try:
                body = he.read().decode('utf-8')
                js = json.loads(body)
                return (js if isinstance(js, list) else []), js
            except Exception:
                return [], { 'status': he.code, 'error': str(he) }
        except URLError as ue:
            raise RuntimeError(f"PDL bulk HTTP error: {ue}")
