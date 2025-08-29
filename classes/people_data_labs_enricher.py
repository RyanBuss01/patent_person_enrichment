# =============================================================================
# classes/people_data_labs_enricher.py - ENHANCED FOR ACCESS DB INTEGRATION
# =============================================================================
import time
import logging
from typing import Dict, List, Optional
from peopledatalabs import PDLPY
from .data_models import PatentData, EnrichedData

logger = logging.getLogger(__name__)

class PeopleDataLabsEnricher:
    """Enrich patent data using PeopleDataLabs API"""
    
    def __init__(self, api_key: str, rate_limit_delay: float = 0.1):
        self.client = PDLPY(api_key=api_key)
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
                # Extract person data
                first_name = (person.get('first_name') or '').strip().lower()
                last_name = (person.get('last_name') or '').strip().lower()
                city = (person.get('city') or '').strip().lower()
                state = (person.get('state') or '').strip().lower()
                country = (person.get('country') or '').strip().lower()
                
                # Prepare PeopleDataLabs parameters
                params = {}
                if first_name:
                    params['first_name'] = first_name
                if last_name:
                    params['last_name'] = last_name
                if city:
                    params['location'] = city
                if state:
                    if 'location' in params:
                        params['location'] += f", {state}"
                    else:
                        params['location'] = state
                if country:
                    if 'location' in params:
                        params['location'] += f", {country}"
                    else:
                        params['location'] = country
                
                # Skip if insufficient data
                if not first_name and not last_name:
                    continue
                
                # Try enrichment
                enriched_person_data = self._enrich_single_person_new_format(person, params)
                if enriched_person_data:
                    enriched_results.append(enriched_person_data)
                
                # Rate limiting
                time.sleep(self.rate_limit_delay)
                
            except Exception as e:
                logger.warning(f"Error enriching person {person.get('first_name', '')} {person.get('last_name', '')}: {e}")
                continue
        
        logger.info(f"Enrichment completed. {len(enriched_results)} people successfully enriched")
        return enriched_results
    
    def _enrich_single_person_new_format(self, person: Dict, params: Dict) -> Optional[Dict]:
        """Enrich a single person in the new Access DB format"""
        try:
            # Try Person Identify first
            response = self.client.person.identify(**params)
            result = response.json()
            
            if result.get('status') == 200 and result.get('matches'):
                best_match = result['matches'][0]
                
                return {
                    'original_name': f"{person.get('first_name', '')} {person.get('last_name', '')}".strip(),
                    'patent_number': person.get('patent_number', ''),
                    'patent_title': person.get('patent_title', ''),
                    'match_score': best_match.get('match_score', 0.0),
                    'enriched_data': {
                        'person_type': person.get('person_type', ''),
                        'original_data': person,
                        'pdl_data': best_match,
                        'api_method': 'identify'
                    }
                }
            
            # Fallback to enrichment
            response = self.client.person.enrichment(**params)
            result = response.json()
            
            if result.get('status') == 200 and result.get('data'):
                return {
                    'original_name': f"{person.get('first_name', '')} {person.get('last_name', '')}".strip(),
                    'patent_number': person.get('patent_number', ''),
                    'patent_title': person.get('patent_title', ''),
                    'match_score': 1.0,
                    'enriched_data': {
                        'person_type': person.get('person_type', ''),
                        'original_data': person,
                        'pdl_data': result['data'],
                        'api_method': 'enrichment'
                    }
                }
                
        except Exception as e:
            logger.warning(f"API error for {person.get('first_name', '')} {person.get('last_name', '')}: {e}")
        
        return None
    
    def _enrich_person(self, person_data: Dict, patent: PatentData, person_type: str) -> Optional[EnrichedData]:
        """Enrich a single person using PeopleDataLabs"""
        try:
            # Prepare parameters for PeopleDataLabs
            params = {}
            
            if person_data.get('first_name'):
                params['first_name'] = person_data['first_name']
            if person_data.get('last_name'):
                params['last_name'] = person_data['last_name']
            if person_data.get('city'):
                params['location'] = person_data['city']
            if person_data.get('state'):
                if 'location' in params:
                    params['location'] += f", {person_data['state']}"
                else:
                    params['location'] = person_data['state']
            if person_data.get('country'):
                if 'location' in params:
                    params['location'] += f", {person_data['country']}"
                else:
                    params['location'] = person_data['country']
            
            # Skip if we don't have enough identifying information
            if not params.get('first_name') and not params.get('last_name'):
                return None
            
            # Try Person Identify first (gives multiple matches with scores)
            try:
                response = self.client.person.identify(**params)
                result = response.json()
                
                if result.get('status') == 200 and result.get('matches'):
                    # Use the first (best) match
                    best_match = result['matches'][0]
                    
                    original_name = f"{person_data.get('first_name', '')} {person_data.get('last_name', '')}".strip()
                    
                    return EnrichedData(
                        original_name=original_name,
                        patent_number=patent.patent_number,
                        patent_title=patent.patent_title,
                        enriched_data={
                            'person_type': person_type,
                            'original_data': person_data,
                            'pdl_data': best_match,
                            'api_method': 'identify'
                        },
                        match_score=best_match.get('match_score', 0.0)
                    )
                    
            except Exception as e:
                logger.warning(f"Person Identify failed for {person_data}: {e}")
            
            # Fallback to Person Enrichment if Identify fails
            try:
                enrichment_params = {}
                if params.get('first_name'):
                    enrichment_params['first_name'] = params['first_name']
                if params.get('last_name'):
                    enrichment_params['last_name'] = params['last_name']
                if params.get('location'):
                    enrichment_params['location'] = params['location']
                
                response = self.client.person.enrichment(**enrichment_params)
                result = response.json()
                
                if result.get('status') == 200 and result.get('data'):
                    original_name = f"{person_data.get('first_name', '')} {person_data.get('last_name', '')}".strip()
                    
                    return EnrichedData(
                        original_name=original_name,
                        patent_number=patent.patent_number,
                        patent_title=patent.patent_title,
                        enriched_data={
                            'person_type': person_type,
                            'original_data': person_data,
                            'pdl_data': result['data'],
                            'api_method': 'enrichment'
                        },
                        match_score=1.0  # Enrichment doesn't provide scores
                    )
                    
            except Exception as e:
                logger.warning(f"Person Enrichment failed for {person_data}: {e}")
                
        except Exception as e:
            logger.error(f"Error enriching person {person_data}: {e}")
        
        return None