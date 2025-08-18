# =============================================================================
# runners/integrate_dynamics.py
# Step 3: Integration with Microsoft Dynamics CRM
# =============================================================================
import requests
import json
import logging
from typing import Dict, Any, List
from datetime import datetime
import time

logger = logging.getLogger(__name__)

class DynamicsCRMIntegrator:
    """Integrate enriched patent data into Microsoft Dynamics CRM"""
    
    def __init__(self, tenant_id: str, client_id: str, client_secret: str, crm_url: str):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.crm_url = crm_url.rstrip('/')
        self.access_token = None
        self.token_expires = None
    
    def authenticate(self) -> bool:
        """Authenticate with Microsoft Dynamics CRM"""
        try:
            auth_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            data = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'scope': f"{self.crm_url}/.default"
            }
            
            response = requests.post(auth_url, headers=headers, data=data)
            
            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data['access_token']
                self.token_expires = datetime.now().timestamp() + token_data['expires_in'] - 60  # 1 min buffer
                logger.info("Successfully authenticated with Dynamics CRM")
                return True
            else:
                logger.error(f"Authentication failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return False
    
    def _ensure_authenticated(self) -> bool:
        """Ensure we have a valid access token"""
        if not self.access_token or datetime.now().timestamp() > self.token_expires:
            return self.authenticate()
        return True
    
    def create_lead(self, enriched_person: Dict) -> Dict[str, Any]:
        """Create a lead in Dynamics CRM from enriched person data"""
        if not self._ensure_authenticated():
            return {'success': False, 'error': 'Authentication failed'}
        
        try:
            # Extract data from enriched person
            pdl_data = enriched_person.get('enriched_data', {}).get('pdl_data', {})
            original_data = enriched_person.get('enriched_data', {}).get('original_data', {})
            
            # Prepare lead data for Dynamics
            lead_data = {
                'subject': f"Patent Inventor/Assignee - {enriched_person.get('original_name', 'Unknown')}",
                'firstname': pdl_data.get('first_name') or original_data.get('first_name'),
                'lastname': pdl_data.get('last_name') or original_data.get('last_name'),
                'emailaddress1': pdl_data.get('emails', [None])[0] if pdl_data.get('emails') else None,
                'mobilephone': pdl_data.get('phone_numbers', [None])[0] if pdl_data.get('phone_numbers') else None,
                'jobtitle': pdl_data.get('job_title'),
                'companyname': pdl_data.get('job_company_name'),
                'address1_city': pdl_data.get('location_locality') or original_data.get('city'),
                'address1_stateorprovince': pdl_data.get('location_region') or original_data.get('state'),
                'address1_country': pdl_data.get('location_country') or original_data.get('country'),
                'industrycode': self._map_industry_code(pdl_data.get('industry')),
                'leadsourcecode': 100000000,  # Custom source code for patents
                'description': self._build_description(enriched_person),
                # Custom fields for patent data
                'new_patentnumber': enriched_person.get('patent_number'),
                'new_patenttitle': enriched_person.get('patent_title'),
                'new_persontype': enriched_person.get('enriched_data', {}).get('person_type'),
                'new_matchscore': enriched_person.get('match_score', 0.0),
                'new_linkedinurl': pdl_data.get('linkedin_url'),
                'new_enrichmentdate': datetime.now().isoformat()
            }
            
            # Remove None values
            lead_data = {k: v for k, v in lead_data.items() if v is not None}
            
            # Create lead via API
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'OData-MaxVersion': '4.0',
                'OData-Version': '4.0'
            }
            
            response = requests.post(
                f"{self.crm_url}/api/data/v9.2/leads",
                headers=headers,
                json=lead_data
            )
            
            if response.status_code == 204:  # Success - no content returned
                lead_id = response.headers.get('OData-EntityId', '').split('(')[-1].split(')')[0]
                logger.info(f"Successfully created lead: {lead_id}")
                return {
                    'success': True,
                    'lead_id': lead_id,
                    'person_name': enriched_person.get('original_name')
                }
            else:
                logger.error(f"Failed to create lead: {response.status_code} - {response.text}")
                return {
                    'success': False,
                    'error': f"HTTP {response.status_code}: {response.text}",
                    'person_name': enriched_person.get('original_name')
                }
                
        except Exception as e:
            logger.error(f"Error creating lead for {enriched_person.get('original_name')}: {e}")
            return {
                'success': False,
                'error': str(e),
                'person_name': enriched_person.get('original_name')
            }
    
    def _map_industry_code(self, industry: str) -> int:
        """Map industry string to Dynamics industry code"""
        industry_mapping = {
            'technology': 1,
            'software': 1,
            'healthcare': 2,
            'biotechnology': 2,
            'pharmaceutical': 2,
            'manufacturing': 3,
            'automotive': 4,
            'aerospace': 5,
            'telecommunications': 6,
            'finance': 7,
            'energy': 8,
            'consulting': 9,
            'education': 10
        }
        
        if not industry:
            return None
            
        industry_lower = industry.lower()
        for key, value in industry_mapping.items():
            if key in industry_lower:
                return value
        
        return None  # Unknown industry
    
    def _build_description(self, enriched_person: Dict) -> str:
        """Build description field with patent and enrichment info"""
        lines = []
        
        # Patent info
        lines.append(f"Patent: {enriched_person.get('patent_number', 'Unknown')}")
        lines.append(f"Title: {enriched_person.get('patent_title', 'Unknown')}")
        lines.append(f"Role: {enriched_person.get('enriched_data', {}).get('person_type', 'Unknown')}")
        
        # Enrichment info
        match_score = enriched_person.get('match_score', 0.0)
        lines.append(f"Match Score: {match_score:.2f}")
        
        pdl_data = enriched_person.get('enriched_data', {}).get('pdl_data', {})
        if pdl_data.get('job_title'):
            lines.append(f"Current Role: {pdl_data['job_title']}")
        if pdl_data.get('job_company_name'):
            lines.append(f"Current Company: {pdl_data['job_company_name']}")
        
        lines.append(f"Enriched on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return " | ".join(lines)

def run_dynamics_integration(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run the Dynamics CRM integration process
    
    Args:
        config: Dictionary containing configuration parameters
        
    Returns:
        Dictionary containing results and statistics
    """
    try:
        # Initialize integrator
        integrator = DynamicsCRMIntegrator(
            tenant_id=config['DYNAMICS_TENANT_ID'],
            client_id=config['DYNAMICS_CLIENT_ID'],
            client_secret=config['DYNAMICS_CLIENT_SECRET'],
            crm_url=config['DYNAMICS_CRM_URL']
        )
        
        # Authenticate
        if not integrator.authenticate():
            return {
                'success': False,
                'error': "Failed to authenticate with Dynamics CRM",
                'leads_created': 0,
                'leads_failed': 0
            }
        
        # Load enriched data
        enriched_data = config.get('enriched_data', [])
        if not enriched_data:
            return {
                'success': False,
                'error': "No enriched data provided",
                'leads_created': 0,
                'leads_failed': 0
            }
        
        # Create leads
        results = []
        successful_leads = 0
        failed_leads = 0
        
        for person in enriched_data:
            result = integrator.create_lead(person)
            results.append(result)
            
            if result['success']:
                successful_leads += 1
            else:
                failed_leads += 1
            
            # Rate limiting
            time.sleep(0.1)  # Small delay between requests
        
        logger.info(f"Dynamics integration completed. Created: {successful_leads}, Failed: {failed_leads}")
        
        return {
            'success': True,
            'leads_created': successful_leads,
            'leads_failed': failed_leads,
            'total_processed': len(enriched_data),
            'results': results
        }
        
    except Exception as e:
        logger.error(f"Error in Dynamics integration: {e}")
        return {
            'success': False,
            'error': str(e),
            'leads_created': 0,
            'leads_failed': 0
        }