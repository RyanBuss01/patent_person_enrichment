# =============================================================================
# runners/enrich.py - Simplified All-in-One Enrichment
# =============================================================================
import logging
import time
import json
import pandas as pd
import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Optional
from peopledatalabs import PDLPY

logger = logging.getLogger(__name__)

def run_enrichment(config: Dict[str, str]) -> Dict[str, Any]:
    """
    Run the patent enrichment process - ONLY for new people
    Simplified all-in-one function
    
    Args:
        config: Dictionary containing configuration parameters
        
    Returns:
        Dictionary containing results and statistics
    """
    try:
        # Configuration
        enrich_only_new = config.get('ENRICH_ONLY_NEW_PEOPLE', True)
        max_api_calls = config.get('MAX_ENRICHMENT_COST', 1000)
        api_key = config.get('PEOPLEDATALABS_API_KEY')
        
        if api_key == 'YOUR_PDL_API_KEY':
            logger.warning("⚠️ PeopleDataLabs API key not configured - using mock data")
            return _create_mock_enrichment_result()
        
        # Get people to enrich
        people_to_enrich = _get_people_to_enrich(config, enrich_only_new, max_api_calls)
        
        if not people_to_enrich:
            logger.info("No people found to enrich")
            return {
                'success': True,
                'total_patents': 0,
                'total_people': 0,
                'enriched_count': 0,
                'enrichment_rate': 0.0,
                'enriched_data': [],
                'cost_savings': 'No enrichment needed - all people already in database'
            }
        
        logger.info(f"Starting enrichment for {len(people_to_enrich)} people")
        logger.info(f"Estimated API cost: ${len(people_to_enrich) * 0.03:.2f}")
        
        # Enrich with PeopleDataLabs
        enriched_data = _enrich_people_with_pdl(people_to_enrich, api_key)
        
        # Export results
        logger.info("Exporting results...")
        _export_to_csv(enriched_data, config.get('OUTPUT_CSV', 'output/enriched_patents.csv'))
        _export_to_json(enriched_data, config.get('OUTPUT_JSON', 'output/enriched_patents.json'))
        
        # Calculate statistics
        total_people = len(people_to_enrich)
        enrichment_rate = len(enriched_data) / total_people * 100 if total_people > 0 else 0
        
        # Calculate cost savings
        original_people_count = sum(
            len(p.get('inventors', [])) + len(p.get('assignees', [])) 
            for p in config.get('patents_data', [])
        )
        saved_api_calls = max(0, original_people_count - len(people_to_enrich))
        cost_savings = saved_api_calls * 0.03
        
        logger.info("Patent enrichment process completed successfully!")
        logger.info(f"API calls saved: {saved_api_calls} (${cost_savings:.2f})")
        
        return {
            'success': True,
            'total_patents': len(set(p.get('patent_number') for p in people_to_enrich)),
            'total_people': total_people,
            'enriched_count': len(enriched_data),
            'enrichment_rate': enrichment_rate,
            'enriched_data': enriched_data,
            'api_calls_made': len(people_to_enrich),
            'api_calls_saved': saved_api_calls,
            'estimated_cost_savings': f"${cost_savings:.2f}",
            'actual_api_cost': f"${len(people_to_enrich) * 0.03:.2f}"
        }
        
    except Exception as e:
        logger.error(f"Error in enrichment process: {e}")
        return {
            'success': False,
            'error': str(e),
            'total_patents': 0,
            'total_people': 0,
            'enriched_count': 0,
            'enrichment_rate': 0.0
        }

def _get_people_to_enrich(config: Dict[str, Any], enrich_only_new: bool, max_api_calls: int) -> List[Dict]:
    """Get list of people to enrich based on configuration"""
    
    # Get new people data from integration step
    new_people_data = config.get('new_people_data', [])
    
    if enrich_only_new and new_people_data:
        logger.info(f"Enriching ONLY {len(new_people_data)} new people (cost optimization)")
        people_to_enrich = new_people_data[:max_api_calls]
        
        if len(new_people_data) > max_api_calls:
            logger.warning(f"Limiting enrichment to {max_api_calls} people to control costs")
        
        return people_to_enrich
    
    else:
        # Fallback to XML parsing if no new people data provided
        logger.info("No new people data found, falling back to XML parsing")
        xml_file_path = config.get('XML_FILE_PATH')
        
        if not xml_file_path:
            logger.error("No XML file path provided")
            return []
        
        patents = _parse_xml_file(xml_file_path)
        
        if not patents:
            logger.error("No patents found in XML file")
            return []
        
        # Extract all people from patents
        people_to_enrich = []
        for patent in patents:
            # Add inventors
            for inventor in patent.get('inventors', []):
                people_to_enrich.append({
                    **inventor,
                    'patent_number': patent['patent_number'],
                    'patent_title': patent['patent_title'],
                    'person_type': 'inventor'
                })
            
            # Add assignees (individuals only)
            for assignee in patent.get('assignees', []):
                if assignee.get('first_name') or assignee.get('last_name'):
                    people_to_enrich.append({
                        **assignee,
                        'patent_number': patent['patent_number'],
                        'patent_title': patent['patent_title'],
                        'person_type': 'assignee'
                    })
        
        return people_to_enrich[:max_api_calls]

def _parse_xml_file(xml_file_path: str) -> List[Dict]:
    """Parse XML file and extract patent data"""
    try:
        logger.info(f"Parsing XML file: {xml_file_path}")
        
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        
        # Handle single patent or multiple patents
        if root.tag == 'us-patent-grant':
            patents = [root]
        else:
            patents = root.findall('.//us-patent-grant')
        
        extracted_patents = []
        for patent_elem in patents:
            patent_data = _extract_patent_from_xml(patent_elem)
            if patent_data:
                extracted_patents.append(patent_data)
        
        logger.info(f"Extracted {len(extracted_patents)} patents")
        return extracted_patents
        
    except Exception as e:
        logger.error(f"Error parsing XML file: {e}")
        return []

def _extract_patent_from_xml(patent_elem) -> Optional[Dict]:
    """Extract patent data from XML element"""
    try:
        # Basic patent info
        patent_number = _get_xml_text(patent_elem, './/publication-reference/document-id/doc-number')
        patent_title = _get_xml_text(patent_elem, './/invention-title')
        patent_date = _get_xml_text(patent_elem, './/publication-reference/document-id/date')
        
        # Extract inventors
        inventors = []
        inventor_elements = (
            patent_elem.findall('.//parties/applicants/applicant[@app-type="applicant-inventor"]') or
            patent_elem.findall('.//parties/inventors/inventor') or
            patent_elem.findall('.//us-parties/inventors/inventor')
        )
        
        for inventor in inventor_elements:
            inventor_data = {
                'first_name': _get_xml_text(inventor, './/addressbook/first-name') or _get_xml_text(inventor, './/first-name'),
                'last_name': _get_xml_text(inventor, './/addressbook/last-name') or _get_xml_text(inventor, './/last-name'),
                'city': _get_xml_text(inventor, './/addressbook/address/city') or _get_xml_text(inventor, './/address/city'),
                'state': _get_xml_text(inventor, './/addressbook/address/state') or _get_xml_text(inventor, './/address/state'),
                'country': _get_xml_text(inventor, './/addressbook/address/country') or _get_xml_text(inventor, './/address/country'),
            }
            
            if inventor_data['first_name'] or inventor_data['last_name']:
                inventors.append(inventor_data)
        
        # Extract assignees
        assignees = []
        assignee_elements = (
            patent_elem.findall('.//parties/assignees/assignee') or
            patent_elem.findall('.//us-parties/assignees/assignee')
        )
        
        for assignee in assignee_elements:
            assignee_data = {
                'organization': _get_xml_text(assignee, './/addressbook/orgname') or _get_xml_text(assignee, './/orgname'),
                'first_name': _get_xml_text(assignee, './/addressbook/first-name') or _get_xml_text(assignee, './/first-name'),
                'last_name': _get_xml_text(assignee, './/addressbook/last-name') or _get_xml_text(assignee, './/last-name'),
                'city': _get_xml_text(assignee, './/addressbook/address/city') or _get_xml_text(assignee, './/address/city'),
                'state': _get_xml_text(assignee, './/addressbook/address/state') or _get_xml_text(assignee, './/address/state'),
                'country': _get_xml_text(assignee, './/addressbook/address/country') or _get_xml_text(assignee, './/address/country'),
            }
            
            if assignee_data['organization'] or assignee_data['first_name'] or assignee_data['last_name']:
                assignees.append(assignee_data)
        
        return {
            'patent_number': patent_number or "Unknown",
            'patent_title': patent_title or "Unknown",
            'patent_date': patent_date or "Unknown",
            'inventors': inventors,
            'assignees': assignees
        }
        
    except Exception as e:
        logger.warning(f"Error extracting patent data: {e}")
        return None

def _get_xml_text(element, xpath: str) -> Optional[str]:
    """Safely extract text from XML element"""
    if element is None:
        return None
    found = element.find(xpath)
    return found.text.strip() if found is not None and found.text else None

def _enrich_people_with_pdl(people_list: List[Dict], api_key: str) -> List[Dict]:
    """Enrich people using PeopleDataLabs Bulk API - Fixed for None values"""
    logger.info(f"Starting enrichment for {len(people_list)} people using Bulk API")
    
    client = PDLPY(api_key=api_key)
    enriched_results = []
    
    # Process in batches of 100 (PDL bulk API limit)
    batch_size = 100
    for i in range(0, len(people_list), batch_size):
        batch = people_list[i:i + batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}: {len(batch)} people")
        
        # Prepare bulk request
        bulk_requests = []
        for idx, person in enumerate(batch):
            # Prepare search parameters with safe None handling
            params = {}
            first_name = (person.get('first_name') or '').strip()
            last_name = (person.get('last_name') or '').strip()
            city = (person.get('city') or '').strip()
            state = (person.get('state') or '').strip()
            country = (person.get('country') or '').strip()
            
            # Skip if insufficient data
            if not first_name and not last_name:
                logger.debug(f"Skipping person with no name: {person.get('patent_number', 'Unknown')}")
                continue
            
            if first_name:
                params['first_name'] = first_name
            if last_name:
                params['last_name'] = last_name
            
            # Build location
            location_parts = []
            if city:
                location_parts.append(city)
            if state:
                location_parts.append(state)
            if country:
                location_parts.append(country)
            
            if location_parts:
                params['location'] = ', '.join(location_parts)
            
            # Add to bulk request with metadata to track original person
            bulk_requests.append({
                "metadata": {
                    "batch_index": idx,
                    "person_id": person.get('person_id', f"{first_name}_{last_name}"),
                    "patent_number": person.get('patent_number', ''),
                    "person_type": person.get('person_type', '')
                },
                "params": params
            })
        
        if not bulk_requests:
            logger.warning(f"No valid requests in batch {i//batch_size + 1}")
            continue
        
        # Make bulk API call
        try:
            bulk_data = {
                "requests": bulk_requests
            }
            
            response = client.person.bulk(**bulk_data)
            
            if response.status_code == 200:
                responses = response.json()
                
                # Process responses
                for response_item in responses:
                    if response_item.get('status') == 200:
                        # Get original person data from metadata
                        metadata = response_item.get('metadata', {})
                        batch_index = metadata.get('batch_index')
                        
                        if batch_index is not None and batch_index < len(batch):
                            original_person = batch[batch_index]
                            pdl_data = response_item.get('data', {})
                            likelihood = response_item.get('likelihood', 0)
                            
                            # Safe name handling
                            orig_first = (original_person.get('first_name') or '').strip()
                            orig_last = (original_person.get('last_name') or '').strip()
                            original_name = f"{orig_first} {orig_last}".strip()
                            
                            enriched_person = {
                                'original_name': original_name or 'Unknown',
                                'patent_number': original_person.get('patent_number', ''),
                                'patent_title': original_person.get('patent_title', ''),
                                'match_score': likelihood / 10.0,  # Convert PDL likelihood (0-10) to score (0-1)
                                'enriched_data': {
                                    'person_type': original_person.get('person_type', ''),
                                    'original_data': original_person,
                                    'pdl_data': pdl_data,
                                    'api_method': 'bulk_enrichment',
                                    'likelihood': likelihood
                                }
                            }
                            enriched_results.append(enriched_person)
                    else:
                        # Log failed enrichments
                        metadata = response_item.get('metadata', {})
                        person_id = metadata.get('person_id', 'unknown')
                        logger.warning(f"Failed to enrich {person_id}: status {response_item.get('status')}")
            else:
                logger.error(f"Bulk API request failed: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Error in bulk API call for batch {i//batch_size + 1}: {e}")
            continue
        
        # Small delay between batches to be respectful
        if i + batch_size < len(people_list):
            time.sleep(0.5)
    
    logger.info(f"Bulk enrichment completed: {len(enriched_results)} out of {len(people_list)} people enriched")
    return enriched_results
# Removed _enrich_single_person function as we're now using bulk API

def _export_to_csv(enriched_data: List[Dict], filename: str):
    """Export enriched data to CSV - Fixed for None values"""
    if not enriched_data:
        logger.warning("No enriched data to export")
        return
    
    rows = []
    for data in enriched_data:
        pdl_data = data.get('enriched_data', {}).get('pdl_data', {})
        original_data = data.get('enriched_data', {}).get('original_data', {})
        
        # Safe handling of list fields that might be None
        emails = pdl_data.get('emails') or []
        phone_numbers = pdl_data.get('phone_numbers') or []
        
        # Ensure emails and phone_numbers are lists
        if not isinstance(emails, list):
            emails = []
        if not isinstance(phone_numbers, list):
            phone_numbers = []
        
        row = {
            'patent_number': data.get('patent_number'),
            'patent_title': data.get('patent_title'),
            'original_name': data.get('original_name'),
            'person_type': data.get('enriched_data', {}).get('person_type'),
            'match_score': data.get('match_score'),
            'api_method': data.get('enriched_data', {}).get('api_method'),
            
            # Original data
            'original_first_name': original_data.get('first_name'),
            'original_last_name': original_data.get('last_name'),
            'original_city': original_data.get('city'),
            'original_state': original_data.get('state'),
            'original_country': original_data.get('country'),
            
            # Enriched data - with safe list handling
            'enriched_full_name': pdl_data.get('full_name'),
            'enriched_first_name': pdl_data.get('first_name'),
            'enriched_last_name': pdl_data.get('last_name'),
            'enriched_emails': ', '.join(emails),  # Safe join
            'enriched_phone_numbers': ', '.join(phone_numbers),  # Safe join
            'enriched_linkedin_url': pdl_data.get('linkedin_url'),
            'enriched_current_title': pdl_data.get('job_title'),
            'enriched_current_company': pdl_data.get('job_company_name'),
            'enriched_city': pdl_data.get('location_locality'),
            'enriched_state': pdl_data.get('location_region'),
            'enriched_country': pdl_data.get('location_country'),
            'enriched_industry': pdl_data.get('industry'),
        }
        rows.append(row)
    
    df = pd.DataFrame(rows)
    df.to_csv(filename, index=False)
    logger.info(f"Exported {len(rows)} records to {filename}")

def _export_to_json(enriched_data: List[Dict], filename: str):
    """Export enriched data to JSON"""
    if not enriched_data:
        logger.warning("No enriched data to export")
        return
    
    with open(filename, 'w') as f:
        json.dump(enriched_data, f, indent=2, default=str)
    
    logger.info(f"Exported {len(enriched_data)} records to {filename}")

def _create_mock_enrichment_result() -> Dict[str, Any]:
    """Create mock enrichment result when API key is not configured"""
    return {
        'success': True,
        'total_patents': 0,
        'total_people': 0,
        'enriched_count': 0,
        'enrichment_rate': 0.0,
        'enriched_data': [],
        'api_calls_made': 0,
        'api_calls_saved': 0,
        'estimated_cost_savings': '$0.00',
        'actual_api_cost': '$0.00',
        'warning': 'Mock result - PeopleDataLabs API key not configured'
    }