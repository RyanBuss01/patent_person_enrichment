# =============================================================================
# runners/extract_patents.py - Fixed for New PatentsView API
# Alternate Step 0 runner: Extract patents via PatentsView (legacy flow)
# Produces the same outputs as the primary Step 0 downloader so downstream
# steps (integration/enrichment) work without changes.
# =============================================================================
import requests
import pandas as pd
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

def extract_patents_api(api_key: str, days_back: int = 7, max_results: int = 1000) -> List[Dict]:
    """Extract patents from USPTO PatentsView API (PatentSearch API).

    Returns the raw API items (with nested inventors/assignees) which will be
    normalized by process_raw_patents().
    """
    
    # Calculate date range
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    logger.info(f"Extracting patents from {start_date} onwards (max {max_results})")
    
    # NEW API setup - Updated endpoint and headers
    # prefer the same base endpoint form used elsewhere
    base_url = "https://search.patentsview.org/api/v1/patent"
    headers = {
        "X-Api-Key": api_key, 
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    # Updated query format for new API
    query = {
        "_gte": {
            "patent_date": start_date
        }
    }
    
    # Updated field names for new API
    fields = [
        "patent_id", 
        "patent_title", 
        "patent_date", 
        "patent_abstract",
        "inventors.inventor_name_first", 
        "inventors.inventor_name_last",
        "inventors.inventor_city", 
        "inventors.inventor_state", 
        "inventors.inventor_country",
        "assignees.assignee_organization", 
        "assignees.assignee_individual_name_first",
        "assignees.assignee_individual_name_last", 
        "assignees.assignee_city",
        "assignees.assignee_state", 
        "assignees.assignee_country"
    ]
    
    # Paginated requests using new API format
    all_patents = []
    offset = 0
    size = 100  # Changed from per_page to size
    
    while len(all_patents) < max_results:
        # Use POST method with JSON body (recommended for complex queries)
        payload = {
            "q": query,
            "f": fields,
            "o": {
                "size": size,
                "offset": offset
            }
        }
        
        try:
            logger.info(f"Making API request with offset {offset}, size {size}")
            response = requests.post(base_url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                # Handle both { data: { patents: [...] } } and { patents: [...] }
                patents = data.get("data", {}).get("patents", []) or data.get("patents", [])
                
                if not patents:
                    logger.info("No more patents found, stopping pagination")
                    break
                    
                all_patents.extend(patents)
                logger.info(f"Retrieved {len(patents)} patents from offset {offset}")
                offset += size
                
                # Rate limiting - be respectful to the API
                time.sleep(0.5)
                
            elif response.status_code == 429:
                logger.warning("Rate limit hit, waiting 60 seconds...")
                time.sleep(60)
                continue
            elif response.status_code == 401:
                logger.error("API Key authentication failed - check your API key")
                raise Exception("Invalid API key")
            elif response.status_code == 400:
                logger.error(f"Bad Request (400): {response.text}")
                raise Exception(f"Bad API request: {response.text}")
            else:
                logger.error(f"API Error: {response.status_code} - {response.text}")
                raise Exception(f"API Error {response.status_code}: {response.text}")
                
        except requests.exceptions.Timeout:
            logger.error("Request timeout - API may be slow")
            break
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            break
    
    logger.info(f"Total patents extracted: {len(all_patents)}")
    return all_patents[:max_results]


def _safe_strip(value) -> str:
    if value is None:
        return ''
    return str(value).strip()


def _process_inventors_nested(raw_inventors: Optional[List[Dict]]) -> List[Dict]:
    processed = []
    for inventor in (raw_inventors or []):
        item = {
            'first_name': _safe_strip(inventor.get('inventor_name_first')),
            'last_name': _safe_strip(inventor.get('inventor_name_last')),
            'city': _safe_strip(inventor.get('inventor_city')),
            'state': _safe_strip(inventor.get('inventor_state')),
            'country': _safe_strip(inventor.get('inventor_country'))
        }
        if item['first_name'] or item['last_name']:
            processed.append(item)
    return processed


def _process_assignees_nested(raw_assignees: Optional[List[Dict]]) -> List[Dict]:
    processed = []
    for assignee in (raw_assignees or []):
        # detect organization vs individual
        org = _safe_strip(assignee.get('assignee_organization'))
        first = _safe_strip(assignee.get('assignee_individual_name_first'))
        last = _safe_strip(assignee.get('assignee_individual_name_last'))
        obj = {
            'organization': org,
            'first_name': first,
            'last_name': last,
            'city': _safe_strip(assignee.get('assignee_city')),
            'state': _safe_strip(assignee.get('assignee_state')),
            'country': _safe_strip(assignee.get('assignee_country')),
            'type': 'organization' if org else 'individual'
        }
        if obj['organization'] or obj['first_name'] or obj['last_name']:
            processed.append(obj)
    return processed


def process_raw_patents(raw_patents: List[Dict]) -> List[Dict]:
    """Normalize raw PatentsView items to our standard structure used by Step 1+.

    Output keys:
    - patent_number, patent_title, patent_date, patent_abstract
    - inventors: [{first_name,last_name,city,state,country}]
    - assignees: [{type,organization,first_name,last_name,city,state,country}]
    """
    processed = []
    for p in raw_patents:
        processed.append({
            'patent_number': _safe_strip(p.get('patent_id')),
            'patent_title': _safe_strip(p.get('patent_title')),
            'patent_date': _safe_strip(p.get('patent_date')),
            'patent_abstract': _safe_strip(p.get('patent_abstract')),
            'inventors': _process_inventors_nested(p.get('inventors')),
            'assignees': _process_assignees_nested(p.get('assignees'))
        })
    return processed

def run_patent_extraction(config: Dict[str, Any]) -> Dict[str, Any]:
    """Run the patent extraction process and write standard Step 0 outputs."""
    try:
        # Check API key
        api_key = config.get('PATENTSVIEW_API_KEY')
        if not api_key or api_key == "YOUR_API_KEY":
            return {
                'success': False, 
                'error': "PatentsView API key not configured. Please set PATENTSVIEW_API_KEY in your .env file",
                'total_patents': 0,
                'help': "Get API key from: https://patentsview.org/apis/keyrequest"
            }
        
        logger.info("Starting patent extraction from PatentsView API...")
        
        # Extract patents
        raw_patents = extract_patents_api(
            api_key=api_key,
            days_back=config.get('DAYS_BACK', 7),
            max_results=config.get('MAX_RESULTS', 1000)
        )
        
        if not raw_patents:
            return {
                'success': False, 
                'error': "No patents extracted - may be due to API issues or no recent patents matching criteria",
                'total_patents': 0,
                'suggestion': "Try increasing DAYS_BACK or check API key validity"
            }
        # Normalize
        patents = process_raw_patents(raw_patents)
        
        # Save results
        output_dir = config.get('OUTPUT_DIR', 'output')
        os.makedirs(output_dir, exist_ok=True)
        # Match Step 0 downloader filenames
        json_file = os.path.join(output_dir, 'downloaded_patents.json')
        csv_file = os.path.join(output_dir, 'downloaded_patents.csv')
        
        # Save JSON
        with open(json_file, 'w') as f:
            json.dump(patents, f, indent=2, default=str)
        
        # Save CSV (flattened similar to main downloader)
        try:
            csv_rows = []
            for patent in patents:
                base = {
                    'patent_number': patent.get('patent_number', ''),
                    'patent_title': patent.get('patent_title', ''),
                    'patent_date': patent.get('patent_date', ''),
                    'patent_abstract': (patent.get('patent_abstract') or '')
                }
                abs_txt = base['patent_abstract']
                if abs_txt and len(abs_txt) > 500:
                    base['patent_abstract'] = abs_txt[:500] + '...'
                if patent.get('inventors'):
                    inv = patent['inventors'][0]
                    base.update({
                        'inventor_first_name': inv.get('first_name', ''),
                        'inventor_last_name': inv.get('last_name', ''),
                        'inventor_city': inv.get('city', ''),
                        'inventor_state': inv.get('state', ''),
                        'inventor_country': inv.get('country', ''),
                    })
                if patent.get('assignees'):
                    ass = patent['assignees'][0]
                    if (ass.get('type') or '') == 'organization':
                        base['assignee_organization'] = ass.get('organization', '')
                    else:
                        base['assignee_first_name'] = ass.get('first_name', '')
                        base['assignee_last_name'] = ass.get('last_name', '')
                    base.update({
                        'assignee_city': ass.get('city', ''),
                        'assignee_state': ass.get('state', ''),
                        'assignee_country': ass.get('country', ''),
                        'assignee_type': ass.get('type', ''),
                    })
                csv_rows.append(base)

            df = pd.DataFrame(csv_rows)
            df.to_csv(csv_file, index=False)
            
        except Exception as csv_error:
            logger.warning(f"Could not create CSV file: {csv_error}")
            # Still continue - JSON file is more important
        # Write a download_results.json summary to align with Step 0
        results = {
            'success': True,
            'mode': 'alternate',
            'patents_downloaded': len(patents),
            'download_parameters': {
                'days_back': config.get('DAYS_BACK', 7),
                'max_results': config.get('MAX_RESULTS', 1000)
            },
            'output_files': {
                'json': json_file,
                'csv': csv_file
            }
        }
        with open(os.path.join(output_dir, 'download_results.json'), 'w') as f:
            json.dump(results, f, indent=2)

        logger.info(f"Saved {len(patents)} patents to {json_file}")

        return {
            'success': True,
            'total_patents': len(patents),
            'patents_data': patents,
            'output_files': [json_file, csv_file],
            'api_info': {
                'endpoint': 'https://search.patentsview.org/api/v1/patent',
                'api_version': 'PatentSearch API v2',
                'date_range': f"Patents from {config.get('DAYS_BACK', 7)} days ago onwards"
            }
        }
        
    except Exception as e:
        logger.error(f"Patent extraction failed: {e}")
        return {
            'success': False, 
            'error': str(e), 
            'total_patents': 0,
            'troubleshooting': {
                'check_api_key': 'Ensure PATENTSVIEW_API_KEY is set correctly',
                'check_internet': 'Verify internet connection',
                'check_rate_limits': 'API allows 45 requests/minute',
                'get_api_key': 'https://patentsview.org/apis/keyrequest'
            }
        }
