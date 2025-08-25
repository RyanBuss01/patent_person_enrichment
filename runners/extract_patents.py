# =============================================================================
# runners/extract_patents.py - Fixed for New PatentsView API
# Step 2: Data Extraction from the US Patent Office
# =============================================================================
import requests
import pandas as pd
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

def extract_patents_api(api_key: str, days_back: int = 7, max_results: int = 1000) -> List[Dict]:
    """Extract patents from USPTO PatentsView API (New PatentSearch API)"""
    
    # Calculate date range
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    logger.info(f"Extracting patents from {start_date} onwards (max {max_results})")
    
    # NEW API setup - Updated endpoint and headers
    base_url = "https://search.patentsview.org/api/v1/patent/"
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
                patents = data.get("patents", [])
                
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

def run_patent_extraction(config: Dict[str, Any]) -> Dict[str, Any]:
    """Run the patent extraction process"""
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
        patents = extract_patents_api(
            api_key=api_key,
            days_back=config.get('DAYS_BACK', 7),
            max_results=config.get('MAX_RESULTS', 1000)
        )
        
        if not patents:
            return {
                'success': False, 
                'error': "No patents extracted - may be due to API issues or no recent patents matching criteria",
                'total_patents': 0,
                'suggestion': "Try increasing DAYS_BACK or check API key validity"
            }
        
        # Save results
        output_dir = config.get('OUTPUT_DIR', 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        json_file = os.path.join(output_dir, 'raw_patents.json')
        csv_file = os.path.join(output_dir, 'raw_patents.csv')
        
        # Save JSON
        with open(json_file, 'w') as f:
            json.dump(patents, f, indent=2, default=str)
        
        # Save CSV with flattened structure
        try:
            # Flatten nested structures for CSV
            flattened_patents = []
            for patent in patents:
                flat_patent = patent.copy()
                
                # Flatten inventors
                if 'inventors' in patent and patent['inventors']:
                    for i, inventor in enumerate(patent['inventors']):
                        prefix = f"inventor_{i}_" if i > 0 else "inventor_"
                        for key, value in inventor.items():
                            flat_patent[f"{prefix}{key}"] = value
                
                # Flatten assignees
                if 'assignees' in patent and patent['assignees']:
                    for i, assignee in enumerate(patent['assignees']):
                        prefix = f"assignee_{i}_" if i > 0 else "assignee_"
                        for key, value in assignee.items():
                            flat_patent[f"{prefix}{key}"] = value
                
                # Remove nested structures
                flat_patent.pop('inventors', None)
                flat_patent.pop('assignees', None)
                
                flattened_patents.append(flat_patent)
            
            df = pd.DataFrame(flattened_patents)
            df.to_csv(csv_file, index=False)
            
        except Exception as csv_error:
            logger.warning(f"Could not create CSV file: {csv_error}")
            # Still continue - JSON file is more important
        
        logger.info(f"Saved {len(patents)} patents to {json_file}")
        
        return {
            'success': True,
            'total_patents': len(patents),
            'patents_data': patents,
            'output_files': [json_file, csv_file],
            'api_info': {
                'endpoint': 'https://search.patentsview.org/api/v1/patent/',
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