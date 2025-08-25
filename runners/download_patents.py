# =============================================================================
# runners/download_patents.py
# Step 0 Runner: Download patents from PatentsView API
# Core business logic for patent downloading with smart and manual modes
# =============================================================================
import requests
import time
import logging
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class PatentsViewAPIClient:
    """Enhanced API client for PatentsView with rate limiting and error handling"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://search.patentsview.org/api/v1/patent/"
        self.headers = {
            "X-Api-Key": api_key,
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        self.rate_limit = 45  # requests per minute
        self.last_request_time = 0
        self.request_count = 0
        
    def _respect_rate_limit(self):
        """Ensure we don't exceed API rate limits (45 requests/minute)"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        min_interval = 60.0 / self.rate_limit  # 1.33 seconds between requests
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
        self.request_count += 1
    
    def fetch_patents(self, query: Dict, fields: List[str], max_results: int = 1000) -> List[Dict]:
        """Fetch patents with pagination and rate limiting"""
        all_patents = []
        page = 1
        per_page = min(100, max_results)  # API supports up to 1000, but 100 gives better progress tracking
        
        logger.info(f"Starting patent fetch - query: {query}, max_results: {max_results}")
        
        while len(all_patents) < max_results:
            self._respect_rate_limit()
            
            logger.info(f"Fetching page {page}, current total: {len(all_patents)}")
            
            # Prepare request payload
            request_payload = {
                "q": query,
                "f": fields,
                "o": {
                    "size": per_page,
                    "offset": (page - 1) * per_page
                },
                "s": [{"patent_date": "desc"}]  # Most recent first
            }
            
            try:
                response = requests.post(
                    self.base_url,
                    headers=self.headers,
                    json=request_payload,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    patents = data.get("patents", [])
                    
                    if not patents:
                        logger.info("No more patents available from API")
                        break
                    
                    all_patents.extend(patents)
                    logger.info(f"Retrieved {len(patents)} patents from page {page}")
                    
                    # Check if we have all available patents
                    total_available = data.get("total_hits", 0)
                    logger.info(f"Total available patents: {total_available}")
                    
                    if len(all_patents) >= total_available:
                        logger.info(f"Retrieved all {total_available} available patents")
                        break
                    
                    page += 1
                    
                elif response.status_code == 429:
                    logger.warning("Rate limit exceeded, waiting 60 seconds...")
                    time.sleep(60)
                    continue
                    
                else:
                    logger.error(f"API request failed: {response.status_code} - {response.text}")
                    break
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                break
        
        final_patents = all_patents[:max_results]  # Ensure we don't exceed max_results
        logger.info(f"Patent fetch complete: {len(final_patents)} patents retrieved")
        return final_patents

class PatentDownloader:
    """Main patent download orchestrator"""
    
    def __init__(self, api_key: str):
        self.api_client = PatentsViewAPIClient(api_key)
        self.standard_fields = [
            "patent_id",
            "patent_title", 
            "patent_date",
            "patent_abstract",
            "inventors.inventor_name_first",
            "inventors.inventor_name_last",
            "inventors.inventor_city",
            "inventors.inventor_state", 
            "inventors.inventor_country",
            "assignees.assignee_individual_name_first",
            "assignees.assignee_individual_name_last",
            "assignees.assignee_organization",
            "assignees.assignee_city",
            "assignees.assignee_state",
            "assignees.assignee_country",
            "assignees.assignee_type"
        ]
    
    def download_smart_mode(self, days_back: int = 7, max_results: int = 1000) -> List[Dict]:
        """
        Smart mode: Intelligently searches for patents going back day by day
        Handles the fact that patents aren't issued every day
        """
        logger.info(f"Starting smart mode download - {days_back} days back, max {max_results} results")
        
        all_patents = []
        current_date = datetime.now().date()
        
        # Strategy 1: Try recent days one by one
        for day_offset in range(days_back):
            if len(all_patents) >= max_results:
                break
                
            target_date = current_date - timedelta(days=day_offset)
            date_str = target_date.strftime('%Y-%m-%d')
            
            logger.info(f"Checking for patents on {date_str}")
            
            # Query for patents on this specific date
            query = {
                "_and": [
                    {"_gte": {"patent_date": date_str}},
                    {"_lt": {"patent_date": (target_date + timedelta(days=1)).strftime('%Y-%m-%d')}}
                ]
            }
            
            day_patents = self.api_client.fetch_patents(
                query, 
                self.standard_fields, 
                max_results - len(all_patents)
            )
            
            if day_patents:
                all_patents.extend(day_patents)
                logger.info(f"Found {len(day_patents)} patents on {date_str}. Total: {len(all_patents)}")
                
                # If we have enough patents, we can stop
                if len(all_patents) >= max_results:
                    break
        
        # Strategy 2: If we didn't find enough patents, try a broader range
        if len(all_patents) < min(max_results, 100):  # If we found very few patents
            logger.info("Insufficient patents found in recent days, trying broader range")
            
            end_date = current_date
            start_date = current_date - timedelta(days=days_back * 2)  # Double the range
            
            broader_query = {
                "_and": [
                    {"_gte": {"patent_date": start_date.strftime('%Y-%m-%d')}},
                    {"_lte": {"patent_date": end_date.strftime('%Y-%m-%d')}}
                ]
            }
            
            broader_patents = self.api_client.fetch_patents(
                broader_query,
                self.standard_fields,
                max_results
            )
            
            # Use broader results if they're better
            if len(broader_patents) > len(all_patents):
                logger.info(f"Broader search yielded more results: {len(broader_patents)} vs {len(all_patents)}")
                all_patents = broader_patents
        
        logger.info(f"Smart mode complete: {len(all_patents)} patents downloaded")
        return all_patents
    
    def download_manual_mode(self, start_date: str, end_date: str, max_results: int = 1000) -> List[Dict]:
        """
        Manual mode: Downloads patents from specific date range
        """
        logger.info(f"Starting manual mode download - {start_date} to {end_date}, max {max_results} results")
        
        query = {
            "_and": [
                {"_gte": {"patent_date": start_date}},
                {"_lte": {"patent_date": end_date}}
            ]
        }
        
        patents = self.api_client.fetch_patents(query, self.standard_fields, max_results)
        
        logger.info(f"Manual mode complete: {len(patents)} patents downloaded")
        return patents
    
    def process_raw_patents(self, raw_patents: List[Dict]) -> List[Dict]:
        """Process raw API response into standardized format"""
        logger.info(f"Processing {len(raw_patents)} raw patents into standard format")
        
        processed_patents = []
        
        for patent in raw_patents:
            processed_patent = {
                'patent_number': patent.get('patent_id', ''),
                'patent_title': patent.get('patent_title', ''),
                'patent_date': patent.get('patent_date', ''),
                'patent_abstract': patent.get('patent_abstract', ''),
                'inventors': self._process_inventors(patent.get('inventors', [])),
                'assignees': self._process_assignees(patent.get('assignees', []))
            }
            
            processed_patents.append(processed_patent)
        
        logger.info(f"Processing complete: {len(processed_patents)} patents standardized")
        return processed_patents
    
    def _process_inventors(self, raw_inventors: List[Dict]) -> List[Dict]:
        """Process inventor data into standard format"""
        processed_inventors = []
        
        for inventor in raw_inventors:
            processed_inventor = {
                'first_name': inventor.get('inventor_name_first', '').strip(),
                'last_name': inventor.get('inventor_name_last', '').strip(),
                'city': inventor.get('inventor_city', '').strip(),
                'state': inventor.get('inventor_state', '').strip(),
                'country': inventor.get('inventor_country', '').strip()
            }
            
            # Only add if we have at least a name
            if processed_inventor['first_name'] or processed_inventor['last_name']:
                processed_inventors.append(processed_inventor)
        
        return processed_inventors
    
    def _process_assignees(self, raw_assignees: List[Dict]) -> List[Dict]:
        """Process assignee data into standard format"""
        processed_assignees = []
        
        for assignee in raw_assignees:
            assignee_type = assignee.get('assignee_type', '').lower()
            
            if 'company' in assignee_type or 'organization' in assignee_type:
                # Company/Organization assignee
                processed_assignee = {
                    'organization': assignee.get('assignee_organization', '').strip(),
                    'city': assignee.get('assignee_city', '').strip(),
                    'state': assignee.get('assignee_state', '').strip(),
                    'country': assignee.get('assignee_country', '').strip(),
                    'type': 'organization'
                }
                
                # Only add if we have an organization name
                if processed_assignee['organization']:
                    processed_assignees.append(processed_assignee)
                    
            else:
                # Individual assignee
                processed_assignee = {
                    'first_name': assignee.get('assignee_individual_name_first', '').strip(),
                    'last_name': assignee.get('assignee_individual_name_last', '').strip(),
                    'city': assignee.get('assignee_city', '').strip(),
                    'state': assignee.get('assignee_state', '').strip(),
                    'country': assignee.get('assignee_country', '').strip(),
                    'type': 'individual'
                }
                
                # Only add if we have at least a name
                if processed_assignee['first_name'] or processed_assignee['last_name']:
                    processed_assignees.append(processed_assignee)
        
        return processed_assignees

def run_patent_download(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main runner function for patent download
    
    Args:
        config: Configuration dictionary with download parameters
        
    Returns:
        Dictionary containing results and statistics
    """
    try:
        # Extract configuration
        mode = config.get('mode', 'smart')
        api_key = config.get('PATENTSVIEW_API_KEY')
        max_results = config.get('max_results', 1000)
        output_dir = Path(config.get('OUTPUT_DIR', 'output'))
        
        # Validate API key
        if not api_key or api_key == 'YOUR_API_KEY':
            return {
                'success': False,
                'error': 'Valid PatentsView API key required. Please set PATENTSVIEW_API_KEY.',
                'patents_downloaded': 0
            }
        
        # Create output directory
        output_dir.mkdir(exist_ok=True)
        
        # Initialize downloader
        downloader = PatentDownloader(api_key)
        
        # Download patents based on mode
        start_time = time.time()
        
        if mode == 'smart':
            days_back = config.get('days_back', 7)
            logger.info(f"Using smart mode: {days_back} days back")
            raw_patents = downloader.download_smart_mode(days_back, max_results)
            
        elif mode == 'manual':
            start_date = config.get('start_date')
            end_date = config.get('end_date')
            
            if not start_date or not end_date:
                return {
                    'success': False,
                    'error': 'Manual mode requires start_date and end_date',
                    'patents_downloaded': 0
                }
            
            logger.info(f"Using manual mode: {start_date} to {end_date}")
            raw_patents = downloader.download_manual_mode(start_date, end_date, max_results)
            
        else:
            return {
                'success': False,
                'error': f'Invalid mode: {mode}. Use "smart" or "manual"',
                'patents_downloaded': 0
            }
        
        download_time = time.time() - start_time
        
        if not raw_patents:
            logger.warning("No patents downloaded")
            return {
                'success': True,
                'patents_downloaded': 0,
                'message': 'No patents found for the specified criteria',
                'download_time_minutes': download_time / 60,
                'mode': mode
            }
        
        # Process patents into standard format
        processed_patents = downloader.process_raw_patents(raw_patents)
        
        # Save results
        logger.info("Saving download results to files")
        
        # Save JSON
        patents_json_file = output_dir / 'downloaded_patents.json'
        with open(patents_json_file, 'w', encoding='utf-8') as f:
            json.dump(processed_patents, f, indent=2, ensure_ascii=False, default=str)
        
        # Save CSV (flattened format)
        try:
            csv_data = []
            for patent in processed_patents:
                base_row = {
                    'patent_number': patent['patent_number'],
                    'patent_title': patent['patent_title'],
                    'patent_date': patent['patent_date'],
                    'patent_abstract': patent.get('patent_abstract', '')[:500] + '...' if len(patent.get('patent_abstract', '')) > 500 else patent.get('patent_abstract', '')  # Truncate for CSV
                }
                
                # Add first inventor info
                if patent['inventors']:
                    inv = patent['inventors'][0]
                    base_row.update({
                        'inventor_first_name': inv['first_name'],
                        'inventor_last_name': inv['last_name'],
                        'inventor_city': inv['city'],
                        'inventor_state': inv['state'],
                        'inventor_country': inv['country']
                    })
                
                # Add first assignee info
                if patent['assignees']:
                    ass = patent['assignees'][0]
                    if ass.get('type') == 'organization':
                        base_row['assignee_organization'] = ass.get('organization', '')
                    else:
                        base_row['assignee_first_name'] = ass.get('first_name', '')
                        base_row['assignee_last_name'] = ass.get('last_name', '')
                    
                    base_row.update({
                        'assignee_city': ass.get('city', ''),
                        'assignee_state': ass.get('state', ''),
                        'assignee_country': ass.get('country', ''),
                        'assignee_type': ass.get('type', '')
                    })
                
                csv_data.append(base_row)
            
            df = pd.DataFrame(csv_data)
            patents_csv_file = output_dir / 'downloaded_patents.csv'
            df.to_csv(patents_csv_file, index=False, encoding='utf-8')
            
        except Exception as e:
            logger.warning(f"Could not create CSV file: {e}")
        
        # Create summary results
        results = {
            'success': True,
            'mode': mode,
            'patents_downloaded': len(processed_patents),
            'download_parameters': {
                'mode': mode,
                'max_results': max_results,
                'days_back': config.get('days_back') if mode == 'smart' else None,
                'start_date': config.get('start_date') if mode == 'manual' else None,
                'end_date': config.get('end_date') if mode == 'manual' else None
            },
            'download_time_minutes': download_time / 60,
            'api_requests_made': downloader.api_client.request_count,
            'output_files': {
                'json': str(patents_json_file),
                'csv': str(patents_csv_file) if 'patents_csv_file' in locals() else None
            }
        }
        
        # Save results summary
        results_file = output_dir / 'download_results.json'
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Download complete: {len(processed_patents)} patents saved")
        return results
        
    except Exception as e:
        logger.error(f"Error in patent download: {e}")
        return {
            'success': False,
            'error': str(e),
            'patents_downloaded': 0
        }