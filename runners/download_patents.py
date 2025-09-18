# =============================================================================
# runners/download_patents.py - FIXED VERSION
# Step 0 Runner: Download patents from PatentsView API
# Fixed to use correct API and handle current data limitations
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
    """Fixed API client for PatentsView with current API endpoint and data limitations"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        # FIXED: Use the correct PatentSearch API endpoint
        self.base_url = "https://search.patentsview.org/api/v1/patent"
        self.headers = {
            "X-Api-Key": api_key,
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        self.rate_limit = 45  # requests per minute
        self.last_request_time = 0
        self.request_count = 0
        # FIXED: Data limitation - PatentsView only has data through 2024-12-31
        self.max_date = datetime(2024, 12, 31).date()
        
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
    
    def _validate_date_range(self, start_date: str, end_date: str) -> tuple:
        """Validate and adjust dates to be within available data range"""
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        except ValueError as e:
            raise ValueError(f"Invalid date format. Use YYYY-MM-DD: {e}")
        
        # FIXED: Adjust dates to be within available data range
        original_start = start_dt
        original_end = end_dt
        
        if start_dt > self.max_date:
            logger.warning(f"Start date {start_date} is beyond available data. Using {self.max_date}")
            start_dt = self.max_date
            
        if end_dt > self.max_date:
            logger.warning(f"End date {end_date} is beyond available data. Using {self.max_date}")
            end_dt = self.max_date
        
        # If both dates are in the future, use recent data instead
        if original_start > self.max_date and original_end > self.max_date:
            logger.info("Both dates are in the future. Using recent data from past 30 days instead.")
            end_dt = self.max_date
            start_dt = self.max_date - timedelta(days=30)
        
        return start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d')
    
    def fetch_patents(self, query: Dict, fields: List[str], max_results: int = 1000) -> List[Dict]:
        """CORRECT: Fetch patents using proper PatentsView API cursor-based pagination"""
        all_patents = []
        seen_patent_ids = set()
        # Use larger page size to support high max_results efficiently
        # PatentsView PatentSearch API supports up to 1000 per request
        page_size = min(1000, max_results)
        cursor = None
        page_count = 0
        # Safety limit scaled to requested results (allow a small buffer of +5 pages)
        max_pages = max(100, (max_results + page_size - 1) // page_size + 5)
        
        logger.info(f"Starting patent fetch - query: {query}, max_results: {max_results}")
        
        while len(all_patents) < max_results and page_count < max_pages:
            self._respect_rate_limit()
            page_count += 1
            
            # Build correct parameters according to documentation
            params = {
                "q": json.dumps(query),
                "f": json.dumps(fields),
            }
            
            # Build options object for pagination
            options = {"size": min(page_size, max_results - len(all_patents))}
            
            # Add cursor for pagination (after first request)
            if cursor is not None:
                options["after"] = cursor
            
            params["o"] = json.dumps(options)
            
            # Add sorting for consistent cursor pagination
            sort_spec = [{"patent_id": "asc"}]  # Sort by patent_id for cursor pagination
            params["s"] = json.dumps(sort_spec)
            
            logger.info(f"Page {page_count}: Requesting {options['size']} patents" + 
                    (f" after cursor {cursor}" if cursor else " (first page)"))
            
            try:
                response = requests.get(
                    self.base_url,
                    headers=self.headers,
                    params=params,
                    timeout=30
                )
                
                logger.debug(f"API Response Status: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get("error"):
                        error_msg = data.get("message", "Unknown API error")
                        logger.error(f"API returned error: {error_msg}")
                        break
                    
                    # Extract patents from response
                    patents = data.get("data", {}).get("patents", []) or data.get("patents", [])
                    
                    if not patents:
                        logger.info(f"No more patents available (page {page_count})")
                        break
                    
                    # Process patents and check for duplicates
                    new_patents = []
                    duplicates_count = 0
                    
                    for patent in patents:
                        patent_id = patent.get('patent_id', '')
                        if patent_id and patent_id not in seen_patent_ids:
                            seen_patent_ids.add(patent_id)
                            new_patents.append(patent)
                        elif patent_id:
                            duplicates_count += 1
                    
                    all_patents.extend(new_patents)
                    
                    # Log results
                    total_hits = data.get("total_hits", 0)
                    logger.info(f"Page {page_count}: {len(patents)} received, "
                            f"{len(new_patents)} new, {duplicates_count} duplicates, "
                            f"total unique: {len(all_patents)}, total_hits: {total_hits}")
                    
                    # Set cursor for next page (last patent_id from current page)
                    if patents and len(all_patents) < max_results:
                        # Use the LAST patent's ID as cursor for next page
                        last_patent = patents[-1]
                        cursor = last_patent.get('patent_id')
                        logger.info(f"Next cursor: {cursor}")
                    else:
                        logger.info("No cursor set - stopping pagination")
                        break
                    
                    # Stop if no new patents (API exhausted)
                    if len(new_patents) == 0:
                        logger.info("No new patents received - stopping")
                        break
                        
                elif response.status_code == 400:
                    try:
                        error_data = response.json()
                        error_msg = error_data.get("message", response.text)
                    except:
                        error_msg = response.text
                    
                    logger.error(f"API request failed (400 Bad Request): {error_msg}")
                    logger.error(f"Parameters: {params}")
                    break
                    
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
        
        final_patents = all_patents[:max_results]
        logger.info(f"Patent fetch complete: {len(final_patents)} unique patents retrieved in {page_count} pages")
        
        return final_patents

    def _try_pagination_strategy(self, query: Dict, fields: List[str], max_results: int, 
                            per_page: int, seen_patent_ids: set, strategy_name: str, 
                            sort_param: List[Dict] = None) -> Dict:
        """Try a specific pagination strategy"""
        logger.info(f"Trying pagination strategy: {strategy_name}")
        
        patents = []
        page = 1
        consecutive_duplicate_pages = 0
        max_duplicate_pages = 5  # Allow more duplicate pages before giving up
        # Scale page cap to requested results (with buffer), keep an overall ceiling
        max_pages = min(2000, max(100, (max_results + per_page - 1) // per_page + 5))
        
        while len(patents) < max_results and page <= max_pages and consecutive_duplicate_pages < max_duplicate_pages:
            self._respect_rate_limit()
            
            params = {
                "q": json.dumps(query),
                "f": json.dumps(fields),
                "per_page": per_page,
                "page": page,
            }
            
            if sort_param:
                params["s"] = json.dumps(sort_param)
            
            try:
                response = requests.get(
                    self.base_url,
                    headers=self.headers,
                    params=params,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get("error"):
                        logger.error(f"API error: {data.get('message', 'Unknown error')}")
                        break
                    
                    page_patents = data.get("data", {}).get("patents", []) or data.get("patents", [])
                    
                    if not page_patents:
                        logger.info(f"{strategy_name}: Empty page {page}")
                        consecutive_duplicate_pages += 1
                        page += 1
                        continue
                    
                    # Check for new patents on this page
                    new_patents_on_page = []
                    duplicates_on_page = 0
                    
                    for patent in page_patents:
                        patent_id = patent.get('patent_id', '')
                        if patent_id and patent_id not in seen_patent_ids:
                            new_patents_on_page.append(patent)
                            seen_patent_ids.add(patent_id)
                        elif patent_id:
                            duplicates_on_page += 1
                    
                    patents.extend(new_patents_on_page)
                    
                    logger.info(f"{strategy_name} page {page}: {len(page_patents)} received, "
                            f"{len(new_patents_on_page)} new, {duplicates_on_page} duplicates, "
                            f"total unique: {len(patents)}")
                    
                    # Check total available
                    total_available = data.get("total_hits", 0) or data.get("data", {}).get("total_hits", 0)
                    if total_available > 0:
                        logger.info(f"{strategy_name}: Total available = {total_available}")
                    
                    # Only increment duplicate page counter if we got ALL duplicates
                    if len(new_patents_on_page) == 0 and len(page_patents) > 0:
                        consecutive_duplicate_pages += 1
                        logger.warning(f"{strategy_name}: Page {page} had all duplicates ({consecutive_duplicate_pages}/{max_duplicate_pages})")
                    else:
                        consecutive_duplicate_pages = 0  # Reset counter when we get new patents
                    
                    # Continue to next page
                    page += 1
                    
                else:
                    logger.error(f"{strategy_name}: API error {response.status_code}: {response.text}")
                    break
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"{strategy_name}: Request failed: {e}")
                break
        
        logger.info(f"{strategy_name} strategy complete: {len(patents)} patents")
        return {'patents': patents, 'seen_ids': seen_patent_ids}

    def _try_offset_pagination(self, query: Dict, fields: List[str], max_results: int, 
                            per_page: int, seen_patent_ids: set) -> List[Dict]:
        """Try offset-based pagination as fallback"""
        logger.info("Trying offset-based pagination...")
        
        patents = []
        offset = 0
        max_offset_attempts = 50  # Limit offset attempts
        
        while len(patents) < max_results and offset < max_offset_attempts * per_page:
            self._respect_rate_limit()
            
            params = {
                "q": json.dumps(query),
                "f": json.dumps(fields),
                "per_page": per_page,
                "offset": offset,
            }
            
            try:
                response = requests.get(
                    self.base_url,
                    headers=self.headers,
                    params=params,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    page_patents = data.get("data", {}).get("patents", []) or data.get("patents", [])
                    
                    if not page_patents:
                        logger.info(f"Offset {offset}: No more patents")
                        break
                    
                    new_patents = [p for p in page_patents 
                                if p.get('patent_id') and p.get('patent_id') not in seen_patent_ids]
                    
                    patents.extend(new_patents)
                    logger.info(f"Offset {offset}: {len(page_patents)} received, {len(new_patents)} new")
                    
                    offset += per_page
                else:
                    logger.error(f"Offset pagination failed: {response.status_code}")
                    break
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Offset pagination request failed: {e}")
                break
        
        logger.info(f"Offset pagination complete: {len(patents)} patents")
        return patents

class PatentDownloader:
    """Main patent download orchestrator with fixes for current API"""
    
    def __init__(self, api_key: str):
        self.api_client = PatentsViewAPIClient(api_key)
        # FIXED: Use correct nested field names like the working example
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
        FIXED Smart mode: Searches for patents going back from the latest available data
        Handles the fact that PatentsView data only goes through 2024-12-31
        """
        logger.info(f"Starting smart mode download - {days_back} days back, max {max_results} results")
        
        # FIXED: Start from the latest available data date, not current date
        max_date = self.api_client.max_date
        logger.info(f"Using latest available data date: {max_date}")
        
        all_patents = []
        
        # Strategy 1: Try recent days one by one from the latest available data
        for day_offset in range(days_back):
            if len(all_patents) >= max_results:
                break
                
            target_date = max_date - timedelta(days=day_offset)
            date_str = target_date.strftime('%Y-%m-%d')
            
            logger.info(f"Checking for patents on {date_str}")
            
            # FIXED: Use correct query format for PatentSearch API
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
        if len(all_patents) < min(max_results // 4, 100):  # If we found very few patents
            logger.info("Insufficient patents found in recent days, trying broader range")
            
            end_date = max_date
            start_date = max_date - timedelta(days=days_back * 4)  # Broader range
            
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
            
            # FIXED: Also try a simple query without date restrictions to test the API
        if len(all_patents) < min(max_results // 10, 50):  # If we found very few patents
            logger.info("Still insufficient patents found, trying simple test query...")
            
            # Try a simple query for recent patents without specific date ranges
            test_query = {"_gte": {"patent_date": "2024-01-01"}}  # Just get patents from 2024
            
            test_patents = self.api_client.fetch_patents(
                test_query,
                self.standard_fields,
                max_results
            )
            
            # Use test results if they're better
            if len(test_patents) > len(all_patents):
                logger.info(f"Simple test query yielded more results: {len(test_patents)} vs {len(all_patents)}")
                all_patents = test_patents
        
        logger.info(f"Smart mode complete: {len(all_patents)} patents downloaded")
        return all_patents
    
    def download_manual_mode(self, start_date: str, end_date: str, max_results: int = 1000) -> List[Dict]:
        """
        FIXED Manual mode: Downloads patents from specific date range with validation
        """
        logger.info(f"Starting manual mode download - {start_date} to {end_date}, max {max_results} results")
        
        # FIXED: Validate and adjust date range
        validated_start, validated_end = self.api_client._validate_date_range(start_date, end_date)
        
        if validated_start != start_date or validated_end != end_date:
            logger.info(f"Adjusted date range from {start_date}-{end_date} to {validated_start}-{validated_end}")
        
        query = {
            "_and": [
                {"_gte": {"patent_date": validated_start}},
                {"_lte": {"patent_date": validated_end}}
            ]
        }
        
        patents = self.api_client.fetch_patents(query, self.standard_fields, max_results)
        
        logger.info(f"Manual mode complete: {len(patents)} patents downloaded")
        return patents
    
    def process_raw_patents(self, raw_patents: List[Dict]) -> List[Dict]:
        """Process raw API response into standardized format - handles nested inventor/assignee data"""
        logger.info(f"Processing {len(raw_patents)} raw patents into standard format")
        
        processed_patents = []
        
        for patent in raw_patents:
            # FIXED: Handle None values in patent fields too
            processed_patent = {
                'patent_number': self._safe_strip(patent.get('patent_id')),
                'patent_title': self._safe_strip(patent.get('patent_title')),
                'patent_date': self._safe_strip(patent.get('patent_date')),
                'patent_abstract': self._safe_strip(patent.get('patent_abstract')),
                'inventors': self._process_inventors_nested(patent.get('inventors', [])),
                'assignees': self._process_assignees_nested(patent.get('assignees', []))
            }
            
            processed_patents.append(processed_patent)
        
        logger.info(f"Processing complete: {len(processed_patents)} patents standardized")
        return processed_patents
    
    def _process_inventors_nested(self, raw_inventors: List[Dict]) -> List[Dict]:
        """Process nested inventor data from the API response - handles None values"""
        processed_inventors = []
        
        for inventor in raw_inventors:
            # FIXED: Handle None values properly with safe_strip function
            processed_inventor = {
                'first_name': self._safe_strip(inventor.get('inventor_name_first')),
                'last_name': self._safe_strip(inventor.get('inventor_name_last')),
                'city': self._safe_strip(inventor.get('inventor_city')),
                'state': self._safe_strip(inventor.get('inventor_state')),
                'country': self._safe_strip(inventor.get('inventor_country'))
            }
            
            # Only add if we have at least a name
            if processed_inventor['first_name'] or processed_inventor['last_name']:
                processed_inventors.append(processed_inventor)
        
        return processed_inventors
    
    def _safe_strip(self, value):
        """Safely strip a value that might be None"""
        if value is None:
            return ''
        return str(value).strip()
    
    def _process_assignees_nested(self, raw_assignees: List[Dict]) -> List[Dict]:
        """Process nested assignee data from the API response - handles None values"""
        processed_assignees = []
        
        for assignee in raw_assignees:
            assignee_type = self._safe_strip(assignee.get('assignee_type')).lower()
            
            if 'company' in assignee_type or 'organization' in assignee_type or assignee.get('assignee_organization'):
                # Organization assignee
                processed_assignee = {
                    'organization': self._safe_strip(assignee.get('assignee_organization')),
                    'city': self._safe_strip(assignee.get('assignee_city')),
                    'state': self._safe_strip(assignee.get('assignee_state')),
                    'country': self._safe_strip(assignee.get('assignee_country')),
                    'type': 'organization'
                }
                
                # Only add if we have an organization name
                if processed_assignee['organization']:
                    processed_assignees.append(processed_assignee)
                    
            else:
                # Individual assignee
                processed_assignee = {
                    'first_name': self._safe_strip(assignee.get('assignee_individual_name_first')),
                    'last_name': self._safe_strip(assignee.get('assignee_individual_name_last')),
                    'city': self._safe_strip(assignee.get('assignee_city')),
                    'state': self._safe_strip(assignee.get('assignee_state')),
                    'country': self._safe_strip(assignee.get('assignee_country')),
                    'type': 'individual'
                }
                
                # Only add if we have at least a name
                if processed_assignee['first_name'] or processed_assignee['last_name']:
                    processed_assignees.append(processed_assignee)
        
        return processed_assignees

def run_patent_download(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    FIXED Main runner function for patent download
    
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
        
        # FIXED: Show data limitation warning
        logger.warning(f"Note: PatentsView data is currently available through {downloader.api_client.max_date}")
        
        # Download patents based on mode
        start_time = time.time()
        
        if mode == 'smart':
            days_back = config.get('days_back', 7)
            logger.info(f"Using smart mode: {days_back} days back from latest data")
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
                'message': f'No patents found for the specified criteria. Note: Data only available through {downloader.api_client.max_date}',
                'download_time_minutes': download_time / 60,
                'mode': mode,
                'data_limitation': f'PatentsView data only available through {downloader.api_client.max_date}'
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
        
        # Also persist to SQL for downstream hydration (best-effort)
        try:
            _save_download_to_sql(processed_patents)
        except Exception as e:
            logger.warning(f"Could not save downloaded patents to SQL: {e}")
        
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
            'data_limitation': f'PatentsView data available through {downloader.api_client.max_date}',
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


def _save_download_to_sql(patents: List[Dict[str, Any]]):
    """Save downloaded patents and their inventors into SQL tables.
    Creates tables if missing: downloaded_patents, downloaded_people.
    """
    if not patents:
        return
    try:
        from database.db_manager import DatabaseManager, DatabaseConfig
    except Exception as e:
        raise RuntimeError(f"DB modules unavailable: {e}")

    db = DatabaseManager(DatabaseConfig.from_env())

    # Create tables if not exist (DDL generally autocommits in MySQL)
    try:
        db.execute_query(
            "CREATE TABLE IF NOT EXISTS downloaded_patents ("
            " id BIGINT PRIMARY KEY AUTO_INCREMENT,"
            " patent_number VARCHAR(50) NOT NULL UNIQUE,"
            " patent_title TEXT,"
            " patent_date DATE,"
            " patent_abstract TEXT,"
            " raw_data JSON,"
            " processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            " INDEX idx_patent_number (patent_number),"
            " INDEX idx_patent_date (patent_date)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        )
        db.execute_query(
            "CREATE TABLE IF NOT EXISTS downloaded_people ("
            " id BIGINT PRIMARY KEY AUTO_INCREMENT,"
            " patent_number VARCHAR(50),"
            " first_name VARCHAR(100),"
            " last_name VARCHAR(100),"
            " city VARCHAR(100),"
            " state VARCHAR(50),"
            " country VARCHAR(100),"
            " INDEX idx_patent (patent_number),"
            " INDEX idx_name (first_name,last_name),"
            " INDEX idx_loc (city,state)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        )
    except Exception:
        pass

    # Prepare batched inserts
    patent_rows = []
    people_rows = []
    for p in patents:
        pn = (p.get('patent_number') or '').strip()
        if not pn:
            continue
        patent_rows.append({
            'patent_number': pn,
            'patent_title': p.get('patent_title') or '',
            'patent_date': p.get('patent_date') or None,
            'patent_abstract': p.get('patent_abstract') or '',
            'raw_data': p,
        })
        for inv in (p.get('inventors') or []):
            people_rows.append({
                'patent_number': pn,
                'first_name': (inv.get('first_name') or '')[:100],
                'last_name': (inv.get('last_name') or '')[:100],
                'city': (inv.get('city') or '')[:100],
                'state': (inv.get('state') or '')[:50],
                'country': (inv.get('country') or '')[:100],
            })

    # Insert patents (ignore duplicates)
    if patent_rows:
        cols = ['patent_number','patent_title','patent_date','patent_abstract','raw_data']
        placeholders = ', '.join(['%s'] * len(cols))
        sql = f"INSERT IGNORE INTO downloaded_patents ({', '.join(cols)}) VALUES ({placeholders})"
        params = [tuple(r.get(c) for c in cols) for r in patent_rows]
        db.execute_many(sql, params)

    # Insert people (no unique constraint)
    if people_rows:
        cols2 = ['patent_number','first_name','last_name','city','state','country']
        placeholders2 = ', '.join(['%s'] * len(cols2))
        sql2 = f"INSERT INTO downloaded_people ({', '.join(cols2)}) VALUES ({placeholders2})"
        # Chunk to avoid packet size issues
        step = 1000
        for i in range(0, len(people_rows), step):
            batch = people_rows[i:i+step]
            params2 = [tuple(r.get(c) for c in cols2) for r in batch]
            db.execute_many(sql2, params2)
