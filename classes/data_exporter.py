# =============================================================================
# classes/data_exporter.py
# =============================================================================
import json
import pandas as pd
import logging
from typing import List
from .data_models import EnrichedData

logger = logging.getLogger(__name__)

class DataExporter:
    """Export enriched data to various formats"""
    
    @staticmethod
    def to_csv(enriched_data: List[EnrichedData], filename: str):
        """Export enriched data to CSV"""
        rows = []
        
        for data in enriched_data:
            pdl_data = data.enriched_data.get('pdl_data', {})
            original_data = data.enriched_data.get('original_data', {})
            
            row = {
                'patent_number': data.patent_number,
                'patent_title': data.patent_title,
                'original_name': data.original_name,
                'person_type': data.enriched_data.get('person_type'),
                'match_score': data.match_score,
                'api_method': data.enriched_data.get('api_method'),
                
                # Original data
                'original_first_name': original_data.get('first_name'),
                'original_last_name': original_data.get('last_name'),
                'original_city': original_data.get('city'),
                'original_state': original_data.get('state'),
                'original_country': original_data.get('country'),
                'original_address': original_data.get('address'),
                
                # Enriched data
                'enriched_full_name': pdl_data.get('full_name'),
                'enriched_first_name': pdl_data.get('first_name'),
                'enriched_last_name': pdl_data.get('last_name'),
                'enriched_emails': ', '.join(pdl_data.get('emails', [])),
                'enriched_phone_numbers': ', '.join(pdl_data.get('phone_numbers', [])),
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
        # Write UTF-8 to ensure extended characters work on Windows
        df.to_csv(filename, index=False, encoding='utf-8')
        logger.info(f"Exported {len(rows)} records to {filename}")
    
    @staticmethod
    def to_json(enriched_data: List[EnrichedData], filename: str):
        """Export enriched data to JSON"""
        data_list = []
        
        for data in enriched_data:
            data_dict = {
                'patent_number': data.patent_number,
                'patent_title': data.patent_title,
                'original_name': data.original_name,
                'match_score': data.match_score,
                'enriched_data': data.enriched_data
            }
            data_list.append(data_dict)
        
        with open(filename, 'w') as f:
            json.dump(data_list, f, indent=2, default=str)
        
        logger.info(f"Exported {len(data_list)} records to {filename}")
