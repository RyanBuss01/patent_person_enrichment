from dataclasses import dataclass
from typing import Dict, List

@dataclass
class PatentData:
    """Data class to store patent information"""
    patent_number: str
    patent_title: str
    patent_date: str
    inventors: List[Dict]
    assignees: List[Dict]

@dataclass
class EnrichedData:
    """Data class to store enriched person information"""
    original_name: str
    patent_number: str
    patent_title: str
    enriched_data: Dict
    match_score: float = 0.0
