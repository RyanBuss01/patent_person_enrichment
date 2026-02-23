from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class TrademarkData:
    """Data class to store trademark contact information"""
    trademark_number: str
    contact_name: str
    address_1: str
    address_2: str
    city: str
    state: str
    zip_code: str
    country: str


@dataclass
class EnrichedCompanyData:
    """Data class to store enriched company information"""
    original_name: str
    trademark_number: str
    enriched_data: Dict
    match_score: float = 0.0
