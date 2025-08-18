import xml.etree.ElementTree as ET
import logging
from typing import List, Optional, Dict
from .data_models import PatentData

logger = logging.getLogger(__name__)

class PatentXMLParser:
    """Parse USPTO XML patent files and extract inventor/assignee data"""
    
    def __init__(self, xml_file_path: str):
        self.xml_file_path = xml_file_path
        self.patents = []
    
    def parse_xml_file(self) -> List[PatentData]:
        """Parse the XML file and extract patent data"""
        logger.info(f"Parsing XML file: {self.xml_file_path}")
        
        try:
            tree = ET.parse(self.xml_file_path)
            root = tree.getroot()
            
            # Handle single patent or multiple patents
            if root.tag == 'us-patent-grant':
                patents = [root]
            else:
                patents = root.findall('.//us-patent-grant')
            
            for patent_elem in patents:
                patent_data = self._extract_patent_data(patent_elem)
                if patent_data:
                    self.patents.append(patent_data)
                    
            logger.info(f"Extracted {len(self.patents)} patents")
            return self.patents
            
        except Exception as e:
            logger.error(f"Error parsing XML file: {e}")
            return []
    
    def _extract_patent_data(self, patent_elem) -> Optional[PatentData]:
        """Extract data from a single patent element"""
        try:
            # Extract basic patent info
            patent_number = self._get_text(patent_elem, './/publication-reference/document-id/doc-number')
            patent_title = self._get_text(patent_elem, './/invention-title')
            patent_date = self._get_text(patent_elem, './/publication-reference/document-id/date')
            
            # Extract inventors
            inventors = self._extract_inventors(patent_elem)
            
            # Extract assignees
            assignees = self._extract_assignees(patent_elem)
            
            return PatentData(
                patent_number=patent_number or "Unknown",
                patent_title=patent_title or "Unknown",
                patent_date=patent_date or "Unknown",
                inventors=inventors,
                assignees=assignees
            )
            
        except Exception as e:
            logger.warning(f"Error extracting patent data: {e}")
            return None
    
    def _extract_inventors(self, patent_elem) -> List[Dict]:
        """Extract inventor information"""
        inventors = []
        
        # Look for inventors in different possible locations
        inventor_elements = (
            patent_elem.findall('.//parties/applicants/applicant[@app-type="applicant-inventor"]') or
            patent_elem.findall('.//parties/inventors/inventor') or
            patent_elem.findall('.//us-parties/inventors/inventor')
        )
        
        for inventor in inventor_elements:
            inventor_data = {
                'first_name': self._get_text(inventor, './/addressbook/first-name') or self._get_text(inventor, './/first-name'),
                'last_name': self._get_text(inventor, './/addressbook/last-name') or self._get_text(inventor, './/last-name'),
                'address': self._extract_address(inventor),
                'city': self._get_text(inventor, './/addressbook/address/city') or self._get_text(inventor, './/address/city'),
                'state': self._get_text(inventor, './/addressbook/address/state') or self._get_text(inventor, './/address/state'),
                'country': self._get_text(inventor, './/addressbook/address/country') or self._get_text(inventor, './/address/country'),
                'postal_code': self._get_text(inventor, './/addressbook/address/postcode') or self._get_text(inventor, './/address/postcode')
            }
            
            # Only add if we have at least a name
            if inventor_data['first_name'] or inventor_data['last_name']:
                inventors.append(inventor_data)
        
        return inventors
    
    def _extract_assignees(self, patent_elem) -> List[Dict]:
        """Extract assignee information"""
        assignees = []
        
        assignee_elements = (
            patent_elem.findall('.//parties/assignees/assignee') or
            patent_elem.findall('.//us-parties/assignees/assignee')
        )
        
        for assignee in assignee_elements:
            assignee_data = {
                'organization': self._get_text(assignee, './/addressbook/orgname') or self._get_text(assignee, './/orgname'),
                'first_name': self._get_text(assignee, './/addressbook/first-name') or self._get_text(assignee, './/first-name'),
                'last_name': self._get_text(assignee, './/addressbook/last-name') or self._get_text(assignee, './/last-name'),
                'address': self._extract_address(assignee),
                'city': self._get_text(assignee, './/addressbook/address/city') or self._get_text(assignee, './/address/city'),
                'state': self._get_text(assignee, './/addressbook/address/state') or self._get_text(assignee, './/address/state'),
                'country': self._get_text(assignee, './/addressbook/address/country') or self._get_text(assignee, './/address/country'),
                'postal_code': self._get_text(assignee, './/addressbook/address/postcode') or self._get_text(assignee, './/address/postcode')
            }
            
            # Only add if we have organization name or person name
            if assignee_data['organization'] or assignee_data['first_name'] or assignee_data['last_name']:
                assignees.append(assignee_data)
        
        return assignees
    
    def _extract_address(self, element) -> str:
        """Extract full address as a single string"""
        address_parts = []
        
        # Try different address element paths
        address_elem = (
            element.find('.//addressbook/address') or
            element.find('.//address')
        )
        
        if address_elem is not None:
            for addr_line in address_elem.findall('address-1'):
                if addr_line.text:
                    address_parts.append(addr_line.text.strip())
            for addr_line in address_elem.findall('address-2'):
                if addr_line.text:
                    address_parts.append(addr_line.text.strip())
        
        return ', '.join(address_parts) if address_parts else ""
    
    def _get_text(self, element, xpath: str) -> Optional[str]:
        """Safely extract text from XML element"""
        if element is None:
            return None
        found = element.find(xpath)
        return found.text.strip() if found is not None and found.text else None
