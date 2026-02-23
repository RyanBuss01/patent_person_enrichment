import xml.etree.ElementTree as ET
import logging
import os
from typing import List, Optional, Dict, Set

logger = logging.getLogger(__name__)

# Valid US state names and abbreviations for filtering
US_STATE_ABBREVS = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC', 'PR', 'VI', 'GU', 'AS', 'MP'
}

US_STATE_NAMES = {
    'ALABAMA', 'ALASKA', 'ARIZONA', 'ARKANSAS', 'CALIFORNIA', 'COLORADO',
    'CONNECTICUT', 'DELAWARE', 'FLORIDA', 'GEORGIA', 'HAWAII', 'IDAHO',
    'ILLINOIS', 'INDIANA', 'IOWA', 'KANSAS', 'KENTUCKY', 'LOUISIANA',
    'MAINE', 'MARYLAND', 'MASSACHUSETTS', 'MICHIGAN', 'MINNESOTA',
    'MISSISSIPPI', 'MISSOURI', 'MONTANA', 'NEBRASKA', 'NEVADA',
    'NEW HAMPSHIRE', 'NEW JERSEY', 'NEW MEXICO', 'NEW YORK',
    'NORTH CAROLINA', 'NORTH DAKOTA', 'OHIO', 'OKLAHOMA', 'OREGON',
    'PENNSYLVANIA', 'RHODE ISLAND', 'SOUTH CAROLINA', 'SOUTH DAKOTA',
    'TENNESSEE', 'TEXAS', 'UTAH', 'VERMONT', 'VIRGINIA', 'WASHINGTON',
    'WEST VIRGINIA', 'WISCONSIN', 'WYOMING', 'DISTRICT OF COLUMBIA',
    'PUERTO RICO', 'VIRGIN ISLANDS', 'GUAM', 'AMERICAN SAMOA',
    'NORTHERN MARIANA ISLANDS'
}

# Map full state names to abbreviations
STATE_NAME_TO_ABBREV = {
    'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR',
    'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE',
    'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI', 'IDAHO': 'ID',
    'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA', 'KANSAS': 'KS',
    'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
    'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN',
    'MISSISSIPPI': 'MS', 'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE',
    'NEVADA': 'NV', 'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ',
    'NEW MEXICO': 'NM', 'NEW YORK': 'NY', 'NORTH CAROLINA': 'NC',
    'NORTH DAKOTA': 'ND', 'OHIO': 'OH', 'OKLAHOMA': 'OK', 'OREGON': 'OR',
    'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
    'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT',
    'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA',
    'WEST VIRGINIA': 'WV', 'WISCONSIN': 'WI', 'WYOMING': 'WY',
    'DISTRICT OF COLUMBIA': 'DC', 'PUERTO RICO': 'PR',
    'VIRGIN ISLANDS': 'VI', 'GUAM': 'GU', 'AMERICAN SAMOA': 'AS',
    'NORTHERN MARIANA ISLANDS': 'MP'
}


class TrademarkXMLParser:
    """Parse USPTO Trademark Assignment XML files.

    Expected format: <trademark-assignments> root with <assignment-entry> children.
    Each entry has <assignment>, <assignors>, <assignees>, <properties>.
    We extract the ASSIGNEE data (the businesses receiving the trademark).
    """

    def __init__(self, xml_file_path: str):
        self.xml_file_path = xml_file_path
        self.trademarks = []

    def parse_xml_file(self) -> List[Dict]:
        """Parse the trademark assignment XML and extract assignee (business) data.

        Each <assignment-entry> can have multiple assignees and multiple properties.
        We create one record per assignee, attaching all serial/registration numbers
        from that entry's <properties>.
        """
        logger.info(f"Parsing trademark XML file: {self.xml_file_path}")

        try:
            count = 0
            context = ET.iterparse(self.xml_file_path, events=('end',))
            for event, elem in context:
                if elem.tag == 'assignment-entry':
                    records = self._extract_assignment_entry(elem)
                    for rec in records:
                        self.trademarks.append(rec)
                        count += 1
                    elem.clear()

            logger.info(f"Extracted {count} trademark assignee records")
            return self.trademarks

        except ET.ParseError as e:
            logger.warning(f"XML parse error, trying fallback approach: {e}")
            return self._parse_xml_fallback()
        except Exception as e:
            logger.error(f"Error parsing trademark XML file: {e}")
            return []

    def _parse_xml_fallback(self) -> List[Dict]:
        """Fallback parser for XML files with DTD declarations or encoding issues."""
        try:
            with open(self.xml_file_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()

            # Strip DTD/DOCTYPE declarations that can cause parsing issues
            # The inline DTD can span many lines: <!DOCTYPE ... [ ... ]>
            import re
            content = re.sub(r'<!DOCTYPE[^[>]*\[.*?\]>', '', content, flags=re.DOTALL)
            content = re.sub(r'<!DOCTYPE[^>]*>', '', content)
            content = re.sub(r'<!ENTITY[^>]*>', '', content)

            # Wrap if no root element
            if '<trademark-assignments' not in content[:500]:
                content = '<trademark-assignments>' + content + '</trademark-assignments>'

            root = ET.fromstring(content)
            entries = root.findall('.//assignment-entry')

            for entry in entries:
                records = self._extract_assignment_entry(entry)
                self.trademarks.extend(records)

            logger.info(f"Fallback parser extracted {len(self.trademarks)} trademark records")
            return self.trademarks

        except Exception as e:
            logger.error(f"Fallback parser also failed: {e}")
            return []

    def _extract_assignment_entry(self, entry_elem) -> List[Dict]:
        """Extract business records from a single <assignment-entry>.

        Returns one record per assignee, each with the entry's property numbers.
        """
        records = []

        # Extract property serial/registration numbers
        serial_numbers = []
        registration_numbers = []
        properties = entry_elem.find('properties')
        if properties is not None:
            for prop in properties.findall('property'):
                serial = self._get_text(prop, 'serial-no')
                reg = self._get_text(prop, 'registration-no')
                if serial:
                    serial_numbers.append(serial.strip())
                if reg:
                    registration_numbers.append(reg.strip())

        trademark_number = serial_numbers[0] if serial_numbers else ''
        all_serial_numbers = ', '.join(serial_numbers) if serial_numbers else ''
        all_registration_numbers = ', '.join(registration_numbers) if registration_numbers else ''

        # Extract correspondent info from <assignment>
        assignment = entry_elem.find('assignment')
        correspondent_name = ''
        correspondent_address = ''
        if assignment is not None:
            correspondent = assignment.find('correspondent')
            if correspondent is not None:
                correspondent_name = self._get_text(correspondent, 'person-or-organization-name') or ''
                addr_parts = []
                for addr_tag in ['address-1', 'address-2', 'address-3', 'address-4']:
                    val = self._get_text(correspondent, addr_tag)
                    if val:
                        addr_parts.append(val)
                correspondent_address = ', '.join(addr_parts)

        # Extract assignee data (the businesses)
        assignees_elem = entry_elem.find('assignees')
        if assignees_elem is not None:
            for assignee in assignees_elem.findall('assignee'):
                record = self._extract_assignee(assignee)
                if record:
                    record['trademark_number'] = trademark_number
                    record['all_serial_numbers'] = all_serial_numbers
                    record['all_registration_numbers'] = all_registration_numbers
                    record['correspondent_name'] = correspondent_name
                    record['correspondent_address'] = correspondent_address
                    records.append(record)

        return records

    def _extract_assignee(self, assignee_elem) -> Optional[Dict]:
        """Extract data from a single <assignee> element."""
        try:
            contact_name = self._get_text(assignee_elem, 'person-or-organization-name') or ''
            if not contact_name.strip():
                return None

            address_1 = self._get_text(assignee_elem, 'address-1') or ''
            address_2 = self._get_text(assignee_elem, 'address-2') or ''
            city = self._get_text(assignee_elem, 'city') or ''
            state = self._get_text(assignee_elem, 'state') or ''
            country = self._get_text(assignee_elem, 'country-name') or ''
            postcode = self._get_text(assignee_elem, 'postcode') or ''
            legal_entity = self._get_text(assignee_elem, 'legal-entity-text') or ''
            nationality = self._get_text(assignee_elem, 'nationality') or ''

            # Normalize state: convert full name to abbreviation if needed
            state_upper = state.strip().upper()
            if state_upper in STATE_NAME_TO_ABBREV:
                state_abbrev = STATE_NAME_TO_ABBREV[state_upper]
            elif state_upper in US_STATE_ABBREVS:
                state_abbrev = state_upper
            else:
                state_abbrev = state.strip()

            return {
                'contact_name': contact_name.strip(),
                'address_1': address_1.strip(),
                'address_2': address_2.strip(),
                'city': city.strip(),
                'state': state_abbrev,
                'zip_code': postcode.strip(),
                'country': country.strip().upper() if country.strip() else '',
                'legal_entity_type': legal_entity.strip(),
                'nationality': nationality.strip(),
            }

        except Exception as e:
            logger.warning(f"Error extracting assignee data: {e}")
            return None

    def filter_us_only(self, trademarks: List[Dict]) -> List[Dict]:
        """Filter to US addresses only.

        Considers a record US if:
        - country is US/USA/UNITED STATES or empty
        - OR state is a valid US state abbreviation or name
        - OR nationality contains UNITED STATES
        """
        us_trademarks = []
        filtered_count = 0

        for tm in trademarks:
            country = (tm.get('country') or '').upper().strip()
            state = (tm.get('state') or '').upper().strip()
            nationality = (tm.get('nationality') or '').upper().strip()

            is_us_country = country in ('US', 'USA', 'UNITED STATES', '')
            has_us_state = state in US_STATE_ABBREVS or state in US_STATE_NAMES
            is_us_nationality = 'UNITED STATES' in nationality

            if is_us_country or has_us_state or is_us_nationality:
                us_trademarks.append(tm)
            else:
                filtered_count += 1

        logger.info(f"US filter: kept {len(us_trademarks)}, filtered out {filtered_count} foreign records")
        return us_trademarks

    def deduplicate(self, trademarks: List[Dict], past_names: Set[str] = None) -> List[Dict]:
        """Remove duplicate names from the list and against past run history.

        Deduplication key: lowercase contact_name.
        """
        if past_names is None:
            past_names = set()

        seen = set()
        unique_trademarks = []
        dup_count = 0
        past_dup_count = 0

        for tm in trademarks:
            name_key = (tm.get('contact_name') or '').strip().lower()
            if not name_key:
                continue

            if name_key in past_names:
                past_dup_count += 1
                continue

            if name_key in seen:
                dup_count += 1
                continue

            seen.add(name_key)
            unique_trademarks.append(tm)

        logger.info(
            f"Deduplication: {len(unique_trademarks)} unique, "
            f"{dup_count} batch duplicates, {past_dup_count} past duplicates removed"
        )
        return unique_trademarks

    def _get_text(self, element, tag: str) -> Optional[str]:
        """Safely extract text from a child element by tag name."""
        if element is None:
            return None
        found = element.find(tag)
        if found is not None and found.text:
            return found.text.strip()
        return None
