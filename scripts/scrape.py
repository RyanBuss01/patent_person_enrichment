#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
import json
import re
import sys
import argparse
from urllib.parse import quote

# Default constants
DEFAULT_FIRST_NAME = "Gary"
DEFAULT_LAST_NAME = "Siriano"
DEFAULT_STATE = "MA"
DEFAULT_CITY = "LUNENBURG"

def format_city_for_url(city):
    """Convert city name to URL-friendly format"""
    return city.lower().replace(" ", "-")

def format_state_name(state_code):
    """Convert state code to full state name for URL"""
    state_mapping = {
        'NJ': 'new-jersey',
        'NY': 'new-york',
        'CA': 'california',
        'TX': 'texas',
        'FL': 'florida',
        'PA': 'pennsylvania',
        'IL': 'illinois',
        'OH': 'ohio',
        'GA': 'georgia',
        'NC': 'north-carolina',
        'MI': 'michigan',
        'VA': 'virginia',
        'WA': 'washington',
        'AZ': 'arizona',
        'MA': 'massachusetts',
        'TN': 'tennessee',
        'IN': 'indiana',
        'MO': 'missouri',
        'MD': 'maryland',
        'WI': 'wisconsin',
        'CO': 'colorado',
        'MN': 'minnesota',
        'SC': 'south-carolina',
        'AL': 'alabama',
        'LA': 'louisiana',
        'KY': 'kentucky',
        'OR': 'oregon',
        'OK': 'oklahoma',
        'CT': 'connecticut',
        'IA': 'iowa',
        'MS': 'mississippi',
        'AR': 'arkansas',
        'UT': 'utah',
        'KS': 'kansas',
        'NV': 'nevada',
        'NM': 'new-mexico',
        'NE': 'nebraska',
        'WV': 'west-virginia',
        'ID': 'idaho',
        'HI': 'hawaii',
        'NH': 'new-hampshire',
        'ME': 'maine',
        'MT': 'montana',
        'RI': 'rhode-island',
        'DE': 'delaware',
        'SD': 'south-dakota',
        'ND': 'north-dakota',
        'AK': 'alaska',
        'VT': 'vermont',
        'WY': 'wyoming'
    }
    return state_mapping.get(state_code.upper(), state_code.lower())

def construct_url(first_name, last_name, state, city):
    """Construct the ZabaSearch URL"""
    formatted_city = format_city_for_url(city)
    formatted_state = format_state_name(state)
    formatted_first = first_name.lower()
    formatted_last = last_name.lower()
    
    url = f"https://www.zabasearch.com/people/{formatted_first}-{formatted_last}/{formatted_state}/{formatted_city}/#CTA"
    return url

def extract_phone_numbers(soup):
    """Extract phone numbers from the page"""
    phones = []
    phone_section = soup.find('h3', string='Associated Phone Numbers')
    if phone_section:
        phone_list = phone_section.find_next('ul', class_='showMore-list')
        if phone_list:
            for li in phone_list.find_all('li'):
                phone_link = li.find('a')
                if phone_link:
                    phone_text = phone_link.get_text(strip=True)
                    phones.append(phone_text)
    return phones

def extract_email_addresses(soup):
    """Extract email addresses from the page"""
    emails = []
    email_section = soup.find('h3', string='Associated Email Addresses')
    if email_section:
        email_list = email_section.find_next('ul', class_='showMore-list')
        if email_list:
            for li in email_list.find_all('li'):
                # Get the full text and reconstruct the email
                full_text = li.get_text(strip=True)
                # Extract the domain part (after @)
                if '@' in full_text:
                    domain_part = full_text.split('@')[1]
                    # Get the blurred part
                    blur_span = li.find('span', class_='blur')
                    if blur_span:
                        username = blur_span.get_text(strip=True)
                        full_email = f"{username}@{domain_part}"
                        emails.append(full_email)
                    else:
                        # If no blur span, take the whole text
                        emails.append(full_text)
    return emails

def extract_addresses(soup):
    """Extract current and past addresses from the page"""
    addresses = {
        'current': None,
        'past': []
    }
    
    # Extract current address
    current_section = soup.find('h3', string='Last Known Address')
    if current_section:
        address_div = current_section.find_next('div', class_='flex')
        if address_div:
            address_p = address_div.find('p')
            if address_p:
                # Get the address text and clean it up
                address_text = address_p.get_text(separator=' ', strip=True)
                addresses['current'] = address_text
    
    # Extract past addresses
    past_section = soup.find('h3', string='Past Addresses')
    if past_section:
        address_list = past_section.find_next('ul')
        if address_list:
            for li in address_list.find_all('li'):
                address_text = li.get_text(separator=' ', strip=True)
                addresses['past'].append(address_text)
    
    return addresses

def parse_address_components(address_text):
    """Parse address text to extract street address and ZIP code"""
    components = {
        'street': '',
        'zip': ''
    }
    
    if not address_text:
        return components
    
    # Extract ZIP code (5 digits at the end)
    zip_match = re.search(r'\b(\d{5})\b\s*$', address_text)
    if zip_match:
        components['zip'] = zip_match.group(1)
        # Remove ZIP from address to get street portion
        street_part = address_text[:zip_match.start()].strip()
        # Remove state abbreviation if present
        street_part = re.sub(r',?\s+[A-Z]{2}\s*$', '', street_part)
        components['street'] = street_part.strip()
    else:
        # If no ZIP found, use the whole address as street
        components['street'] = address_text.strip()
    
    return components

def scrape_zabasearch(first_name, last_name, state, city):
    """Main scraping function"""
    url = construct_url(first_name, last_name, state, city)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    try:
        print(f"Scraping URL: {url}")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract data
        phones = extract_phone_numbers(soup)
        emails = extract_email_addresses(soup)
        addresses = extract_addresses(soup)
        
        # Parse current address for street and ZIP
        current_address = addresses.get('current', '')
        address_components = parse_address_components(current_address)
        
        # Construct result with additional parsed fields
        result = {
            'search_parameters': {
                'first_name': first_name,
                'last_name': last_name,
                'state': state,
                'city': city,
                'url': url
            },
            'data': {
                'phone_numbers': phones,
                'email_addresses': emails,
                'addresses': addresses
            },
            # Add parsed address components for integration
            'mail_to_add1': address_components['street'],
            'zip': address_components['zip']
        }
        
        return result
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching the page: {e}")
        return None
    except Exception as e:
        print(f"Error parsing the page: {e}")
        return None

def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(description='Scrape ZabaSearch for person information')
    parser.add_argument('--first', '-f', default=DEFAULT_FIRST_NAME, 
                       help=f'First name (default: {DEFAULT_FIRST_NAME})')
    parser.add_argument('--last', '-l', default=DEFAULT_LAST_NAME,
                       help=f'Last name (default: {DEFAULT_LAST_NAME})')
    parser.add_argument('--state', '-s', default=DEFAULT_STATE,
                       help=f'State code (default: {DEFAULT_STATE})')
    parser.add_argument('--city', '-c', default=DEFAULT_CITY,
                       help=f'City name (default: {DEFAULT_CITY})')
    parser.add_argument('--json', '-j', action='store_true',
                       help='Output only JSON result (no formatting)')
    
    args = parser.parse_args()
    
    # Run the scraper
    result = scrape_zabasearch(args.first, args.last, args.state, args.city)
    
    if result:
        if args.json:
            # Output only JSON for programmatic use
            print(json.dumps(result, ensure_ascii=False))
        else:
            # Pretty print the JSON result
            print("\n" + "="*50)
            print("SCRAPING RESULTS")
            print("="*50)
            print(json.dumps(result, indent=2, ensure_ascii=False))
            print("="*50)
        return 0
    else:
        print("Failed to scrape data from ZabaSearch")
        return 1

if __name__ == "__main__":
    sys.exit(main())