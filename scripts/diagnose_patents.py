#!/usr/bin/env python3
"""
Standalone Patent Download Diagnostics Script
Usage: python diagnose_patents.py [path_to_json_file]
"""

import json
import sys
import os
from pathlib import Path
from collections import Counter, defaultdict
from typing import Dict, Any, List

def diagnose_downloaded_patents(json_file_path: str) -> Dict[str, Any]:
    """
    Diagnose the downloaded patents JSON file for duplicates and issues
    """
    try:
        print(f"Reading file: {json_file_path}")
        with open(json_file_path, 'r') as f:
            patents = json.load(f)
        
        print(f"\n=== PATENT DOWNLOAD DIAGNOSTICS ===")
        print(f"Total patents in file: {len(patents)}")
        
        # Check for duplicate patent numbers
        patent_numbers = [p.get('patent_number', '') for p in patents]
        patent_counts = Counter(patent_numbers)
        duplicates = {k: v for k, v in patent_counts.items() if v > 1}
        
        print(f"Unique patent numbers: {len(patent_counts)}")
        print(f"Duplicate patent numbers: {len(duplicates)}")
        
        if duplicates:
            print(f"\n🚨 DUPLICATE PATENTS FOUND:")
            for patent_num, count in list(duplicates.items())[:10]:  # Show first 10
                print(f"  Patent {patent_num}: appears {count} times")
            
            if len(duplicates) > 10:
                print(f"  ... and {len(duplicates) - 10} more duplicates")
        else:
            print(f"\n✅ No duplicate patents found")
        
        # Check for empty patent numbers
        empty_patents = sum(1 for pn in patent_numbers if not pn or pn.strip() == '')
        print(f"Patents with empty/missing numbers: {empty_patents}")
        
        # Sample some patents to see their structure
        print(f"\nSAMPLE PATENTS (first 5):")
        for i, patent in enumerate(patents[:5]):
            print(f"  Patent {i+1}:")
            print(f"    Number: {patent.get('patent_number')}")
            print(f"    Title: {patent.get('patent_title', '')[:60]}...")
            print(f"    Date: {patent.get('patent_date')}")
            print(f"    Inventors: {len(patent.get('inventors', []))}")
            print(f"    Assignees: {len(patent.get('assignees', []))}")
        
        # Check inventor distribution
        total_inventors = sum(len(p.get('inventors', [])) for p in patents)
        patents_with_inventors = sum(1 for p in patents if p.get('inventors', []))
        
        print(f"\nINVENTOR ANALYSIS:")
        print(f"  Total inventors across all patents: {total_inventors}")
        print(f"  Patents with inventors: {patents_with_inventors}")
        if len(patents) > 0:
            print(f"  Average inventors per patent: {total_inventors/len(patents):.2f}")
        
        # Sample some inventors from different patents
        inventor_samples = []
        for patent in patents[:20]:  # First 20 patents
            patent_num = patent.get('patent_number', 'Unknown')
            for inv in patent.get('inventors', []):
                inventor_samples.append({
                    'name': f"{inv.get('first_name', '')} {inv.get('last_name', '')}".strip(),
                    'location': f"{inv.get('city', '')}, {inv.get('state', '')}".strip(),
                    'patent': patent_num
                })
        
        print(f"\nSAMPLE INVENTORS (from first 20 patents):")
        for i, inv in enumerate(inventor_samples[:15]):
            print(f"  {i+1}. {inv['name']} ({inv['location']}) - Patent: {inv['patent']}")
        
        # Check for inventor duplicates across patents
        inventor_keys = []
        for inv in inventor_samples:
            if inv['name'].strip():
                key = f"{inv['name'].lower()}_{inv['location'].lower()}"
                inventor_keys.append(key)
        
        inventor_counts = Counter(inventor_keys)
        inventor_duplicates = {k: v for k, v in inventor_counts.items() if v > 1}
        
        print(f"\nINVENTOR DUPLICATE ANALYSIS:")
        print(f"  Unique inventor+location combinations: {len(inventor_counts)}")
        print(f"  Inventors appearing on multiple patents: {len(inventor_duplicates)}")
        
        if inventor_duplicates:
            print(f"  Top inventors with multiple patents:")
            for key, count in list(inventor_counts.most_common(5)):
                print(f"    {key.replace('_', ' at ')}: {count} patents")
        
        results = {
            'total_patents': len(patents),
            'unique_patent_numbers': len(patent_counts),
            'duplicates_found': len(duplicates),
            'duplicate_details': dict(list(duplicates.items())[:50]),  # Limit to 50 for output
            'empty_patent_numbers': empty_patents,
            'total_inventors': total_inventors,
            'patents_with_inventors': patents_with_inventors,
            'unique_inventors': len(inventor_counts),
            'inventors_on_multiple_patents': len(inventor_duplicates)
        }
        
        # Summary recommendation
        print(f"\n=== SUMMARY ===")
        if len(duplicates) > 0:
            print(f"🚨 ISSUE DETECTED: {len(duplicates)} patent numbers have duplicates")
            print(f"   This means the download process is storing the same patent multiple times")
            print(f"   Recommendation: Fix the download code to prevent duplicates")
            print(f"   Quick fix: Run deduplication on this file")
        else:
            print(f"✅ Download looks clean - no duplicate patents detected")
        
        if empty_patents > 0:
            print(f"⚠️  WARNING: {empty_patents} patents have missing patent numbers")
        
        return results
        
    except FileNotFoundError:
        print(f"❌ Error: File not found: {json_file_path}")
        return {'error': 'File not found'}
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON file: {e}")
        return {'error': f'Invalid JSON: {e}'}
    except Exception as e:
        print(f"❌ Error reading patents file: {e}")
        return {'error': str(e)}


def create_deduplication_script():
    """
    Create a standalone script to deduplicate an existing patents JSON file
    """
    script_content = '''#!/usr/bin/env python3
"""
Standalone Patent Deduplication Script
Usage: python deduplicate_patents.py input_file.json output_file.json
"""

import json
import sys
from collections import defaultdict

def deduplicate_patents(input_file, output_file):
    """Remove duplicate patents from JSON file"""
    
    try:
        print(f"Reading input file: {input_file}")
        with open(input_file, 'r') as f:
            patents = json.load(f)
        
        print(f"Original patents: {len(patents)}")
        
        # Group by patent_id/patent_number
        seen_ids = set()
        unique_patents = []
        duplicates_removed = 0
        
        for patent in patents:
            patent_id = patent.get('patent_number') or patent.get('patent_id', '')
            
            if patent_id and patent_id not in seen_ids:
                seen_ids.add(patent_id)
                unique_patents.append(patent)
            elif patent_id:
                duplicates_removed += 1
            else:
                print(f"Warning: Patent with no ID found: {patent}")
        
        print(f"Unique patents: {len(unique_patents)}")
        print(f"Duplicates removed: {duplicates_removed}")
        
        # Save deduplicated file
        print(f"Saving to: {output_file}")
        with open(output_file, 'w') as f:
            json.dump(unique_patents, f, indent=2, default=str)
        
        print(f"✅ Deduplicated file saved as: {output_file}")
        
        return {
            'original_count': len(patents),
            'unique_count': len(unique_patents),
            'duplicates_removed': duplicates_removed
        }
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python deduplicate_patents.py input_file.json output_file.json")
        print("Example: python deduplicate_patents.py downloaded_patents.json downloaded_patents_clean.json")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    result = deduplicate_patents(input_file, output_file)
    if result:
        print("\\n🎉 Deduplication complete!")
        print(f"   Original: {result['original_count']} patents")
        print(f"   Cleaned: {result['unique_count']} patents")
        print(f"   Removed: {result['duplicates_removed']} duplicates")
'''
    
    with open('deduplicate_patents.py', 'w') as f:
        f.write(script_content)
    
    print("\n📄 Created deduplicate_patents.py script")
    print("Usage: python deduplicate_patents.py input.json output.json")


def main():
    """Main function to run diagnostics"""
    
    # Default file path
    default_file = "output/downloaded_patents.json"
    
    # Check command line arguments
    if len(sys.argv) > 1:
        json_file = sys.argv[1]
    else:
        json_file = default_file
        print(f"No file specified, using default: {json_file}")
    
    # Check if file exists
    if not Path(json_file).exists():
        print(f"❌ File not found: {json_file}")
        
        # Try to find the file in common locations
        possible_paths = [
            "downloaded_patents.json",
            "output/downloaded_patents.json", 
            "../output/downloaded_patents.json",
            "Patent_Grants/output/downloaded_patents.json"
        ]
        
        print("🔍 Searching for downloaded patents file in common locations...")
        found = False
        for path in possible_paths:
            if Path(path).exists():
                print(f"✅ Found file at: {path}")
                json_file = path
                found = True
                break
        
        if not found:
            print("❌ Could not find downloaded_patents.json file")
            print("Please specify the correct path:")
            print("   python diagnose_patents.py /path/to/downloaded_patents.json")
            sys.exit(1)
    
    # Run diagnostics
    results = diagnose_downloaded_patents(json_file)
    
    if results.get('duplicates_found', 0) > 0:
        print(f"\n🛠️  NEXT STEPS:")
        print(f"1. Create deduplication script (will be created automatically)")
        create_deduplication_script()
        
        print(f"2. Run deduplication:")
        clean_file = json_file.replace('.json', '_clean.json')
        print(f"   python deduplicate_patents.py {json_file} {clean_file}")
        
        print(f"3. Use the clean file for Step 1 processing")
        print(f"   Update your config to use: {clean_file}")
        
    else:
        print(f"\n✅ Your downloaded patents file looks good!")
        print(f"   No duplicates detected - the issue is likely in Step 1 processing")


if __name__ == "__main__":
    main()