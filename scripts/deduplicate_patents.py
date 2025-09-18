#!/usr/bin/env python3
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
        print("\n🎉 Deduplication complete!")
        print(f"   Original: {result['original_count']} patents")
        print(f"   Cleaned: {result['unique_count']} patents")
        print(f"   Removed: {result['duplicates_removed']} duplicates")
