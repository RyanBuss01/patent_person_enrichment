#!/usr/bin/env python3
# =============================================================================
# diagnostic_matching.py - Self-Contained Data Analysis and Testing Tool
# Analyze CSV structure, XML structure, test comparisons - NO EXTERNAL IMPORTS
# =============================================================================

import pandas as pd
import xml.etree.ElementTree as ET
import json
import os
import re
from pathlib import Path
from collections import defaultdict, Counter
import logging
from datetime import datetime

# Try to import fuzzywuzzy, fall back to basic matching if not available
try:
    from fuzzywuzzy import fuzz
    FUZZY_AVAILABLE = True
except ImportError:
    FUZZY_AVAILABLE = False
    print("⚠️  fuzzywuzzy not available - using basic string matching")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SelfContainedDataAnalyzer:
    """Complete self-contained data analysis tool"""
    
    def __init__(self):
        self.csv_analysis = {}
        self.xml_analysis = {}
        self.found_files = {}
        
        # Built-in data storage for comparison testing
        self.existing_patents = set()
        self.existing_people = set()
        
    def run_full_analysis(self):
        """Run complete analysis suite"""
        print("🔍 SELF-CONTAINED DATA ANALYSIS SUITE")
        print("=" * 60)
        print("This tool analyzes your data structure and tests comparison logic")
        print("without requiring any external imports or dependencies.")
        print()
        
        # Step 1: Find all data files
        self.find_data_files()
        
        # Step 2: Analyze CSV structure
        self.analyze_csv_databases()
        
        # Step 3: Analyze XML structure
        self.analyze_xml_files()
        
        # Step 4: Test comparison logic
        self.test_comparison_matching()
        
        # Step 5: Run actual comparison test
        self.run_comparison_test()
        
        # Step 6: Generate recommendations
        self.generate_recommendations()
        
        # Step 7: Save comprehensive report
        self.save_analysis_report()
    
    def find_data_files(self):
        """Find and catalog all data files"""
        print("🕵️ STEP 1: FINDING DATA FILES")
        print("=" * 40)
        
        # Current and parent directories
        current_dir = Path.cwd()
        parent_dir = current_dir.parent
        
        print(f"📁 Current directory: {current_dir}")
        print(f"📁 Parent directory: {parent_dir}")
        
        # Search locations
        search_locations = [
            ("../converted_databases", "Converted databases folder"),
            ("../converted_databases/csv", "CSV files folder"),
            ("../patent_system", "Patent system folder"), 
            ("../USPC_Download", "USPC Download folder"),
            ("../Office20", "Office20 folder"),
            ("../ipg250812.xml", "Main XML file"),
            ("./converted_databases", "Local converted databases"),
            ("./ipg250812.xml", "Local main XML file"),
        ]
        
        self.found_files = {'csv_folders': [], 'xml_files': [], 'other_folders': []}
        
        print("\n🎯 Search Results:")
        for path, description in search_locations:
            exists = os.path.exists(path)
            status = "✅" if exists else "❌"
            print(f"{status} {description}: {path}")
            
            if exists:
                if 'csv' in path.lower() and os.path.isdir(path):
                    self.found_files['csv_folders'].append(path)
                elif path.endswith('.xml'):
                    self.found_files['xml_files'].append(path)
                elif os.path.isdir(path):
                    self.found_files['other_folders'].append(path)
        
        # Find CSV files in all folders
        print(f"\n📊 CSV Files Found:")
        all_csv_files = []
        for folder in self.found_files['csv_folders']:
            csv_files = [f for f in os.listdir(folder) if f.endswith('.csv')]
            if csv_files:
                print(f"   📁 {folder}: {len(csv_files)} files")
                print(f"      Files: {csv_files[:3]}{'...' if len(csv_files) > 3 else ''}")
                all_csv_files.extend([(folder, f) for f in csv_files])
        
        # Find XML files in folders
        print(f"\n📄 XML Files Found:")
        all_xml_files = []
        for folder in [f for f in self.found_files['other_folders'] if 'uspc' in f.lower()]:
            if os.path.isdir(folder):
                xml_files = [f for f in os.listdir(folder) if f.endswith(('.xml', '.txt'))]
                if xml_files:
                    print(f"   📁 {folder}: {len(xml_files)} files")
                    all_xml_files.extend([os.path.join(folder, f) for f in xml_files])
        
        # Add individual XML files
        all_xml_files.extend(self.found_files['xml_files'])
        
        self.found_files['all_csv_files'] = all_csv_files
        self.found_files['all_xml_files'] = all_xml_files
        
        print(f"\n📈 Summary:")
        print(f"   CSV files: {len(all_csv_files)}")
        print(f"   XML files: {len(all_xml_files)}")
    
    def analyze_csv_databases(self):
        """Analyze CSV database structure"""
        print(f"\n🔍 STEP 2: ANALYZING CSV DATABASES")
        print("=" * 40)
        
        if not self.found_files['all_csv_files']:
            print("❌ No CSV files found to analyze")
            return
        
        for folder, csv_file in self.found_files['all_csv_files'][:10]:  # Analyze first 10
            file_path = os.path.join(folder, csv_file)
            print(f"\n📄 Analyzing: {csv_file}")
            
            try:
                analysis = self._analyze_csv_file(file_path)
                self.csv_analysis[csv_file] = analysis
                self._print_csv_summary(analysis, csv_file)
                
                # Store data for comparison testing
                self._extract_csv_data_for_testing(file_path, analysis)
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
    
    def _analyze_csv_file(self, file_path: str) -> dict:
        """Detailed CSV file analysis"""
        # Try different encodings
        for encoding in ['utf-8', 'latin1', 'cp1252']:
            try:
                df = pd.read_csv(file_path, encoding=encoding, low_memory=False, nrows=5000)
                break
            except UnicodeDecodeError:
                continue
        else:
            raise ValueError("Could not read file with any encoding")
        
        analysis = {
            'file_info': {
                'rows': len(df),
                'columns': len(df.columns),
                'size_mb': os.path.getsize(file_path) / (1024 * 1024),
                'encoding_used': encoding
            },
            'columns': list(df.columns),
            'sample_data': df.head(3).to_dict('records'),
            'potential_patents': self._find_patent_columns(df),
            'potential_people': self._find_people_columns(df),
            'potential_locations': self._find_location_columns(df),
            'data_quality': self._assess_data_quality(df)
        }
        
        return analysis
    
    def _find_patent_columns(self, df: pd.DataFrame) -> list:
        """Find potential patent number columns"""
        patent_keywords = ['patent', 'number', 'id', 'publication', 'application', 'doc']
        patent_columns = []
        
        for col in df.columns:
            col_lower = str(col).lower()
            if any(keyword in col_lower for keyword in patent_keywords):
                sample_values = df[col].dropna().head(20).astype(str).tolist()
                
                # Check if values look like patent numbers
                patent_like_count = 0
                for val in sample_values:
                    val_clean = re.sub(r'[^\d]', '', str(val))
                    if len(val_clean) >= 4 and val_clean.isdigit():
                        patent_like_count += 1
                
                confidence = patent_like_count / len(sample_values) if sample_values else 0
                
                patent_columns.append({
                    'column': col,
                    'sample_values': sample_values[:5],
                    'non_null_count': df[col].notna().sum(),
                    'unique_count': df[col].nunique(),
                    'confidence': confidence,
                    'looks_like_patents': confidence > 0.5
                })
        
        return patent_columns
    
    def _find_people_columns(self, df: pd.DataFrame) -> dict:
        """Find potential people/name columns"""
        name_patterns = {
            'first_name': ['first', 'fname', 'firstname', 'given', 'forename'],
            'last_name': ['last', 'lname', 'lastname', 'surname', 'family'],
            'full_name': ['name', 'fullname', 'inventor', 'assignee', 'person'],
            'organization': ['org', 'company', 'corporation', 'business', 'entity']
        }
        
        people_columns = {}
        
        for name_type, keywords in name_patterns.items():
            matches = []
            for col in df.columns:
                col_lower = str(col).lower()
                for keyword in keywords:
                    if keyword in col_lower:
                        sample_values = df[col].dropna().head(10).astype(str).tolist()
                        confidence = self._assess_name_quality(sample_values, name_type)
                        
                        matches.append({
                            'column': col,
                            'sample_values': sample_values[:5],
                            'non_null_count': df[col].notna().sum(),
                            'unique_count': df[col].nunique(),
                            'confidence': confidence,
                            'keyword_matched': keyword
                        })
                        break
            
            if matches:
                # Take the best match
                best_match = max(matches, key=lambda x: x['confidence'])
                if best_match['confidence'] > 0.3:
                    people_columns[name_type] = best_match
        
        return people_columns
    
    def _find_location_columns(self, df: pd.DataFrame) -> dict:
        """Find potential location columns"""
        location_patterns = {
            'city': ['city', 'town', 'locality', 'municipality'],
            'state': ['state', 'province', 'region', 'territory'],
            'country': ['country', 'nation'],
            'address': ['address', 'street', 'location', 'addr'],
            'postal_code': ['zip', 'postal', 'postcode', 'zipcode']
        }
        
        location_columns = {}
        
        for loc_type, keywords in location_patterns.items():
            for col in df.columns:
                col_lower = str(col).lower()
                if any(keyword in col_lower for keyword in keywords):
                    sample_values = df[col].dropna().head(10).astype(str).tolist()
                    location_columns[loc_type] = {
                        'column': col,
                        'sample_values': sample_values[:5],
                        'non_null_count': df[col].notna().sum(),
                        'unique_count': df[col].nunique()
                    }
                    break
        
        return location_columns
    
    def _assess_name_quality(self, sample_values: list, name_type: str) -> float:
        """Assess if sample values look like names"""
        if not sample_values:
            return 0.0
        
        name_like_count = 0
        for val in sample_values:
            val_str = str(val).strip()
            
            if name_type in ['first_name', 'last_name']:
                if (val_str.replace('-', '').replace("'", "").isalpha() and 
                    2 <= len(val_str) <= 25 and
                    val_str.lower() not in ['nan', 'none', 'null', 'unknown']):
                    name_like_count += 1
            elif name_type == 'full_name':
                words = val_str.split()
                if (len(words) >= 2 and 
                    all(word.replace('-', '').replace("'", "").isalpha() for word in words) and
                    len(val_str) <= 60):
                    name_like_count += 1
            elif name_type == 'organization':
                if (len(val_str) >= 3 and 
                    any(c.isalpha() for c in val_str) and
                    val_str.lower() not in ['nan', 'none', 'null', 'unknown']):
                    name_like_count += 1
        
        return name_like_count / len(sample_values)
    
    def _assess_data_quality(self, df: pd.DataFrame) -> dict:
        """Assess overall data quality"""
        total_cells = df.shape[0] * df.shape[1]
        null_cells = df.isnull().sum().sum()
        
        return {
            'completeness': (total_cells - null_cells) / total_cells if total_cells > 0 else 0,
            'null_percentage': null_cells / total_cells * 100 if total_cells > 0 else 0,
            'columns_with_nulls': sum(df.isnull().any()),
            'duplicate_rows': df.duplicated().sum()
        }
    
    def _extract_csv_data_for_testing(self, file_path: str, analysis: dict):
        """Extract sample data for comparison testing"""
        try:
            df = pd.read_csv(file_path, encoding='utf-8', low_memory=False, nrows=1000)
        except:
            try:
                df = pd.read_csv(file_path, encoding='latin1', low_memory=False, nrows=1000)
            except:
                return
        
        # Extract patent numbers
        for patent_col_info in analysis['potential_patents']:
            if patent_col_info['looks_like_patents']:
                col = patent_col_info['column']
                for patent_num in df[col].dropna().head(50):
                    normalized = self._normalize_patent_number(patent_num)
                    if normalized:
                        self.existing_patents.add(normalized)
        
        # Extract people
        people_cols = analysis['potential_people']
        if 'first_name' in people_cols and 'last_name' in people_cols:
            first_col = people_cols['first_name']['column']
            last_col = people_cols['last_name']['column']
            
            for _, row in df.head(50).iterrows():
                first = str(row.get(first_col, '')).strip()
                last = str(row.get(last_col, '')).strip()
                person_id = self._create_person_identifier(first, last)
                if person_id:
                    self.existing_people.add(person_id)
    
    def _print_csv_summary(self, analysis: dict, filename: str):
        """Print CSV analysis summary"""
        info = analysis['file_info']
        quality = analysis['data_quality']
        
        print(f"   📊 {info['rows']:,} rows, {info['columns']} columns, {info['size_mb']:.1f} MB")
        print(f"   📈 Data quality: {quality['completeness']:.1%} complete, {quality['null_percentage']:.1f}% null")
        
        # Patent columns
        patents = analysis['potential_patents']
        if patents:
            high_confidence = [p for p in patents if p['looks_like_patents']]
            print(f"   📋 Patent columns: {len(patents)} found, {len(high_confidence)} high confidence")
            for p in high_confidence:
                print(f"      ✅ {p['column']}: {p['non_null_count']:,} values ({p['confidence']:.1%} confidence)")
        
        # People columns
        people = analysis['potential_people']
        if people:
            print(f"   👥 People columns found:")
            for name_type, match in people.items():
                if match['confidence'] > 0.3:
                    print(f"      ✅ {name_type}: {match['column']} ({match['confidence']:.1%} confidence)")
    
    def analyze_xml_files(self):
        """Analyze XML file structure"""
        print(f"\n🔍 STEP 3: ANALYZING XML FILES")
        print("=" * 40)
        
        if not self.found_files['all_xml_files']:
            print("❌ No XML files found to analyze")
            return
        
        # Analyze the main XML file first
        main_xml = next((f for f in self.found_files['all_xml_files'] if 'ipg250812.xml' in f), None)
        
        if main_xml:
            print(f"📄 Analyzing main XML file: {main_xml}")
            analysis = self._analyze_xml_file(main_xml)
            self.xml_analysis[main_xml] = analysis
            self._print_xml_summary(analysis, main_xml)
        
        # Analyze other XML files (limit to first few)
        other_files = [f for f in self.found_files['all_xml_files'] if f != main_xml][:3]
        for xml_file in other_files:
            print(f"\n📄 Analyzing: {os.path.basename(xml_file)}")
            try:
                analysis = self._analyze_xml_file(xml_file)
                self.xml_analysis[xml_file] = analysis
                self._print_xml_summary(analysis, xml_file)
            except Exception as e:
                print(f"   ❌ Error: {e}")
    
    def _analyze_xml_file(self, xml_file: str) -> dict:
        """Detailed XML file analysis"""
        with open(xml_file, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        analysis = {
            'file_info': {
                'size_mb': len(content) / (1024*1024),
                'total_chars': len(content)
            },
            'patent_count': content.count('<us-patent-grant'),
            'inventor_count': content.count('<inventor'),
            'assignee_count': content.count('<assignee'),
            'sample_patents': [],
            'xml_parsing_errors': 0
        }
        
        # Parse sample patents for detailed structure
        try:
            xml_docs = content.split('<?xml version="1.0" encoding="UTF-8"?>')
            
            parsed_count = 0
            for i, doc in enumerate(xml_docs[1:6]):  # Parse first 5 patents
                try:
                    full_doc = '<?xml version="1.0" encoding="UTF-8"?>' + doc
                    root = ET.fromstring(full_doc)
                    
                    patent_info = self._extract_sample_patent_structure(root)
                    if patent_info:
                        analysis['sample_patents'].append(patent_info)
                        parsed_count += 1
                        
                except ET.XMLSyntaxError:
                    analysis['xml_parsing_errors'] += 1
                    continue
                except Exception:
                    analysis['xml_parsing_errors'] += 1
                    continue
            
            analysis['successfully_parsed'] = parsed_count
            
        except Exception as e:
            analysis['parsing_error'] = str(e)
        
        return analysis
    
    def _extract_sample_patent_structure(self, root) -> dict:
        """Extract detailed patent structure for analysis"""
        try:
            patent_info = {
                'patent_number': self._get_xml_text(root, './/document-id/doc-number'),
                'patent_title': self._get_xml_text(root, './/invention-title'),
                'patent_date': self._get_xml_text(root, './/document-id/date'),
                'inventors': [],
                'assignees': [],
                'xml_structure': {}
            }
            
            # Find inventor path
            inventor_paths = [
                './/parties/inventors/inventor',
                './/us-parties/inventors/inventor',
                './/applicants/applicant[@app-type="applicant-inventor"]'
            ]
            
            inventors = []
            for path in inventor_paths:
                inventors = root.findall(path)
                if inventors:
                    patent_info['xml_structure']['inventor_path'] = path
                    break
            
            # Extract sample inventors
            for inv in inventors[:3]:
                inventor_data = {
                    'first_name': self._get_xml_text(inv, './/first-name'),
                    'last_name': self._get_xml_text(inv, './/last-name'),
                    'city': self._get_xml_text(inv, './/city'),
                    'state': self._get_xml_text(inv, './/state'),
                    'country': self._get_xml_text(inv, './/country')
                }
                patent_info['inventors'].append(inventor_data)
            
            # Find assignee path
            assignee_paths = [
                './/parties/assignees/assignee',
                './/us-parties/assignees/assignee'
            ]
            
            assignees = []
            for path in assignee_paths:
                assignees = root.findall(path)
                if assignees:
                    patent_info['xml_structure']['assignee_path'] = path
                    break
            
            # Extract sample assignees
            for ass in assignees[:3]:
                assignee_data = {
                    'organization': self._get_xml_text(ass, './/orgname'),
                    'first_name': self._get_xml_text(ass, './/first-name'),
                    'last_name': self._get_xml_text(ass, './/last-name'),
                    'city': self._get_xml_text(ass, './/city'),
                    'state': self._get_xml_text(ass, './/state')
                }
                patent_info['assignees'].append(assignee_data)
            
            return patent_info
            
        except Exception as e:
            return {'error': str(e)}
    
    def _get_xml_text(self, element, xpath: str) -> str:
        """Safely extract text from XML element"""
        try:
            found = element.find(xpath)
            return found.text.strip() if found is not None and found.text else ''
        except:
            return ''
    
    def _print_xml_summary(self, analysis: dict, filename: str):
        """Print XML analysis summary"""
        info = analysis['file_info']
        print(f"   📊 File size: {info['size_mb']:.1f} MB")
        print(f"   📄 Patents: {analysis['patent_count']:,}")
        print(f"   👥 Inventors: {analysis['inventor_count']:,}")
        print(f"   🏢 Assignees: {analysis['assignee_count']:,}")
        
        if analysis['sample_patents']:
            print(f"   ✅ Successfully parsed: {analysis.get('successfully_parsed', 0)} sample patents")
            sample = analysis['sample_patents'][0]
            if 'xml_structure' in sample:
                structure = sample['xml_structure']
                if 'inventor_path' in structure:
                    print(f"   🔍 Inventor XML path: {structure['inventor_path']}")
    
    def test_comparison_matching(self):
        """Test the comparison and matching logic"""
        print(f"\n🔍 STEP 4: TESTING COMPARISON LOGIC")
        print("=" * 40)
        
        # Test patent number matching
        self._test_patent_matching()
        
        # Test person matching
        self._test_person_matching()
        
        # Test fuzzy matching
        self._test_fuzzy_matching()
    
    def _test_patent_matching(self):
        """Test patent number normalization and matching"""
        print("\n📋 Testing Patent Number Matching:")
        
        # Get sample patent numbers from XML
        xml_patents = []
        for filename, analysis in self.xml_analysis.items():
            for patent in analysis.get('sample_patents', [])[:5]:
                if patent.get('patent_number'):
                    xml_patents.append(patent['patent_number'])
        
        if not xml_patents:
            print("   ⚠️  No XML patent numbers found to test")
            return
        
        print(f"   📊 Testing with {len(self.existing_patents)} CSV patents and {len(xml_patents)} XML patents")
        
        # Test normalization
        print("   🔧 Patent number normalization test:")
        test_cases = list(self.existing_patents)[:5] + xml_patents[:5]
        
        for patent_num in test_cases:
            normalized = self._normalize_patent_number(patent_num)
            print(f"      '{patent_num}' → '{normalized}'")
        
        # Test matching
        xml_normalized = set(filter(None, [self._normalize_patent_number(p) for p in xml_patents]))
        matches = self.existing_patents.intersection(xml_normalized)
        print(f"   🎯 Found {len(matches)} potential matches between CSV and XML patents")
        
        if matches:
            print(f"      Sample matches: {list(matches)[:3]}")
    
    def _test_person_matching(self):
        """Test person name matching logic"""
        print("\n👥 Testing Person Name Matching:")
        
        # Get sample people from XML
        xml_people = []
        for filename, analysis in self.xml_analysis.items():
            for patent in analysis.get('sample_patents', []):
                for inventor in patent.get('inventors', [])[:3]:
                    if inventor.get('first_name') and inventor.get('last_name'):
                        xml_people.append(inventor)
        
        if not xml_people:
            print("   ⚠️  No XML people names found to test")
            return
        
        print(f"   📊 Testing with {len(self.existing_people)} CSV people and {len(xml_people)} XML people")
        
        # Test person identifier creation
        print("   🔧 Person identifier test:")
        test_people = list(self.existing_people)[:3] + [
            self._create_person_identifier(p.get('first_name', ''), p.get('last_name', ''))
            for p in xml_people[:3]
        ]
        
        for person_id in test_people:
            if person_id:
                print(f"      → '{person_id}'")
        
        # Test exact matching
        xml_ids = set()
        for person in xml_people:
            person_id = self._create_person_identifier(
                person.get('first_name', ''),
                person.get('last_name', '')
            )
            if person_id:
                xml_ids.add(person_id)
        
        exact_matches = self.existing_people.intersection(xml_ids)
        print(f"   🎯 Found {len(exact_matches)} exact matches between CSV and XML people")
        
        if exact_matches:
            print(f"      Sample matches: {list(exact_matches)[:3]}")
    
    def _test_fuzzy_matching(self):
        """Test fuzzy string matching for names"""
        print("\n🔍 Testing Fuzzy Name Matching:")
        
        if not FUZZY_AVAILABLE:
            print("   ⚠️  fuzzywuzzy not available - using basic string comparison")
            test_pairs = [
                ("John", "Jon"),
                ("Smith", "Smyth"),
                ("Catherine", "Katherine")
            ]
            
            print("   🔧 Basic string matching test:")
            for name1, name2 in test_pairs:
                similarity = self._basic_similarity(name1, name2)
                match_status = "✅ SIMILAR" if similarity > 0.8 else "❌ DIFFERENT"
                print(f"      '{name1}' vs '{name2}': {similarity:.2f} {match_status}")
        else:
            test_pairs = [
                ("John", "Jon"),
                ("Smith", "Smyth"),
                ("Catherine", "Katherine"),
                ("McDonald", "MacDonald"),
                ("Robert Johnson", "Bob Johnson"),
                ("Mary K. Smith", "Mary Smith"),
                ("completely different", "names here")
            ]
            
            print("   🔧 Fuzzy matching test cases:")
            for name1, name2 in test_pairs:
                similarity = fuzz.ratio(name1.lower(), name2.lower())
                match_status = "✅ MATCH" if similarity >= 85 else "❌ NO MATCH"
                print(f"      '{name1}' vs '{name2}': {similarity}% {match_status}")
    
    def _basic_similarity(self, str1: str, str2: str) -> float:
        """Basic string similarity without fuzzywuzzy"""
        str1, str2 = str1.lower(), str2.lower()
        if str1 == str2:
            return 1.0
        
        # Simple character overlap calculation
        set1, set2 = set(str1), set(str2)
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        
        return intersection / union if union > 0 else 0.0
    
    def run_comparison_test(self):
        """Run actual comparison test with real data"""
        print(f"\n🔍 STEP 5: RUNNING REAL DATA COMPARISON TEST")
        print("=" * 40)
        
        if not self.csv_analysis or not self.xml_analysis:
            print("❌ Need both CSV and XML data for comparison test")
            return
        
        try:
            # Parse a small sample of XML data
            main_xml = next((f for f in self.found_files['all_xml_files'] if 'ipg250812.xml' in f), None)
            if not main_xml:
                print("❌ No main XML file found for testing")
                return
            
            print(f"📄 Processing sample from: {os.path.basename(main_xml)}")
            xml_patents = self._parse_xml_sample(main_xml, max_patents=50)
            
            if not xml_patents:
                print("❌ No XML patents parsed")
                return
            
            print(f"✅ Parsed {len(xml_patents)} sample patents")
            
            # Run comparison
            results = self._compare_xml_vs_csv(xml_patents)
            
            # Display results
            print(f"\n📊 COMPARISON TEST RESULTS")
            print("=" * 30)
            print(f"🗃️  Existing patents in CSV: {len(self.existing_patents):,}")
            print(f"👥 Existing people in CSV: {len(self.existing_people):,}")
            print(f"📄 XML patents tested: {len(xml_patents):,}")
            print(f"🆕 New patents found: {results['new_patents']:,}")
            print(f"👤 New people found: {results['new_people']:,}")
            print(f"📈 Match rate: {results['match_rate']:.1f}%")
            print(f"💰 Estimated cost savings: ${results['cost_savings']:.2f}")
            print(f"🎯 Exact matches: {results['exact_matches']:,}")
            print(f"🔍 Fuzzy matches: {results['fuzzy_matches']:,}")
            
        except Exception as e:
            print(f"❌ Comparison test failed: {e}")
    
    def _parse_xml_sample(self, xml_file: str, max_patents: int = 50) -> list:
        """Parse a small sample of XML patents"""
        patents = []
        
        try:
            # Read first part of file
            with open(xml_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read(2000000)  # First 2MB
            
            xml_docs = content.split('<?xml version="1.0" encoding="UTF-8"?>')
            
            for doc in xml_docs[1:max_patents+1]:
                try:
                    full_doc = '<?xml version="1.0" encoding="UTF-8"?>' + doc
                    root = ET.fromstring(full_doc)
                    
                    patent = self._extract_patent_data(root)
                    if patent:
                        patents.append(patent)
                        
                except ET.XMLSyntaxError:
                    continue
                except Exception:
                    continue
        
        except Exception as e:
            print(f"   ❌ Error parsing XML: {e}")
        
        return patents
    
    def _extract_patent_data(self, root):
        """Extract patent data from XML"""
        try:
            patent_number = self._get_xml_text(root, './/document-id/doc-number')
            patent_title = self._get_xml_text(root, './/invention-title')
            
            if not patent_number:
                return None
            
            inventors = []
            inventor_paths = [
                './/parties/inventors/inventor',
                './/us-parties/inventors/inventor'
            ]
            
            for path in inventor_paths:
                inventor_elements = root.findall(path)
                if inventor_elements:
                    break
            
            for inventor in inventor_elements:
                inventor_data = {
                    'first_name': self._get_xml_text(inventor, './/first-name'),
                    'last_name': self._get_xml_text(inventor, './/last-name'),
                    'city': self._get_xml_text(inventor, './/city'),
                    'state': self._get_xml_text(inventor, './/state')
                }
                
                if inventor_data['first_name'] or inventor_data['last_name']:
                    inventors.append(inventor_data)
            
            return {
                'patent_number': patent_number,
                'patent_title': patent_title,
                'inventors': inventors,
                'total_people': len(inventors)
            }
            
        except Exception:
            return None
    
    def _compare_xml_vs_csv(self, xml_patents: list) -> dict:
        """Compare XML patents against CSV data"""
        new_patents = 0
        new_people = 0
        exact_matches = 0
        fuzzy_matches = 0
        total_xml_people = 0
        
        for patent in xml_patents:
            # Check patent
            patent_num = self._normalize_patent_number(patent.get('patent_number'))
            if patent_num not in self.existing_patents:
                new_patents += 1
            
            # Check people
            for inventor in patent.get('inventors', []):
                total_xml_people += 1
                
                # Exact match
                person_id = self._create_person_identifier(
                    inventor.get('first_name', ''),
                    inventor.get('last_name', '')
                )
                
                if person_id in self.existing_people:
                    exact_matches += 1
                else:
                    # Try fuzzy match
                    if self._fuzzy_person_match(inventor):
                        fuzzy_matches += 1
                    else:
                        new_people += 1
        
        match_rate = ((exact_matches + fuzzy_matches) / total_xml_people * 100) if total_xml_people > 0 else 0
        cost_savings = (exact_matches + fuzzy_matches) * 0.03
        
        return {
            'new_patents': new_patents,
            'new_people': new_people,
            'exact_matches': exact_matches,
            'fuzzy_matches': fuzzy_matches,
            'match_rate': match_rate,
            'cost_savings': cost_savings,
            'total_xml_people': total_xml_people
        }
    
    def _fuzzy_person_match(self, xml_person: dict) -> bool:
        """Check if XML person fuzzy matches any existing person"""
        if not FUZZY_AVAILABLE:
            return False
        
        xml_name = f"{xml_person.get('first_name', '')}_{xml_person.get('last_name', '')}".lower()
        
        for existing_person_id in list(self.existing_people)[:100]:  # Check sample
            similarity = fuzz.ratio(xml_name, existing_person_id)
            if similarity >= 85:
                return True
        
        return False
    
    def _normalize_patent_number(self, patent_num: str) -> str:
        """Normalize patent number"""
        if not patent_num or str(patent_num).lower() in ['nan', 'none', 'null']:
            return None
        
        clean = str(patent_num).strip().upper()
        clean = re.sub(r'^(US|USPTO|US-|D)', '', clean)
        clean = re.sub(r'[^\d]', '', clean)
        clean = clean.lstrip('0')
        
        if clean and len(clean) >= 4 and clean.isdigit():
            return clean
        return None
    
    def _create_person_identifier(self, first_name: str, last_name: str) -> str:
        """Create person identifier"""
        first = str(first_name).lower().strip() if first_name else ''
        last = str(last_name).lower().strip() if last_name else ''
        
        if not first and not last:
            return ''
        
        return f"{first}_{last}".strip('_')
    
    def generate_recommendations(self):
        """Generate recommendations based on analysis"""
        print(f"\n🎯 STEP 6: RECOMMENDATIONS")
        print("=" * 40)
        
        print("📊 Configuration Recommendations:")
        if self.found_files['csv_folders']:
            best_csv_folder = self.found_files['csv_folders'][0]
            print(f"   CSV_FOLDER = \"{best_csv_folder}\"")
        
        if self.found_files['all_xml_files']:
            main_xml = next((f for f in self.found_files['all_xml_files'] if 'ipg250812.xml' in f), 
                           self.found_files['all_xml_files'][0])
            print(f"   XML_SOURCE = \"{main_xml}\"")
        
        print(f"\n💡 Data Quality Recommendations:")
        csv_quality_issues = []
        for filename, analysis in self.csv_analysis.items():
            quality = analysis.get('data_quality', {})
            if quality.get('completeness', 1) < 0.8:
                csv_quality_issues.append(f"{filename}: {quality['completeness']:.1%} complete")
        
        if csv_quality_issues:
            print("   ⚠️  CSV Data Quality Issues:")
            for issue in csv_quality_issues[:3]:
                print(f"      • {issue}")
        
        print(f"\n🔧 Matching Strategy Recommendations:")
        if self.csv_analysis and self.xml_analysis:
            print("   ✅ Use enhanced comparison with fuzzy matching")
            if FUZZY_AVAILABLE:
                print("   ✅ Set fuzzy matching threshold to 85%")
            else:
                print("   💡 Install fuzzywuzzy for better matching: pip install fuzzywuzzy")
            print("   ✅ Include location data in person matching")
        
        # Performance recommendations
        total_csv_rows = sum(a.get('file_info', {}).get('rows', 0) for a in self.csv_analysis.values())
        total_xml_patents = sum(a.get('patent_count', 0) for a in self.xml_analysis.values())
        
        print(f"\n⚡ Performance Recommendations:")
        print(f"   📊 Total CSV rows: {total_csv_rows:,}")
        print(f"   📄 Total XML patents: {total_xml_patents:,}")
        
        if total_csv_rows > 100000:
            print("   💡 Large CSV dataset - consider processing in chunks")
        if total_xml_patents > 10000:
            print("   💡 Large XML dataset - consider limiting initial test runs")
        
        # Cost estimation
        estimated_people = total_xml_patents * 2.5
        estimated_api_cost = estimated_people * 0.03
        print(f"   💰 Estimated enrichment cost without deduplication: ${estimated_api_cost:.2f}")
    
    def save_analysis_report(self):
        """Save comprehensive analysis report"""
        print(f"\n💾 STEP 7: SAVING ANALYSIS REPORT")
        print("=" * 40)
        
        report = {
            'analysis_timestamp': datetime.now().isoformat(),
            'files_found': self.found_files,
            'csv_analysis': self.csv_analysis,
            'xml_analysis': self.xml_analysis,
            'summary': {
                'csv_files_analyzed': len(self.csv_analysis),
                'xml_files_analyzed': len(self.xml_analysis),
                'total_csv_rows': sum(a.get('file_info', {}).get('rows', 0) for a in self.csv_analysis.values()),
                'total_xml_patents': sum(a.get('patent_count', 0) for a in self.xml_analysis.values()),
                'existing_patents_found': len(self.existing_patents),
                'existing_people_found': len(self.existing_people)
            }
        }
        
        # Save to output folder
        output_dir = Path("../output")
        output_dir.mkdir(exist_ok=True)
        
        report_file = output_dir / f"self_contained_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"📄 Detailed report saved: {report_file}")
        
        # Also save a simple summary
        summary_file = output_dir / "analysis_summary.txt"
        with open(summary_file, 'w') as f:
            f.write("SELF-CONTAINED DATA ANALYSIS SUMMARY\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"CSV Files: {len(self.csv_analysis)}\n")
            f.write(f"XML Files: {len(self.xml_analysis)}\n")
            f.write(f"Total CSV Rows: {report['summary']['total_csv_rows']:,}\n")
            f.write(f"Total XML Patents: {report['summary']['total_xml_patents']:,}\n")
            f.write(f"Existing Patents Found: {len(self.existing_patents):,}\n")
            f.write(f"Existing People Found: {len(self.existing_people):,}\n")
        
        print(f"📝 Summary saved: {summary_file}")
        print(f"\n✅ Self-contained analysis complete!")

def main():
    """Main function"""
    print("🕵️ SELF-CONTAINED DATA ANALYSIS TOOL")
    print("=" * 60)
    print("This tool analyzes your data structure and tests comparison logic")
    print("without requiring any external imports or dependencies.")
    print("\nChoose an option:")
    print("1. Full comprehensive analysis (recommended)")
    print("2. Data structure analysis only")
    print("3. Comparison test only")
    
    try:
        choice = input("\nEnter choice (1/2/3) or press Enter for full analysis: ").strip()
    except:
        choice = "1"
    
    analyzer = SelfContainedDataAnalyzer()
    
    if choice == "2":
        analyzer.find_data_files()
        analyzer.analyze_csv_databases()
        analyzer.analyze_xml_files()
        analyzer.generate_recommendations()
        analyzer.save_analysis_report()
    elif choice == "3":
        analyzer.find_data_files()
        analyzer.analyze_csv_databases()
        analyzer.analyze_xml_files()
        analyzer.test_comparison_matching()
        analyzer.run_comparison_test()
    else:
        analyzer.run_full_analysis()

if __name__ == "__main__":
    main()