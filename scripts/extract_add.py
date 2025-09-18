#!/usr/bin/env python3
# =============================================================================
# extract_address_phone.py - Extract address and phone fields from Access DB
# Extract inventor_add1 and inventor_phone fields from Access databases for SQL upload
# =============================================================================
import pandas as pd
import os
import logging
import subprocess
from pathlib import Path
import json
from datetime import datetime
import sys
import traceback
from io import StringIO

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'address_phone_extraction_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AddressPhoneExtractor:
    """Extract inventor_add1 and inventor_phone fields from Access databases"""
    
    def __init__(self, database_folder_path: str, output_folder: str = "address_phone_data"):
        self.database_folder = Path(database_folder_path)
        self.output_folder = Path(output_folder)
        self.output_folder.mkdir(exist_ok=True)
        
        # Check if mdb-tools is available
        self.check_mdb_tools()
    
    def check_mdb_tools(self):
        """Check if mdb-tools is available"""
        logger.info("Checking mdb-tools availability")
        
        try:
            result = subprocess.run(['mdb-ver'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                logger.info(f"mdb-tools available: {result.stdout.strip()}")
                return True
            else:
                logger.warning("mdb-ver returned non-zero code, but continuing...")
                return True
        except FileNotFoundError:
            logger.error("mdb-tools not installed")
            logger.info("Install with: brew install mdb-tools")
            return False
        except Exception as e:
            logger.warning(f"mdb-tools version check failed: {e}, but continuing...")
            return True

    def find_target_databases(self):
        """Find the target database files"""
        target_patterns = [
            "uspc_patent_data.*",
            "uspc_new_issue.*",
            "*patent*",
            "*issue*"
        ]
        
        found_databases = {}
        
        for pattern in target_patterns:
            for file_path in self.database_folder.rglob(pattern):
                if file_path.suffix.lower() in ['.accdb', '.mdb']:
                    db_type = self.classify_database(file_path.name)
                    if db_type not in found_databases:
                        found_databases[db_type] = file_path
                        logger.info(f"Found database: {file_path} (type: {db_type})")
        
        return found_databases

    def classify_database(self, filename):
        """Classify database type based on filename"""
        filename_lower = filename.lower()
        if 'patent_data' in filename_lower:
            return 'patent_data'
        elif 'new_issue' in filename_lower or 'issue' in filename_lower:
            return 'new_issue'
        elif 'patent' in filename_lower:
            return 'patent_data'
        else:
            return 'unknown'

    def get_table_list(self, db_path: Path):
        """Get list of tables in the database"""
        try:
            logger.info(f"Getting table list for {db_path.name}")
            result = subprocess.run(['mdb-tables', str(db_path)], 
                                  capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                tables = [t.strip() for t in result.stdout.strip().split() if t.strip()]
                user_tables = [t for t in tables if not t.startswith('MSys') and t.strip()]
                logger.info(f"Found tables: {user_tables}")
                return user_tables
            else:
                logger.error(f"mdb-tables failed: {result.stderr}")
                return []
                
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout getting tables for {db_path.name}")
            return []
        except Exception as e:
            logger.error(f"Error getting tables for {db_path.name}: {e}")
            return []

    def export_table(self, db_path: Path, table_name: str):
        """Export a table from the database"""
        try:
            logger.info(f"Exporting '{table_name}' table from {db_path.name}")
            
            result = subprocess.run(['mdb-export', str(db_path), table_name], 
                                  capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0:
                csv_data = result.stdout
                if csv_data.strip():
                    df = pd.read_csv(StringIO(csv_data))
                    logger.info(f"Successfully exported {table_name} table: {df.shape}")
                    logger.info(f"Columns found: {list(df.columns)}")
                    return df
                else:
                    logger.error("Table returned empty data")
                    return None
            else:
                logger.error(f"mdb-export failed: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout exporting {table_name} table")
            return None
        except Exception as e:
            logger.error(f"Error exporting {table_name} table: {e}")
            return None

    def find_relevant_tables(self, db_path: Path):
        """Find tables that might contain the fields we need"""
        tables = self.get_table_list(db_path)
        relevant_tables = []
        
        # Tables likely to contain inventor_add1, inventor_phone
        priority_table_names = [
            'inventor', 'inventors', 
            'new_issue', 'issue', 'issues',
            'patent', 'patents',
            'matches', 'match'
        ]
        
        for table in tables:
            table_lower = table.lower()
            for priority in priority_table_names:
                if priority in table_lower:
                    relevant_tables.append(table)
                    break
        
        # If no priority tables found, include all non-system tables
        if not relevant_tables:
            relevant_tables = tables
        
        logger.info(f"Relevant tables to check: {relevant_tables}")
        return relevant_tables

    def extract_inventor_data(self, db_path: Path):
        """Extract inventor data from database"""
        logger.info(f"Processing database: {db_path.name}")
        
        relevant_tables = self.find_relevant_tables(db_path)
        all_records = []
        
        for table_name in relevant_tables:
            df = self.export_table(db_path, table_name)
            
            if df is None or df.empty:
                continue
            
            # Look for our target fields and matching criteria
            records = self.extract_fields_from_table(df, table_name, db_path.name)
            if records:
                all_records.extend(records)
        
        return all_records

    def extract_fields_from_table(self, df, table_name, db_name):
        """Extract address and phone fields from a table"""
        records = []
        
        # Map columns to standard names (case insensitive matching)
        column_mapping = self.map_columns(df.columns)
        
        # Check if we have the fields we need
        has_target_fields = any(field in column_mapping.values() for field in ['address', 'phone'])
        has_identity_fields = any(field in column_mapping.values() for field in ['first_name', 'last_name', 'city', 'state'])
        
        if not (has_target_fields and has_identity_fields):
            logger.info(f"Table {table_name} doesn't have required fields, skipping")
            return records
        
        logger.info(f"Processing {len(df)} records from {table_name}")
        
        # Rename columns to standard names
        df_renamed = df.rename(columns=column_mapping)
        
        for _, row in df_renamed.iterrows():
            # Only include records that have identity information and at least one target field
            if self.has_valid_identity(row) and self.has_target_data(row):
                record = {
                    'source_database': db_name,
                    'source_table': table_name,
                    'first_name': self.clean_string(row.get('first_name', '')),
                    'last_name': self.clean_string(row.get('last_name', '')),
                    'city': self.clean_string(row.get('city', '')),
                    'state': self.clean_string(row.get('state', '')),
                    'country': self.clean_string(row.get('country', '')),
                    'address': self.clean_string(row.get('address', '')),
                    'phone': self.clean_string(row.get('phone', '')),
                    # Additional context fields
                    'patent_number': self.clean_string(row.get('patent_number', '')),
                    'issue_date': self.parse_date(row.get('issue_date')),
                    'inventor_id': self.clean_int(row.get('inventor_id')),
                    'location_id': self.clean_int(row.get('location_id')),
                }
                
                records.append(record)
        
        logger.info(f"Extracted {len(records)} records with target fields from {table_name}")
        return records

    def map_columns(self, columns):
        """Map actual column names to standard field names"""
        column_mapping = {}
        available_cols = list(columns)
        
        # Define field mappings - what we want vs what might be in the database
        field_mappings = {
            'address': ['inventor_add1', 'inventor_address', 'address', 'addr1', 'address1'],
            'phone': ['inventor_phone', 'phone', 'phone_number', 'telephone'],
            'first_name': ['inventor_first', 'first_name', 'fname'],
            'last_name': ['inventor_last', 'last_name', 'lname'],
            'city': ['inventor_city', 'city'],
            'state': ['inventor_state', 'state'],
            'country': ['inventor_country', 'country'],
            'patent_number': ['patent_num', 'patent_number', 'patent_no'],
            'issue_date': ['issue_date', 'patent_date', 'date'],
            'inventor_id': ['inventor_id', 'id'],
            'location_id': ['location_id', 'loc_id'],
        }
        
        for standard_name, possible_names in field_mappings.items():
            for possible in possible_names:
                for actual_col in available_cols:
                    if possible.lower() == actual_col.lower():
                        column_mapping[actual_col] = standard_name
                        break
                if standard_name in column_mapping.values():
                    break
        
        return column_mapping

    def has_valid_identity(self, row):
        """Check if record has enough identity information"""
        first_name = self.clean_string(row.get('first_name', ''))
        last_name = self.clean_string(row.get('last_name', ''))
        state = self.clean_string(row.get('state', ''))
        
        return bool(first_name and last_name and state)

    def has_target_data(self, row):
        """Check if record has at least one of our target fields with data"""
        address = self.clean_string(row.get('address', ''))
        phone = self.clean_string(row.get('phone', ''))
        
        return bool(address or phone)

    def clean_string(self, value):
        """Clean string value"""
        if pd.isna(value) or str(value).lower() in ['null', 'none', '']:
            return ''
        return str(value).strip()

    def clean_int(self, value):
        """Clean integer value"""
        if pd.isna(value) or str(value).lower() in ['null', 'none', '']:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def parse_date(self, date_str):
        """Parse date string to proper format"""
        if pd.isna(date_str) or str(date_str).lower() == 'null':
            return None
        
        try:
            date = pd.to_datetime(date_str)
            return date.strftime('%Y-%m-%d')
        except:
            return None

    def create_update_csv(self, all_records):
        """Create CSV file for SQL updates"""
        if not all_records:
            logger.error("No records to process")
            return None
        
        # Create DataFrame
        update_df = pd.DataFrame(all_records)
        
        # Remove duplicates based on identity fields, keeping the record with most data
        update_df['data_completeness'] = (
            update_df['address'].apply(lambda x: 1 if x else 0) +
            update_df['phone'].apply(lambda x: 1 if x else 0)
        )
        
        # Sort by completeness and drop duplicates
        update_df = update_df.sort_values('data_completeness', ascending=False)
        update_df = update_df.drop_duplicates(
            subset=['first_name', 'last_name', 'city', 'state'], 
            keep='first'
        )
        
        # Drop the helper column
        update_df = update_df.drop('data_completeness', axis=1)
        
        # Save the CSV
        output_file = self.output_folder / "address_phone_updates.csv"
        update_df.to_csv(output_file, index=False)
        
        logger.info(f"Created address/phone update CSV: {output_file}")
        logger.info(f"Records to update: {len(update_df)}")
        
        # Create metadata
        metadata = {
            'extraction_date': datetime.now().isoformat(),
            'total_records': len(update_df),
            'records_with_address': len(update_df[update_df['address'] != '']),
            'records_with_phone': len(update_df[update_df['phone'] != '']),
            'source_databases': list(update_df['source_database'].unique()),
            'source_tables': list(update_df['source_table'].unique()),
            'columns_extracted': list(update_df.columns),
            'output_file': str(output_file)
        }
        
        metadata_file = self.output_folder / "address_phone_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Created metadata file: {metadata_file}")
        
        # Show summary
        logger.info(f"Summary:")
        logger.info(f"  Records with address: {metadata['records_with_address']}")
        logger.info(f"  Records with phone: {metadata['records_with_phone']}")
        
        return output_file

    def extract_all_address_phone_fields(self):
        """Main method to extract all address and phone fields"""
        logger.info("Starting address/phone fields extraction")
        
        # Find databases
        databases = self.find_target_databases()
        if not databases:
            logger.error("No target databases found")
            return False
        
        all_records = []
        
        # Extract from each database
        for db_type, db_path in databases.items():
            logger.info(f"Processing {db_type} database: {db_path}")
            records = self.extract_inventor_data(db_path)
            if records:
                all_records.extend(records)
                logger.info(f"Found {len(records)} records in {db_type}")
        
        if not all_records:
            logger.error("No data extracted from any database")
            return False
        
        # Create update CSV
        output_file = self.create_update_csv(all_records)
        if output_file is None:
            logger.error("Failed to create update CSV")
            return False
        
        logger.info("Address/phone fields extraction completed successfully")
        return True

def main():
    """Main function to run the address/phone fields extraction"""
    
    # Configuration
    DATABASE_FOLDER = "../patent_system"  # Folder containing your .mdb/.accdb files
    OUTPUT_FOLDER = "../address_phone_data"  # Where to save the CSV file
    
    print("Starting Address/Phone Fields Extraction")
    print(f"Looking for databases in: {DATABASE_FOLDER}")
    print(f"Output will be saved to: {OUTPUT_FOLDER}")
    
    # Check if source folder exists
    if not os.path.exists(DATABASE_FOLDER):
        print(f"Error: Database folder '{DATABASE_FOLDER}' not found!")
        print("Please update the DATABASE_FOLDER variable to point to your Access database files.")
        return
    
    try:
        # Create extractor and run extraction
        extractor = AddressPhoneExtractor(DATABASE_FOLDER, OUTPUT_FOLDER)
        success = extractor.extract_all_address_phone_fields()
        
        if success:
            print("\nExtraction completed! Check the output folder for your CSV file.")
            print("Files created:")
            print(f"   - address_phone_updates.csv")
            print(f"   - address_phone_metadata.json")
            print("\nNext step: Run the JavaScript upload script to update your SQL database")
        else:
            print("Extraction failed. Check the log file for details.")
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        print(f"Extraction failed: {e}")

if __name__ == "__main__":
    main()