#!/usr/bin/env python3
# =============================================================================
# extract_additional_fields.py - Extract additional inventor fields from Access DB
# Extract missing fields from both uspc_patent_data.accdb and uspc_new_issue.accdb
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
        logging.FileHandler(f'additional_fields_extraction_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AdditionalFieldsExtractor:
    """Extract additional inventor fields from Access databases"""
    
    def __init__(self, database_folder_path: str, output_folder: str = "additional_fields_data"):
        self.database_folder = Path(database_folder_path)
        self.output_folder = Path(output_folder)
        self.output_folder.mkdir(exist_ok=True)
        
        # Check if mdb-tools is available
        self.check_mdb_tools()
    
    def check_mdb_tools(self):
        """Check if mdb-tools is available"""
        logger.info("🔍 CHECKING MDB-TOOLS AVAILABILITY")
        
        try:
            result = subprocess.run(['mdb-ver'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                logger.info(f"✅ mdb-tools available: {result.stdout.strip()}")
                return True
            else:
                logger.warning("⚠️ mdb-ver returned non-zero code, but continuing...")
                return True
        except FileNotFoundError:
            logger.error("❌ mdb-tools not installed")
            logger.info("   Install with: brew install mdb-tools")
            return False
        except Exception as e:
            logger.warning(f"⚠️ mdb-tools version check failed: {e}, but continuing...")
            return True

    def find_target_databases(self):
        """Find the target database files"""
        target_files = [
            "uspc_patent_data.accdb",
            "uspc_patent_data.mdb",
            "uspc_new_issue.accdb",
            "uspc_new_issue.mdb"
        ]
        
        found_databases = {}
        
        for target_file in target_files:
            target_path = self.database_folder / target_file
            if target_path.exists():
                logger.info(f"✅ Found target database: {target_path}")
                db_type = "patent_data" if "patent_data" in target_file else "new_issue"
                found_databases[db_type] = target_path
                continue
                
            # Look in subdirectories
            for file_path in self.database_folder.rglob(target_file):
                logger.info(f"✅ Found target database in subdirectory: {file_path}")
                db_type = "patent_data" if "patent_data" in target_file else "new_issue"
                found_databases[db_type] = file_path
                break
        
        if not found_databases:
            logger.error("❌ Could not find any target databases")
        
        return found_databases

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
            logger.info(f"🔄 Exporting '{table_name}' table from {db_path.name}")
            
            result = subprocess.run(['mdb-export', str(db_path), table_name], 
                                  capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0:
                csv_data = result.stdout
                if csv_data.strip():
                    df = pd.read_csv(StringIO(csv_data))
                    logger.info(f"✅ Successfully exported {table_name} table: {df.shape}")
                    logger.info(f"📊 Columns found: {list(df.columns)[:10]}...")
                    return df
                else:
                    logger.error("❌ Table returned empty data")
                    return None
            else:
                logger.error(f"❌ mdb-export failed: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            logger.error(f"❌ Timeout exporting {table_name} table")
            return None
        except Exception as e:
            logger.error(f"❌ Error exporting {table_name} table: {e}")
            return None

    def extract_inventor_data(self, patent_db_path: Path):
        """Extract inventor data from patent database"""
        logger.info("🔄 Processing Inventor table from patent database")
        
        tables = self.get_table_list(patent_db_path)
        
        # Find inventor table (case insensitive)
        inventor_table = None
        for table in tables:
            if table.lower() == 'inventor':
                inventor_table = table
                break
        
        if not inventor_table:
            logger.error("❌ 'Inventor' table not found")
            return None
        
        df = self.export_table(patent_db_path, inventor_table)
        
        if df is None or df.empty:
            logger.error("❌ No inventor data found")
            return None
        
        # Map columns to standard names (case insensitive matching)
        column_mapping = {}
        available_cols = list(df.columns)
        
        # Define field mappings - what we want vs what might be in the database
        field_mappings = {
            'inventor_id': ['inventor_id', 'id'],
            'inventor_first': ['inventor_first', 'first_name', 'fname'],
            'inventor_last': ['inventor_last', 'last_name', 'lname'],
            'inventor_contact': ['inventor_contact', 'contact'],
            'inventor_add1': ['inventor_add1', 'address', 'addr1'],
            'inventor_add2': ['inventor_add2', 'addr2'],
            'inventor_city': ['inventor_city', 'city'],
            'inventor_state': ['inventor_state', 'state'],
            'inventor_zip': ['inventor_zip', 'zip'],
            'inventor_country': ['inventor_country', 'country'],
            'inventor_phone': ['inventor_phone', 'phone'],
            'inventor_email': ['inventor_email', 'email'],
            'assign_id': ['assign_id'],
            'location_id': ['location_id'],
            'send_to': ['send_to'],
            'mail_stop': ['mail_stop'],
            'assign_name': ['assign_name'],
            'mod_user': ['mod_user', 'modified_by', 'last_modified_by']
        }
        
        for standard_name, possible_names in field_mappings.items():
            for possible in possible_names:
                for actual_col in available_cols:
                    if possible.lower() == actual_col.lower():
                        column_mapping[actual_col] = standard_name
                        break
                if standard_name in column_mapping.values():
                    break
        
        logger.info(f"📊 Column mappings found: {column_mapping}")
        
        # Rename columns
        df_renamed = df.rename(columns=column_mapping)
        
        return df_renamed

    def extract_new_issue_data(self, new_issue_db_path: Path):
        """Extract data from new issue database"""
        logger.info("🔄 Processing New_Issue table from new issue database")
        
        tables = self.get_table_list(new_issue_db_path)
        
        # Find New_Issue table (case insensitive)
        new_issue_table = None
        for table in tables:
            if 'new' in table.lower() and 'issue' in table.lower():
                new_issue_table = table
                break
        
        if not new_issue_table:
            logger.error("❌ 'New_Issue' table not found")
            logger.info(f"Available tables: {tables}")
            return None
        
        df = self.export_table(new_issue_db_path, new_issue_table)
        
        if df is None or df.empty:
            logger.error("❌ No new issue data found")
            return None
        
        # Map columns to standard names
        column_mapping = {}
        available_cols = list(df.columns)
        
        field_mappings = {
            'new_issue_rec_num': ['new_issue_rec_num', 'issue_rec_num', 'rec_num'],
            'issue_id': ['issue_id'],
            'patent_num': ['patent_num', 'patent_no', 'patent_number'],
            'title': ['title', 'patent_title'],
            'issue_date': ['issue_date', 'date'],
            'inventor_first': ['inventor_first', 'first_name', 'fname'],
            'inventor_last': ['inventor_last', 'last_name', 'lname'],
            'inventor_contact': ['inventor_contact', 'contact'],
            'inventor_id': ['inventor_id'],
            'bar_code': ['bar_code', 'barcode'],
            'mod_user': ['mod_user', 'modified_by', 'last_modified_by']
        }
        
        for standard_name, possible_names in field_mappings.items():
            for possible in possible_names:
                for actual_col in available_cols:
                    if possible.lower() == actual_col.lower():
                        column_mapping[actual_col] = standard_name
                        break
                if standard_name in column_mapping.values():
                    break
        
        logger.info(f"📊 Column mappings found: {column_mapping}")
        
        # Rename columns
        df_renamed = df.rename(columns=column_mapping)
        
        return df_renamed

    def merge_and_create_update_csv(self, inventor_df, new_issue_df):
        """Merge data and create CSV for SQL updates"""
        logger.info("🔄 Merging data and creating update CSV")
        
        combined_records = []
        
        # Process inventor data
        if inventor_df is not None:
            for _, row in inventor_df.iterrows():
                record = {
                    'data_source': 'inventor_table',
                    'inventor_id': row.get('inventor_id'),
                    'inventor_first': row.get('inventor_first', ''),
                    'inventor_last': row.get('inventor_last', ''),
                    'inventor_contact': self.convert_to_bool(row.get('inventor_contact', True)),
                    'address': row.get('inventor_add1', ''),
                    'address2': row.get('inventor_add2', ''),
                    'city': row.get('inventor_city', ''),
                    'state': row.get('inventor_state', ''),
                    'zip': row.get('inventor_zip', ''),
                    'country': row.get('inventor_country', ''),
                    'phone': row.get('inventor_phone', ''),
                    'email': row.get('inventor_email', ''),
                    'assign_id': row.get('assign_id'),
                    'location_id': row.get('location_id'),
                    'send_to': row.get('send_to', ''),
                    'mail_stop': row.get('mail_stop', ''),
                    'assign_name': row.get('assign_name', ''),
                    'mod_user': row.get('mod_user', ''),
                }
                
                # Clean empty values
                record = {k: (v if pd.notna(v) and v != '' else None) for k, v in record.items()}
                combined_records.append(record)
        
        # Process new issue data
        if new_issue_df is not None:
            for _, row in new_issue_df.iterrows():
                record = {
                    'data_source': 'new_issue_table',
                    'issue_id': row.get('issue_id'),
                    'new_issue_rec_num': row.get('new_issue_rec_num'),
                    'inventor_id': row.get('inventor_id'),
                    'patent_no': row.get('patent_num', ''),
                    'title': row.get('title', ''),
                    'issue_date': self.parse_date(row.get('issue_date')),
                    'inventor_first': row.get('inventor_first', ''),
                    'inventor_last': row.get('inventor_last', ''),
                    'inventor_contact': self.convert_to_bool(row.get('inventor_contact', True)),
                    'bar_code': row.get('bar_code', ''),
                    'mod_user': row.get('mod_user', ''),
                }
                
                # Clean empty values
                record = {k: (v if pd.notna(v) and v != '' else None) for k, v in record.items()}
                combined_records.append(record)
        
        if not combined_records:
            logger.error("❌ No records to process")
            return None
        
        # Create DataFrame
        update_df = pd.DataFrame(combined_records)
        
        # Save the CSV
        output_file = self.output_folder / "additional_fields_updates.csv"
        update_df.to_csv(output_file, index=False)
        
        logger.info(f"✅ Created additional fields update CSV: {output_file}")
        logger.info(f"📊 Records to update: {len(update_df)}")
        
        # Create metadata
        metadata = {
            'extraction_date': datetime.now().isoformat(),
            'source_databases': ['uspc_patent_data.accdb', 'uspc_new_issue.accdb'],
            'total_records': len(update_df),
            'inventor_records': len([r for r in combined_records if r['data_source'] == 'inventor_table']),
            'new_issue_records': len([r for r in combined_records if r['data_source'] == 'new_issue_table']),
            'columns_extracted': list(update_df.columns),
            'output_file': str(output_file)
        }
        
        metadata_file = self.output_folder / "additional_fields_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"✅ Created metadata file: {metadata_file}")
        
        return output_file

    def convert_to_bool(self, value):
        """Convert various formats to boolean"""
        if pd.isna(value):
            return True  # Default to True
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            value = value.lower().strip()
            if value in ['true', 'yes', '1', 'y', 'on', '-1']:
                return True
            elif value in ['false', 'no', '0', 'n', 'off']:
                return False
            else:
                return True
        return True

    def parse_date(self, date_str):
        """Parse date string to proper format"""
        if pd.isna(date_str) or str(date_str).lower() == 'null':
            return None
        
        try:
            date = pd.to_datetime(date_str)
            return date.strftime('%Y-%m-%d')
        except:
            return None

    def extract_all_additional_fields(self):
        """Main method to extract all additional fields"""
        logger.info("🚀 Starting additional fields extraction")
        
        # Find databases
        databases = self.find_target_databases()
        if not databases:
            logger.error("❌ No target databases found")
            return False
        
        inventor_df = None
        new_issue_df = None
        
        # Extract from patent data database
        if 'patent_data' in databases:
            inventor_df = self.extract_inventor_data(databases['patent_data'])
        
        # Extract from new issue database
        if 'new_issue' in databases:
            new_issue_df = self.extract_new_issue_data(databases['new_issue'])
        
        if inventor_df is None and new_issue_df is None:
            logger.error("❌ No data extracted from any database")
            return False
        
        # Create update CSV
        output_file = self.merge_and_create_update_csv(inventor_df, new_issue_df)
        if output_file is None:
            logger.error("❌ Failed to create update CSV")
            return False
        
        logger.info("✅ Additional fields extraction completed successfully")
        return True

def main():
    """Main function to run the additional fields extraction"""
    
    # Configuration - go up one folder since script is in 'scripts' folder
    DATABASE_FOLDER = "../patent_system"  # Folder containing your .mdb/.accdb files
    OUTPUT_FOLDER = "../additional_fields_data"  # Where to save the CSV file
    
    print("🚀 Starting Additional Fields Extraction")
    print(f"📁 Looking for databases in: {DATABASE_FOLDER}")
    print(f"📁 Output will be saved to: {OUTPUT_FOLDER}")
    
    # Check if source folder exists
    if not os.path.exists(DATABASE_FOLDER):
        print(f"❌ Error: Database folder '{DATABASE_FOLDER}' not found!")
        print("Please update the DATABASE_FOLDER variable to point to your Access database files.")
        return
    
    try:
        # Create extractor and run extraction
        extractor = AdditionalFieldsExtractor(DATABASE_FOLDER, OUTPUT_FOLDER)
        success = extractor.extract_all_additional_fields()
        
        if success:
            print("\n🎉 Extraction completed! Check the output folder for your CSV file.")
            print("📄 Files created:")
            print(f"   - additional_fields_updates.csv")
            print(f"   - additional_fields_metadata.json")
            print("\n📋 Next step: Run the JavaScript upload script to update your SQL database")
        else:
            print("❌ Extraction failed. Check the log file for details.")
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        print(f"❌ Extraction failed: {e}")

if __name__ == "__main__":
    main()