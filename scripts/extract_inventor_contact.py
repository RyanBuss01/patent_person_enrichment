#!/usr/bin/env python3
# =============================================================================
# extract_inventor_contact_v2.py - Extract inventor_contact field from Access DB
# Extract the missing inventor_contact field and create CSV for SQL upload
# FIXED VERSION with case-insensitive table matching
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
        logging.FileHandler(f'inventor_contact_extraction_v2_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class InventorContactExtractor:
    """Extract inventor_contact field from Access database"""
    
    def __init__(self, database_folder_path: str, output_folder: str = "inventor_contact_data"):
        self.database_folder = Path(database_folder_path)
        self.output_folder = Path(output_folder)
        self.output_folder.mkdir(exist_ok=True)
        
        # Check if mdb-tools is available (but don't fail if version check fails)
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
                return True  # Continue anyway, mdb-tools might still work
        except FileNotFoundError:
            logger.error("❌ mdb-tools not installed")
            logger.info("   Install with: brew install mdb-tools")
            return False
        except Exception as e:
            logger.warning(f"⚠️ mdb-tools version check failed: {e}, but continuing...")
            return True  # Continue anyway

    def find_target_database(self):
        """Find the uspc_patent_data.accdb file"""
        target_file = "uspc_patent_data.accdb"
        
        # Look for the specific file
        target_path = self.database_folder / target_file
        if target_path.exists():
            logger.info(f"✅ Found target database: {target_path}")
            return target_path
        
        # Also check for .mdb version
        target_mdb = self.database_folder / "uspc_patent_data.mdb"
        if target_mdb.exists():
            logger.info(f"✅ Found target database (MDB): {target_mdb}")
            return target_mdb
        
        # Look in subdirectories
        for file_path in self.database_folder.rglob("uspc_patent_data.*"):
            if file_path.suffix.lower() in ['.accdb', '.mdb']:
                logger.info(f"✅ Found target database in subdirectory: {file_path}")
                return file_path
        
        logger.error(f"❌ Could not find {target_file} in {self.database_folder}")
        return None

    def get_table_list(self, db_path: Path):
        """Get list of tables in the database"""
        try:
            logger.info(f"Getting table list for {db_path.name}")
            result = subprocess.run(['mdb-tables', str(db_path)], 
                                  capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                tables = [t.strip() for t in result.stdout.strip().split() if t.strip()]
                # Filter out system tables
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

    def export_inventor_table(self, db_path: Path, table_name: str):
        """Export the inventor table with all fields including inventor_contact"""
        try:
            logger.info(f"🔄 Exporting '{table_name}' table from {db_path.name}")
            
            # Export the table
            result = subprocess.run(['mdb-export', str(db_path), table_name], 
                                  capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0:
                csv_data = result.stdout
                if csv_data.strip():
                    df = pd.read_csv(StringIO(csv_data))
                    logger.info(f"✅ Successfully exported {table_name} table: {df.shape}")
                    logger.info(f"📊 Columns found: {list(df.columns)}")
                    
                    # Check if inventor_contact column exists
                    if 'inventor_contact' in df.columns:
                        logger.info("✅ inventor_contact column found in data")
                        # Show some sample values
                        sample_values = df['inventor_contact'].value_counts()
                        logger.info(f"📊 inventor_contact values: {dict(sample_values)}")
                    else:
                        logger.warning("⚠️ inventor_contact column not found in exported data")
                        logger.info(f"Available columns: {list(df.columns)}")
                    
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

    def create_contact_update_csv(self, inventor_df):
        """Create a CSV file for updating inventor_contact in SQL database"""
        
        if inventor_df is None or inventor_df.empty:
            logger.error("❌ No inventor data to process")
            return None
        
        logger.info("🔄 Processing inventor data for SQL update")
        
        # Check what identification columns we have
        available_cols = list(inventor_df.columns)
        logger.info(f"📊 Available columns: {available_cols}")
        
        # Common identifier columns to look for (case insensitive)
        id_columns = ['id', 'inventor_id', 'person_id', 'first_name', 'last_name', 'fname', 'lname']
        identifier_cols = []
        
        for id_col in id_columns:
            for actual_col in available_cols:
                if id_col.lower() == actual_col.lower():
                    identifier_cols.append(actual_col)
                    break
        
        if not identifier_cols:
            logger.error("❌ No identifier columns found")
            logger.error(f"Available columns: {available_cols}")
            return None
        
        logger.info(f"📋 Using identifier columns: {identifier_cols}")
        
        # Check if inventor_contact exists (case insensitive)
        contact_col = None
        for col in available_cols:
            if 'inventor_contact' in col.lower() or 'contact' in col.lower():
                contact_col = col
                break
        
        if not contact_col:
            logger.warning("⚠️ inventor_contact column not found, creating default values")
            inventor_df['inventor_contact'] = True  # Default to True as mentioned
            contact_col = 'inventor_contact'
        
        # Create the update CSV with minimal required columns
        update_columns = identifier_cols + [contact_col]
        update_df = inventor_df[update_columns].copy()
        
        # Rename contact column to standardized name
        if contact_col != 'inventor_contact':
            update_df = update_df.rename(columns={contact_col: 'inventor_contact'})
        
        # Clean the data
        update_df = update_df.dropna(subset=identifier_cols)
        
        # Convert inventor_contact to boolean if it's not already
        if 'inventor_contact' in update_df.columns:
            def convert_to_bool(value):
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
                        return True  # Default to True for unknown values
                return True  # Default to True
            
            update_df['inventor_contact'] = update_df['inventor_contact'].apply(convert_to_bool)
        
        # Save the CSV
        output_file = self.output_folder / "inventor_contact_updates.csv"
        update_df.to_csv(output_file, index=False)
        
        logger.info(f"✅ Created inventor contact update CSV: {output_file}")
        logger.info(f"📊 Records to update: {len(update_df)}")
        
        # Show sample data
        if len(update_df) > 0:
            logger.info("📄 Sample records:")
            for i, row in update_df.head(3).iterrows():
                sample_info = []
                for col in identifier_cols:
                    if col in row and pd.notna(row[col]):
                        sample_info.append(f"{col}={row[col]}")
                contact_status = "✓" if row['inventor_contact'] else "✗"
                logger.info(f"   {contact_status} {', '.join(sample_info)}")
        
        # Show contact statistics
        contact_stats = update_df['inventor_contact'].value_counts()
        logger.info(f"📊 Contact statistics: {dict(contact_stats)}")
        
        # Create metadata file
        metadata = {
            'extraction_date': datetime.now().isoformat(),
            'source_database': 'uspc_patent_data.accdb',
            'source_table': 'Inventor',
            'total_records': len(update_df),
            'identifier_columns': identifier_cols,
            'contact_true_count': int(update_df['inventor_contact'].sum()),
            'contact_false_count': int((~update_df['inventor_contact']).sum()),
            'output_file': str(output_file)
        }
        
        metadata_file = self.output_folder / "inventor_contact_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"✅ Created metadata file: {metadata_file}")
        
        return output_file

    def extract_inventor_contact_data(self):
        """Main method to extract inventor_contact data"""
        logger.info("🚀 Starting inventor_contact extraction")
        
        # Find the target database
        db_path = self.find_target_database()
        if not db_path:
            logger.error("❌ Target database not found")
            return False
        
        # Get table list to find 'inventor' table (case insensitive)
        tables = self.get_table_list(db_path)
        
        if not tables:
            logger.error("❌ No tables found in database")
            return False
        
        # Look for inventor table (case insensitive)
        inventor_table = None
        for table in tables:
            logger.info(f"Checking table: '{table}' (lowercase: '{table.lower()}')")
            if table.lower() == 'inventor':
                inventor_table = table
                logger.info(f"✅ FOUND MATCH: '{table}' matches 'inventor'")
                break
        
        if not inventor_table:
            logger.error("❌ 'inventor' table not found in database")
            logger.info(f"Available tables: {tables}")
            # Let's also check if there's any table with 'inventor' in the name
            possible_tables = [t for t in tables if 'inventor' in t.lower()]
            if possible_tables:
                logger.info(f"Tables containing 'inventor': {possible_tables}")
            return False
        
        logger.info(f"✅ Found inventor table: '{inventor_table}'")
        
        # Export inventor table
        inventor_df = self.export_inventor_table(db_path, inventor_table)
        if inventor_df is None:
            logger.error("❌ Failed to export inventor table")
            return False
        
        # Create update CSV
        output_file = self.create_contact_update_csv(inventor_df)
        if output_file is None:
            logger.error("❌ Failed to create update CSV")
            return False
        
        logger.info("✅ Inventor contact extraction completed successfully")
        return True

def main():
    """Main function to run the inventor contact extraction"""
    
    # Configuration - go up one folder since script is in 'scripts' folder
    DATABASE_FOLDER = "../patent_system"  # Folder containing your .mdb/.accdb files
    OUTPUT_FOLDER = "../inventor_contact_data"  # Where to save the CSV file
    
    print("🚀 Starting Inventor Contact Field Extraction (V2 - FIXED)")
    print(f"📁 Looking for database in: {DATABASE_FOLDER}")
    print(f"📁 Output will be saved to: {OUTPUT_FOLDER}")
    
    # Check if source folder exists
    if not os.path.exists(DATABASE_FOLDER):
        print(f"❌ Error: Database folder '{DATABASE_FOLDER}' not found!")
        print("Please update the DATABASE_FOLDER variable to point to your Access database files.")
        return
    
    try:
        # Create extractor and run extraction
        extractor = InventorContactExtractor(DATABASE_FOLDER, OUTPUT_FOLDER)
        success = extractor.extract_inventor_contact_data()
        
        if success:
            print("\n🎉 Extraction completed! Check the output folder for your CSV file.")
            print("📄 Files created:")
            print(f"   - inventor_contact_updates.csv")
            print(f"   - inventor_contact_metadata.json")
            print("\n📋 Next step: Run the JavaScript upload script to update your SQL database")
        else:
            print("❌ Extraction failed. Check the log file for details.")
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        print(f"❌ Extraction failed: {e}")

if __name__ == "__main__":
    main()