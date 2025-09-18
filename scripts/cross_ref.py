#!/usr/bin/env python3
# =============================================================================
# cross_reference_field_updater.py - Cross-reference Access DB to update SQL missing fields
# Pull records from SQL, find missing fields in Access DB, update SQL table
# =============================================================================
import pandas as pd
import os
import logging
import subprocess
import mysql.connector
from pathlib import Path
import json
from datetime import datetime
import sys
import traceback
from io import StringIO
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'cross_reference_updater_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CrossReferenceFieldUpdater:
    """Cross-reference Access database to update missing SQL fields"""
    
    def __init__(self, database_folder_path: str = "../", progress_file: str = "field_update_progress.json"):
        self.database_folder = Path(database_folder_path)
        self.progress_file = Path(progress_file)
        self.batch_size = 500
        
        # SQL connection config from environment
        self.sql_config = {
            'host': os.getenv('DB_HOST', os.getenv('SQL_HOST', 'localhost')),
            'port': int(os.getenv('DB_PORT', os.getenv('SQL_PORT', '3306'))),
            'database': os.getenv('DB_NAME', os.getenv('SQL_DATABASE', 'patent_data')),
            'user': os.getenv('DB_USER', os.getenv('SQL_USER', 'root')),
            'password': os.getenv('DB_PASSWORD', os.getenv('SQL_PASSWORD', 'password')),
            'charset': 'utf8mb4',
            'sql_mode': ''  # Disable strict mode
        }
        
        # Fields to check and update (excluding problematic date field for now)
        self.target_fields = [
            'address', 'zip', 'issue_id', 'new_issue_rec_num', 'inventor_id', 
            'patent_no', 'mail_to_name', 'mail_to_send_key', 
            'mod_user', 'bar_code'
        ]
        
        # Date fields to handle separately
        self.date_fields = ['issue_date']
        
        # Progress tracking
        self.progress = {
            'last_processed_id': 0,
            'total_records_checked': 0,
            'total_records_updated': 0,
            'records_with_missing_fields': 0,
            'fields_found_and_updated': {},
            'start_time': None,
            'last_update_time': None
        }
        
        # Access database cache
        self.access_tables_cache = {}
        
        # Check tools
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

    def find_access_databases(self):
        """Find Access database files"""
        access_files = []
        
        for pattern in ["*.accdb", "*.mdb"]:
            for file_path in self.database_folder.rglob(pattern):
                if not file_path.name.startswith('~'):  # Skip temp files
                    access_files.append(file_path)
                    logger.info(f"Found Access database: {file_path}")
        
        return access_files

    def get_table_list(self, db_path: Path):
        """Get list of tables in Access database"""
        try:
            result = subprocess.run(['mdb-tables', str(db_path)], 
                                  capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                tables = [t.strip() for t in result.stdout.strip().split() if t.strip()]
                user_tables = [t for t in tables if not t.startswith('MSys') and t.strip()]
                return user_tables
            else:
                logger.error(f"mdb-tables failed for {db_path}: {result.stderr}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting tables for {db_path}: {e}")
            return []

    def export_access_table(self, db_path: Path, table_name: str):
        """Export table from Access database"""
        try:
            result = subprocess.run(['mdb-export', str(db_path), table_name], 
                                  capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0:
                csv_data = result.stdout
                if csv_data.strip():
                    df = pd.read_csv(StringIO(csv_data))
                    logger.debug(f"Exported {table_name}: {df.shape}")
                    return df
                else:
                    return None
            else:
                logger.error(f"mdb-export failed for {table_name}: {result.stderr}")
                return None
                
        except Exception as e:
            logger.error(f"Error exporting {table_name}: {e}")
            return None

    def load_csv_tables(self):
        """Load CSV files from converted databases into cache"""
        logger.info("Loading CSV files from converted databases...")
        
        csv_folder = self.database_folder / "converted_databases" / "csv"
        if not csv_folder.exists():
            logger.error(f"CSV folder not found: {csv_folder}")
            return False
        
        # Find all CSV files
        csv_files = list(csv_folder.glob("*.csv"))
        if not csv_files:
            logger.error(f"No CSV files found in {csv_folder}")
            return False
        
        logger.info(f"Found {len(csv_files)} CSV files")
        
        # Look for relevant CSV files that might contain our target fields
        relevant_patterns = [
            'inventor', 'new_issue', 'issue', 'matches', 'match', 'patent'
        ]
        
        for csv_file in csv_files:
            file_name_lower = csv_file.name.lower()
            
            # Check if this CSV might contain relevant data
            is_relevant = any(pattern in file_name_lower for pattern in relevant_patterns)
            
            if is_relevant:
                try:
                    logger.info(f"Loading CSV: {csv_file.name}")
                    df = pd.read_csv(csv_file)
                    
                    if not df.empty:
                        # Use filename (without extension) as cache key
                        cache_key = csv_file.stem
                        self.access_tables_cache[cache_key] = df
                        logger.info(f"Cached {cache_key}: {df.shape} - Columns: {list(df.columns)[:5]}...")
                    else:
                        logger.warning(f"Empty CSV file: {csv_file.name}")
                        
                except Exception as e:
                    logger.error(f"Failed to load {csv_file.name}: {e}")
            else:
                logger.debug(f"Skipping non-relevant CSV: {csv_file.name}")
        
        logger.info(f"Loaded {len(self.access_tables_cache)} CSV tables into cache")
        
        # Show what tables we have
        if self.access_tables_cache:
            logger.info("Available tables:")
            for key, df in self.access_tables_cache.items():
                logger.info(f"  {key}: {df.shape[0]:,} rows, {df.shape[1]} columns")
        
        return len(self.access_tables_cache) > 0

    def load_progress(self):
        """Load progress from file"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    saved_progress = json.load(f)
                    self.progress.update(saved_progress)
                logger.info(f"Loaded progress: last_processed_id = {self.progress['last_processed_id']}")
            except Exception as e:
                logger.warning(f"Could not load progress: {e}")

    def save_progress(self):
        """Save progress to file"""
        self.progress['last_update_time'] = datetime.now().isoformat()
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save progress: {e}")

    def connect_sql(self):
        """Connect to SQL database"""
        try:
            connection = mysql.connector.connect(**self.sql_config)
            
            # Set SQL mode to handle problematic dates
            cursor = connection.cursor()
            cursor.execute("SET SESSION sql_mode = 'ALLOW_INVALID_DATES'")
            cursor.execute("SET SESSION foreign_key_checks = 0")
            cursor.close()
            
            logger.info(f"Connected to SQL database: {self.sql_config['database']}")
            return connection
        except Exception as e:
            logger.error(f"SQL connection failed: {e}")
            return None

    def get_records_with_missing_fields(self, connection, limit=None):
        """Get records that have missing fields - avoiding problematic date fields"""
        # Simple query avoiding date fields entirely
        query = """
        SELECT id, first_name, last_name, city, state, country,
               address, zip, issue_id, new_issue_rec_num, inventor_id, 
               patent_no, mail_to_name, mail_to_send_key, mod_user, bar_code
        FROM existing_people 
        WHERE id > %s
        ORDER BY id
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, (self.progress['last_processed_id'],))
        records = cursor.fetchall()
        cursor.close()
        
        # Filter to only records that actually have missing fields
        filtered_records = []
        for record in records:
            missing_fields = self.identify_missing_fields(record)
            if missing_fields:
                filtered_records.append(record)
        
        logger.info(f"Found {len(filtered_records)} records with missing fields out of {len(records)} total")
        return filtered_records

    def identify_missing_fields(self, record):
        """Identify which fields are missing for a record"""
        missing_fields = []
        for field in self.target_fields:
            value = record.get(field)
            # Consider empty strings, NULL, '0000-00-00' dates as missing
            if (not value or 
                value == '' or 
                value == '0000-00-00' or 
                str(value).lower() in ['null', 'none']):
                missing_fields.append(field)
        return missing_fields

    def search_access_for_fields(self, record, missing_fields):
        """Search Access tables for missing field values"""
        found_fields = {}
        
        # Create search criteria
        search_criteria = {
            'first_name': record.get('first_name', ''),
            'last_name': record.get('last_name', ''),
            'city': record.get('city', ''),
            'state': record.get('state', ''),
        }
        
        # Field mappings from Access to SQL
        field_mappings = {
            'address': ['inventor_add1', 'inventor_address', 'address', 'addr1'],
            'zip': ['inventor_zip', 'zip', 'postal_code'],
            'issue_id': ['issue_id', 'id'],
            'new_issue_rec_num': ['new_issue_rec_num', 'issue_rec_num', 'rec_num'],
            'inventor_id': ['inventor_id', 'id'],
            'patent_no': ['patent_num', 'patent_number', 'patent_no'],
            'issue_date': ['issue_date', 'patent_date', 'date'],
            'mail_to_name': ['mail_to_name', 'inventor_first', 'inventor_last'],
            'mail_to_send_key': ['mail_to_send_key', 'send_key'],
            'mod_user': ['mod_user', 'modified_by', 'last_modified_by'],
            'bar_code': ['bar_code', 'barcode']
        }
        
        # Search through cached Access tables
        for table_key, df in self.access_tables_cache.items():
            if df.empty:
                continue
                
            # Try to find matching record
            matches = self.find_matching_records(df, search_criteria)
            
            if not matches.empty:
                logger.debug(f"Found {len(matches)} matches in {table_key}")
                
                # Extract field values
                for missing_field in missing_fields:
                    if missing_field in found_fields:
                        continue  # Already found this field
                    
                    possible_columns = field_mappings.get(missing_field, [missing_field])
                    
                    for col_name in possible_columns:
                        # Find matching column (case insensitive)
                        actual_col = None
                        for actual in df.columns:
                            if col_name.lower() == actual.lower():
                                actual_col = actual
                                break
                        
                        if actual_col and actual_col in matches.columns:
                            # Get the first non-null, non-empty value
                            values = matches[actual_col].dropna()
                            values = values[values != '']
                            
                            if not values.empty:
                                found_value = str(values.iloc[0]).strip()
                                if found_value and found_value.lower() != 'null':
                                    found_fields[missing_field] = found_value
                                    logger.debug(f"Found {missing_field} = {found_value} in {table_key}.{actual_col}")
                                    break
                    
                    if missing_field in found_fields:
                        break  # Move to next missing field
        
        return found_fields

    def find_matching_records(self, df, search_criteria):
        """Find records in DataFrame matching search criteria"""
        mask = pd.Series([True] * len(df))
        
        # Try exact match first
        for field, value in search_criteria.items():
            if not value:
                continue
                
            # Find matching column (case insensitive)
            matching_cols = []
            possible_names = [field, f'inventor_{field}']
            
            for possible in possible_names:
                for col in df.columns:
                    if possible.lower() == col.lower():
                        matching_cols.append(col)
                        break
            
            if matching_cols:
                col = matching_cols[0]
                mask &= df[col].astype(str).str.lower() == str(value).lower()
        
        exact_matches = df[mask]
        
        if not exact_matches.empty:
            return exact_matches
        
        # Try partial match (state + last name)
        mask = pd.Series([True] * len(df))
        
        if search_criteria.get('last_name') and search_criteria.get('state'):
            last_name = search_criteria['last_name']
            state = search_criteria['state']
            
            # Find last name column
            for col in df.columns:
                if 'last' in col.lower():
                    mask &= df[col].astype(str).str.lower() == last_name.lower()
                    break
            
            # Find state column
            for col in df.columns:
                if 'state' in col.lower():
                    mask &= df[col].astype(str).str.lower() == state.lower()
                    break
            
            return df[mask]
        
        return pd.DataFrame()

    def update_sql_record(self, connection, record_id, found_fields):
        """Update SQL record with found field values"""
        if not found_fields:
            return False
        
        # Build UPDATE query
        set_clauses = []
        values = []
        
        for field, value in found_fields.items():
            set_clauses.append(f"{field} = %s")
            # Clean and format value
            cleaned_value = self.clean_field_value(field, value)
            values.append(cleaned_value)
        
        set_clauses.append("updated_at = NOW()")
        
        query = f"""
        UPDATE existing_people 
        SET {', '.join(set_clauses)}
        WHERE id = %s
        """
        
        values.append(record_id)
        
        try:
            cursor = connection.cursor()
            cursor.execute(query, values)
            connection.commit()
            cursor.close()
            
            # Update progress tracking
            for field in found_fields.keys():
                if field not in self.progress['fields_found_and_updated']:
                    self.progress['fields_found_and_updated'][field] = 0
                self.progress['fields_found_and_updated'][field] += 1
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update record {record_id}: {e}")
            connection.rollback()
            return False

    def clean_field_value(self, field_name, value):
        """Clean and format field value"""
        if not value or str(value).lower() in ['null', 'none', '']:
            return None
        
        value = str(value).strip()
        
        # Date fields
        if 'date' in field_name.lower():
            try:
                # Handle various date formats and empty dates
                if value in ['', '0000-00-00', '00/00/0000', 'NULL']:
                    return None
                date = pd.to_datetime(value)
                if pd.isna(date):
                    return None
                return date.strftime('%Y-%m-%d')
            except:
                return None
        
        # Integer fields
        if field_name in ['issue_id', 'new_issue_rec_num', 'inventor_id']:
            try:
                if value in ['', 'NULL', '0']:
                    return None
                return int(float(value))
            except:
                return None
        
        # String fields - limit length
        if field_name in ['patent_no', 'bar_code']:
            return value[:50] if value else None
        elif field_name in ['address']:
            return value[:255] if value else None
        elif field_name in ['zip']:
            return value[:20] if value else None
        else:
            return value[:100] if value else None

    def process_batch(self, connection):
        """Process a batch of records"""
        logger.info(f"Getting batch of {self.batch_size} records starting from ID {self.progress['last_processed_id']}")
        
        records = self.get_records_with_missing_fields(connection, self.batch_size)
        
        if not records:
            logger.info("No more records to process")
            return False
        
        logger.info(f"Processing {len(records)} records with missing fields")
        
        batch_updated = 0
        batch_checked = 0
        
        for record in records:
            batch_checked += 1
            record_id = record['id']
            
            # Identify missing fields
            missing_fields = self.identify_missing_fields(record)
            
            if missing_fields:
                self.progress['records_with_missing_fields'] += 1
                logger.debug(f"Record {record_id}: Missing fields: {missing_fields}")
                
                # Search Access databases
                found_fields = self.search_access_for_fields(record, missing_fields)
                
                if found_fields:
                    logger.info(f"Record {record_id}: Found fields: {list(found_fields.keys())}")
                    
                    # Update SQL record
                    if self.update_sql_record(connection, record_id, found_fields):
                        batch_updated += 1
                        self.progress['total_records_updated'] += 1
                    else:
                        logger.error(f"Failed to update record {record_id}")
                else:
                    logger.debug(f"Record {record_id}: No fields found in Access databases")
            
            # Update progress
            self.progress['last_processed_id'] = record_id
            self.progress['total_records_checked'] += 1
            
            # Log progress every 50 records
            if batch_checked % 50 == 0:
                logger.info(f"Batch progress: {batch_checked}/{len(records)} checked, {batch_updated} updated")
        
        # Save progress after each batch
        self.save_progress()
        
        logger.info(f"Batch complete: {batch_checked} checked, {batch_updated} updated")
        return True

    def run(self):
        """Main execution method"""
        logger.info("Starting cross-reference field updater")
        
        # Load progress
        self.load_progress()
        
        if not self.progress['start_time']:
            self.progress['start_time'] = datetime.now().isoformat()
        
        # Load CSV tables instead of Access databases
        if not self.load_csv_tables():
            logger.error("Failed to load CSV tables")
            return False
        
        # Connect to SQL
        connection = self.connect_sql()
        if not connection:
            logger.error("Failed to connect to SQL database")
            return False
        
        try:
            # Process batches until no more records
            while True:
                if not self.process_batch(connection):
                    break
                
                # Small delay between batches
                time.sleep(1)
            
            # Final report
            self.generate_final_report()
            
        except KeyboardInterrupt:
            logger.info("Process interrupted by user")
            self.save_progress()
        except Exception as e:
            logger.error(f"Process failed: {e}")
            logger.error(traceback.format_exc())
        finally:
            connection.close()
            logger.info("Database connection closed")
        
        return True

    def generate_final_report(self):
        """Generate final processing report"""
        logger.info("\n" + "="*50)
        logger.info("CROSS-REFERENCE UPDATE COMPLETE")
        logger.info("="*50)
        logger.info(f"Total records checked: {self.progress['total_records_checked']:,}")
        logger.info(f"Records with missing fields: {self.progress['records_with_missing_fields']:,}")
        logger.info(f"Records updated: {self.progress['total_records_updated']:,}")
        logger.info(f"Last processed ID: {self.progress['last_processed_id']:,}")
        
        if self.progress['fields_found_and_updated']:
            logger.info("\nFields found and updated:")
            for field, count in self.progress['fields_found_and_updated'].items():
                logger.info(f"  {field}: {count:,}")
        
        # Calculate processing time
        if self.progress['start_time']:
            start_time = datetime.fromisoformat(self.progress['start_time'])
            elapsed = datetime.now() - start_time
            logger.info(f"\nTotal processing time: {elapsed}")
        
        logger.info("="*50)

def main():
    """Main function"""
    print("Cross-Reference Access to SQL Field Updater")
    print("===========================================")
    print("This script will:")
    print("1. Read records from SQL existing_people table")
    print("2. Identify missing fields")
    print("3. Search Access databases for those fields")
    print("4. Update SQL records with found values")
    print("5. Process in batches with resumable progress")
    print()
    
    # Configuration
    DATABASE_FOLDER = "../"  # Where Access databases are located
    PROGRESS_FILE = "field_update_progress.json"
    
    try:
        updater = CrossReferenceFieldUpdater(DATABASE_FOLDER, PROGRESS_FILE)
        success = updater.run()
        
        if success:
            print("\nCross-reference update completed!")
            print(f"Check the log file for detailed results.")
            print(f"Progress saved in: {PROGRESS_FILE}")
        else:
            print("Cross-reference update failed. Check the log for details.")
    
    except Exception as e:
        logger.error(f"Script failed: {e}")
        logger.error(traceback.format_exc())
        print(f"Script failed: {e}")

if __name__ == "__main__":
    main()