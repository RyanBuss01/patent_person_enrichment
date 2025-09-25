#!/usr/bin/env python3
# =============================================================================
# optimized_cross_reference_field_updater.py - Fast cross-reference updater with batching and caching
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
from collections import defaultdict
import hashlib

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'optimized_updater_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OptimizedCrossReferenceUpdater:
    """Optimized cross-reference Access database to update missing SQL fields"""
    
    def __init__(self, database_folder_path: str = "../", progress_file: str = "optimized_update_progress.json"):
        self.database_folder = Path(database_folder_path)
        self.progress_file = Path(progress_file)
        self.batch_size = 2000  # Much larger batch size
        self.update_batch_size = 500  # SQL update batch size
        
        # SQL connection config from environment
        self.sql_config = {
            'host': os.getenv('DB_HOST', os.getenv('SQL_HOST', 'localhost')),
            'port': int(os.getenv('DB_PORT', os.getenv('SQL_PORT', '3306'))),
            'database': os.getenv('DB_NAME', os.getenv('SQL_DATABASE', 'patent_data')),
            'user': os.getenv('DB_USER', os.getenv('SQL_USER', 'root')),
            'password': os.getenv('DB_PASSWORD', os.getenv('SQL_PASSWORD', 'password')),
            'charset': 'utf8mb4',
            'sql_mode': '',
            'autocommit': False  # Manual commit for batching
        }
        
        # Updated fields to check and update
        self.target_fields = [
            'address', 'zip', 'issue_id', 'new_issue_rec_num', 'inventor_id', 
            'patent_no', 'mail_to_name', 'mail_to_send_key', 
            'mod_user', 'bar_code', 'issue_date', 'inventor_contact'
        ]
        
        # Progress tracking
        self.progress = {
            'last_processed_id': 0,
            'total_records_checked': 0,
            'total_records_updated': 0,
            'records_with_missing_fields': 0,
            'fields_found_and_updated': {},
            'start_time': None,
            'last_update_time': None,
            'cache_hits': 0,
            'cache_misses': 0
        }
        
        # Access database cache with optimized indexing
        self.access_tables_cache = {}
        self.lookup_cache = {}  # Cache for duplicate lookups
        self.indexed_tables = {}  # Pre-indexed tables for fast lookup
        
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

    def load_csv_tables_optimized(self):
        """Load and index CSV tables for fast lookup"""
        logger.info("Loading and indexing CSV files for optimized lookup...")
        
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
        
        # Look for relevant CSV files
        relevant_patterns = [
            'inventor', 'new_issue', 'issue', 'matches', 'match', 'patent'
        ]
        
        for csv_file in csv_files:
            file_name_lower = csv_file.name.lower()
            
            # Check if this CSV might contain relevant data
            is_relevant = any(pattern in file_name_lower for pattern in relevant_patterns)
            
            if is_relevant:
                try:
                    logger.info(f"Loading and indexing CSV: {csv_file.name}")
                    df = pd.read_csv(csv_file, low_memory=False)
                    
                    if not df.empty:
                        # Use filename (without extension) as cache key
                        cache_key = csv_file.stem
                        self.access_tables_cache[cache_key] = df
                        
                        # Create indexed version for fast lookup
                        self.create_table_index(cache_key, df)
                        
                        logger.info(f"Cached and indexed {cache_key}: {df.shape} - Columns: {list(df.columns)[:10]}...")
                    else:
                        logger.warning(f"Empty CSV file: {csv_file.name}")
                        
                except Exception as e:
                    logger.error(f"Failed to load {csv_file.name}: {e}")
            else:
                logger.debug(f"Skipping non-relevant CSV: {csv_file.name}")
        
        logger.info(f"Loaded and indexed {len(self.access_tables_cache)} CSV tables")
        return len(self.access_tables_cache) > 0

    def create_table_index(self, table_name, df):
        """Create optimized indexes for fast lookup"""
        if df.empty:
            return
            
        # Create indexes based on common lookup patterns
        indexes = {}
        
        # Find name columns
        name_columns = {
            'first_name': None,
            'last_name': None,
            'city': None,
            'state': None
        }
        
        for col in df.columns:
            col_lower = col.lower()
            if any(x in col_lower for x in ['first', 'fname']) and 'name' in col_lower:
                name_columns['first_name'] = col
            elif any(x in col_lower for x in ['last', 'lname']) and 'name' in col_lower:
                name_columns['last_name'] = col
            elif 'city' in col_lower:
                name_columns['city'] = col
            elif 'state' in col_lower:
                name_columns['state'] = col
        
        # Create lookup key index if we have the necessary columns
        if name_columns['first_name'] and name_columns['last_name']:
            logger.info(f"Creating lookup index for {table_name}")
            
            # Create multi-level index for fast lookup
            df_clean = df.copy()
            
            # Clean and normalize lookup columns
            for key, col in name_columns.items():
                if col:
                    df_clean[f'norm_{key}'] = df_clean[col].astype(str).str.strip().str.upper()
            
            # Group by lookup keys for fast access
            if name_columns['city'] and name_columns['state']:
                # Full match index: first_name + last_name + city + state
                full_key = df_clean.apply(lambda row: 
                    f"{row.get(f'norm_first_name', '')}__{row.get(f'norm_last_name', '')}__{row.get(f'norm_city', '')}__{row.get(f'norm_state', '')}", 
                    axis=1)
                indexes['full_match'] = df_clean.groupby(full_key).apply(lambda x: x.index.tolist()).to_dict()
                
                # State match index: first_name + last_name + state
                state_key = df_clean.apply(lambda row: 
                    f"{row.get(f'norm_first_name', '')}__{row.get(f'norm_last_name', '')}__{row.get(f'norm_state', '')}", 
                    axis=1)
                indexes['state_match'] = df_clean.groupby(state_key).apply(lambda x: x.index.tolist()).to_dict()
            
            # Name-only index: first_name + last_name
            name_key = df_clean.apply(lambda row: 
                f"{row.get(f'norm_first_name', '')}__{row.get(f'norm_last_name', '')}", 
                axis=1)
            indexes['name_match'] = df_clean.groupby(name_key).apply(lambda x: x.index.tolist()).to_dict()
            
            self.indexed_tables[table_name] = {
                'indexes': indexes,
                'columns': name_columns,
                'df': df
            }
        
        logger.debug(f"Created {len(indexes)} indexes for {table_name}")

    def create_lookup_key(self, first_name, last_name, city=None, state=None):
        """Create standardized lookup key"""
        # Normalize values
        first = str(first_name or '').strip().upper()
        last = str(last_name or '').strip().upper()
        city_norm = str(city or '').strip().upper()
        state_norm = str(state or '').strip().upper()
        
        if city and state:
            return f"{first}__{last}__{city_norm}__{state_norm}"
        elif state:
            return f"{first}__{last}__{state_norm}"
        else:
            return f"{first}__{last}"

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
        """Connect to SQL database with optimized settings"""
        try:
            connection = mysql.connector.connect(**self.sql_config)
            
            # Optimize connection settings
            cursor = connection.cursor()
            cursor.execute("SET SESSION sql_mode = 'ALLOW_INVALID_DATES'")
            cursor.execute("SET SESSION foreign_key_checks = 0")
            cursor.execute("SET SESSION unique_checks = 0")
            cursor.execute("SET SESSION autocommit = 0")  # Manual commits
            cursor.close()
            
            logger.info(f"Connected to SQL database with optimizations: {self.sql_config['database']}")
            return connection
        except Exception as e:
            logger.error(f"SQL connection failed: {e}")
            return None

    def get_records_batch_optimized(self, connection, limit):
        """Get batch of records with optimized query"""
        query = """
        SELECT id, first_name, last_name, city, state, country,
               address, zip, issue_id, new_issue_rec_num, inventor_id, 
               patent_no, mail_to_name, mail_to_send_key, mod_user, 
               bar_code, issue_date, inventor_contact
        FROM existing_people 
        WHERE id > %s
        ORDER BY id
        LIMIT %s
        """
        
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, (self.progress['last_processed_id'], limit))
        records = cursor.fetchall()
        cursor.close()
        
        return records

    def identify_missing_fields_batch(self, records):
        """Identify missing fields for a batch of records"""
        records_with_missing = []
        
        for record in records:
            missing_fields = []
            for field in self.target_fields:
                value = record.get(field)
                # Consider empty strings, NULL, '0000-00-00' dates as missing
                if (not value or 
                    value == '' or 
                    value == '0000-00-00' or 
                    str(value).lower() in ['null', 'none'] or
                    (field == 'inventor_contact' and value in [0, '0', None]) or
                    (field == 'issue_date' and value in ['0000-00-00 00:00:00', None])):
                    missing_fields.append(field)
            
            if missing_fields:
                record['_missing_fields'] = missing_fields
                records_with_missing.append(record)
        
        return records_with_missing

    def search_access_optimized(self, records_batch):
        """Optimized search using indexes and caching"""
        updates_needed = []
        
        # Group records by lookup key to avoid duplicate searches
        lookup_groups = defaultdict(list)
        
        for record in records_batch:
            lookup_key = self.create_lookup_key(
                record.get('first_name'), 
                record.get('last_name'),
                record.get('city'),
                record.get('state')
            )
            lookup_groups[lookup_key].append(record)
        
        logger.info(f"Processing {len(lookup_groups)} unique lookup keys for {len(records_batch)} records")
        
        # Process each unique lookup key
        for lookup_key, records_group in lookup_groups.items():
            # Check cache first
            if lookup_key in self.lookup_cache:
                found_fields = self.lookup_cache[lookup_key]
                self.progress['cache_hits'] += len(records_group)
            else:
                # Search in indexed tables
                found_fields = self.search_indexed_tables(records_group[0])  # Use first record as template
                self.lookup_cache[lookup_key] = found_fields
                self.progress['cache_misses'] += 1
            
            # Apply found fields to all records in this group
            if found_fields:
                for record in records_group:
                    # Only apply fields that are actually missing
                    applicable_fields = {
                        field: value for field, value in found_fields.items()
                        if field in record.get('_missing_fields', [])
                    }
                    
                    if applicable_fields:
                        updates_needed.append({
                            'id': record['id'],
                            'fields': applicable_fields
                        })
        
        return updates_needed

    def search_indexed_tables(self, record):
        """Search using pre-built indexes"""
        found_fields = {}
        
        # Field mappings from Access to SQL
        field_mappings = {
            'address': ['inventor_add1', 'inventor_address', 'address', 'addr1'],
            'zip': ['inventor_zip', 'zip', 'postal_code'],
            'issue_id': ['issue_id', 'id'],
            'new_issue_rec_num': ['new_issue_rec_num', 'issue_rec_num', 'rec_num'],
            'inventor_id': ['inventor_id', 'id'],
            'patent_no': ['patent_num', 'patent_number', 'patent_no'],
            'issue_date': ['inventor_created', 'issue_date', 'date_created', 'created_date'],
            'mail_to_name': ['mail_to_name', 'inventor_first', 'inventor_last'],
            'mail_to_send_key': ['mail_to_send_key', 'send_key'],
            'mod_user': ['mod_user', 'modified_by', 'last_modified_by'],
            'bar_code': ['bar_code', 'barcode'],
            'inventor_contact': ['inventor_contact', 'contact']
        }
        
        # Create lookup keys
        first_name = record.get('first_name', '')
        last_name = record.get('last_name', '')
        city = record.get('city', '')
        state = record.get('state', '')
        
        full_key = self.create_lookup_key(first_name, last_name, city, state)
        state_key = self.create_lookup_key(first_name, last_name, None, state)
        name_key = self.create_lookup_key(first_name, last_name)
        
        # Search indexed tables in priority order
        for table_name, table_info in self.indexed_tables.items():
            df = table_info['df']
            indexes = table_info['indexes']
            
            # Try full match first (highest priority)
            matching_indices = None
            if 'full_match' in indexes and full_key in indexes['full_match']:
                matching_indices = indexes['full_match'][full_key]
            elif 'state_match' in indexes and state_key in indexes['state_match']:
                matching_indices = indexes['state_match'][state_key]
            elif 'name_match' in indexes and name_key in indexes['name_match']:
                matching_indices = indexes['name_match'][name_key]
            
            if matching_indices:
                # Get matching records
                matches_df = df.iloc[matching_indices]
                
                # Extract field values
                for missing_field in record.get('_missing_fields', []):
                    if missing_field in found_fields:
                        continue  # Already found
                    
                    possible_columns = field_mappings.get(missing_field, [missing_field])
                    
                    for col_name in possible_columns:
                        # Find matching column (case insensitive)
                        actual_col = None
                        for actual in df.columns:
                            if col_name.lower() == actual.lower():
                                actual_col = actual
                                break
                        
                        if actual_col and actual_col in matches_df.columns:
                            # Get the first non-null, non-empty value
                            values = matches_df[actual_col].dropna()
                            values = values[values.astype(str) != '']
                            values = values[values.astype(str).str.lower() != 'null']
                            
                            if not values.empty:
                                found_value = str(values.iloc[0]).strip()
                                if found_value:
                                    found_fields[missing_field] = found_value
                                    break
                    
                    if missing_field in found_fields:
                        break  # Move to next missing field
        
        return found_fields

    def batch_update_sql(self, connection, updates_batch):
        """Batch update SQL records"""
        if not updates_batch:
            return 0
        
        # Group updates by field combinations for efficient batching
        update_groups = defaultdict(list)
        
        for update in updates_batch:
            field_signature = tuple(sorted(update['fields'].keys()))
            update_groups[field_signature].append(update)
        
        total_updated = 0
        
        try:
            cursor = connection.cursor()
            
            for field_signature, group_updates in update_groups.items():
                if not group_updates:
                    continue
                
                # Build batch UPDATE query
                set_clauses = [f"{field} = %s" for field in field_signature]
                set_clauses.append("updated_at = NOW()")
                
                query = f"""
                UPDATE existing_people 
                SET {', '.join(set_clauses)}
                WHERE id = %s
                """
                
                # Prepare batch data
                batch_data = []
                for update in group_updates:
                    values = []
                    for field in field_signature:
                        cleaned_value = self.clean_field_value(field, update['fields'][field])
                        values.append(cleaned_value)
                    values.append(update['id'])  # WHERE id = %s
                    batch_data.append(values)
                
                # Execute batch update
                cursor.executemany(query, batch_data)
                updated_count = cursor.rowcount
                total_updated += updated_count
                
                # Update progress tracking
                for field in field_signature:
                    if field not in self.progress['fields_found_and_updated']:
                        self.progress['fields_found_and_updated'][field] = 0
                    self.progress['fields_found_and_updated'][field] += updated_count
                
                logger.debug(f"Batch updated {updated_count} records with fields: {field_signature}")
            
            # Commit all updates
            connection.commit()
            cursor.close()
            
            logger.info(f"Successfully batch updated {total_updated} records")
            return total_updated
            
        except Exception as e:
            logger.error(f"Batch update failed: {e}")
            connection.rollback()
            return 0

    def clean_field_value(self, field_name, value):
        """Clean and format field value with enhanced date handling"""
        if not value or str(value).lower() in ['null', 'none', '']:
            return None
        
        value = str(value).strip()
        
        # Date fields - handle issue_date specially with multiple formats
        if field_name == 'issue_date':
            try:
                # Handle various date formats and empty dates
                if value in ['', '0000-00-00', '00/00/0000', 'NULL']:
                    return None
                
                # Handle MM/DD/YY format (like "01/04/22 00:00:00")
                if '/' in value and len(value.split('/')[2].split()[0]) == 2:
                    # Convert YY to 20YY
                    parts = value.replace('\\/', '/').split()
                    date_part = parts[0]  # Get just the date part
                    month, day, year = date_part.split('/')
                    
                    # Convert 2-digit year to 4-digit
                    if len(year) == 2:
                        year_int = int(year)
                        if year_int <= 30:  # Assume 00-30 means 2000-2030
                            year = f"20{year}"
                        else:  # Assume 31-99 means 1931-1999
                            year = f"19{year}"
                    
                    # Reconstruct date string
                    formatted_date = f"{month}/{day}/{year}"
                    date = pd.to_datetime(formatted_date)
                else:
                    # Try standard parsing
                    date = pd.to_datetime(value.replace('\\/', '/'))
                
                if pd.isna(date):
                    return None
                return date.strftime('%Y-%m-%d')
            except Exception as e:
                logger.debug(f"Date parsing failed for '{value}': {e}")
                return None
        
        # Boolean fields
        if field_name == 'inventor_contact':
            # Convert to boolean integer
            if str(value).lower() in ['true', '1', 'yes', 'y']:
                return 1
            else:
                return 0
        
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

    def process_batch_optimized(self, connection):
        """Process a batch with all optimizations"""
        logger.info(f"Getting batch of {self.batch_size} records starting from ID {self.progress['last_processed_id']}")
        
        # Get batch of records
        records = self.get_records_batch_optimized(connection, self.batch_size)
        
        if not records:
            logger.info("No more records to process")
            return False
        
        # Identify records with missing fields
        records_with_missing = self.identify_missing_fields_batch(records)
        
        if not records_with_missing:
            # Update progress even if no missing fields
            self.progress['last_processed_id'] = records[-1]['id']
            self.progress['total_records_checked'] += len(records)
            logger.info(f"Batch complete: {len(records)} records, 0 with missing fields")
            return True
        
        logger.info(f"Processing {len(records_with_missing)} records with missing fields out of {len(records)} total")
        
        # Search for missing fields using optimized lookup
        updates_needed = self.search_access_optimized(records_with_missing)
        
        # Batch update SQL
        updated_count = 0
        if updates_needed:
            # Process updates in smaller batches
            for i in range(0, len(updates_needed), self.update_batch_size):
                batch = updates_needed[i:i + self.update_batch_size]
                updated_count += self.batch_update_sql(connection, batch)
        
        # Update progress
        self.progress['last_processed_id'] = records[-1]['id']
        self.progress['total_records_checked'] += len(records)
        self.progress['records_with_missing_fields'] += len(records_with_missing)
        self.progress['total_records_updated'] += updated_count
        
        # Save progress
        self.save_progress()
        
        logger.info(f"Batch complete: {len(records)} checked, {len(records_with_missing)} with missing fields, {updated_count} updated")
        logger.info(f"Cache stats: {self.progress['cache_hits']} hits, {self.progress['cache_misses']} misses")
        
        return True

    def run(self):
        """Main execution method with all optimizations"""
        logger.info("Starting optimized cross-reference field updater (ROUND 2 - Focus on remaining fields)")
        logger.info("Primarily targeting: issue_date, inventor_contact, bar_code")
        
        # Load progress
        self.load_progress()
        
        if not self.progress['start_time']:
            self.progress['start_time'] = datetime.now().isoformat()
        
        # Load and index CSV tables
        if not self.load_csv_tables_optimized():
            logger.error("Failed to load CSV tables")
            return False
        
        # Connect to SQL
        connection = self.connect_sql()
        if not connection:
            logger.error("Failed to connect to SQL database")
            return False
        
        try:
            # Process batches until no more records
            batch_count = 0
            while True:
                batch_count += 1
                logger.info(f"Starting batch {batch_count}")
                
                if not self.process_batch_optimized(connection):
                    break
                
                # Small delay between batches (reduced)
                time.sleep(0.1)
            
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
        logger.info("\n" + "="*60)
        logger.info("OPTIMIZED CROSS-REFERENCE UPDATE COMPLETE")
        logger.info("="*60)
        logger.info(f"Total records checked: {self.progress['total_records_checked']:,}")
        logger.info(f"Records with missing fields: {self.progress['records_with_missing_fields']:,}")
        logger.info(f"Records updated: {self.progress['total_records_updated']:,}")
        logger.info(f"Last processed ID: {self.progress['last_processed_id']:,}")
        logger.info(f"Cache hits: {self.progress['cache_hits']:,}")
        logger.info(f"Cache misses: {self.progress['cache_misses']:,}")
        
        if self.progress['cache_hits'] + self.progress['cache_misses'] > 0:
            cache_hit_rate = self.progress['cache_hits'] / (self.progress['cache_hits'] + self.progress['cache_misses']) * 100
            logger.info(f"Cache hit rate: {cache_hit_rate:.1f}%")
        
        if self.progress['fields_found_and_updated']:
            logger.info("\nFields found and updated:")
            for field, count in sorted(self.progress['fields_found_and_updated'].items()):
                logger.info(f"  {field}: {count:,}")
        
        # Calculate processing time and rate
        if self.progress['start_time']:
            start_time = datetime.fromisoformat(self.progress['start_time'])
            elapsed = datetime.now() - start_time
            logger.info(f"\nTotal processing time: {elapsed}")
            
            if elapsed.total_seconds() > 0:
                rate = self.progress['total_records_checked'] / elapsed.total_seconds()
                logger.info(f"Processing rate: {rate:.1f} records/second")
        
        logger.info("="*60)

def main():
    """Main function"""
    print("Optimized Cross-Reference Access to SQL Field Updater")
    print("=====================================================")
    print("This optimized script includes:")
    print("• Larger batch processing (2000 records at a time)")
    print("• Pre-indexed table lookups for fast searches")
    print("• Duplicate detection and caching")
    print("• Batch SQL updates")
    print("• Enhanced progress tracking")
    print("• Support for issue_date, inventor_contact, and bar_code fields")
    print()
    
    # Configuration
    DATABASE_FOLDER = "../"  # Where Access databases are located
    PROGRESS_FILE = "optimized_update_progress.json"
    
    try:
        updater = OptimizedCrossReferenceUpdater(DATABASE_FOLDER, PROGRESS_FILE)
        success = updater.run()
        
        if success:
            print("\nOptimized cross-reference update completed!")
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