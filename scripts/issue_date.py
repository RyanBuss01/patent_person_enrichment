#!/usr/bin/env python3
# =============================================================================
# clean_issue_date_updater.py - With last_updated fallback
# =============================================================================
import pandas as pd
import os
import logging
import mysql.connector
from pathlib import Path
import json
from datetime import datetime
import sys
import traceback
from collections import defaultdict

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'clean_issue_date_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CleanIssueDateUpdater:
    """Clean implementation with last_updated fallback"""
    
    def __init__(self, database_folder_path: str = "../"):
        self.database_folder = Path(database_folder_path)
        self.batch_size = 50000
        self.offset = 0
        
        # SQL connection config
        self.sql_config = {
            'host': os.getenv('DB_HOST', os.getenv('SQL_HOST', 'localhost')),
            'port': int(os.getenv('DB_PORT', os.getenv('SQL_PORT', '3306'))),
            'database': os.getenv('DB_NAME', os.getenv('SQL_DATABASE', 'patent_data')),
            'user': os.getenv('DB_USER', os.getenv('SQL_USER', 'root')),
            'password': os.getenv('DB_PASSWORD', os.getenv('SQL_PASSWORD', 'password')),
            'charset': 'utf8mb4',
            'sql_mode': 'ALLOW_INVALID_DATES,NO_ZERO_DATE',
            'autocommit': False
        }
        
        self.json_folder = self.database_folder / "converted_databases" / "json"
        
        # Track source files that don't have issue_date OR last_updated
        self.invalid_source_files = set()

    def connect_sql(self):
        """Connect to SQL database"""
        try:
            connection = mysql.connector.connect(**self.sql_config)
            cursor = connection.cursor()
            cursor.execute("SET SESSION sql_mode = 'ALLOW_INVALID_DATES,NO_ZERO_DATE'")
            cursor.execute("SET SESSION autocommit = 0")
            cursor.close()
            logger.info(f"Connected to SQL database: {self.sql_config['database']}")
            return connection
        except Exception as e:
            logger.error(f"SQL connection failed: {e}")
            return None

    def get_null_issue_date_records(self, connection):
        """Get 50k records with NULL issue_date using LIMIT/OFFSET"""
        # Build exclusion clause for invalid source files
        exclusion_clause = ""
        params = [self.batch_size, self.offset]
        
        if self.invalid_source_files:
            placeholders = ",".join(["%s"] * len(self.invalid_source_files))
            exclusion_clause = f" AND source_file NOT IN ({placeholders})"
            params = list(self.invalid_source_files) + params
        
        query = f"""
        SELECT id, first_name, last_name, city, state, source_file
        FROM existing_people 
        WHERE issue_date IS NULL{exclusion_clause}
        ORDER BY id
        LIMIT %s OFFSET %s
        """
        
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, params)
        records = cursor.fetchall()
        cursor.close()
        
        return records

    def check_json_has_date_fields(self, source_file):
        """Check if JSON file has issue_date OR last_updated fields"""
        if source_file in self.invalid_source_files:
            return False
            
        json_filename = source_file.replace('.csv', '.json')
        json_path = self.json_folder / json_filename
        
        if not json_path.exists():
            logger.warning(f"JSON file not found: {json_filename}")
            self.invalid_source_files.add(source_file)
            return False
        
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not data:
                logger.warning(f"Empty JSON file: {json_filename}")
                self.invalid_source_files.add(source_file)
                return False
            
            # Check first record for available fields
            first_record = data[0]
            has_issue_date = 'issue_date' in first_record
            has_last_updated = 'last_updated' in first_record
            
            if not has_issue_date and not has_last_updated:
                logger.warning(f"No issue_date OR last_updated field in {json_filename}")
                self.invalid_source_files.add(source_file)
                return False
            
            # Check if we have actual values (not all None) in either field
            issue_date_values = 0
            last_updated_values = 0
            
            for record in data[:100]:  # Check first 100 records
                # Check issue_date
                issue_date = record.get('issue_date')
                if issue_date and str(issue_date).strip() and str(issue_date).lower() != 'none':
                    issue_date_values += 1
                
                # Check last_updated
                last_updated = record.get('last_updated')
                if last_updated and str(last_updated).strip() and str(last_updated).lower() != 'none':
                    last_updated_values += 1
            
            if issue_date_values == 0 and last_updated_values == 0:
                logger.warning(f"All date values are None/empty in {json_filename}")
                self.invalid_source_files.add(source_file)
                return False
            
            logger.info(f"✓ {json_filename} has date fields: issue_date({issue_date_values}/100), last_updated({last_updated_values}/100)")
            return True
            
        except Exception as e:
            logger.error(f"Error checking {json_filename}: {e}")
            self.invalid_source_files.add(source_file)
            return False

    def process_source_file(self, source_file, sql_records):
        """Process records from one source file"""
        logger.info(f"\nProcessing {len(sql_records)} records from {source_file}")
        
        # Check if source file has valid date data
        if not self.check_json_has_date_fields(source_file):
            logger.warning(f"Skipping {source_file} - no valid date data")
            return []
        
        json_filename = source_file.replace('.csv', '.json')
        json_path = self.json_folder / json_filename
        
        try:
            # Load JSON data
            logger.info(f"Loading {json_filename}...")
            with open(json_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            logger.info(f"Loaded {len(json_data)} records from JSON")
            
            # Extract unique values from SQL records for filtering
            sql_first_names = set(str(r['first_name']).strip().upper() for r in sql_records if r.get('first_name'))
            sql_last_names = set(str(r['last_name']).strip().upper() for r in sql_records if r.get('last_name'))
            sql_cities = set(str(r['city']).strip().upper() for r in sql_records if r.get('city'))
            sql_states = set(str(r['state']).strip().upper() for r in sql_records if r.get('state'))
            
            logger.info(f"Filtering JSON by {len(sql_first_names)} first names, {len(sql_last_names)} last names, {len(sql_cities)} cities, {len(sql_states)} states")
            
            # Filter JSON data step by step
            filtered_data = json_data
            logger.info(f"Starting with {len(filtered_data)} JSON records")
            
            # Filter by first names
            filtered_data = [
                record for record in filtered_data
                if str(record.get('inventor_first', '')).strip().upper() in sql_first_names
            ]
            logger.info(f"After first name filter: {len(filtered_data)} records")
            
            # Filter by last names
            filtered_data = [
                record for record in filtered_data
                if str(record.get('inventor_last', '')).strip().upper() in sql_last_names
            ]
            logger.info(f"After last name filter: {len(filtered_data)} records")
            
            # Filter by cities
            filtered_data = [
                record for record in filtered_data
                if str(record.get('inventor_city', '')).strip().upper() in sql_cities
            ]
            logger.info(f"After city filter: {len(filtered_data)} records")
            
            # Filter by states
            filtered_data = [
                record for record in filtered_data
                if str(record.get('inventor_state', '')).strip().upper() in sql_states
            ]
            logger.info(f"After state filter: {len(filtered_data)} records")
            
            if not filtered_data:
                logger.warning("No records remaining after filtering")
                return []
            
            # Create lookup dictionary for fast matching
            logger.info("Creating lookup dictionary...")
            lookup_dict = {}
            issue_date_used = 0
            last_updated_used = 0
            
            for json_record in filtered_data:
                key = (
                    str(json_record.get('inventor_first', '')).strip().upper(),
                    str(json_record.get('inventor_last', '')).strip().upper(),
                    str(json_record.get('inventor_city', '')).strip().upper(),
                    str(json_record.get('inventor_state', '')).strip().upper()
                )
                
                if key not in lookup_dict:  # Use first occurrence
                    # Try issue_date first
                    issue_date = json_record.get('issue_date')
                    if issue_date and str(issue_date).strip() and str(issue_date).lower() != 'none':
                        lookup_dict[key] = str(issue_date).strip()
                        issue_date_used += 1
                    else:
                        # Fallback to last_updated
                        last_updated = json_record.get('last_updated')
                        if last_updated and str(last_updated).strip() and str(last_updated).lower() != 'none':
                            lookup_dict[key] = str(last_updated).strip()
                            last_updated_used += 1
            
            logger.info(f"Created lookup dictionary with {len(lookup_dict)} entries")
            logger.info(f"  - Used issue_date: {issue_date_used}")
            logger.info(f"  - Used last_updated: {last_updated_used}")
            
            # Map SQL records to dates
            updates = []
            for sql_record in sql_records:
                key = (
                    str(sql_record['first_name']).strip().upper(),
                    str(sql_record['last_name']).strip().upper(),
                    str(sql_record['city']).strip().upper(),
                    str(sql_record['state']).strip().upper()
                )
                
                if key in lookup_dict:
                    date_raw = lookup_dict[key]
                    cleaned_date = self.clean_issue_date(date_raw)
                    
                    if cleaned_date:
                        updates.append({
                            'id': sql_record['id'],
                            'issue_date': cleaned_date
                        })
            
            logger.info(f"Found dates for {len(updates)} out of {len(sql_records)} SQL records")
            
            # Show sample matches
            if updates:
                logger.info("Sample matches:")
                for i, update in enumerate(updates[:5]):
                    sql_record = next(r for r in sql_records if r['id'] == update['id'])
                    logger.info(f"  {sql_record['first_name']} {sql_record['last_name']} -> {update['issue_date']}")
            
            return updates
            
        except Exception as e:
            logger.error(f"Error processing {source_file}: {e}")
            return []

    def clean_issue_date(self, date_value):
        """Clean and format date value (issue_date or last_updated)"""
        if not date_value or str(date_value).lower() in ['null', 'none', '']:
            return None
        
        date_str = str(date_value).strip()
        
        try:
            if date_str in ['', '0000-00-00', '00/00/0000', 'NULL']:
                return None
            
            # Handle MM/DD/YY format (like "01/04/22 00:00:00")
            if '/' in date_str and len(date_str.split('/')[2].split()[0]) == 2:
                parts = date_str.replace('\\/', '/').split()
                date_part = parts[0]
                month, day, year = date_part.split('/')
                
                if len(year) == 2:
                    year_int = int(year)
                    if year_int <= 30:
                        year = f"20{year}"
                    else:
                        year = f"19{year}"
                
                formatted_date = f"{month}/{day}/{year}"
                date_obj = pd.to_datetime(formatted_date)
            else:
                date_obj = pd.to_datetime(date_str.replace('\\/', '/'))
            
            if pd.isna(date_obj):
                return None
            return date_obj.strftime('%Y-%m-%d')
            
        except Exception as e:
            logger.debug(f"Date parsing failed for '{date_value}': {e}")
            return None

    def batch_update_sql(self, connection, updates):
        """Update all records in single SQL operation"""
        if not updates:
            return 0
        
        logger.info(f"Updating {len(updates)} records in SQL...")
        
        try:
            cursor = connection.cursor()
            query = "UPDATE existing_people SET issue_date = %s, updated_at = NOW() WHERE id = %s"
            
            batch_data = [(update['issue_date'], update['id']) for update in updates]
            cursor.executemany(query, batch_data)
            
            updated_count = cursor.rowcount
            connection.commit()
            cursor.close()
            
            logger.info(f"Successfully updated {updated_count} records")
            return updated_count
            
        except Exception as e:
            logger.error(f"SQL update failed: {e}")
            connection.rollback()
            return 0

    def run(self):
        """Main loop with last_updated fallback"""
        logger.info("="*60)
        logger.info("CLEAN ISSUE_DATE UPDATER WITH LAST_UPDATED FALLBACK")
        logger.info("="*60)
        
        connection = self.connect_sql()
        if not connection:
            return False
        
        try:
            total_processed = 0
            total_updated = 0
            
            while True:
                logger.info(f"\n{'='*40}")
                logger.info(f"BATCH {(self.offset // self.batch_size) + 1}")
                logger.info(f"{'='*40}")
                
                # Get 50k records with NULL issue_date
                logger.info(f"Getting {self.batch_size} records with NULL issue_date (offset: {self.offset})...")
                if self.invalid_source_files:
                    logger.info(f"Excluding {len(self.invalid_source_files)} invalid source files")
                
                records = self.get_null_issue_date_records(connection)
                
                if not records:
                    logger.info("No more records with NULL issue_date found")
                    break
                
                logger.info(f"Found {len(records)} records with NULL issue_date")
                
                # Group by source_file
                source_groups = defaultdict(list)
                for record in records:
                    source_file = record.get('source_file')
                    if source_file:
                        source_groups[source_file].append(record)
                
                logger.info(f"Grouped into {len(source_groups)} source files:")
                for source_file, file_records in source_groups.items():
                    logger.info(f"  {source_file}: {len(file_records)} records")
                
                # Process each source file
                batch_updates = 0
                for source_file, file_records in source_groups.items():
                    updates = self.process_source_file(source_file, file_records)
                    
                    if updates:
                        updated_count = self.batch_update_sql(connection, updates)
                        batch_updates += updated_count
                        logger.info(f"✓ Updated {updated_count} records from {source_file}")
                    else:
                        logger.info(f"✗ No updates for {source_file}")
                
                total_processed += len(records)
                total_updated += batch_updates
                
                logger.info(f"Batch complete: {len(records)} processed, {batch_updates} updated")
                logger.info(f"Running totals: {total_processed} processed, {total_updated} updated")
                
                # Move to next batch
                self.offset += self.batch_size
                
                # If we got fewer records than batch_size, we're done
                if len(records) < self.batch_size:
                    logger.info("Reached end of data")
                    break
            
            # Final report
            logger.info(f"\n{'='*60}")
            logger.info("FINAL RESULTS")
            logger.info(f"{'='*60}")
            logger.info(f"Total records processed: {total_processed:,}")
            logger.info(f"Total records updated: {total_updated:,}")
            if self.invalid_source_files:
                logger.info(f"Invalid source files skipped: {list(self.invalid_source_files)}")
            logger.info(f"{'='*60}")
            
        except KeyboardInterrupt:
            logger.info("Process interrupted by user")
        except Exception as e:
            logger.error(f"Process failed: {e}")
            logger.error(traceback.format_exc())
        finally:
            connection.close()
            logger.info("Database connection closed")
        
        return True

def main():
    """Main function"""
    print("Clean Issue Date Updater with last_updated fallback")
    print("===================================================")
    print("Enhanced implementation:")
    print("• Try issue_date first")
    print("• Fallback to last_updated if issue_date is null/missing")
    print("• Simple SQL with LIMIT/OFFSET")
    print("• Group by source_file")
    print("• Filter JSON data efficiently")  
    print("• Map to date values")
    print("• Single SQL update per source file")
    print()
    
    try:
        updater = CleanIssueDateUpdater("../")
        success = updater.run()
        
        if success:
            print("\nClean issue date update completed!")
        else:
            print("Update failed. Check the log for details.")
    
    except Exception as e:
        logger.error(f"Script failed: {e}")
        print(f"Script failed: {e}")

if __name__ == "__main__":
    main()