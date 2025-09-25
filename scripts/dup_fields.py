#!/usr/bin/env python3
# =============================================================================
# duplicate_fields_updater.py - Fast batch processing for multiple fields
# =============================================================================
import pandas as pd
import os
import logging
import mysql.connector
from pathlib import Path
from datetime import datetime
import sys
import traceback

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'duplicate_fields_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FastDuplicateFieldsUpdater:
    """Fast batch processing for multiple fields - only update empty/null values"""
    
    def __init__(self):
        self.batch_size = 100000  # Process 100k records at a time
        self.offset = 0
        
        # Fields to update (only if empty/null)
        self.fields_to_update = [
            'address', 'zip', 'email', 'inventor_id', 'patent_no', 
            'title', 'mail_to_name', 'mail_to_send_key', 'mod_user', 'bar_code'
        ]
        
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

    def get_batch_records(self, connection):
        """Get batch of 100k records with all fields"""
        # Build query with all fields we need
        fields = ['id', 'first_name', 'last_name', 'city', 'state'] + self.fields_to_update
        fields_str = ', '.join(fields)
        
        query = f"""
        SELECT {fields_str}
        FROM existing_people 
        WHERE first_name IS NOT NULL 
          AND last_name IS NOT NULL 
          AND city IS NOT NULL 
          AND state IS NOT NULL
          AND first_name != ''
          AND last_name != ''
          AND city != ''
          AND state != ''
        ORDER BY id
        LIMIT %s OFFSET %s
        """
        
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, (self.batch_size, self.offset))
        records = cursor.fetchall()
        cursor.close()
        
        return records

    def is_empty_value(self, value):
        """Check if a value is considered empty (None, '', or whitespace)"""
        if value is None:
            return True
        if isinstance(value, str) and value.strip() == '':
            return True
        return False

    def get_best_value_for_field(self, group_df, field):
        """Get the best non-empty value for a field from a duplicate group"""
        # Get all non-empty values for this field
        non_empty_values = []
        
        for value in group_df[field]:
            if not self.is_empty_value(value):
                non_empty_values.append(str(value).strip())
        
        if not non_empty_values:
            return None
        
        # For most fields, just return the first non-empty value
        # You could add field-specific logic here if needed
        if field in ['patent_no', 'inventor_id']:
            # For numeric-like fields, prefer longer values (more complete)
            non_empty_values.sort(key=len, reverse=True)
        elif field in ['email']:
            # For email, prefer ones with @ symbol
            email_values = [v for v in non_empty_values if '@' in v]
            if email_values:
                non_empty_values = email_values
        
        return non_empty_values[0]

    def process_batch_with_pandas(self, records):
        """Use pandas to efficiently find duplicates and best field values"""
        if not records:
            return []
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Create grouping key
        df['group_key'] = (df['first_name'].astype(str) + '|' + 
                          df['last_name'].astype(str) + '|' + 
                          df['city'].astype(str) + '|' + 
                          df['state'].astype(str))
        
        # Find groups with more than 1 record
        group_counts = df['group_key'].value_counts()
        duplicate_groups = group_counts[group_counts > 1].index
        
        if len(duplicate_groups) == 0:
            logger.info(f"No duplicates found in batch of {len(records)} records")
            return []
        
        logger.info(f"Found {len(duplicate_groups)} duplicate groups in batch")
        
        # Filter to only duplicate records
        duplicate_df = df[df['group_key'].isin(duplicate_groups)].copy()
        
        # Process each field
        updates = []
        field_stats = {field: 0 for field in self.fields_to_update}
        
        for group_key in duplicate_groups:
            group_df = duplicate_df[duplicate_df['group_key'] == group_key].copy()
            
            # For each field, find the best value and records that need updating
            group_updates = {}
            
            for field in self.fields_to_update:
                best_value = self.get_best_value_for_field(group_df, field)
                
                if best_value is not None:
                    # Find records in this group that have empty values for this field
                    empty_mask = group_df[field].apply(self.is_empty_value)
                    records_to_update = group_df[empty_mask]
                    
                    for _, record in records_to_update.iterrows():
                        record_id = record['id']
                        if record_id not in group_updates:
                            group_updates[record_id] = {}
                        group_updates[record_id][field] = best_value
                        field_stats[field] += 1
            
            # Add group updates to main updates list
            for record_id, field_updates in group_updates.items():
                updates.append({
                    'id': record_id,
                    'updates': field_updates
                })
        
        logger.info(f"Prepared updates for {len(updates)} records")
        logger.info("Field update counts:")
        for field, count in field_stats.items():
            if count > 0:
                logger.info(f"  {field}: {count} updates")
        
        # Show sample of what's being updated
        if updates:
            sample_size = min(3, len(updates))
            logger.info(f"Sample updates:")
            for i in range(sample_size):
                update = updates[i]
                original_record = next(r for r in records if r['id'] == update['id'])
                logger.info(f"  ID {update['id']}: {original_record['first_name']} {original_record['last_name']}")
                for field, new_value in update['updates'].items():
                    old_value = original_record.get(field, 'NULL')
                    logger.info(f"    {field}: '{old_value}' -> '{new_value}'")
        
        return updates

    def batch_update_sql(self, connection, updates):
        """Update all records in batch with multiple fields"""
        if not updates:
            return 0
        
        logger.info(f"Updating {len(updates)} records in SQL...")
        
        try:
            cursor = connection.cursor()
            total_updated = 0
            
            for update in updates:
                record_id = update['id']
                field_updates = update['updates']
                
                if not field_updates:
                    continue
                
                # Build dynamic UPDATE query
                set_clauses = []
                params = []
                
                for field, value in field_updates.items():
                    set_clauses.append(f"{field} = %s")
                    params.append(value)
                
                # Add updated_at
                set_clauses.append("updated_at = NOW()")
                params.append(record_id)  # For WHERE clause
                
                query = f"""
                UPDATE existing_people 
                SET {', '.join(set_clauses)}
                WHERE id = %s
                """
                
                cursor.execute(query, params)
                total_updated += cursor.rowcount
            
            connection.commit()
            cursor.close()
            
            logger.info(f"Successfully updated {total_updated} records")
            return total_updated
            
        except Exception as e:
            logger.error(f"SQL update failed: {e}")
            connection.rollback()
            return 0

    def get_summary_stats(self, connection):
        """Get summary statistics for each field"""
        stats = {}
        cursor = connection.cursor(dictionary=True)
        
        # Total records
        cursor.execute("SELECT COUNT(*) as count FROM existing_people")
        stats['total_records'] = cursor.fetchone()['count']
        
        # Stats for each field
        for field in self.fields_to_update:
            # Records with non-empty values
            query = f"""
            SELECT COUNT(*) as count 
            FROM existing_people 
            WHERE {field} IS NOT NULL AND {field} != ''
            """
            cursor.execute(query)
            stats[f'{field}_filled'] = cursor.fetchone()['count']
            
            # Records with empty values
            query = f"""
            SELECT COUNT(*) as count 
            FROM existing_people 
            WHERE {field} IS NULL OR {field} = ''
            """
            cursor.execute(query)
            stats[f'{field}_empty'] = cursor.fetchone()['count']
        
        cursor.close()
        return stats

    def run(self):
        """Main execution method with fast batch processing"""
        logger.info("="*60)
        logger.info("FAST DUPLICATE FIELDS UPDATER")
        logger.info("="*60)
        logger.info("Updates these fields for duplicate groups:")
        for field in self.fields_to_update:
            logger.info(f"  • {field}")
        logger.info("")
        logger.info("Process:")
        logger.info("1. Process 100k records at a time")
        logger.info("2. Find duplicate groups (same first_name, last_name, city, state)")
        logger.info("3. For each field, find best non-empty value in group")
        logger.info("4. Update ONLY empty/null values (no overwriting)")
        logger.info("="*60)
        
        connection = self.connect_sql()
        if not connection:
            return False
        
        try:
            # Get initial statistics
            logger.info("\nGetting initial statistics...")
            initial_stats = self.get_summary_stats(connection)
            logger.info(f"Initial state:")
            logger.info(f"  Total records: {initial_stats['total_records']:,}")
            
            logger.info("  Field fill rates:")
            for field in self.fields_to_update:
                filled = initial_stats[f'{field}_filled']
                empty = initial_stats[f'{field}_empty']
                total = filled + empty
                pct = (filled / total * 100) if total > 0 else 0
                logger.info(f"    {field}: {filled:,} filled ({pct:.1f}%), {empty:,} empty")
            
            # Confirm before proceeding
            print(f"\nThis will process {initial_stats['total_records']:,} records in batches of {self.batch_size:,}")
            print("Will update empty/null fields with values from duplicate records.")
            print("Will NOT overwrite existing data.")
            response = input("Do you want to proceed? (y/N): ").strip().lower()
            
            if response != 'y':
                logger.info("Operation cancelled by user")
                return False
            
            # Main processing loop
            total_processed = 0
            total_updated = 0
            batch_num = 0
            
            while True:
                batch_num += 1
                logger.info(f"\n{'='*50}")
                logger.info(f"BATCH {batch_num} - Offset: {self.offset:,}")
                logger.info(f"{'='*50}")
                
                # Get batch of records
                logger.info(f"Fetching {self.batch_size:,} records...")
                records = self.get_batch_records(connection)
                
                if not records:
                    logger.info("No more records to process")
                    break
                
                logger.info(f"Retrieved {len(records):,} records")
                
                # Process duplicates in this batch
                updates = self.process_batch_with_pandas(records)
                
                # Update the database
                if updates:
                    updated_count = self.batch_update_sql(connection, updates)
                    total_updated += updated_count
                    logger.info(f"✓ Updated {updated_count} field values in this batch")
                else:
                    logger.info("✓ No updates needed in this batch")
                
                total_processed += len(records)
                
                # Progress report
                logger.info(f"Batch {batch_num} complete:")
                logger.info(f"  This batch: {len(records):,} processed, {len(updates) if updates else 0} records updated")
                logger.info(f"  Running totals: {total_processed:,} processed, {total_updated:,} field updates")
                
                # Move to next batch
                self.offset += self.batch_size
                
                # If we got fewer records than batch_size, we're done
                if len(records) < self.batch_size:
                    logger.info("Reached end of data")
                    break
            
            # Get final statistics
            logger.info("\nGetting final statistics...")
            final_stats = self.get_summary_stats(connection)
            
            # Final report
            logger.info(f"\n{'='*60}")
            logger.info("FINAL RESULTS")
            logger.info(f"{'='*60}")
            logger.info(f"Total records processed: {total_processed:,}")
            logger.info(f"Total field updates: {total_updated:,}")
            
            logger.info("\nField improvements:")
            for field in self.fields_to_update:
                initial_filled = initial_stats[f'{field}_filled']
                final_filled = final_stats[f'{field}_filled']
                improvement = final_filled - initial_filled
                
                initial_empty = initial_stats[f'{field}_empty']
                final_empty = final_stats[f'{field}_empty']
                
                if improvement > 0:
                    logger.info(f"  {field}: +{improvement:,} filled ({initial_filled:,} -> {final_filled:,})")
                    logger.info(f"    Empty: {initial_empty:,} -> {final_empty:,}")
                else:
                    logger.info(f"  {field}: No change ({final_filled:,} filled)")
            
            logger.info(f"{'='*60}")
            
        except KeyboardInterrupt:
            logger.info("Process interrupted by user")
            connection.rollback()
        except Exception as e:
            logger.error(f"Process failed: {e}")
            logger.error(traceback.format_exc())
            connection.rollback()
        finally:
            connection.close()
            logger.info("Database connection closed")
        
        return True

def main():
    """Main function"""
    print("Fast Duplicate Fields Updater")
    print("==============================")
    print("Updates multiple fields for duplicate records:")
    
    fields = ['address', 'zip', 'email', 'inventor_id', 'patent_no', 
              'title', 'mail_to_name', 'mail_to_send_key', 'mod_user', 'bar_code']
    
    for field in fields:
        print(f"  • {field}")
    
    print()
    print("Key features:")
    print("• Processes 100,000 records per batch")
    print("• Finds duplicates (same first_name, last_name, city, state)")
    print("• For each field, uses best non-empty value from group")
    print("• ONLY updates empty/null values (preserves existing data)")
    print("• Fast pandas-based processing")
    print()
    
    try:
        updater = FastDuplicateFieldsUpdater()
        success = updater.run()
        
        if success:
            print("\nFast duplicate fields update completed!")
        else:
            print("Update failed. Check the log for details.")
    
    except Exception as e:
        logger.error(f"Script failed: {e}")
        print(f"Script failed: {e}")

if __name__ == "__main__":
    main()