#!/usr/bin/env python3
# =============================================================================
# duplicate_issue_date_updater.py - Fast batch processing version
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
        logging.FileHandler(f'duplicate_issue_date_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FastDuplicateIssueDateUpdater:
    """Fast batch processing version for 9M+ records"""
    
    def __init__(self):
        self.batch_size = 100000  # Process 100k records at a time
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
        """Get batch of 100k records with potential duplicates"""
        query = """
        SELECT id, first_name, last_name, city, state, issue_date
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

    def process_batch_with_pandas(self, records):
        """Use pandas to efficiently find duplicates and newest dates"""
        if not records:
            return []
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Convert issue_date to datetime, handling various formats
        df['issue_date_parsed'] = pd.to_datetime(df['issue_date'], errors='coerce')
        
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
        
        # Find the newest date for each group
        group_newest = duplicate_df.groupby('group_key')['issue_date_parsed'].max().reset_index()
        group_newest.columns = ['group_key', 'newest_date']
        
        # Merge back to get newest date for each record
        duplicate_df = duplicate_df.merge(group_newest, on='group_key', how='left')
        
        # Filter to records that need updating (current date is None or older than newest)
        needs_update = (
            duplicate_df['issue_date_parsed'].isna() |
            (duplicate_df['issue_date_parsed'] < duplicate_df['newest_date'])
        ) & duplicate_df['newest_date'].notna()
        
        update_records = duplicate_df[needs_update].copy()
        
        if len(update_records) == 0:
            logger.info("No records need updating in this batch")
            return []
        
        # Prepare update data
        updates = []
        for _, row in update_records.iterrows():
            newest_date_str = row['newest_date'].strftime('%Y-%m-%d') if pd.notna(row['newest_date']) else None
            if newest_date_str:
                updates.append({
                    'id': row['id'],
                    'issue_date': newest_date_str
                })
        
        logger.info(f"Prepared {len(updates)} updates from {len(duplicate_df)} duplicate records")
        
        # Show sample of what's being updated
        if updates:
            sample_size = min(5, len(updates))
            logger.info(f"Sample updates:")
            for i in range(sample_size):
                update = updates[i]
                original_record = next(r for r in records if r['id'] == update['id'])
                logger.info(f"  ID {update['id']}: {original_record['first_name']} {original_record['last_name']} -> {update['issue_date']}")
        
        return updates

    def batch_update_sql(self, connection, updates):
        """Update all records in batch"""
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

    def get_summary_stats(self, connection):
        """Get summary statistics"""
        queries = {
            'total_records': "SELECT COUNT(*) as count FROM existing_people",
            'records_with_issue_date': "SELECT COUNT(*) as count FROM existing_people WHERE issue_date IS NOT NULL",
            'records_without_issue_date': "SELECT COUNT(*) as count FROM existing_people WHERE issue_date IS NULL"
        }
        
        stats = {}
        cursor = connection.cursor(dictionary=True)
        
        for stat_name, query in queries.items():
            cursor.execute(query)
            result = cursor.fetchone()
            stats[stat_name] = result['count']
        
        cursor.close()
        return stats

    def run(self):
        """Main execution method with fast batch processing"""
        logger.info("="*60)
        logger.info("FAST DUPLICATE ISSUE_DATE UPDATER")
        logger.info("="*60)
        logger.info("Fast batch processing approach:")
        logger.info("1. Process 100k records at a time")
        logger.info("2. Use pandas for efficient duplicate detection")
        logger.info("3. Find newest issue_date per group within batch")
        logger.info("4. Batch update all records that need updating")
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
            logger.info(f"  Records with issue_date: {initial_stats['records_with_issue_date']:,}")
            logger.info(f"  Records without issue_date: {initial_stats['records_without_issue_date']:,}")
            
            # Confirm before proceeding
            print(f"\nThis will process {initial_stats['total_records']:,} records in batches of {self.batch_size:,}")
            print("Each batch will update duplicates with the newest issue_date within that batch.")
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
                    logger.info(f"✓ Updated {updated_count} records in this batch")
                else:
                    logger.info("✓ No updates needed in this batch")
                
                total_processed += len(records)
                
                # Progress report
                logger.info(f"Batch {batch_num} complete:")
                logger.info(f"  This batch: {len(records):,} processed, {len(updates) if updates else 0} updated")
                logger.info(f"  Running totals: {total_processed:,} processed, {total_updated:,} updated")
                
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
            logger.info(f"Total records updated: {total_updated:,}")
            logger.info(f"Final state:")
            logger.info(f"  Total records: {final_stats['total_records']:,}")
            logger.info(f"  Records with issue_date: {final_stats['records_with_issue_date']:,}")
            logger.info(f"  Records without issue_date: {final_stats['records_without_issue_date']:,}")
            logger.info(f"Changes:")
            logger.info(f"  Records with issue_date: {final_stats['records_with_issue_date'] - initial_stats['records_with_issue_date']:+,}")
            logger.info(f"  Records without issue_date: {final_stats['records_without_issue_date'] - initial_stats['records_without_issue_date']:+,}")
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
    print("Fast Duplicate Issue Date Updater")
    print("==================================")
    print("Optimized for large datasets (9M+ records):")
    print("• Processes 100,000 records per batch")
    print("• Uses pandas for efficient duplicate detection")
    print("• Updates newest issue_date within each batch")
    print("• Much faster than processing individual groups")
    print()
    print("Note: This processes duplicates within each 100k batch,")
    print("so it may not catch all duplicates across the entire dataset")
    print("but will be much faster for large datasets.")
    print()
    
    try:
        updater = FastDuplicateIssueDateUpdater()
        success = updater.run()
        
        if success:
            print("\nFast duplicate issue date update completed!")
        else:
            print("Update failed. Check the log for details.")
    
    except Exception as e:
        logger.error(f"Script failed: {e}")
        print(f"Script failed: {e}")

if __name__ == "__main__":
    main()