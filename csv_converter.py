#!/usr/bin/env python3
# =============================================================================
# csv_converter.py - Working Access Database Converter
# Convert Microsoft Access databases to CSV using mdb-tools (PROVEN TO WORK)
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
        logging.FileHandler(f'mdb_conversion_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MDBToolsConverter:
    """Convert Microsoft Access databases to CSV files using mdb-tools"""
    
    def __init__(self, database_folder_path: str, output_folder: str = "converted_databases"):
        self.database_folder = Path(database_folder_path)
        self.output_folder = Path(output_folder)
        self.output_folder.mkdir(exist_ok=True)
        
        # Create subfolders for organization
        (self.output_folder / "csv").mkdir(exist_ok=True)
        (self.output_folder / "json").mkdir(exist_ok=True)
        (self.output_folder / "metadata").mkdir(exist_ok=True)
        
        self.conversion_log = []
        
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
                logger.error("❌ mdb-tools not working properly")
                return False
        except FileNotFoundError:
            logger.error("❌ mdb-tools not installed")
            logger.info("   Install with: brew install mdb-tools")
            return False
        except Exception as e:
            logger.error(f"❌ Error checking mdb-tools: {e}")
            return False

    def find_access_databases(self):
        """Find all Access database files in the folder"""
        access_files = []
        
        # Look for .mdb and .accdb files
        for extension in ['*.mdb', '*.accdb']:
            access_files.extend(self.database_folder.glob(extension))
        
        logger.info(f"Found {len(access_files)} Access database files:")
        for file in access_files:
            file_size = file.stat().st_size / (1024 * 1024)  # Size in MB
            logger.info(f"  - {file.name} ({file_size:.1f} MB)")
        
        return access_files
    
    def get_table_list(self, db_path: Path):
        """Get list of tables in the database"""
        try:
            logger.debug(f"Getting table list for {db_path.name}")
            result = subprocess.run(['mdb-tables', str(db_path)], 
                                  capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                tables = [t.strip() for t in result.stdout.strip().split() if t.strip()]
                # Filter out system tables
                user_tables = [t for t in tables if not t.startswith('MSys') and t.strip()]
                logger.debug(f"Found {len(user_tables)} user tables: {user_tables[:5]}{'...' if len(user_tables) > 5 else ''}")
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

    def export_table_to_dataframe(self, db_path: Path, table_name: str):
        """Export a single table to pandas DataFrame"""
        try:
            logger.debug(f"Exporting table {table_name} from {db_path.name}")
            
            # Use mdb-export to get CSV data
            result = subprocess.run(['mdb-export', str(db_path), table_name], 
                                  capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0:
                # Convert CSV string to DataFrame
                csv_data = result.stdout
                if csv_data.strip():
                    df = pd.read_csv(StringIO(csv_data))
                    logger.debug(f"Successfully exported {table_name}: {df.shape}")
                    return df
                else:
                    logger.debug(f"Table {table_name} returned empty data")
                    return None
            else:
                logger.debug(f"mdb-export failed for {table_name}: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            logger.warning(f"Timeout exporting table {table_name}")
            return None
        except Exception as e:
            logger.debug(f"Error exporting table {table_name}: {e}")
            return None

    def convert_database(self, db_path: Path):
        """Convert a single Access database to CSV files"""
        logger.info(f"🔄 Converting database: {db_path.name}")
        
        # Get table list
        tables = self.get_table_list(db_path)
        
        if not tables:
            logger.warning(f"   ⚠️ No tables found in {db_path.name}")
            return False
        
        logger.info(f"   📋 Found {len(tables)} tables: {tables[:3]}{'...' if len(tables) > 3 else ''}")
        
        # Convert each table
        db_name = db_path.stem
        successful_tables = 0
        
        db_info = {
            'database_name': db_name,
            'source_file': str(db_path),
            'conversion_date': datetime.now().isoformat(),
            'method_used': 'mdb-tools',
            'tables': {}
        }
        
        for table_name in tables:
            try:
                logger.info(f"     🔄 Converting table: {table_name}")
                
                # Export table data
                df = self.export_table_to_dataframe(db_path, table_name)
                
                if df is not None and not df.empty:
                    # Generate file names
                    csv_filename = f"{db_name}_{table_name}.csv"
                    json_filename = f"{db_name}_{table_name}.json"
                    
                    csv_path = self.output_folder / "csv" / csv_filename
                    json_path = self.output_folder / "json" / json_filename
                    
                    # Save as CSV
                    df.to_csv(csv_path, index=False, encoding='utf-8')
                    
                    # Save as JSON (for complex data types)
                    df.to_json(json_path, orient='records', indent=2, date_format='iso')
                    
                    # Store metadata
                    table_info = {
                        'row_count': len(df),
                        'column_count': len(df.columns),
                        'columns': list(df.columns),
                        'csv_file': str(csv_path),
                        'json_file': str(json_path),
                        'sample_data': df.head(3).to_dict('records') if len(df) > 0 else []
                    }
                    
                    db_info['tables'][table_name] = table_info
                    successful_tables += 1
                    
                    logger.info(f"       ✅ Converted {table_name}: {len(df):,} rows, {len(df.columns)} columns")
                    
                    # Log conversion
                    self.conversion_log.append({
                        'database': db_name,
                        'table': table_name,
                        'rows': len(df),
                        'columns': len(df.columns),
                        'method': 'mdb-tools',
                        'status': 'success'
                    })
                    
                else:
                    logger.warning(f"       ⚠️ Table {table_name} is empty or could not be exported")
                    self.conversion_log.append({
                        'database': db_name,
                        'table': table_name,
                        'rows': 0,
                        'columns': 0,
                        'method': 'mdb-tools',
                        'status': 'empty'
                    })
                    
            except Exception as e:
                logger.error(f"       ❌ Error converting table {table_name}: {e}")
                logger.debug(f"       Full traceback: {traceback.format_exc()}")
                self.conversion_log.append({
                    'database': db_name,
                    'table': table_name,
                    'error': str(e),
                    'method': 'mdb-tools',
                    'status': 'failed'
                })
        
        # Save database metadata
        if successful_tables > 0:
            metadata_file = self.output_folder / "metadata" / f"{db_name}_metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(db_info, f, indent=2, default=str)
            
            logger.info(f"   ✅ Completed conversion of {db_path.name} ({successful_tables} tables)")
            return True
        else:
            logger.warning(f"   ⚠️ No tables successfully converted from {db_path.name}")
            return False
    
    def convert_all_databases(self):
        """Convert all Access databases found in the folder"""
        databases = self.find_access_databases()
        
        if not databases:
            logger.warning("No Access database files found!")
            return
        
        successful_conversions = 0
        
        for db_path in databases:
            logger.info(f"\n{'='*60}")
            if self.convert_database(db_path):
                successful_conversions += 1
        
        # Generate summary report
        self.generate_summary_report(successful_conversions, len(databases))
    
    def generate_summary_report(self, successful: int, total: int):
        """Generate a summary report of the conversion"""
        summary = {
            'conversion_summary': {
                'total_databases': total,
                'successful_conversions': successful,
                'failed_conversions': total - successful,
                'conversion_date': datetime.now().isoformat(),
                'method': 'mdb-tools'
            },
            'conversion_log': self.conversion_log,
            'output_structure': {
                'csv_files': str(self.output_folder / "csv"),
                'json_files': str(self.output_folder / "json"),
                'metadata_files': str(self.output_folder / "metadata")
            }
        }
        
        # Save summary report
        summary_file = self.output_folder / "conversion_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        # Calculate totals
        total_tables = len([log for log in self.conversion_log if log['status'] == 'success'])
        total_rows = sum(log.get('rows', 0) for log in self.conversion_log if log['status'] == 'success')
        
        # Print summary
        print("\n" + "="*70)
        print("🔄 ACCESS DATABASE CONVERSION SUMMARY")
        print("="*70)
        print(f"📁 Source folder: {self.database_folder}")
        print(f"📁 Output folder: {self.output_folder}")
        print(f"🗄️  Total databases: {total}")
        print(f"✅ Successful conversions: {successful}")
        print(f"❌ Failed conversions: {total - successful}")
        print(f"📊 Total tables converted: {total_tables}")
        print(f"📈 Total rows converted: {total_rows:,}")
        
        if self.conversion_log:
            print(f"\n📋 Conversion Details (Top 10 tables by size):")
            # Sort by rows descending
            sorted_logs = sorted([log for log in self.conversion_log if log['status'] == 'success'], 
                               key=lambda x: x.get('rows', 0), reverse=True)
            
            for log in sorted_logs[:10]:
                status_emoji = "✅"
                print(f"  {status_emoji} {log['database']}.{log['table']}: {log.get('rows', 0):,} rows")
        
        print(f"\n📄 Summary report saved: {summary_file}")
        print(f"📄 CSV files location: {self.output_folder / 'csv'}")
        print(f"📄 JSON files location: {self.output_folder / 'json'}")
        print("="*70)

def main():
    """Main function to run the Access database conversion"""
    
    # Configuration
    DATABASE_FOLDER = "patent_system"  # Folder containing your .mdb/.accdb files
    OUTPUT_FOLDER = "converted_databases"  # Where to save the converted files
    
    print("🚀 Starting Access Database Conversion with mdb-tools")
    print(f"📁 Looking for databases in: {DATABASE_FOLDER}")
    print(f"📁 Output will be saved to: {OUTPUT_FOLDER}")
    
    # Check if source folder exists
    if not os.path.exists(DATABASE_FOLDER):
        print(f"❌ Error: Database folder '{DATABASE_FOLDER}' not found!")
        print("Please update the DATABASE_FOLDER variable to point to your Access database files.")
        return
    
    try:
        # Create converter and run conversion
        converter = MDBToolsConverter(DATABASE_FOLDER, OUTPUT_FOLDER)
        converter.convert_all_databases()
        
        print("\n🎉 Conversion completed! Check the output folder for your CSV and JSON files.")
        
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        print(f"❌ Conversion failed: {e}")

if __name__ == "__main__":
    main()