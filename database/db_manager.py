# =============================================================================
# database/db_manager.py
# Database configuration and connection management
# =============================================================================


import os
import logging
import json
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
import mysql.connector
from mysql.connector import Error
import sqlite3
from contextlib import contextmanager
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """Database configuration data class"""
    host: str
    port: int
    database: str
    username: str
    password: str
    engine: str = 'mysql'  # mysql, sqlite, postgresql
    
    @classmethod
    def from_env(cls) -> 'DatabaseConfig':
        """Create config from environment variables"""
        return cls(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '3306')),
            database=os.getenv('DB_NAME', 'patent_data'),
            username=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', 'password'),
            engine=os.getenv('DB_ENGINE', 'mysql').lower()
        )

class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
        
    @contextmanager
    def get_connection(self):
        """Get database connection with automatic cleanup"""
        conn = None
        try:
            if self.config.engine == 'mysql':
                conn = mysql.connector.connect(
                    host=self.config.host,
                    port=self.config.port,
                    database=self.config.database,
                    user=self.config.username,
                    password=self.config.password,
                    charset='utf8mb4',
                    collation='utf8mb4_unicode_ci',
                    autocommit=False
                )
            elif self.config.engine == 'sqlite':
                # For testing/development
                conn = sqlite3.connect(self.config.database)
                conn.row_factory = sqlite3.Row  # Enable column access by name
                
            yield conn
            
        except Error as e:
            logger.error(f"Database connection error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                try:
                    # MySQL connector has is_connected(); sqlite3 does not
                    if hasattr(conn, 'is_connected'):
                        if conn.is_connected():
                            conn.close()
                    else:
                        conn.close()
                except Exception:
                    pass
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def initialize_schema(self, schema_file: Optional[str] = None) -> bool:
        """Initialize database schema from SQL file"""
        if not schema_file:
            schema_file = Path(__file__).parent / 'schema.sql'
            
        try:
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Split and execute each statement
                statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
                for statement in statements:
                    if statement:
                        cursor.execute(statement)
                
                conn.commit()
                logger.info("Database schema initialized successfully")
                return True
                
        except Exception as e:
            logger.error(f"Schema initialization failed: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[tuple] = None, fetch_one: bool = False) -> Any:
        """Execute a query and return results"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)  # Return results as dictionaries
            cursor.execute(query, params or ())
            
            if fetch_one:
                return cursor.fetchone()
            else:
                return cursor.fetchall()
    
    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """Execute query with multiple parameter sets"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
            return cursor.rowcount
    
    def insert_batch(self, table: str, records: List[Dict[str, Any]], ignore_duplicates: bool = True) -> int:
        """Insert multiple records efficiently"""
        if not records:
            return 0
        
        # Get column names from first record
        columns = list(records[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join(columns)
        
        # Prepare query
        insert_type = 'INSERT IGNORE' if ignore_duplicates else 'INSERT'
        query = f"{insert_type} INTO {table} ({column_names}) VALUES ({placeholders})"
        
        # Prepare data
        data = []
        for record in records:
            row = []
            for col in columns:
                value = record.get(col)
                # Convert lists/dicts to JSON strings
                if isinstance(value, (list, dict)):
                    value = json.dumps(value)
                row.append(value)
            data.append(tuple(row))
        
        return self.execute_many(query, data)


class ExistingDataDAO:
    """Data Access Object for existing patents and people data"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def load_existing_patents(self) -> set:
        """Load all existing patent numbers"""
        query = "SELECT DISTINCT patent_number FROM existing_patents"
        results = self.db.execute_query(query)
        return {row['patent_number'] for row in results if row['patent_number']}
    
    def load_existing_people(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Load existing people for matching"""
        query = """
        SELECT id, first_name, last_name, city, state, country, 
            address, zip, phone, email, company_name, record_type, source_file,
            issue_id, inventor_id, mod_user
        FROM existing_people
        WHERE (first_name IS NOT NULL AND first_name != '') 
        OR (last_name IS NOT NULL AND last_name != '')
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        return self.db.execute_query(query)

    def find_people_by_lastnames_batch(self, last_names: List[str], limit_per_name: int = 1000) -> List[Dict[str, Any]]:
        """
        OPTIMIZED with deduplication and pagination: Find all people for multiple last names
        Uses DISTINCT to eliminate exact duplicates on the 4 key matching fields
        Uses pagination to ensure all records are returned regardless of MySQL limits
        """
        if not last_names:
            return []
            
        try:
            # Create placeholders for IN clause
            placeholders = ', '.join(['%s'] * len(last_names))
            
            # UPDATED query to include the missing fields: address, email, issue_id, inventor_id, mod_user
            base_query = f"""
            SELECT DISTINCT 
                LOWER(TRIM(first_name)) as first_name,
                LOWER(TRIM(last_name)) as last_name, 
                LOWER(TRIM(city)) as city, 
                LOWER(TRIM(state)) as state,
                country, address, zip, phone, email, company_name, record_type, source_file,
                issue_id, inventor_id, mod_user
            FROM existing_people 
            WHERE LOWER(TRIM(last_name)) IN ({placeholders})
            ORDER BY last_name, first_name, city, state
            """
            
            # Implement pagination to get ALL records
            page_size = 50000  # Reasonable page size to avoid memory issues
            all_results = []
            offset = 0
            cleaned_names = [name.strip().lower() for name in last_names]
            
            with self.db.get_connection() as conn:
                with conn.cursor(dictionary=True) as cursor:
                    
                    while True:
                        # Add pagination to the query
                        paginated_query = base_query + f" LIMIT {page_size} OFFSET {offset}"
                        
                        cursor.execute(paginated_query, tuple(cleaned_names))
                        page_results = cursor.fetchall()
                        
                        if not page_results:
                            break  # No more results
                        
                        all_results.extend(page_results)
                        
                        logger.info(f"Batch query page {offset//page_size + 1}: {len(page_results)} records (total so far: {len(all_results)})")
                        
                        # If we got fewer results than page_size, we're done
                        if len(page_results) < page_size:
                            break
                            
                        offset += page_size
                        
                        # Safety check to prevent infinite loops
                        if offset > 1000000:  # Max 1M records per batch
                            logger.warning(f"Reached maximum offset limit for batch query")
                            break
            
            logger.info(f"Batch query for {len(last_names)} last names returned {len(all_results)} total DISTINCT records across {offset//page_size + 1} pages")
            return all_results
            
        except Exception as e:
            logger.error(f"Error in batch query for last names {last_names[:5]}...: {e}")
            return []

    def bulk_insert_patents(self, patents: List[Dict[str, Any]]) -> int:
        """Bulk insert existing patents"""
        return self.db.insert_batch('existing_patents', patents)
    
    def bulk_insert_people(self, people: List[Dict[str, Any]]) -> int:
        """Bulk insert existing people"""
        return self.db.insert_batch('existing_people', people)
    
    def find_people_matches(self, first_name: str, last_name: str, 
                           city: str = None, state: str = None) -> List[Dict[str, Any]]:
        """Find potential matches for a person using SQL"""
        conditions = []
        params = []
        
        # Name matching with various strategies
        if first_name and last_name:
            conditions.append("""
                (first_name LIKE %s AND last_name = %s) OR
                (first_name = %s AND last_name = %s) OR
                (SUBSTRING(first_name, 1, 1) = %s AND last_name = %s)
            """)
            params.extend([
                f"{first_name}%", last_name,  # First name starts with
                first_name, last_name,        # Exact match
                first_name[0] if first_name else '', last_name  # First initial
            ])
        elif last_name:
            conditions.append("last_name = %s")
            params.append(last_name)
        
        # Location matching
        if city and state:
            conditions.append("(city = %s AND state = %s)")
            params.extend([city, state])
        elif state:
            conditions.append("state = %s")
            params.append(state)
        
        if not conditions:
            return []
        
        query = f"""
        SELECT *, 
               CASE 
                   WHEN first_name = %s AND last_name = %s AND city = %s AND state = %s THEN 50
                   WHEN first_name = %s AND last_name = %s AND state = %s AND city != %s THEN 25
                   WHEN SUBSTRING(first_name, 1, 1) = %s AND last_name = %s AND state = %s THEN 15
                   WHEN first_name = %s AND last_name = %s THEN 10
                   ELSE 5
               END as match_score
        FROM existing_people 
        WHERE ({' AND '.join(conditions)})
        ORDER BY match_score DESC
        LIMIT 10
        """
        
        # Add scoring parameters
        score_params = [
            first_name or '', last_name or '', city or '', state or '',  # Score 50
            first_name or '', last_name or '', state or '', city or '',  # Score 25  
            first_name[0] if first_name else '', last_name or '', state or '',  # Score 15
            first_name or '', last_name or ''  # Score 10
        ]
        
        all_params = score_params + params
        
        return self.db.execute_query(query, tuple(all_params))


class ProcessingDataDAO:
    """Data Access Object for processing pipeline data"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def save_downloaded_patents(self, patents: List[Dict[str, Any]], batch_id: str) -> int:
        """Save downloaded patents from Step 0"""
        for patent in patents:
            patent['download_batch_id'] = batch_id
            patent['raw_data'] = json.dumps(patent.get('raw_data', {}))
        
        return self.db.insert_batch('downloaded_patents', patents)
    
    def save_downloaded_people(self, people: List[Dict[str, Any]], batch_id: str) -> int:
        """Save people from downloaded patents"""
        for person in people:
            person['download_batch_id'] = batch_id
        
        return self.db.insert_batch('downloaded_people', people)
    
    def save_match_results(self, matches: List[Dict[str, Any]]) -> int:
        """Save person matching results from Step 1"""
        return self.db.insert_batch('person_matches', matches)
    
    def get_people_for_enrichment(self, verification_needed: bool = None) -> List[Dict[str, Any]]:
        """Get people ready for enrichment"""
        query = """
        SELECT * FROM people_for_enrichment 
        WHERE enrichment_status = 'pending'
        """
        params = []
        
        if verification_needed is not None:
            query += " AND verification_needed = %s"
            params.append(verification_needed)
        
        query += " ORDER BY match_score ASC"
        
        return self.db.execute_query(query, tuple(params) if params else None)
    
    def save_enrichment_batch(self, people: List[Dict[str, Any]]) -> int:
        """Save people selected for enrichment"""
        return self.db.insert_batch('people_for_enrichment', people)
    
    def save_enriched_results(self, results: List[Dict[str, Any]]) -> int:
        """Save enrichment API results"""
        return self.db.insert_batch('enriched_people', results)
    
    def update_enrichment_status(self, person_id: int, status: str, error_msg: str = None):
        """Update enrichment status for a person"""
        query = """
        UPDATE people_for_enrichment 
        SET enrichment_status = %s, enriched_at = NOW()
        WHERE id = %s
        """
        params = [status, person_id]
        
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, tuple(params))
            conn.commit()
    
    def start_processing_batch(self, batch_id: str, step_name: str, config: Dict[str, Any]) -> bool:
        """Record start of a processing batch"""
        
        # Use a direct INSERT query instead of insert_batch for datetime functions
        query = """
        INSERT INTO processing_batches (id, step_name, status, config, started_at)
        VALUES (%s, %s, %s, %s, NOW())
        """
        
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (batch_id, step_name, 'started', json.dumps(config)))
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to start batch {batch_id}: {e}")
            return False

    def complete_processing_batch(self, batch_id: str, stats: Dict[str, Any], error_msg: str = None):
        """Mark a processing batch as complete"""
        status = 'failed' if error_msg else 'completed'
        
        query = """
        UPDATE processing_batches 
        SET status = %s, stats = %s, completed_at = NOW(), error_message = %s
        WHERE id = %s
        """
        
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (status, json.dumps(stats), error_msg, batch_id))
            conn.commit()


# =============================================================================
# DATABASE MIGRATION UTILITIES
# =============================================================================

class CSVToSQLMigrator:
    """Migrate existing CSV files to SQL database"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.existing_dao = ExistingDataDAO(db_manager)
    
    def migrate_csv_folder(self, csv_folder: str, batch_size: int = 1000) -> Dict[str, int]:
        """Migrate all CSV files in a folder to SQL"""
        csv_path = Path(csv_folder)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV folder not found: {csv_folder}")
        
        results = {'files_processed': 0, 'patents_imported': 0, 'people_imported': 0}
        
        csv_files = list(csv_path.glob("*.csv"))
        logger.info(f"Found {len(csv_files)} CSV files to migrate")
        
        for csv_file in csv_files:
            try:
                file_results = self._migrate_single_csv(csv_file, batch_size)
                results['files_processed'] += 1
                results['patents_imported'] += file_results['patents']
                results['people_imported'] += file_results['people']
                
                logger.info(f"Migrated {csv_file.name}: {file_results['patents']} patents, {file_results['people']} people")
                
            except Exception as e:
                logger.error(f"Failed to migrate {csv_file}: {e}")
                continue
        
        return results
    
    def _migrate_single_csv(self, csv_file: Path, batch_size: int) -> Dict[str, int]:
        """Migrate a single CSV file"""
        import pandas as pd
        
        df = pd.read_csv(csv_file, encoding='utf-8', low_memory=False)
        results = {'patents': 0, 'people': 0}
        
        # Extract patents
        patents = self._extract_patents_from_df(df, csv_file.name)
        if patents:
            # Process in batches
            for i in range(0, len(patents), batch_size):
                batch = patents[i:i + batch_size]
                self.existing_dao.bulk_insert_patents(batch)
                results['patents'] += len(batch)
        
        # Extract people
        people = self._extract_people_from_df(df, csv_file.name)
        if people:
            # Process in batches
            for i in range(0, len(people), batch_size):
                batch = people[i:i + batch_size]
                self.existing_dao.bulk_insert_people(batch)
                results['people'] += len(batch)
        
        return results
    
    def _extract_patents_from_df(self, df: pd.DataFrame, filename: str) -> List[Dict[str, Any]]:
        """Extract patent records from DataFrame"""
        patents = []
        
        # Find patent number column
        patent_col = None
        patent_columns = ['patent_number', 'patent_id', 'publication_number', 'doc_number']
        for col in df.columns:
            if any(pcol.lower() in col.lower() for pcol in patent_columns):
                patent_col = col
                break
        
        if not patent_col:
            return patents
        
        # Find other relevant columns
        title_col = self._find_column(df.columns, ['patent_title', 'title', 'invention_title'])
        date_col = self._find_column(df.columns, ['patent_date', 'date', 'publication_date'])
        abstract_col = self._find_column(df.columns, ['patent_abstract', 'abstract', 'description'])
        
        for _, row in df.iterrows():
            patent_number = self._clean_patent_number(str(row[patent_col]))
            if patent_number:
                patent_record = {
                    'patent_number': patent_number,
                    'patent_title': str(row.get(title_col, ''))[:500] if title_col else '',
                    'patent_date': str(row.get(date_col, '')) if date_col else None,
                    'patent_abstract': str(row.get(abstract_col, ''))[:1000] if abstract_col else '',
                    'source_file': filename
                }
                patents.append(patent_record)
        
        return patents
    
    def _extract_people_from_df(self, df: pd.DataFrame, filename: str) -> List[Dict[str, Any]]:
        """Extract people records from DataFrame"""
        people = []
        
        # Find name columns
        first_name_col = self._find_column(df.columns, 
            ['first_name', 'firstname', 'fname', 'first', 'inventor_first'])
        last_name_col = self._find_column(df.columns, 
            ['last_name', 'lastname', 'lname', 'last', 'inventor_last'])
        
        if not (first_name_col or last_name_col):
            return people
        
        # Find location and other columns
        city_col = self._find_column(df.columns, ['city', 'inventor_city', 'location_city'])
        state_col = self._find_column(df.columns, ['state', 'inventor_state', 'location_state'])
        country_col = self._find_column(df.columns, ['country', 'inventor_country'])
        address_col = self._find_column(df.columns, ['address', 'addr', 'location_add1'])
        zip_col = self._find_column(df.columns, ['zip', 'postal_code', 'zipcode'])
        phone_col = self._find_column(df.columns, ['phone', 'telephone', 'phone_number'])
        email_col = self._find_column(df.columns, ['email', 'email_address', 'e_mail'])
        company_col = self._find_column(df.columns, ['company', 'organization', 'assignee_org'])
        
        for _, row in df.iterrows():
            first_name = self._clean_string(row.get(first_name_col))
            last_name = self._clean_string(row.get(last_name_col))
            
            if first_name or last_name:
                person_record = {
                    'first_name': first_name,
                    'last_name': last_name,
                    'city': self._clean_string(row.get(city_col, '')) if city_col else '',
                    'state': self._clean_string(row.get(state_col, '')) if state_col else '',
                    'country': self._clean_string(row.get(country_col, '')) if country_col else '',
                    'address': self._clean_string(row.get(address_col, '')) if address_col else '',
                    'zip': self._clean_string(row.get(zip_col, '')) if zip_col else '',
                    'phone': self._clean_string(row.get(phone_col, '')) if phone_col else '',
                    'email': self._clean_string(row.get(email_col, '')) if email_col else '',
                    'company_name': self._clean_string(row.get(company_col, '')) if company_col else '',
                    'record_type': 'inventor',  # Default, could be smarter
                    'source_file': filename
                }
                people.append(person_record)
        
        return people
    
    def _find_column(self, columns: List[str], patterns: List[str]) -> Optional[str]:
        """Find column matching patterns"""
        for col in columns:
            col_lower = col.lower()
            if any(pattern.lower() in col_lower for pattern in patterns):
                return col
        return None
    
    def _clean_string(self, value) -> str:
        """Clean string values"""
        if not value or str(value).lower() in ['nan', 'none', 'null', '']:
            return ''
        return str(value).strip()
    
    def _clean_patent_number(self, patent_num: str) -> Optional[str]:
        """Clean patent number"""
        if not patent_num or str(patent_num).lower() in ['nan', 'none', '', 'null']:
            return None
        
        clean_num = str(patent_num).strip().upper()
        clean_num = clean_num.replace('US', '').replace('USPTO', '')
        clean_num = clean_num.replace(',', '').replace(' ', '').replace('-', '')
        clean_num = clean_num.lstrip('0')
        
        return clean_num if clean_num and clean_num.isdigit() else None