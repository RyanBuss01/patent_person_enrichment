const mysql = require('mysql2/promise');

// Configuration
const LOCAL_CONFIG = {
  host: 'localhost',
  user: 'root',
  password: 'password',
  database: 'patent_data',
  multipleStatements: true
};

const CLOUD_CONFIG = {
  host: 'mysql-patent-nationalengravers.mysql.database.azure.com',
  user: 'rootuser',
  password: 'S3cur3Adm1n1124!',
  database: 'patent_data',
  multipleStatements: true,
  ssl: { rejectUnauthorized: false } // Azure MySQL requires SSL
};

const BATCH_SIZE = 1000; // Number of rows to insert per batch
const EXCLUDED_TABLES = ['existing_people_new', 'existing_people_old'];

class DatabaseReplicator {
  constructor() {
    this.localConn = null;
    this.cloudConn = null;
  }

  async connect() {
    console.log('Connecting to databases...');
    this.localConn = await mysql.createConnection(LOCAL_CONFIG);
    this.cloudConn = await mysql.createConnection(CLOUD_CONFIG);
    console.log('Connected successfully!\n');
  }

  async disconnect() {
    if (this.localConn) await this.localConn.end();
    if (this.cloudConn) await this.cloudConn.end();
    console.log('\nDisconnected from databases.');
  }

  async getTables() {
    const [rows] = await this.localConn.query('SHOW TABLES');
    const tableKey = Object.keys(rows[0])[0];
    return rows
      .map(row => row[tableKey])
      .filter(table => !EXCLUDED_TABLES.includes(table));
  }

  async getTableSchema(tableName) {
    const [rows] = await this.localConn.query(`SHOW CREATE TABLE \`${tableName}\``);
    if (!rows || rows.length === 0 || !rows[0]['Create Table']) {
      throw new Error(`Could not get table schema for ${tableName}`);
    }
    return rows[0]['Create Table'];
  }

  async getRowCount(tableName) {
    const [rows] = await this.localConn.query(`SELECT COUNT(*) as count FROM \`${tableName}\``);
    return rows[0].count;
  }

  async dropTableIfExists(tableName) {
    await this.cloudConn.query(`DROP TABLE IF EXISTS \`${tableName}\``);
  }

  async createTable(tableName, createStatement) {
    if (!createStatement) {
      throw new Error(`Invalid create statement for table ${tableName}`);
    }
    await this.cloudConn.query(createStatement);
  }

  escapeValue(value) {
    if (value === null || value === undefined) {
      return 'NULL';
    }
    
    if (typeof value === 'number') {
      return value;
    }
    
    if (typeof value === 'boolean') {
      return value ? 1 : 0;
    }
    
    if (value instanceof Date) {
      return this.cloudConn.escape(value);
    }
    
    if (Buffer.isBuffer(value)) {
      return this.cloudConn.escape(value);
    }
    
    // Handle objects and arrays - convert to JSON string
    if (typeof value === 'object') {
      return this.cloudConn.escape(JSON.stringify(value));
    }
    
    // Handle strings
    return this.cloudConn.escape(value);
  }

  async copyTableData(tableName, totalRows) {
    if (totalRows === 0) {
      console.log(`  No data to copy.`);
      return;
    }

    let offset = 0;
    let copiedRows = 0;

    while (offset < totalRows) {
      // Fetch batch from local
      const [rows] = await this.localConn.query(
        `SELECT * FROM \`${tableName}\` LIMIT ${BATCH_SIZE} OFFSET ${offset}`
      );

      if (rows.length === 0) break;

      // Get column names from first row
      const columns = Object.keys(rows[0]);
      const columnList = columns.map(col => `\`${col}\``).join(', ');

      // Prepare values for batch insert using placeholders
      const valuePlaceholders = rows.map(() => 
        `(${columns.map(() => '?').join(', ')})`
      ).join(', ');

      // Flatten all values in correct order
      const allValues = [];
      for (const row of rows) {
        for (const col of columns) {
          const value = row[col];
          
          // Handle special types
          if (value === null || value === undefined) {
            allValues.push(null);
          } else if (Buffer.isBuffer(value)) {
            allValues.push(value);
          } else if (value instanceof Date) {
            allValues.push(value);
          } else if (typeof value === 'object') {
            // Convert objects/arrays to JSON strings
            allValues.push(JSON.stringify(value));
          } else {
            allValues.push(value);
          }
        }
      }

      // Insert batch into cloud using parameterized query
      const insertQuery = `INSERT INTO \`${tableName}\` (${columnList}) VALUES ${valuePlaceholders}`;
      await this.cloudConn.execute(insertQuery, allValues);

      copiedRows += rows.length;
      offset += BATCH_SIZE;

      // Progress indicator
      const progress = ((copiedRows / totalRows) * 100).toFixed(1);
      process.stdout.write(`\r  Progress: ${copiedRows}/${totalRows} rows (${progress}%)`);
    }

    console.log(`\r  ✓ Copied ${copiedRows} rows successfully.`);
  }

  async replicateTable(tableName) {
    console.log(`\n[${tableName}]`);
    
    try {
      // Get row count
      const rowCount = await this.getRowCount(tableName);
      console.log(`  Rows in local: ${rowCount}`);

      // Get table structure
      console.log(`  Getting table schema...`);
      const createStatement = await this.getTableSchema(tableName);
      
      if (!createStatement) {
        throw new Error(`Failed to retrieve CREATE TABLE statement`);
      }

      // Drop existing table in cloud
      console.log(`  Dropping cloud table if exists...`);
      await this.dropTableIfExists(tableName);

      // Create table in cloud
      console.log(`  Creating table in cloud...`);
      await this.createTable(tableName, createStatement);

      // Copy data
      if (rowCount > 0) {
        console.log(`  Copying data...`);
        await this.copyTableData(tableName, rowCount);
      } else {
        console.log(`  No data to copy.`);
      }

      console.log(`  ✓ Table replication complete!`);
    } catch (error) {
      console.error(`\n  ✗ Error replicating table: ${error.message}`);
      console.error(`  Stack: ${error.stack}`);
      throw error;
    }
  }

  async replicate() {
    try {
      await this.connect();

      // Get all tables
      console.log('Fetching table list...');
      const tables = await this.getTables();
      console.log(`Found ${tables.length} tables to replicate (excluding ${EXCLUDED_TABLES.join(', ')})\n`);

      // Disable foreign key checks in cloud
      await this.cloudConn.query('SET FOREIGN_KEY_CHECKS = 0');

      // Replicate each table
      for (let i = 0; i < tables.length; i++) {
        console.log(`\n========== Table ${i + 1}/${tables.length} ==========`);
        await this.replicateTable(tables[i]);
      }

      // Re-enable foreign key checks
      await this.cloudConn.query('SET FOREIGN_KEY_CHECKS = 1');

      console.log('\n\n========================================');
      console.log('✓ REPLICATION COMPLETE!');
      console.log('========================================');

    } catch (error) {
      console.error('\n\n========================================');
      console.error('✗ REPLICATION FAILED!');
      console.error('========================================');
      console.error('Error:', error.message);
      throw error;
    } finally {
      await this.disconnect();
    }
  }
}

// Run the replication
(async () => {
  const replicator = new DatabaseReplicator();
  
  console.log('========================================');
  console.log('DATABASE REPLICATION SCRIPT');
  console.log('========================================');
  console.log('Source: Local MySQL Database');
  console.log('Target: Azure MySQL Cloud Database');
  console.log('========================================\n');

  try {
    await replicator.replicate();
    process.exit(0);
  } catch (error) {
    console.error('\nFatal error:', error);
    process.exit(1);
  }
})();