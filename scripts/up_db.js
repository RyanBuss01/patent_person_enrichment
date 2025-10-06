const mysql = require('mysql2/promise');
const crypto = require('crypto');
const fs = require('fs').promises;
const path = require('path');

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
  ssl: { rejectUnauthorized: false }
};

const BATCH_SIZE = 5000; // Rows to fetch per batch
const INSERT_BATCH_SIZE = 500; // Rows to insert at once (reduced for stability)
const EXCLUDED_TABLES = ['existing_people_new', 'existing_people_old'];
const PROGRESS_FILE = path.join(__dirname, 'update_progress.json');

class IncrementalUpdater {
  constructor() {
    this.localConn = null;
    this.cloudConn = null;
    this.progress = {};
  }

  async loadProgress() {
    try {
      const data = await fs.readFile(PROGRESS_FILE, 'utf8');
      this.progress = JSON.parse(data);
      console.log('Loaded progress from previous run\n');
    } catch (error) {
      this.progress = {};
    }
  }

  async saveProgress() {
    await fs.writeFile(PROGRESS_FILE, JSON.stringify(this.progress, null, 2));
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
    const [rows] = await this.localConn.query('SHOW FULL TABLES WHERE Table_type = "BASE TABLE"');
    const tableKey = Object.keys(rows[0])[0];
    return rows
      .map(row => row[tableKey])
      .filter(table => !EXCLUDED_TABLES.includes(table));
  }

  async getTablePrimaryKey(tableName) {
    const [rows] = await this.localConn.query(`
      SELECT COLUMN_NAME 
      FROM information_schema.KEY_COLUMN_USAGE 
      WHERE TABLE_SCHEMA = ? 
        AND TABLE_NAME = ? 
        AND CONSTRAINT_NAME = 'PRIMARY'
      ORDER BY ORDINAL_POSITION
    `, [LOCAL_CONFIG.database, tableName]);
    
    return rows.map(row => row.COLUMN_NAME);
  }

  async getRowCount(conn, tableName) {
    const [rows] = await conn.query(`SELECT COUNT(*) as count FROM \`${tableName}\``);
    return rows[0].count;
  }

  async getMaxId(conn, tableName, idColumn) {
    try {
      const [rows] = await conn.query(
        `SELECT MAX(\`${idColumn}\`) as max_id FROM \`${tableName}\``
      );
      return rows[0].max_id || 0;
    } catch (error) {
      return null; // Column doesn't exist or isn't numeric
    }
  }

  async getAllColumns(tableName) {
    const [rows] = await this.localConn.query(`
      SELECT COLUMN_NAME 
      FROM information_schema.COLUMNS 
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
      ORDER BY ORDINAL_POSITION
    `, [LOCAL_CONFIG.database, tableName]);
    
    return rows.map(row => row.COLUMN_NAME);
  }

  async getColumnType(tableName, columnName) {
    const [rows] = await this.localConn.query(`
      SELECT DATA_TYPE, COLUMN_TYPE
      FROM information_schema.COLUMNS 
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
    `, [LOCAL_CONFIG.database, tableName, columnName]);
    
    return rows[0];
  }

  async insertBatch(tableName, rows, startIdx, totalRows) {
    if (rows.length === 0) return 0;

    const columns = Object.keys(rows[0]);
    const columnList = columns.map(col => `\`${col}\``).join(', ');
    
    const valuePlaceholders = rows.map(() => 
      `(${columns.map(() => '?').join(', ')})`
    ).join(', ');
    
    const allValues = [];
    for (const row of rows) {
      for (const col of columns) {
        const value = row[col];
        
        if (value === null || value === undefined) {
          allValues.push(null);
        } else if (Buffer.isBuffer(value)) {
          allValues.push(value);
        } else if (value instanceof Date) {
          allValues.push(value);
        } else if (typeof value === 'object') {
          allValues.push(JSON.stringify(value));
        } else {
          allValues.push(value);
        }
      }
    }
    
    const insertQuery = `INSERT INTO \`${tableName}\` (${columnList}) VALUES ${valuePlaceholders}`;
    
    try {
      await this.cloudConn.execute(insertQuery, allValues);
      return rows.length;
    } catch (error) {
      // If batch fails, try one by one
      console.log(`\n  Batch insert failed, trying row by row...`);
      let inserted = 0;
      for (const row of rows) {
        try {
          const singleValuePlaceholders = `(${columns.map(() => '?').join(', ')})`;
          const singleValues = columns.map(col => {
            const value = row[col];
            if (value === null || value === undefined) return null;
            if (Buffer.isBuffer(value)) return value;
            if (value instanceof Date) return value;
            if (typeof value === 'object') return JSON.stringify(value);
            return value;
          });
          
          const singleInsertQuery = `INSERT INTO \`${tableName}\` (${columnList}) VALUES ${singleValuePlaceholders}`;
          await this.cloudConn.execute(singleInsertQuery, singleValues);
          inserted++;
        } catch (rowError) {
          console.log(`  Failed to insert row: ${rowError.message}`);
        }
      }
      return inserted;
    }
  }

  async insertMissingRowsStream(tableName, localCount, cloudMaxId, idColumn) {
    console.log(`  Inserting rows with ${idColumn} > ${cloudMaxId}...`);
    
    let offset = 0;
    let insertedTotal = 0;
    let batch = [];
    const rowsToInsert = localCount - cloudMaxId;
    
    // Stream rows from local where id > cloudMaxId
    while (true) {
      const [rows] = await this.localConn.query(
        `SELECT * FROM \`${tableName}\` WHERE \`${idColumn}\` > ? LIMIT ${BATCH_SIZE} OFFSET ${offset}`,
        [cloudMaxId]
      );
      
      if (rows.length === 0) break;
      
      for (const row of rows) {
        batch.push(row);
        
        // When batch is full, insert it
        if (batch.length >= INSERT_BATCH_SIZE) {
          const inserted = await this.insertBatch(tableName, batch, insertedTotal, rowsToInsert);
          insertedTotal += inserted;
          
          // Save progress
          this.progress[tableName] = {
            lastInsertedId: batch[batch.length - 1][idColumn],
            insertedCount: insertedTotal
          };
          await this.saveProgress();
          
          const progress = ((insertedTotal / rowsToInsert) * 100).toFixed(1);
          process.stdout.write(`\r  Progress: ${insertedTotal.toLocaleString()}/${rowsToInsert.toLocaleString()} (${progress}%)`);
          
          batch = [];
        }
      }
      
      offset += BATCH_SIZE;
    }
    
    // Insert remaining batch
    if (batch.length > 0) {
      const inserted = await this.insertBatch(tableName, batch, insertedTotal, rowsToInsert);
      insertedTotal += inserted;
      
      this.progress[tableName] = {
        lastInsertedId: batch[batch.length - 1][idColumn],
        insertedCount: insertedTotal,
        completed: true
      };
      await this.saveProgress();
    }
    
    console.log(`\r  ✓ Inserted ${insertedTotal.toLocaleString()} rows successfully`);
    return insertedTotal;
  }

  async insertMissingRowsByPK(tableName, primaryKeys, localCount, cloudCount) {
    console.log(`  Finding missing rows by primary key...`);
    
    // Build cloud PK set in chunks to manage memory
    const cloudKeys = new Set();
    let cloudOffset = 0;
    
    while (cloudOffset < cloudCount) {
      const pkSelect = primaryKeys.map(pk => `\`${pk}\``).join(', ');
      const [rows] = await this.cloudConn.query(
        `SELECT ${pkSelect} FROM \`${tableName}\` LIMIT ${BATCH_SIZE} OFFSET ${cloudOffset}`
      );
      
      for (const row of rows) {
        const key = primaryKeys.map(pk => String(row[pk] ?? '')).join('|');
        cloudKeys.add(key);
      }
      
      cloudOffset += BATCH_SIZE;
      process.stdout.write(`\r  Loading cloud keys: ${Math.min(cloudOffset, cloudCount).toLocaleString()}/${cloudCount.toLocaleString()}`);
    }
    
    console.log(`\r  ✓ Loaded ${cloudKeys.size.toLocaleString()} cloud primary keys`);
    
    // Stream local rows and insert missing ones
    let localOffset = 0;
    let insertedTotal = 0;
    let batch = [];
    let checkedCount = 0;
    
    while (localOffset < localCount) {
      const [rows] = await this.localConn.query(
        `SELECT * FROM \`${tableName}\` LIMIT ${BATCH_SIZE} OFFSET ${localOffset}`
      );
      
      if (rows.length === 0) break;
      
      for (const row of rows) {
        checkedCount++;
        const key = primaryKeys.map(pk => String(row[pk] ?? '')).join('|');
        
        if (!cloudKeys.has(key)) {
          batch.push(row);
          
          if (batch.length >= INSERT_BATCH_SIZE) {
            const inserted = await this.insertBatch(tableName, batch, insertedTotal, localCount - cloudCount);
            insertedTotal += inserted;
            
            // Save progress
            this.progress[tableName] = {
              lastCheckedOffset: localOffset,
              insertedCount: insertedTotal
            };
            await this.saveProgress();
            
            process.stdout.write(`\r  Checked: ${checkedCount.toLocaleString()}/${localCount.toLocaleString()} | Inserted: ${insertedTotal.toLocaleString()}`);
            batch = [];
          }
        }
      }
      
      localOffset += BATCH_SIZE;
    }
    
    // Insert remaining
    if (batch.length > 0) {
      const inserted = await this.insertBatch(tableName, batch, insertedTotal, localCount - cloudCount);
      insertedTotal += inserted;
      
      this.progress[tableName] = {
        insertedCount: insertedTotal,
        completed: true
      };
      await this.saveProgress();
    }
    
    console.log(`\r  ✓ Inserted ${insertedTotal.toLocaleString()} missing rows`);
    return insertedTotal;
  }

  async updateTable(tableName) {
    console.log(`\n[${tableName}]`);
    
    // Check if already completed
    if (this.progress[tableName]?.completed) {
      console.log(`  ✓ Already completed in previous run`);
      return { table: tableName, inserted: this.progress[tableName].insertedCount, skipped: false };
    }
    
    try {
      const localCount = await this.getRowCount(this.localConn, tableName);
      const cloudCount = await this.getRowCount(this.cloudConn, tableName);
      
      console.log(`  Local rows: ${localCount.toLocaleString()}`);
      console.log(`  Cloud rows: ${cloudCount.toLocaleString()}`);
      
      if (localCount === cloudCount) {
        console.log(`  ✓ Row counts match - skipping`);
        return { table: tableName, inserted: 0, skipped: true };
      }
      
      if (localCount < cloudCount) {
        console.log(`  ⚠ Cloud has MORE rows than local - skipping`);
        return { table: tableName, inserted: 0, skipped: true };
      }
      
      const primaryKeys = await this.getTablePrimaryKey(tableName);
      
      // Strategy 1: If single numeric ID column, use max ID approach (FASTEST)
      if (primaryKeys.length === 1) {
        const idColumn = primaryKeys[0];
        const columnInfo = await this.getColumnType(tableName, idColumn);
        
        // Check if it's an integer type
        if (columnInfo && ['int', 'bigint', 'smallint', 'tinyint', 'mediumint'].includes(columnInfo.DATA_TYPE)) {
          console.log(`  Using optimized ID-based insertion (${idColumn})`);
          
          const localMaxId = await this.getMaxId(this.localConn, tableName, idColumn);
          const cloudMaxId = await this.getMaxId(this.cloudConn, tableName, idColumn);
          
          console.log(`  Local max ${idColumn}: ${localMaxId}`);
          console.log(`  Cloud max ${idColumn}: ${cloudMaxId}`);
          
          if (localMaxId > cloudMaxId) {
            const inserted = await this.insertMissingRowsStream(
              tableName, 
              localMaxId, 
              cloudMaxId, 
              idColumn
            );
            
            this.progress[tableName] = { completed: true, insertedCount: inserted };
            await this.saveProgress();
            
            console.log(`  ✓ Table update complete!`);
            return { table: tableName, inserted, skipped: false };
          } else {
            console.log(`  ✓ Cloud is up to date`);
            return { table: tableName, inserted: 0, skipped: true };
          }
        }
      }
      
      // Strategy 2: Primary key comparison (slower but works for any PK)
      if (primaryKeys.length > 0) {
        console.log(`  Using primary key comparison: ${primaryKeys.join(', ')}`);
        const inserted = await this.insertMissingRowsByPK(
          tableName, 
          primaryKeys, 
          localCount, 
          cloudCount
        );
        
        this.progress[tableName] = { completed: true, insertedCount: inserted };
        await this.saveProgress();
        
        console.log(`  ✓ Table update complete!`);
        return { table: tableName, inserted, skipped: false };
      }
      
      // No good strategy available
      console.log(`  ⚠ No primary key - manual sync required`);
      return { table: tableName, inserted: 0, skipped: true };
      
    } catch (error) {
      console.error(`\n  ✗ Error updating table: ${error.message}`);
      console.error(`  Stack: ${error.stack}`);
      throw error;
    }
  }

  async update() {
    try {
      await this.loadProgress();
      await this.connect();

      const tables = await this.getTables();
      console.log(`Found ${tables.length} tables to check\n`);

      await this.cloudConn.query('SET FOREIGN_KEY_CHECKS = 0');

      const results = [];
      
      for (let i = 0; i < tables.length; i++) {
        console.log(`\n========== Table ${i + 1}/${tables.length} ==========`);
        const result = await this.updateTable(tables[i]);
        results.push(result);
      }

      await this.cloudConn.query('SET FOREIGN_KEY_CHECKS = 1');

      // Summary
      console.log('\n\n========================================');
      console.log('UPDATE SUMMARY');
      console.log('========================================');
      
      const totalInserted = results.reduce((sum, r) => sum + r.inserted, 0);
      const skipped = results.filter(r => r.skipped).length;
      const updated = results.filter(r => !r.skipped && r.inserted > 0).length;
      
      console.log(`Total tables: ${tables.length}`);
      console.log(`Updated: ${updated}`);
      console.log(`Skipped: ${skipped}`);
      console.log(`Total inserted: ${totalInserted.toLocaleString()}`);
      
      if (updated > 0) {
        console.log('\nUpdated tables:');
        results
          .filter(r => !r.skipped && r.inserted > 0)
          .forEach(r => console.log(`  - ${r.table}: +${r.inserted.toLocaleString()} rows`));
      }
      
      // Clean up progress file on success
      await fs.unlink(PROGRESS_FILE).catch(() => {});
      
      console.log('\n✓ UPDATE COMPLETE!');
      console.log('========================================');

    } catch (error) {
      console.error('\n✗ UPDATE FAILED!');
      console.error('Error:', error.message);
      console.log('\nProgress saved. Run again to continue from last checkpoint.');
      throw error;
    } finally {
      await this.disconnect();
    }
  }
}

// Run the update
(async () => {
  const updater = new IncrementalUpdater();
  
  console.log('========================================');
  console.log('INCREMENTAL DATABASE UPDATE');
  console.log('========================================');
  console.log('Source: Local MySQL');
  console.log('Target: Azure MySQL');
  console.log('========================================\n');

  try {
    await updater.update();
    process.exit(0);
  } catch (error) {
    console.error('\nFatal error:', error);
    process.exit(1);
  }
})();