#!/usr/bin/env node

const mysql = require('mysql2/promise');

async function replicateDatabase(sourceConfig, targetConfig) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  let logMessages = [];
  
  const log = (message) => {
    logMessages.push(message);
    console.log(message);
  };
  
  log(`[${timestamp}] Starting complete database replication`);
  
  let sourceConn = null;
  let targetConn = null;
  
  try {
    // Create connections
    log('Connecting to source database...');
    sourceConn = await mysql.createConnection(sourceConfig);
    
    log('Connecting to target database...');
    targetConn = await mysql.createConnection(targetConfig);
    
    // Get database names
    const [sourceDbResult] = await sourceConn.query('SELECT DATABASE() as db');
    const [targetDbResult] = await targetConn.query('SELECT DATABASE() as db');
    
    const sourceDb = sourceDbResult[0].db;
    const targetDb = targetDbResult[0].db;
    
    log(`Source: ${sourceDb} -> Target: ${targetDb}`);
    
    // Disable foreign key checks for smooth operation
    await targetConn.query('SET FOREIGN_KEY_CHECKS = 0');
    
    // Get all tables from source
    const [sourceTables] = await sourceConn.query('SHOW TABLES');
    const sourceTableNames = sourceTables.map(row => Object.values(row)[0]);
    
    log(`Found ${sourceTableNames.length} tables to replicate`);
    
    // Identify tables vs views
    const tablesAndViews = await Promise.all(sourceTableNames.map(async (table) => {
      const [createInfo] = await sourceConn.query(`SHOW CREATE TABLE \`${table}\``);
      const isView = createInfo[0]['Create View'] !== undefined;
      return { 
        name: table, 
        isView, 
        createStatement: isView ? createInfo[0]['Create View'] : createInfo[0]['Create Table']
      };
    }));
    
    const tables = tablesAndViews.filter(t => !t.isView);
    const views = tablesAndViews.filter(t => t.isView);
    
    log(`${tables.length} tables, ${views.length} views`);
    
    // Step 1: Drop all existing tables and views in target
    log('\nDropping existing tables and views in target...');
    const [existingTables] = await targetConn.query('SHOW TABLES');
    for (const tableRow of existingTables) {
      const tableName = Object.values(tableRow)[0];
      try {
        await targetConn.query(`DROP VIEW IF EXISTS \`${tableName}\``);
        await targetConn.query(`DROP TABLE IF EXISTS \`${tableName}\``);
        log(`  Dropped: ${tableName}`);
      } catch (err) {
        log(`  Warning: Could not drop ${tableName}: ${err.message}`);
      }
    }
    
    // Step 2: Create all tables (structure only)
    log('\nCreating table structures...');
    for (const table of tables) {
      log(`  Creating table: ${table.name}`);
      try {
        // Clean the create statement to remove AUTO_INCREMENT values
        let createStatement = table.createStatement;
        createStatement = createStatement.replace(/AUTO_INCREMENT=\d+/gi, '');
        
        await targetConn.query(createStatement);
      } catch (err) {
        log(`  Error creating table ${table.name}: ${err.message}`);
      }
    }
    
    // Step 3: Copy data for all tables
    log('\nCopying table data...');
    for (const table of tables) {
      const tableName = table.name;
      log(`  Copying data for: ${tableName}`);
      
      try {
        // Get column information from both source and target to ensure compatibility
        const [sourceColumns] = await sourceConn.query(`SHOW COLUMNS FROM \`${tableName}\``);
        const [targetColumns] = await targetConn.query(`SHOW COLUMNS FROM \`${tableName}\``);
        
        const sourceColumnNames = sourceColumns.map(col => col.Field);
        const targetColumnNames = targetColumns.map(col => col.Field);
        
        // Use only columns that exist in both tables
        const commonColumns = sourceColumnNames.filter(col => targetColumnNames.includes(col));
        
        if (commonColumns.length === 0) {
          log(`    No common columns found, skipping table`);
          continue;
        }
        
        if (sourceColumnNames.length !== targetColumnNames.length) {
          log(`    Column mismatch: source(${sourceColumnNames.length}) vs target(${targetColumnNames.length}), using common columns only`);
          log(`    Source columns: ${sourceColumnNames.join(', ')}`);
          log(`    Target columns: ${targetColumnNames.join(', ')}`);
          log(`    Common columns: ${commonColumns.join(', ')}`);
        }
        
        // Get row count
        const [countResult] = await sourceConn.query(`SELECT COUNT(*) as count FROM \`${tableName}\``);
        const totalRows = countResult[0].count;
        
        if (totalRows === 0) {
          log(`    No data to copy`);
          continue;
        }
        
        log(`    ${totalRows} rows to copy using ${commonColumns.length} columns`);
        
        // Copy data in batches to handle large tables
        const batchSize = 10000; // Back to larger batch size for speed
        let offset = 0;
        let copiedRows = 0;
        
        // Build column list for SELECT and INSERT
        const columnList = commonColumns.map(col => `\`${col}\``).join(', ');
        
        while (offset < totalRows) {
          const [rows] = await sourceConn.query(
            `SELECT ${columnList} FROM \`${tableName}\` LIMIT ${batchSize} OFFSET ${offset}`
          );
          
          if (rows.length === 0) break;
          
          // Prepare INSERT statement using only common columns
          if (rows.length > 0) {
            try {
              // Build batch insert with escaped values for speed
              const values = rows.map(row => {
                const rowValues = commonColumns.map(col => {
                  const value = row[col];
                  if (value === null || value === undefined) return 'NULL';
                  if (typeof value === 'string') return mysql.escape(value);
                  if (value instanceof Date) return mysql.escape(value);
                  if (Buffer.isBuffer(value)) return mysql.escape(value);
                  if (typeof value === 'boolean') return value ? 1 : 0;
                  if (typeof value === 'object') {
                    // Handle JSON objects by stringifying them
                    return mysql.escape(JSON.stringify(value));
                  }
                  return mysql.escape(value);
                });
                return `(${rowValues.join(', ')})`;
              });
              
              const batchInsertQuery = `INSERT INTO \`${tableName}\` (${columnList}) VALUES ${values.join(', ')}`;
              await targetConn.query(batchInsertQuery);
              copiedRows += rows.length;
              
              // Show progress updates for any table with more than 1000 rows
              if (totalRows > 1000 && copiedRows % 5000 === 0) {
                const percentage = ((copiedRows / totalRows) * 100).toFixed(1);
                log(`    Progress: ${copiedRows.toLocaleString()}/${totalRows.toLocaleString()} rows (${percentage}%)`);
              }
              
            } catch (err) {
              log(`    Error in batch insert: ${err.message}`);
              
              // Fallback: Insert row by row only if batch fails
              for (const row of rows) {
                try {
                  const singleRowValues = commonColumns.map(col => {
                    const value = row[col];
                    if (value === null || value === undefined) return 'NULL';
                    if (typeof value === 'string') return mysql.escape(value);
                    if (value instanceof Date) return mysql.escape(value);
                    if (Buffer.isBuffer(value)) return mysql.escape(value);
                    if (typeof value === 'boolean') return value ? 1 : 0;
                    if (typeof value === 'object') {
                      // Handle JSON objects by stringifying them
                      return mysql.escape(JSON.stringify(value));
                    }
                    return mysql.escape(value);
                  });
                  
                  const singleInsertQuery = `INSERT INTO \`${tableName}\` (${columnList}) VALUES (${singleRowValues.join(', ')})`;
                  await targetConn.query(singleInsertQuery);
                  copiedRows++;
                } catch (rowErr) {
                  log(`    Failed to insert row: ${rowErr.message}`);
                  // Log the problematic row for debugging (truncated)
                  const rowData = JSON.stringify(row);
                  log(`    Problematic data: ${rowData.length > 200 ? rowData.substring(0, 200) + '...' : rowData}`);
                }
              }
            }
          }
          
          offset += batchSize;
        }
        
        log(`    Completed: ${copiedRows}/${totalRows} rows copied`);
        
      } catch (err) {
        log(`  Error copying data for ${tableName}: ${err.message}`);
      }
    }
    
    // Step 4: Create views
    if (views.length > 0) {
      log('\nCreating views...');
      
      // Sort views by dependencies (simple approach)
      const maxAttempts = views.length * 2;
      let attempts = 0;
      const remainingViews = [...views];
      
      while (remainingViews.length > 0 && attempts < maxAttempts) {
        const view = remainingViews.shift();
        attempts++;
        
        try {
          // Clean the create statement
          let createStatement = view.createStatement;
          createStatement = createStatement.replace(/DEFINER=`[^`]+`@`[^`]+`/g, '');
          
          await targetConn.query(createStatement);
          log(`  Created view: ${view.name}`);
        } catch (err) {
          log(`  Error creating view ${view.name}, will retry: ${err.message}`);
          remainingViews.push(view); // Try again later
        }
      }
      
      if (remainingViews.length > 0) {
        log(`  Warning: ${remainingViews.length} views could not be created due to dependency issues`);
      }
    }
    
    // Step 5: Re-enable foreign key checks
    await targetConn.query('SET FOREIGN_KEY_CHECKS = 1');
    
    log('\n✅ Database replication completed successfully');
    return {
      success: true,
      logMessages: logMessages
    };
    
  } catch (error) {
    log(`\n❌ ERROR: ${error.message}`);
    log(error.stack);
    
    try {
      if (targetConn) {
        await targetConn.query('SET FOREIGN_KEY_CHECKS = 1');
      }
    } catch (e) {
      log(`Error while trying to re-enable foreign key checks: ${e.message}`);
    }
    
    return {
      success: false,
      error: error.message,
      logMessages: logMessages
    };
  } finally {
    if (sourceConn) await sourceConn.end();
    if (targetConn) await targetConn.end();
  }
}

// Main execution
(async () => {
  // Local database configuration
  const sourceConfig = {
    host: 'localhost',
    user: 'root',
    password: 'password',
    database: 'patent_data',
    multipleStatements: true
  };
  
  // Cloud database configuration
  const targetConfig = {
    host: 'mysql-patent-nationalengravers.mysql.database.azure.com',
    user: 'rootuser',
    password: 'S3cur3Adm1n1124!',
    database: 'patent_data',
    multipleStatements: true,
    ssl: {
      rejectUnauthorized: false
    }
  };
  
  console.log('🚀 Starting complete database replication...');
  console.log(`Source: ${sourceConfig.host}/${sourceConfig.database}`);
  console.log(`Target: ${targetConfig.host}/${targetConfig.database}`);
  console.log('\n⚠️  WARNING: This will completely replace the target database!');
  
  const { success, error } = await replicateDatabase(sourceConfig, targetConfig);
  
  if (!success) {
    console.error('\n❌ Replication failed:', error);
    process.exit(1);
  }
  
  console.log('\n🎉 Database replication completed successfully!');
})();