const mysql = require('mysql2/promise');

const CONFIG = {
  host: process.env.SQL_HOST || 'mysql-patent-nationalengravers.mysql.database.azure.com',
  user: process.env.SQL_USER || 'rootuser',
  password: process.env.SQL_PASSWORD || 'S3cur3Adm1n1124!',
  database: process.env.SQL_DATABASE || 'patent_data',
  ssl: { rejectUnauthorized: false },
  connectTimeout: 15000,
  supportBigNumbers: true,
  bigNumberStrings: true
};

async function main() {
  let conn;
  try {
    console.log('🔌 Attempting to connect to cloud MySQL...');
    const start = Date.now();
    conn = await mysql.createConnection(CONFIG);
    console.log(`✅ Connected in ${Date.now() - start} ms`);

    const [versionRows] = await conn.query('SELECT VERSION() AS version');
    console.log(`🆔 Server version: ${versionRows[0]?.version}`);

    const [dbRows] = await conn.query('SELECT DATABASE() AS db');
    console.log(`📁 Active database: ${dbRows[0]?.db}`);

    const [tableRows] = await conn.query(`
      SELECT TABLE_NAME, TABLE_ROWS
      FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = ?
      ORDER BY TABLE_ROWS DESC
      LIMIT 5
    `, [CONFIG.database]);
    console.log('📊 Top tables by row count:');
    for (const row of tableRows) {
      const name = row.TABLE_NAME;
      const count = row.TABLE_ROWS ?? 'unknown';
      console.log(`   • ${name}: ${count.toLocaleString ? count.toLocaleString() : count}`);
    }

    const [existingPeopleCount] = await conn.query('SELECT COUNT(*) AS cnt FROM existing_people LIMIT 1');
    console.log(`👥 existing_people rows: ${Number(existingPeopleCount[0]?.cnt || 0).toLocaleString()}`);

    const [testRow] = await conn.query('SELECT 1 AS ping');
    console.log(`📶 Test query result: ${testRow[0]?.ping}`);
  } catch (error) {
    console.error('❌ Connection or query failed:', error.message);
    if (error.code) {
      console.error(`   ↳ MySQL code: ${error.code}`);
    }
    if (error.errno) {
      console.error(`   ↳ Errno: ${error.errno}`);
    }
    if (error.sqlState) {
      console.error(`   ↳ SQLState: ${error.sqlState}`);
    }
    process.exitCode = 1;
  } finally {
    if (conn) {
      try {
        await conn.end();
      } catch (closeErr) {
        console.error('⚠️  Error closing connection:', closeErr.message);
      }
    }
  }
}

main();
