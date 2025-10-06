const mysql = require('mysql2/promise');

const LOCAL_CONFIG = {
  host: 'localhost',
  user: 'root',
  password: 'password',
  database: 'patent_data',
  multipleStatements: false,
  supportBigNumbers: true,
  bigNumberStrings: true
};

const CLOUD_CONFIG = {
  host: 'mysql-patent-nationalengravers.mysql.database.azure.com',
  user: 'rootuser',
  password: 'S3cur3Adm1n1124!',
  database: 'patent_data',
  multipleStatements: false,
  ssl: { rejectUnauthorized: false },
  supportBigNumbers: true,
  bigNumberStrings: true
};

const BUCKET_SIZE = 500000;
const DESIRED_COLUMNS = [
  'id',
  'first_name',
  'last_name',
  'city',
  'state',
  'zip',
  'country',
  'issue_date',
  'mail_to_add1',
  'mail_to_zip',
  'inventor_id',
  'mod_user'
];

function buildChecksumExpression(columns) {
  const pieces = columns.map(col => `COALESCE(CAST(\`${col}\` AS CHAR), '__NULL__')`);
  return `BIT_XOR(CRC32(CONCAT_WS('|', ${pieces.join(', ')} )))`;
}

async function fetchColumns(conn) {
  const [rows] = await conn.query('SHOW COLUMNS FROM existing_people');
  return rows.map(row => row.Field);
}

function normalizeRow(row) {
  return {
    count: Number(row?.count || 0),
    checksum: row?.checksum == null ? '0' : row.checksum.toString()
  };
}

async function getGlobalStats(conn, checksumExpr) {
  const query = `
    SELECT COUNT(*) AS count,
           MIN(id) AS minId,
           MAX(id) AS maxId,
           ${checksumExpr} AS checksum
    FROM existing_people
  `;
  const [rows] = await conn.query(query);
  const row = rows[0] || {};
  return {
    count: Number(row.count || 0),
    minId: row.minId != null ? Number(row.minId) : null,
    maxId: row.maxId != null ? Number(row.maxId) : null,
    checksum: row.checksum == null ? '0' : row.checksum.toString()
  };
}

async function fetchBucketPair(localConn, cloudConn, checksumExpr, start, end) {
  const bucketQuery = `
    SELECT COUNT(*) AS count,
           ${checksumExpr} AS checksum
    FROM existing_people
    WHERE id BETWEEN ? AND ?
  `;
  const [localRows, cloudRows] = await Promise.all([
    localConn.query(bucketQuery, [start, end]),
    cloudConn.query(bucketQuery, [start, end])
  ]);
  return {
    local: normalizeRow(localRows[0][0]),
    cloud: normalizeRow(cloudRows[0][0])
  };
}

function pickCommonColumns(localCols, cloudCols) {
  const cloudSet = new Set(cloudCols);
  const cols = DESIRED_COLUMNS.filter(col => localCols.includes(col) && cloudSet.has(col));
  if (!cols.includes('id')) cols.unshift('id');
  return cols;
}

function formatChecksum(checksum) {
  if (checksum === null || checksum === undefined) return '0';
  return checksum;
}

function logGlobalStats(label, stats) {
  console.log(`\n${label}`);
  console.log(`  Rows : ${stats.count.toLocaleString()}`);
  console.log(`  MinID: ${stats.minId != null ? stats.minId : 'n/a'}`);
  console.log(`  MaxID: ${stats.maxId != null ? stats.maxId : 'n/a'}`);
  console.log(`  Hash : ${formatChecksum(stats.checksum)}`);
}

(async () => {
  let localConn;
  let cloudConn;
  try {
    console.log('🔍 Checking existing_people sync between local and cloud databases...');
    localConn = await mysql.createConnection(LOCAL_CONFIG);
    cloudConn = await mysql.createConnection(CLOUD_CONFIG);

    const [localColumns, cloudColumns] = await Promise.all([
      fetchColumns(localConn),
      fetchColumns(cloudConn)
    ]);

    const columns = pickCommonColumns(localColumns, cloudColumns);
    if (columns.length === 0) {
      throw new Error('No common columns found for checksum comparison.');
    }

    console.log(`Using columns: ${columns.join(', ')}`);
    const checksumExpr = buildChecksumExpression(columns);

    const [localStats, cloudStats] = await Promise.all([
      getGlobalStats(localConn, checksumExpr),
      getGlobalStats(cloudConn, checksumExpr)
    ]);

    logGlobalStats('Local existing_people', localStats);
    logGlobalStats('Cloud existing_people', cloudStats);

    if (localStats.count !== cloudStats.count) {
      console.log('\n⚠️ Row counts differ between local and cloud.');
    } else {
      console.log('\n✅ Row counts match.');
    }

    if (formatChecksum(localStats.checksum) !== formatChecksum(cloudStats.checksum)) {
      console.log('⚠️ Global checksum differs; drilling into buckets...');
    } else {
      console.log('✅ Global checksum matches.');
    }

    const minCandidates = [];
    if (typeof localStats.minId === 'number') minCandidates.push(localStats.minId);
    if (typeof cloudStats.minId === 'number') minCandidates.push(cloudStats.minId);
    const maxCandidates = [];
    if (typeof localStats.maxId === 'number') maxCandidates.push(localStats.maxId);
    if (typeof cloudStats.maxId === 'number') maxCandidates.push(cloudStats.maxId);

    if (maxCandidates.length === 0) {
      console.log('\nBoth databases have no rows to compare.');
      return;
    }

    const startId = minCandidates.length ? Math.min(...minCandidates) : 0;
    const endId = Math.max(...maxCandidates);

    console.log(`\nAnalyzing buckets of ${BUCKET_SIZE.toLocaleString()} IDs from ${startId} to ${endId}...`);

    const bucketDifferences = [];
    let bucketIndex = 0;

    for (let rangeStart = startId; rangeStart <= endId; rangeStart += BUCKET_SIZE) {
      const rangeEnd = Math.min(rangeStart + BUCKET_SIZE - 1, endId);
      const { local, cloud } = await fetchBucketPair(localConn, cloudConn, checksumExpr, rangeStart, rangeEnd);
      bucketIndex += 1;

      const countMismatch = local.count !== cloud.count;
      const checksumMismatch = formatChecksum(local.checksum) !== formatChecksum(cloud.checksum);

      if (countMismatch || checksumMismatch) {
        bucketDifferences.push({
          bucket: bucketIndex,
          range: `${rangeStart}-${rangeEnd}`,
          localCount: local.count,
          cloudCount: cloud.count,
          localChecksum: formatChecksum(local.checksum),
          cloudChecksum: formatChecksum(cloud.checksum)
        });
      }
    }

    if (bucketDifferences.length === 0) {
      console.log('\n🎉 All buckets match between local and cloud. existing_people appears in sync.');
    } else {
      console.log(`\n⚠️ Found ${bucketDifferences.length} bucket mismatch(es):`);
      for (const diff of bucketDifferences) {
        console.log(`  Bucket ${diff.bucket} [${diff.range}]`);
        console.log(`    Local  -> count: ${diff.localCount.toLocaleString()}, checksum: ${diff.localChecksum}`);
        console.log(`    Cloud  -> count: ${diff.cloudCount.toLocaleString()}, checksum: ${diff.cloudChecksum}`);
      }
      console.log('\nInspect the listed ranges to identify missing or differing rows.');
    }
  } catch (error) {
    console.error('❌ Error comparing existing_people tables:', error.message);
    process.exitCode = 1;
  } finally {
    if (localConn) await localConn.end();
    if (cloudConn) await cloudConn.end();
  }
})();
