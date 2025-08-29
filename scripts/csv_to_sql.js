#!/usr/bin/env node

/**
 * CSV to SQL Migration Script (JavaScript/Node.js)
 * Migrates existing CSV databases to SQL and sets up the new pipeline
 */

const fs = require('fs').promises;
const path = require('path');
const mysql = require('mysql2/promise');
const csv = require('csv-parser');
const { createReadStream } = require('fs');
const { program } = require('commander');

// Load .env from parent directory since script is in scripts/
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

class DatabaseConfig {
     constructor() {
        this.host = process.env.DB_HOST || 'localhost';
        this.port = parseInt(process.env.DB_PORT) || 3306;
        this.database = process.env.DB_NAME || 'patent_data';
        this.user = process.env.DB_USER || 'root';
        this.password = process.env.DB_PASSWORD || 'password';
        this.engine = (process.env.DB_ENGINE || 'mysql').toLowerCase();
    }
}

class DatabaseManager {
    constructor(config) {
        this.config = config;
        this.connection = null;
    }

    async connect() {
        try {
            this.connection = await mysql.createConnection({
                host: this.config.host,
                port: this.config.port,
                user: this.config.user,
                password: this.config.password,
                database: this.config.database,
                charset: 'utf8mb4'
            });
            return true;
        } catch (error) {
            console.error('Database connection failed:', error.message);
            return false;
        }
    }

    async testConnection() {
        try {
            if (!this.connection) {
                await this.connect();
            }
            const [rows] = await this.connection.execute('SELECT 1 as test');
            return rows.length > 0;
        } catch (error) {
            console.error('Connection test failed:', error.message);
            return false;
        }
    }

    async initializeSchema() {
        const schemaSQL = `
-- Existing patents table
CREATE TABLE IF NOT EXISTS existing_patents (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    patent_number VARCHAR(20) NOT NULL UNIQUE,
    patent_title TEXT,
    patent_date DATE,
    patent_abstract TEXT,
    source_file VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_patent_number (patent_number),
    INDEX idx_patent_date (patent_date),
    INDEX idx_source_file (source_file)
);

-- Existing people table
CREATE TABLE IF NOT EXISTS existing_people (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100),
    source_file VARCHAR(255),
    record_type ENUM('inventor', 'assignee', 'customer') DEFAULT 'inventor',
    address TEXT,
    zip VARCHAR(20),
    phone VARCHAR(50),
    email VARCHAR(255),
    company_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_name (first_name, last_name),
    INDEX idx_location (city, state),
    INDEX idx_source_file (source_file),
    INDEX idx_record_type (record_type),
    FULLTEXT idx_fulltext_name (first_name, last_name)
);

-- Downloaded patents from Step 0
CREATE TABLE IF NOT EXISTS downloaded_patents (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    patent_number VARCHAR(20) NOT NULL,
    patent_title TEXT,
    patent_date DATE,
    patent_abstract TEXT,
    download_batch_id VARCHAR(50),
    api_source VARCHAR(50) DEFAULT 'patentsview',
    raw_data JSON,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_patent_number (patent_number),
    INDEX idx_batch_id (download_batch_id),
    INDEX idx_patent_date (patent_date)
);

-- People from downloaded patents
CREATE TABLE IF NOT EXISTS downloaded_people (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    patent_number VARCHAR(20),
    person_type ENUM('inventor', 'assignee'),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100),
    download_batch_id VARCHAR(50),
    
    INDEX idx_patent (patent_number),
    INDEX idx_name (first_name, last_name),
    INDEX idx_location (city, state),
    INDEX idx_batch_id (download_batch_id)
);

-- Match results from Step 1
CREATE TABLE IF NOT EXISTS person_matches (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    person_id BIGINT,
    existing_person_id BIGINT,
    match_score INT,
    match_reason VARCHAR(255),
    match_status ENUM('auto_matched', 'needs_review', 'new', 'verified_existing', 'verified_new') DEFAULT 'new',
    match_details JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_person_id (person_id),
    INDEX idx_match_score (match_score),
    INDEX idx_match_status (match_status)
);

-- People selected for enrichment
CREATE TABLE IF NOT EXISTS people_for_enrichment (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    person_id BIGINT,
    patent_number VARCHAR(20),
    patent_title TEXT,
    person_type ENUM('inventor', 'assignee'),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100),
    match_score INT DEFAULT 0,
    match_status ENUM('new', 'needs_review', 'verified_new') DEFAULT 'new',
    verification_needed BOOLEAN DEFAULT FALSE,
    enrichment_status ENUM('pending', 'in_progress', 'completed', 'failed', 'skipped') DEFAULT 'pending',
    enrichment_cost_estimate DECIMAL(10,4) DEFAULT 0.03,
    enriched_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_enrichment_status (enrichment_status),
    INDEX idx_verification (verification_needed),
    INDEX idx_patent (patent_number)
);

-- Enriched data results
CREATE TABLE IF NOT EXISTS enriched_people (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    enrichment_id BIGINT,
    original_first_name VARCHAR(100),
    original_last_name VARCHAR(100),
    original_city VARCHAR(100),
    original_state VARCHAR(50),
    original_country VARCHAR(100),
    enriched_email VARCHAR(255),
    enriched_linkedin_url TEXT,
    enriched_phone VARCHAR(50),
    enriched_current_company VARCHAR(255),
    enriched_current_title VARCHAR(255),
    enriched_location JSON,
    enriched_work_history JSON,
    enriched_education JSON,
    api_confidence_score DECIMAL(5,2),
    api_cost DECIMAL(10,4),
    api_response_raw JSON,
    enriched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_enrichment_id (enrichment_id),
    INDEX idx_email (enriched_email),
    INDEX idx_company (enriched_current_company)
);

-- Processing batch tracking
CREATE TABLE IF NOT EXISTS processing_batches (
    id VARCHAR(50) PRIMARY KEY,
    step_name VARCHAR(50),
    status ENUM('started', 'in_progress', 'completed', 'failed') DEFAULT 'started',
    config JSON,
    stats JSON,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    error_message TEXT NULL,
    
    INDEX idx_step_name (step_name),
    INDEX idx_status (status),
    INDEX idx_started_at (started_at)
);`;

        try {
            const statements = schemaSQL
                .split(';')
                .filter(stmt => stmt.trim().length > 0);

            for (const statement of statements) {
                await this.connection.execute(statement);
            }
            
            console.log('✅ Database schema initialized successfully');
            return true;
        } catch (error) {
            console.error('❌ Schema initialization failed:', error.message);
            return false;
        }
    }

    async executeBatch(query, dataArray, batchSize = 100) {
        const results = { insertedRows: 0, errors: 0 };
        
        for (let i = 0; i < dataArray.length; i += batchSize) {
            const batch = dataArray.slice(i, i + batchSize);
            try {
                const [result] = await this.connection.query(query, [batch]);
                results.insertedRows += result.affectedRows || batch.length;
            } catch (error) {
                console.error(`Batch insert error (rows ${i}-${i + batch.length}):`, error.message);
                results.errors += batch.length;
            }
        }
        
        return results;
    }

    async close() {
        if (this.connection) {
            await this.connection.end();
        }
    }
}

class CSVMigrator {
    constructor(dbManager) {
        this.db = dbManager;
    }

    async migrateCsvFolder(csvFolder, batchSize = 1000) {
        console.log(`🔍 Looking for CSV files in: ${csvFolder}`);
        const results = {
            filesProcessed: 0,
            patentsImported: 0,
            peopleImported: 0,
            errors: []
        };

        try {
            const files = await fs.readdir(csvFolder);
            const csvFiles = files.filter(file => file.endsWith('.csv'));

            console.log(`📁 Found ${csvFiles.length} CSV files to migrate`);

            for (const csvFile of csvFiles) {
                try {
                    const filePath = path.join(csvFolder, csvFile);
                    const fileResults = await this.migrateSingleCsv(filePath, csvFile, batchSize);
                    
                    results.filesProcessed++;
                    results.patentsImported += fileResults.patents;
                    results.peopleImported += fileResults.people;
                    
                    console.log(`✅ Migrated ${csvFile}: ${fileResults.patents} patents, ${fileResults.people} people`);
                } catch (error) {
                    console.error(`❌ Failed to migrate ${csvFile}:`, error.message);
                    results.errors.push({ file: csvFile, error: error.message });
                }
            }
        } catch (error) {
            console.error('❌ Error reading CSV folder:', error.message);
            results.errors.push({ folder: csvFolder, error: error.message });
        }

        return results;
    }

    async migrateSingleCsv(filePath, fileName, batchSize) {
        return new Promise((resolve, reject) => {
            const patents = [];
            const people = [];
            let headers = [];
            let isFirstRow = true;

            createReadStream(filePath)
                .pipe(csv())
                .on('headers', (headerList) => {
                    headers = headerList.map(h => h.toLowerCase().trim());
                })
                .on('data', (data) => {
                    if (isFirstRow) {
                        isFirstRow = false;
                        console.log(`📊 Processing ${fileName} with columns:`, headers.slice(0, 5).join(', '), '...');
                    }

                    // Extract patents
                    const patentData = this.extractPatentFromRow(data, headers, fileName);
                    if (patentData) {
                        patents.push(patentData);
                    }

                    // Extract people
                    const personData = this.extractPersonFromRow(data, headers, fileName);
                    if (personData) {
                        people.push(personData);
                    }
                })
                .on('end', async () => {
                    try {
                        let patentCount = 0;
                        let peopleCount = 0;

                        // Insert patents
                        if (patents.length > 0) {
                            patentCount = await this.insertPatents(patents, batchSize);
                        }

                        // Insert people
                        if (people.length > 0) {
                            peopleCount = await this.insertPeople(people, batchSize);
                        }

                        resolve({ patents: patentCount, people: peopleCount });
                    } catch (error) {
                        reject(error);
                    }
                })
                .on('error', (error) => {
                    reject(error);
                });
        });
    }

    extractPatentFromRow(data, headers, fileName) {
        // Find patent number column
        const patentColumns = ['patent_number', 'patent_id', 'publication_number', 'doc_number', 'number'];
        const patentCol = headers.find(header => 
            patentColumns.some(col => header.includes(col))
        );

        if (!patentCol || !data[patentCol]) {
            return null;
        }

        const patentNumber = this.cleanPatentNumber(data[patentCol]);
        if (!patentNumber) {
            return null;
        }

        // Find other columns
        const titleCol = headers.find(h => h.includes('title') || h.includes('invention'));
        const dateCol = headers.find(h => h.includes('date') || h.includes('publication'));
        const abstractCol = headers.find(h => h.includes('abstract') || h.includes('description'));

        return {
            patent_number: patentNumber,
            patent_title: data[titleCol] ? String(data[titleCol]).substring(0, 500) : '',
            patent_date: data[dateCol] ? this.parseDate(data[dateCol]) : null,
            patent_abstract: data[abstractCol] ? String(data[abstractCol]).substring(0, 1000) : '',
            source_file: fileName
        };
    }

    extractPersonFromRow(data, headers, fileName) {
        // Find name columns
        const firstNameCol = headers.find(h => 
            h.includes('first') || h.includes('fname') || h.includes('inventor_first')
        );
        const lastNameCol = headers.find(h => 
            h.includes('last') || h.includes('lname') || h.includes('inventor_last')
        );

        if (!firstNameCol && !lastNameCol) {
            return null;
        }

        const firstName = this.cleanString(data[firstNameCol]);
        const lastName = this.cleanString(data[lastNameCol]);

        if (!firstName && !lastName) {
            return null;
        }

        // Find location columns
        const cityCol = headers.find(h => h.includes('city'));
        const stateCol = headers.find(h => h.includes('state'));
        const countryCol = headers.find(h => h.includes('country'));
        const addressCol = headers.find(h => h.includes('address') || h.includes('addr'));
        const zipCol = headers.find(h => h.includes('zip') || h.includes('postal'));
        const phoneCol = headers.find(h => h.includes('phone') || h.includes('tel'));
        const emailCol = headers.find(h => h.includes('email') || h.includes('mail'));
        const companyCol = headers.find(h => h.includes('company') || h.includes('org'));

        return {
            first_name: firstName,
            last_name: lastName,
            city: this.cleanString(data[cityCol]),
            state: this.cleanString(data[stateCol]),
            country: this.cleanString(data[countryCol]),
            address: this.cleanString(data[addressCol]),
            zip: this.cleanString(data[zipCol]),
            phone: this.cleanString(data[phoneCol]),
            email: this.cleanString(data[emailCol]),
            company_name: this.cleanString(data[companyCol]),
            record_type: 'inventor',
            source_file: fileName
        };
    }

    async insertPatents(patents, batchSize) {
        const query = `
            INSERT IGNORE INTO existing_patents 
            (patent_number, patent_title, patent_date, patent_abstract, source_file) 
            VALUES ?`;

        const values = patents.map(p => [
            p.patent_number,
            p.patent_title,
            p.patent_date,
            p.patent_abstract,
            p.source_file
        ]);

        const result = await this.db.executeBatch(query, values, batchSize);
        return result.insertedRows;
    }

    async insertPeople(people, batchSize) {
        const query = `
            INSERT IGNORE INTO existing_people 
            (first_name, last_name, city, state, country, address, zip, phone, email, company_name, record_type, source_file) 
            VALUES ?`;

        const values = people.map(p => [
            p.first_name,
            p.last_name,
            p.city,
            p.state,
            p.country,
            p.address,
            p.zip,
            p.phone,
            p.email,
            p.company_name,
            p.record_type,
            p.source_file
        ]);

        const result = await this.db.executeBatch(query, values, batchSize);
        return result.insertedRows;
    }

    cleanPatentNumber(patentNum) {
        if (!patentNum || String(patentNum).toLowerCase() === 'null') {
            return null;
        }

        let clean = String(patentNum).trim().toUpperCase();
        clean = clean.replace(/^US|USPTO/g, '');
        clean = clean.replace(/[,\s-]/g, '');
        clean = clean.replace(/^0+/, '');

        return clean && /^\d+$/.test(clean) ? clean : null;
    }

    cleanString(value) {
        if (!value || String(value).toLowerCase() === 'null') {
            return '';
        }
        return String(value).trim();
    }

    parseDate(dateStr) {
        if (!dateStr || String(dateStr).toLowerCase() === 'null') {
            return null;
        }
        
        const date = new Date(dateStr);
        return isNaN(date.getTime()) ? null : date.toISOString().split('T')[0];
    }
}

async function checkPrerequisites() {
    const results = { ready: true, issues: [] };

    // Check .env file - use __dirname to get script location, then navigate to parent
    try {
        const envPath = path.join(__dirname, '..', '.env');
        console.log(`🔍 Checking for .env file at: ${envPath}`);
        await fs.access(envPath);
        const envContent = await fs.readFile(envPath, 'utf8');
        
        const requiredVars = ['SQL_HOST', 'SQL_DATABASE', 'SQL_USER', 'SQL_PASSWORD'];
        const missingVars = requiredVars.filter(varName => 
            !envContent.includes(`${varName}=`)
        );

        if (missingVars.length > 0) {
            results.ready = false;
            results.issues.push(`Missing environment variables: ${missingVars.join(', ')}`);
        }
    } catch (error) {
        results.ready = false;
        results.issues.push('.env file not found in Patent_Grants directory');
    }

    // Check CSV folder - use __dirname to get script location, then navigate to parent
    const csvFolder = path.join(__dirname, '..', 'converted_databases', 'csv');

    console.log(`🔍 Checking for CSV files in: ${csvFolder}`);
    try {
        await fs.access(csvFolder);
        const files = await fs.readdir(csvFolder);
        const csvFiles = files.filter(f => f.endsWith('.csv'));
        
        if (csvFiles.length === 0) {
            results.issues.push('No CSV files found in CSV folder');
        } else {
            results.csvFilesFound = csvFiles.length;
        }
    } catch (error) {
        results.issues.push(`CSV folder not found: ${csvFolder} (will create empty database)`);
    }

    return results;
}

async function verifyMigration(dbManager) {
    try {
        const [patentRows] = await dbManager.connection.execute('SELECT COUNT(*) as count FROM existing_patents');
        const [peopleRows] = await dbManager.connection.execute('SELECT COUNT(*) as count FROM existing_people LIMIT 10');
        const [samplePeople] = await dbManager.connection.execute('SELECT first_name, last_name, city, state FROM existing_people LIMIT 3');

        return {
            success: true,
            patentsCount: patentRows[0].count,
            peopleCount: peopleRows[0].count,
            samplePeople: samplePeople
        };
    } catch (error) {
        return {
            success: false,
            error: error.message
        };
    }
}

async function createTestScript() {
    const testScript = `#!/usr/bin/env node

/**
 * Test SQL Integration
 * Quick test to verify database setup and SQL-based pipeline
 */

const mysql = require('mysql2/promise');
const path = require('path');

// Load .env from parent directory
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

class DatabaseConfig {
    constructor() {
        this.host = process.env.SQL_HOST || 'localhost';
        this.port = parseInt(process.env.SQL_PORT) || 3306;
        this.database = process.env.SQL_DATABASE || 'patent_data';
        this.user = process.env.SQL_USER || 'root';
        this.password = process.env.SQL_PASSWORD || 'password';
    }
}

async function testDatabase() {
    console.log('Testing database connection...');
    
    const config = new DatabaseConfig();
    let connection;
    
    try {
        connection = await mysql.createConnection({
            host: config.host,
            port: config.port,
            user: config.user,
            password: config.password,
            database: config.database
        });

        console.log(\`✅ Connected to \${config.host}:\${config.port}/\${config.database}\`);

        // Test data access
        const [patentRows] = await connection.execute('SELECT COUNT(*) as count FROM existing_patents');
        const [peopleRows] = await connection.execute('SELECT COUNT(*) as count FROM existing_people');
        const [samplePeople] = await connection.execute('SELECT first_name, last_name, city, state FROM existing_people LIMIT 3');

        console.log(\`📋 Found \${patentRows[0].count.toLocaleString()} existing patents\`);
        console.log(\`👥 Found \${peopleRows[0].count.toLocaleString()} existing people\`);

        if (samplePeople.length > 0) {
            console.log('\\n📄 Sample people:');
            samplePeople.forEach(person => {
                const name = \`\${person.first_name || ''} \${person.last_name || ''}\`.trim();
                const location = \`\${person.city || ''}, \${person.state || ''}\`.replace(/^,\\s*|,\\s*$/g, '');
                console.log(\`   • \${name} - \${location}\`);
            });
        }

        return true;
    } catch (error) {
        console.error('❌ Database test failed:', error.message);
        return false;
    } finally {
        if (connection) {
            await connection.end();
        }
    }
}

async function main() {
    if (await testDatabase()) {
        console.log('\\n✅ SQL integration test passed!');
        process.exit(0);
    } else {
        console.log('\\n❌ SQL integration test failed!');
        process.exit(1);
    }
}

main().catch(console.error);`;

    // Create test script in current directory (scripts/)
    const testScriptPath = path.join(process.cwd(), 'test_sql_integration.js');
    
    await fs.writeFile(testScriptPath, testScript);
    
    // Make executable on Unix systems
    try {
        await fs.chmod(testScriptPath, 0o755);
    } catch (error) {
        // Ignore on Windows
    }
    
    return testScriptPath;
}

async function main() {
    program
        .name('migrate-csv-to-sql')
        .description('Migrate CSV databases to SQL')
        .option('--check-only', 'Only check prerequisites')
        .option('--batch-size <size>', 'Batch size for migration', '1000')
        .option('--test-after', 'Run test after migration')
        .parse();

    const options = program.opts();

    console.log('🔄 Patent Processing Pipeline: CSV to SQL Migration');
    console.log('='.repeat(60));

    // Check prerequisites
    console.log('📋 Checking prerequisites...');
    const prereqCheck = await checkPrerequisites();

    if (!prereqCheck.ready) {
        console.log('❌ System not ready for migration:');
        prereqCheck.issues.forEach(issue => console.log(`   • ${issue}`));
        console.log('\nPlease resolve these issues and try again.');
        process.exit(1);
    }

    if (prereqCheck.issues.length > 0) {
        console.log('⚠️  Warnings:');
        prereqCheck.issues.forEach(issue => console.log(`   • ${issue}`));
    }

    if (prereqCheck.csvFilesFound) {
        console.log(`✅ Found ${prereqCheck.csvFilesFound} CSV files to migrate`);
    }

    if (options.checkOnly) {
        console.log('✅ Prerequisites check complete');
        process.exit(0);
    }

    // Initialize database
    console.log('\n💾 Initializing database...');
    const config = new DatabaseConfig();
    const dbManager = new DatabaseManager(config);

    if (!(await dbManager.testConnection())) {
        console.log('❌ Database connection failed!');
        console.log('   Check your database credentials and ensure the database server is running');
        process.exit(1);
    }

    console.log(`✅ Connected to database: ${config.host}:${config.port}/${config.database}`);

    // Initialize schema
    if (!(await dbManager.initializeSchema())) {
        console.log('❌ Schema initialization failed!');
        process.exit(1);
    }

    // Migrate CSV data
    console.log('\n📁 Migrating CSV data...');
    const csvFolder = path.join(__dirname, '..', 'converted_databases', 'csv');
    
    const migrator = new CSVMigrator(dbManager);
    const migrationResults = await migrator.migrateCsvFolder(csvFolder, parseInt(options.batchSize));

    console.log('✅ CSV migration complete:');
    console.log(`   📄 Files processed: ${migrationResults.filesProcessed}`);
    console.log(`   📋 Patents imported: ${migrationResults.patentsImported.toLocaleString()}`);
    console.log(`   👥 People imported: ${migrationResults.peopleImported.toLocaleString()}`);

    if (migrationResults.errors.length > 0) {
        console.log(`   ❌ Errors: ${migrationResults.errors.length}`);
        migrationResults.errors.forEach(error => 
            console.log(`      ${error.file || error.folder}: ${error.error}`)
        );
    }

    // Verify migration
    console.log('\n🔍 Verifying migration...');
    const verification = await verifyMigration(dbManager);

    if (!verification.success) {
        console.log(`❌ Verification failed: ${verification.error}`);
        process.exit(1);
    }

    console.log('✅ Migration verified:');
    console.log(`   📋 Patents in database: ${verification.patentsCount.toLocaleString()}`);
    console.log(`   👥 People in database: ${verification.peopleCount.toLocaleString()}`);

    // Create test script
    console.log('\n🧪 Creating test script...');
    const testScript = await createTestScript();
    console.log(`✅ Created test script: ${testScript}`);

    // Run test if requested
    if (options.testAfter) {
        console.log('\n🧪 Running integration test...');
        const { spawn } = require('child_process');
        
        try {
            const testProcess = spawn('node', [testScript], { stdio: 'inherit' });
            
            testProcess.on('close', (code) => {
                if (code === 0) {
                    console.log('✅ Integration test passed!');
                } else {
                    console.log('❌ Integration test failed!');
                }
            });
        } catch (error) {
            console.log('❌ Could not run integration test:', error.message);
        }
    }

    await dbManager.close();

    console.log('\n🎉 MIGRATION COMPLETE!');
    console.log('='.repeat(60));
    console.log('📋 Next steps:');
    console.log('   1. Test the SQL integration:');
    console.log(`      node ${testScript}`);
    console.log('   2. Update your Node.js pipeline to use SQL');
    console.log('   3. Monitor the database using SQL queries');

    process.exit(0);
}

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    process.exit(1);
});

if (require.main === module) {
    main().catch(error => {
        console.error('Migration failed:', error);
        process.exit(1);
    });
}