#!/usr/bin/env node

/**
 * Upload Inventor Contact Data to SQL
 * Updates the existing_people table with inventor_contact field values
 */

const fs = require('fs').promises;
const path = require('path');
const mysql = require('mysql2/promise');
const csv = require('csv-parser');
const { createReadStream } = require('fs');
const { program } = require('commander');

// Load .env from parent directory
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

class DatabaseConfig {
    constructor() {
        this.host = process.env.DB_HOST || process.env.SQL_HOST || 'localhost';
        this.port = parseInt(process.env.DB_PORT || process.env.SQL_PORT) || 3306;
        this.database = process.env.DB_NAME || process.env.SQL_DATABASE || 'patent_data';
        this.user = process.env.DB_USER || process.env.SQL_USER || 'root';
        this.password = process.env.DB_PASSWORD || process.env.SQL_PASSWORD || 'password';
    }
}

class InventorContactUploader {
    constructor(dbConfig) {
        this.config = dbConfig;
        this.connection = null;
        this.stats = {
            totalRecords: 0,
            successfulUpdates: 0,
            failedUpdates: 0,
            noMatchFound: 0,
            multipleMatches: 0,
            errors: []
        };
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

    async checkSchema() {
        try {
            // Check if existing_people table exists
            const [tables] = await this.connection.execute(`
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'existing_people'
            `, [this.config.database]);

            if (tables.length === 0) {
                console.error('❌ existing_people table not found!');
                return false;
            }

            // Check if inventor_contact column exists
            const [columns] = await this.connection.execute(`
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'existing_people' AND COLUMN_NAME = 'inventor_contact'
            `, [this.config.database]);

            if (columns.length === 0) {
                console.log('⚠️  inventor_contact column not found, adding it...');
                await this.addInventorContactColumn();
            } else {
                console.log('✅ inventor_contact column found');
                const col = columns[0];
                console.log(`   Type: ${col.DATA_TYPE}, Nullable: ${col.IS_NULLABLE}, Default: ${col.COLUMN_DEFAULT}`);
            }

            return true;
        } catch (error) {
            console.error('Schema check failed:', error.message);
            return false;
        }
    }

    async addInventorContactColumn() {
        try {
            await this.connection.execute(`
                ALTER TABLE existing_people 
                ADD COLUMN inventor_contact BOOLEAN DEFAULT TRUE
            `);
            console.log('✅ Added inventor_contact column to existing_people table');
            return true;
        } catch (error) {
            console.error('Failed to add inventor_contact column:', error.message);
            return false;
        }
    }

    async loadCsvData(csvPath) {
        return new Promise((resolve, reject) => {
            const records = [];
            let headers = [];
            
            console.log(`📁 Reading CSV file: ${csvPath}`);
            
            createReadStream(csvPath)
                .pipe(csv())
                .on('headers', (headerList) => {
                    headers = headerList;
                    console.log(`📊 CSV headers: ${headers.join(', ')}`);
                })
                .on('data', (data) => {
                    records.push(data);
                })
                .on('end', () => {
                    console.log(`✅ Loaded ${records.length} records from CSV`);
                    resolve({ records, headers });
                })
                .on('error', (error) => {
                    reject(error);
                });
        });
    }

    async findMatchingPerson(record, strategy = 'best_match') {
        try {
            let whereClause = '';
            let params = [];

            // Strategy 1: Exact ID match (if ID column exists)
            if (record.id || record.inventor_id || record.person_id) {
                const idValue = record.id || record.inventor_id || record.person_id;
                whereClause = 'id = ?';
                params = [idValue];
            }
            // Strategy 2: Name match
            else if (record.first_name || record.last_name) {
                const firstName = record.first_name || '';
                const lastName = record.last_name || '';
                
                if (firstName && lastName) {
                    whereClause = 'first_name = ? AND last_name = ?';
                    params = [firstName, lastName];
                } else if (lastName) {
                    whereClause = 'last_name = ?';
                    params = [lastName];
                } else if (firstName) {
                    whereClause = 'first_name = ?';
                    params = [firstName];
                }
            } else {
                return { matches: [], count: 0 };
            }

            const query = `
                SELECT id, first_name, last_name, city, state, inventor_contact
                FROM existing_people 
                WHERE ${whereClause}
                LIMIT 10
            `;

            const [matches] = await this.connection.execute(query, params);
            
            return {
                matches: matches,
                count: matches.length,
                query: query,
                params: params
            };
        } catch (error) {
            console.error('Error finding matching person:', error.message);
            return { matches: [], count: 0, error: error.message };
        }
    }

    async updatePersonContact(personId, contactValue) {
        try {
            const [result] = await this.connection.execute(`
                UPDATE existing_people 
                SET inventor_contact = ?, updated_at = CURRENT_TIMESTAMP 
                WHERE id = ?
            `, [contactValue, personId]);

            return result.affectedRows > 0;
        } catch (error) {
            console.error(`Error updating person ${personId}:`, error.message);
            return false;
        }
    }

    async processRecord(record, index) {
        try {
            this.stats.totalRecords++;

            // Convert inventor_contact to boolean
            let contactValue = true; // Default
            if (record.inventor_contact !== undefined) {
                const val = String(record.inventor_contact).toLowerCase();
                contactValue = !['false', 'no', '0', 'n', 'off'].includes(val);
            }

            // Find matching person(s)
            const matchResult = await this.findMatchingPerson(record);

            if (matchResult.error) {
                this.stats.failedUpdates++;
                this.stats.errors.push({
                    record: index + 1,
                    error: matchResult.error,
                    data: record
                });
                return false;
            }

            if (matchResult.count === 0) {
                this.stats.noMatchFound++;
                if (index < 5) { // Log first few for debugging
                    console.log(`⚠️  No match found for record ${index + 1}:`, {
                        first_name: record.first_name,
                        last_name: record.last_name,
                        id: record.id || record.inventor_id || record.person_id
                    });
                }
                return false;
            }

            if (matchResult.count > 1) {
                this.stats.multipleMatches++;
                // Use the first match for now, but log it
                if (index < 5) {
                    console.log(`⚠️  Multiple matches found for record ${index + 1}, using first match`);
                }
            }

            // Update the person
            const person = matchResult.matches[0];
            const updateSuccess = await this.updatePersonContact(person.id, contactValue);

            if (updateSuccess) {
                this.stats.successfulUpdates++;
                if (index < 5) { // Log first few for verification
                    const status = contactValue ? "✓" : "✗";
                    console.log(`${status} Updated ${person.first_name} ${person.last_name} (ID: ${person.id}) → contact: ${contactValue}`);
                }
            } else {
                this.stats.failedUpdates++;
            }

            return updateSuccess;
        } catch (error) {
            this.stats.failedUpdates++;
            this.stats.errors.push({
                record: index + 1,
                error: error.message,
                data: record
            });
            return false;
        }
    }

    async uploadContactData(csvPath, batchSize = 100) {
        console.log('🚀 Starting inventor contact upload...');

        // Load CSV data
        const { records, headers } = await this.loadCsvData(csvPath);
        
        if (records.length === 0) {
            console.log('❌ No records found in CSV file');
            return false;
        }

        console.log(`📊 Processing ${records.length} records...`);

        // Process records in batches
        for (let i = 0; i < records.length; i += batchSize) {
            const batch = records.slice(i, i + batchSize);
            const batchNum = Math.floor(i / batchSize) + 1;
            const totalBatches = Math.ceil(records.length / batchSize);
            
            console.log(`🔄 Processing batch ${batchNum}/${totalBatches} (records ${i + 1}-${Math.min(i + batchSize, records.length)})`);

            // Process each record in the batch
            for (let j = 0; j < batch.length; j++) {
                await this.processRecord(batch[j], i + j);
            }

            // Show progress
            const progressPercent = Math.round(((i + batch.length) / records.length) * 100);
            console.log(`   Progress: ${progressPercent}% (${this.stats.successfulUpdates} updated, ${this.stats.failedUpdates} failed)`);
        }

        return true;
    }

    async generateReport() {
        console.log('\n' + '='.repeat(60));
        console.log('📊 INVENTOR CONTACT UPLOAD SUMMARY');
        console.log('='.repeat(60));
        console.log(`📁 Total records processed: ${this.stats.totalRecords.toLocaleString()}`);
        console.log(`✅ Successful updates: ${this.stats.successfulUpdates.toLocaleString()}`);
        console.log(`❌ Failed updates: ${this.stats.failedUpdates.toLocaleString()}`);
        console.log(`⚠️  No match found: ${this.stats.noMatchFound.toLocaleString()}`);
        console.log(`🔄 Multiple matches: ${this.stats.multipleMatches.toLocaleString()}`);

        if (this.stats.totalRecords > 0) {
            const successRate = Math.round((this.stats.successfulUpdates / this.stats.totalRecords) * 100);
            console.log(`📈 Success rate: ${successRate}%`);
        }

        // Verify some updates
        try {
            const [result] = await this.connection.execute(`
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN inventor_contact = true THEN 1 ELSE 0 END) as contact_true,
                    SUM(CASE WHEN inventor_contact = false THEN 1 ELSE 0 END) as contact_false
                FROM existing_people
            `);

            const verification = result[0];
            console.log('\n📋 Database verification:');
            console.log(`   Total people: ${verification.total.toLocaleString()}`);
            console.log(`   Contact allowed: ${verification.contact_true.toLocaleString()}`);
            console.log(`   Contact not allowed: ${verification.contact_false.toLocaleString()}`);
        } catch (error) {
            console.error('Verification query failed:', error.message);
        }

        // Show errors if any
        if (this.stats.errors.length > 0) {
            console.log('\n❌ Errors encountered:');
            this.stats.errors.slice(0, 5).forEach(err => {
                console.log(`   Record ${err.record}: ${err.error}`);
            });
            if (this.stats.errors.length > 5) {
                console.log(`   ... and ${this.stats.errors.length - 5} more errors`);
            }
        }

        console.log('='.repeat(60));
        return this.stats;
    }

    async close() {
        if (this.connection) {
            await this.connection.end();
        }
    }
}

async function findCsvFile() {
    // Look for the CSV file in common locations
    const possiblePaths = [
        'inventor_contact_data/inventor_contact_updates.csv',
        '../inventor_contact_data/inventor_contact_updates.csv',
        './inventor_contact_updates.csv',
        '../inventor_contact_updates.csv'
    ];

    for (const csvPath of possiblePaths) {
        try {
            await fs.access(csvPath);
            console.log(`✅ Found CSV file: ${csvPath}`);
            return csvPath;
        } catch (error) {
            // File doesn't exist, continue searching
        }
    }

    return null;
}

async function main() {
    program
        .name('upload-inventor-contact')
        .description('Upload inventor contact data to SQL database')
        .option('--csv <file>', 'Path to CSV file with inventor contact data')
        .option('--batch-size <size>', 'Batch size for processing', '100')
        .option('--dry-run', 'Show what would be updated without making changes')
        .parse();

    const options = program.opts();

    console.log('🔄 Inventor Contact Upload to SQL Database');
    console.log('='.repeat(50));

    // Find CSV file
    let csvPath = options.csv;
    if (!csvPath) {
        csvPath = await findCsvFile();
        if (!csvPath) {
            console.error('❌ CSV file not found!');
            console.log('Please specify the CSV file path with --csv option or ensure the file exists in:');
            console.log('   - ../inventor_contact_data/inventor_contact_updates.csv');
            console.log('   - ../inventor_contact_updates.csv');
            process.exit(1);
        }
    }

    // Verify CSV file exists
    try {
        await fs.access(csvPath);
    } catch (error) {
        console.error(`❌ CSV file not found: ${csvPath}`);
        process.exit(1);
    }

    // Initialize database connection
    const config = new DatabaseConfig();
    const uploader = new InventorContactUploader(config);

    console.log(`📊 Database: ${config.host}:${config.port}/${config.database}`);
    console.log(`📁 CSV file: ${csvPath}`);

    if (options.dryRun) {
        console.log('🔍 DRY RUN MODE - No changes will be made');
    }

    try {
        // Test database connection
        if (!(await uploader.testConnection())) {
            console.error('❌ Database connection failed!');
            process.exit(1);
        }

        console.log('✅ Database connection successful');

        // Check/setup schema
        if (!(await uploader.checkSchema())) {
            console.error('❌ Schema check failed!');
            process.exit(1);
        }

        // Upload data (unless dry run)
        if (!options.dryRun) {
            const success = await uploader.uploadContactData(csvPath, parseInt(options.batchSize));
            
            if (!success) {
                console.error('❌ Upload failed!');
                process.exit(1);
            }
        } else {
            console.log('🔍 Dry run completed - would have processed the CSV file');
        }

        // Generate report
        await uploader.generateReport();

        console.log('\n🎉 Upload completed successfully!');

    } catch (error) {
        console.error('❌ Upload failed:', error.message);
        console.error(error.stack);
        process.exit(1);
    } finally {
        await uploader.close();
    }
}

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    process.exit(1);
});

if (require.main === module) {
    main().catch(error => {
        console.error('Upload failed:', error);
        process.exit(1);
    });
}