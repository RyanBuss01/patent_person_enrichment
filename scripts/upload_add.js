#!/usr/bin/env node

/**
 * Address/Phone Bulk Processor
 * Updates address and phone fields in existing records only
 */

const fs = require('fs').promises;
const path = require('path');
const mysql = require('mysql2/promise');
const csv = require('csv-parser');
const { createReadStream } = require('fs');

require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

class AddressPhoneBulkProcessor {
    constructor() {
        this.config = {
            host: process.env.DB_HOST || process.env.SQL_HOST || 'localhost',
            port: parseInt(process.env.DB_PORT || process.env.SQL_PORT) || 3306,
            database: process.env.DB_NAME || process.env.SQL_DATABASE || 'patent_data',
            user: process.env.DB_USER || process.env.SQL_USER || 'root',
            password: process.env.DB_PASSWORD || process.env.SQL_PASSWORD || 'password'
        };
        
        this.connection = null;
        this.stats = {
            totalProcessed: 0,
            totalUpdated: 0,
            addressUpdates: 0,
            phoneUpdates: 0,
            batchesProcessed: 0,
            startTime: null
        };
        
        this.progressFile = path.join(__dirname, 'address_phone_progress.json');
        this.batchSize = 1000;
        this.processedOffset = 0;
    }

    async connect() {
        try {
            this.connection = await mysql.createConnection(this.config);
            await this.connection.execute('SET SESSION foreign_key_checks = 0');
            await this.connection.execute('SET SESSION unique_checks = 0');
            console.log(`Connected to database: ${this.config.database}`);
            return true;
        } catch (error) {
            console.error('Database connection failed:', error.message);
            return false;
        }
    }

    async loadProgress() {
        try {
            const progressData = await fs.readFile(this.progressFile, 'utf8');
            const progress = JSON.parse(progressData);
            this.processedOffset = progress.processedOffset || 0;
            this.stats = { ...this.stats, ...progress.stats };
            console.log(`Resuming from offset: ${this.processedOffset.toLocaleString()}`);
        } catch (error) {
            console.log('Starting fresh');
        }
    }

    async saveProgress() {
        const progress = {
            processedOffset: this.processedOffset,
            stats: this.stats,
            lastSaved: new Date().toISOString()
        };
        await fs.writeFile(this.progressFile, JSON.stringify(progress, null, 2));
    }

    async processBatch(batch) {
        const batchStart = Date.now();
        console.log(`Processing batch ${this.stats.batchesProcessed + 1} (${batch.length} records)...`);

        // Create temporary table for this batch
        const tempTable = `temp_addr_phone_${Date.now()}`;
        
        try {
            // Create temporary table
            await this.connection.execute(`
                CREATE TEMPORARY TABLE ${tempTable} (
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    city VARCHAR(100),
                    state VARCHAR(50),
                    country VARCHAR(100),
                    address VARCHAR(255),
                    phone VARCHAR(100),
                    source_database VARCHAR(255),
                    INDEX idx_name_state (first_name, last_name, state)
                ) ENGINE=MEMORY
            `);

            // Bulk insert batch data into temp table
            if (batch.length > 0) {
                const values = batch.map(record => [
                    record.first_name, record.last_name, record.city, record.state, record.country,
                    record.address, record.phone, record.source_database
                ]);

                await this.connection.query(
                    `INSERT INTO ${tempTable} (first_name, last_name, city, state, country, address, phone, source_database) VALUES ?`,
                    [values]
                );
            }

            // Bulk update existing records using JOIN - ONLY UPDATES, NO INSERTS
            console.log('Running bulk UPDATE for address and phone...');
            const [updateResult] = await this.connection.execute(`
                UPDATE existing_people_new ep
                INNER JOIN ${tempTable} tmp ON (
                    ep.first_name = tmp.first_name 
                    AND ep.last_name = tmp.last_name 
                    AND ep.state = tmp.state
                )
                SET 
                    ep.address = CASE 
                        WHEN (ep.address IS NULL OR ep.address = '') 
                        THEN COALESCE(tmp.address, ep.address)
                        ELSE ep.address 
                    END,
                    ep.phone = CASE 
                        WHEN (ep.phone IS NULL OR ep.phone = '') 
                        THEN COALESCE(tmp.phone, ep.phone)
                        ELSE ep.phone 
                    END,
                    ep.updated_at = CURRENT_TIMESTAMP
                WHERE 
                    ((ep.address IS NULL OR ep.address = '') AND tmp.address IS NOT NULL AND tmp.address != '') OR
                    ((ep.phone IS NULL OR ep.phone = '') AND tmp.phone IS NOT NULL AND tmp.phone != '')
            `);
            console.log(`UPDATE result: ${updateResult.affectedRows} rows updated`);

            // Check how many potential matches exist
            const [matchCheck] = await this.connection.execute(`
                SELECT COUNT(*) as matches FROM existing_people_new ep
                INNER JOIN ${tempTable} tmp ON (
                    ep.first_name = tmp.first_name 
                    AND ep.last_name = tmp.last_name 
                    AND ep.state = tmp.state
                )
            `);
            console.log(`Found ${matchCheck[0].matches} potential matches in existing table`);

            // Count specific field updates
            const [addressUpdateCount] = await this.connection.execute(`
                SELECT COUNT(*) as count FROM existing_people_new ep
                INNER JOIN ${tempTable} tmp ON (
                    ep.first_name = tmp.first_name 
                    AND ep.last_name = tmp.last_name 
                    AND ep.state = tmp.state
                )
                WHERE (ep.address IS NULL OR ep.address = '') 
                AND tmp.address IS NOT NULL AND tmp.address != ''
            `);

            const [phoneUpdateCount] = await this.connection.execute(`
                SELECT COUNT(*) as count FROM existing_people_new ep
                INNER JOIN ${tempTable} tmp ON (
                    ep.first_name = tmp.first_name 
                    AND ep.last_name = tmp.last_name 
                    AND ep.state = tmp.state
                )
                WHERE (ep.phone IS NULL OR ep.phone = '') 
                AND tmp.phone IS NOT NULL AND tmp.phone != ''
            `);

            console.log(`Potential address updates: ${addressUpdateCount[0].count}`);
            console.log(`Potential phone updates: ${phoneUpdateCount[0].count}`);

            // Explicit commit
            await this.connection.execute('COMMIT');
            console.log('Transaction committed');

            // Verify the changes were actually saved
            const [verifyResult] = await this.connection.execute(`
                SELECT 
                    COUNT(*) as total_with_address,
                    (SELECT COUNT(*) FROM existing_people_new WHERE phone IS NOT NULL AND phone != '') as total_with_phone
                FROM existing_people_new 
                WHERE address IS NOT NULL AND address != ''
            `);
            console.log(`Verification: ${verifyResult[0].total_with_address} records now have addresses, ${verifyResult[0].total_with_phone} have phone numbers`);

            const batchTime = (Date.now() - batchStart) / 1000;
            const updatedRows = updateResult.affectedRows || 0;

            this.stats.totalUpdated += updatedRows;
            this.stats.addressUpdates += addressUpdateCount[0].count;
            this.stats.phoneUpdates += phoneUpdateCount[0].count;
            this.stats.totalProcessed += batch.length;
            this.stats.batchesProcessed++;
            this.processedOffset += batch.length;

            console.log(`Batch complete: ${updatedRows} records updated in ${batchTime.toFixed(1)}s`);

            // Calculate progress
            const totalTime = (Date.now() - this.stats.startTime) / 1000;
            const rate = Math.round(this.stats.totalProcessed / totalTime);
            console.log(`Total: ${this.stats.totalProcessed.toLocaleString()} processed | ${rate}/sec | Runtime: ${(totalTime/60).toFixed(1)}m`);

            // Save progress
            await this.saveProgress();

            return { updated: updatedRows };

        } catch (error) {
            console.error('Batch failed:', error.message);
            try {
                await this.connection.execute(`DROP TEMPORARY TABLE IF EXISTS ${tempTable}`);
            } catch (cleanupError) {
                // Ignore cleanup errors
            }
            throw error;
        }
    }

    cleanRecord(record) {
        return {
            first_name: (record.first_name || '').trim().substring(0, 100),
            last_name: (record.last_name || '').trim().substring(0, 100),
            city: (record.city || '').trim().substring(0, 100),
            state: (record.state || '').trim().substring(0, 50),
            country: (record.country || '').trim().substring(0, 100),
            address: (record.address || '').trim().substring(0, 255),
            phone: (record.phone || '').trim().substring(0, 100),
            source_database: (record.source_database || 'unknown').substring(0, 255)
        };
    }

    isValidRecord(record) {
        return record.first_name && record.last_name && record.state && 
               (record.address || record.phone);
    }

    async processFile(csvPath) {
        console.log('Starting efficient address/phone bulk processing...');
        this.stats.startTime = Date.now();

        return new Promise((resolve, reject) => {
            let currentBatch = [];
            let recordCount = 0;
            let skippedRecords = 0;

            const stream = createReadStream(csvPath).pipe(csv());
            
            stream.on('data', async (data) => {
                recordCount++;

                // Skip records we've already processed
                if (recordCount <= this.processedOffset) {
                    skippedRecords++;
                    if (skippedRecords % 10000 === 0) {
                        console.log(`Skipping already processed: ${skippedRecords.toLocaleString()}`);
                    }
                    return;
                }

                const cleaned = this.cleanRecord(data);
                if (this.isValidRecord(cleaned)) {
                    currentBatch.push(cleaned);

                    if (currentBatch.length >= this.batchSize) {
                        stream.pause();
                        
                        try {
                            await this.processBatch([...currentBatch]);
                            currentBatch = [];
                            
                            setTimeout(() => {
                                stream.resume();
                            }, 10);
                            
                        } catch (error) {
                            stream.destroy();
                            reject(error);
                            return;
                        }
                    }
                }
            });

            stream.on('end', async () => {
                try {
                    if (currentBatch.length > 0) {
                        console.log(`Processing final batch of ${currentBatch.length} records...`);
                        await this.processBatch(currentBatch);
                    }
                    
                    await this.saveProgress();
                    resolve(true);
                } catch (error) {
                    reject(error);
                }
            });

            stream.on('error', reject);
        });
    }

    async generateReport() {
        const totalTime = (Date.now() - this.stats.startTime) / 1000;
        const rate = Math.round(this.stats.totalProcessed / totalTime);
        
        console.log('\n' + '='.repeat(50));
        console.log('ADDRESS/PHONE PROCESSING COMPLETE');
        console.log('='.repeat(50));
        console.log(`Total time: ${(totalTime / 60).toFixed(1)} minutes`);
        console.log(`Records processed: ${this.stats.totalProcessed.toLocaleString()}`);
        console.log(`Records updated: ${this.stats.totalUpdated.toLocaleString()}`);
        console.log(`Address fields updated: ${this.stats.addressUpdates.toLocaleString()}`);
        console.log(`Phone fields updated: ${this.stats.phoneUpdates.toLocaleString()}`);
        console.log(`Processing rate: ${rate.toLocaleString()} records/second`);
        console.log('='.repeat(50));
    }

    async close() {
        if (this.connection) {
            await this.connection.execute('SET SESSION foreign_key_checks = 1');
            await this.connection.execute('SET SESSION unique_checks = 1');
            await this.connection.end();
        }
    }
}

async function main() {
    const processor = new AddressPhoneBulkProcessor();
    
    console.log('Address/Phone Bulk Processor');
    console.log('============================');
    console.log(`Batch size: ${processor.batchSize.toLocaleString()} records`);
    console.log('NOTE: This script only UPDATES existing records, no new records will be inserted');

    if (!(await processor.connect())) {
        process.exit(1);
    }

    await processor.loadProgress();

    try {
        const csvPath = '../address_phone_data/address_phone_updates.csv';
        await processor.processFile(csvPath);
        await processor.generateReport();
        console.log('\nProcessing completed successfully!');
    } catch (error) {
        console.error('Processing failed:', error.message);
        process.exit(1);
    } finally {
        await processor.close();
    }
}

process.on('SIGINT', () => {
    console.log('\nProgress saved. Restart to resume.');
    process.exit(0);
});

if (require.main === module) {
    main().catch(console.error);
}