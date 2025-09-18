#!/usr/bin/env node

/**
 * Efficient Bulk Processor
 * Uses proper bulk SQL operations for maximum speed
 */

const fs = require('fs').promises;
const path = require('path');
const mysql = require('mysql2/promise');
const csv = require('csv-parser');
const { createReadStream } = require('fs');

require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

class EfficientBulkProcessor {
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
            totalInserted: 0,
            batchesProcessed: 0,
            startTime: null
        };
        
        this.progressFile = path.join(__dirname, 'bulk_progress.json');
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
        const tempTable = `temp_batch_${Date.now()}`;
        
        try {
            // Create temporary table
            await this.connection.execute(`
                CREATE TEMPORARY TABLE ${tempTable} (
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    city VARCHAR(100),
                    state VARCHAR(50),
                    country VARCHAR(100),
                    inventor_id INT,
                    mod_user VARCHAR(255),
                    title VARCHAR(255),
                    patent_no VARCHAR(50),
                    source_database VARCHAR(255),
                    INDEX idx_name_state (first_name, last_name, state)
                ) ENGINE=MEMORY
            `);

            // Bulk insert batch data into temp table
            if (batch.length > 0) {
                const values = batch.map(record => [
                    record.first_name, record.last_name, record.city, record.state, record.country,
                    record.inventor_id, record.mod_user, record.title, record.patent_no, record.source_database
                ]);

                await this.connection.query(
                    `INSERT INTO ${tempTable} (first_name, last_name, city, state, country, inventor_id, mod_user, title, patent_no, source_database) VALUES ?`,
                    [values]
                );
            }

            // Bulk update existing records using JOIN
            console.log('Running bulk UPDATE...');
            const [updateResult] = await this.connection.execute(`
                UPDATE existing_people_new ep
                INNER JOIN ${tempTable} tmp ON (
                    ep.first_name = tmp.first_name 
                    AND ep.last_name = tmp.last_name 
                    AND ep.state = tmp.state
                )
                SET 
                    ep.inventor_id = COALESCE(ep.inventor_id, tmp.inventor_id),
                    ep.mod_user = CASE 
                        WHEN (ep.mod_user IS NULL OR ep.mod_user = '') 
                        THEN COALESCE(tmp.mod_user, ep.mod_user)
                        ELSE ep.mod_user 
                    END,
                    ep.title = CASE 
                        WHEN (ep.title IS NULL OR ep.title = '') 
                        THEN COALESCE(tmp.title, ep.title)
                        ELSE ep.title 
                    END,
                    ep.patent_no = COALESCE(ep.patent_no, tmp.patent_no),
                    ep.updated_at = CURRENT_TIMESTAMP
                WHERE 
                    (ep.inventor_id IS NULL AND tmp.inventor_id IS NOT NULL) OR
                    ((ep.mod_user IS NULL OR ep.mod_user = '') AND tmp.mod_user IS NOT NULL) OR
                    ((ep.title IS NULL OR ep.title = '') AND tmp.title IS NOT NULL) OR
                    ((ep.patent_no IS NULL OR ep.patent_no = '') AND tmp.patent_no IS NOT NULL)
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

            // Bulk insert new records
            console.log('Running bulk INSERT...');
            const [insertResult] = await this.connection.execute(`
                INSERT INTO existing_people_new (
                    first_name, last_name, city, state, country, source_file, 
                    record_type, inventor_id, mod_user, title, patent_no
                )
                SELECT DISTINCT
                    tmp.first_name, tmp.last_name, tmp.city, tmp.state, tmp.country,
                    tmp.source_database, 'inventor', tmp.inventor_id, tmp.mod_user, tmp.title, tmp.patent_no
                FROM ${tempTable} tmp
                LEFT JOIN existing_people_new ep ON (
                    ep.first_name = tmp.first_name 
                    AND ep.last_name = tmp.last_name 
                    AND ep.state = tmp.state
                )
                WHERE ep.id IS NULL
            `);
            console.log(`INSERT result: ${insertResult.affectedRows} rows inserted`);

            // Explicit commit
            await this.connection.execute('COMMIT');
            console.log('Transaction committed');

            // Verify the changes were actually saved
            const [verifyResult] = await this.connection.execute(`
                SELECT COUNT(*) as count FROM existing_people_new 
                WHERE inventor_id IS NOT NULL OR mod_user IS NOT NULL OR title IS NOT NULL OR patent_no IS NOT NULL
            `);
            console.log(`Verification: ${verifyResult[0].count} records now have at least one field filled`);

            const batchTime = (Date.now() - batchStart) / 1000;
            const updatedRows = updateResult.affectedRows || 0;
            const insertedRows = insertResult.affectedRows || 0;

            this.stats.totalUpdated += updatedRows;
            this.stats.totalInserted += insertedRows;
            this.stats.totalProcessed += batch.length;
            this.stats.batchesProcessed++;
            this.processedOffset += batch.length;

            console.log(`Batch complete: ${updatedRows} updated, ${insertedRows} inserted in ${batchTime.toFixed(1)}s`);

            // Calculate progress
            const totalTime = (Date.now() - this.stats.startTime) / 1000;
            const rate = Math.round(this.stats.totalProcessed / totalTime);
            console.log(`Total: ${this.stats.totalProcessed.toLocaleString()} processed | ${rate}/sec | Runtime: ${(totalTime/60).toFixed(1)}m`);

            // Save progress
            await this.saveProgress();

            return { updated: updatedRows, inserted: insertedRows };

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
            inventor_id: this.parseInteger(record.inventor_id),
            mod_user: (record.mod_user || '').trim().substring(0, 255),
            title: (record.title || '').trim().substring(0, 255),
            patent_no: ((record.patent_no || record.patent_number || '') + '').trim().substring(0, 50),
            source_database: (record.source_database || 'unknown').substring(0, 255)
        };
    }

    isValidRecord(record) {
        return record.first_name && record.last_name && record.state && 
               (record.inventor_id || record.mod_user || record.title);
    }

    parseInteger(value) {
        if (!value || value === '' || value === 'null') return null;
        const parsed = parseInt(value);
        return isNaN(parsed) ? null : parsed;
    }

    async processFile(csvPath) {
        console.log('Starting efficient bulk processing...');
        this.stats.startTime = Date.now();

        return new Promise((resolve, reject) => {
            let currentBatch = [];
            let recordCount = 0;
            let skippedRecords = 0;
            let inputHasInventorId = 0;
            let inputHasPatentNo = 0;
            let inputHasTitle = 0;

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
                if (cleaned.inventor_id != null) inputHasInventorId++;
                if (cleaned.patent_no) inputHasPatentNo++;
                if (cleaned.title) inputHasTitle++;
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
                    // Write input diagnostics to output/logs
                    try {
                        const fsSync = require('fs');
                        const path = require('path');
                        const dir = path.join(__dirname, '..', 'output', 'logs');
                        if (!fsSync.existsSync(dir)) fsSync.mkdirSync(dir, { recursive: true });
                        const diag = {
                            csvPath,
                            totalRows: recordCount,
                            with_inventor_id: inputHasInventorId,
                            with_patent_no: inputHasPatentNo,
                            with_title: inputHasTitle,
                            generated_at: new Date().toISOString()
                        };
                        fsSync.writeFileSync(path.join(dir, 'update_fields_input_counts.json'), JSON.stringify(diag, null, 2));
                        console.log('[diag] Wrote input field counts to output/logs/update_fields_input_counts.json');
                    } catch (e) {
                        console.warn('[diag] Failed to write input diagnostics:', e.message);
                    }
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
        console.log('BULK PROCESSING COMPLETE');
        console.log('='.repeat(50));
        console.log(`Total time: ${(totalTime / 60).toFixed(1)} minutes`);
        console.log(`Records processed: ${this.stats.totalProcessed.toLocaleString()}`);
        console.log(`Records updated: ${this.stats.totalUpdated.toLocaleString()}`);
        console.log(`Records inserted: ${this.stats.totalInserted.toLocaleString()}`);
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
    const processor = new EfficientBulkProcessor();
    
    console.log('Efficient Bulk Processor');
    console.log('========================');
    console.log(`Batch size: ${processor.batchSize.toLocaleString()} records`);

    if (!(await processor.connect())) {
        process.exit(1);
    }

    await processor.loadProgress();

    try {
        const csvPath = '../missing_fields_data/missing_fields_updates.csv';
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
