#!/usr/bin/env node

/**
 * Memory-Efficient Streaming Batch Processor
 * Processes large CSV files without loading everything into memory
 */

const fs = require('fs').promises;
const path = require('path');
const mysql = require('mysql2/promise');
const csv = require('csv-parser');
const { createReadStream } = require('fs');
const { program } = require('commander');

// Load .env from parent directory
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

class MemoryEfficientUpdater {
    constructor() {
        this.config = {
            host: process.env.DB_HOST || process.env.SQL_HOST || 'localhost',
            port: parseInt(process.env.DB_PORT || process.env.SQL_PORT) || 3306,
            database: process.env.DB_NAME || process.env.SQL_DATABASE || 'patent_data',
            user: process.env.DB_USER || process.env.SQL_USER || 'root',
            password: process.env.DB_PASSWORD || process.env.SQL_PASSWORD || 'password'
        };
        
        this.connection = null;
        this.batchSize = 1000;
        this.stats = {
            totalProcessed: 0,
            successfulUpdates: 0,
            newRecords: 0,
            failedUpdates: 0,
            startTime: null
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

            // Set session variables for better performance
            await this.connection.execute('SET SESSION wait_timeout = 28800');
            await this.connection.execute('SET SESSION interactive_timeout = 28800');
            await this.connection.execute('SET SESSION foreign_key_checks = 0');
            await this.connection.execute('SET SESSION unique_checks = 0');

            console.log(`Connected to database: ${this.config.host}:${this.config.port}/${this.config.database}`);
            return true;
        } catch (error) {
            console.error('Database connection failed:', error.message);
            return false;
        }
    }

    cleanDateValue(dateStr) {
        if (!dateStr || dateStr === '' || dateStr === 'null' || dateStr === 'NULL') {
            return null;
        }
        
        try {
            const date = new Date(dateStr);
            if (isNaN(date.getTime())) {
                return null;
            }
            return date.toISOString().split('T')[0];
        } catch (error) {
            return null;
        }
    }

    cleanRecord(record) {
        return {
            inventor_first: record.inventor_first || '',
            inventor_last: record.inventor_last || '',
            city: record.city || '',
            state: record.state || '',
            country: record.country || '',
            address: record.address || '',
            zip: record.zip || '',
            phone: record.phone || '',
            email: record.email || '',
            company_name: record.company_name || '',
            issue_id: record.issue_id || null,
            new_issue_rec_num: record.new_issue_rec_num || null,
            inventor_id: record.inventor_id || null,
            patent_no: record.patent_no || '',
            title: record.title || '',
            issue_date: this.cleanDateValue(record.issue_date),
            inventor_contact: record.inventor_contact === 'True' || record.inventor_contact === 'true' || record.inventor_contact === true,
            bar_code: record.bar_code || '',
            mod_user: record.mod_user || '',
            data_source: record.data_source || 'additional_fields_update'
        };
    }

    async processBatch(records) {
        console.log(`🔄 Processing batch of ${records.length} records...`);
        
        // Check which records already exist
        const existingIds = new Map();
        const insertRecords = [];
        
        // For each record, try to find existing match
        for (const record of records) {
            if (record.inventor_first && record.inventor_last && record.state) {
                try {
                    const [rows] = await this.connection.execute(
                        'SELECT id FROM existing_people WHERE first_name = ? AND last_name = ? AND state = ? LIMIT 1',
                        [record.inventor_first, record.inventor_last, record.state]
                    );
                    
                    if (rows.length > 0) {
                        existingIds.set(record, rows[0].id);
                    } else {
                        insertRecords.push(record);
                    }
                } catch (error) {
                    console.warn(`Error checking existing record:`, error.message);
                    insertRecords.push(record);
                }
            } else {
                insertRecords.push(record);
            }
        }

        let updateCount = 0;
        let insertCount = 0;

        // Update existing records
        for (const [record, existingId] of existingIds.entries()) {
            try {
                // Only update inventor_contact for simplicity and speed
                await this.connection.execute(
                    'UPDATE existing_people SET inventor_contact = ?, updated_at = NOW() WHERE id = ?',
                    [record.inventor_contact, existingId]
                );
                updateCount++;
            } catch (error) {
                console.warn(`Update failed for ID ${existingId}:`, error.message);
            }
        }

        // Insert new records in batch
        if (insertRecords.length > 0) {
            try {
                const values = insertRecords.map(record => [
                    record.inventor_first,
                    record.inventor_last,
                    record.city,
                    record.state,
                    record.country,
                    record.address,
                    record.zip,
                    record.phone,
                    record.email,
                    record.company_name,
                    'inventor',
                    record.data_source,
                    record.inventor_contact,
                    record.issue_id,
                    record.new_issue_rec_num,
                    record.inventor_id,
                    record.patent_no,
                    record.title,
                    record.issue_date,
                    record.bar_code,
                    record.mod_user
                ]);

                const insertQuery = `
                    INSERT INTO existing_people (
                        first_name, last_name, city, state, country, address, zip, phone, email, 
                        company_name, record_type, source_file, inventor_contact, issue_id, 
                        new_issue_rec_num, inventor_id, patent_no, title, issue_date, 
                        bar_code, mod_user
                    ) VALUES ?
                `;

                const [result] = await this.connection.query(insertQuery, [values]);
                insertCount = result.affectedRows || 0;
            } catch (error) {
                console.error('Batch insert failed:', error.message);
            }
        }

        this.stats.successfulUpdates += updateCount;
        this.stats.newRecords += insertCount;
        this.stats.totalProcessed += records.length;

        const progress = ((this.stats.totalProcessed / 1475880) * 100).toFixed(2);
        const elapsed = (Date.now() - this.stats.startTime) / 1000;
        const rate = Math.round(this.stats.totalProcessed / elapsed);

        console.log(`✅ Batch complete: ${updateCount} updates, ${insertCount} inserts | ${progress}% | ${rate} records/sec | Memory: ${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB`);

        // Force garbage collection if available
        if (global.gc) {
            global.gc();
        }

        return { updates: updateCount, inserts: insertCount };
    }

    async processCsvStreaming(csvFilePath) {
        console.log(`📊 Starting streaming processing of CSV...`);
        console.log(`💾 Initial memory usage: ${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB`);
        
        this.stats.startTime = Date.now();
        
        return new Promise((resolve, reject) => {
            let batch = [];
            let batchNumber = 1;
            let recordCount = 0;
            let isPaused = false;

            const stream = createReadStream(csvFilePath).pipe(csv());
            
            stream.on('data', async (data) => {
                if (isPaused) return;
                
                recordCount++;
                
                // Show progress every 10,000 records read
                if (recordCount % 10000 === 0) {
                    console.log(`📖 Read ${recordCount.toLocaleString()} records from CSV...`);
                }
                
                const cleaned = this.cleanRecord(data);
                batch.push(cleaned);

                if (batch.length >= this.batchSize) {
                    isPaused = true;
                    stream.pause();
                    
                    try {
                        console.log(`\n--- BATCH ${batchNumber} ---`);
                        await this.processBatch(batch);
                        
                        // Clear the batch to free memory
                        batch = [];
                        batchNumber++;
                        
                        // Small delay to prevent overwhelming the database
                        setTimeout(() => {
                            isPaused = false;
                            stream.resume();
                        }, 50);
                        
                    } catch (error) {
                        console.error(`Batch ${batchNumber} failed:`, error.message);
                        this.stats.failedUpdates += batch.length;
                        batch = [];
                        batchNumber++;
                        
                        isPaused = false;
                        stream.resume();
                    }
                }
            });

            stream.on('end', async () => {
                console.log(`\n📖 Finished reading CSV file. Total records: ${recordCount.toLocaleString()}`);
                
                // Process final batch
                if (batch.length > 0) {
                    try {
                        console.log(`\n--- FINAL BATCH ${batchNumber} ---`);
                        await this.processBatch(batch);
                    } catch (error) {
                        console.error(`Final batch failed:`, error.message);
                        this.stats.failedUpdates += batch.length;
                    }
                }

                const totalTime = (Date.now() - this.stats.startTime) / 1000;
                const rate = Math.round(this.stats.totalProcessed / totalTime);
                
                console.log(`\n🏁 Processing completed in ${(totalTime / 60).toFixed(1)} minutes`);
                console.log(`📊 Average rate: ${rate} records/second`);
                console.log(`💾 Final memory usage: ${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB`);
                
                resolve();
            });

            stream.on('error', (error) => {
                reject(error);
            });
        });
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
    program
        .name('memory-efficient-updater')
        .description('Process large CSV updates with memory-efficient streaming')
        .option('--csv-file <file>', 'CSV file path', '../additional_fields_data/additional_fields_updates.csv')
        .option('--batch-size <size>', 'Batch size for processing', '500')
        .parse();

    const options = program.opts();

    console.log('Memory-Efficient Patent Data Processor');
    console.log('=====================================');

    const csvFile = path.resolve(options.csvFile);
    try {
        await fs.access(csvFile);
        console.log(`Found CSV file: ${csvFile}`);
    } catch (error) {
        console.error(`CSV file not found: ${csvFile}`);
        process.exit(1);
    }

    const updater = new MemoryEfficientUpdater();
    updater.batchSize = parseInt(options.batchSize);

    console.log(`Using batch size: ${updater.batchSize}`);

    if (!(await updater.connect())) {
        console.error('Database connection failed!');
        process.exit(1);
    }

    try {
        await updater.processCsvStreaming(csvFile);

        // Print final summary
        console.log('\n' + '='.repeat(50));
        console.log('FINAL SUMMARY');
        console.log('='.repeat(50));
        console.log(`Total processed: ${updater.stats.totalProcessed.toLocaleString()}`);
        console.log(`Successful updates: ${updater.stats.successfulUpdates.toLocaleString()}`);
        console.log(`Records not found: ${updater.stats.notFound || 0}`);
        console.log(`Failed: ${updater.stats.failedUpdates.toLocaleString()}`);

        const totalTime = (Date.now() - updater.stats.startTime) / 1000;
        const finalRate = Math.round(updater.stats.totalProcessed / totalTime);
        console.log(`Final processing rate: ${finalRate} records/second`);

    } catch (error) {
        console.error('Processing failed:', error.message);
        process.exit(1);
    } finally {
        await updater.close();
    }

    console.log('\nMemory-efficient processing completed!');
    process.exit(0);
}

// Handle process signals for clean shutdown
process.on('SIGINT', () => {
    console.log('\n🛑 Received SIGINT, shutting down gracefully...');
    process.exit(0);
});

process.on('uncaughtException', (error) => {
    console.error('💥 Uncaught Exception:', error);
    process.exit(1);
});

if (require.main === module) {
    main().catch(error => {
        console.error('Script failed:', error);
        process.exit(1);
    });
}