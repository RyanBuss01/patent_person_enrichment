#!/usr/bin/env node

/**
 * Transfer Missing Fields from existing_people_new to existing_people
 * Transfers 5 specific fields only if they're missing in existing_people
 * Skips records where inventor_id = 0 in the source table
 */

const fs = require('fs').promises;
const path = require('path');
const mysql = require('mysql2/promise');

// Load .env from parent directory
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

class FieldTransferProcessor {
    constructor() {
        this.config = {
            host: process.env.DB_HOST || process.env.SQL_HOST || 'localhost',
            port: parseInt(process.env.DB_PORT || process.env.SQL_PORT) || 3306,
            database: process.env.DB_NAME || process.env.SQL_DATABASE || 'patent_data',
            user: process.env.DB_USER || process.env.SQL_USER || 'root',
            password: process.env.DB_PASSWORD || process.env.SQL_PASSWORD || 'password',
            charset: 'utf8mb4'
        };
        
        this.connection = null;
        this.batchSize = 100000;
        
        // Target fields to transfer
        this.targetFields = [
            'inventor_id',
            'mod_user', 
            'address',
            'zip'
        ];
        
        this.stats = {
            totalProcessed: 0,
            totalUpdated: 0,
            skippedZeroInventorId: 0,
            skippedNoMissingFields: 0,
            fieldUpdates: {},
            startTime: null
        };
        
        this.progressFile = path.join(__dirname, 'field_transfer_progress.json');
        this.processedOffset = 0;
    }

    async connect() {
        try {
            this.connection = await mysql.createConnection(this.config);
            
            // Set session variables for better performance
            await this.connection.execute('SET SESSION foreign_key_checks = 0');
            await this.connection.execute('SET SESSION unique_checks = 0');
            await this.connection.execute("SET SESSION sql_mode = ''");
            
            console.log(`Connected to database: ${this.config.host}:${this.config.port}/${this.config.database}`);
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
            Object.assign(this.stats, progress.stats || {});
            console.log(`Resuming from offset: ${this.processedOffset.toLocaleString()}`);
        } catch (error) {
            console.log('Starting fresh - no previous progress found');
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

    async getSourceRecords(offset, limit) {
        /**
         * Get records from existing_people_new that:
         * 1. Have inventor_id != 0 (or NULL)
         * 2. Have at least one of our target fields with valid data
         * 3. Can potentially match records in existing_people
         */
        const query = `
            SELECT id, first_name, last_name, city, state,
                   inventor_id, mod_user, address, zip
            FROM existing_people_new
            WHERE (inventor_id IS NULL OR inventor_id != 0)
            AND (first_name IS NOT NULL AND first_name != '')
            AND (last_name IS NOT NULL AND last_name != '')
            AND (state IS NOT NULL AND state != '')
            AND (
                (inventor_id IS NOT NULL AND inventor_id != 0) OR
                (mod_user IS NOT NULL AND mod_user != '') OR
                (address IS NOT NULL AND address != '') OR
                (zip IS NOT NULL AND zip != '')
            )
            ORDER BY id
            LIMIT ${limit} OFFSET ${offset}
        `;
        
        const [rows] = await this.connection.execute(query);
        return rows;
    }

    async findMatchingTargetRecord(sourceRecord) {
        /**
         * Find matching record in existing_people using name + state matching
         */
        const query = `
            SELECT id, inventor_id, mod_user, address, zip
            FROM existing_people
            WHERE first_name = ? AND last_name = ? AND state = ?
            LIMIT 1
        `;
        
        const [rows] = await this.connection.execute(query, [
            sourceRecord.first_name,
            sourceRecord.last_name, 
            sourceRecord.state
        ]);
        
        return rows.length > 0 ? rows[0] : null;
    }

    identifyMissingFields(sourceRecord, targetRecord) {
        /**
         * Identify which fields are missing in the target record
         * and available in the source record
         */
        const updates = {};
        
        for (const field of this.targetFields) {
            const targetValue = targetRecord[field];
            const sourceValue = sourceRecord[field];
            
            // Check if target field is missing/empty
            const targetIsMissing = (
                targetValue === null || 
                targetValue === undefined || 
                targetValue === '' ||
                (field === 'inventor_id' && targetValue === 0)
            );
            
            // Check if source has valid data
            const sourceHasData = (
                sourceValue !== null && 
                sourceValue !== undefined && 
                sourceValue !== '' &&
                !(field === 'inventor_id' && sourceValue === 0)
            );
            
            if (targetIsMissing && sourceHasData) {
                updates[field] = sourceValue;
            }
        }
        
        return updates;
    }

    async updateTargetRecord(targetId, updates) {
        /**
         * Update the existing_people record with the missing field values
         */
        if (Object.keys(updates).length === 0) {
            return false;
        }
        
        const setClauses = [];
        const values = [];
        
        for (const [field, value] of Object.entries(updates)) {
            setClauses.push(`${field} = ?`);
            values.push(value);
        }
        
        setClauses.push('updated_at = NOW()');
        values.push(targetId);
        
        const query = `
            UPDATE existing_people 
            SET ${setClauses.join(', ')}
            WHERE id = ?
        `;
        
        try {
            await this.connection.execute(query, values);
            
            // Update statistics
            for (const field of Object.keys(updates)) {
                if (!this.stats.fieldUpdates[field]) {
                    this.stats.fieldUpdates[field] = 0;
                }
                this.stats.fieldUpdates[field]++;
            }
            
            return true;
        } catch (error) {
            console.error(`Failed to update record ${targetId}:`, error.message);
            return false;
        }
    }

    async processBatch(sourceRecords) {
        console.log(`Processing batch of ${sourceRecords.length} source records...`);
        
        let batchUpdated = 0;
        let batchSkippedZeroId = 0;
        let batchSkippedNoMatch = 0;
        let batchSkippedNoMissing = 0;
        
        for (const sourceRecord of sourceRecords) {
            // Skip if inventor_id is 0
            if (sourceRecord.inventor_id === 0) {
                batchSkippedZeroId++;
                continue;
            }
            
            // Find matching target record
            const targetRecord = await this.findMatchingTargetRecord(sourceRecord);
            if (!targetRecord) {
                batchSkippedNoMatch++;
                continue;
            }
            
            // Identify missing fields
            const updates = this.identifyMissingFields(sourceRecord, targetRecord);
            if (Object.keys(updates).length === 0) {
                batchSkippedNoMissing++;
                continue;
            }
            
            // Update the target record
            const success = await this.updateTargetRecord(targetRecord.id, updates);
            if (success) {
                batchUpdated++;
                console.log(`Updated record ${targetRecord.id} with fields: ${Object.keys(updates).join(', ')}`);
            }
        }
        
        // Update statistics
        this.stats.totalProcessed += sourceRecords.length;
        this.stats.totalUpdated += batchUpdated;
        this.stats.skippedZeroInventorId += batchSkippedZeroId;
        this.stats.skippedNoMissingFields += batchSkippedNoMissing;
        
        console.log(`Batch complete: ${batchUpdated} updated, ${batchSkippedZeroId} skipped (zero ID), ${batchSkippedNoMatch} skipped (no match), ${batchSkippedNoMissing} skipped (no missing fields)`);
        
        return sourceRecords.length;
    }

    async processAllRecords() {
        console.log('Starting field transfer process...');
        this.stats.startTime = Date.now();
        
        let hasMore = true;
        let batchNumber = 1;
        
        while (hasMore) {
            console.log(`\n--- BATCH ${batchNumber} (offset: ${this.processedOffset.toLocaleString()}) ---`);
            
            // Get source records
            const sourceRecords = await this.getSourceRecords(this.processedOffset, this.batchSize);
            
            if (sourceRecords.length === 0) {
                console.log('No more records to process');
                hasMore = false;
                break;
            }
            
            // Process batch
            const processedCount = await this.processBatch(sourceRecords);
            
            // Update offset
            this.processedOffset += processedCount;
            
            // Save progress
            await this.saveProgress();
            
            // Show progress
            const elapsed = (Date.now() - this.stats.startTime) / 1000;
            const rate = Math.round(this.stats.totalProcessed / elapsed);
            console.log(`Progress: ${this.stats.totalProcessed.toLocaleString()} processed | ${rate}/sec | ${(elapsed/60).toFixed(1)}m elapsed`);
            
            batchNumber++;
            
            // Small delay to prevent overwhelming the database
            await new Promise(resolve => setTimeout(resolve, 100));
        }
        
        await this.generateFinalReport();
    }

    async generateFinalReport() {
        const totalTime = (Date.now() - this.stats.startTime) / 1000;
        const rate = Math.round(this.stats.totalProcessed / totalTime);
        
        console.log('\n' + '='.repeat(60));
        console.log('FIELD TRANSFER COMPLETE');
        console.log('='.repeat(60));
        console.log(`Total time: ${(totalTime / 60).toFixed(1)} minutes`);
        console.log(`Records processed: ${this.stats.totalProcessed.toLocaleString()}`);
        console.log(`Records updated: ${this.stats.totalUpdated.toLocaleString()}`);
        console.log(`Skipped (inventor_id = 0): ${this.stats.skippedZeroInventorId.toLocaleString()}`);
        console.log(`Skipped (no missing fields): ${this.stats.skippedNoMissingFields.toLocaleString()}`);
        console.log(`Processing rate: ${rate.toLocaleString()} records/second`);
        
        if (Object.keys(this.stats.fieldUpdates).length > 0) {
            console.log('\nFields transferred:');
            for (const [field, count] of Object.entries(this.stats.fieldUpdates)) {
                console.log(`  ${field}: ${count.toLocaleString()}`);
            }
        }
        
        console.log('='.repeat(60));
        
        // Save final report
        const report = {
            ...this.stats,
            totalTime: totalTime,
            rate: rate,
            completedAt: new Date().toISOString()
        };
        
        await fs.writeFile(
            path.join(__dirname, 'field_transfer_report.json'),
            JSON.stringify(report, null, 2)
        );
        
        console.log('\nFinal report saved to: field_transfer_report.json');
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
    console.log('Field Transfer Processor');
    console.log('========================');
    console.log('Transferring fields from existing_people_new to existing_people');
    console.log('Fields: inventor_id, mod_user, address, zip');
    console.log('Conditions:');
    console.log('- Only updates missing/empty fields in target table');
    console.log('- Skips records where inventor_id = 0 in source table');
    console.log('- Matches records by first_name + last_name + state');
    console.log();

    const processor = new FieldTransferProcessor();
    
    if (!(await processor.connect())) {
        console.error('Database connection failed!');
        process.exit(1);
    }

    await processor.loadProgress();

    try {
        await processor.processAllRecords();
        console.log('\nField transfer completed successfully!');
    } catch (error) {
        console.error('Field transfer failed:', error.message);
        console.error(error.stack);
        process.exit(1);
    } finally {
        await processor.close();
    }
}

// Handle process signals for clean shutdown
process.on('SIGINT', async () => {
    console.log('\nReceived SIGINT, shutting down gracefully...');
    process.exit(0);
});

process.on('uncaughtException', (error) => {
    console.error('Uncaught Exception:', error);
    process.exit(1);
});

if (require.main === module) {
    main().catch(error => {
        console.error('Script failed:', error);
        process.exit(1);
    });
}