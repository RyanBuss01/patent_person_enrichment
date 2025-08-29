#!/usr/bin/env node

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

        console.log(`✅ Connected to ${config.host}:${config.port}/${config.database}`);

        // Test data access
        const [patentRows] = await connection.execute('SELECT COUNT(*) as count FROM existing_patents');
        const [peopleRows] = await connection.execute('SELECT COUNT(*) as count FROM existing_people');
        const [samplePeople] = await connection.execute('SELECT first_name, last_name, city, state FROM existing_people LIMIT 3');

        console.log(`📋 Found ${patentRows[0].count.toLocaleString()} existing patents`);
        console.log(`👥 Found ${peopleRows[0].count.toLocaleString()} existing people`);

        if (samplePeople.length > 0) {
            console.log('\n📄 Sample people:');
            samplePeople.forEach(person => {
                const name = `${person.first_name || ''} ${person.last_name || ''}`.trim();
                const location = `${person.city || ''}, ${person.state || ''}`.replace(/^,\s*|,\s*$/g, '');
                console.log(`   • ${name} - ${location}`);
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
        console.log('\n✅ SQL integration test passed!');
        process.exit(0);
    } else {
        console.log('\n❌ SQL integration test failed!');
        process.exit(1);
    }
}

main().catch(console.error);