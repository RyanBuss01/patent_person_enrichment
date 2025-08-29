#!/usr/bin/env node

/**
 * Debug Path Calculation
 * Check what paths are being calculated and which ones actually exist
 */

const path = require('path');
const fs = require('fs');

console.log('=== PATH DEBUGGING ===');
console.log('__dirname:', __dirname);
console.log('process.cwd():', process.cwd());

console.log('\n=== PATH CALCULATIONS ===');
const path1 = path.join(__dirname, '..', 'converted_databases', 'csv');
console.log('__dirname + .. approach:', path1);

const path2 = path.join(process.cwd(), '..', 'converted_databases', 'csv');  
console.log('process.cwd() + .. approach:', path2);

const path3 = path.resolve(__dirname, '..', 'converted_databases', 'csv');
console.log('resolve approach:', path3);

// The path we know works
const knownGoodPath = '/Users/ryanbussert/Desktop/Work/National_Engravers/Patent_Grants/converted_databases/csv';
console.log('Known good path:', knownGoodPath);

console.log('\n=== EXISTENCE CHECKS ===');
console.log('Path 1 exists:', fs.existsSync(path1));
console.log('Path 2 exists:', fs.existsSync(path2)); 
console.log('Path 3 exists:', fs.existsSync(path3));
console.log('Known good path exists:', fs.existsSync(knownGoodPath));

// Check what files are in each existing path
[path1, path2, path3, knownGoodPath].forEach((p, i) => {
    console.log(`\n=== FILES IN PATH ${i + 1} ===`);
    if (fs.existsSync(p)) {
        try {
            const files = fs.readdirSync(p);
            const csvFiles = files.filter(f => f.endsWith('.csv'));
            console.log(`Found ${csvFiles.length} CSV files in: ${p}`);
            console.log('First 3 CSV files:', csvFiles.slice(0, 3));
        } catch (error) {
            console.log('Error reading directory:', error.message);
        }
    } else {
        console.log('Path does not exist:', p);
    }
});