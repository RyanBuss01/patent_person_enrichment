const express = require('express');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

// Load environment variables from the frontend .env file
require('dotenv').config({ path: path.join(__dirname, '.env') });

const app = express();
const PORT = 3000;

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname)));

// Serve the main HTML file
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

// Helper function to run Python scripts
function runPythonScript(scriptPath, args = []) {
    return new Promise((resolve, reject) => {
        console.log(`Running: python ${scriptPath} ${args.join(' ')}`);
        
        // Pass frontend environment variables to Python
        const env = {
            ...process.env,
            PYTHONPATH: path.join(__dirname, '..'),
            // Pass API keys from frontend .env to Python
            PEOPLEDATALABS_API_KEY: process.env.PEOPLEDATALABS_API_KEY || 'YOUR_PDL_API_KEY',
            PATENTSVIEW_API_KEY: process.env.PATENTSVIEW_API_KEY || 'YOUR_API_KEY',
            MAX_ENRICHMENT_COST: process.env.MAX_ENRICHMENT_COST || '1000',
            DAYS_BACK: process.env.DAYS_BACK || '7',
            MAX_RESULTS: process.env.MAX_RESULTS || '1000'
        };
        
        const python = spawn('python', [scriptPath, ...args], {
            cwd: path.join(__dirname, '..'), // Run from Patent_Grants directory
            env: env
        });
        
        let stdout = '';
        let stderr = '';
        
        python.stdout.on('data', (data) => {
            stdout += data.toString();
            console.log(data.toString());
        });
        
        python.stderr.on('data', (data) => {
            stderr += data.toString();
            console.error(data.toString());
        });
        
        python.on('close', (code) => {
            if (code === 0) {
                resolve({ success: true, output: stdout, stderr });
            } else {
                reject({ success: false, error: `Process exited with code ${code}`, stderr, stdout });
            }
        });
    });
}

// Helper function to create a configuration dict for Python scripts
function createConfig() {
    return {
        'ACCESS_DB_PATH': process.env.ACCESS_DB_PATH || "patent_system/Database.mdb",
        'USPC_DOWNLOAD_PATH': process.env.USPC_DOWNLOAD_PATH || "USPC_Download",
        'CSV_DATABASE_FOLDER': "converted_databases/csv",
        'USE_EXISTING_DATA': true,
        'ENRICH_ONLY_NEW_PEOPLE': true,
        'MAX_ENRICHMENT_COST': parseInt(process.env.MAX_ENRICHMENT_COST || '1000'),
        'PATENTSVIEW_API_KEY': process.env.PATENTSVIEW_API_KEY || "oq371zFI.BjeAbayJsdHdvEgbei0vskz5bTK3KM1S",
        'EXTRACT_BY_DATE': true,
        'DAYS_BACK': parseInt(process.env.DAYS_BACK || '7'),
        'MAX_RESULTS': parseInt(process.env.MAX_RESULTS || '1000'),
        'PEOPLEDATALABS_API_KEY': process.env.PEOPLEDATALABS_API_KEY || "YOUR_PDL_API_KEY",
        'XML_FILE_PATH': "ipg250812.xml",
        'OUTPUT_DIR': process.env.OUTPUT_DIR || 'output',
        'OUTPUT_CSV': "output/enriched_patents.csv",
        'OUTPUT_JSON': "output/enriched_patents.json"
    };
}

// Helper function to read JSON files
function readJsonFile(filePath) {
    try {
        const fullPath = path.join(__dirname, '..', filePath);
        if (fs.existsSync(fullPath)) {
            return JSON.parse(fs.readFileSync(fullPath, 'utf8'));
        }
        return null;
    } catch (error) {
        console.error(`Error reading ${filePath}:`, error);
        return null;
    }
}

// Helper function to get file stats
function getFileStats(filePath) {
    try {
        const fullPath = path.join(__dirname, '..', filePath);
        if (fs.existsSync(fullPath)) {
            const stats = fs.statSync(fullPath);
            return {
                exists: true,
                size: stats.size,
                modified: stats.mtime,
                sizeKB: Math.round(stats.size / 1024)
            };
        }
        return { exists: false };
    } catch (error) {
        return { exists: false, error: error.message };
    }
}

// Step 0: Integrate Existing Data
app.post('/api/step0', async (req, res) => {
    try {
        console.log('Starting Step 0: Integrate Existing Data');
        
        const result = await runPythonScript('front-end/run_step0_wrapper.py');
        
        // Read the results
        const integrationResults = readJsonFile('output/integration_results.json');
        const newPeople = readJsonFile('output/new_people_for_enrichment.json');
        const newPatents = readJsonFile('output/filtered_new_patents.json');
        
        res.json({
            success: true,
            message: 'Step 0 completed successfully',
            results: integrationResults,
            files: {
                newPeople: {
                    count: newPeople ? newPeople.length : 0,
                    stats: getFileStats('output/new_people_for_enrichment.json')
                },
                newPatents: {
                    count: newPatents ? newPatents.length : 0,
                    stats: getFileStats('output/filtered_new_patents.json')
                },
                integrationResults: getFileStats('output/integration_results.json')
            },
            output: result.output
        });
        
    } catch (error) {
        console.error('Step 0 error:', error);
        res.status(500).json({
            success: false,
            error: error.error || error.message,
            stderr: error.stderr,
            stdout: error.stdout
        });
    }
});

// Step 1: Extract Patents (only if needed)
app.post('/api/step1', async (req, res) => {
    try {
        console.log('Starting Step 1: Extract Patents from USPTO API');
        
        const result = await runPythonScript('front-end/run_step1_wrapper.py');
        
        // Check if Step 1 was skipped (using XML data instead)
        const extractionResults = readJsonFile('output/extraction_results.json');
        
        let files;
        if (extractionResults && extractionResults.skipped_api) {
            // Step 1 was skipped, point to Step 0 data instead
            const newPatents = readJsonFile('output/filtered_new_patents.json');
            files = {
                rawPatents: {
                    count: newPatents ? newPatents.length : 0,
                    stats: getFileStats('output/filtered_new_patents.json'),
                    source: 'xml_data_from_step0'
                },
                rawPatentsCsv: { 
                    exists: false, 
                    note: 'Using XML data from Step 0 instead' 
                }
            };
        } else {
            // Normal API extraction occurred
            const rawPatents = readJsonFile('output/raw_patents.json');
            files = {
                rawPatents: {
                    count: rawPatents ? rawPatents.length : 0,
                    stats: getFileStats('output/raw_patents.json')
                },
                rawPatentsCsv: getFileStats('output/raw_patents.csv')
            };
        }
        
        res.json({
            success: true,
            message: extractionResults && extractionResults.skipped_api 
                ? 'Step 1 skipped - using XML data from Step 0' 
                : 'Step 1 completed successfully',
            files: files,
            output: result.output
        });
        
    } catch (error) {
        console.error('Step 1 error:', error);
        res.status(500).json({
            success: false,
            error: error.error || error.message,
            stderr: error.stderr,
            stdout: error.stdout
        });
    }
});

// Step 2: Data Enrichment
app.post('/api/step2', async (req, res) => {
    const { testMode = false } = req.body;
    
    try {
        console.log(`Starting Step 2: Data Enrichment${testMode ? ' (TEST MODE)' : ''}`);
        
        const args = testMode ? ['--test'] : [];
        const result = await runPythonScript('front-end/run_step2_wrapper.py', args);
        
        // Read the results
        const enrichedData = readJsonFile('output/enriched_patents.json');
        const enrichmentResults = readJsonFile('output/enrichment_results.json');
        
        res.json({
            success: true,
            message: `Step 2 completed successfully${testMode ? ' (test mode)' : ''}`,
            results: enrichmentResults,
            files: {
                enrichedData: {
                    count: enrichedData ? enrichedData.length : 0,
                    stats: getFileStats('output/enriched_patents.json')
                },
                enrichedCsv: getFileStats('output/enriched_patents.csv'),
                enrichmentResults: getFileStats('output/enrichment_results.json')
            },
            output: result.output
        });
        
    } catch (error) {
        console.error('Step 2 error:', error);
        res.status(500).json({
            success: false,
            error: error.error || error.message,
            stderr: error.stderr,
            stdout: error.stdout
        });
    }
});

// Get current status of all files
app.get('/api/status', (req, res) => {
    const status = {
        step0: {
            integrationResults: getFileStats('output/integration_results.json'),
            newPeople: getFileStats('output/new_people_for_enrichment.json'),
            newPatents: getFileStats('output/filtered_new_patents.json')
        },
        step1: {
            rawPatents: getFileStats('output/raw_patents.json'),
            rawPatentsCsv: getFileStats('output/raw_patents.csv'),
            // Also check if Step 1 was skipped
            extractionResults: getFileStats('output/extraction_results.json')
        },
        step2: {
            enrichedData: getFileStats('output/enriched_patents.json'),
            enrichedCsv: getFileStats('output/enriched_patents.csv'),
            enrichmentResults: getFileStats('output/enrichment_results.json')
        }
    };
    
    // Add file counts
    const newPeople = readJsonFile('output/new_people_for_enrichment.json');
    const newPatents = readJsonFile('output/filtered_new_patents.json');
    const enrichedData = readJsonFile('output/enriched_patents.json');
    
    // For Step 1, check if it was skipped
    const extractionResults = readJsonFile('output/extraction_results.json');
    let rawPatentsCount = 0;
    if (extractionResults && extractionResults.skipped_api) {
        // Step 1 was skipped, use Step 0 data count
        rawPatentsCount = newPatents ? newPatents.length : 0;
    } else {
        // Normal API extraction
        const rawPatents = readJsonFile('output/raw_patents.json');
        rawPatentsCount = rawPatents ? rawPatents.length : 0;
    }
    
    status.counts = {
        newPeople: newPeople ? newPeople.length : 0,
        newPatents: newPatents ? newPatents.length : 0,
        rawPatents: rawPatentsCount,
        enrichedData: enrichedData ? enrichedData.length : 0
    };
    
    // Add Step 1 status info
    status.step1.skipped = extractionResults && extractionResults.skipped_api;
    status.step1.source = extractionResults && extractionResults.skipped_api ? 'xml_data' : 'api_data';
    
    res.json(status);
});

// Get sample data from files
app.get('/api/sample/:step', (req, res) => {
    const { step } = req.params;
    let sampleData;
    
    try {
        switch(step) {
            case 'step0-people':
                sampleData = readJsonFile('output/new_people_for_enrichment.json');
                if (sampleData) sampleData = sampleData.slice(0, 5);
                break;
            case 'step0-patents':
                sampleData = readJsonFile('output/filtered_new_patents.json');
                if (sampleData) sampleData = sampleData.slice(0, 3);
                break;
            case 'step1':
                // Check if Step 1 was skipped and used XML data instead
                const extractionResults = readJsonFile('output/extraction_results.json');
                if (extractionResults && extractionResults.skipped_api) {
                    // Use XML data from Step 0
                    sampleData = readJsonFile('output/filtered_new_patents.json');
                    if (sampleData) {
                        sampleData = sampleData.slice(0, 3);
                        // Add a note that this is from XML, not API
                        sampleData = {
                            note: "Step 1 was skipped - showing XML data from Step 0 instead",
                            source: "filtered_new_patents.json (from Step 0)",
                            data: sampleData
                        };
                    }
                } else {
                    // Normal API data
                    sampleData = readJsonFile('output/raw_patents.json');
                    if (sampleData) sampleData = sampleData.slice(0, 3);
                }
                break;
            case 'step2':
                sampleData = readJsonFile('output/enriched_patents.json');
                if (sampleData) sampleData = sampleData.slice(0, 3);
                break;
            default:
                return res.status(400).json({ error: 'Invalid step' });
        }
        
        res.json({ sample: sampleData });
        
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.listen(PORT, () => {
    console.log(`Server running at http://localhost:${PORT}`);
    console.log('Make sure you have activated your Python environment (patent_env)');
});