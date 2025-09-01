const express = require('express');
const { spawn, spawnSync } = require('child_process');
const path = require('path');
const fs = require('fs');

// Load environment variables from the frontend .env file
require('dotenv').config({ path: path.join(__dirname, '.env') });

const app = express();
const PORT = process.env.PORT || 3000;

// Global state for tracking running processes
const runningProcesses = new Map();

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname)));

// Serve the main HTML file
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

// Resolve a Python interpreter, preferring an active venv if provided
function resolvePython() {
    const isWindows = process.platform === 'win32';

    // 1) Explicit override via env var
    if (process.env.PYTHON_BIN && fs.existsSync(process.env.PYTHON_BIN)) {
        return process.env.PYTHON_BIN;
    }

    // 2) Venv path provided via env var
    const venvPath = process.env.VENV_PATH;
    if (venvPath) {
        const candidate = path.join(venvPath, isWindows ? 'Scripts' : 'bin', isWindows ? 'python.exe' : 'python3');
        if (fs.existsSync(candidate)) return candidate;
        const candidatePy = path.join(venvPath, isWindows ? 'Scripts' : 'bin', isWindows ? 'python.exe' : 'python');
        if (fs.existsSync(candidatePy)) return candidatePy;
    }

    // 3) Project-local venv (avoid using an incompatible copied venv if not executable)
    const projectVenv = path.join(__dirname, '..', 'patent_env');
    const projectCandidates = [
        path.join(projectVenv, isWindows ? 'Scripts' : 'bin', isWindows ? 'python.exe' : 'python3'),
        path.join(projectVenv, isWindows ? 'Scripts' : 'bin', isWindows ? 'python.exe' : 'python'),
    ];
    for (const c of projectCandidates) {
        try {
            if (fs.existsSync(c)) {
                fs.accessSync(c, fs.constants.X_OK);
                return c;
            }
        } catch (_) { /* not executable on this system */ }
    }

    // 4) Fallback to system python3/python
    const which = (cmd) => {
        try {
            const out = spawnSync(isWindows ? 'where' : 'which', [cmd], { encoding: 'utf8' });
            if (out.status === 0) {
                const p = out.stdout.split(/\r?\n/).find(Boolean);
                if (p && fs.existsSync(p)) return p.trim();
            }
        } catch (_) {}
        return null;
    };
    return which('python3') || which('python') || (isWindows ? 'python' : 'python3');
}

// Helper function to run Python scripts asynchronously
function runPythonScriptAsync(scriptPath, args = [], stepId) {
    return new Promise((resolve, reject) => {
        const pythonExec = resolvePython();
        console.log(`Running: ${pythonExec} ${scriptPath} ${args.join(' ')}`);
        
        // Pass frontend environment variables to Python
        const env = {
            ...process.env,
            PYTHONPATH: path.join(__dirname, '..'),
            
            // Database credentials - pass from Node.js to Python
            DB_HOST: process.env.DB_HOST || 'localhost',
            DB_PORT: process.env.DB_PORT || '3306',
            DB_NAME: process.env.DB_NAME || 'patent_data',
            DB_USER: process.env.DB_USER || 'root',
            DB_PASSWORD: process.env.DB_PASSWORD || 'password',
            DB_ENGINE: process.env.DB_ENGINE || 'mysql',
            
            // API keys
            PEOPLEDATALABS_API_KEY: process.env.PEOPLEDATALABS_API_KEY || 'YOUR_PDL_API_KEY',
            PATENTSVIEW_API_KEY: process.env.PATENTSVIEW_API_KEY || 'oq371zFI.BjeAbayJsdHdvEgbei0vskz5bTK3KM1S',
            MAX_ENRICHMENT_COST: process.env.MAX_ENRICHMENT_COST || '1000',
            DAYS_BACK: process.env.DAYS_BACK || '7',
            MAX_RESULTS: process.env.MAX_RESULTS || '1000'
        };
        
        const python = spawn(pythonExec, [scriptPath, ...args], {
            cwd: path.join(__dirname, '..'),
            env: env
        });

        
        let stdout = '';
        let stderr = '';
        
        // Store process info for status tracking
        runningProcesses.set(stepId, {
            process: python,
            startTime: new Date(),
            progress: 'Starting...',
            stdout: '',
            stderr: ''
        });
        
        python.stdout.on('data', (data) => {
            const output = data.toString();
            stdout += output;
            console.log(output);
            
            // Update progress from output
            const processInfo = runningProcesses.get(stepId);
            if (processInfo) {
                processInfo.stdout += output;
                
                // Extract progress information from output
                if (output.includes('PROGRESS:')) {
                    const progressMatch = output.match(/PROGRESS: (.+)/);
                    if (progressMatch) {
                        processInfo.progress = progressMatch[1];
                    }
                } else if (output.includes('Processing')) {
                    processInfo.progress = 'Processing data...';
                } else if (output.includes('Loading')) {
                    processInfo.progress = 'Loading databases...';
                } else if (output.includes('Downloading')) {
                    processInfo.progress = 'Downloading patents from API...';
                } else if (output.includes('Enhanced filtering')) {
                    processInfo.progress = 'Filtering and matching data...';
                }
            }
        });
        
        python.stderr.on('data', (data) => {
            const output = data.toString();
            stderr += output;
            console.error(output);
            
            const processInfo = runningProcesses.get(stepId);
            if (processInfo) {
                processInfo.stderr += output;
            }
        });
        
        python.on('close', (code) => {
            // Remove from running processes
            runningProcesses.delete(stepId);
            
            if (code === 0) {
                resolve({ success: true, output: stdout, stderr });
            } else {
                reject({ success: false, error: `Process exited with code ${code}`, stderr, stdout });
            }
        });
        
        python.on('error', (error) => {
            runningProcesses.delete(stepId);
            reject({ success: false, error: error.message, stderr, stdout });
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
        'OUTPUT_JSON': "output/enriched_patents.json",
        'DEDUP_NEW_PEOPLE': true
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

// Helper function to write JSON files
function writeJsonFile(filePath, data) {
    try {
        const fullPath = path.join(__dirname, '..', filePath);
        fs.writeFileSync(fullPath, JSON.stringify(data, null, 2));
        return true;
    } catch (error) {
        console.error(`Error writing ${filePath}:`, error);
        return false;
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

// Helper: get SQL counts via a small Python script
function getSqlCounts() {
    try {
        const pythonPath = resolvePython();
        const scriptPath = path.join(__dirname, '..', 'scripts', 'get_sql_counts.py');
        const env = {
            ...process.env,
            PYTHONPATH: path.join(__dirname, '..'),
        };
        const proc = spawnSync(pythonPath, [scriptPath], { env });
        if (proc.status === 0) {
            const out = proc.stdout.toString().trim();
            return JSON.parse(out || '{}');
        }
        return {};
    } catch (e) {
        return {};
    }
}

// NEW Step 0: Download Patents from PatentsView API
app.post('/api/step0', async (req, res) => {
    const stepId = 'step0';
    
    // Check if already running
    if (runningProcesses.has(stepId)) {
        return res.json({
            success: false,
            error: 'Step 0 is already running'
        });
    }
    
    try {
        console.log('Starting Step 0: Download Patents from PatentsView API (Async)');
        
        // Get options from request body
        const { 
            mode = 'smart', // 'smart' or 'manual'
            startDate = null, 
            endDate = null,
            daysBack = 7,
            maxResults = 1000
        } = req.body;
        
        // Prepare arguments for Python script
        const args = [];
        if (mode === 'manual' && startDate && endDate) {
            args.push('--mode', 'manual', '--start-date', startDate, '--end-date', endDate);
        } else {
            args.push('--mode', 'smart', '--days-back', daysBack.toString());
        }
        args.push('--max-results', maxResults.toString());
        
        // Start the process asynchronously
        runPythonScriptAsync('front-end/run_step0_wrapper.py', args, stepId)
            .then((result) => {
                console.log('Step 0 completed successfully');
                
                // Store completion result for status endpoint
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: true,
                    output: result.output,
                    completedAt: new Date(),
                    files: getStep0Files()
                });
            })
            .catch((error) => {
                console.error('Step 0 failed:', error);
                
                // Store error result for status endpoint
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: false,
                    error: error.error || error.message,
                    stderr: error.stderr,
                    stdout: error.stdout,
                    completedAt: new Date()
                });
            });
        
        // Return immediately with "started" status
        res.json({
            success: true,
            status: 'started',
            processing: true,
            message: 'Step 0 started successfully. Use /api/step0/status to check progress.',
            config: { mode, startDate, endDate, daysBack, maxResults }
        });
        
    } catch (error) {
        console.error('Step 0 startup error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Helper functions for Step 0 completion data
function getStep0Files() {
    const downloadedPatents = readJsonFile('output/downloaded_patents.json');
    
    return {
        downloadedPatents: {
            count: downloadedPatents ? downloadedPatents.length : 0,
            stats: getFileStats('output/downloaded_patents.json')
        },
        downloadResults: getFileStats('output/download_results.json')
    };
}

// Step 0 Status endpoint for polling
app.get('/api/step0/status', (req, res) => {
    const stepId = 'step0';
    
    // Check if completed
    const completedInfo = runningProcesses.get(stepId + '_completed');
    if (completedInfo) {
        // Remove completed info after returning it
        runningProcesses.delete(stepId + '_completed');
        
        return res.json({
            completed: true,
            success: completedInfo.success,
            output: completedInfo.output,
            error: completedInfo.error,
            stderr: completedInfo.stderr,
            stdout: completedInfo.stdout,
            files: completedInfo.files
        });
    }
    
    // Check if still running
    const runningInfo = runningProcesses.get(stepId);
    if (runningInfo) {
        const elapsed = Math.round((new Date() - runningInfo.startTime) / 1000);
        return res.json({
            completed: false,
            running: true,
            progress: runningInfo.progress,
            elapsedSeconds: elapsed
        });
    }
    
    // Not running and not completed
    res.json({
        completed: false,
        running: false,
        error: 'Step 0 has not been started or status was already retrieved'
    });
});

// Step 1: Integrate Existing Data (was Step 0)
app.post('/api/step1', async (req, res) => {
    const stepId = 'step1';
    
    // Check if already running
    if (runningProcesses.has(stepId)) {
        return res.json({
            success: false,
            error: 'Step 1 is already running'
        });
    }
    
    try {
        console.log('Starting Step 1: Integrate Existing Data (Async)');
        
        // Start the process asynchronously
        runPythonScriptAsync('front-end/run_step1_wrapper.py', [], stepId)
            .then((result) => {
                console.log('Step 1 completed successfully');
                
                // Store completion result for status endpoint
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: true,
                    output: result.output,
                    completedAt: new Date(),
                    files: getStep1Files(),
                    potentialMatches: getStep1PotentialMatches()
                });
            })
            .catch((error) => {
                console.error('Step 1 failed:', error);
                
                // Store error result for status endpoint
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: false,
                    error: error.error || error.message,
                    stderr: error.stderr,
                    stdout: error.stdout,
                    completedAt: new Date()
                });
            });
        
        // Return immediately with "started" status
        res.json({
            success: true,
            status: 'started',
            processing: true,
            message: 'Step 1 started successfully. Use /api/step1/status to check progress.'
        });
        
    } catch (error) {
        console.error('Step 1 startup error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Helper functions for Step 1 completion data
function getStep1Files() {
    const newPeople = readJsonFile('output/new_people_for_enrichment.json');
    const newPatents = readJsonFile('output/filtered_new_patents.json');
    
    return {
        newPeople: {
            count: newPeople ? newPeople.length : 0,
            stats: getFileStats('output/new_people_for_enrichment.json')
        },
        newPatents: {
            count: newPatents ? newPatents.length : 0,
            stats: getFileStats('output/filtered_new_patents.json')
        },
        integrationResults: getFileStats('output/integration_results.json')
    };
}

function getStep1PotentialMatches() {
    const newPeople = readJsonFile('output/new_people_for_enrichment.json');
    
    if (!newPeople) return [];
    
    return newPeople.filter(person => 
        person.match_score >= 10 && person.match_score < 25 && 
        person.match_status === 'needs_review'
    );
}

// Step 1 Status endpoint for polling
app.get('/api/step1/status', (req, res) => {
    const stepId = 'step1';
    
    // Check if completed
    const completedInfo = runningProcesses.get(stepId + '_completed');
    if (completedInfo) {
        // Remove completed info after returning it
        runningProcesses.delete(stepId + '_completed');
        
        return res.json({
            completed: true,
            success: completedInfo.success,
            output: completedInfo.output,
            error: completedInfo.error,
            stderr: completedInfo.stderr,
            stdout: completedInfo.stdout,
            files: completedInfo.files,
            potentialMatches: completedInfo.potentialMatches
        });
    }
    
    // Check if still running
    const runningInfo = runningProcesses.get(stepId);
    if (runningInfo) {
        const elapsed = Math.round((new Date() - runningInfo.startTime) / 1000);
        return res.json({
            completed: false,
            running: true,
            progress: runningInfo.progress,
            elapsedSeconds: elapsed
        });
    }
    
    // Not running and not completed
    res.json({
        completed: false,
        running: false,
        error: 'Step 1 has not been started or status was already retrieved'
    });
});

// Step 2 Status endpoint for polling
app.get('/api/step2/status', (req, res) => {
    const stepId = 'step2';
    
    // Check if completed
    const completedInfo = runningProcesses.get(stepId + '_completed');
    if (completedInfo) {
        runningProcesses.delete(stepId + '_completed');
        
        return res.json({
            completed: true,
            success: completedInfo.success,
            output: completedInfo.output,
            error: completedInfo.error,
            files: completedInfo.files
        });
    }
    
    // Check if still running
    const runningInfo = runningProcesses.get(stepId);
    if (runningInfo) {
        const elapsed = Math.round((new Date() - runningInfo.startTime) / 1000);
        return res.json({
            completed: false,
            running: true,
            progress: runningInfo.progress,
            elapsedSeconds: elapsed
        });
    }
    
    res.json({
        completed: false,
        running: false,
        error: 'Step 2 has not been started or status was already retrieved'
    });
});

// Step 3 Status endpoint for polling
app.get('/api/step3/status', (req, res) => {
    const stepId = 'step3';
    
    // Check if completed
    const completedInfo = runningProcesses.get(stepId + '_completed');
    if (completedInfo) {
        runningProcesses.delete(stepId + '_completed');
        
        return res.json({
            completed: true,
            success: completedInfo.success,
            output: completedInfo.output,
            error: completedInfo.error,
            files: completedInfo.files
        });
    }
    
    // Check if still running
    const runningInfo = runningProcesses.get(stepId);
    if (runningInfo) {
        const elapsed = Math.round((new Date() - runningInfo.startTime) / 1000);
        return res.json({
            completed: false,
            running: true,
            progress: runningInfo.progress,
            elapsedSeconds: elapsed
        });
    }
    
    res.json({
        completed: false,
        running: false,
        error: 'Step 3 has not been started or status was already retrieved'
    });
});

// Get verification data (people needing review)
app.get('/api/verification-data', (req, res) => {
    try {
        const newPeople = readJsonFile('output/new_people_for_enrichment.json');
        
        if (!newPeople) {
            return res.json({
                success: false,
                error: 'No people data found. Run Step 1 first.'
            });
        }
        
        // Filter people with potential matches (scores 10-24) for verification
        const potentialMatches = newPeople.filter(person => 
            person.match_score >= 10 && person.match_score < 25 && 
            person.match_status === 'needs_review'
        );
        
        res.json({
            success: true,
            potentialMatches: potentialMatches,
            totalCount: potentialMatches.length
        });
        
    } catch (error) {
        console.error('Error getting verification data:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Finalize verification decisions
app.post('/api/finalize-verification', (req, res) => {
    try {
        const { decisions } = req.body;
        
        if (!decisions) {
            return res.status(400).json({
                success: false,
                error: 'No decisions provided'
            });
        }
        
        // Read current people data
        const newPeople = readJsonFile('output/new_people_for_enrichment.json');
        if (!newPeople) {
            return res.status(400).json({
                success: false,
                error: 'No people data found'
            });
        }
        
        // Create updated lists based on decisions
        const updatedEnrichmentList = [];
        const existingPeopleList = [];
        
        newPeople.forEach(person => {
            const decision = decisions[person.person_id];
            
            if (decision) {
                if (decision.decision === 'new') {
                    // Add to enrichment list (remove needs_review flag)
                    updatedEnrichmentList.push({
                        ...person,
                        match_status: 'verified_new',
                        verification_decision: 'user_approved_for_enrichment'
                    });
                } else if (decision.decision === 'existing') {
                    // Move to existing people list
                    existingPeopleList.push({
                        ...person,
                        match_status: 'verified_existing',
                        verification_decision: 'user_marked_as_existing'
                    });
                } else if (decision.decision === 'skip') {
                    // Keep in enrichment list but mark as skipped
                    updatedEnrichmentList.push({
                        ...person,
                        match_status: 'verification_skipped',
                        verification_decision: 'user_skipped'
                    });
                }
            } else {
                // No decision made, keep as is
                updatedEnrichmentList.push(person);
            }
        });
        
        // Save updated enrichment list
        const success = writeJsonFile('output/new_people_for_enrichment.json', updatedEnrichmentList);
        
        if (success) {
            // Also save the existing people list for reference
            const existingPeopleFound = readJsonFile('output/existing_people_found.json') || [];
            const combinedExisting = [...existingPeopleFound, ...existingPeopleList];
            writeJsonFile('output/existing_people_found.json', combinedExisting);
            
            // Update integration results with new counts
            const integrationResults = readJsonFile('output/integration_results.json') || {};
            integrationResults.verified_new_people_count = updatedEnrichmentList.length;
            integrationResults.verified_existing_people_count = existingPeopleList.length;
            integrationResults.verification_completed = true;
            integrationResults.verification_timestamp = new Date().toISOString();
            writeJsonFile('output/integration_results.json', integrationResults);
            
            res.json({
                success: true,
                message: 'Verification decisions saved successfully',
                stats: {
                    enrichmentListCount: updatedEnrichmentList.length,
                    existingPeopleCount: existingPeopleList.length,
                    decisionsProcessed: Object.keys(decisions).length
                }
            });
        } else {
            res.status(500).json({
                success: false,
                error: 'Failed to save verification decisions'
            });
        }
        
    } catch (error) {
        console.error('Error finalizing verification:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Step 2: Data Enrichment (moved up from Step 3)
app.post('/api/step2', async (req, res) => {
    const { testMode = false } = req.body;
    const stepId = 'step2';
    
    if (runningProcesses.has(stepId)) {
        return res.json({
            success: false,
            error: 'Step 2 is already running'
        });
    }
    
    try {
        console.log(`Starting Step 2: Data Enrichment${testMode ? ' (TEST MODE)' : ''} (Async)`);
        const args = testMode ? ['--test'] : [];
        runPythonScriptAsync('front-end/run_step2_wrapper.py', args, stepId)
            .then((result) => {
                console.log('Step 2 completed successfully');
                const enrichedData = readJsonFile('output/enriched_patents.json');
                const enrichmentResults = readJsonFile('output/enrichment_results.json');
                const files = {
                    enrichedData: {
                        count: enrichedData ? enrichedData.length : 0,
                        stats: getFileStats('output/enriched_patents.json')
                    },
                    enrichedCsv: getFileStats('output/enriched_patents.csv'),
                    enrichmentResults: getFileStats('output/enrichment_results.json')
                };
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: true,
                    output: result.output,
                    files: files,
                    results: enrichmentResults,
                    completedAt: new Date()
                });
            })
            .catch((error) => {
                console.error('Step 2 failed:', error);
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: false,
                    error: error.error || error.message,
                    stderr: error.stderr,
                    stdout: error.stdout,
                    completedAt: new Date()
                });
            });
        
        res.json({
            success: true,
            status: 'started',
            processing: true,
            message: `Step 2 started successfully${testMode ? ' (test mode)' : ''}. Use /api/step2/status to check progress.`
        });
    } catch (error) {
        console.error('Step 2 startup error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Step 3: Data Enrichment (was Step 2)
app.post('/api/step3', async (req, res) => {
    const { testMode = false } = req.body;
    const stepId = 'step3';
    
    if (runningProcesses.has(stepId)) {
        return res.json({
            success: false,
            error: 'Step 3 is already running'
        });
    }
    
    try {
        console.log(`Starting Step 3: Data Enrichment${testMode ? ' (TEST MODE)' : ''} (Async)`);
        
        const args = testMode ? ['--test'] : [];
        
        // Start the process asynchronously
        runPythonScriptAsync('front-end/run_step3_wrapper.py', args, stepId)
            .then((result) => {
                console.log('Step 3 completed successfully');
                
                const enrichedData = readJsonFile('output/enriched_patents.json');
                const enrichmentResults = readJsonFile('output/enrichment_results.json');
                
                const files = {
                    enrichedData: {
                        count: enrichedData ? enrichedData.length : 0,
                        stats: getFileStats('output/enriched_patents.json')
                    },
                    enrichedCsv: getFileStats('output/enriched_patents.csv'),
                    enrichmentResults: getFileStats('output/enrichment_results.json')
                };
                
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: true,
                    output: result.output,
                    files: files,
                    results: enrichmentResults,
                    completedAt: new Date()
                });
            })
            .catch((error) => {
                console.error('Step 3 failed:', error);
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: false,
                    error: error.error || error.message,
                    stderr: error.stderr,
                    stdout: error.stdout,
                    completedAt: new Date()
                });
            });
        
        res.json({
            success: true,
            status: 'started',
            processing: true,
            message: `Step 3 started successfully${testMode ? ' (test mode)' : ''}. Use /api/step3/status to check progress.`
        });
        
    } catch (error) {
        console.error('Step 3 startup error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get current status of all files
app.get('/api/status', (req, res) => {
    const status = {
        step0: {
            downloadResults: getFileStats('output/download_results.json'),
            // downloadedPatents metric removed from UI
            downloadedPatents: getFileStats('output/downloaded_patents.json'),
            running: runningProcesses.has('step0')
        },
        step1: {
            integrationResults: getFileStats('output/integration_results.json'),
            newPeople: getFileStats('output/new_people_for_enrichment.json'),
            newPatents: getFileStats('output/filtered_new_patents.json'),
            running: runningProcesses.has('step1')
        },
        step2: {
            enrichedData: getFileStats('output/enriched_patents.json'),
            enrichedCsv: getFileStats('output/enriched_patents.csv'),
            enrichmentResults: getFileStats('output/enrichment_results.json'),
            running: runningProcesses.has('step2')
        }
    };
    
    // Add file counts
    const downloadedPatents = readJsonFile('output/downloaded_patents.json');
    const newPeople = readJsonFile('output/new_people_for_enrichment.json');
    const newPatents = readJsonFile('output/filtered_new_patents.json');
    const enrichedData = readJsonFile('output/enriched_patents.json');

    // Pull enriched count from SQL
    const sqlCounts = getSqlCounts();
    
    status.counts = {
        newPeople: newPeople ? newPeople.length : 0,
        newPatents: newPatents ? newPatents.length : 0,
        enrichedData: typeof sqlCounts.enriched_people === 'number' ? sqlCounts.enriched_people : (enrichedData ? enrichedData.length : 0)
    };
    
    // Add verification info
    const integrationResults = readJsonFile('output/integration_results.json');
    if (integrationResults) {
        status.verification = {
            completed: integrationResults.verification_completed || false,
            needsReview: newPeople ? newPeople.filter(p => 
                p.match_score >= 10 && p.match_score < 25 && 
                p.match_status === 'needs_review'
            ).length : 0
        };
    }
    
    // Step 2 now represents enrichment; no extraction status
    
    res.json(status);
});

// Get sample data from files
app.get('/api/sample/:step', (req, res) => {
    const { step } = req.params;
    let sampleData;
    
    try {
        switch(step) {
            case 'step0':
                sampleData = readJsonFile('output/downloaded_patents.json');
                if (sampleData) sampleData = sampleData.slice(0, 3);
                break;
            case 'step1-people':
                sampleData = readJsonFile('output/new_people_for_enrichment.json');
                if (sampleData) sampleData = sampleData.slice(0, 5);
                break;
            case 'step1-patents':
                sampleData = readJsonFile('output/filtered_new_patents.json');
                if (sampleData) sampleData = sampleData.slice(0, 3);
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
    console.log('');
    console.log('🚀 Patent Processing Pipeline Server Started (RESTRUCTURED PIPELINE)');
    console.log('📁 Available endpoints:');
    console.log('   POST /api/step0 - Download patents from PatentsView API (NEW)');
    console.log('   GET  /api/step0/status - Check Step 0 progress');
    console.log('   POST /api/step1 - Integrate with existing data (was Step 0)');
    console.log('   GET  /api/step1/status - Check Step 1 progress');
    console.log('   GET  /api/verification-data - Get people needing verification');
    console.log('   POST /api/finalize-verification - Save verification decisions');
    console.log('   POST /api/step2 - Data enrichment');
    console.log('   GET  /api/step2/status - Check Step 2 progress');
    console.log('   GET  /api/status - Get current pipeline status');
    console.log('   GET  /api/sample/:step - Get sample data');
    console.log('');
    console.log('🔧 RESTRUCTURED: All steps moved up +1, new Step 0 for patent download!');
});
