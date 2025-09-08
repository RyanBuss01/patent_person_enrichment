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
// Route-specific body parsers will be used for uploads

// Global process error logging
process.on('unhandledRejection', (reason, p) => {
    console.error('Unhandled Rejection:', reason);
});
process.on('uncaughtException', (err) => {
    console.error('Uncaught Exception:', err);
});

// Serve the main HTML file
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

// Resolve a Python interpreter, preferring an active venv if provided
function resolvePython() {
    const isWindows = process.platform === 'win32';
    const canRun = (cmd) => {
        try {
            const res = spawnSync(cmd, ['-c', 'import sys; print("ok")'], { encoding: 'utf8', timeout: 2000 });
            return res.status === 0 && (res.stdout || '').includes('ok');
        } catch (_) { return false; }
    };

    // 1) Explicit override via env var
    if (process.env.PYTHON_BIN && fs.existsSync(process.env.PYTHON_BIN)) {
        if (canRun(process.env.PYTHON_BIN)) return process.env.PYTHON_BIN;
    }

    // 2) Venv path provided via env var
    const venvPath = process.env.VENV_PATH;
    if (venvPath) {
        const candidate = path.join(venvPath, isWindows ? 'Scripts' : 'bin', isWindows ? 'python.exe' : 'python3');
        if (fs.existsSync(candidate) && canRun(candidate)) return candidate;
        const candidatePy = path.join(venvPath, isWindows ? 'Scripts' : 'bin', isWindows ? 'python.exe' : 'python');
        if (fs.existsSync(candidatePy) && canRun(candidatePy)) return candidatePy;
    }

    // 3) Active shell venv if server was started from it
    if (process.env.VIRTUAL_ENV) {
        const v = process.env.VIRTUAL_ENV;
        const c1 = path.join(v, isWindows ? 'Scripts' : 'bin', isWindows ? 'python.exe' : 'python3');
        const c2 = path.join(v, isWindows ? 'Scripts' : 'bin', isWindows ? 'python.exe' : 'python');
        if (fs.existsSync(c1) && canRun(c1)) return c1;
        if (fs.existsSync(c2) && canRun(c2)) return c2;
    }

    // 4) Project-local venv (avoid using an incompatible copied venv if not runnable)
    const projectVenv = path.join(__dirname, '..', 'patent_env');
    const projectCandidates = [
        path.join(projectVenv, isWindows ? 'Scripts' : 'bin', isWindows ? 'python.exe' : 'python3'),
        path.join(projectVenv, isWindows ? 'Scripts' : 'bin', isWindows ? 'python.exe' : 'python'),
    ];
    for (const c of projectCandidates) {
        try {
            if (fs.existsSync(c) && canRun(c)) return c;
        } catch (_) { /* not runnable on this system */ }
    }

    // 5) Fallback to system python3/python
    const which = (cmd) => {
        try {
            const out = spawnSync(isWindows ? 'where' : 'which', [cmd], { encoding: 'utf8' });
            if (out.status === 0) {
                const p = out.stdout.split(/\r?\n/).find(Boolean);
                if (p && fs.existsSync(p) && canRun(p.trim())) return p.trim();
            }
        } catch (_) {}
        return null;
    };
    return which('python3') || which('python') || (isWindows ? 'python' : 'python3');
}

// Health endpoint to report selected Python
app.get('/api/health', (req, res) => {
    const pythonExec = resolvePython();
    try {
        const info = spawnSync(pythonExec, ['-c', 'import sys,sysconfig; import platform; print("executable=",sys.executable); print("version=",sys.version.split("\\n")[0]); print("platform=",platform.platform()); print("base_prefix=",getattr(sys,"base_prefix","") )'], { encoding: 'utf8', timeout: 2000 });
        res.json({ python: pythonExec, status: info.status, stdout: info.stdout, stderr: info.stderr });
    } catch (e) {
        res.json({ python: pythonExec, error: e.message });
    }
});

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

        // Ensure Python can import local modules and has DB config
        const env = {
            ...process.env,
            PYTHONPATH: path.join(__dirname, '..'),
            // Map SQL_* -> DB_* for Python helpers
            DB_HOST: process.env.DB_HOST || process.env.SQL_HOST || 'localhost',
            DB_PORT: process.env.DB_PORT || process.env.SQL_PORT || '3306',
            DB_NAME: process.env.DB_NAME || process.env.SQL_DATABASE || 'patent_data',
            DB_USER: process.env.DB_USER || process.env.SQL_USER || 'root',
            DB_PASSWORD: process.env.DB_PASSWORD || process.env.SQL_PASSWORD || 'password',
            DB_ENGINE: (process.env.DB_ENGINE || 'mysql').toLowerCase()
        };

        const proc = spawnSync(pythonPath, [scriptPath], { env, cwd: path.join(__dirname, '..') });
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
        // Reset current-run enrichment outputs so counts start at 0 for this cycle
        try {
            const outDir = path.join(__dirname, '..', 'output');
            if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
            fs.writeFileSync(path.join(outDir, 'enriched_patents.json'), '[]');
            const resetMeta = { reset: true, reset_by: 'step0', timestamp: new Date().toISOString() };
            fs.writeFileSync(path.join(outDir, 'enrichment_results.json'), JSON.stringify(resetMeta, null, 2));
            const csvPath = path.join(outDir, 'enriched_patents.csv');
            if (fs.existsSync(csvPath)) fs.unlinkSync(csvPath);
            console.log('Reset current-run enrichment outputs');
        } catch (e) {
            console.warn('Could not reset enrichment output files:', e.message);
        }
        
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

// Step 0: Upload CSV of patents/inventors (alternate input)
app.post('/api/step0/upload-csv', express.text({ type: ['text/csv', 'text/plain', 'application/octet-stream'], limit: '50mb' }), async (req, res) => {
    const stepId = 'step0_upload';
    try {
        console.log('CSV upload hit. content-type=', req.headers['content-type'], 'length=', req.headers['content-length']);
        if (!req.body || typeof req.body !== 'string' || req.body.trim().length === 0) {
            return res.status(400).json({ success: false, error: 'No CSV content received' });
        }

        // Reset current-run enrichment outputs so counts start at 0 for this cycle
        try {
            const outDir = path.join(__dirname, '..', 'output');
            if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
            fs.writeFileSync(path.join(outDir, 'enriched_patents.json'), '[]');
            const resetMeta = { reset: true, reset_by: 'csv_upload', timestamp: new Date().toISOString() };
            fs.writeFileSync(path.join(outDir, 'enrichment_results.json'), JSON.stringify(resetMeta, null, 2));
            const csvPath = path.join(outDir, 'enriched_patents.csv');
            if (fs.existsSync(csvPath)) fs.unlinkSync(csvPath);
        } catch (e) {
            console.warn('Could not reset enrichment output files on upload:', e.message);
        }

        // Hand off parsing to Python for robust CSV handling
        const pythonExec = resolvePython();
        const scriptPath = path.join(__dirname, '..', 'scripts', 'process_uploaded_csv.py');
        const env = { ...process.env, PYTHONPATH: path.join(__dirname, '..') };
        const proc = spawn(pythonExec, [scriptPath], { env, cwd: path.join(__dirname, '..') });
        let stdout = '';
        let stderr = '';
        proc.stdout.on('data', d => { stdout += d.toString(); });
        proc.stderr.on('data', d => { const s = d.toString(); stderr += s; console.error(s); });
        proc.on('close', (code) => {
            if (code === 0) {
                const files = getStep0Files();
                return res.json({ success: true, message: 'CSV uploaded and processed', files, output: stdout });
            } else {
                return res.status(500).json({ success: false, error: `Parser exited with code ${code}`, stderr, stdout });
            }
        });
        // Write CSV body to stdin
        proc.stdin.write(req.body);
        proc.stdin.end();
    } catch (error) {
        console.error('Upload CSV failed:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Step 0: Upload XLSX of patents/inventors (alternate input)
app.post('/api/step0/upload-xlsx', express.raw({ type: ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/octet-stream'], limit: '50mb' }), async (req, res) => {
    try {
        console.log('XLSX upload hit. content-type=', req.headers['content-type'], 'length=', req.headers['content-length']);
        const buf = req.body;
        if (!buf || !Buffer.isBuffer(buf) || buf.length === 0) {
            return res.status(400).json({ success: false, error: 'No XLSX content received' });
        }

        // Reset current-run enrichment outputs so counts start at 0 for this cycle
        try {
            const outDir = path.join(__dirname, '..', 'output');
            if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
            fs.writeFileSync(path.join(outDir, 'enriched_patents.json'), '[]');
            const resetMeta = { reset: true, reset_by: 'xlsx_upload', timestamp: new Date().toISOString() };
            fs.writeFileSync(path.join(outDir, 'enrichment_results.json'), JSON.stringify(resetMeta, null, 2));
            const csvPath = path.join(outDir, 'enriched_patents.csv');
            if (fs.existsSync(csvPath)) fs.unlinkSync(csvPath);
        } catch (e) {
            console.warn('Could not reset enrichment output files on XLSX upload:', e.message);
        }

        const pythonExec = resolvePython();
        const scriptPath = path.join(__dirname, '..', 'scripts', 'process_uploaded_xlsx.py');
        const env = { ...process.env, PYTHONPATH: path.join(__dirname, '..') };
        const proc = spawn(pythonExec, [scriptPath], { env, cwd: path.join(__dirname, '..') });
        let stdout = '';
        let stderr = '';
        proc.stdout.on('data', d => { stdout += d.toString(); });
        proc.stderr.on('data', d => { const s = d.toString(); stderr += s; console.error(s); });
        proc.on('close', (code) => {
            if (code === 0) {
                const files = getStep0Files();
                return res.json({ success: true, message: 'XLSX uploaded and processed', files, output: stdout });
            } else {
                return res.status(500).json({ success: false, error: `Parser exited with code ${code}`, stderr, stdout });
            }
        });
        proc.stdin.write(buf);
        proc.stdin.end();
    } catch (error) {
        console.error('Upload XLSX failed:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Express error handler to surface stack traces
// Keep at the end of routes
app.use((err, req, res, next) => {
    console.error('Express error handler:', err);
    if (res.headersSent) return next(err);
    res.status(500).json({ success: false, error: err.message, stack: err.stack });
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

function getStep1MovedCandidates() {
    const moved = readJsonFile('output/same_name_diff_address.json');
    if (!moved) return [];
    return moved;
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
            potentialMatches: completedInfo.potentialMatches,
            movedCandidates: getStep1MovedCandidates()
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

// Get moved-candidates (same name, different address)
app.get('/api/moved-candidates', (req, res) => {
    try {
        const moved = getStep1MovedCandidates();
        return res.json({ success: true, movedCandidates: moved, totalCount: moved.length });
    } catch (error) {
        console.error('Error getting moved candidates:', error);
        res.status(500).json({ success: false, error: error.message });
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

        // Incorporate moved-candidate decisions (not present in new_people file)
        const movedCandidates = readJsonFile('output/same_name_diff_address.json') || [];
        if (movedCandidates.length > 0) {
            movedCandidates.forEach(item => {
                const p = item.new_person || {};
                if (!p.person_id) return;
                const decision = decisions[p.person_id];
                if (!decision) return;
                if (decision.decision === 'new') {
                    // Add to enrichment list with link to existing
                    const enrichedRec = {
                        ...p,
                        match_status: 'verified_new_moved',
                        verification_decision: 'user_approved_moved_enrichment',
                        linked_existing: item.existing_person || null
                    };
                    updatedEnrichmentList.push(enrichedRec);
                } else if (decision.decision === 'existing') {
                    existingPeopleList.push({
                        ...p,
                        match_status: 'verified_existing_moved',
                        verification_decision: 'user_marked_as_existing_moved',
                        linked_existing: item.existing_person || null
                    });
                } else if (decision.decision === 'skip') {
                    // Do nothing for now; could track skipped moved if desired
                }
            });
        }
        
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

// Step 1: Update existing person address in SQL (from moved-candidate UI)
app.post('/api/step1/update-existing-address', express.json(), async (req, res) => {
    try {
        const body = req.body || {};
        if (!body.existing_id) {
            return res.status(400).json({ success: false, error: 'existing_id is required' });
        }
        const pythonExec = resolvePython();
        const scriptPath = path.join(__dirname, '..', 'scripts', 'update_existing_address.py');

        const env = {
            ...process.env,
            PYTHONPATH: path.join(__dirname, '..'),
            DB_HOST: process.env.DB_HOST || process.env.SQL_HOST || 'localhost',
            DB_PORT: process.env.DB_PORT || process.env.SQL_PORT || '3306',
            DB_NAME: process.env.DB_NAME || process.env.SQL_DATABASE || 'patent_data',
            DB_USER: process.env.DB_USER || process.env.SQL_USER || 'root',
            DB_PASSWORD: process.env.DB_PASSWORD || process.env.SQL_PASSWORD || 'password',
            DB_ENGINE: (process.env.DB_ENGINE || 'mysql').toLowerCase()
        };

        const proc = spawn(pythonExec, [scriptPath], { env, cwd: path.join(__dirname, '..') });
        let stdout = '';
        let stderr = '';
        proc.stdout.on('data', d => stdout += d.toString());
        proc.stderr.on('data', d => { const s = d.toString(); stderr += s; console.error('update-existing-address stderr:', s); });
        proc.on('close', (code) => {
            try {
                const out = stdout.trim() ? JSON.parse(stdout.trim()) : {};
                if (code === 0 && out.success) {
                    return res.json({ success: true });
                }
                return res.status(500).json({ success: false, error: out.error || `Exit code ${code}` });
            } catch (e) {
                return res.status(500).json({ success: false, error: `Invalid script output: ${e.message}` });
            }
        });
        proc.stdin.write(JSON.stringify(body));
        proc.stdin.end();
    } catch (error) {
        console.error('Update existing address failed:', error);
        res.status(500).json({ success: false, error: error.message });
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

// Step 2 (Express): Data Enrichment skipping prior failures
app.post('/api/step2/express', async (req, res) => {
    const stepId = 'step2';
    if (runningProcesses.has(stepId)) {
        return res.json({ success: false, error: 'Step 2 is already running' });
    }
    try {
        console.log('Starting Step 2: Data Enrichment [EXPRESS] (Async)');
        const args = ['--express'];
        runPythonScriptAsync('front-end/run_step2_wrapper.py', args, stepId)
            .then((result) => {
                console.log('Step 2 (express) completed successfully');
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
                console.error('Step 2 (express) failed:', error);
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: false,
                    error: error.error || error.message,
                    stderr: error.stderr,
                    stdout: error.stdout,
                    completedAt: new Date()
                });
            });
        res.json({ success: true, status: 'started', processing: true, message: 'Step 2 (express) started. Use /api/step2/status to check progress.' });
    } catch (error) {
        console.error('Step 2 (express) startup error:', error);
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
            movedCandidates: getFileStats('output/same_name_diff_address.json'),
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
    const movedCandidates = readJsonFile('output/same_name_diff_address.json');
    const enrichedData = readJsonFile('output/enriched_patents.json');
    const currentCycleEnriched = readJsonFile('output/current_cycle_enriched.json');
    const newEnrichedThisRun = readJsonFile('output/enriched_patents_new_this_run.json');

    status.counts = {
        newPeople: newPeople ? newPeople.length : 0,
        newPatents: newPatents ? newPatents.length : 0,
        // Show current cycle enriched (new + duplicates) if available; fallback to total enriched snapshot
        enrichedData: currentCycleEnriched ? currentCycleEnriched.length : (enrichedData ? enrichedData.length : 0),
        newEnrichments: newEnrichedThisRun ? newEnrichedThisRun.length : 0,
        movedCandidates: movedCandidates ? movedCandidates.length : 0
    };
    // Last run timestamps from file stats
    status.lastRun = {
        step0: status.step0.downloadResults.modified || null,
        step1: status.step1.integrationResults.modified || null,
        step2: status.step2.enrichmentResults.modified || null
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
// Utility: CSV escaping
function csvEscape(val) {
    if (val === null || val === undefined) return '';
    const s = String(val);
    if (/[",\n]/.test(s)) {
        return '"' + s.replace(/"/g, '""') + '"';
    }
    return s;
}

function writeCsv(res, filename, headers, rows) {
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.write(headers.join(',') + '\n');
    for (const row of rows) {
        res.write(headers.map(h => csvEscape(row[h])).join(',') + '\n');
    }
    res.end();
}

// Simplify dotted headers to last segment; ensure uniqueness with suffixes.
function simplifyHeadersAndRows(headers, rows) {
    const counts = new Map();
    const mapping = new Map();
    const display = [];
    for (const h of headers) {
        const base = h.split('.').pop();
        const n = (counts.get(base) || 0) + 1;
        counts.set(base, n);
        const name = n === 1 ? base : `${base}_${n}`;
        mapping.set(h, name);
        display.push(name);
    }
    const remappedRows = rows.map(r => {
        const o = {};
        for (const h of headers) {
            o[mapping.get(h)] = r[h] ?? '';
        }
        return o;
    });
    return { headers: display, rows: remappedRows };
}

function extractEnrichedRow(item) {
    const ed = item.enriched_data || {};
    const original = ed.original_data || ed.original_person || {};
    const pdl = ed.pdl_data || {};
    const email = Array.isArray(pdl.emails) && pdl.emails.length > 0
        ? (pdl.emails[0].address || pdl.emails[0])
        : '';
    const company = pdl.job_company_name || (pdl.job_company && (pdl.job_company.name || pdl.job_company)) || '';
    return {
        first_name: original.first_name || item.first_name || '',
        last_name: original.last_name || item.last_name || '',
        city: original.city || item.city || '',
        state: original.state || item.state || '',
        country: original.country || item.country || 'US',
        patent_number: original.patent_number || item.patent_number || '',
        patent_title: item.patent_title || (ed.original_person && ed.original_person.patent_title) || '',
        person_type: original.person_type || item.person_type || 'inventor',
        match_score: item.match_score || (ed.enrichment_result && ed.enrichment_result.match_score) || '',
        email: email || '',
        linkedin_url: pdl.linkedin_url || '',
        job_title: pdl.job_title || '',
        company: company || ''
    };
}

function extractNewPersonRow(item) {
    // Flatten a row from new_people_for_enrichment.json for CSV export
    return {
        first_name: item.first_name || '',
        last_name: item.last_name || '',
        city: item.city || '',
        state: item.state || '',
        country: item.country || 'US',
        patent_number: item.patent_number || '',
        patent_title: item.patent_title || '',
        person_type: item.person_type || 'inventor',
        match_score: item.match_score || '',
        match_status: item.match_status || '',
        associated_patent_count: item.associated_patent_count || (Array.isArray(item.associated_patents) ? item.associated_patents.length : ''),
    };
}

// Generic flattener: dot-joins nested object keys, stringifies arrays/objects
function flattenObject(obj, prefix = '', out = {}) {
    if (obj === null || obj === undefined) {
        if (prefix) out[prefix] = '';
        return out;
    }
    if (typeof obj === 'boolean') {
        if (prefix) out[prefix] = '';
        return out;
    }
    if (Array.isArray(obj)) {
        out[prefix] = JSON.stringify(obj);
        return out;
    }
    if (typeof obj === 'object') {
        const keys = Object.keys(obj);
        if (keys.length === 0 && prefix) {
            out[prefix] = '';
            return out;
        }
        for (const k of keys) {
            const next = prefix ? `${prefix}.${k}` : k;
            flattenObject(obj[k], next, out);
        }
        return out;
    }
    // Primitive
    const s = String(obj);
    out[prefix] = (/^(nan|null|none)$/i).test(s.trim()) ? '' : s;
    return out;
}

function buildFlatRowsFromEnriched(data) {
    return data.map((item) => flattenObject(item));
}

// Export current run enrichments from output/enriched_patents.json
app.get('/api/export/current-enrichments', (req, res) => {
    try {
        // Prefer current cycle combined file (new + matched existing)
        let data = readJsonFile('output/current_cycle_enriched.json');
        if (!data || !Array.isArray(data) || data.length === 0) {
            data = readJsonFile('output/enriched_patents.json');
        }
        if (!data || !Array.isArray(data) || data.length === 0) {
            return res.status(404).json({ error: 'No current enriched data found. Run Step 2 first.' });
        }
        // Flatten each record and build dynamic headers
        const rows = buildFlatRowsFromEnriched(data);
        const headerSet = new Set();
        for (const r of rows) {
            for (const k of Object.keys(r)) headerSet.add(k);
        }
        const headers = Array.from(headerSet).sort();
        const simplified = simplifyHeadersAndRows(headers, rows);
        writeCsv(res, 'current_enrichments.csv', simplified.headers, simplified.rows);
    } catch (e) {
        console.error('Export current enrichments failed:', e);
        res.status(500).json({ error: e.message });
    }
});

// Export only new enrichments from this Step 2 run
app.get('/api/export/new-enrichments', (req, res) => {
    try {
        const data = readJsonFile('output/enriched_patents_new_this_run.json');
        if (!data || !Array.isArray(data) || data.length === 0) {
            return res.status(404).json({ error: 'No new enrichments in this run. Run Step 2 with new people.' });
        }
        const rows = buildFlatRowsFromEnriched(data);
        const headerSet = new Set();
        for (const r of rows) {
            for (const k of Object.keys(r)) headerSet.add(k);
        }
        const headers = Array.from(headerSet).sort();
        const simplified = simplifyHeadersAndRows(headers, rows);
        writeCsv(res, 'new_enrichments.csv', simplified.headers, simplified.rows);
    } catch (e) {
        console.error('Export new enrichments failed:', e);
        res.status(500).json({ error: e.message });
    }
});

// Helper: build formatted row per requested Access-style schema
function sanitizeForCsv(val) {
    if (val === null || val === undefined) return '';
    if (typeof val === 'boolean') return '';
    const s = String(val).trim();
    if (/^(nan|null|none|true|false)$/i.test(s)) {
        // PDL may return presence booleans or 'nan'-like strings; treat as empty
        return '';
    }
    return s;
}

function firstNonEmpty(...vals) {
    for (const v of vals) {
        const s = sanitizeForCsv(v);
        if (s !== '') return s;
    }
    return '';
}
const FORMATTED_HEADERS = [
  'issue_id','new_issue_rec_num','inventor_id','patent_no','title','issue_date',
  'mail_to_assignee','mail_to_name','mail_to_add1','mail_to_add2','mail_to_add3',
  'mail_to_city','mail_to_state','mail_to_zip','mail_to_country','mail_to_send_key',
  'inventor_first','inventor_last','mod_user','bar_code','inventor_contact'
];

function buildFormattedRow(item) {
  // Support both local JSON structure and SQL-style enrichment_data structure
  // edLocal: { original_data/original_person, pdl_data, api_method }
  // edSql: { original_person, enrichment_result: { enriched_data: { original_data, pdl_data } } }
  const hasSqlRoot = item && item.enrichment_result && item.enrichment_result.enriched_data;
  const ed = hasSqlRoot ? item.enrichment_result.enriched_data : (item.enriched_data || {});
  const original = ed.original_data || ed.original_person || item.original_person || {};
  const pdl = ed.pdl_data || (hasSqlRoot ? (item.enrichment_result.enriched_data && item.enrichment_result.enriched_data.pdl_data) : {}) || {};
  // choose an email if present
  let email = '';
  if (Array.isArray(pdl.emails) && pdl.emails.length > 0) {
    const e0 = pdl.emails[0];
    email = (typeof e0 === 'string') ? e0 : (e0 && (e0.address || e0.email || ''));
  }
  const street = firstNonEmpty(pdl.job_company_location_street_address, pdl.location_street_address);
  const line2 = firstNonEmpty(pdl.job_company_location_address_line_2, pdl.location_address_line_2);
  const city = firstNonEmpty(pdl.job_company_location_locality, pdl.location_locality, original.city, item.city);
  const state = firstNonEmpty(pdl.job_company_location_region, pdl.location_region, original.state, item.state);
  const zip = firstNonEmpty(pdl.job_company_location_postal_code, pdl.location_postal_code);
  const country = firstNonEmpty(pdl.job_company_location_country, pdl.location_country, original.country, item.country);
  const first = firstNonEmpty(original.first_name, item.first_name, (item.enrichment_result && item.enrichment_result.original_name && item.enrichment_result.original_name.split(' ')[0]));
  const last = firstNonEmpty(original.last_name, item.last_name, (item.enrichment_result && item.enrichment_result.original_name && item.enrichment_result.original_name.split(' ').slice(1).join(' ')));
  const full = (first || last) ? `${first} ${last}`.trim() : '';
  const formatted = {
    issue_id: '',
    new_issue_rec_num: '',
    inventor_id: '',
    patent_no: firstNonEmpty(original.patent_number, item.patent_number, (item.enrichment_result && item.enrichment_result.patent_number)),
    title: firstNonEmpty(item.patent_title, original.patent_title, (item.enrichment_result && item.enrichment_result.patent_title)),
    issue_date: '',
    mail_to_assignee: '',
    mail_to_name: sanitizeForCsv(full),
    mail_to_add1: street,
    mail_to_add2: line2,
    mail_to_add3: '',
    mail_to_city: city,
    mail_to_state: state,
    mail_to_zip: zip,
    mail_to_country: country,
    mail_to_send_key: '',
    inventor_first: first,
    inventor_last: last,
    mod_user: '',
    bar_code: '',
    inventor_contact: email
  };
  // Ensure all keys exist
  for (const h of FORMATTED_HEADERS) {
    if (!(h in formatted)) formatted[h] = '';
  }
  return formatted;
}

function writeFormattedCsv(res, filename, data) {
  const rows = data.map(buildFormattedRow);
  writeCsv(res, filename, FORMATTED_HEADERS, rows);
}

// Formatted exports
app.get('/api/export/current-enrichments-formatted', (req, res) => {
  try {
    let data = readJsonFile('output/current_cycle_enriched.json');
    if (!data || !Array.isArray(data) || data.length === 0) {
      data = readJsonFile('output/enriched_patents.json');
    }
    if (!data || !Array.isArray(data) || data.length === 0) {
      return res.status(404).json({ error: 'No current enriched data found. Run Step 2 first.' });
    }
    writeFormattedCsv(res, 'current_enrichments_formatted.csv', data);
  } catch (e) {
    console.error('Export current formatted failed:', e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/export/new-enrichments-formatted', (req, res) => {
  try {
    const data = readJsonFile('output/enriched_patents_new_this_run.json');
    if (!data || !Array.isArray(data) || data.length === 0) {
      return res.status(404).json({ error: 'No new enrichments in this run. Run Step 2 with new people.' });
    }
    writeFormattedCsv(res, 'new_enrichments_formatted.csv', data);
  } catch (e) {
    console.error('Export new formatted failed:', e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/export/all-enrichments-formatted', (req, res) => {
  try {
    // Use local snapshot for "all" formatted export
    const data = readJsonFile('output/enriched_patents.json');
    if (!data || !Array.isArray(data) || data.length === 0) {
      return res.status(404).json({ error: 'No enriched data snapshot found.' });
    }
    writeFormattedCsv(res, 'all_enrichments_formatted.csv', data);
  } catch (e) {
    console.error('Export all formatted failed:', e);
    res.status(500).json({ error: e.message });
  }
});

// Step 2 live progress endpoint
app.get('/api/step2/progress', (req, res) => {
    try {
        const p = readJsonFile('output/step2_progress.json') || {};
        const running = runningProcesses.has('step2');
        res.json({ running, ...p });
    } catch (e) {
        res.json({ running: runningProcesses.has('step2') });
    }
});

// Export all enrichments from SQL via Python helper
app.get('/api/export/all-enrichments', (req, res) => {
    try {
        const pythonExec = resolvePython();
        const scriptPath = path.join(__dirname, '..', 'scripts', 'export_all_enrichments.py');
        // Set headers before piping
        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', 'attachment; filename="all_enrichments.csv"');
        const env = { ...process.env, PYTHONPATH: path.join(__dirname, '..') };
        const proc = spawn(pythonExec, [scriptPath], { env, cwd: path.join(__dirname, '..') });
        proc.stdout.pipe(res);
        proc.stderr.on('data', d => console.error(d.toString()));
        proc.on('error', (err) => {
            console.error('Export all enrichments error:', err);
            if (!res.headersSent) res.status(500).end('Failed to start export process');
        });
    } catch (e) {
        console.error('Export all enrichments failed:', e);
        res.status(500).json({ error: e.message });
    }
});

// Export Step 1 "New People" list as CSV (pre-enrichment)
app.get('/api/export/new-people', (req, res) => {
    try {
        const data = readJsonFile('output/new_people_for_enrichment.json');
        if (!data || !Array.isArray(data) || data.length === 0) {
            return res.status(404).json({ error: 'No new people list found. Run Step 1 first.' });
        }
        const headers = ['first_name','last_name','city','state','country','patent_number','patent_title','person_type','match_score','match_status','associated_patent_count'];
        const rows = data.map(extractNewPersonRow);
        writeCsv(res, 'new_people_step1.csv', headers, rows);
    } catch (e) {
        console.error('Export new people failed:', e);
        res.status(500).json({ error: e.message });
    }
});
