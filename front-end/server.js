const express = require('express');
const { spawn, spawnSync } = require('child_process');
const path = require('path');
const fs = require('fs');

// Load environment variables from the frontend .env file
require('dotenv').config({ path: path.join(__dirname, '.env') });

// Ensure SQL_* values from the frontend config are also available as DB_* for Python
[
    ['SQL_HOST', 'DB_HOST'],
    ['SQL_PORT', 'DB_PORT'],
    ['SQL_DATABASE', 'DB_NAME'],
    ['SQL_USER', 'DB_USER'],
    ['SQL_PASSWORD', 'DB_PASSWORD']
].forEach(([sqlKey, dbKey]) => {
    if (!process.env[dbKey] && process.env[sqlKey]) {
        process.env[dbKey] = process.env[sqlKey];
    }
});
if (!process.env.DB_ENGINE && (process.env.SQL_HOST || process.env.DB_HOST)) {
    process.env.DB_ENGINE = 'mysql';
}

const app = express();
const PORT = process.env.PORT || 3000;
const EXPORT_DEBUG = String(process.env.EXPORT_DEBUG || 'false').toLowerCase() === 'true';

// Global state for tracking running processes
const runningProcesses = new Map();

const STEP0_CHUNK_DIR = path.join(__dirname, '..', 'uploads', 'step0_chunks');
try {
    if (!fs.existsSync(STEP0_CHUNK_DIR)) {
        fs.mkdirSync(STEP0_CHUNK_DIR, { recursive: true });
    }
} catch (err) {
    console.warn('Could not create chunk directory:', err.message);
}

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname)));
app.use('/output', express.static(path.join(__dirname, '..', 'output')));
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

app.get('/dev', (req, res) => {
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
        const sqlHost = process.env.SQL_HOST || process.env.DB_HOST || 'localhost';
        const sqlUser = process.env.SQL_USER || process.env.DB_USER || 'root';
        const sqlPassword = process.env.SQL_PASSWORD || process.env.DB_PASSWORD || 'password';
        const sqlDatabase = process.env.SQL_DATABASE || process.env.DB_NAME || 'patent_data';
        const sqlPort = process.env.SQL_PORT || process.env.DB_PORT || '3306';

        const env = {
            ...process.env,
            PYTHONPATH: path.join(__dirname, '..'),

            // Pass SQL credentials explicitly for Python scripts
            SQL_HOST: sqlHost,
            SQL_USER: sqlUser,
            SQL_PASSWORD: sqlPassword,
            SQL_DATABASE: sqlDatabase,
            SQL_PORT: sqlPort,

            // Also provide DB_* aliases used by Python modules
            DB_HOST: sqlHost,
            DB_PORT: sqlPort,
            DB_NAME: sqlDatabase,
            DB_USER: sqlUser,
            DB_PASSWORD: sqlPassword,
            DB_ENGINE: process.env.DB_ENGINE || 'mysql',

            // API keys
            PEOPLEDATALABS_API_KEY: process.env.PEOPLEDATALABS_API_KEY || 'YOUR_PDL_API_KEY',
            PATENTSVIEW_API_KEY: process.env.PATENTSVIEW_API_KEY || 'oq371zFI.BjeAbayJsdHdvEgbei0vskz5bTK3KM1S',
            MAX_ENRICHMENT_COST: process.env.MAX_ENRICHMENT_COST || '1000',
            DAYS_BACK: process.env.DAYS_BACK || '7',
            MAX_RESULTS: process.env.MAX_RESULTS || '1000'
        };
        
        // Force unbuffered Python stdout for real-time logs (-u) and env var
        const spawnArgs = ['-u', scriptPath, ...args];
        const python = spawn(pythonExec, spawnArgs, {
            cwd: path.join(__dirname, '..'),
            env: { ...env, PYTHONUNBUFFERED: '1', PYTHONIOENCODING: 'utf-8' }
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

// Helper: write cycle-start marker to visually reset step states
function markCycleReset(reason = 'step0') {
    try {
        const outDir = path.join(__dirname, '..', 'output');
        if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
        const payload = { reason, timestamp: new Date().toISOString() };
        fs.writeFileSync(path.join(outDir, 'cycle_start.json'), JSON.stringify(payload, null, 2));
    } catch (e) {
        console.warn('Failed to write cycle_start.json:', e.message);
    }
}

function readCycleStart() {
    try {
        const p = path.join(__dirname, '..', 'output', 'cycle_start.json');
        if (!fs.existsSync(p)) return null;
        const data = JSON.parse(fs.readFileSync(p, 'utf8'));
        const ts = data && data.timestamp ? new Date(data.timestamp) : null;
        return ts && !isNaN(ts.getTime()) ? ts : null;
    } catch (_) { return null; }
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

function truncateText(text, maxLength = 4000) {
    if (typeof text !== 'string') {
        return text;
    }
    if (text.length <= maxLength) {
        return text;
    }
    const trimmed = text.slice(0, maxLength);
    const omitted = text.length - maxLength;
    return `${trimmed}\n...[truncated ${omitted} chars]`;
}

function stepStatusPath(stepId) {
    return path.join(__dirname, '..', 'output', `${stepId}_status.json`);
}

function readStepStatus(stepId) {
    try {
        const fullPath = stepStatusPath(stepId);
        if (fs.existsSync(fullPath)) {
            return JSON.parse(fs.readFileSync(fullPath, 'utf8'));
        }
    } catch (error) {
        console.warn(`Could not read status for ${stepId}:`, error.message);
    }
    return null;
}

function writeStepStatus(stepId, payload) {
    try {
        const fullPath = stepStatusPath(stepId);
        const dir = path.dirname(fullPath);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }
        const enrichedPayload = {
            stepId,
            updatedAt: new Date().toISOString(),
            ...payload
        };
        fs.writeFileSync(fullPath, JSON.stringify(enrichedPayload, null, 2));
    } catch (error) {
        console.warn(`Could not persist status for ${stepId}:`, error.message);
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

function totalBytes(buffer) {
    if (Buffer.isBuffer(buffer)) {
        return buffer.length;
    }
    if (typeof buffer === 'string') {
        return Buffer.byteLength(buffer, 'utf8');
    }
    return 0;
}

async function processStep0XmlBuffer(buffer, { modeLabel = 'upload-xml', cycleReason = 'step0_xml_upload' } = {}) {
    const stepId = 'step0';
    return new Promise((resolve, reject) => {
        try {
            markCycleReset(cycleReason);
            try {
                const outDir = path.join(__dirname, '..', 'output');
                if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
                fs.writeFileSync(path.join(outDir, 'enriched_patents.json'), '[]');
                const resetMeta = { reset: true, reset_by: modeLabel, timestamp: new Date().toISOString() };
                fs.writeFileSync(path.join(outDir, 'enrichment_results.json'), JSON.stringify(resetMeta, null, 2));
                const csvPath = path.join(outDir, 'enriched_patents.csv');
                if (fs.existsSync(csvPath)) fs.unlinkSync(csvPath);
            } catch (e) {
                console.warn('Could not reset enrichment output files on XML processing:', e.message);
            }

            const pythonExec = resolvePython();
            const scriptPath = path.join(__dirname, '..', 'scripts', 'process_uploaded_xml.py');
            const env = { ...process.env, PYTHONPATH: path.join(__dirname, '..') };
            const proc = spawn(pythonExec, [scriptPath], { env, cwd: path.join(__dirname, '..') });
            let stdout = '';
            let stderr = '';

            console.log(`[Step0] XML processor started (${modeLabel})`);

            proc.stdout.on('data', d => {
                const out = d.toString();
                stdout += out;
                console.log(out);
            });
            proc.stderr.on('data', d => {
                const err = d.toString();
                stderr += err;
                console.error(err);
            });
            proc.on('close', (code) => {
                if (code === 0) {
                    const files = getStep0Files();
                    try {
                        fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step0_output.txt'), stdout || '');
                    } catch (e) { /* ignore */ }
                    const completedAt = new Date();
                    writeStepStatus(stepId, {
                        success: true,
                        completedAt: completedAt.toISOString(),
                        files,
                        mode: modeLabel,
                        outputSummary: truncateText(stdout || '', 4000),
                        stdoutSnippet: truncateText(stdout || '', 2000)
                    });
                    console.log(`[Step0] XML processing complete (${modeLabel})`);
                    resolve({
                        success: true,
                        message: 'XML uploaded and processed',
                        files,
                        output: stdout
                    });
                } else {
                    const msgParts = [
                        `Parser exited with code ${code}`,
                        stderr,
                        stdout
                    ].filter(Boolean);
                    const combined = msgParts.join('\n\n');
                    try {
                        fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step0_output.txt'), combined);
                    } catch (e) { /* ignore */ }
                    const completedAt = new Date();
                    writeStepStatus(stepId, {
                        success: false,
                        completedAt: completedAt.toISOString(),
                        error: `Parser exited with code ${code}`,
                        stderr: truncateText(stderr || '', 4000),
                        stdoutSnippet: truncateText(stdout || '', 2000),
                        mode: modeLabel
                    });
                    console.error(`[Step0] XML processing failed (${modeLabel}) with code ${code}`);
                    reject({ success: false, error: `Parser exited with code ${code}`, stderr, stdout });
                }
            });

            proc.stdin.end(buffer);
        } catch (err) {
            reject(err);
        }
    });
}

function formatZabaProgress(progress) {
    if (!progress || typeof progress !== 'object') return null;

    if (progress.message && typeof progress.message === 'string' && progress.message.trim().length > 0) {
        return progress.message.trim();
    }

    const saved = Number(progress.newly_enriched || progress.saved || 0);
    const total = Number(progress.total_to_enrich || progress.total || 0);
    const processed = Number(progress.processed || 0);
    const failed = Number(progress.failed || 0);

    let base = `${saved}/${total} - people enriched`;
    if (!Number.isFinite(total) || total <= 0) {
        base = `${saved}/0 - people enriched`;
    }

    if (processed > 0 || failed > 0) {
        return `${base} (processed:${processed}, failed:${failed})`;
    }

    return base;
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
        // Mark start of a new cycle to visually reset downstream steps (1 & 2)
        markCycleReset('step0_download');
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
        
        console.log('[Step0] Download request received', {
            mode,
            startDate,
            endDate,
            daysBack,
            maxResults
        });

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
                console.log('[Step0] Download process completed successfully');
                const completedAt = new Date();
                // Store completion result for status endpoint
                try {
                    fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step0_output.txt'), (result && result.output) ? String(result.output) : '');
                } catch (e) { console.warn('Could not persist last_step0_output.txt:', e.message); }
                const files = getStep0Files();
                writeStepStatus(stepId, {
                    success: true,
                    completedAt: completedAt.toISOString(),
                    files,
                    outputSummary: truncateText((result && result.output) ? String(result.output) : '', 4000),
                    stdoutSnippet: truncateText((result && result.output) ? String(result.output) : '', 2000)
                });
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: true,
                    output: result.output,
                    completedAt,
                    files
                });
            })
            .catch((error) => {
                console.error('[Step0] Download process failed:', error);
                
                // Store error result for status endpoint
                try {
                    const msg = [error.error || error.message || 'Step 0 failed', error.stderr || '', error.stdout || ''].filter(Boolean).join('\n\n');
                    fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step0_output.txt'), msg);
                } catch (e) { /* ignore */ }
                const completedAt = new Date();
                const files = getStep0Files();
                writeStepStatus(stepId, {
                    success: false,
                    completedAt: completedAt.toISOString(),
                    error: error.error || error.message || 'Step 0 failed',
                    stderr: truncateText(error.stderr || '', 4000),
                    stdoutSnippet: truncateText(error.stdout || '', 2000),
                    files
                });
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: false,
                    error: error.error || error.message,
                    stderr: error.stderr,
                    stdout: error.stdout,
                    completedAt,
                    files
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
        console.error('[Step0] Startup error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Alternate Step 0: Run legacy extractor but write standard outputs
app.post('/api/step0/extract', async (req, res) => {
    const stepId = 'step0';
    if (runningProcesses.has(stepId)) {
        return res.json({ success: false, error: 'Step 0 is already running' });
    }
    try {
        console.log('Starting Step 0 (Alternate): Extract patents (Async)');
        // Mark start of a new cycle to visually reset downstream steps (1 & 2)
        markCycleReset('step0_extract');
        // Reset current-run enrichment outputs
        try {
            const outDir = path.join(__dirname, '..', 'output');
            if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
            fs.writeFileSync(path.join(outDir, 'enriched_patents.json'), '[]');
            const resetMeta = { reset: true, reset_by: 'step0_alternate', timestamp: new Date().toISOString() };
            fs.writeFileSync(path.join(outDir, 'enrichment_results.json'), JSON.stringify(resetMeta, null, 2));
            const csvPath = path.join(outDir, 'enriched_patents.csv');
            if (fs.existsSync(csvPath)) fs.unlinkSync(csvPath);
        } catch (e) { console.warn('Could not reset enrichment output files:', e.message); }

        const { daysBack = 7, maxResults = 1000 } = req.body || {};
        console.log('[Step0] Alternate extractor request', { daysBack, maxResults });
        const args = ['--days-back', String(daysBack), '--max-results', String(maxResults)];
        runPythonScriptAsync('front-end/run_step0_extract_wrapper.py', args, stepId)
            .then((result) => {
                const completedAt = new Date();
                const files = getStep0Files();
                try {
                    fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step0_output.txt'), (result && result.output) ? String(result.output) : '');
                } catch (e) { /* ignore */ }
                console.log('[Step0] Alternate extractor completed successfully');
                writeStepStatus(stepId, {
                    success: true,
                    completedAt: completedAt.toISOString(),
                    files,
                    outputSummary: truncateText((result && result.output) ? String(result.output) : '', 4000),
                    stdoutSnippet: truncateText((result && result.output) ? String(result.output) : '', 2000)
                });
                runningProcesses.set(stepId + '_completed', { completed: true, success: true, output: result.output, files, completedAt });
            })
            .catch((error) => {
                try {
                    const msg = [error.error || error.message || 'Step 0 failed', error.stderr || '', error.stdout || ''].filter(Boolean).join('\n\n');
                    fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step0_output.txt'), msg);
                } catch (e) { /* ignore */ }
                const completedAt = new Date();
                const files = getStep0Files();
                console.error('[Step0] Alternate extractor failed:', error);
                writeStepStatus(stepId, {
                    success: false,
                    completedAt: completedAt.toISOString(),
                    error: error.error || error.message || 'Step 0 failed',
                    stderr: truncateText(error.stderr || '', 4000),
                    stdoutSnippet: truncateText(error.stdout || '', 2000),
                    files
                });
                runningProcesses.set(stepId + '_completed', { completed: true, success: false, error: error.error || error.message, stderr: error.stderr, stdout: error.stdout, completedAt, files });
            });
        return res.json({ success: true, status: 'started', processing: true, message: 'Alternate Step 0 started. Use /api/step0/status to check progress.' });
    } catch (error) {
        console.error('Alternate Step 0 startup error:', error);
        return res.status(500).json({ success: false, error: error.message });
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

        // Mark start of a new cycle to visually reset downstream steps (1 & 2)
        markCycleReset('step0_csv_upload');
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

        console.log('[Step0] CSV upload body length (chars):', req.body.length);
        // Hand off parsing to Python for robust CSV handling
        const pythonExec = resolvePython();
        const scriptPath = path.join(__dirname, '..', 'scripts', 'process_uploaded_csv.py');
        const env = { ...process.env, PYTHONPATH: path.join(__dirname, '..') };
        const proc = spawn(pythonExec, [scriptPath], { env, cwd: path.join(__dirname, '..') });
        let stdout = '';
        let stderr = '';
        console.log('[Step0] CSV parser process spawned');
        proc.stdout.on('data', d => { stdout += d.toString(); });
        proc.stderr.on('data', d => { const s = d.toString(); stderr += s; console.error(s); });
        proc.on('close', (code) => {
            if (code === 0) {
                const files = getStep0Files();
                try {
                    fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step0_output.txt'), stdout || '');
                } catch (e) { /* ignore */ }
                console.log('[Step0] CSV upload processed successfully');
                const completedAt = new Date();
                writeStepStatus('step0', {
                    success: true,
                    completedAt: completedAt.toISOString(),
                    files,
                    mode: 'upload-csv',
                    outputSummary: truncateText(stdout || '', 4000),
                    stdoutSnippet: truncateText(stdout || '', 2000)
                });
                return res.json({ success: true, message: 'CSV uploaded and processed', files, output: stdout });
            } else {
                console.error('[Step0] CSV parser exited with code', code);
                try {
                    const msg = [
                        `Parser exited with code ${code}`,
                        stderr,
                        stdout
                    ].filter(Boolean).join('\n\n');
                    fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step0_output.txt'), msg);
                } catch (e) { /* ignore */ }
                const completedAt = new Date();
                writeStepStatus('step0', {
                    success: false,
                    completedAt: completedAt.toISOString(),
                    error: `Parser exited with code ${code}`,
                    stderr: truncateText(stderr || '', 4000),
                    stdoutSnippet: truncateText(stdout || '', 2000),
                    mode: 'upload-csv'
                });
                return res.status(500).json({ success: false, error: `Parser exited with code ${code}`, stderr, stdout });
            }
        });
        // Write CSV body to stdin (ensure trailing newline so last row is read)
        try {
            let body = (typeof req.body === 'string') ? req.body : String(req.body || '');
            if (!body.endsWith('\n')) body += '\n';
            proc.stdin.write(body);
        } catch (e) {
            console.error('Error writing CSV body to parser:', e);
        }
        proc.stdin.end();
    } catch (error) {
        console.error('[Step0] Upload CSV failed:', error);
        try {
            fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step0_output.txt'), `CSV upload failed: ${error.message}`);
        } catch (e) { /* ignore */ }
        const completedAt = new Date();
        writeStepStatus('step0', {
            success: false,
            completedAt: completedAt.toISOString(),
            error: error.message,
            mode: 'upload-csv'
        });
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

        console.log('[Step0] XLSX upload body size (bytes):', buf.length);
        const pythonExec = resolvePython();
        const scriptPath = path.join(__dirname, '..', 'scripts', 'process_uploaded_xlsx.py');
        const env = { ...process.env, PYTHONPATH: path.join(__dirname, '..') };
        const proc = spawn(pythonExec, [scriptPath], { env, cwd: path.join(__dirname, '..') });
        let stdout = '';
        let stderr = '';
        console.log('[Step0] XLSX parser process spawned');
        proc.stdout.on('data', d => { stdout += d.toString(); });
        proc.stderr.on('data', d => { const s = d.toString(); stderr += s; console.error(s); });
        proc.on('close', (code) => {
            if (code === 0) {
                const files = getStep0Files();
                try { fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step0_output.txt'), stdout || ''); } catch (e) { /* ignore */ }
                console.log('[Step0] XLSX upload processed successfully');
                const completedAt = new Date();
                writeStepStatus('step0', {
                    success: true,
                    completedAt: completedAt.toISOString(),
                    files,
                    mode: 'upload-xlsx',
                    outputSummary: truncateText(stdout || '', 4000),
                    stdoutSnippet: truncateText(stdout || '', 2000)
                });
                return res.json({ success: true, message: 'XLSX uploaded and processed', files, output: stdout });
            } else {
                console.error('[Step0] XLSX parser exited with code', code);
                try {
                    const msg = [
                        `Parser exited with code ${code}`,
                        stderr,
                        stdout
                    ].filter(Boolean).join('\n\n');
                    fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step0_output.txt'), msg);
                } catch (e) { /* ignore */ }
                const completedAt = new Date();
                writeStepStatus('step0', {
                    success: false,
                    completedAt: completedAt.toISOString(),
                    error: `Parser exited with code ${code}`,
                    stderr: truncateText(stderr || '', 4000),
                    stdoutSnippet: truncateText(stdout || '', 2000),
                    mode: 'upload-xlsx'
                });
                return res.status(500).json({ success: false, error: `Parser exited with code ${code}`, stderr, stdout });
            }
        });
        proc.stdin.write(buf);
        proc.stdin.end();
    } catch (error) {
        console.error('[Step0] Upload XLSX failed:', error);
        try {
            fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step0_output.txt'), `XLSX upload failed: ${error.message}`);
        } catch (e) { /* ignore */ }
        const completedAt = new Date();
        writeStepStatus('step0', {
            success: false,
            completedAt: completedAt.toISOString(),
            error: error.message,
            mode: 'upload-xlsx'
        });
        res.status(500).json({ success: false, error: error.message });
    }
});

// Step 0: Upload XML (alternate direct ingest)
app.post('/api/step0/upload-xml', express.raw({ type: ['application/xml', 'text/xml', 'application/octet-stream'], limit: '500mb' }), async (req, res) => {
    try {
        console.log('XML upload hit. content-type=', req.headers['content-type'], 'length=', req.headers['content-length']);
        const buf = req.body;
        if (!buf || !buf.length) {
            return res.status(400).json({ success: false, error: 'No XML data received' });
        }
        console.log('[Step0] XML upload body size (bytes):', totalBytes(buf));
        const result = await processStep0XmlBuffer(buf, { modeLabel: 'upload-xml', cycleReason: 'step0_xml_upload' });
        return res.json(result);
    } catch (error) {
        console.error('[Step0] Upload XML failed:', error);
        if (error && error.success === false) {
            return res.status(500).json(error);
        }
        try {
            fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step0_output.txt'), `XML upload failed: ${error.message}`);
        } catch (e) { /* ignore */ }
        const completedAt = new Date();
        writeStepStatus('step0', {
            success: false,
            completedAt: completedAt.toISOString(),
            error: error.message,
            mode: 'upload-xml'
        });
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/step0/upload-xml-chunk', express.raw({ type: ['application/octet-stream', 'application/xml', 'text/plain'], limit: '8mb' }), async (req, res) => {
    try {
        const uploadId = (req.headers['x-upload-id'] || '').toString().trim();
        const chunkIndex = Number.parseInt(req.headers['x-chunk-index'], 10);
        const chunkCount = Number.parseInt(req.headers['x-chunk-count'], 10);
        const originalName = (req.headers['x-original-filename'] || 'unknown.xml').toString();

        if (!uploadId) {
            return res.status(400).json({ success: false, error: 'Missing upload identifier' });
        }
        if (!Number.isFinite(chunkIndex) || chunkIndex < 0) {
            return res.status(400).json({ success: false, error: 'Invalid chunk index' });
        }
        if (!Number.isFinite(chunkCount) || chunkCount <= 0) {
            return res.status(400).json({ success: false, error: 'Invalid chunk count' });
        }

        const chunkBuffer = req.body;
        if (!chunkBuffer || !chunkBuffer.length) {
            return res.status(400).json({ success: false, error: 'Empty chunk received' });
        }

        const tempPath = path.join(STEP0_CHUNK_DIR, `${uploadId}.xml.part`);

        if (chunkIndex === 0 && fs.existsSync(tempPath)) {
            fs.unlinkSync(tempPath);
        }

        fs.appendFileSync(tempPath, chunkBuffer);
        console.log(`[Step0] Received XML chunk ${chunkIndex + 1}/${chunkCount} for ${uploadId} (${totalBytes(chunkBuffer)} bytes, file ${originalName})`);

        const progressPercent = Math.min(95, Math.round(((chunkIndex + 1) / chunkCount) * 100));

        if (chunkIndex + 1 < chunkCount) {
            return res.json({ success: true, chunkReceived: true, index: chunkIndex, progress: progressPercent });
        }

        let result;
        try {
            const fullBuffer = fs.readFileSync(tempPath);
            console.log(`[Step0] Final chunk received for ${uploadId}, total size ${totalBytes(fullBuffer)} bytes. Processing...`);
            result = await processStep0XmlBuffer(fullBuffer, { modeLabel: 'upload-xml-chunked', cycleReason: 'step0_xml_upload_chunked' });
        } finally {
            try {
                if (fs.existsSync(tempPath)) {
                    fs.unlinkSync(tempPath);
                }
            } catch (cleanupErr) {
                console.warn(`[Step0] Could not remove temp chunk file ${tempPath}:`, cleanupErr.message);
            }
        }

        return res.json(result);
    } catch (error) {
        console.error('[Step0] Chunked XML upload failed:', error);
        try {
            const uploadId = (req.headers['x-upload-id'] || '').toString().trim();
            if (uploadId) {
                const tempPath = path.join(STEP0_CHUNK_DIR, `${uploadId}.xml.part`);
                if (fs.existsSync(tempPath)) {
                    fs.unlinkSync(tempPath);
                }
            }
        } catch (cleanupErr) {
            console.warn('[Step0] Failed to clean up chunk temp file:', cleanupErr.message);
        }
        if (error && error.success === false) {
            return res.status(500).json(error);
        }
        res.status(500).json({ success: false, error: error.message });
    }
});

// Express error handler to surface stack traces
// Keep at the end of routes
app.use((err, req, res, next) => {
    console.error('Express error handler:', err);
    if (err && err.type === 'entity.too.large') {
        console.error('[Step0] Request entity too large', {
            path: req.path,
            contentLength: req.headers['content-length'] || 'unknown'
        });
        return res.status(413).json({ success: false, error: 'Payload too large for current server configuration.' });
    }
    if (res.headersSent) return next(err);
    res.status(500).json({ success: false, error: err.message, stack: err.stack });
});

// Helper functions for Step 0 completion data
function getStep0Files() {
    const downloadedPatents = readJsonFile('output/downloaded_patents.json');
    const progressSnapshot = readJsonFile('output/step0_download_progress.json');

    return {
        downloadedPatents: {
            count: downloadedPatents ? downloadedPatents.length : 0,
            stats: getFileStats('output/downloaded_patents.json')
        },
        downloadResults: {
            stats: getFileStats('output/download_results.json')
        },
        progressLog: {
            stats: getFileStats('output/step0_download_progress.log')
        },
        progressSnapshot: {
            stats: getFileStats('output/step0_download_progress.json'),
            stage: progressSnapshot && progressSnapshot.stage,
            details: progressSnapshot && progressSnapshot.details,
            updatedAt: progressSnapshot && progressSnapshot.timestamp
        },
        lastOutput: {
            stats: getFileStats('output/last_step0_output.txt')
        }
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

// Get latest persisted Step 0 output (for refresh)
app.get('/api/step0/latest-output', (req, res) => {
    try {
        const outPath = path.join(__dirname, '..', 'output', 'last_step0_output.txt');
        let output = '';
        if (fs.existsSync(outPath)) {
            output = fs.readFileSync(outPath, 'utf8');
        }
        const files = getStep0Files();
        return res.json({ success: true, output, files });
    } catch (e) {
        return res.status(500).json({ success: false, error: e.message });
    }
});

// Step 1: Integrate Existing Data (was Step 0)
app.post('/api/step1/dev', async (req, res) => {
    const stepId = 'step1';
    const rawCutoff = req.body && req.body.issueDateCutoff;
    const cutoff = (typeof rawCutoff === 'string' ? rawCutoff : (rawCutoff !== undefined ? String(rawCutoff) : '')).trim();
    const skipFilter = Boolean(
        (req.body && (req.body.skipEnrichmentFilter || req.body.skip_enrichment_filter)) ||
        (req.query && (req.query.skipEnrichmentFilter || req.query.skip_enrichment_filter))
    );

    if (!cutoff) {
        return res.status(400).json({
            success: false,
            error: 'issueDateCutoff is required for dev integration.'
        });
    }

    if (runningProcesses.has(stepId)) {
        return res.json({
            success: false,
            error: 'Step 1 is already running'
        });
    }

    try {
        console.log(`Starting Step 1 (Dev Mode) with issue date cutoff ${cutoff}`);

        const args = ['--dev-mode', '--issue-date', cutoff];
        if (skipFilter) {
            args.push('--skip-enrichment-filter');
        }

        runPythonScriptAsync('front-end/run_step1_wrapper.py', args, stepId)
            .then((result) => {
                console.log('Step 1 (Dev Mode) completed successfully');
                try {
                    fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step1_output.txt'), (result && result.output) ? String(result.output) : '');
                } catch (e) { console.warn('Could not persist last_step1_output.txt:', e.message); }
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: true,
                    output: result.output,
                    completedAt: new Date(),
                    files: getStep1Files(),
                    potentialMatches: getStep1PotentialMatches(),
                    devMode: true,
                    issueDateCutoff: cutoff,
                    skipEnrichmentFilter: skipFilter
                });
            })
            .catch((error) => {
                console.error('Step 1 (Dev Mode) failed:', error);
                try {
                    const msg = [error.error || error.message || 'Step 1 failed', error.stderr || '', error.stdout || ''].filter(Boolean).join('\n\n');
                    fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step1_output.txt'), msg);
                } catch (e) { /* ignore */ }
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: false,
                    error: error.error || error.message,
                    stderr: error.stderr,
                    stdout: error.stdout,
                    completedAt: new Date(),
                    devMode: true,
                    issueDateCutoff: cutoff,
                    skipEnrichmentFilter: skipFilter
                });
            });

        res.json({
            success: true,
            status: 'started',
            processing: true,
            message: 'Step 1 (dev mode) started successfully. Use /api/step1/status to check progress.'
        });
    } catch (error) {
        console.error('Step 1 (Dev Mode) startup error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

app.post('/api/step1/dev/enrich-all', (req, res) => {
    const stepId = 'step1';

    if (runningProcesses.has(stepId)) {
        return res.json({
            success: false,
            error: 'Step 1 is already running'
        });
    }

    try {
        const downloadedPatents = readJsonFile('output/downloaded_patents.json');
        if (!Array.isArray(downloadedPatents) || downloadedPatents.length === 0) {
            return res.status(400).json({
                success: false,
                error: 'No downloaded patents found. Run Step 0 first.'
            });
        }

        const outputDir = path.join(__dirname, '..', 'output');
        try {
            fs.mkdirSync(outputDir, { recursive: true });
        } catch (e) { /* ignore directory errors */ }

        const toArray = (value) => Array.isArray(value) ? value : [];
        let personCounter = 0;
        const newPatents = [];
        const newPeople = [];

        downloadedPatents.forEach((rawPatent, patentIndex) => {
            if (!rawPatent) {
                return;
            }

            const patentNumberRaw = rawPatent.patent_number || rawPatent.patentNumber || rawPatent.patent_id || rawPatent.number;
            const patentNumber = (patentNumberRaw ? String(patentNumberRaw) : `unknown_${patentIndex}`).trim();
            const patentTitle = (rawPatent.patent_title || rawPatent.title || rawPatent.patentTitle || '').toString();
            const patentDate = (rawPatent.patent_date || rawPatent.issue_date || rawPatent.date || '').toString();

            const inventors = toArray(rawPatent.inventors || rawPatent.inventor_list || rawPatent.inventor || []);
            const assignees = toArray(rawPatent.assignees || rawPatent.assignee_list || rawPatent.assignee || []);

            newPatents.push({
                patent_number: patentNumber,
                patent_title: patentTitle,
                patent_date: patentDate,
                inventors,
                assignees
            });

            const normalizeAndPush = (source, type) => {
                if (!source) {
                    return;
                }

                const first = (source.first_name || source.firstName || source.inventor_name_first || source.name_first || source.given_name || '').toString().trim();
                let last = (source.last_name || source.lastName || source.inventor_name_last || source.name_last || source.surname || '').toString().trim();
                const organization = (source.organization || source.org_name || source.assignee_organization || source.company || source.name || '').toString().trim();

                if (!first && !last && organization && type === 'assignee') {
                    last = organization;
                }

                if (!first && !last && !organization) {
                    return;
                }

                const city = (source.city || source.city_name || source.city_or_town || '').toString().trim();
                const state = (source.state || source.state_code || source.state_abbr || '').toString().trim();
                const country = (source.country || source.country_code || '').toString().trim();
                const address = (source.address || source.mail_to_add1 || source.address1 || source.street || '').toString().trim();
                const postalCode = (source.zip || source.postal_code || source.mail_to_zip || '').toString().trim();

                const record = {
                    first_name: first,
                    last_name: last,
                    city,
                    state,
                    country,
                    address: address || undefined,
                    postal_code: postalCode || undefined,
                    patent_number: patentNumber,
                    patent_title: patentTitle,
                    patent_date: patentDate,
                    person_type: type,
                    person_id: `${patentNumber || 'unknown'}_${type}_${personCounter}`,
                    match_status: 'dev_enrich_all',
                    match_score: 0,
                    associated_patents: patentNumber ? [patentNumber] : [],
                    associated_patent_count: patentNumber ? 1 : 0,
                    dev_enrich_all: true
                };

                if (organization) {
                    record.organization = organization;
                }

                ['email', 'phone', 'raw_name'].forEach((field) => {
                    if (source[field]) {
                        record[field] = source[field];
                    }
                });

                newPeople.push(record);
                personCounter += 1;
            };

            inventors.forEach((inventor) => normalizeAndPush(inventor, 'inventor'));
            assignees.forEach((assignee) => normalizeAndPush(assignee, 'assignee'));
        });

        if (newPeople.length === 0) {
            return res.status(400).json({
                success: false,
                error: 'No people were found in the downloaded data.'
            });
        }

        writeJsonFile('output/new_people_for_enrichment.json', newPeople);
        writeJsonFile('output/filtered_new_patents.json', newPatents);
        writeJsonFile('output/existing_people_found.json', []);
        writeJsonFile('output/same_name_diff_address.json', []);

        const summaryMessage = `Dev Enrich All complete: queued ${newPeople.length.toLocaleString()} people from ${newPatents.length.toLocaleString()} patents.`;

        const integrationSummary = {
            success: true,
            mode: 'dev_enrich_all',
            message: summaryMessage,
            original_patents_count: downloadedPatents.length,
            new_patents_count: newPatents.length,
            new_people_count: newPeople.length,
            total_xml_patents: downloadedPatents.length,
            total_xml_people: newPeople.length,
            verification_completed: true,
            dev_enrich_all: true,
            processed_at: new Date().toISOString(),
            match_statistics: {
                auto_matched: 0,
                needs_review: 0,
                definitely_new: newPeople.length
            },
            warning: 'Integration bypassed in dev mode: all people queued for enrichment.'
        };

        writeJsonFile('output/integration_results.json', integrationSummary);

        try {
            fs.writeFileSync(path.join(outputDir, 'last_step1_output.txt'), summaryMessage);
        } catch (errorWriting) {
            console.warn('Could not persist last_step1_output.txt:', errorWriting.message);
        }

        const files = getStep1Files();

        runningProcesses.set(stepId + '_completed', {
            completed: true,
            success: true,
            output: summaryMessage,
            files,
            potentialMatches: [],
            devMode: true,
            devEnrichAll: true,
            completedAt: new Date()
        });

        res.json({
            success: true,
            output: summaryMessage,
            files,
            potentialMatches: [],
            mode: 'dev_enrich_all'
        });
    } catch (error) {
        console.error('Step 1 (Dev Enrich All) error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

app.post('/api/step2/dev/zaba-enrich', async (req, res) => {
    const { testMode = false } = req.body;
    const stepId = 'step2_zaba';
    
    if (runningProcesses.has(stepId)) {
        return res.json({
            success: false,
            error: 'ZabaSearch enrichment is already running'
        });
    }
    
    try {
        console.log(`Starting Step 2: ZabaSearch Enrichment${testMode ? ' (TEST MODE)' : ''} (Async)`);
        
        // Use the updated wrapper with --zaba flag
        const args = testMode ? ['--zaba', '--test'] : ['--zaba'];
        
        runPythonScriptAsync('front-end/run_step2_wrapper.py', args, stepId)
            .then((result) => {
                console.log('Step 2 ZabaSearch enrichment completed successfully');
                
                // Read ZabaSearch-specific result files
                const enrichedData = readJsonFile('output/enriched_patents.json');
                const enrichmentResults = readJsonFile('output/enrichment_results.json');
                const resolveRelPath = (...relPaths) => {
                    for (const rel of relPaths) {
                        const abs = path.join(__dirname, '..', rel);
                        if (fs.existsSync(abs)) {
                            return rel;
                        }
                    }
                    return relPaths[0];
                };

                const currentFormattedRel = resolveRelPath('output/current_enrichments_formatted.csv', 'output/enrichments_formatted_zaba.csv');
                const newFormattedRel = resolveRelPath('output/new_enrichments_formatted.csv', 'output/new_enrichments_formatted_zaba.csv');
                const contactCurrentRel = resolveRelPath('output/contact_current.csv', 'output/contacts_zaba.csv');
                const contactNewRel = resolveRelPath('output/contact_new.csv');
                const combinedRel = resolveRelPath('output/new_and_existing_enrichments.csv');
                const newCsvRel = resolveRelPath('output/new_enrichments.csv');

                const files = {
                    enrichedData: {
                        count: enrichedData ? enrichedData.length : 0,
                        stats: getFileStats('output/enriched_patents.json')
                    },
                    enrichedCsv: getFileStats('output/enriched_patents.csv'),
                    currentFormatted: getFileStats(currentFormattedRel),
                    newFormatted: getFileStats(newFormattedRel),
                    contactCurrent: getFileStats(contactCurrentRel),
                    contactNew: getFileStats(contactNewRel),
                    combinedCsv: getFileStats(combinedRel),
                    newEnrichmentsCsv: getFileStats(newCsvRel),
                    enrichmentResults: getFileStats('output/enrichment_results.json')
                };
                
                // Persist output for retrieval after refresh
                try {
                    fs.writeFileSync(
                        path.join(__dirname, '..', 'output', 'last_step2_zaba_output.txt'), 
                        (result && result.output) ? String(result.output) : ''
                    );
                } catch (e) { 
                    console.warn('Could not persist last_step2_zaba_output.txt:', e.message); 
                }
                
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: true,
                    method: 'zabasearch',
                    output: result.output,
                    files: files,
                    results: {
                        ...enrichmentResults,
                        method: 'zabasearch',
                        scraping_cost: '$0.00'
                    },
                    completedAt: new Date()
                });
            })
            .catch((error) => {
                console.error('Step 2 ZabaSearch enrichment failed:', error);
                
                try {
                    const msg = [
                        error.error || error.message || 'ZabaSearch enrichment failed', 
                        error.stderr || '', 
                        error.stdout || ''
                    ].filter(Boolean).join('\n\n');
                    
                    fs.writeFileSync(
                        path.join(__dirname, '..', 'output', 'last_step2_zaba_output.txt'), 
                        msg
                    );
                } catch (e) { /* ignore */ }
                
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: false,
                    method: 'zabasearch',
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
            method: 'zabasearch',
            message: `ZabaSearch enrichment started successfully${testMode ? ' (test mode)' : ''}. Use /api/step2/zaba/status to check progress.`
        });
        
    } catch (error) {
        console.error('ZabaSearch enrichment startup error:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message,
            method: 'zabasearch'
        });
    }
});

// Status endpoint for ZabaSearch enrichment
app.get('/api/step2/zaba/status', (req, res) => {
    const stepId = 'step2_zaba';
    
    const runningInfo = runningProcesses.get(stepId);
    if (runningInfo) {
        const progressPath = path.join(__dirname, '..', 'output', 'step2_zaba_progress.json');
        let progressDetails = null;

        try {
            if (fs.existsSync(progressPath)) {
                const progressData = fs.readFileSync(progressPath, 'utf8');
                progressDetails = JSON.parse(progressData);
            }
        } catch (e) {
            console.warn('Could not read ZabaSearch progress file:', e.message);
        }

        const elapsedSeconds = runningInfo.startTime ? Math.round((Date.now() - runningInfo.startTime.getTime()) / 1000) : null;
        let progressText = (runningInfo.progress && typeof runningInfo.progress === 'string') ? runningInfo.progress.trim() : '';
        if (!progressText) {
            progressText = formatZabaProgress(progressDetails) || '';
        }

        const responsePayload = {
            success: true,
            status: 'running',
            running: true,
            processing: true,
            method: 'zabasearch',
            progress: progressText || null,
            progressDetails,
            message: progressText ? `ZabaSearch enrichment in progress — ${progressText}` : 'ZabaSearch enrichment is currently running...'
        };

        if (Number.isFinite(elapsedSeconds) && elapsedSeconds >= 0) {
            responsePayload.elapsedSeconds = elapsedSeconds;
        }

        return res.json(responsePayload);
    }
    
    // Check for completed process
    const completedKey = stepId + '_completed';
    if (runningProcesses.has(completedKey)) {
        const completedResult = runningProcesses.get(completedKey);
        const progressPath = path.join(__dirname, '..', 'output', 'step2_zaba_progress.json');
        let progressDetails = null;

        try {
            if (fs.existsSync(progressPath)) {
                const progressData = fs.readFileSync(progressPath, 'utf8');
                progressDetails = JSON.parse(progressData);
            }
        } catch (e) {
            console.warn('Could not read ZabaSearch progress file after completion:', e.message);
        }

        return res.json({
            success: true,
            status: 'completed',
            processing: false,
            method: 'zabasearch',
            completed: true,
            progress: formatZabaProgress(progressDetails),
            progressDetails,
            ...completedResult
        });
    }
    
    // No process found
    res.json({
        success: true,
        status: 'idle',
        processing: false,
        method: 'zabasearch',
        message: 'No ZabaSearch enrichment process running'
    });
});

// Output endpoint for ZabaSearch enrichment logs
app.get('/api/step2/zaba/output', (req, res) => {
    const outputPath = path.join(__dirname, '..', 'output', 'last_step2_zaba_output.txt');
    
    try {
        if (fs.existsSync(outputPath)) {
            const output = fs.readFileSync(outputPath, 'utf8');
            res.json({
                success: true,
                output: output,
                method: 'zabasearch'
            });
        } else {
            res.json({
                success: true,
                output: 'No ZabaSearch enrichment output available',
                method: 'zabasearch'
            });
        }
    } catch (error) {
        console.error('Error reading ZabaSearch output file:', error);
        res.status(500).json({
            success: false,
            error: 'Could not read ZabaSearch output file',
            method: 'zabasearch'
        });
    }
});

// Files endpoint to download ZabaSearch-specific CSV files
app.get('/api/step2/zaba/files/:filename', (req, res) => {
    const { filename } = req.params;
    
    // Whitelist allowed ZabaSearch CSV files
    const allowedFiles = [
        'current_enrichments_formatted.csv',
        'enrichments_formatted_zaba.csv',
        'new_enrichments_formatted.csv',
        'new_enrichments_formatted_zaba.csv',
        'contact_current.csv',
        'contact_current_addresses.csv',
        'contacts_zaba.csv',
        'contact_new.csv',
        'contact_new_addresses.csv',
        'new_enrichments.csv',
        'new_and_existing_enrichments.csv',
        'enriched_patents.csv',
        'enriched_patents.json',
        'enrichment_results.json'
    ];
    
    if (!allowedFiles.includes(filename)) {
        return res.status(400).json({
            success: false,
            error: 'File not allowed',
            method: 'zabasearch'
        });
    }
    
    const filePath = path.join(__dirname, '..', 'output', filename);
    
    if (!fs.existsSync(filePath)) {
        return res.status(404).json({
            success: false,
            error: 'File not found',
            method: 'zabasearch'
        });
    }
    
    try {
        res.download(filePath, filename, (err) => {
            if (err) {
                console.error('Error downloading ZabaSearch file:', err);
                res.status(500).json({
                    success: false,
                    error: 'Error downloading file',
                    method: 'zabasearch'
                });
            }
        });
    } catch (error) {
        console.error('Error serving ZabaSearch file:', error);
        res.status(500).json({
            success: false,
            error: 'Error serving file',
            method: 'zabasearch'
        });
    }
});

app.post('/api/step1', async (req, res) => {
    const stepId = 'step1';
    const skipFilter = Boolean(
        (req.body && (req.body.skipEnrichmentFilter || req.body.skip_enrichment_filter)) ||
        (req.query && (req.query.skipEnrichmentFilter || req.query.skip_enrichment_filter))
    );
    
    // Check if already running
    if (runningProcesses.has(stepId)) {
        return res.json({
            success: false,
            error: 'Step 1 is already running'
        });
    }
    
    try {
        console.log('Starting Step 1: Integrate Existing Data (Async)' + (skipFilter ? ' [skip already-enriched filter]' : ''));
        
        // Start the process asynchronously
        const args = [];
        if (skipFilter) {
            args.push('--skip-enrichment-filter');
        }

        runPythonScriptAsync('front-end/run_step1_wrapper.py', args, stepId)
            .then((result) => {
                console.log('Step 1 completed successfully');
                
                // Store completion result for status endpoint
                try {
                    fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step1_output.txt'), (result && result.output) ? String(result.output) : '');
                } catch (e) { console.warn('Could not persist last_step1_output.txt:', e.message); }
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: true,
                    output: result.output,
                    completedAt: new Date(),
                    files: getStep1Files(),
                    potentialMatches: getStep1PotentialMatches(),
                    skipEnrichmentFilter: skipFilter
                });
            })
            .catch((error) => {
                console.error('Step 1 failed:', error);
                
                // Store error result for status endpoint
                try {
                    const msg = [error.error || error.message || 'Step 1 failed', error.stderr || '', error.stdout || ''].filter(Boolean).join('\n\n');
                    fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step1_output.txt'), msg);
                } catch (e) { /* ignore */ }
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: false,
                    error: error.error || error.message,
                    stderr: error.stderr,
                    stdout: error.stdout,
                    completedAt: new Date(),
                    skipEnrichmentFilter: skipFilter
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

// Get latest persisted Step 1 output (for refresh)
app.get('/api/step1/latest-output', (req, res) => {
    try {
        const outPath = path.join(__dirname, '..', 'output', 'last_step1_output.txt');
        let output = '';
        if (fs.existsSync(outPath)) {
            output = fs.readFileSync(outPath, 'utf8');
        }
        const files = {
            newPeople: getFileStats('output/new_people_for_enrichment.json'),
            newPatents: getFileStats('output/filtered_new_patents.json'),
            integrationResults: getFileStats('output/integration_results.json')
        };
        return res.json({ success: true, output, files });
    } catch (e) {
        return res.status(500).json({ success: false, error: e.message });
    }
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
                // Persist last output log for retrieval after refresh
                try {
                    fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step2_output.txt'), (result && result.output) ? String(result.output) : '');
                } catch (e) { console.warn('Could not persist last_step2_output.txt:', e.message); }
                const completedAt = new Date();
                writeStepStatus(stepId, {
                    success: true,
                    completedAt: completedAt.toISOString(),
                    files,
                    outputSummary: truncateText((result && result.output) ? String(result.output) : '', 4000)
                });
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: true,
                    output: result.output,
                    files: files,
                    results: enrichmentResults,
                    completedAt
                });
            })
            .catch((error) => {
                console.error('Step 2 failed:', error);
                try {
                    const msg = [error.error || error.message || 'Step 2 failed', error.stderr || '', error.stdout || ''].filter(Boolean).join('\n\n');
                    fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step2_output.txt'), msg);
                } catch (e) { /* ignore */ }
                const completedAt = new Date();
                writeStepStatus(stepId, {
                    success: false,
                    completedAt: completedAt.toISOString(),
                    error: error.error || error.message || 'Step 2 failed',
                    stderr: truncateText(error.stderr || '', 4000),
                    stdoutSnippet: truncateText(error.stdout || '', 2000)
                });
                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: false,
                    error: error.error || error.message,
                    stderr: error.stderr,
                    stdout: error.stdout,
                    completedAt
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
                try {
                    fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step2_output.txt'), (result && result.output) ? String(result.output) : '');
                } catch (e) { /* ignore */ }
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
                try {
                    const msg = [error.error || error.message || 'Step 2 failed', error.stderr || '', error.stdout || ''].filter(Boolean).join('\n\n');
                    fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step2_output.txt'), msg);
                } catch (e) { /* ignore */ }
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

// Step 2: Rebuild CSVs from SQL (no API calls)
app.post('/api/step2/rebuild-csvs', async (req, res) => {
  try {
    const stepId = 'step2';
    if (runningProcesses.has(stepId)) {
      return res.json({ success: false, error: 'Step 2 is already running' });
    }

    const args = ['--rebuild'];
    console.log('Starting Step 2: Rebuild CSVs (no API calls)');
    runPythonScriptAsync('front-end/run_step2_wrapper.py', args, stepId)
      .then((result) => {
        const msg = result && result.output ? String(result.output) : '';
        try {
          fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step2_output.txt'), msg);
        } catch (_) {}

        const files = {
          currentFormatted: getFileStats('output/current_enrichments_formatted.csv'),
          newFormatted: getFileStats('output/new_enrichments_formatted.csv'),
          combinedFormatted: getFileStats('output/new_and_existing_enrichments_formatted.csv'),
          currentCsv: getFileStats('output/current_enrichments.csv'),
          combinedCsv: getFileStats('output/new_and_existing_enrichments.csv'),
          contactCurrent: getFileStats('output/contact_current.csv'),
          contactNew: getFileStats('output/contact_new.csv')
        };

        runningProcesses.set(stepId + '_completed', {
          completed: true,
          success: true,
          output: msg,
          files,
          completedAt: new Date()
        });
      })
      .catch((err) => {
        const msg = `Step 2 Rebuild failed: ${err && err.message ? err.message : String(err)}`;
        try {
          fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step2_output.txt'), msg);
        } catch (_) {}
        runningProcesses.set(stepId + '_completed', {
          completed: true,
          success: false,
          error: err && err.message ? err.message : String(err),
          stderr: err && err.stderr,
          stdout: err && err.stdout,
          completedAt: new Date()
        });
      });

    res.json({ success: true, status: 'started', processing: true, message: 'Rebuild CSVs started. Use /api/step2/status to check progress.' });
  } catch (e) {
    console.error('Failed to start Step 2 rebuild:', e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// Step 2: Generate All & Current CSVs (slower operation)
app.post('/api/step2/generate-all-current', async (req, res) => {
    try {
        const stepId = 'step2-all-current';
        if (runningProcesses.has(stepId)) {
            return res.json({
                success: false,
                error: 'All & Current CSV generation is already running'
            });
        }

        // Clear any stale completion marker so UI can use fresh status
        runningProcesses.delete(stepId + '_completed');

        const args = ['--generate-all-current'];
        console.log('Starting Step 2: Generate All & Current CSVs');
        runPythonScriptAsync('front-end/run_step2_wrapper.py', args, stepId)
            .then((result) => {
                const msg = result && result.output ? String(result.output) : '';
                try {
                    fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step2_all_current_output.txt'), msg);
                } catch (writeErr) {
                    console.warn('Could not persist last_step2_all_current_output.txt:', writeErr.message);
                }

                const files = {
                    allCsv: getFileStats('output/all_enrichments.csv'),
                    currentCsv: getFileStats('output/current_enrichments.csv'),
                    allFormatted: getFileStats('output/all_enrichments_formatted.csv'),
                    currentFormatted: getFileStats('output/current_enrichments_formatted.csv'),
                    contactsCurrent: getFileStats('output/contacts_current.csv'),
                    addressesCurrent: getFileStats('output/addresses_current.csv')
                };
                const completedAt = new Date();

                writeStepStatus(stepId, {
                    success: true,
                    completedAt: completedAt.toISOString(),
                    files,
                    outputSummary: truncateText(msg, 4000)
                });

                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: true,
                    output: msg,
                    files,
                    completedAt
                });
            })
            .catch((err) => {
                const msgParts = [
                    err && err.message ? err.message : 'Step 2 All & Current generation failed',
                    err && err.stderr ? err.stderr : '',
                    err && err.stdout ? err.stdout : ''
                ].filter(Boolean);
                const msg = msgParts.join('\n\n');
                try {
                    fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step2_all_current_output.txt'), msg);
                } catch (writeErr) {
                    console.warn('Could not persist last_step2_all_current_output.txt:', writeErr.message);
                }

                const completedAt = new Date();
                writeStepStatus(stepId, {
                    success: false,
                    completedAt: completedAt.toISOString(),
                    error: err && err.message ? err.message : 'Step 2 All & Current generation failed',
                    stderr: truncateText(err && err.stderr ? err.stderr : '', 4000),
                    stdoutSnippet: truncateText(err && err.stdout ? err.stdout : '', 2000)
                });

                runningProcesses.set(stepId + '_completed', {
                    completed: true,
                    success: false,
                    error: err && err.message ? err.message : 'Step 2 All & Current generation failed',
                    stderr: err && err.stderr,
                    stdout: err && err.stdout,
                    completedAt
                });
            });
        res.json({
            success: true,
            status: 'started',
            processing: true,
            message: 'All & Current CSV generation started. This may take several minutes.'
        });
    } catch (e) {
        console.error('Failed to start All & Current CSV generation:', e);
        res.status(500).json({ success: false, error: e.message });
    }
});

app.post('/api/step2/rebuild-zaba-csvs', async (req, res) => {
  try {
    const stepId = `step2-zaba-rebuild-${Date.now()}`;
    const pythonExec = resolvePython();
    const args = ['front-end/run_step2_wrapper.py', '--rebuild', '--zaba'];
    console.log('Starting Step 2: Rebuild ZabaSearch CSVs (no scraping)');
    runPythonScriptAsync('front-end/run_step2_wrapper.py', args, stepId)
      .then((result) => {
        const msg = result && result.output ? String(result.output) : '';
        fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step2_zaba_output.txt'), msg);
      })
      .catch((err) => {
        const msg = `Step 2 ZabaSearch Rebuild failed: ${err && err.message ? err.message : String(err)}`;
        fs.writeFileSync(path.join(__dirname, '..', 'output', 'last_step2_zaba_output.txt'), msg);
      });
    res.json({
      success: true,
      status: 'started',
      processing: true,
      message: 'ZabaSearch CSV rebuild started. Use /api/step2/status to check progress.'
    });
  } catch (e) {
    console.error('Failed to start Step 2 ZabaSearch rebuild:', e);
    res.status(500).json({ success: false, error: e.message });
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
    const step0LastStatus = readStepStatus('step0');
    const status = {
        step0: {
            downloadResults: getFileStats('output/download_results.json'),
            // downloadedPatents metric removed from UI
            downloadedPatents: getFileStats('output/downloaded_patents.json'),
            running: runningProcesses.has('step0'),
            lastRunStatus: step0LastStatus
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
    let step2LastStatus = readStepStatus('step2');
    if (step2LastStatus) {
        status.step2.lastRunStatus = step2LastStatus;
    }
    // Apply cycle reset: treat Step 1/2 results as not completed if older than cycle start
    try {
        const cycleStartTs = readCycleStart();
        if (cycleStartTs) {
            try {
                if (status.step1 && status.step1.integrationResults && status.step1.integrationResults.exists) {
                    const m = status.step1.integrationResults.modified;
                    if (m && new Date(m).getTime() <= cycleStartTs.getTime()) {
                        status.step1.integrationResults.exists = false;
                    }
                }
            } catch (_) {}
            try {
                if (status.step2) {
                    // Mask enrichmentResults
                    if (status.step2.enrichmentResults && status.step2.enrichmentResults.exists) {
                        const m = status.step2.enrichmentResults.modified;
                        if (m && new Date(m).getTime() <= cycleStartTs.getTime()) {
                            status.step2.enrichmentResults.exists = false;
                        }
                    }
                    // Mask enrichedData (used by UI to mark Completed)
                    if (status.step2.enrichedData && status.step2.enrichedData.exists) {
                        const m2 = status.step2.enrichedData.modified;
                        if (m2 && new Date(m2).getTime() <= cycleStartTs.getTime()) {
                            status.step2.enrichedData.exists = false;
                        }
                    }
                    // Mask enrichedCsv as well for consistency
                    if (status.step2.enrichedCsv && status.step2.enrichedCsv.exists) {
                        const m3 = status.step2.enrichedCsv.modified;
                        if (m3 && new Date(m3).getTime() <= cycleStartTs.getTime()) {
                            status.step2.enrichedCsv.exists = false;
                        }
                    }
                    if (status.step2.lastRunStatus && status.step2.lastRunStatus.completedAt) {
                        const completedAt = new Date(status.step2.lastRunStatus.completedAt);
                        if (!Number.isNaN(completedAt.getTime()) && completedAt.getTime() <= cycleStartTs.getTime()) {
                            status.step2.lastRunStatus = null;
                        }
                    }
                }
            } catch (_) {}
            // If enrichment_results.json contains a reset flag, force Step 2 to appear not completed
            try {
                const enr = readJsonFile('output/enrichment_results.json');
                const enrStats = status.step2 && status.step2.enrichmentResults;
                const enrWasReset = enr && enr.reset === true && (!enrStats || !enrStats.modified || (new Date(enrStats.modified).getTime() >= cycleStartTs.getTime()));
                if (enrWasReset && status.step2) {
                    if (status.step2.enrichmentResults) status.step2.enrichmentResults.exists = false;
                    if (status.step2.enrichedData) status.step2.enrichedData.exists = false;
                    if (status.step2.enrichedCsv) status.step2.enrichedCsv.exists = false;
                }
            } catch (_) { /* ignore */ }
        }
    } catch (_) { /* ignore */ }
    step2LastStatus = status.step2.lastRunStatus || null;
    
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
        step0: (() => {
            if (step0LastStatus && step0LastStatus.completedAt) {
                return step0LastStatus.completedAt;
            }
            return status.step0.downloadResults.modified || null;
        })(),
        step1: status.step1.integrationResults.modified || null,
        step2: (() => {
            // Hide last run timestamp if this is a reset marker
            try {
                const enr = readJsonFile('output/enrichment_results.json');
                if (enr && enr.reset === true) return null;
            } catch (_) {}
            if (step2LastStatus && step2LastStatus.completedAt) {
                return step2LastStatus.completedAt;
            }
            return status.step2.enrichmentResults.modified || null;
        })()
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

    status.runningProcesses = Array.from(runningProcesses.entries())
        .filter(([, info]) => info && info.process)
        .map(([key]) => key);

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
    console.log('   POST /api/step0/extract - Alternate Step 0 (legacy extractor)');
    console.log('   GET  /api/step0/status - Check Step 0 progress');
    console.log('   POST /api/step0/upload-xml - Upload raw XML to ingest');
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

function writeCsvToFile(filePath, headers, rows) {
    try {
        const full = path.join(__dirname, '..', filePath);
        const dir = path.dirname(full);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        const fd = fs.openSync(full, 'w');
        fs.writeSync(fd, headers.join(',') + '\n');
        for (const row of rows) {
            const line = headers.map(h => csvEscape(row[h])).join(',') + '\n';
            fs.writeSync(fd, line);
        }
        fs.closeSync(fd);
        const stats = fs.statSync(full);
        console.log(`[export] Wrote CSV file ${filePath} (${stats.size} bytes, rows=${rows.length})`);
        return true;
    } catch (e) {
        console.error('[export] Failed writing CSV file', filePath, e);
        return false;
    }
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

// Minimal hydrator: only fetch inventor_id, mod_user, title from SQL by name+city+state
async function hydrateThreeFields(arr, label = 'hydrate_three_fields') {
    let conn;
    try {
        const mysql = require('mysql2/promise');
        conn = await mysql.createConnection({
            host: process.env.DB_HOST || 'localhost',
            port: Number(process.env.DB_PORT || 3306),
            database: process.env.DB_NAME || 'patent_data',
            user: process.env.DB_USER || 'root',
            password: process.env.DB_PASSWORD || 'password'
        });
    } catch (e) {
        console.warn('[export] Could not connect to DB to hydrate (3 fields):', e.message);
        return arr; // return unmodified if no DB
    }

    const cache = new Map();
    const patentTitleCache = new Map();
    const norm = (v) => (v || '').toString().trim();
    const keyOf = (first, last, city, state) => `${norm(first).toLowerCase()}|${norm(last).toLowerCase()}|${norm(city).toLowerCase()}|${norm(state).toLowerCase()}`;
    const debug = { queried: 0, foundExact: 0, foundNoCity: 0, assigned: { inventor_id:0, mod_user:0, title:0, patent_no:0, mail_to_add1:0, mail_to_zip:0 }, errors: 0, columns: [], selected: {} };

    // Build select list dynamically based on available columns (existing_people)
    let selectCols = `inventor_id, mod_user, title, patent_no, mail_to_add1, mail_to_zip`;
    try {
        const [colRows] = await conn.execute('SHOW COLUMNS FROM existing_people');
        const cols = Array.isArray(colRows) ? colRows.map(r => r.Field || r.COLUMN_NAME || r.field).filter(Boolean) : [];
        debug.columns = cols;
        const pick = (want, alts=[]) => {
            if (cols.includes(want)) return want;
            for (const a of alts) if (cols.includes(a)) return a;
            return null;
        };
        const mapping = {
            inventor_id: pick('inventor_id'),
            mod_user: pick('mod_user'),
            title: pick('title'),
            patent_no: pick('patent_no'),
            mail_to_add1: pick('mail_to_add1', ['address','mail_to_add_1','addr1']),
            mail_to_zip: pick('mail_to_zip', ['zip','postal_code'])
        };
        debug.selected = mapping;
        const parts = [];
        for (const [alias, col] of Object.entries(mapping)) {
            if (!col) continue;
            parts.push(col === alias ? col : `${col} AS ${alias}`);
        }
        if (parts.length > 0) selectCols = parts.join(', ');
        // Prefer rows that actually have these fields populated
        const orderParts = [];
        if (mapping.patent_no) orderParts.push(`(${mapping.patent_no} IS NOT NULL AND ${mapping.patent_no} <> '') DESC`);
        if (mapping.title) orderParts.push(`(${mapping.title} IS NOT NULL AND ${mapping.title} <> '') DESC`);
        if (mapping.mail_to_add1) orderParts.push(`(${mapping.mail_to_add1} IS NOT NULL AND ${mapping.mail_to_add1} <> '') DESC`);
        if (mapping.mail_to_zip) orderParts.push(`(${mapping.mail_to_zip} IS NOT NULL AND ${mapping.mail_to_zip} <> '') DESC`);
        var hydrateOrderClause = orderParts.length ? ` ORDER BY ${orderParts.join(', ')}` : '';
    } catch (e) {
        console.warn('[export][hydrate] SHOW COLUMNS failed:', e.message);
    }

    // Build select list for existing_people_new separately (may have different columns)
    let selectColsNew = selectCols;
    let hydrateOrderClauseNew = hydrateOrderClause || '';
    try {
        const [colRows2] = await conn.execute('SHOW COLUMNS FROM existing_people_new');
        const cols2 = Array.isArray(colRows2) ? colRows2.map(r => r.Field || r.COLUMN_NAME || r.field).filter(Boolean) : [];
        const pick2 = (want, alts=[]) => {
            if (cols2.includes(want)) return want;
            for (const a of alts) if (cols2.includes(a)) return a;
            return null;
        };
        const mapping2 = {
            inventor_id: pick2('inventor_id'),
            mod_user: pick2('mod_user'),
            title: pick2('title'),
            patent_no: pick2('patent_no'),
            mail_to_add1: pick2('mail_to_add1', ['address','mail_to_add_1','addr1']),
            mail_to_zip: pick2('mail_to_zip', ['zip','postal_code'])
        };
        const parts2 = [];
        for (const [alias, col] of Object.entries(mapping2)) {
            if (!col) continue;
            parts2.push(col === alias ? col : `${col} AS ${alias}`);
        }
        if (parts2.length > 0) selectColsNew = parts2.join(', ');
        const orderParts2 = [];
        if (mapping2.patent_no) orderParts2.push(`(${mapping2.patent_no} IS NOT NULL AND ${mapping2.patent_no} <> '') DESC`);
        if (mapping2.title) orderParts2.push(`(${mapping2.title} IS NOT NULL AND ${mapping2.title} <> '') DESC`);
        if (mapping2.mail_to_add1) orderParts2.push(`(${mapping2.mail_to_add1} IS NOT NULL AND ${mapping2.mail_to_add1} <> '') DESC`);
        if (mapping2.mail_to_zip) orderParts2.push(`(${mapping2.mail_to_zip} IS NOT NULL AND ${mapping2.mail_to_zip} <> '') DESC`);
        hydrateOrderClauseNew = orderParts2.length ? ` ORDER BY ${orderParts2.join(', ')}` : '';
    } catch (e) {
        // Table may not exist; ignore
    }
    const makeQuery = (table) => `SELECT ${selectCols} FROM ${table}
                   WHERE first_name = ? AND last_name = ?
                     AND IFNULL(city,'') = ? AND IFNULL(state,'') = ?${typeof hydrateOrderClause === 'string' ? hydrateOrderClause : ''}
                   LIMIT 1`;
    const makeQueryNew = (table) => `SELECT ${selectColsNew} FROM ${table}
                    WHERE first_name = ? AND last_name = ?
                      AND IFNULL(city,'') = ? AND IFNULL(state,'') = ?${typeof hydrateOrderClauseNew === 'string' ? hydrateOrderClauseNew : ''}
                    LIMIT 1`;

    for (const item of arr) {
        // Try to locate original person fields from enriched structures
        const edRoot = (item && item.enrichment_result && item.enrichment_result.enriched_data)
            ? item.enrichment_result.enriched_data
            : (item && item.enriched_data) || null;
        const original = edRoot ? (edRoot.original_data || edRoot.original_person || item.original_person || {}) : (item || {});
        const first = original.first_name || item.first_name || '';
        const last = original.last_name || item.last_name || '';
        const city = original.city || item.city || '';
        const state = original.state || item.state || '';
        const k = keyOf(first, last, city, state);
        if (cache.has(k)) {
            Object.assign(item, cache.get(k));
            continue;
        }
        try {
            let [rows] = await conn.execute(makeQuery('existing_people'), [norm(first), norm(last), norm(city), norm(state)]);
            debug.queried++;
            if (!rows || rows.length === 0) {
                [rows] = await conn.execute(makeQueryNew('existing_people_new'), [norm(first), norm(last), norm(city), norm(state)]);
            }
            let source = 'exact';
            // Fallback: ignore city if still not found
            if (!rows || rows.length === 0) {
                const makeQueryNoCity = (table) => `SELECT ${selectCols} FROM ${table}
                    WHERE first_name = ? AND last_name = ?
                      AND IFNULL(state,'') = ?${typeof hydrateOrderClause === 'string' ? hydrateOrderClause : ''}
                    LIMIT 1`;
                [rows] = await conn.execute(makeQueryNoCity('existing_people'), [norm(first), norm(last), norm(state)]);
                if (!rows || rows.length === 0) {
                    const makeQueryNoCityNew = (table) => `SELECT ${selectColsNew} FROM ${table}
                        WHERE first_name = ? AND last_name = ?
                          AND IFNULL(state,'') = ?${typeof hydrateOrderClauseNew === 'string' ? hydrateOrderClauseNew : ''}
                        LIMIT 1`;
                    [rows] = await conn.execute(makeQueryNoCityNew('existing_people_new'), [norm(first), norm(last), norm(state)]);
                }
                source = 'no_city';
            }
            const extra = rows && rows[0] ? rows[0] : {};
            cache.set(k, extra);
            // Only attach the three fields explicitly
            item._hydrated_fields = item._hydrated_fields || [];
            if (rows && rows.length > 0) {
                if (source === 'exact') debug.foundExact++; else debug.foundNoCity++;
            }
            if (extra.inventor_id != null) { item.inventor_id = extra.inventor_id; debug.assigned.inventor_id++; item._hydrated_fields.push('inventor_id'); }
            if (extra.mod_user != null) { item.mod_user = extra.mod_user; debug.assigned.mod_user++; item._hydrated_fields.push('mod_user'); }
            if (extra.title != null) { item.title = extra.title; debug.assigned.title++; item._hydrated_fields.push('title'); }
            if (extra.patent_no != null) { item.patent_no = extra.patent_no; debug.assigned.patent_no++; item._hydrated_fields.push('patent_no'); }
            if (extra.mail_to_add1 != null) { item.mail_to_add1 = extra.mail_to_add1; debug.assigned.mail_to_add1++; item._hydrated_fields.push('mail_to_add1'); }
            if (extra.mail_to_zip != null) { item.mail_to_zip = extra.mail_to_zip; debug.assigned.mail_to_zip++; item._hydrated_fields.push('mail_to_zip'); }
            // Ensure patent_no at least reflects patent_number if present
            if (!item.patent_no && (original.patent_number || item.patent_number)) {
                item.patent_no = original.patent_number || item.patent_number;
            }
            // If title is still empty, try to resolve from downloaded_patents by patent_number
            const pn = (original.patent_number || item.patent_number || item.patent_no || '').toString().trim();
            if (!item.title && pn) {
                if (!patentTitleCache.has(pn)) {
                    try {
                        const [prow] = await conn.execute("SELECT patent_title FROM downloaded_patents WHERE patent_number = ? LIMIT 1", [pn]);
                        const title = (prow && prow[0] && (prow[0].patent_title || prow[0].title)) || '';
                        patentTitleCache.set(pn, title || '');
                    } catch (_) {
                        patentTitleCache.set(pn, '');
                    }
                }
                const t = patentTitleCache.get(pn);
                if (t) { item.title = t; item._hydrated_fields.push('title'); }
            }
        } catch (e) {
            // On query error, just skip hydration for this item
            cache.set(k, {});
            debug.errors++;
            if (debug.errors <= 5) console.warn('[export][hydrate] query error for', {first, last, city, state}, e.message);
        }
    }
    try { await conn.end(); } catch (_) {}
    try {
        const dir = ensureLogsDir();
        if (dir) {
            const fname = `${String(label).replace(/\s+/g,'_').toLowerCase()}_hydration_summary.json`;
            const out = path.join(dir, fname);
            fs.writeFileSync(out, JSON.stringify({ debug, generated_at: new Date().toISOString() }, null, 2));
            if (EXPORT_DEBUG) console.log(`[export][diag] hydration summary written: ${out}`);
        } else {
            console.log('[export][diag] hydration summary (no logs dir):', debug);
        }
    } catch (_) {}
    return arr;
}

// Build a stable person signature to dedupe across sources (enriched vs step1 lists)
function personSignature(item) {
    // Try to locate original person fields from enriched structures
    const edRoot = (item && (item.enrichment_result && item.enrichment_result.enriched_data))
        ? item.enrichment_result.enriched_data
        : (item && item.enriched_data) || null;
    const original = edRoot ? (edRoot.original_data || edRoot.original_person || item.original_person || {}) : (item || {});

    const get = (k) => {
        const v = (original && original[k]) ?? item[k] ?? '';
        return typeof v === 'string' ? v.trim().toLowerCase() : String(v || '').trim().toLowerCase();
    };
    const first = get('first_name');
    const last = get('last_name');
    const city = get('city');
    const state = get('state');
    const patent = (original.patent_number || item.patent_number || '').toString().trim();
    return `${first}_${last}_${city}_${state}_${patent}`;
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

function extractExistingRow(item) {
    // Map a Step 1 existing person (already in DB) into the unified export shape
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
        email: '',
        linkedin_url: '',
        job_title: '',
        company: ''
    };
}

const UNIFIED_HEADERS = [
    'first_name','last_name','city','state','country',
    'patent_number','patent_title','person_type','match_score',
    'email','linkedin_url','job_title','company'
];

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
        const outPath = path.join(__dirname, '..', 'output', 'current_enrichments.csv');
        if (!fs.existsSync(outPath)) {
            return res.status(404).json({ error: 'current_enrichments.csv not found. Run Step 2 first.' });
        }
        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', 'attachment; filename="current_enrichments.csv"');
        fs.createReadStream(outPath).pipe(res);
    } catch (e) {
        console.error('Export current enrichments failed:', e);
        res.status(500).json({ error: e.message });
    }
});

// Export only new enrichments from this Step 2 run
app.get('/api/export/new-enrichments', (req, res) => {
    try {
        const outPath = path.join(__dirname, '..', 'output', 'new_enrichments.csv');
        if (!fs.existsSync(outPath)) {
            return res.status(404).json({ error: 'new_enrichments.csv not found. Run Step 2 first.' });
        }
        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', 'attachment; filename="new_enrichments.csv"');
        fs.createReadStream(outPath).pipe(res);
    } catch (e) {
        console.error('Export new enrichments failed:', e);
        res.status(500).json({ error: e.message });
    }
});

// Export new + existing enriched records relevant to this run
app.get('/api/export/new-and-existing-enrichments', (req, res) => {
    try {
        const outPath = path.join(__dirname, '..', 'output', 'new_and_existing_enrichments.csv');
        if (!fs.existsSync(outPath)) {
            return res.status(404).json({ error: 'new_and_existing_enrichments.csv not found. Run Step 2 first.' });
        }
        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', 'attachment; filename="new_and_existing_enrichments.csv"');
        fs.createReadStream(outPath).pipe(res);
    } catch (e) {
        console.error('Export new & existing enrichments failed:', e);
        res.status(500).json({ error: e.message });
    }
});

// Helper: build formatted row per requested Access-style schema
function sanitizeForCsv(val) {
    if (val === null || val === undefined) return '';
    if (typeof val === 'boolean') return '';
    const s = String(val).trim();
    if (/^(nan|null|none)$/i.test(s)) {
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
// Helpers to safely extract address details from PDL payloads
function pickPdlStreet(pdl) {
  if (!pdl) return '';
  // Per request: use only the company street address field
  const s = firstNonEmpty(pdl.job_company_location_street_address);
  if (s) return s;
  return '';
}
function pickPdlZip(pdl) {
  if (!pdl) return '';
  // Per request: use only the company postal code field
  const z = firstNonEmpty(pdl.job_company_location_postal_code);
  if (z) return z;
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
  const existing = ed.existing_record || {}; // backfilled from SQL at save time when available
  // choose an email if present
  let email = '';
  if (Array.isArray(pdl.emails) && pdl.emails.length > 0) {
    const e0 = pdl.emails[0];
    email = (typeof e0 === 'string') ? e0 : (e0 && (e0.address || e0.email || ''));
  }
  const streetFromPdl = pickPdlStreet(pdl);
  // Prefer the PDL company street address first
  const street = firstNonEmpty(streetFromPdl, item.mail_to_add1, existing.mail_to_add1);
  const line2 = firstNonEmpty(pdl.job_company_location_address_line_2, pdl.location_address_line_2);
  const city = firstNonEmpty(pdl.job_company_location_locality, pdl.location_locality, original.city, item.city);
  // Do not override state with PDL; preserve original/existing which are 2-letter by default
  const state = firstNonEmpty(item.mail_to_state, existing.mail_to_state, original.state, item.state);
  const zipFromPdl = pickPdlZip(pdl);
  // Prefer the PDL company postal code first
  const zip = firstNonEmpty(zipFromPdl, item.mail_to_zip, existing.mail_to_zip);
  const country = firstNonEmpty(pdl.job_company_location_country, pdl.location_country, original.country, item.country);
  const first = firstNonEmpty(original.first_name, item.first_name, (item.enrichment_result && item.enrichment_result.original_name && item.enrichment_result.original_name.split(' ')[0]));
  const last = firstNonEmpty(original.last_name, item.last_name, (item.enrichment_result && item.enrichment_result.original_name && item.enrichment_result.original_name.split(' ').slice(1).join(' ')));
  const full = (first || last) ? `${first} ${last}`.trim() : '';
  const formatted = {
    issue_id: item.issue_id || '',
    new_issue_rec_num: item.new_issue_rec_num || '',
    inventor_id: firstNonEmpty(item.inventor_id, existing.inventor_id),
    patent_no: firstNonEmpty(item.patent_no, existing.patent_no, original.patent_number, item.patent_number, (item.enrichment_result && item.enrichment_result.patent_number)),
    title: firstNonEmpty(item.title, existing.title, item.patent_title, original.patent_title, (item.enrichment_result && item.enrichment_result.patent_title)),
    issue_date: item.issue_date || '',
    mail_to_assignee: item.mail_to_assignee || '',
    mail_to_name: sanitizeForCsv(firstNonEmpty(item.mail_to_name, full)),
    mail_to_add1: firstNonEmpty(item.mail_to_add1, existing.mail_to_add1, street),
    mail_to_add2: firstNonEmpty(item.mail_to_add2, line2),
    mail_to_add3: item.mail_to_add3 || '',
    mail_to_city: firstNonEmpty(item.mail_to_city, city),
    mail_to_state: firstNonEmpty(item.mail_to_state, state),
    mail_to_zip: firstNonEmpty(item.mail_to_zip, existing.mail_to_zip, zip),
    mail_to_country: firstNonEmpty(item.mail_to_country, country),
    mail_to_send_key: item.mail_to_send_key || '',
    inventor_first: first,
    inventor_last: last,
    mod_user: firstNonEmpty(item.mod_user, existing.mod_user),
    bar_code: item.bar_code || '',
    inventor_contact: firstNonEmpty(item.inventor_contact, email)
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

function writeFormattedCsvToFile(filePath, data) {
  const rows = data.map(buildFormattedRow);
  return writeCsvToFile(filePath, FORMATTED_HEADERS, rows);
}

// Build the unified CURRENT scope for both normal and formatted exports
// CURRENT = people involved in this run only:
//   - current_cycle_enriched.json (newly_enriched + matched_existing)
//   - step1 existing people (existing_people_in_db.json or existing_people_found.json)
// Then dedupe by signature and, when possible, overlay the enriched
// version from the full snapshot so details are present for skipped entries.
function getCurrentScopeRecordsForExport() {
  // Helper signature for current scope: ignore patent number; use only name+city+state
  const sigByPerson = (item) => {
    const edRoot = (item && (item.enrichment_result && item.enrichment_result.enriched_data))
      ? item.enrichment_result.enriched_data
      : (item && item.enriched_data) || null;
    const original = edRoot ? (edRoot.original_data || edRoot.original_person || item.original_person || {}) : (item || {});
    const norm = (v) => (v == null ? '' : String(v)).trim().toLowerCase();
    const first = norm(original.first_name || item.first_name);
    const last = norm(original.last_name || item.last_name);
    const city = norm(original.city || item.city);
    const state = norm(original.state || item.state);
    return `${first}|${last}|${city}|${state}`;
  };

  // Load current-cycle records (newly_enriched + matched_existing saved by Step 2)
  let current = readJsonFile('output/current_cycle_enriched.json');
  if (!Array.isArray(current)) current = [];

  // Step 1 existing people for this run
  let step1ExistingRaw = readJsonFile('output/existing_people_in_db.json');
  if (!Array.isArray(step1ExistingRaw) || step1ExistingRaw.length === 0) {
    const fallbackExisting = readJsonFile('output/existing_people_found.json');
    if (Array.isArray(fallbackExisting)) {
      step1ExistingRaw = fallbackExisting;
    }
  }
  if (!Array.isArray(step1ExistingRaw)) step1ExistingRaw = [];

  const filteredExisting = readJsonFile('output/existing_filtered_enriched_people.json');
  if (Array.isArray(filteredExisting) && filteredExisting.length > 0) {
    step1ExistingRaw = step1ExistingRaw.concat(filteredExisting);
  }

  // Dedupe current by person
  const curSeen = new Set();
  const currentDedup = [];
  for (const it of current) {
    const s = sigByPerson(it);
    if (!s) continue;
    if (curSeen.has(s)) continue;
    curSeen.add(s);
    currentDedup.push(it);
  }

  // Merge current + step1 existing, preferring enriched entries on duplicates
  const combined = currentDedup.concat((Array.isArray(step1ExistingRaw) ? step1ExistingRaw : []).filter(it => !!sigByPerson(it)));
  const merged = dedupeBySignature(combined);

  // Build enrichment overlay map from full snapshot (allows skipped/already-enriched to have details)
  let fullSnapshot = readJsonFile('output/enriched_patents.json');
  if (!Array.isArray(fullSnapshot)) fullSnapshot = [];
  const enrichedMap = new Map();
  for (const rec of fullSnapshot) {
    const s = sigByPerson(rec);
    if (s) enrichedMap.set(s, rec);
  }

  // Overlay for merged set
  const currentOverlaid = merged.map(it => {
    const s = sigByPerson(it);
    if (s && enrichedMap.has(s)) return enrichedMap.get(s);
    return it;
  });
  const scoped = currentOverlaid;

  console.log(`[export] CURRENT scope -> current_cycle=${current.length}, step1_existing=${Array.isArray(step1ExistingRaw)?step1ExistingRaw.length:0},` +
              ` current_dedup=${currentDedup.length}, merged=${merged.length}, final=${scoped.length}`);
  return scoped;
}

// Dedupe helpers: keep one row per person signature; prefer rows that have enriched_data
function dedupeBySignature(items) {
  const seen = new Map();
  for (const it of items) {
    const sig = personSignature(it);
    if (!sig) continue;
    if (!seen.has(sig)) {
      seen.set(sig, it);
      continue;
    }
    const cur = seen.get(sig);
    const curHasEnriched = !!(cur && (cur.enriched_data || (cur.enrichment_result && cur.enrichment_result.enriched_data)));
    const itHasEnriched = !!(it && (it.enriched_data || (it.enrichment_result && it.enrichment_result.enriched_data)));
    // Prefer entries that carry enriched data; otherwise keep the first
    if (itHasEnriched && !curHasEnriched) {
      seen.set(sig, it);
    }
  }
  return Array.from(seen.values());
}

// Diagnostics for formatted exports
function ensureLogsDir() {
  try {
    const dir = path.join(__dirname, '..', 'output', 'logs');
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    return dir;
  } catch (_) { return null; }
}

function gatherFieldStats(data) {
  const fields = ['patent_no','mail_to_add1','mail_to_zip','title','inventor_id','mod_user'];
  const stats = { total: data.length, from_item: {}, from_existing_record: {}, hydrated: {}, missing: {} };
  for (const f of fields) {
    stats.from_item[f] = 0;
    stats.from_existing_record[f] = 0;
    stats.hydrated[f] = 0;
    stats.missing[f] = 0;
  }
  const sample = [];
  let hydratedList = [];
  for (const it of data) {
    const edRoot = (it && it.enrichment_result && it.enrichment_result.enriched_data) ? it.enrichment_result.enriched_data : (it && it.enriched_data) || {};
    const existing = (edRoot && edRoot.existing_record) || {};
    for (const f of Object.keys(stats.from_item)) {
      const hasItem = it && it[f] != null && String(it[f]).trim() !== '';
      const hasExisting = existing && existing[f] != null && String(existing[f]).trim() !== '';
      hydratedList = Array.isArray(it._hydrated_fields) ? it._hydrated_fields : [];
      const wasHydrated = Array.isArray(hydratedList) && hydratedList.includes(f);
      if (hasItem) stats.from_item[f]++;
      if (hasExisting) stats.from_existing_record[f]++;
      if (wasHydrated) stats.hydrated[f]++;
      if (!(hasItem || hasExisting || wasHydrated)) stats.missing[f]++;
    }
    if (sample.length < 25) {
      sample.push({
        name: `${it.first_name || ''} ${it.last_name || ''}`.trim(),
        city: it.city || '', state: it.state || '',
        from_item: {
          patent_no: it.patent_no || '', mail_to_add1: it.mail_to_add1 || '', mail_to_zip: it.mail_to_zip || '', title: it.title || '', inventor_id: it.inventor_id || '', mod_user: it.mod_user || ''
        },
        from_existing_record: existing,
        hydrated_fields: Array.isArray(hydratedList) ? hydratedList : []
      });
    }
  }
  return { stats, sample };
}

function logFormattedExportDiagnostics(label, data) {
  try {
    const { stats, sample } = gatherFieldStats(data);
    console.log(`[export][diag] ${label} total=${stats.total} fields:`, stats);
    const dir = ensureLogsDir();
    if (dir) {
      const out = path.join(dir, `${label.replace(/\s+/g,'_').toLowerCase()}_formatted_diag.json`);
      fs.writeFileSync(out, JSON.stringify({ label, stats, sample, generated_at: new Date().toISOString() }, null, 2));
    }
  } catch (e) {
    console.warn('[export][diag] Failed to write diagnostics:', e.message);
  }
}

// Formatted exports
app.get('/api/export/current-enrichments-formatted', (req, res) => {
  try {
    const outPath = path.join(__dirname, '..', 'output', 'current_enrichments_formatted.csv');
    if (!fs.existsSync(outPath)) {
      return res.status(404).json({ error: 'current_enrichments_formatted.csv not found. Run Step 2 first.' });
    }
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="current_enrichments_formatted.csv"');
    fs.createReadStream(outPath).pipe(res);
  } catch (e) {
    console.error('Export current formatted failed:', e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/export/new-enrichments-formatted', (req, res) => {
  try {
    const outPath = path.join(__dirname, '..', 'output', 'new_enrichments_formatted.csv');
    if (!fs.existsSync(outPath)) {
      return res.status(404).json({ error: 'new_enrichments_formatted.csv not found. Run Step 2 first.' });
    }
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="new_enrichments_formatted.csv"');
    fs.createReadStream(outPath).pipe(res);
  } catch (e) {
    console.error('Export new formatted failed:', e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/export/new-and-existing-enrichments-formatted', (req, res) => {
  try {
    const outPath = path.join(__dirname, '..', 'output', 'new_and_existing_enrichments_formatted.csv');
    if (!fs.existsSync(outPath)) {
      return res.status(404).json({ error: 'new_and_existing_enrichments_formatted.csv not found. Run Step 2 first.' });
    }
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="new_and_existing_enrichments_formatted.csv"');
    fs.createReadStream(outPath).pipe(res);
  } catch (e) {
    console.error('Export new+existing formatted failed:', e);
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
    hydrateThreeFields(data, 'all_enrichments_formatted').then((hydrated) => {
      writeFormattedCsv(res, 'all_enrichments_formatted.csv', hydrated);
      logFormattedExportDiagnostics('all_enrichments', hydrated);
    }).catch((e) => {
      console.warn('[export] Hydration (3 fields) failed for all formatted, sending unhydrated:', e.message);
      writeFormattedCsv(res, 'all_enrichments_formatted.csv', data);
      logFormattedExportDiagnostics('all_enrichments_unhydrated', data);
    });
  } catch (e) {
    console.error('Export all formatted failed:', e);
    res.status(500).json({ error: e.message });
  }
});

// Contact CSV Exports (pre-formatted contact CSVs)
app.get('/api/export/contact-current', (req, res) => {
  try {
    const outPath = path.join(__dirname, '..', 'output', 'contact_current.csv');
    if (!fs.existsSync(outPath)) {
      return res.status(404).json({ error: 'contact_current.csv not found. Run Step 2 first.' });
    }
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="contact_current.csv"');
    fs.createReadStream(outPath).pipe(res);
  } catch (e) {
    console.error('Export contact current failed:', e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/export/contact-current-addresses', (req, res) => {
  try {
    const outPath = path.join(__dirname, '..', 'output', 'contact_current_addresses.csv');
    if (!fs.existsSync(outPath)) {
      return res.status(404).json({ error: 'contact_current_addresses.csv not found. Run Step 2 first.' });
    }
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="contact_current_addresses.csv"');
    fs.createReadStream(outPath).pipe(res);
  } catch (e) {
    console.error('Export contact current addresses failed:', e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/export/contact-new', (req, res) => {
  try {
    const outPath = path.join(__dirname, '..', 'output', 'contact_new.csv');
    if (!fs.existsSync(outPath)) {
      return res.status(404).json({ error: 'contact_new.csv not found. Run Step 2 first.' });
    }
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="contact_new.csv"');
    fs.createReadStream(outPath).pipe(res);
  } catch (e) {
    console.error('Export contact new failed:', e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/export/contact-new-addresses', (req, res) => {
  try {
    const outPath = path.join(__dirname, '..', 'output', 'contact_new_addresses.csv');
    if (!fs.existsSync(outPath)) {
      return res.status(404).json({ error: 'contact_new_addresses.csv not found. Run Step 2 first.' });
    }
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="contact_new_addresses.csv"');
    fs.createReadStream(outPath).pipe(res);
  } catch (e) {
    console.error('Export contact new addresses failed:', e);
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

// Get latest persisted Step 2 output (for refresh)
app.get('/api/step2/latest-output', (req, res) => {
    try {
        const outPath = path.join(__dirname, '..', 'output', 'last_step2_output.txt');
        let output = '';
        if (fs.existsSync(outPath)) {
            output = fs.readFileSync(outPath, 'utf8');
        }
        const files = {
            enrichedData: getFileStats('output/enriched_patents.json'),
            enrichedCsv: getFileStats('output/enriched_patents.csv'),
            enrichmentResults: getFileStats('output/enrichment_results.json')
        };
        return res.json({ success: true, output, files });
    } catch (e) {
        return res.status(500).json({ success: false, error: e.message });
    }
});

// Export all enrichments from SQL via Python helper
app.get('/api/export/all-enrichments', (req, res) => {
    try {
        const outPath = path.join(__dirname, '..', 'output', 'all_enrichments.csv');
        if (!fs.existsSync(outPath)) {
            return res.status(404).json({ error: 'all_enrichments.csv not found. Run Step 2 first.' });
        }
        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', 'attachment; filename="all_enrichments.csv"');
        fs.createReadStream(outPath).pipe(res);
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
