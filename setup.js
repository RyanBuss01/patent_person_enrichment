#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { execSync, spawn } = require('child_process');
const readline = require('readline');

// Configuration
const CONFIG = {
  projectName: 'Patent_Processing_Environment',
  nodeVersion: 'v22.12.0',
  pythonCommand: 'python3',
  pipCommand: 'pip3',
  venvs: {
    patent: 'patent_env',
    scraping: 'zaba_venv'
  }
};

// Color codes for logging
const colors = {
  reset: '\x1b[0m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  bold: '\x1b[1m'
};

class EnvironmentSetup {
  constructor() {
    this.projectPath = '';
    this.logMessages = [];
    this.startTime = Date.now();
  }

  log(message, color = 'reset') {
    const timestamp = new Date().toLocaleTimeString();
    const coloredMessage = `${colors[color]}[${timestamp}] ${message}${colors.reset}`;
    console.log(coloredMessage);
    this.logMessages.push(`[${timestamp}] ${message}`);
  }

  error(message) {
    this.log(`❌ ERROR: ${message}`, 'red');
  }

  success(message) {
    this.log(`✅ ${message}`, 'green');
  }

  info(message) {
    this.log(`ℹ️  ${message}`, 'blue');
  }

  warn(message) {
    this.log(`⚠️  ${message}`, 'yellow');
  }

  execCommand(command, options = {}) {
    try {
      this.log(`Running: ${command}`, 'cyan');
      const result = execSync(command, { 
        stdio: options.silent ? 'pipe' : 'inherit',
        encoding: 'utf8',
        ...options 
      });
      return { success: true, output: result };
    } catch (error) {
      this.error(`Command failed: ${command}`);
      this.error(error.message);
      return { success: false, error: error.message };
    }
  }

  async getUserInput(question) {
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout
    });

    return new Promise((resolve) => {
      rl.question(question, (answer) => {
        rl.close();
        resolve(answer.trim());
      });
    });
  }

  checkPrerequisites() {
    this.log('🔍 Checking prerequisites...', 'blue');
    
    // Check Node.js
    try {
      const nodeVersion = execSync('node --version', { encoding: 'utf8' }).trim();
      this.success(`Node.js found: ${nodeVersion}`);
    } catch (error) {
      this.error('Node.js not found. Please install Node.js first.');
      return false;
    }

    // Check Python3
    try {
      const pythonVersion = execSync(`${CONFIG.pythonCommand} --version`, { encoding: 'utf8' }).trim();
      this.success(`Python found: ${pythonVersion}`);
    } catch (error) {
      this.error('Python3 not found. Please install Python3 first.');
      return false;
    }

    // Check pip3
    try {
      const pipVersion = execSync(`${CONFIG.pipCommand} --version`, { encoding: 'utf8' }).trim();
      this.success(`Pip found: ${pipVersion}`);
    } catch (error) {
      this.error('pip3 not found. Please install pip3 first.');
      return false;
    }

    return true;
  }

  async setupProjectStructure() {
    this.log('📁 Setting up project structure...', 'blue');

    const projectName = await this.getUserInput(`Enter project folder name [${CONFIG.projectName}]: `) || CONFIG.projectName;
    this.projectPath = path.resolve(projectName);

    if (fs.existsSync(this.projectPath)) {
      const overwrite = await this.getUserInput(`Directory ${projectName} exists. Overwrite? [y/N]: `);
      if (overwrite.toLowerCase() !== 'y') {
        this.error('Setup cancelled.');
        process.exit(1);
      }
      fs.rmSync(this.projectPath, { recursive: true, force: true });
    }

    // Create project structure
    const folders = [
      '',
      'classes',
      'runners', 
      'front-end',
      'front-end/public',
      'front-end/views',
      'setup',
      'logs',
      'converted_databases',
      'converted_databases/csv',
      'matching_results',
      'output'
    ];

    folders.forEach(folder => {
      const folderPath = path.join(this.projectPath, folder);
      fs.mkdirSync(folderPath, { recursive: true });
      this.log(`Created: ${folder || 'root'}`);
    });

    this.success(`Project structure created at: ${this.projectPath}`);
    return true;
  }

  createPackageJson() {
    this.log('📦 Creating package.json in front-end folder...', 'blue');

    const packageJson = {
      "name": "patent-processing-frontend",
      "version": "1.0.0",
      "description": "Frontend interface for patent processing pipeline",
      "main": "server.js",
      "scripts": {
        "start": "node server.js",
        "dev": "nodemon server.js"
      },
      "dependencies": {
        "dotenv": "^17.2.1",
        "express": "^4.18.2",
        "mysql2": "^3.14.5"
      },
      "devDependencies": {
        "nodemon": "^3.0.1"
      },
      "keywords": [
        "patent",
        "processing",
        "pipeline",
        "frontend"
      ],
      "author": "Patent Processing Team",
      "license": "MIT"
    };

    const packagePath = path.join(this.projectPath, 'front-end', 'package.json');
    fs.writeFileSync(packagePath, JSON.stringify(packageJson, null, 2));
    this.success('package.json created in front-end/');
  }

  createPythonRequirements() {
    this.log('🐍 Creating Python requirements files...', 'blue');

    // Patent environment requirements (main processing)
    const patentRequirements = `annotated-types==0.7.0
beautifulsoup4==4.13.5
bs4==0.0.2
certifi==2025.8.3
charset-normalizer==3.4.3
DateTime==5.5
dnspython==2.7.0
email-validator==2.3.0
et_xmlfile==2.0.0
Flask==3.0.2
fuzzywuzzy==0.18.0
idna==3.10
Jinja2==3.1.4
Levenshtein==0.27.1
lxml==6.0.1
matplotlib==3.8.4
mysql-connector-python==9.4.0
mysqlclient==2.2.7
numpy==2.0.2
openpyxl==3.1.5
pandas==2.3.2
pandas-access==0.0.1
pathlib==1.0.1
peopledatalabs==6.4.3
pillow==11.3.0
pydantic==2.11.7
pydantic_core==2.33.2
python-dateutil==2.9.0.post0
python-dotenv==1.1.1
python-Levenshtein==0.27.1
pytz==2025.2
RapidFuzz==3.13.0
requests==2.32.5
seaborn==0.13.2
six==1.17.0
typing==3.7.4.3
typing-inspection==0.4.1
typing_extensions==4.15.0
tzdata==2025.2
urllib3==1.26.18
Werkzeug==3.1.3
zope.interface==7.2`;

    // Scraping environment requirements (lighter)
    const scrapingRequirements = `beautifulsoup4==4.13.5
bs4==0.0.2
certifi==2025.8.3
charset-normalizer==3.4.3
idna==3.10
requests==2.32.5
soupsieve==2.8
typing_extensions==4.15.0
urllib3==2.5.0`;

    // Write requirements files
    fs.writeFileSync(path.join(this.projectPath, 'requirements_patent.txt'), patentRequirements);
    fs.writeFileSync(path.join(this.projectPath, 'requirements_scraping.txt'), scrapingRequirements);
    
    this.success('Python requirements files created');
  }

  async createPythonEnvironments() {
    this.log('🏗️  Creating Python virtual environments...', 'blue');

    const originalDir = process.cwd();
    process.chdir(this.projectPath);

    try {
      // Create patent_env (Python 3.9.6 compatible)
      this.info(`Creating ${CONFIG.venvs.patent} environment...`);
      let result = this.execCommand(`${CONFIG.pythonCommand} -m venv ${CONFIG.venvs.patent}`);
      if (!result.success) return false;

      // Activate and install patent requirements
      const patentActivate = process.platform === 'win32' 
        ? `${CONFIG.venvs.patent}\\Scripts\\activate` 
        : `source ${CONFIG.venvs.patent}/bin/activate`;
      
      const patentPip = process.platform === 'win32'
        ? `${CONFIG.venvs.patent}\\Scripts\\pip3`
        : `${CONFIG.venvs.patent}/bin/pip3`;

      this.info('Installing patent environment packages...');
      result = this.execCommand(`${patentPip} install --upgrade pip`);
      if (!result.success) return false;

      result = this.execCommand(`${patentPip} install -r requirements_patent.txt`);
      if (!result.success) {
        this.warn('Some patent packages failed to install. Continuing...');
      }

      // Create zaba_venv (lighter scraping environment)
      this.info(`Creating ${CONFIG.venvs.scraping} environment...`);
      result = this.execCommand(`${CONFIG.pythonCommand} -m venv ${CONFIG.venvs.scraping}`);
      if (!result.success) return false;

      const scrapingPip = process.platform === 'win32'
        ? `${CONFIG.venvs.scraping}\\Scripts\\pip3`
        : `${CONFIG.venvs.scraping}/bin/pip3`;

      this.info('Installing scraping environment packages...');
      result = this.execCommand(`${scrapingPip} install --upgrade pip`);
      if (!result.success) return false;

      result = this.execCommand(`${scrapingPip} install -r requirements_scraping.txt`);
      if (!result.success) {
        this.warn('Some scraping packages failed to install. Continuing...');
      }

      this.success('Python virtual environments created successfully');
      return true;

    } catch (error) {
      this.error(`Failed to create Python environments: ${error.message}`);
      return false;
    } finally {
      process.chdir(originalDir);
    }
  }

  installNodeDependencies() {
    this.log('📦 Installing Node.js dependencies in front-end/...', 'blue');

    const originalDir = process.cwd();
    const frontendDir = path.join(this.projectPath, 'front-end');
    process.chdir(frontendDir);

    try {
      const result = this.execCommand('npm install');
      if (result.success) {
        this.success('Node.js dependencies installed successfully in front-end/');
        return true;
      }
      return false;
    } finally {
      process.chdir(originalDir);
    }
  }

  createStartupScripts() {
    this.log('🚀 Creating startup scripts...', 'blue');

    // Cross-platform startup script for development
    const startDevScript = `#!/usr/bin/env node

const { spawn, execSync } = require('child_process');
const path = require('path');

console.log('🚀 Starting Patent Processing Development Environment...');

// Function to activate virtual environment and run command
function runWithVenv(venvName, command, args = []) {
  const isWindows = process.platform === 'win32';
  const venvPath = path.join(__dirname, venvName);
  const pythonPath = isWindows 
    ? path.join(venvPath, 'Scripts', 'python') 
    : path.join(venvPath, 'bin', 'python3');

  console.log(\`📍 Using \${venvName}: \${pythonPath}\`);
  
  const proc = spawn(pythonPath, [command, ...args], {
    stdio: 'inherit',
    env: { 
      ...process.env,
      VIRTUAL_ENV: venvPath,
      PATH: isWindows 
        ? \`\${path.join(venvPath, 'Scripts')};\${process.env.PATH}\`
        : \`\${path.join(venvPath, 'bin')}:\${process.env.PATH}\`
    }
  });

  return proc;
}

// Command line arguments
const command = process.argv[2];

switch(command) {
  case 'frontend':
  case 'web':
    console.log('🌐 Starting frontend server...');
    process.chdir('front-end');
    spawn('npm', ['run', 'dev'], { stdio: 'inherit' });
    break;

  case 'python':
  case 'main':
    console.log('🐍 Starting Python main pipeline...');
    runWithVenv('patent_env', 'main.py');
    break;

  case 'scraping':
    console.log('🕷️  Starting scraping environment...');
    runWithVenv('zaba_venv', process.argv[3] || 'python3');
    break;

  case 'both':
    console.log('🔄 Starting both frontend and Python...');
    process.chdir('front-end');
    spawn('npm', ['run', 'dev'], { stdio: 'inherit' });
    process.chdir('..');
    setTimeout(() => {
      runWithVenv('patent_env', 'main.py');
    }, 2000);
    break;

  default:
    console.log(\`
🎯 Usage: node start.js [command]

Commands:
  frontend, web    - Start the Express frontend server
  python, main     - Run the Python main pipeline
  scraping         - Start scraping environment
  both             - Start both frontend and Python
  
Examples:
  node start.js frontend
  node start.js python  
  node start.js both
\`);
}`;

    fs.writeFileSync(path.join(this.projectPath, 'start.js'), startDevScript);

    // Create environment activation helpers
    const activatePatent = process.platform === 'win32' 
      ? `@echo off\ncall patent_env\\Scripts\\activate.bat\ncmd /k`
      : `#!/bin/bash\nsource patent_env/bin/activate\nexec "$SHELL"`;

    const activateScraping = process.platform === 'win32'
      ? `@echo off\ncall zaba_venv\\Scripts\\activate.bat\ncmd /k`
      : `#!/bin/bash\nsource zaba_venv/bin/activate\nexec "$SHELL"`;

    const patentFile = process.platform === 'win32' ? 'activate_patent.bat' : 'activate_patent.sh';
    const scrapingFile = process.platform === 'win32' ? 'activate_scraping.bat' : 'activate_scraping.sh';

    fs.writeFileSync(path.join(this.projectPath, patentFile), activatePatent);
    fs.writeFileSync(path.join(this.projectPath, scrapingFile), activateScraping);

    // Make shell scripts executable on Unix systems
    if (process.platform !== 'win32') {
      try {
        execSync(`chmod +x "${path.join(this.projectPath, 'start.js')}"`);
        execSync(`chmod +x "${path.join(this.projectPath, 'activate_patent.sh')}"`);
        execSync(`chmod +x "${path.join(this.projectPath, 'activate_scraping.sh')}"`);
      } catch (error) {
        this.warn('Could not make scripts executable');
      }
    }

    this.success('Startup scripts created');
  }

  createEnvironmentFile() {
    this.log('🔧 Creating environment configuration...', 'blue');

    const envTemplate = `# Database Configuration
DB_ENGINE=mysql
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=password
DB_NAME=patent_data

# Cloud Database (Optional)
CLOUD_DB_HOST=mysql-patent-nationalengravers.mysql.database.azure.com
CLOUD_DB_USER=rootuser
CLOUD_DB_PASSWORD=S3cur3Adm1n1124!
CLOUD_DB_NAME=patent_data

# API Keys
PEOPLEDATALABS_API_KEY=your_pdl_api_key_here

# File Paths
CSV_DATABASE_FOLDER=converted_databases/csv
OUTPUT_FOLDER=output
LOGS_FOLDER=logs

# Processing Options
BATCH_SIZE=1000
ENABLE_LOGGING=true
LOG_LEVEL=INFO

# Frontend Configuration  
PORT=3000
NODE_ENV=development`;

    fs.writeFileSync(path.join(this.projectPath, '.env.example'), envTemplate);
    fs.writeFileSync(path.join(this.projectPath, '.env'), envTemplate);

    this.success('Environment files created');
  }

  createReadme() {
    this.log('📚 Creating documentation...', 'blue');

    const readme = `# Patent Processing Pipeline

Complete patent processing environment with dual Python virtual environments and Express frontend.

## Quick Start

1. **Configure environment:**
   \`\`\`bash
   cp .env.example .env
   # Edit .env with your database credentials and API keys
   \`\`\`

2. **Install frontend dependencies:**
   \`\`\`bash
   cd front-end
   npm install
   cd ..
   \`\`\`

3. **Start the application:**
   \`\`\`bash
   # Start frontend server
   node start.js frontend

   # Start Python pipeline  
   node start.js python

   # Start both together
   node start.js both
   \`\`\`

## Project Structure

\`\`\`
Patent_Processing_Environment/
├── start.js                        # Unified startup script
├── main.py                         # Python main orchestrator
├── requirements_patent.txt         # Patent processing dependencies
├── requirements_scraping.txt       # Web scraping dependencies
├── .env                            # Environment variables
│
├── patent_env/                     # Main Python environment
├── zaba_venv/                      # Scraping Python environment
│
├── front-end/                      # Express.js web interface
│   ├── package.json                # Node.js dependencies (frontend only)
│   ├── server.js                   # Main web server
│   ├── public/                     # Static assets
│   └── views/                      # HTML templates
│
├── classes/                        # Python data processing classes
├── runners/                        # Python pipeline steps
├── converted_databases/            # Access database conversions
├── matching_results/               # Processing outputs
└── logs/                          # Application logs
\`\`\`

## Python Virtual Environments

### Patent Environment (\`patent_env\`)
Main processing environment with full dependencies:
- Patent XML parsing
- Data enrichment (PeopleDataLabs)
- Database operations (MySQL)
- Data analysis (pandas, numpy)
- Excel processing (openpyxl)

### Scraping Environment (\`zaba_venv\`)
Lightweight environment for web scraping:
- Web scraping (BeautifulSoup, requests)
- Data extraction
- Minimal dependencies

## Manual Activation

### Activate Patent Environment:
\`\`\`bash
# Linux/Mac
source patent_env/bin/activate

# Windows  
patent_env\\Scripts\\activate
\`\`\`

### Activate Scraping Environment:
\`\`\`bash
# Linux/Mac
source zaba_venv/bin/activate

# Windows
zaba_venv\\Scripts\\activate
\`\`\`

## Available Scripts

### From project root:
- \`node start.js frontend\` - Start web interface
- \`node start.js python\` - Run Python processing
- \`node start.js both\` - Start both services

### From front-end/ folder:
- \`npm start\` - Start frontend server
- \`npm run dev\` - Start frontend with nodemon

### Python environments:
- \`python3 main.py\` - Run main pipeline (with patent_env activated)

## Database Setup

1. Install MySQL locally or use cloud instance
2. Update \`.env\` with your database credentials
3. The application will create necessary tables automatically

## Development

- Frontend runs on http://localhost:3000
- Node.js environment is isolated in \`front-end/\` folder
- Python logs are written to \`logs/\` folder
- Processing outputs go to \`matching_results/\` and \`output/\`

## Environment Variables

Key variables in \`.env\`:
- \`DB_*\` - Database connection settings
- \`PEOPLEDATALABS_API_KEY\` - API key for data enrichment
- \`PORT\` - Frontend server port
- \`LOG_LEVEL\` - Logging verbosity

## Troubleshooting

1. **Python package issues:** Try upgrading pip in each venv
2. **MySQL connection errors:** Check database credentials in \`.env\`  
3. **Permission errors:** Ensure scripts are executable with \`chmod +x\`
4. **Node modules missing:** \`cd front-end && npm install\`
5. **Frontend startup issues:** Make sure you're in the right directory

For more help, check the logs in the \`logs/\` folder.`;

    fs.writeFileSync(path.join(this.projectPath, 'README.md'), readme);
    this.success('README.md created');
  }

  createInitFiles() {
    this.log('📝 Creating Python package files...', 'blue');

    // Create __init__.py files to make folders Python packages
    const initContent = '# This file makes the directory a Python package\n';
    
    fs.writeFileSync(path.join(this.projectPath, 'classes', '__init__.py'), initContent);
    fs.writeFileSync(path.join(this.projectPath, 'runners', '__init__.py'), initContent);

    // Create basic main.py template
    const mainPyTemplate = `#!/usr/bin/env python3
"""
Patent Processing Pipeline - Main Orchestrator
Created by environment setup script
"""

import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

def main():
    print("🚀 Patent Processing Pipeline Starting...")
    print(f"📁 Project root: {project_root}")
    print(f"🐍 Python version: {sys.version}")
    
    # TODO: Import and run your pipeline steps
    # from runners.integrate_existing_data import run_existing_data_integration
    # from runners.extract_patents import run_patent_extraction
    # from runners.enrich import run_enrichment
    
    print("✅ Setup complete! Add your pipeline logic here.")

if __name__ == "__main__":
    main()
`;

    fs.writeFileSync(path.join(this.projectPath, 'main.py'), mainPyTemplate);

    // Create basic Express server template
    const serverTemplate = `const express = require('express');
const path = require('path');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Routes
app.get('/', (req, res) => {
    res.send(\`
    <h1>🚀 Patent Processing Pipeline</h1>
    <p>Frontend server is running!</p>
    <p>Environment: \${process.env.NODE_ENV || 'development'}</p>
    <p>Database: \${process.env.DB_ENGINE || 'mysql'}</p>
    \`);
});

app.get('/health', (req, res) => {
    res.json({ 
        status: 'healthy', 
        timestamp: new Date().toISOString(),
        environment: process.env.NODE_ENV || 'development'
    });
});

// Start server
app.listen(PORT, () => {
    console.log(\`🌐 Frontend server running on http://localhost:\${PORT}\`);
    console.log(\`📊 Health check: http://localhost:\${PORT}/health\`);
});
`;

    fs.writeFileSync(path.join(this.projectPath, 'front-end', 'server.js'), serverTemplate);

    this.success('Template files created');
  }

  generateSummary() {
    const duration = Math.round((Date.now() - this.startTime) / 1000);
    
    this.log('\n' + '='.repeat(60), 'cyan');
    this.log('🎉 ENVIRONMENT SETUP COMPLETE!', 'green');
    this.log('='.repeat(60), 'cyan');
    this.log(`📁 Project Location: ${this.projectPath}`, 'blue');
    this.log(`⏱️  Setup Duration: ${duration} seconds`, 'blue');
    this.log('='.repeat(60), 'cyan');
    
    console.log(`
${colors.green}Next Steps:${colors.reset}

1. ${colors.cyan}Navigate to project:${colors.reset}
   cd "${this.projectPath}"

2. ${colors.cyan}Configure environment:${colors.reset}
   Edit .env file with your database credentials and API keys

3. ${colors.cyan}Start development:${colors.reset}
   node start.js both     # Start frontend + Python
   node start.js frontend # Just frontend
   node start.js python   # Just Python pipeline

4. ${colors.cyan}Access your app:${colors.reset}
   http://localhost:3000

${colors.yellow}Virtual Environments Created:${colors.reset}
📦 patent_env  - Main processing (${colors.green}${this.getPackageCount('patent')} packages${colors.reset})
📦 zaba_venv   - Web scraping (${colors.green}${this.getPackageCount('scraping')} packages${colors.reset})

${colors.blue}Manual Activation:${colors.reset}
source patent_env/bin/activate    # Main environment
source zaba_venv/bin/activate     # Scraping environment
`);
  }

  getPackageCount(type) {
    try {
      const file = type === 'patent' ? 'requirements_patent.txt' : 'requirements_scraping.txt';
      const content = fs.readFileSync(path.join(this.projectPath, file), 'utf8');
      return content.split('\n').filter(line => line.trim() && !line.startsWith('#')).length;
    } catch {
      return 'unknown';
    }
  }

  async run() {
    console.log(`${colors.bold}${colors.blue}
╔══════════════════════════════════════════════════════════════╗
║                 🚀 Patent Processing Environment Setup       ║
║                        Complete Installation                 ║
╚══════════════════════════════════════════════════════════════╝
${colors.reset}`);

    try {
      // Step 1: Check prerequisites
      if (!this.checkPrerequisites()) {
        process.exit(1);
      }

      // Step 2: Setup project structure  
      if (!await this.setupProjectStructure()) {
        process.exit(1);
      }

      // Step 3: Create configuration files
      this.createPackageJson();
      this.createPythonRequirements();
      this.createEnvironmentFile();

      // Step 4: Create Python virtual environments
      if (!await this.createPythonEnvironments()) {
        this.error('Failed to create Python environments');
        process.exit(1);
      }

      // Step 5: Install Node.js dependencies
      if (!this.installNodeDependencies()) {
        this.error('Failed to install Node.js dependencies');
        process.exit(1);
      }

      // Step 6: Create startup scripts and documentation
      this.createStartupScripts();
      this.createInitFiles();
      this.createReadme();

      // Step 7: Generate summary
      this.generateSummary();

      // Save setup log
      const logPath = path.join(this.projectPath, 'logs', 'setup.log');
      fs.writeFileSync(logPath, this.logMessages.join('\n'));
      this.log(`📋 Setup log saved: ${logPath}`, 'blue');

    } catch (error) {
      this.error(`Setup failed: ${error.message}`);
      console.error(error.stack);
      process.exit(1);
    }
  }
}

// Run the setup
const setup = new EnvironmentSetup();
setup.run().catch(console.error);