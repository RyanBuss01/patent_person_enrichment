#!/usr/bin/env node

/*
==============================================================
🚀 Patent Processing Environment Setup Script
==============================================================

----------------- MacOS --------------------

// Install Node.js (if not installed)
brew install node


// Install Git (usually pre-installed)
git --version 


----------------- Windows -----------------

# Install Chocolatey first
Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

# Install packages
choco install nodejs git -y
choco install python --version=3.9.19 -y  
choco install python --version=3.13.3 -y

# run setup.js with Node.js
node setup.js

# install pm2
npm install -g pm2

# start Server
pm2 start front-end/server.js --name "patent-pipeline"

# pm2 commands:
pm2 status
pm2 restart patent-pipeline
pm2 stop patent-pipeline
pm2 logs patent-pipeline

# run update push:
git fetch
git reset --hard origin/main 
git pull
cd front-end
npm install
pm2 restart patent-pipeline



URLS:
# Download node from https://nodejs.org/ (choose LTS version)
# Download Git from https://git-scm.com/download/win

---------------------------------------------




*/

const fs = require('fs');
const path = require('path');
const { execSync, spawn } = require('child_process');
const readline = require('readline'); // Removed - no user input needed

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

  // Removed getUserInput - no user interaction needed

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

    // Check if Python 3.9 is available, install if not
    this.checkAndInstallPython39();

    return true;
  }

  checkAndInstallPython39() {
    this.log('🔍 Checking for Python 3.9...', 'blue');
    
    try {
      const python39Version = execSync('python3.9 --version', { encoding: 'utf8' }).trim();
      this.success(`Python 3.9 found: ${python39Version}`);
      return true;
    } catch (error) {
      this.warn('Python 3.9 not found. Attempting to install...');
      return this.installPython39();
    }
  }

  installPython39() {
    const isWindows = process.platform === 'win32';
    const isMac = process.platform === 'darwin';
    const isLinux = process.platform === 'linux';

    try {
      if (isMac) {
        // macOS - use Homebrew
        this.info('Installing Python 3.9 via Homebrew...');
        let result = this.execCommand('brew --version', { silent: true });
        
        if (!result.success) {
          this.error('Homebrew not found. Please install Homebrew first or install Python 3.9 manually.');
          this.info('Install Homebrew: /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"');
          return false;
        }

        result = this.execCommand('brew install python@3.9');
        if (result.success) {
          // Create symlink if needed
          this.execCommand('brew link python@3.9 --force', { silent: true });
          this.success('Python 3.9 installed via Homebrew');
          return true;
        } else {
          this.warn('Homebrew installation failed, trying with pyenv...');
          return this.installPython39WithPyenv();
        }

      } else if (isWindows) {
        // Windows - use chocolatey or direct download
        this.info('Installing Python 3.9 on Windows...');
        
        // Try chocolatey first
        let result = this.execCommand('choco --version', { silent: true });
        if (result.success) {
          result = this.execCommand('choco install python --version=3.9.19 -y');
          if (result.success) {
            this.success('Python 3.9 installed via Chocolatey');
            return true;
          }
        }

        // If chocolatey fails, provide manual instructions
        this.warn('Automated installation failed on Windows.');
        this.info('Please install Python 3.9 manually from: https://www.python.org/downloads/release/python-3919/');
        this.info('Make sure to add Python to PATH and install for all users.');
        return false;

      } else if (isLinux) {
        // Linux - use apt/yum/dnf
        this.info('Installing Python 3.9 on Linux...');
        
        // Try apt (Ubuntu/Debian)
        let result = this.execCommand('apt --version', { silent: true });
        if (result.success) {
          result = this.execCommand('sudo apt update && sudo apt install -y python3.9 python3.9-venv python3.9-pip');
          if (result.success) {
            this.success('Python 3.9 installed via apt');
            return true;
          }
        }

        // Try yum (RHEL/CentOS)
        result = this.execCommand('yum --version', { silent: true });
        if (result.success) {
          result = this.execCommand('sudo yum install -y python39 python39-pip');
          if (result.success) {
            this.success('Python 3.9 installed via yum');
            return true;
          }
        }

        // Try dnf (Fedora)
        result = this.execCommand('dnf --version', { silent: true });
        if (result.success) {
          result = this.execCommand('sudo dnf install -y python3.9 python3.9-pip');
          if (result.success) {
            this.success('Python 3.9 installed via dnf');
            return true;
          }
        }

        this.warn('Automated installation failed on Linux. Trying pyenv...');
        return this.installPython39WithPyenv();

      } else {
        this.error(`Unsupported platform: ${process.platform}`);
        return false;
      }

    } catch (error) {
      this.error(`Failed to install Python 3.9: ${error.message}`);
      return false;
    }
  }

  installPython39WithPyenv() {
    this.info('Trying to install Python 3.9 with pyenv...');
    
    try {
      // Check if pyenv is installed
      let result = this.execCommand('pyenv --version', { silent: true });
      
      if (!result.success) {
        this.info('Installing pyenv first...');
        if (process.platform === 'darwin') {
          result = this.execCommand('brew install pyenv');
        } else {
          result = this.execCommand('curl https://pyenv.run | bash');
        }
        
        if (!result.success) {
          this.error('Failed to install pyenv. Please install Python 3.9 manually.');
          return false;
        }
      }

      // Install Python 3.9 with pyenv
      result = this.execCommand('pyenv install 3.9.19');
      if (!result.success) {
        this.error('Failed to install Python 3.9 with pyenv');
        return false;
      }

      // Set global or local version
      this.execCommand('pyenv global 3.9.19', { silent: true });
      
      this.success('Python 3.9 installed via pyenv');
      this.info('You may need to restart your terminal or run: source ~/.bashrc');
      return true;

    } catch (error) {
      this.error(`Pyenv installation failed: ${error.message}`);
      return false;
    }
  }

  setupProjectStructure() {
    this.log('📁 Setting up environment in current directory...', 'blue');

    // Use current directory instead of creating new folder
    this.projectPath = process.cwd();
    
    // Create missing folders only (don't overwrite existing)
    const foldersToCheck = [
      'logs',
      'converted_databases',
      'converted_databases/csv', 
      'matching_results',
      'output'
    ];

    foldersToCheck.forEach(folder => {
      const folderPath = path.join(this.projectPath, folder);
      if (!fs.existsSync(folderPath)) {
        fs.mkdirSync(folderPath, { recursive: true });
        this.log(`Created: ${folder}`);
      } else {
        this.log(`Exists: ${folder}`);
      }
    });

    // Check for existing structure
    const existingFolders = ['classes', 'runners', 'front-end'];
    existingFolders.forEach(folder => {
      if (fs.existsSync(path.join(this.projectPath, folder))) {
        this.log(`Found existing: ${folder}`);
      }
    });

    this.success(`Environment setup in: ${this.projectPath}`);
    return true;
  }

  createPackageJson() {
    this.log('📦 Checking package.json in front-end folder...', 'blue');

    const frontEndPath = path.join(this.projectPath, 'front-end');
    const packagePath = path.join(frontEndPath, 'package.json');
    
    if (fs.existsSync(packagePath)) {
      this.success('package.json already exists in front-end/');
      return;
    }

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

    fs.writeFileSync(packagePath, JSON.stringify(packageJson, null, 2));
    this.success('package.json created in front-end/');
  }

  createPythonRequirements() {
    this.log('🐍 Using existing requirements files from setup/...', 'blue');

    // Use setup folder requirements files directly - no copying needed
    const setupPatentReq = path.join(this.projectPath, 'setup', 'patent_env_requirements.txt');
    const setupScrapingReq = path.join(this.projectPath, 'setup', 'zaba_venv_requirements.txt');

    if (!fs.existsSync(setupPatentReq)) {
      this.error('setup/patent_env_requirements.txt not found');
      return false;
    }

    if (!fs.existsSync(setupScrapingReq)) {
      this.error('setup/zaba_venv_requirements.txt not found');
      return false;
    }
    
    this.success('Found requirements files in setup/ folder');
    return true;
  }

  async createPythonEnvironments() {
    this.log('🏗️  Creating Python virtual environments...', 'blue');

    const originalDir = process.cwd();
    process.chdir(this.projectPath);

    try {
      // Create patent_env with Python 3.9 (to match your working environment)
      this.info(`Creating ${CONFIG.venvs.patent} environment with Python 3.9...`);
      let result = this.execCommand('python3.9 -m venv patent_env');
      if (!result.success) {
        this.warn('Python 3.9 not found, trying with default python3');
        result = this.execCommand(`${CONFIG.pythonCommand} -m venv ${CONFIG.venvs.patent}`);
        if (!result.success) return false;
      }

      const patentPip = process.platform === 'win32'
        ? `${CONFIG.venvs.patent}\\Scripts\\pip3`
        : `${CONFIG.venvs.patent}/bin/pip3`;

      this.info('Installing patent environment packages...');
      result = this.execCommand(`${patentPip} install --upgrade pip`);
      if (!result.success) return false;

      // Use requirements file from setup folder directly
      result = this.execCommand(`${patentPip} install -r setup/patent_env_requirements.txt`);
      if (!result.success) {
        this.warn('Some patent packages failed to install. Continuing...');
      }

      // Create zaba_venv with Python 3.13 (to match your working scraping environment)  
      this.info(`Creating ${CONFIG.venvs.scraping} environment with Python 3.13...`);
      result = this.execCommand(`${CONFIG.pythonCommand} -m venv ${CONFIG.venvs.scraping}`);
      if (!result.success) return false;

      const scrapingPip = process.platform === 'win32'
        ? `${CONFIG.venvs.scraping}\\Scripts\\pip3`
        : `${CONFIG.venvs.scraping}/bin/pip3`;

      this.info('Installing scraping environment packages...');
      result = this.execCommand(`${scrapingPip} install --upgrade pip`);
      if (!result.success) return false;

      // Use requirements file from setup folder directly
      result = this.execCommand(`${scrapingPip} install -r setup/zaba_venv_requirements.txt`);
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

  // Removed - no unnecessary startup scripts needed

  createEnvironmentFile() {
    this.log('🔧 Checking .env file...', 'blue');

    const envPath = path.join(this.projectPath, '.env');
    
    if (fs.existsSync(envPath)) {
      this.success('.env file already exists - skipping');
      return;
    }

    // Create .env in setup folder as template, don't clutter root
    this.info('No .env found - you may need to create one with your database credentials');
    this.success('Environment check complete');
  }

  // Removed createReadme() - no extra documentation files needed

  createInitFiles() {
    this.log('📝 Checking Python package files...', 'blue');

    // Only create __init__.py files if they don't exist - minimal intervention
    const initContent = '# This file makes the directory a Python package\n';
    
    const classesInit = path.join(this.projectPath, 'classes', '__init__.py');
    if (!fs.existsSync(classesInit)) {
      fs.writeFileSync(classesInit, initContent);
      this.log('Created classes/__init__.py');
    }

    const runnersInit = path.join(this.projectPath, 'runners', '__init__.py');
    if (!fs.existsSync(runnersInit)) {
      fs.writeFileSync(runnersInit, initContent);
      this.log('Created runners/__init__.py');
    }

    // Check existing files but don't create templates
    if (fs.existsSync(path.join(this.projectPath, 'main.py'))) {
      this.success('main.py exists');
    }

    if (fs.existsSync(path.join(this.projectPath, 'front-end', 'server.js'))) {
      this.success('front-end/server.js exists');
    }

    this.success('Python package files checked');
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
      if (!this.setupProjectStructure()) {
        process.exit(1);
      }

      // Step 3: Check requirements and create package.json if needed
      if (!this.createPythonRequirements()) {
        process.exit(1);
      }
      this.createPackageJson();
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

      // Step 6: Create minimal Python package files
      this.createInitFiles();

      // Step 7: Generate summary (removed README creation)
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