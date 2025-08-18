#!/usr/bin/env python3
# =============================================================================
# main.py - Enhanced with Detailed Logging
# Complete Patent Processing Pipeline Orchestrator
# =============================================================================
import logging
import os
from datetime import datetime
from typing import Dict, Any
from dotenv import load_dotenv
import pandas as pd
import json

# Load environment variables from .env file
load_dotenv()

# Import all runner functions
from runners.integrate_existing_data import run_existing_data_integration
from runners.extract_patents import run_patent_extraction
from runners.enrich import run_enrichment
from runners.integrate_dynamics import run_dynamics_integration
from runners.automate_email import run_email_automation
from runners.monitor_report import run_monitoring_report

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_configuration() -> Dict[str, Any]:
    """Load configuration parameters for the entire pipeline"""
    return {
        # Step 0: Existing Data Integration Configuration
        'ACCESS_DB_PATH': os.getenv('ACCESS_DB_PATH', "patent_system/Database.mdb"),
        'USPC_DOWNLOAD_PATH': os.getenv('USPC_DOWNLOAD_PATH', "USPC_Download"),
        # FIXED: Point to the correct CSV folder
        'CSV_DATABASE_FOLDER': "converted_databases/csv",
        'USE_EXISTING_DATA': os.getenv('USE_EXISTING_DATA', 'true').lower() == 'true',
        'ENRICH_ONLY_NEW_PEOPLE': os.getenv('ENRICH_ONLY_NEW_PEOPLE', 'true').lower() == 'true',
        'MAX_ENRICHMENT_COST': int(os.getenv('MAX_ENRICHMENT_COST', '1000')),
        
        # Step 1: Patent Extraction Configuration
        'PATENTSVIEW_API_KEY': os.getenv('PATENTSVIEW_API_KEY', "oq371zFI.BjeAbayJsdHdvEgbei0vskz5bTK3KM1S"),
        'EXTRACT_BY_DATE': os.getenv('EXTRACT_BY_DATE', 'true').lower() == 'true',
        'DAYS_BACK': int(os.getenv('DAYS_BACK', '7')),
        'CPC_CODES': ['H04', 'G06'],  # Technology areas (electronics, computing)
        'MAX_RESULTS': int(os.getenv('MAX_RESULTS', '1000')),
        
        # Step 2: Data Enrichment Configuration
        'PEOPLEDATALABS_API_KEY': os.getenv('PEOPLEDATALABS_API_KEY', "YOUR_PDL_API_KEY"),
        'XML_FILE_PATH': "ipg250812.xml",
        
        # Step 3: Dynamics CRM Configuration
        'DYNAMICS_TENANT_ID': os.getenv('DYNAMICS_TENANT_ID', "YOUR_TENANT_ID"),
        'DYNAMICS_CLIENT_ID': os.getenv('DYNAMICS_CLIENT_ID', "YOUR_CLIENT_ID"),
        'DYNAMICS_CLIENT_SECRET': os.getenv('DYNAMICS_CLIENT_SECRET', "YOUR_CLIENT_SECRET"),
        'DYNAMICS_CRM_URL': os.getenv('DYNAMICS_CRM_URL', "https://yourorg.crm.dynamics.com"),
        
        # Step 4: Email Automation Configuration
        'SMTP_SERVER': os.getenv('SMTP_SERVER', "smtp.gmail.com"),
        'SMTP_PORT': int(os.getenv('SMTP_PORT', '587')),
        'SENDER_EMAIL': os.getenv('SENDER_EMAIL', "your.email@company.com"),
        'EMAIL_PASSWORD': os.getenv('EMAIL_PASSWORD', "your_app_password"),
        'EMAIL_TEMPLATE_PATH': os.getenv('EMAIL_TEMPLATE_PATH', "templates/outreach_email.txt"),
        'EMAIL_DELAY_SECONDS': int(os.getenv('EMAIL_DELAY_SECONDS', '2')),
        'SENDER_NAME': os.getenv('SENDER_NAME', "Your Name"),
        'SENDER_TITLE': os.getenv('SENDER_TITLE', "Business Development Manager"),
        'COMPANY_NAME': os.getenv('COMPANY_NAME', "Your Company"),
        'SENDER_PHONE': os.getenv('SENDER_PHONE', "+1-555-123-4567"),
        
        # Step 5: Reporting Configuration
        'REPORTS_OUTPUT_DIR': os.getenv('REPORTS_OUTPUT_DIR', 'reports'),
        
        # General Configuration
        'OUTPUT_DIR': os.getenv('OUTPUT_DIR', 'output'),
        'OUTPUT_CSV': "output/enriched_patents.csv",
        'OUTPUT_JSON': "output/enriched_patents.json"
    }

def debug_folder_structure():
    """Debug function to show exact folder structure"""
    print("\n🔍 DEBUG: FOLDER STRUCTURE CHECK")
    print("=" * 50)
    
    # Check current directory
    current_dir = os.getcwd()
    print(f"📁 Current directory: {current_dir}")
    
    # Check main folder
    main_folder = "converted_databases"
    print(f"📁 '{main_folder}' exists: {os.path.exists(main_folder)}")
    if os.path.exists(main_folder):
        contents = os.listdir(main_folder)
        print(f"   Contents: {contents}")
        
        # Check CSV subfolder
        csv_subfolder = os.path.join(main_folder, "csv")
        print(f"📁 '{csv_subfolder}' exists: {os.path.exists(csv_subfolder)}")
        if os.path.exists(csv_subfolder):
            csv_files = [f for f in os.listdir(csv_subfolder) if f.endswith('.csv')]
            print(f"   CSV files in subfolder: {len(csv_files)}")
            if csv_files:
                print(f"   First 3 CSV files: {csv_files[:3]}")
    
    # Double-check with the exact path from config
    config_path = "converted_databases/csv"
    print(f"📁 Config path '{config_path}' exists: {os.path.exists(config_path)}")
    if os.path.exists(config_path):
        csv_files = [f for f in os.listdir(config_path) if f.endswith('.csv')]
        print(f"   CSV files found: {len(csv_files)}")

def log_csv_database_inspection(config: Dict[str, Any]):
    """Log inspection of CSV databases before integration"""
    logger.info("🔍 INSPECTING CSV DATABASES")
    logger.info("=" * 50)
    
    # Get the correct config value
    csv_folder = config.get('CSV_DATABASE_FOLDER')
    logger.info(f"🔍 Config CSV_DATABASE_FOLDER value: {csv_folder}")
    
    # Use fallback only if None
    if not csv_folder:
        csv_folder = "converted_databases/csv"
    
    logger.info(f"🔍 Looking for CSV files in: {csv_folder}")
    
    if not os.path.exists(csv_folder):
        logger.warning(f"❌ CSV database folder not found: {csv_folder}")
        return
    
    csv_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]
    
    if not csv_files:
        logger.warning(f"❌ No CSV files found in {csv_folder}")
        return
    
    logger.info(f"📁 Found {len(csv_files)} CSV database files:")
    
    for i, csv_file in enumerate(csv_files[:3]):  # Show first 3 files
        try:
            file_path = os.path.join(csv_folder, csv_file)
            df = pd.read_csv(file_path, nrows=1000)  # Read first 1000 rows for inspection
            
            logger.info(f"\n📄 File {i+1}: {csv_file}")
            logger.info(f"   📏 Rows: {len(df):,} (showing first 1000)")
            logger.info(f"   📊 Columns: {len(df.columns)} - {list(df.columns)[:5]}{'...' if len(df.columns) > 5 else ''}")
            
            # Show first few rows
            logger.info(f"   🔎 Sample data (first 3 rows):")
            for idx, row in df.head(3).iterrows():
                row_preview = {k: str(v)[:50] + ('...' if len(str(v)) > 50 else '') 
                             for k, v in row.to_dict().items()}
                logger.info(f"      Row {idx}: {row_preview}")
            
            # Check for common patent/people columns
            patent_columns = [col for col in df.columns if any(term in col.lower() 
                            for term in ['patent', 'number', 'id', 'publication'])]
            people_columns = [col for col in df.columns if any(term in col.lower() 
                            for term in ['first', 'last', 'name', 'inventor', 'assignee'])]
            
            if patent_columns:
                logger.info(f"   📋 Patent columns found: {patent_columns}")
            if people_columns:
                logger.info(f"   👥 People columns found: {people_columns}")
                
        except Exception as e:
            logger.error(f"   ❌ Error reading {csv_file}: {e}")
    
    if len(csv_files) > 3:
        logger.info(f"\n📁 ... and {len(csv_files) - 3} more CSV files")

def log_xml_data_inspection(xml_patents: list):
    """Log inspection of XML patent data"""
    logger.info("\n🔍 INSPECTING XML PATENT DATA")
    logger.info("=" * 50)
    
    if not xml_patents:
        logger.warning("❌ No XML patent data found")
        return
    
    logger.info(f"📊 Total XML patents loaded: {len(xml_patents):,}")
    
    # Count total people
    total_inventors = sum(len(p.get('inventors', [])) for p in xml_patents)
    total_assignees = sum(len(p.get('assignees', [])) for p in xml_patents)
    total_people = total_inventors + total_assignees
    
    logger.info(f"👥 Total people in XML data:")
    logger.info(f"   🔬 Inventors: {total_inventors:,}")
    logger.info(f"   🏢 Assignees: {total_assignees:,}")
    logger.info(f"   🤝 Total: {total_people:,}")
    
    # Show sample patents
    logger.info(f"\n🔎 Sample XML patents (first 3):")
    for i, patent in enumerate(xml_patents[:3]):
        logger.info(f"   Patent {i+1}:")
        logger.info(f"      📋 Number: {patent.get('patent_number', 'Unknown')}")
        logger.info(f"      📝 Title: {patent.get('patent_title', 'Unknown')[:100]}{'...' if len(patent.get('patent_title', '')) > 100 else ''}")
        logger.info(f"      👨‍🔬 Inventors: {len(patent.get('inventors', []))}")
        logger.info(f"      🏢 Assignees: {len(patent.get('assignees', []))}")
        
        # Show sample people
        if patent.get('inventors'):
            inv = patent['inventors'][0]
            logger.info(f"      📍 Sample inventor: {inv.get('first_name', '')} {inv.get('last_name', '')} from {inv.get('city', 'Unknown')}, {inv.get('state', 'Unknown')}")

def log_integration_results(result: Dict[str, Any]):
    """Log detailed integration results"""
    logger.info("\n🔍 INTEGRATION RESULTS ANALYSIS")
    logger.info("=" * 50)
    
    if not result.get('success'):
        logger.error(f"❌ Integration failed: {result.get('error')}")
        return
    
    logger.info(f"📊 DUPLICATE DETECTION RESULTS:")
    logger.info(f"   🗃️  Existing patents in DB: {result.get('existing_patents_count', 0):,}")
    logger.info(f"   👥 Existing people in DB: {result.get('existing_people_count', 0):,}")
    logger.info(f"   📋 Total XML patents: {result.get('total_xml_patents', 0):,}")
    logger.info(f"   👨‍👩‍👧‍👦 Total XML people: {result.get('total_xml_people', 0):,}")
    
    logger.info(f"\n💡 FILTERING RESULTS:")
    logger.info(f"   🆕 New patents: {result.get('new_patents_count', 0):,}")
    logger.info(f"   🆕 New people: {result.get('new_people_count', 0):,}")
    logger.info(f"   🔁 Duplicate patents: {result.get('duplicate_patents_count', 0):,}")
    logger.info(f"   🔁 Duplicate people: {result.get('duplicate_people_count', 0):,}")
    
    # Calculate savings
    total_xml_people = result.get('total_xml_people', 0)
    new_people = result.get('new_people_count', 0)
    if total_xml_people > 0:
        savings_percent = (total_xml_people - new_people) / total_xml_people * 100
        estimated_cost_saved = (total_xml_people - new_people) * 0.03  # $0.03 per API call
        logger.info(f"\n💰 COST SAVINGS:")
        logger.info(f"   📉 API calls avoided: {total_xml_people - new_people:,} ({savings_percent:.1f}%)")
        logger.info(f"   💵 Estimated cost saved: ${estimated_cost_saved:.2f}")
        logger.info(f"   💸 Estimated cost for new people: ${new_people * 0.03:.2f}")
    
    # Show sample new people
    new_people_data = result.get('new_people_data', [])
    if new_people_data:
        logger.info(f"\n🔎 SAMPLE NEW PEOPLE (first 5):")
        for i, person in enumerate(new_people_data[:5]):
            logger.info(f"   Person {i+1}:")
            logger.info(f"      👤 Name: {person.get('first_name', '')} {person.get('last_name', '')}")
            logger.info(f"      📍 Location: {person.get('city', 'Unknown')}, {person.get('state', 'Unknown')}")
            logger.info(f"      📋 Patent: {person.get('patent_number', 'Unknown')}")
            logger.info(f"      🏷️  Type: {person.get('person_type', 'Unknown')}")
            logger.info(f"      🆔 Person ID: {person.get('person_id', 'Unknown')}")

def step0_integrate_existing_data(config: Dict[str, Any]) -> Dict[str, Any]:
    """Step 0: Integrate with existing patent database and XML files"""
    logger.info("=" * 70)
    logger.info("🚀 STEP 0: INTEGRATING EXISTING PATENT DATA")
    logger.info("=" * 70)
    
    if not config.get('USE_EXISTING_DATA', True):
        logger.info("⏭️  Skipping existing data integration (disabled in config)")
        return {'success': True, 'skipped': True}

    debug_folder_structure()
    
    # Inspect CSV databases first
    log_csv_database_inspection(config)
    
    # Run integration
    logger.info("\n🔄 RUNNING INTEGRATION PROCESS...")
    result = run_existing_data_integration(config)
    
    # Log detailed results
    log_integration_results(result)
    
    if result['success']:
        logger.info(f"\n✅ Data integration completed successfully!")
    else:
        logger.error(f"\n❌ Data integration failed: {result['error']}")
    
    return result

def step2_enrich_data(config: Dict[str, Any], patents_data=None) -> Dict[str, Any]:
    """Step 2: Enrich patent data with PeopleDataLabs"""
    logger.info("=" * 70)
    logger.info("🚀 STEP 2: ENRICHING PATENT DATA")
    logger.info("=" * 70)
    
    # Add patents data to config if provided
    if patents_data:
        config['patents_data'] = patents_data
    
    # Log enrichment configuration
    logger.info(f"⚙️  ENRICHMENT CONFIGURATION:")
    logger.info(f"   🎯 Enrich only new people: {config.get('ENRICH_ONLY_NEW_PEOPLE', True)}")
    logger.info(f"   💰 Max API calls allowed: {config.get('MAX_ENRICHMENT_COST', 1000):,}")
    logger.info(f"   🔑 API key configured: {'✅' if config.get('PEOPLEDATALABS_API_KEY') != 'YOUR_PDL_API_KEY' else '❌'}")
    
    # Check what data we have for enrichment
    new_people_data = config.get('new_people_data', [])
    if new_people_data:
        logger.info(f"\n📊 NEW PEOPLE TO ENRICH:")
        logger.info(f"   👥 Total new people: {len(new_people_data):,}")
        
        # Analyze by type
        inventors = [p for p in new_people_data if p.get('person_type') == 'inventor']
        assignees = [p for p in new_people_data if p.get('person_type') == 'assignee']
        logger.info(f"   🔬 Inventors: {len(inventors):,}")
        logger.info(f"   🏢 Assignees: {len(assignees):,}")
        
        # Show examples of people to be enriched
        logger.info(f"\n🔎 EXAMPLES OF PEOPLE TO ENRICH (first 5):")
        for i, person in enumerate(new_people_data[:5]):
            logger.info(f"   Person {i+1}:")
            logger.info(f"      👤 Name: {person.get('first_name', '')} {person.get('last_name', '')}")
            logger.info(f"      📍 Location: {person.get('city', 'Unknown')}, {person.get('state', 'Unknown')}")
            logger.info(f"      📋 Patent: {person.get('patent_number', 'Unknown')}")
            logger.info(f"      🏷️  Type: {person.get('person_type', 'Unknown')}")
        
        if len(new_people_data) > config.get('MAX_ENRICHMENT_COST', 1000):
            logger.warning(f"⚠️  Will limit to {config.get('MAX_ENRICHMENT_COST', 1000):,} people to control costs")
    else:
        logger.warning("⚠️  No new people data found, will fall back to XML parsing")
    
    # Run enrichment
    logger.info(f"\n🔄 STARTING ENRICHMENT PROCESS...")
    result = run_enrichment(config)
    
    # Log enrichment results
    if result['success']:
        logger.info(f"\n✅ Data enrichment completed successfully!")
        logger.info(f"📊 ENRICHMENT RESULTS:")
        logger.info(f"   👥 People processed: {result.get('total_people', 0):,}")
        logger.info(f"   ✅ Successfully enriched: {result.get('enriched_count', 0):,}")
        logger.info(f"   📈 Enrichment rate: {result.get('enrichment_rate', 0):.1f}%")
        
        if result.get('api_calls_saved'):
            logger.info(f"   💰 API calls saved: {result.get('api_calls_saved', 0):,}")
            logger.info(f"   💵 Estimated cost savings: {result.get('estimated_cost_savings', '$0.00')}")
            logger.info(f"   💸 Actual API cost: {result.get('actual_api_cost', '$0.00')}")
        
        # Show examples of enriched data
        enriched_data = result.get('enriched_data', [])
        if enriched_data:
            logger.info(f"\n🔎 SAMPLE ENRICHED RESULTS (first 3):")
            for i, person in enumerate(enriched_data[:3]):
                pdl_data = person.get('enriched_data', {}).get('pdl_data', {})
                logger.info(f"   Enriched Person {i+1}:")
                logger.info(f"      👤 Original: {person.get('original_name', 'Unknown')}")
                logger.info(f"      📋 Patent: {person.get('patent_number', 'Unknown')}")
                logger.info(f"      ✨ Enriched name: {pdl_data.get('full_name', 'Not found')}")
                logger.info(f"      📧 Email: {'✅ Found' if pdl_data.get('emails') else '❌ Not found'}")
                logger.info(f"      🏢 Current job: {pdl_data.get('job_title', 'Unknown')} at {pdl_data.get('job_company_name', 'Unknown')}")
                logger.info(f"      🎯 Match score: {person.get('match_score', 0):.2f}")
    else:
        logger.error(f"\n❌ Data enrichment failed: {result['error']}")
    
    return result

def step1_extract_patents(config: Dict[str, Any]) -> Dict[str, Any]:
    """Step 1: Extract patent data from USPTO"""
    logger.info("=" * 50)
    logger.info("STEP 1: EXTRACTING PATENTS FROM USPTO")
    logger.info("=" * 50)
    
    result = run_patent_extraction(config)
    
    if result['success']:
        logger.info(f"✅ Patent extraction completed successfully!")
        logger.info(f"📊 Total patents extracted: {result['total_patents']}")
    else:
        logger.error(f"❌ Patent extraction failed: {result['error']}")
    
    return result



def run_full_pipeline():
    """Run the complete patent processing pipeline"""
    logger.info("🚀 Starting Complete Patent Processing Pipeline")
    logger.info(f"⏰ Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load configuration
    config = load_configuration()
    
    # Create output directories
    os.makedirs(config['OUTPUT_DIR'], exist_ok=True)
    os.makedirs(config['REPORTS_OUTPUT_DIR'], exist_ok=True)
    
    # Store results from each step
    pipeline_results = {}
    
    try:
        # Step 0: Integrate Existing Data
        integration_result = step0_integrate_existing_data(config)
        pipeline_results['data_integration'] = integration_result
        config['new_people_data'] = integration_result.get('new_people_data', [])
        
        # Only run Step 1 if Step 0 didn't find XML data
        if integration_result.get('success') and integration_result.get('new_patents_count', 0) > 0:
            logger.info("✅ Using patent data from existing XML files - SKIPPING USPTO API extraction")
            config['patents_data'] = integration_result.get('new_patents_data', [])
            # Create a successful extraction result from XML data
            pipeline_results['extraction'] = {
                'success': True,
                'total_patents': integration_result.get('new_patents_count', 0),
                'source': 'xml_files',
                'skipped_api': True
            }
        else:
            # Step 1: Extract Patents from API (only if no XML data)
            logger.info("No XML data found, proceeding with USPTO API extraction...")
            extraction_result = step1_extract_patents(config)
            pipeline_results['extraction'] = extraction_result
            
            if not extraction_result['success']:
                logger.error("Pipeline stopped due to extraction failure")
                return pipeline_results
        
        # Step 2: Enrich Data (uncomment when ready)
        # enrichment_result = step2_enrich_data(config)
        # pipeline_results['enrichment'] = enrichment_result
        
        # if not enrichment_result['success']:
        #     logger.error("Pipeline stopped due to enrichment failure")
        #     return pipeline_results
        
        
        
        # Print final summary
        print_pipeline_summary(pipeline_results)
        
    except Exception as e:
        logger.error(f"💥 Pipeline failed with error: {e}")
        pipeline_results['pipeline_error'] = str(e)
    
    logger.info(f"⏰ End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return pipeline_results

def run_individual_steps():
    """Run individual steps for testing (uncomment the ones you want to run)"""
    config = load_configuration()
    os.makedirs(config['OUTPUT_DIR'], exist_ok=True)
    
    # Uncomment the step(s) you want to test:
    
    # Test Step 1: Patent Extraction
    # result = step1_extract_patents(config)
    # print(f"Extraction result: {result}")
    
    # Test Step 2: Data Enrichment (requires existing XML file or previous extraction)
    # result = step2_enrich_data(config)
    # print(f"Enrichment result: {result}")
    
    # Test Step 3: CRM Integration (requires enriched data)
    # result = step3_integrate_crm(config)
    # print(f"CRM result: {result}")
    
    # Test Step 4: Email Automation (requires enriched data)
    # result = step4_automate_emails(config)
    # print(f"Email result: {result}")
    
    # Test Step 5: Reporting (requires pipeline results)
    # pipeline_results = {'extraction': {'success': True, 'total_patents': 100}}
    # result = step5_generate_reports(config, pipeline_results)
    # print(f"Report result: {result}")
    
    print("Individual step testing complete")

def print_pipeline_summary(results: Dict[str, Any]):
    """Print a comprehensive pipeline summary"""
    print("\n" + "="*70)
    print("🏁 PIPELINE EXECUTION SUMMARY")
    print("="*70)
    
    integration = results.get('data_integration', {})
    enrichment = results.get('enrichment', {})
    
    if integration.get('success'):
        print(f"📊 DATA INTEGRATION:")
        print(f"   🗃️  Existing patents in DB: {integration.get('existing_patents_count', 0):,}")
        print(f"   👥 Existing people in DB: {integration.get('existing_people_count', 0):,}")
        print(f"   🆕 New patents found: {integration.get('new_patents_count', 0):,}")
        print(f"   🆕 New people found: {integration.get('new_people_count', 0):,}")
    
    if enrichment.get('success'):
        print(f"\n💎 DATA ENRICHMENT:")
        print(f"   👥 People processed: {enrichment.get('total_people', 0):,}")
        print(f"   ✅ Successfully enriched: {enrichment.get('enriched_count', 0):,}")
        print(f"   📈 Enrichment rate: {enrichment.get('enrichment_rate', 0):.1f}%")
        if enrichment.get('estimated_cost_savings'):
            print(f"   💰 Cost savings: {enrichment.get('estimated_cost_savings', '$0.00')}")
    
    # Print any errors
    errors = []
    for step, result in results.items():
        if not result.get('success') and 'error' in result:
            errors.append(f"{step}: {result['error']}")
    
    if errors:
        print(f"\n❌ Errors encountered:")
        for error in errors:
            print(f"   • {error}")
    else:
        print(f"\n✅ Pipeline completed successfully!")
    
    print("="*70)

def main():
    """Main entry point"""
    # Run the pipeline
    run_full_pipeline()

if __name__ == "__main__":
    main()