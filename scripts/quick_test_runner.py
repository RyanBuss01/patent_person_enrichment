#!/usr/bin/env python3
# =============================================================================
# quick_test_runner.py - Quick Test of Data Comparison
# Run this to quickly test your data structure and comparison logic
# =============================================================================

import os
import sys
from pathlib import Path

def check_environment():
    """Check if required files and folders exist"""
    print("🔍 CHECKING ENVIRONMENT")
    print("=" * 40)
    
    # Check for required folders
    folders_to_check = [
        "converted_databases",
        "converted_databases/csv", 
        "USPC_Download",
        "output"
    ]
    
    for folder in folders_to_check:
        exists = os.path.exists(folder)
        print(f"📁 {folder}: {'✅' if exists else '❌'}")
        if not exists and folder == "output":
            os.makedirs(folder, exist_ok=True)
            print(f"   Created output folder")
    
    # Check for XML files
    xml_files = [
        "ipg250812.xml",
        "USPC_Download/ipg250812.xml"  # Alternative location
    ]
    
    xml_found = False
    for xml_file in xml_files:
        if os.path.exists(xml_file):
            print(f"📄 XML file found: {xml_file} ✅")
            xml_found = True
            break
    
    if not xml_found:
        print(f"📄 XML files: ❌ (checked {xml_files})")
    
    # Check for CSV files
    csv_folder = "converted_databases/csv"
    if os.path.exists(csv_folder):
        csv_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]
        print(f"📄 CSV files found: {len(csv_files)} files")
        if csv_files:
            print(f"   Sample files: {csv_files[:3]}")
    
    return xml_found and os.path.exists(csv_folder)

def run_structure_analysis():
    """Run the data structure analyzer"""
    print("\n🔍 RUNNING STRUCTURE ANALYSIS")
    print("=" * 40)
    
    try:
        # Import and run the analyzer
        from data_structure_analyzer import DataStructureAnalyzer
        
        analyzer = DataStructureAnalyzer()
        
        # Analyze CSV files
        analyzer.analyze_csv_folder("converted_databases/csv")
        
        # Try to analyze XML file
        xml_files = ["ipg250812.xml", "USPC_Download/ipg250812.xml"]
        for xml_file in xml_files:
            if os.path.exists(xml_file):
                analyzer.analyze_xml_file(xml_file)
                break
        
        # Generate suggestions
        analyzer.generate_mapping_suggestions()
        analyzer.save_analysis_report()
        
        return True
        
    except ImportError as e:
        print(f"❌ Could not import analyzer: {e}")
        return False
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        return False

def run_quick_comparison():
    """Run a quick comparison test"""
    print("\n🔍 RUNNING QUICK COMPARISON TEST")
    print("=" * 40)
    
    try:
        # Import and run the improved comparator
        from improved_data_comparison import ImprovedDataComparator
        
        # Initialize with smaller scope for testing
        comparator = ImprovedDataComparator("converted_databases/csv", "output")
        
        # Load CSV data
        print("📊 Loading CSV databases...")
        if not comparator.load_csv_databases():
            print("❌ Failed to load CSV data")
            return False
        
        # Process XML (limit to first few patents for testing)
        print("📄 Processing XML patents...")
        xml_files = ["ipg250812.xml", "USPC_Download/ipg250812.xml"]
        xml_patents = []
        
        for xml_file in xml_files:
            if os.path.exists(xml_file):
                xml_patents = comparator.process_xml_patents(xml_file)
                break
        
        if not xml_patents:
            print("❌ No XML patents found")
            return False
        
        # Limit to first 50 patents for quick test
        xml_patents = xml_patents[:50]
        print(f"✅ Processing {len(xml_patents)} patents for quick test")
        
        # Run comparison
        print("🔍 Running comparison...")
        results = comparator.compare_and_filter(xml_patents, use_fuzzy_matching=True)
        
        # Save results
        print("💾 Saving results...")
        file_paths = comparator.save_results(results, prefix="quick_test")
        
        # Print summary
        print(f"\n📊 QUICK TEST RESULTS")
        print("=" * 30)
        print(f"🗃️  Existing patents: {len(comparator.existing_patents):,}")
        print(f"👥 Existing people: {len(comparator.existing_people):,}")
        print(f"📄 XML patents tested: {len(xml_patents):,}")
        print(f"🆕 New patents: {len(results['new_patents']):,}")
        print(f"👤 New people: {len(results['new_people']):,}")
        print(f"📈 Match rate: {results['match_rate']:.1f}%")
        print(f"💰 Est. cost savings: ${results['cost_savings']:.2f}")
        
        return True
        
    except ImportError as e:
        print(f"❌ Could not import comparator: {e}")
        return False
    except Exception as e:
        print(f"❌ Comparison failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main test runner"""
    print("🚀 QUICK DATA COMPARISON TEST")
    print("=" * 50)
    
    # Check environment
    env_ok = check_environment()
    
    if not env_ok:
        print("\n❌ Environment check failed. Please ensure:")
        print("   • converted_databases/csv folder exists with CSV files")
        print("   • XML file (ipg250812.xml) exists")
        print("   • Run the csv_converter.py first if needed")
        return
    
    # Run structure analysis
    print("\n" + "="*50)
    analysis_ok = run_structure_analysis()
    
    # Run quick comparison
    print("\n" + "="*50)
    comparison_ok = run_quick_comparison()
    
    # Final summary
    print("\n" + "="*50)
    print("🎯 TEST SUMMARY")
    print("=" * 20)
    print(f"Environment: {'✅' if env_ok else '❌'}")
    print(f"Structure Analysis: {'✅' if analysis_ok else '❌'}")
    print(f"Comparison Test: {'✅' if comparison_ok else '❌'}")
    
    if analysis_ok and comparison_ok:
        print("\n🎉 All tests passed! Check the output folder for results.")
        print("📁 Files generated:")
        print("   • data_structure_analysis.json")
        print("   • quick_test_new_people_*.json")
        print("   • quick_test_statistics_*.json")
    else:
        print("\n⚠️  Some tests failed. Check the error messages above.")

if __name__ == "__main__":
    main()