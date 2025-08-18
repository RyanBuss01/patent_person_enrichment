#!/bin/bash
# Quick script to find and extract comparison logic

echo "🔍 SEARCHING FOR COMPARISON LOGIC..."
echo "================================="

# 1. First, let's find all database files
echo "📁 Looking for all database files..."
find . -name "*.accdb" -o -name "*.mdb" | head -10

echo ""
echo "🎯 KEY FILES TO CHECK:"

# 2. Check the UpdatePatent.bat file (this is likely the key!)
if [ -f "patent_system/UpdatePatent.bat" ]; then
    echo "✅ Found UpdatePatent.bat - This likely contains the process!"
    echo "--- UpdatePatent.bat Content ---"
    cat "patent_system/UpdatePatent.bat"
    echo "--- End of UpdatePatent.bat ---"
else
    echo "❌ UpdatePatent.bat not found"
fi

echo ""

# 3. Check for batch files in current directory
echo "📄 Looking for batch files..."
find . -name "*.bat" | while read batfile; do
    echo "Found: $batfile"
    echo "--- Content ---"
    cat "$batfile"
    echo "--- End ---"
    echo ""
done

# 4. Look for the programs database in different locations
echo "🔍 Searching for programs database..."
find . -name "*program*" -type f
find . -name "*update*" -type f | grep -v ".log"

# 5. Check the main databases for queries
echo ""
echo "📋 Checking main databases for queries..."

if [ -f "patent_system/Database.mdb" ]; then
    echo "Checking Database.mdb for queries..."
    mdb-queries "patent_system/Database.mdb" 2>/dev/null || echo "No queries found"
fi

if [ -f "patent_system/uspc_patent_data.accdb" ]; then
    echo "Checking uspc_patent_data.accdb for queries..."
    mdb-queries "patent_system/uspc_patent_data.accdb" 2>/dev/null || echo "No queries found"
fi

if [ -f "patent_system/uspc_new_issue.accdb" ]; then
    echo "Checking uspc_new_issue.accdb for queries..."
    mdb-queries "patent_system/uspc_new_issue.accdb" 2>/dev/null || echo "No queries found"
fi

echo ""
echo "🔧 Next steps:"
echo "1. Check the UpdatePatent.bat content above"
echo "2. Look at any queries found"
echo "3. These will show the comparison process"