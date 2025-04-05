#!/usr/bin/env python3
import os
import sys
import json
from dotenv import load_dotenv
from database import get_db_connection, save_tender_category

# Load environment variables from .env file
load_dotenv('../.env')

def import_categories_from_json(json_file_path):
    """
    Import tender categories from a JSON file into the database.
    
    Args:
        json_file_path: Path to the JSON file containing category data
    """
    print("=" * 70)
    print("TENDER CATEGORIES IMPORT")
    print("=" * 70)
    
    # Check if file exists
    if not os.path.exists(json_file_path):
        print(f"❌ Error: File not found at {json_file_path}")
        return
    
    # Load JSON data
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            categories = json.load(f)
        print(f"✅ Successfully loaded {len(categories)} categories from {json_file_path}")
    except Exception as e:
        print(f"❌ Error loading JSON file: {str(e)}")
        return
    
    # Connect to database
    conn = get_db_connection()
    if not conn:
        print("❌ Cannot proceed without database connection. Exiting.")
        return
    
    try:
        # Process each category
        success_count = 0
        error_count = 0
        
        print(f"🔄 Importing categories...")
        
        # Group by category type for organized output
        category_types = {}
        for item in categories:
            cat_type = item["category"]
            if cat_type not in category_types:
                category_types[cat_type] = []
            category_types[cat_type].append(item)
        
        # Process each category type
        for cat_type, items in category_types.items():
            print(f"\n📂 Processing {cat_type} ({len(items)} items):")
            
            for item in items:
                category_id = item["id"]
                name = item["name"]
                category = item["category"]
                
                try:
                    # Save to database
                    if save_tender_category(conn, category_id, name, category):
                        print(f"  ✅ Imported: [{category_id}] {name}")
                        success_count += 1
                    else:
                        print(f"  ❌ Failed to import: [{category_id}] {name}")
                        error_count += 1
                except Exception as e:
                    print(f"  ❌ Error importing [{category_id}] {name}: {str(e)}")
                    error_count += 1
        
        # Get category counts from database
        cur = conn.cursor()
        cur.execute("""
        SELECT category, COUNT(*) 
        FROM tender_categories 
        GROUP BY category
        ORDER BY category;
        """)
        category_counts = cur.fetchall()
        
        # Print summary
        print("\n" + "=" * 70)
        print("IMPORT SUMMARY")
        print("=" * 70)
        print(f"✅ Successfully imported: {success_count}")
        print(f"❌ Failed to import: {error_count}")
        
        print("\n📊 Category counts in database:")
        for category, count in category_counts:
            print(f"  - {category}: {count} categories")
        
        print("\n✨ Import Complete! ✨")
        
    except Exception as e:
        print(f"❌ Error during import: {str(e)}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Get JSON file path from command line or use default
    json_file_path = sys.argv[1] if len(sys.argv) > 1 else "data/tender_categories.json"
    import_categories_from_json(json_file_path)