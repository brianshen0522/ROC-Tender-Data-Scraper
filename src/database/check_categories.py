#!/usr/bin/env python3
import os
import json
from .database import get_db_connection, ensure_connection, save_tender_category

def load_json_data(json_path):
    """Load tender category data from JSON file"""
    try:
        if not os.path.exists(json_path):
            print(f"❌ JSON file not found at: {json_path}")
            return None
            
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"✅ Loaded {len(data)} tender categories from JSON file")
        return data
    except json.JSONDecodeError:
        print(f"❌ Invalid JSON format in file: {json_path}")
        return None
    except Exception as e:
        print(f"❌ Error loading JSON data: {e}")
        return None

def check_tender_categories(conn, json_data):
    """Check if tender categories in the JSON data match those in the database"""
    if not conn or not json_data:
        return False
    
    try:
        # Ensure the connection is alive
        conn = ensure_connection(conn)
        if not conn:
            print("❌ Database connection lost. Cannot proceed.")
            return False
            
        cur = conn.cursor()
        
        # Get all existing tender categories from database
        cur.execute("SELECT id, name, category FROM tender_categories")
        # Create a dictionary with id as key and tuple (name, category) as value
        db_categories = {row[0]: (row[1], row[2]) for row in cur.fetchall()}
        
        print(f"📊 Found {len(db_categories)} tender categories in database")
        
        # Check for missing categories (in JSON but not in DB)
        missing_categories = []
        for category in json_data:
            if category['id'] not in db_categories:
                missing_categories.append(category)
        
        # Check for mismatched details (same ID but different name or category)
        mismatched_categories = []
        for category in json_data:
            if category['id'] in db_categories:
                db_name, db_category = db_categories[category['id']]
                if db_name != category['name'] or db_category != category['category']:
                    mismatched_categories.append({
                        'id': category['id'],
                        'json_name': category['name'],
                        'json_category': category['category'],
                        'db_name': db_name,
                        'db_category': db_category
                    })
        
        # Check for extra categories (in DB but not in JSON)
        json_ids = {category['id'] for category in json_data}
        extra_categories = []
        for category_id, (name, category) in db_categories.items():
            if category_id not in json_ids:
                extra_categories.append({
                    'id': category_id,
                    'name': name,
                    'category': category
                })
        
        # Print results
        print("\n" + "="*70)
        print("TENDER CATEGORIES COMPARISON RESULTS")
        print("="*70)
        
        if not missing_categories and not mismatched_categories and not extra_categories:
            print("✅ Perfect match! All tender categories in JSON match the database.")
            print(f"📊 Total tender categories: {len(json_data)}")
            return True
        
        if missing_categories:
            print(f"\n❌ Missing {len(missing_categories)} tender categories in database:")
            for category in missing_categories[:10]:  # Show first 10 only to avoid clutter
                print(f"  - {category['id']}: {category['name']} ({category['category']})")
            if len(missing_categories) > 10:
                print(f"  ... and {len(missing_categories) - 10} more")
        
        if mismatched_categories:
            print(f"\n⚠️ Found {len(mismatched_categories)} tender categories with mismatched details:")
            for category in mismatched_categories[:10]:
                print(f"  - {category['id']}:")
                print(f"    JSON: {category['json_name']} ({category['json_category']})")
                print(f"    DB:   {category['db_name']} ({category['db_category']})")
            if len(mismatched_categories) > 10:
                print(f"  ... and {len(mismatched_categories) - 10} more")
        
        if extra_categories:
            print(f"\n📌 Found {len(extra_categories)} extra tender categories in database:")
            for category in extra_categories[:10]:
                print(f"  - {category['id']}: {category['name']} ({category['category']})")
            if len(extra_categories) > 10:
                print(f"  ... and {len(extra_categories) - 10} more")
        
        # Statistics
        print("\n" + "="*70)
        print("SUMMARY")
        print(f"📊 Tender categories in JSON: {len(json_data)}")
        print(f"📊 Tender categories in database: {len(db_categories)}")
        print(f"❌ Missing in database: {len(missing_categories)}")
        print(f"⚠️ Detail mismatches: {len(mismatched_categories)}")
        print(f"📌 Extra in database: {len(extra_categories)}")
        print("="*70)
        
        return len(missing_categories) == 0  # Return True only if no missing categories
        
    except Exception as e:
        print(f"❌ Error checking tender categories: {e}")
        return False

def import_missing_categories(conn, json_data):
    """Import missing tender categories from the JSON file into the database"""
    if not conn or not json_data:
        return False
    
    try:
        # Ensure the connection is alive
        conn = ensure_connection(conn)
        if not conn:
            print("❌ Database connection lost. Cannot proceed.")
            return False
            
        cur = conn.cursor()
        
        # Get existing category IDs from database
        cur.execute("SELECT id FROM tender_categories")
        existing_ids = {row[0] for row in cur.fetchall()}
        
        # Identify missing categories
        missing_categories = [category for category in json_data 
                             if category['id'] not in existing_ids]
        
        if not missing_categories:
            print("✅ No missing tender categories to import.")
            return True
        
        print(f"📥 Importing {len(missing_categories)} missing tender categories...")
        
        # Insert missing categories using the save_tender_category function
        success_count = 0
        for category in missing_categories:
            if save_tender_category(conn, category['id'], category['name'], category['category']):
                success_count += 1
            else:
                print(f"⚠️ Failed to import tender category: {category['id']} - {category['name']}")
        
        print(f"✅ Successfully imported {success_count} tender categories into the database.")
        return True
        
    except Exception as e:
        print(f"❌ Error importing tender categories: {e}")
        return False

def main():
    print("=" * 70)
    print("TENDER CATEGORIES DATA VERIFICATION")
    print("=" * 70)
    
    # Define JSON file path
    json_path = "../../data/tender_categories.json"
    
    # Connect to database using the function from database.py
    conn = get_db_connection()
    if not conn:
        print("❌ Cannot proceed without database connection. Exiting.")
        return
    
    try:
        # Load JSON data
        json_data = load_json_data(json_path)
        if not json_data:
            print("❌ Cannot proceed without JSON data. Exiting.")
            return
        
        # Check if tender categories match
        match_result = check_tender_categories(conn, json_data)
        
        # Ask user if they want to import missing categories
        if not match_result:
            user_input = input("\nDo you want to import missing tender categories into the database? (y/n): ")
            if user_input.lower() == 'y':
                import_missing_categories(conn, json_data)
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        conn.close()
        print("\n✨ Verification complete ✨")

if __name__ == "__main__":
    main()