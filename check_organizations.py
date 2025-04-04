#!/usr/bin/env python3
import os
import json
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_db_connection():
    """Create and return a new database connection using environment variables"""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        print("✅ Database connection established")
        return conn
    except Exception as e:
        print(f"⚠️ Database connection error: {e}")
        return None

def load_json_data(json_path):
    """Load organization data from JSON file"""
    try:
        if not os.path.exists(json_path):
            print(f"❌ JSON file not found at: {json_path}")
            return None
            
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"✅ Loaded {len(data)} organizations from JSON file")
        return data
    except json.JSONDecodeError:
        print(f"❌ Invalid JSON format in file: {json_path}")
        return None
    except Exception as e:
        print(f"❌ Error loading JSON data: {e}")
        return None

def check_organizations(conn, json_data):
    """Check if organizations in the JSON data match those in the database"""
    if not conn or not json_data:
        return False
    
    try:
        cur = conn.cursor()
        
        # Get all existing organization records from database
        cur.execute("SELECT site_id, name FROM organizations")
        db_orgs = {row[0]: row[1] for row in cur.fetchall()}
        
        print(f"📊 Found {len(db_orgs)} organizations in database")
        
        # Check for missing organizations (in JSON but not in DB)
        missing_orgs = []
        for site_id, name in json_data.items():
            if site_id not in db_orgs:
                missing_orgs.append((site_id, name))
        
        # Check for mismatched names (same ID but different name)
        mismatched_orgs = []
        for site_id, name in json_data.items():
            if site_id in db_orgs and db_orgs[site_id] != name:
                mismatched_orgs.append((site_id, name, db_orgs[site_id]))
        
        # Check for extra organizations (in DB but not in JSON)
        extra_orgs = []
        for site_id, name in db_orgs.items():
            if site_id not in json_data:
                extra_orgs.append((site_id, name))
        
        # Print results
        print("\n" + "="*70)
        print("ORGANIZATIONS COMPARISON RESULTS")
        print("="*70)
        
        if not missing_orgs and not mismatched_orgs and not extra_orgs:
            print("✅ Perfect match! All organizations in JSON match the database.")
            print(f"📊 Total organizations: {len(json_data)}")
            return True
        
        if missing_orgs:
            print(f"\n❌ Missing {len(missing_orgs)} organizations in database:")
            for site_id, name in missing_orgs[:10]:  # Show first 10 only to avoid clutter
                print(f"  - {site_id}: {name}")
            if len(missing_orgs) > 10:
                print(f"  ... and {len(missing_orgs) - 10} more")
        
        if mismatched_orgs:
            print(f"\n⚠️ Found {len(mismatched_orgs)} organizations with mismatched names:")
            for site_id, json_name, db_name in mismatched_orgs[:10]:
                print(f"  - {site_id}:")
                print(f"    JSON: {json_name}")
                print(f"    DB:   {db_name}")
            if len(mismatched_orgs) > 10:
                print(f"  ... and {len(mismatched_orgs) - 10} more")
        
        if extra_orgs:
            print(f"\n📌 Found {len(extra_orgs)} extra organizations in database:")
            for site_id, name in extra_orgs[:10]:
                print(f"  - {site_id}: {name}")
            if len(extra_orgs) > 10:
                print(f"  ... and {len(extra_orgs) - 10} more")
        
        # Statistics
        print("\n" + "="*70)
        print("SUMMARY")
        print(f"📊 Organizations in JSON: {len(json_data)}")
        print(f"📊 Organizations in database: {len(db_orgs)}")
        print(f"❌ Missing in database: {len(missing_orgs)}")
        print(f"⚠️ Name mismatches: {len(mismatched_orgs)}")
        print(f"📌 Extra in database: {len(extra_orgs)}")
        print("="*70)
        
        return len(missing_orgs) == 0  # Return True only if no missing organizations
        
    except Exception as e:
        print(f"❌ Error checking organizations: {e}")
        return False

def import_missing_organizations(conn, json_data):
    """Import missing organizations from the JSON file into the database"""
    if not conn or not json_data:
        return False
    
    try:
        cur = conn.cursor()
        
        # Get existing organization IDs from database
        cur.execute("SELECT site_id FROM organizations")
        existing_ids = {row[0] for row in cur.fetchall()}
        
        # Identify missing organizations
        missing_orgs = [(site_id, name) for site_id, name in json_data.items() 
                        if site_id not in existing_ids]
        
        if not missing_orgs:
            print("✅ No missing organizations to import.")
            return True
        
        print(f"📥 Importing {len(missing_orgs)} missing organizations...")
        
        # Insert missing organizations
        for site_id, name in missing_orgs:
            cur.execute(
                "INSERT INTO organizations (site_id, name) VALUES (%s, %s) ON CONFLICT (site_id) DO NOTHING",
                (site_id, name)
            )
        
        conn.commit()
        print(f"✅ Successfully imported {len(missing_orgs)} organizations into the database.")
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error importing organizations: {e}")
        return False

def main():
    print("=" * 70)
    print("ORGANIZATION DATA VERIFICATION")
    print("=" * 70)
    
    # Define JSON file path
    json_path = "./data/organizations.json"
    
    # Connect to database
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
        
        # Check if organizations match
        match_result = check_organizations(conn, json_data)
        
        # Ask user if they want to import missing organizations
        if not match_result:
            user_input = input("\nDo you want to import missing organizations into the database? (y/n): ")
            if user_input.lower() == 'y':
                import_missing_organizations(conn, json_data)
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        conn.close()
        print("\n✨ Verification complete ✨")

if __name__ == "__main__":
    main()