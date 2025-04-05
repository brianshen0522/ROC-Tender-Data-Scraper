import os
import psycopg2
from dotenv import load_dotenv
from utils import convert_to_roc_date, parse_roc_date
from datetime import datetime

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
        return conn
    except Exception as e:
        print(f"⚠️ Database connection error: {e}")
        return None

def ensure_connection(conn):
    """Ensure the connection is alive, reconnect if needed, and roll back any failed transactions"""
    try:
        # Try rolling back any failed transaction first
        try:
            if conn:
                conn.rollback()
        except:
            # If rollback fails, we definitely need a new connection
            pass
            
        # Test if connection is alive
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return conn
        else:
            raise Exception("Connection is None")
    except Exception as e:
        print(f"🔄 Database connection lost or in error state, reconnecting... ({e})")
        try:
            # Close the broken connection if it's still around
            if conn:
                try:
                    conn.close()
                except:
                    pass
            
            # Create a new connection
            new_conn = get_db_connection()
            if new_conn:
                print("✅ Successfully reconnected to database")
                return new_conn
            else:
                print("❌ Failed to reconnect to database")
                return None
        except Exception as e:
            print(f"❌ Reconnection failed: {e}")
            return None

def setup_database(conn):
    """Create necessary database tables if they don't exist"""
    if not conn:
        print("❌ Cannot set up database without connection. Exiting.")
        return False
    
    cur = conn.cursor()

    # Create organizations table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS organizations (
        site_id TEXT PRIMARY KEY,
        name TEXT UNIQUE NOT NULL
    );
    """)

    # Create tender_category table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tender_categories (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        category TEXT NOT NULL
    );
    """)

    # Create merged tenders table with a composite primary key and url as unique
    # Note: publication_date and deadline are now TEXT to store ROC dates
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tenders (
        organization_id TEXT,
        tender_no TEXT,
        url TEXT UNIQUE,
        pk_pms_main TEXT,
        project_name TEXT,
        publication_date TEXT,
        deadline TEXT,
        scrap_status TEXT,
        org_name TEXT,
        agency_address TEXT,
        contact_person TEXT,
        contact_phone TEXT,
        fax_number TEXT,
        email TEXT,
        procurement_data TEXT,
        tender_id TEXT,
        tender_title TEXT,
        item_category TEXT REFERENCES tender_categories(id),
        nature_of_procurement TEXT,
        procurement_amount_range TEXT,
        handling_method TEXT,
        according_to_laws TEXT,
        procurement_act_49 TEXT,
        sensitive_procurement TEXT,
        national_security_procurement TEXT,
        budget_amount TEXT,
        budget_public TEXT,
        subsequent_expansion TEXT,
        agency_subsidy TEXT,
        promotional_service TEXT,
        tender_method TEXT,
        awarding_method TEXT,
        most_advantageous_bid_reference TEXT,
        e_quotation TEXT,
        announcement_transmission_count TEXT,
        tender_status TEXT,
        multiple_awards TEXT,
        base_price_set TEXT,
        price_included_in_evaluation TEXT,
        weight_above_20_percent TEXT,
        special_procurement TEXT,
        public_inspection_done TEXT,
        package_tender TEXT,
        joint_supply_contract TEXT,
        joint_procurement TEXT,
        engineer_certification TEXT,
        negotiation_measures TEXT,
        applicable_procurement_law TEXT,
        processed_according_to_procurement_act TEXT,
        e_tender TEXT,
        e_bidding TEXT,
        bid_deadline TEXT,
        bid_opening_time TEXT,
        bid_opening_location TEXT,
        bid_bond_required TEXT,
        performance_bond_required TEXT,
        bid_text TEXT,
        bid_document_collection_location TEXT,
        PRIMARY KEY (tender_no, organization_id, publication_date),
        FOREIGN KEY (organization_id) REFERENCES organizations(site_id)
    );
    """)
    conn.commit()
    return True

def save_organization(conn, site_id, name):
    """Save an organization to the database"""
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO organizations (site_id, name) VALUES (%s, %s) ON CONFLICT (site_id) DO NOTHING",
            (site_id, name)
        )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"⚠️ Error saving organization: {e}")
        conn.rollback()
        return False

def save_tender_category(conn, category_id, name, category_type):
    """Save a tender category to the database"""
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO tender_categories (id, name, category) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING",
            (category_id, name, category_type)
        )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"⚠️ Error saving tender category: {e}")
        conn.rollback()
        return False

def get_or_create_category(conn, category_data):
    """Get existing category or create a new one if it doesn't exist
    
    Args:
        conn: Database connection
        category_data: String like "勞務類\n866 - 與管理顧問有關之服務"
        
    Returns:
        category_id: The ID of the category (e.g., "866")
    """
    if not conn or not category_data:
        return None
    
    try:
        # Parse the category data
        lines = category_data.strip().split('\n')
        if len(lines) < 2:
            print(f"⚠️ Invalid category format: {category_data}")
            return None
        
        category_type = lines[0].strip()
        
        # Parse the ID and name
        parts = lines[1].strip().split(' - ', 1)
        if len(parts) < 2:
            print(f"⚠️ Cannot parse category ID and name from: {lines[1]}")
            return None
        
        category_id = parts[0].strip()
        name = parts[1].strip()
        
        # Check if category exists
        cur = conn.cursor()
        cur.execute("SELECT id FROM tender_categories WHERE id = %s", (category_id,))
        result = cur.fetchone()
        
        if not result:
            # Create new category
            save_tender_category(conn, category_id, name, category_type)
            print(f"✅ Created new tender category: {category_id} - {name} ({category_type})")
        
        cur.close()
        return category_id
    except Exception as e:
        print(f"⚠️ Error getting or creating category: {e}")
        conn.rollback()
        return None

def get_organization_id(conn, org_name):
    """Get organization ID from the database by name"""
    if not conn:
        return None
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT site_id FROM organizations WHERE name = %s", (org_name,))
        result = cur.fetchone()
        cur.close()
        return result[0] if result else None
    except Exception as e:
        print(f"⚠️ Error getting organization ID: {e}")
        conn.rollback()
        return None

def check_tender_status(conn, detail_link):
    """Check if a tender already exists in the database and get its status"""
    if not conn:
        return None
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT scrap_status FROM tenders WHERE url = %s", (detail_link,))
        result = cur.fetchone()
        cur.close()
        return result[0] if result else None
    except Exception as e:
        print(f"⚠️ Error checking tender status: {e}")
        conn.rollback()
        return None

def save_tender(conn, tender_data):
    """Save or update a tender in the database"""
    if not conn:
        return False
    
    # Check for required primary key fields
    if 'tender_no' not in tender_data or 'organization_id' not in tender_data or 'publication_date' not in tender_data:
        print("⚠️ Missing primary key fields in tender data. Cannot save.")
        return False
    
    # Check for NULL values in primary key fields
    if tender_data['tender_no'] is None or tender_data['organization_id'] is None or tender_data['publication_date'] is None:
        print("⚠️ NULL values in primary key fields. Cannot save.")
        return False
    
    # Process item_category if present
    if 'item_category' in tender_data and tender_data['item_category']:
        category_id = get_or_create_category(conn, tender_data['item_category'])
        if category_id:
            tender_data['item_category'] = category_id
        else:
            # If we couldn't parse the category, don't update this field
            tender_data.pop('item_category', None)
    
    # Convert dates to ROC format
    if 'publication_date' in tender_data and tender_data['publication_date']:
        # If it's already an ROC date string (e.g., '113/10/30'), keep it as is
        if not isinstance(tender_data['publication_date'], str) or '/' not in tender_data['publication_date']:
            tender_data['publication_date'] = convert_to_roc_date(tender_data['publication_date'])
    
    if 'deadline' in tender_data and tender_data['deadline']:
        # If it's already an ROC date string (e.g., '113/10/30'), keep it as is
        if not isinstance(tender_data['deadline'], str) or '/' not in tender_data['deadline']:
            tender_data['deadline'] = convert_to_roc_date(tender_data['deadline'])
    
    try:
        cur = conn.cursor()
        
        # First check if this record exists
        cur.execute("""
            SELECT 1 FROM tenders 
            WHERE tender_no = %s AND organization_id = %s AND publication_date = %s
        """, (tender_data['tender_no'], tender_data['organization_id'], tender_data['publication_date']))
        
        record_exists = cur.fetchone() is not None
        
        if record_exists:
            # DIRECT UPDATE APPROACH - more reliable than ON CONFLICT for complex updates
            print(f"🔄 Updating existing tender with tender_no={tender_data['tender_no']}")
            
            # Build SET clause and parameters for UPDATE
            set_items = []
            params = []
            
            for col, val in tender_data.items():
                # Skip primary key columns for the SET clause
                if col not in ('tender_no', 'organization_id', 'publication_date'):
                    set_items.append(f"{col} = %s")
                    params.append(val)
            
            # Add WHERE clause parameters
            params.extend([
                tender_data['tender_no'],
                tender_data['organization_id'],
                tender_data['publication_date']
            ])
            
            # Execute update
            update_sql = f"""
                UPDATE tenders 
                SET {', '.join(set_items)} 
                WHERE tender_no = %s AND organization_id = %s AND publication_date = %s
            """
            
            cur.execute(update_sql, params)
            
        else:
            # INSERT for new records
            print(f"➕ Inserting new tender with tender_no={tender_data['tender_no']}")
            
            # Build INSERT statement
            columns = list(tender_data.keys())
            placeholders = ["%s"] * len(columns)
            values = [tender_data[col] for col in columns]
            
            insert_sql = f"""
                INSERT INTO tenders ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
            """
            
            cur.execute(insert_sql, values)
        
        # Commit and close
        conn.commit()
        
        # Verify the update worked
        verify_sql = """
            SELECT scrap_status FROM tenders 
            WHERE tender_no = %s AND organization_id = %s AND publication_date = %s
        """
        cur.execute(verify_sql, (
            tender_data['tender_no'], 
            tender_data['organization_id'], 
            tender_data['publication_date']
        ))
        result = cur.fetchone()
        print(f"✅ Verification: tender status is now '{result[0] if result else 'unknown'}'")
        
        cur.close()
        return True
        
    except Exception as e:
        print(f"⚠️ Error saving/updating tender: {e}")
        conn.rollback()
        return False

# Add a function to migrate existing dates to ROC format
def migrate_dates_to_roc_format(conn):
    """
    Migrate existing Gregorian dates in the database to ROC format
    """
    if not conn:
        print("❌ Cannot migrate dates without database connection.")
        return False
    
    try:
        cur = conn.cursor()
        
        # Check if we need migration by examining column type
        cur.execute("""
        SELECT data_type 
        FROM information_schema.columns 
        WHERE table_name = 'tenders' AND column_name = 'publication_date';
        """)
        column_type = cur.fetchone()[0]
        
        if column_type.upper() == 'DATE':
            print("🔄 Date column is still DATE type, performing schema migration...")
            
            # First, create backup tables
            print("📦 Creating backup of tenders table...")
            cur.execute("CREATE TABLE IF NOT EXISTS tenders_date_backup AS SELECT * FROM tenders;")
            
            # Get count of rows in backup
            cur.execute("SELECT COUNT(*) FROM tenders_date_backup;")
            backup_count = cur.fetchone()[0]
            print(f"✅ Backed up {backup_count} tender records")
            
            # Alter the table to change column types
            print("🔧 Altering table schema...")
            cur.execute("ALTER TABLE tenders ALTER COLUMN publication_date TYPE TEXT;")
            cur.execute("ALTER TABLE tenders ALTER COLUMN deadline TYPE TEXT;")
            
            # Now update all dates to ROC format
            print("🔄 Converting dates to ROC format...")
            
            # First get all tenders with dates
            cur.execute("""
            SELECT tender_no, organization_id, publication_date, deadline 
            FROM tenders_date_backup
            WHERE publication_date IS NOT NULL
            """)
            
            tenders_with_dates = cur.fetchall()
            print(f"📊 Found {len(tenders_with_dates)} tenders with dates to convert")
            
            # Update each tender's dates
            update_count = 0
            for tender_no, org_id, pub_date, deadline in tenders_with_dates:
                roc_pub_date = convert_to_roc_date(pub_date)
                roc_deadline = convert_to_roc_date(deadline) if deadline else None
                
                if roc_pub_date:
                    cur.execute("""
                    UPDATE tenders 
                    SET publication_date = %s, deadline = %s
                    WHERE tender_no = %s AND organization_id = %s
                    """, (roc_pub_date, roc_deadline, tender_no, org_id))
                    update_count += 1
            
            conn.commit()
            print(f"✅ Successfully migrated {update_count} tenders to ROC date format")
        else:
            print("✅ Date columns are already TEXT type, no schema migration needed.")
            
            # Check if there are any dates in Gregorian format that need conversion
            cur.execute("""
            SELECT COUNT(*) FROM tenders 
            WHERE publication_date ~ '^\d{4}-\d{2}-\d{2}$'
            """)
            
            gregorian_count = cur.fetchone()[0]
            
            if gregorian_count > 0:
                print(f"🔄 Found {gregorian_count} dates in Gregorian format (YYYY-MM-DD), converting to ROC...")
                
                # Get all tenders with Gregorian dates
                cur.execute("""
                SELECT tender_no, organization_id, publication_date, deadline 
                FROM tenders
                WHERE publication_date ~ '^\d{4}-\d{2}-\d{2}$'
                """)
                
                tenders_with_gregorian = cur.fetchall()
                
                # Update each tender's dates
                update_count = 0
                for tender_no, org_id, pub_date, deadline in tenders_with_gregorian:
                    try:
                        # Parse the Gregorian date string
                        gregorian_date = datetime.strptime(pub_date, "%Y-%m-%d").date()
                        roc_pub_date = convert_to_roc_date(gregorian_date)
                        
                        # Handle deadline if it exists
                        roc_deadline = None
                        if deadline and isinstance(deadline, str) and deadline.strip() and '-' in deadline:
                            try:
                                gregorian_deadline = datetime.strptime(deadline, "%Y-%m-%d").date()
                                roc_deadline = convert_to_roc_date(gregorian_deadline)
                            except:
                                pass
                        
                        if roc_pub_date:
                            cur.execute("""
                            UPDATE tenders 
                            SET publication_date = %s
                            WHERE tender_no = %s AND organization_id = %s
                            """, (roc_pub_date, tender_no, org_id))
                            
                            if roc_deadline:
                                cur.execute("""
                                UPDATE tenders 
                                SET deadline = %s
                                WHERE tender_no = %s AND organization_id = %s
                                """, (roc_deadline, tender_no, org_id))
                                
                            update_count += 1
                    except Exception as e:
                        print(f"⚠️ Error converting dates for tender {tender_no}: {e}")
                
                conn.commit()
                print(f"✅ Successfully converted {update_count} tenders from Gregorian to ROC date format")
            else:
                print("✅ All dates are already in ROC format.")
        
        return True
    
    except Exception as e:
        print(f"❌ Error during date migration: {e}")
        conn.rollback()
        return False

if __name__ == "__main__":
    # Check database status when run independently
    print("=" * 70)
    print("Database Status Check")
    print("=" * 70)
    
    # Get database connection
    conn = get_db_connection()
    if not conn:
        print("❌ Cannot connect to database. Exiting.")
        exit(1)
    
    try:
        # Check if we need to migrate dates
        print("\n🔍 Checking if date migration is needed...")
        migrate_dates_to_roc_format(conn)
        
        cur = conn.cursor()
        
        # Check if tables exist
        cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name IN ('organizations', 'tenders', 'tender_categories');
        """)
        existing_tables = [row[0] for row in cur.fetchall()]
        
        tables_need_setup = False
        
        if 'organizations' not in existing_tables or 'tenders' not in existing_tables or 'tender_categories' not in existing_tables:
            tables_need_setup = True
            print("🔍 Some tables are missing and need to be created.")
        else:
            # Check if tender_categories table has the expected schema
            cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default, 
                   (SELECT constraint_type FROM information_schema.table_constraints tc
                    JOIN information_schema.constraint_column_usage ccu 
                    ON tc.constraint_name = ccu.constraint_name
                    WHERE tc.table_name = c.table_name 
                    AND ccu.column_name = c.column_name 
                    AND tc.constraint_type = 'PRIMARY KEY')
            FROM information_schema.columns c
            WHERE table_name = 'tender_categories';
            """)
            category_columns = {row[0]: row for row in cur.fetchall()}
            
            # Check if the item_category in tenders table is a foreign key to tender_categories
            cur.execute("""
            SELECT tc.constraint_name, tc.constraint_type, kcu.column_name, 
                   ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
              ON tc.constraint_name = ccu.constraint_name
            WHERE tc.table_name = 'tenders'
              AND tc.constraint_type = 'FOREIGN KEY'
              AND kcu.column_name = 'item_category';
            """)
            item_category_fk = cur.fetchone()
            
            # Check schema matches expected structure
            schema_mismatch = False
            
            # Check tender_categories table schema
            if 'id' not in category_columns or category_columns['id'][4] != 'PRIMARY KEY':
                print("⚠️ tender_categories table schema mismatch: id should be PRIMARY KEY")
                schema_mismatch = True
            
            # Check tenders.item_category foreign key
            if not item_category_fk or item_category_fk[3] != 'tender_categories' or item_category_fk[4] != 'id':
                print("⚠️ Tenders table schema mismatch: item_category should be a foreign key to tender_categories(id)")
                schema_mismatch = True
            
            if schema_mismatch:
                print("🔍 Database schema needs to be updated.")
                tables_need_setup = True
            else:
                print("✅ Database tables exist with correct schema.")
        
        if tables_need_setup:
            print("Setting up database tables...")
            # Backup existing data if tables exist
            if 'organizations' in existing_tables:
                print("📁 Creating backup of existing organizations data...")
                cur.execute("CREATE TABLE IF NOT EXISTS organizations_backup AS SELECT * FROM organizations;")
                cur.execute("SELECT COUNT(*) FROM organizations_backup;")
                backup_count = cur.fetchone()[0]
                print(f"✅ Backed up {backup_count} organization records")
            
            if 'tenders' in existing_tables:
                print("📁 Creating backup of existing tenders data...")
                cur.execute("CREATE TABLE IF NOT EXISTS tenders_backup AS SELECT * FROM tenders;")
                cur.execute("SELECT COUNT(*) FROM tenders_backup;")
                backup_count = cur.fetchone()[0]
                print(f"✅ Backed up {backup_count} tender records")
                
                # Drop tables (in correct order due to foreign key)
                print("🗑️ Dropping existing tables...")
                cur.execute("DROP TABLE IF EXISTS tenders;")
                cur.execute("DROP TABLE IF EXISTS tender_categories;")
                cur.execute("DROP TABLE IF EXISTS organizations;")
                
            # Create fresh tables with correct schema
            setup_database(conn)
            print("✅ Database tables created successfully.")
            
            # Restore data if we had backups
            if 'organizations' in existing_tables:
                print("📤 Importing organizations data from backup...")
                cur.execute("""
                INSERT INTO organizations
                SELECT * FROM organizations_backup
                ON CONFLICT DO NOTHING;
                """)
                cur.execute("SELECT COUNT(*) FROM organizations;")
                imported_count = cur.fetchone()[0]
                print(f"✅ Re-imported {imported_count} organization records")
            
            if 'tenders' in existing_tables:
                print("📤 Importing tenders data from backup...")
                
                # We'll need to extract category information from the old item_category field
                cur.execute("""
                CREATE TABLE IF NOT EXISTS extracted_categories AS
                SELECT DISTINCT item_category FROM tenders_backup
                WHERE item_category IS NOT NULL AND item_category != '';
                """)
                
                # Get all the categories for processing
                cur.execute("SELECT item_category FROM extracted_categories;")
                categories = cur.fetchall()
                
                # Process and insert categories
                for (category_data,) in categories:
                    if not category_data or category_data.strip() == '':
                        continue
                        
                    try:
                        # Try to parse the category
                        lines = category_data.strip().split('\n')
                        if len(lines) < 2:
                            print(f"⚠️ Invalid category format: {category_data}")
                            continue
                        
                        category_type = lines[0].strip()
                        
                        # Parse the ID and name
                        parts = lines[1].strip().split(' - ', 1)
                        if len(parts) < 2:
                            print(f"⚠️ Cannot parse category ID and name from: {lines[1]}")
                            continue
                        
                        category_id = parts[0].strip()
                        name = parts[1].strip()
                        
                        # Insert the category
                        cur.execute(
                            "INSERT INTO tender_categories (id, name, category) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING",
                            (category_id, name, category_type)
                        )
                        print(f"✅ Imported category: {category_id} - {name} ({category_type})")
                    except Exception as e:
                        print(f"⚠️ Error processing category '{category_data}': {e}")
                
                conn.commit()
                
                # Get all columns except item_category from tenders_backup
                cur.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'tenders_backup' 
                AND column_name != 'item_category'
                ORDER BY ordinal_position;
                """)
                backup_columns = [row[0] for row in cur.fetchall()]
                
                # Get all columns except item_category from tenders
                cur.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'tenders' 
                AND column_name != 'item_category'
                ORDER BY ordinal_position;
                """)
                new_columns = [row[0] for row in cur.fetchall()]
                
                # Find common columns
                common_columns = [col for col in backup_columns if col in new_columns]
                
                # Insert data from backup, omitting item_category for now
                cur.execute(f"""
                INSERT INTO tenders ({', '.join(common_columns)})
                SELECT {', '.join(common_columns)}
                FROM tenders_backup
                WHERE tender_no IS NOT NULL
                AND organization_id IS NOT NULL
                AND publication_date IS NOT NULL
                ON CONFLICT DO NOTHING;
                """)
                
                # Now update item_category for each tender, parsing the ID from the original value
                cur.execute("""
                SELECT tender_no, organization_id, publication_date, item_category 
                FROM tenders_backup
                WHERE item_category IS NOT NULL AND item_category != '';
                """)
                
                tenders_with_categories = cur.fetchall()
                updated_count = 0
                
                for tender_no, org_id, pub_date, category_data in tenders_with_categories:
                    try:
                        # Parse the category_id from category_data
                        lines = category_data.strip().split('\n')
                        if len(lines) < 2:
                            continue
                        
                        parts = lines[1].strip().split(' - ', 1)
                        if len(parts) < 2:
                            continue
                        
                        category_id = parts[0].strip()
                        
                        # Update the tender with the category_id reference
                        cur.execute("""
                        UPDATE tenders
                        SET item_category = %s
                        WHERE tender_no = %s AND organization_id = %s AND publication_date = %s;
                        """, (category_id, tender_no, org_id, pub_date))
                        
                        updated_count += 1
                    except Exception as e:
                        print(f"⚠️ Error updating category for tender {tender_no}: {e}")
                
                conn.commit()
                print(f"✅ Updated item_category for {updated_count} tenders")
                
                cur.execute("SELECT COUNT(*) FROM tenders;")
                imported_count = cur.fetchone()[0]
                print(f"✅ Re-imported {imported_count} tender records")
                
                # Check if any records were lost
                cur.execute("SELECT COUNT(*) FROM tenders_backup;")
                backup_count = cur.fetchone()[0]
                if imported_count < backup_count:
                    print(f"⚠️ Warning: {backup_count - imported_count} tender records could not be imported due to NULL values in primary key columns")
                
                # Update all existing records to have scrap_status='found' if they don't have details
                cur.execute("""
                UPDATE tenders
                SET scrap_status = 'found'
                WHERE scrap_status IS NULL OR scrap_status = ''
                OR (scrap_status != 'finished' AND tender_method IS NULL);
                """)
                cur.execute("SELECT COUNT(*) FROM tenders WHERE scrap_status = 'found';")
                found_count = cur.fetchone()[0]
                print(f"📊 Set {found_count} tenders to 'found' status for detail collection")
                
        else:
            print("✅ Database tables already exist.")
            
            # Count records in each table
            cur.execute("SELECT COUNT(*) FROM organizations;")
            org_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM tender_categories;")
            category_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM tenders;")
            tender_count = cur.fetchone()[0]
            
            print(f"\nDatabase Statistics:")
            print(f"📊 Organizations: {org_count} records")
            print(f"📊 Tender Categories: {category_count} records")
            print(f"📊 Tenders: {tender_count} records")
            
            # Additional statistics
            if tender_count > 0:
                cur.execute("""
                SELECT COUNT(DISTINCT organization_id) 
                FROM tenders
                WHERE organization_id IS NOT NULL;
                """)
                org_with_tenders = cur.fetchone()[0]
                
                cur.execute("""
                SELECT category, COUNT(*) 
                FROM tender_categories 
                GROUP BY category
                ORDER BY category;
                """)
                category_counts = cur.fetchall()
                
                cur.execute("""
                SELECT scrap_status, COUNT(*) 
                FROM tenders 
                GROUP BY scrap_status
                ORDER BY scrap_status;
                """)
                status_counts = cur.fetchall()
                
                print(f"📊 Organizations with tenders: {org_with_tenders}")
                
                print("\nCategory type distribution:")
                for category, count in category_counts:
                    print(f"  - {category}: {count} categories")
                
                print("\nTender status distribution:")
                for status, count in status_counts:
                    status_label = status or "NULL"
                    print(f"  - {status_label}: {count} records")
                
                # Show most recent tender date
                cur.execute("""
                SELECT MAX(publication_date) 
                FROM tenders
                WHERE publication_date IS NOT NULL;
                """)
                latest_date = cur.fetchone()[0]
                if latest_date:
                    print(f"\n📅 Most recent tender date: {latest_date}")
        
    except Exception as e:
        print(f"❌ Error checking database status: {e}")
    finally:
        conn.close()
        
    print("=" * 70)