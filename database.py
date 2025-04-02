import os
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

    # Create merged tenders table with a composite primary key and url as unique
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tenders (
        organization_id TEXT,
        tender_no TEXT,
        url TEXT UNIQUE,
        project_name TEXT,
        publication_date DATE,
        deadline DATE,
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
        item_category TEXT,
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
    
    try:
        cur = conn.cursor()
        
        # Build dynamic SQL for insert/update
        columns = list(tender_data.keys())
        values = [tender_data[col] for col in columns]
        placeholders = ["%s"] * len(values)
        
        # Create the SET clause for UPDATE
        # Exclude primary key columns from update clause
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns 
                                 if col not in ('tender_no', 'organization_id', 'publication_date')])
        
        insert_sql = f"""
            INSERT INTO tenders ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT (tender_no, organization_id, publication_date) DO UPDATE SET
                {update_clause};
        """
        
        cur.execute(insert_sql, values)
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"⚠️ Error inserting/updating tender: {e}")
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
        cur = conn.cursor()
        
        # Check if tables exist
        cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name IN ('organizations', 'tenders');
        """)
        existing_tables = [row[0] for row in cur.fetchall()]
        
        tables_need_setup = False
        
        if 'organizations' not in existing_tables or 'tenders' not in existing_tables:
            tables_need_setup = True
            print("🔍 Some tables are missing and need to be created.")
        else:
            # Check if organization table has the expected schema
            cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default, 
                   (SELECT constraint_type FROM information_schema.table_constraints tc
                    JOIN information_schema.constraint_column_usage ccu 
                    ON tc.constraint_name = ccu.constraint_name
                    WHERE tc.table_name = c.table_name 
                    AND ccu.column_name = c.column_name 
                    AND tc.constraint_type = 'PRIMARY KEY')
            FROM information_schema.columns c
            WHERE table_name = 'organizations';
            """)
            org_columns = {row[0]: row for row in cur.fetchall()}
            
            # Check if tenders table has the expected schema with composite primary key
            cur.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = 'tenders'
            AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position;
            """)
            primary_key_columns = [row[0] for row in cur.fetchall()]
            
            # Check if url has a unique constraint
            cur.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = 'tenders'
            AND tc.constraint_type = 'UNIQUE'
            AND kcu.column_name = 'url';
            """)
            url_unique = cur.fetchone() is not None
            
            # Check if schema matches expected structure
            schema_mismatch = False
            
            # Check organization table schema
            if 'site_id' not in org_columns or org_columns['site_id'][4] != 'PRIMARY KEY':
                print("⚠️ Organizations table schema mismatch: site_id should be PRIMARY KEY")
                schema_mismatch = True
            
            # Check tenders table composite primary key
            expected_pk_columns = ['tender_no', 'organization_id', 'publication_date']
            if set(primary_key_columns) != set(expected_pk_columns):
                print(f"⚠️ Tenders table schema mismatch: primary key should be {expected_pk_columns}, but found {primary_key_columns}")
                schema_mismatch = True
            
            # Check URL unique constraint
            if not url_unique:
                print("⚠️ Tenders table schema mismatch: url should have a UNIQUE constraint")
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
                # Handle potential NULL values in PK columns
                cur.execute("""
                INSERT INTO tenders
                SELECT * FROM tenders_backup
                WHERE tender_no IS NOT NULL
                AND organization_id IS NOT NULL
                AND publication_date IS NOT NULL
                ON CONFLICT DO NOTHING;
                """)
                cur.execute("SELECT COUNT(*) FROM tenders;")
                imported_count = cur.fetchone()[0]
                print(f"✅ Re-imported {imported_count} tender records")
                
                # Check if any records were lost
                cur.execute("SELECT COUNT(*) FROM tenders_backup;")
                backup_count = cur.fetchone()[0]
                if imported_count < backup_count:
                    print(f"⚠️ Warning: {backup_count - imported_count} tender records could not be imported due to NULL values in primary key columns")
        else:
            print("✅ Database tables already exist.")
            
            # Count records in each table
            cur.execute("SELECT COUNT(*) FROM organizations;")
            org_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM tenders;")
            tender_count = cur.fetchone()[0]
            
            print(f"\nDatabase Statistics:")
            print(f"📊 Organizations: {org_count} records")
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
                SELECT scrap_status, COUNT(*) 
                FROM tenders 
                GROUP BY scrap_status
                ORDER BY scrap_status;
                """)
                status_counts = cur.fetchall()
                
                print(f"📊 Organizations with tenders: {org_with_tenders}")
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