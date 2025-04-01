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

    # Create merged tenders table with url as primary key and a unique constraint on the combination
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tenders (
        organization_id TEXT,
        tender_no TEXT,
        url TEXT PRIMARY KEY,
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
        UNIQUE (organization_id, tender_no, url),
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
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns 
                                 if col != 'url'])
        
        insert_sql = f"""
            INSERT INTO tenders ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT (url) DO UPDATE SET
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