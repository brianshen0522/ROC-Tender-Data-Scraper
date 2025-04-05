import os
import time
import argparse
from dotenv import load_dotenv

from utils import setup_debug_directory
from database import get_db_connection, ensure_connection, setup_database, get_organization_id, save_organization, check_tender_status, save_tender
from scraper import setup_selenium_driver, fetch_org_id_from_site, fetch_tender_details, check_page_data_loaded, extract_tender_info
from captcha_solver import handle_captcha

import sys
sys.stdout.reconfigure(encoding='utf-8')

# Load environment variables from .env file
load_dotenv()

def discovery_phase(driver, conn, query_sentence, time_range, page_size, keep_debug_files, headless=False):
    """Phase 1: Find all tenders and save basic information with 'found' status"""
    print("\n" + "="*70)
    print("PHASE 1: TENDER DISCOVERY")
    print("="*70)
    
    # Construct the base URL with the provided parameters
    base_url = (f"https://web.pcc.gov.tw/prkms/tender/common/bulletion/readBulletion?"
               f"querySentence={query_sentence}&tenderStatusType=%E6%8B%9B%E6%A8%99&"
               f"sortCol=TENDER_NOTICE_DATE&timeRange={time_range}&pageSize={page_size}")
    
    current_page = 1
    more_pages = True
    tender_count = 0
    
    try:
        while more_pages:
            # Construct URL with pagination parameter
            if current_page == 1:
                current_url = base_url
                print(f"🔍 Starting search with parameters: query='{query_sentence}', year={time_range}")
            else:
                current_url = f"{base_url}&d-3611040-p={current_page}"
                print(f"📄 Loading page {current_page}: {current_url}")
                
            driver.get(current_url)
            time.sleep(1)

            # Handle CAPTCHA if present
            handle_captcha(driver, keep_debug_files)

            # Check if data is loaded correctly - now returns potentially updated driver
            # and a flag indicating whether to advance page
            rows, more_pages, driver, advance_page = check_page_data_loaded(
                driver, 
                page_size, 
                base_url,           # Base URL for initial search
                current_url,        # Current paginated URL
                query_sentence,     # Query parameters for establishing a new session
                time_range,         # Time range for search
                headless
            )
            
            # Process tender rows
            for row_index, row in enumerate(rows):
                try:
                    # Ensure database connection is active
                    conn = ensure_connection(conn)
                    if not conn:
                        print("❌ Database connection failed. Exiting.")
                        return tender_count, driver
                    
                    # Extract tender info from the row
                    tender_info = extract_tender_info(row)
                    if not tender_info:
                        continue
                    
                    org_name = tender_info["org_name"]
                    tender_no = tender_info["tender_no"]
                    project_name = tender_info["project_name"]
                    detail_link = tender_info["detail_link"]
                    pk_pms_main = tender_info["pk_pms_main"]
                    pub_date = tender_info["pub_date"]  # This is now in ROC format
                    deadline = tender_info["deadline"]  # This is now in ROC format
                    
                    # Skip tenders without publication date (required for primary key)
                    if pub_date is None:
                        print(f"⚠️ Skipping tender '{tender_no}' - missing publication date")
                        continue
                    elif isinstance(pub_date, str) and pub_date.strip() == '':
                        print(f"⚠️ Skipping tender '{tender_no}' - empty publication date")
                        continue
                    
                    print(f"Discovering tender {row_index+1}/{len(rows)}: '{tender_no}'")
                    
                    # Check if organization exists in DB
                    org_site_id = get_organization_id(conn, org_name)
                    if not org_site_id:
                        org_site_id = fetch_org_id_from_site(driver, org_name)
                        print(f"🏢 Got org site ID for '{org_name}': {org_site_id}")
                        if org_site_id:
                            save_organization(conn, org_site_id, org_name)
                        else:
                            print(f"⚠️ Skipping tender — site ID not found for org: {org_name}")
                            continue
                    
                    # Check if this tender already exists with a "finished" status
                    existing_status = check_tender_status(conn, detail_link)
                    if existing_status == "finished":
                        print(f"✅ Skipping tender '{tender_no}' - already processed completely")
                        continue
                    elif existing_status == "found":
                        print(f"📋 Tender '{tender_no}' already discovered, keeping 'found' status")
                    else:
                        print(f"🆕 New tender: '{tender_no}' from org '{org_site_id}'")
                        tender_count += 1
                    
                    # Prepare basic data for initial insertion
                    basic_data = {
                        "organization_id": org_site_id,
                        "tender_no": tender_no,
                        "project_name": project_name,
                        "publication_date": pub_date,  # Use the ROC date string
                        "deadline": deadline,          # Use the ROC date string
                        "url": detail_link,
                        "pk_pms_main": pk_pms_main,  # Store this for later detail fetch
                        "scrap_status": "found",
                        "org_name": org_name
                    }
                    
                    # Save tender data to database
                    if save_tender(conn, basic_data):
                        print(f"💾 Saved basic tender info for '{tender_no}'")
                    else:
                        print(f"⚠️ Failed to save basic tender info for '{tender_no}'")
                
                except Exception as e:
                    print(f"⚠️ Error processing row: {e}")
                    # Try to roll back and refresh connection in case it was in a failed transaction
                    try:
                        if conn:
                            conn.rollback()
                        conn = ensure_connection(conn)
                    except Exception as e2:
                        print(f"⚠️ Error recovering from row processing error: {e2}")
                    continue
            
            # Check if we should advance to the next page
            if advance_page:
                # Move to the next page
                current_page += 1
                print(f"✅ Advancing to page {current_page}")
            else:
                print(f"🔄 Staying on page {current_page} for retry with fresh browser")
                # Give a short pause before trying again with the fresh browser
                time.sleep(2)
            
            # Add a short delay before loading the next page
            time.sleep(1)
            
            # If we've processed all pages, exit the loop
            if not more_pages:
                print("Reached the last page of results.")
                break

    except KeyboardInterrupt:
        print("\n🛑 Tender discovery stopped by user.")
    except Exception as e:
        print(f"❌ Unexpected error during discovery: {e}")
        
    print(f"Discovery phase completed. Found {tender_count} new tenders.")
    return tender_count, driver

def detail_phase(driver, conn, keep_debug_files):
    """Phase 2: Fetch detailed information for all tenders with 'found' status"""
    print("\n" + "="*70)
    print("PHASE 2: FETCH TENDER DETAILS")
    print("="*70)
    
    # Get all tenders with 'found' status
    cur = conn.cursor()
    cur.execute("""
    SELECT tender_no, organization_id, url, pk_pms_main, publication_date
    FROM tenders 
    WHERE scrap_status = 'found'
    ORDER BY publication_date DESC
    """)
    
    tenders_to_process = cur.fetchall()
    total_tenders = len(tenders_to_process)
    
    print(f"Found {total_tenders} tenders needing detailed information.")
    
    if total_tenders == 0:
        print("No tenders need detailed information. Skipping detail phase.")
        return 0
    
    processed_count = 0
    success_count = 0
    
    for index, (tender_no, org_id, url, pk_pms_main, publication_date) in enumerate(tenders_to_process):
        try:
            # Ensure database connection is active
            conn = ensure_connection(conn)
            if not conn:
                print("❌ Database connection failed. Exiting detail phase.")
                break
            
            print(f"Fetching details [{index+1}/{total_tenders}]: Tender '{tender_no}' from org '{org_id}'")
            
            # Fetch extended tender details
            detail_data = {}
            status = "failed"
            try:
                detail_data = fetch_tender_details(driver, pk_pms_main)
                # Use a simple check—if a key like "tender_method" exists, mark as finished.
                status = "finished" if detail_data.get("tender_method") else "failed"
                if status == "finished":
                    success_count += 1
            except Exception as e:
                print(f"⚠️ Error visiting detail page: {e}")
            
            # Prepare data for update - include all primary key fields
            detail_data["tender_no"] = tender_no
            detail_data["organization_id"] = org_id
            detail_data["publication_date"] = publication_date
            detail_data["scrap_status"] = status
            
            # Keep the pk_pms_main value for future reference
            if "pk_pms_main" not in detail_data:
                detail_data["pk_pms_main"] = pk_pms_main
            
            # Debug output
            print(f"  Primary key values: tender_no={tender_no}, org_id={org_id}, pub_date={publication_date}")
            print(f"  Setting status to: {status}")
            
            # Update tender data in database
            if save_tender(conn, detail_data):
                print(f"💾 Updated tender info with details for '{tender_no}', status: {status}")
                processed_count += 1
            else:
                print(f"⚠️ Failed to update tender info for '{tender_no}'")
            
            # Handle CAPTCHA occasionally to avoid detection
            if index % 20 == 19:  # every 20 tenders
                driver.get("https://web.pcc.gov.tw/prkms/tender/common/bulletion/readBulletion")
                handle_captcha(driver, keep_debug_files)
                time.sleep(2)
            
            # Add a short delay between requests to avoid overwhelming the server
            time.sleep(0.5)
            
        except KeyboardInterrupt:
            print("\n🛑 Detail fetching stopped by user.")
            break
        except Exception as e:
            print(f"⚠️ Error processing tender '{tender_no}': {e}")
            # Try to roll back and refresh connection
            try:
                if conn:
                    conn.rollback()
                conn = ensure_connection(conn)
            except Exception as e2:
                print(f"⚠️ Error recovering from tender processing error: {e2}")
            continue
    
    print(f"Detail phase completed. Processed {processed_count}/{total_tenders} tenders, {success_count} successfully.")
    return processed_count

def main():
    """Main function using the two-phase approach to scrape tender data"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Scrape tender data from PCC website with a two-phase approach.')
    parser.add_argument('--query', type=str, default=os.getenv('DEFAULT_QUERY', '案'), 
                      help='Query sentence for tender search (default: from .env or 案)')
    parser.add_argument('--time', type=str, default=os.getenv('DEFAULT_TIME_RANGE', '113'), 
                      help='Republic of China era year to search (default: from .env or 113)')
    parser.add_argument('--size', type=int, default=int(os.getenv('DEFAULT_PAGE_SIZE', '100')), 
                      help='Page size for results (default: from .env or 100, max: 100)')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    parser.add_argument('--keep-debug', action='store_true', 
                        help='Keep debug images (default: delete after CAPTCHA solving)')
    parser.add_argument('--phase', type=str, choices=['discovery', 'detail', 'both'], default='both',
                      help='Run only discovery phase, only detail phase, or both (default: both)')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Ensure page size doesn't exceed maximum
    page_size = min(args.size, 100)
    query_sentence = args.query
    time_range = args.time
    headless = args.headless
    keep_debug_files = args.keep_debug
    phase = args.phase
    
    print(f"Starting scraper with parameters:")
    print(f"  Query Sentence: {query_sentence}")
    print(f"  Time Range: {time_range}")  
    print(f"  Page Size: {page_size}")
    print(f"  Headless Mode: {headless}")
    print(f"  Keep Debug Files: {keep_debug_files}")
    print(f"  Phase: {phase}")
    
    # Create debug directory
    setup_debug_directory()
    
    # Initialize database connection
    conn = get_db_connection()
    if not conn:
        print("❌ Cannot proceed without database connection. Exiting.")
        return
    
    # Set up database tables
    if not setup_database(conn):
        print("❌ Cannot proceed without database setup. Exiting.")
        return
    
    # Set up Selenium WebDriver
    driver = setup_selenium_driver(headless=headless)
    
    try:
        # Run the requested phases
        if phase in ['discovery', 'both']:
            tender_count, driver = discovery_phase(driver, conn, query_sentence, time_range, page_size, keep_debug_files, headless)
        
        if phase in ['detail', 'both']:
            # Make sure we're using the potentially updated driver from the discovery phase
            detail_phase(driver, conn, keep_debug_files)
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        # Cleanup
        print("\n" + "="*70)
        print("🧹 Cleaning up resources...")
        driver.quit()
        if conn:
            conn.close()
        print("✨ Done scraping - all complete! ✨")

if __name__ == "__main__":
    main()