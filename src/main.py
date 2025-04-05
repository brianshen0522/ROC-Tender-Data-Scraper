import concurrent.futures
import os
import time
import argparse
from dotenv import load_dotenv

from .database.database import get_db_connection, ensure_connection, setup_database, get_organization_id, save_organization, check_tender_status, save_tender
from .scraper.scraper import setup_selenium_driver, fetch_org_id_from_site, fetch_tender_details, check_page_data_loaded, extract_tender_info
from .scraper.captcha_solver import handle_captcha
from .utils.utils import setup_debug_directory

import sys
sys.stdout.reconfigure(encoding='utf-8')

# Load environment variables from .env file
load_dotenv()

def process_tender_row(row_index, row, org_site_id_cache, conn_params):
    """Process a single tender row with its own database connection"""
    # Create a new database connection for this thread
    try:
        conn = get_db_connection()
        if not conn:
            print(f"❌ Thread {row_index}: Database connection failed.")
            return {'success': False, 'message': 'Database connection failed'}
        
        # Extract tender info from the row
        tender_info = extract_tender_info(row)
        if not tender_info:
            conn.close()
            return {'success': False, 'message': 'Could not extract tender info'}
        
        org_name = tender_info["org_name"]
        tender_no = tender_info["tender_no"]
        project_name = tender_info["project_name"]
        detail_link = tender_info["detail_link"]
        pk_pms_main = tender_info["pk_pms_main"]
        pub_date = tender_info["pub_date"]  # This is in ROC format
        deadline = tender_info["deadline"]  # This is in ROC format
        
        # Skip tenders without publication date (required for primary key)
        if pub_date is None or (isinstance(pub_date, str) and pub_date.strip() == ''):
            conn.close()
            return {'success': False, 'message': f'Skipping tender - missing or empty publication date', 'tender_no': tender_no}
        
        # Check if organization is in the cache first
        org_site_id = None
        if org_name in org_site_id_cache:
            org_site_id = org_site_id_cache[org_name]
        else:
            # Check if organization exists in DB
            org_site_id = get_organization_id(conn, org_name)
            # Only cache valid org_site_ids
            if org_site_id:
                org_site_id_cache[org_name] = org_site_id
        
        # If we don't have an org_site_id, this needs to be processed in the main thread
        if not org_site_id:
            conn.close()
            return {
                'success': False, 
                'message': 'Organization site ID not found in DB', 
                'tender_no': tender_no,
                'org_name': org_name,
                'need_org_id': True,
                'tender_info': tender_info
            }
        
        # Check if this tender already exists with a "finished" status
        existing_status = check_tender_status(conn, detail_link)
        if existing_status == "finished":
            conn.close()
            return {'success': True, 'message': f'Skipping tender - already processed completely', 'tender_no': tender_no, 'status': 'skipped'}
        
        # Prepare basic data for initial insertion
        basic_data = {
            "organization_id": org_site_id,
            "tender_no": tender_no,
            "project_name": project_name,
            "publication_date": pub_date,
            "deadline": deadline,
            "url": detail_link,
            "pk_pms_main": pk_pms_main,
            "scrap_status": "found",
            "org_name": org_name
        }
        
        # Save tender data to database
        if save_tender(conn, basic_data):
            status = 'new' if existing_status != 'found' else 'updated'
            conn.close()
            return {'success': True, 'message': f'Saved basic tender info', 'tender_no': tender_no, 'status': status}
        else:
            conn.close()
            return {'success': False, 'message': f'Failed to save basic tender info', 'tender_no': tender_no}
        
    except Exception as e:
        if 'conn' in locals() and conn:
            try:
                conn.rollback()
                conn.close()
            except:
                pass
        return {'success': False, 'message': f'Error processing row: {str(e)}', 'error': str(e)}

def discovery_phase(driver, conn, query_sentence, time_range, page_size, keep_debug_files, headless=False, max_workers=10):
    """Phase 1: Find all tenders and save basic information with 'found' status using multi-threading"""
    print("\n" + "="*70)
    print("PHASE 1: TENDER DISCOVERY (MULTI-THREADED)")
    print("="*70)
    
    # Construct the base URL with the provided parameters
    base_url = (f"https://web.pcc.gov.tw/prkms/tender/common/bulletion/readBulletion?"
               f"querySentence={query_sentence}&tenderStatusType=%E6%8B%9B%E6%A8%99&"
               f"sortCol=TENDER_NOTICE_DATE&timeRange={time_range}&pageSize={page_size}")
    
    current_page = 1
    more_pages = True
    tender_count = 0
    
    # Create a cache for organization site IDs to reduce DB lookups
    org_site_id_cache = {}
    
    # Get connection parameters for threads
    conn_params = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT")
    }
    
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

            # Check if data is loaded correctly
            rows, more_pages, driver, advance_page = check_page_data_loaded(
                driver, 
                page_size, 
                base_url,
                current_url,
                query_sentence,
                time_range,
                headless
            )
            
            # Skip if no rows found
            if not rows:
                print("⚠️ No tender rows found on this page")
                if advance_page:
                    current_page += 1
                continue
                
            print(f"Found {len(rows)} tenders on this page - processing with multi-threading")
            
            # Process tender rows using multi-threading
            successful_tenders = 0
            skipped_tenders = 0
            failed_tenders = 0
            
            # List to store tasks that need organization ID fetching
            needs_org_id = []
            
            # Use ThreadPoolExecutor to process rows in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_row = {
                    executor.submit(process_tender_row, idx, row, org_site_id_cache, conn_params): idx
                    for idx, row in enumerate(rows)
                }
                
                # Process results as they complete
                for future in concurrent.futures.as_completed(future_to_row):
                    row_idx = future_to_row[future]
                    try:
                        result = future.result()
                        
                        # Handle results
                        if result.get('success'):
                            if result.get('status') == 'skipped':
                                print(f"✅ [{row_idx+1}/{len(rows)}] {result.get('message')} '{result.get('tender_no')}'")
                                skipped_tenders += 1
                            else:
                                print(f"💾 [{row_idx+1}/{len(rows)}] {result.get('message')} for '{result.get('tender_no')}'")
                                successful_tenders += 1
                                if result.get('status') == 'new':
                                    tender_count += 1
                        else:
                            if result.get('need_org_id'):
                                # Save for processing in the main thread
                                needs_org_id.append(result)
                            else:
                                print(f"⚠️ [{row_idx+1}/{len(rows)}] {result.get('message')} for '{result.get('tender_no', 'unknown')}'")
                                failed_tenders += 1
                    except Exception as e:
                        print(f"⚠️ Error processing result for row {row_idx+1}: {str(e)}")
                        failed_tenders += 1
            
            # Process tenders that need organization IDs (can't be done in threads)
            if needs_org_id:
                print(f"🔄 Processing {len(needs_org_id)} tenders that need organization IDs...")
                
                # Ensure main connection is active
                conn = ensure_connection(conn)
                if not conn:
                    print("❌ Database connection failed. Exiting.")
                    return tender_count, driver
                
                for result in needs_org_id:
                    try:
                        org_name = result['org_name']
                        tender_info = result['tender_info']
                        tender_no = tender_info["tender_no"]
                        
                        print(f"🏢 Fetching site ID for org: '{org_name}'")
                        org_site_id = fetch_org_id_from_site(driver, org_name)
                        
                        if org_site_id:
                            print(f"🏢 Got org site ID for '{org_name}': {org_site_id}")
                            # Cache the org_site_id for future use
                            org_site_id_cache[org_name] = org_site_id
                            
                            # Save organization to DB
                            save_organization(conn, org_site_id, org_name)
                            
                            # Prepare basic data
                            basic_data = {
                                "organization_id": org_site_id,
                                "tender_no": tender_no,
                                "project_name": tender_info["project_name"],
                                "publication_date": tender_info["pub_date"],
                                "deadline": tender_info["deadline"],
                                "url": tender_info["detail_link"],
                                "pk_pms_main": tender_info["pk_pms_main"],
                                "scrap_status": "found",
                                "org_name": org_name
                            }
                            
                            # Save tender data
                            if save_tender(conn, basic_data):
                                print(f"💾 Saved basic tender info for '{tender_no}'")
                                successful_tenders += 1
                                tender_count += 1
                            else:
                                print(f"⚠️ Failed to save basic tender info for '{tender_no}'")
                                failed_tenders += 1
                        else:
                            print(f"⚠️ Skipping tender - site ID not found for org: {org_name}")
                            failed_tenders += 1
                    except Exception as e:
                        print(f"⚠️ Error processing organization ID: {e}")
                        failed_tenders += 1
            
            # Print summary for this page
            print(f"📊 Page {current_page} summary: {successful_tenders} saved, {skipped_tenders} skipped, {failed_tenders} failed")
            
            # Check if we should advance to the next page
            if advance_page:
                current_page += 1
                print(f"✅ Advancing to page {current_page}")
            else:
                print(f"🔄 Staying on page {current_page} for retry with fresh browser")
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