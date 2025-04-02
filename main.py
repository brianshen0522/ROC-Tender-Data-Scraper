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

def main():
    """Main function to scrape tender data with configurable parameters"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Scrape tender data from PCC website with pagination.')
    parser.add_argument('--query', type=str, default=os.getenv('DEFAULT_QUERY', '案'), 
                      help='Query sentence for tender search (default: from .env or 案)')
    parser.add_argument('--time', type=str, default=os.getenv('DEFAULT_TIME_RANGE', '113'), 
                      help='Republic of China era year to search (default: from .env or 113)')
    parser.add_argument('--size', type=int, default=int(os.getenv('DEFAULT_PAGE_SIZE', '100')), 
                      help='Page size for results (default: from .env or 100, max: 100)')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    parser.add_argument('--keep-debug', action='store_true', 
                        help='Keep debug images (default: delete after CAPTCHA solving)')
    # Parse arguments
    args = parser.parse_args()
    
    # Ensure page size doesn't exceed maximum
    page_size = min(args.size, 100)
    query_sentence = args.query
    time_range = args.time
    headless = args.headless
    keep_debug_files = args.keep_debug
    
    print(f"Starting scraper with parameters:")
    print(f"  Query Sentence: {query_sentence}")
    print(f"  Time Range: {time_range}")  
    print(f"  Page Size: {page_size}")
    print(f"  Headless Mode: {headless}")
    print(f"  Keep Debug Files: {keep_debug_files}")
    
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
    
    # Construct the base URL with the provided parameters
    base_url = (f"https://web.pcc.gov.tw/prkms/tender/common/bulletion/readBulletion?"
               f"querySentence={query_sentence}&tenderStatusType=%E6%8B%9B%E6%A8%99&"
               f"sortCol=TENDER_NOTICE_DATE&timeRange={time_range}&pageSize={page_size}&onlyOrgAndTenderName=true")
    
    current_page = 1
    more_pages = True
    
    try:
        while more_pages:
            # Construct URL with pagination parameter
            if current_page == 1:
                url = base_url
                print(f"🔍 Starting search with parameters: query='{query_sentence}', year={time_range}")
            else:
                url = f"{base_url}&d-3611040-p={current_page}"
                print(f"📄 Loading page {current_page}: {url}")
                
            driver.get(url)
            time.sleep(1)

            # Handle CAPTCHA if present
            handle_captcha(driver, keep_debug_files)

            # Check if data is loaded correctly
            rows, more_pages = check_page_data_loaded(driver, page_size)
            
            # Process tender rows
            for row_index, row in enumerate(rows):
                try:
                    # Ensure database connection is active
                    conn = ensure_connection(conn)
                    if not conn:
                        print("❌ Database connection failed. Exiting.")
                        return
                    
                    # Extract tender info from the row
                    tender_info = extract_tender_info(row)
                    if not tender_info:
                        continue
                    
                    org_name = tender_info["org_name"]
                    tender_no = tender_info["tender_no"]
                    project_name = tender_info["project_name"]
                    detail_link = tender_info["detail_link"]
                    pk_pms_main = tender_info["pk_pms_main"]
                    pub_date = tender_info["pub_date"]
                    deadline = tender_info["deadline"]
                    
                    print(f"Processing tender {row_index+1}/{len(rows)}: '{tender_no}'")
                    
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
                        print(f"✅ Skipping tender '{tender_no}' from org '{org_site_id}' with URL '{detail_link}' - already processed completely")
                        continue
                    elif existing_status:
                        print(f"🔄 Re-processing tender '{tender_no}' from org '{org_site_id}' with URL '{detail_link}' - previous status: '{existing_status}'")
                    else:
                        print(f"🆕 New tender: '{tender_no}' from org '{org_site_id}' with URL '{detail_link}'")
                    
                    # Fetch extended tender details
                    detail_data = {}
                    status = "failed"
                    try:
                        detail_data = fetch_tender_details(driver, pk_pms_main)
                        # Use a simple check—if a key like "tender_method" exists, mark as finished.
                        status = "finished" if detail_data.get("tender_method") else "failed"
                    except Exception as e:
                        print(f"⚠️ Error visiting detail page: {e}")
                    
                    # Prepare data for insertion/update in merged table
                    merged_data = {
                        "organization_id": org_site_id,
                        "tender_no": tender_no,
                        "project_name": project_name,
                        "publication_date": pub_date,
                        "deadline": deadline,
                        "url": detail_link,
                        "scrap_status": status
                    }
                    
                    # Add all detail data to merged data
                    merged_data.update(detail_data)
                    
                    # Save tender data to database
                    if save_tender(conn, merged_data):
                        print(f"💾 Saved tender info for '{tender_no}' from org '{org_site_id}' with URL '{detail_link}'")
                    else:
                        print(f"⚠️ Failed to save tender info for '{tender_no}'")
                
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
            
            # Move to the next page
            current_page += 1
            
            # Add a short delay before loading the next page
            time.sleep(1)
            
            # If we've processed all pages, exit the loop
            if not more_pages:
                print("Reached the last page of results.")
                break

    except KeyboardInterrupt:
        print("\n🛑 Scraping stopped by user.")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        # Cleanup
        print("🧹 Cleaning up resources...")
        driver.quit()
        if conn:
            conn.close()
        print("✨ Done scraping - all complete! ✨")

if __name__ == "__main__":
    main()