import time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from .captcha_solver import handle_captcha
from ..utils.utils import parse_roc_date

def setup_selenium_driver(headless=False):
    """Set up and return a configured Selenium WebDriver with logs disabled."""
    # Suppress Selenium's own logging
    # logging.getLogger('selenium').setLevel(logging.WARNING)
    
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.add_argument("--disable-cache")
    chrome_options.add_argument("--log-level=3")  # Additional Chrome log suppression
    
    if headless:
        chrome_options.add_argument("--headless")
    
    print("🔐 Setting up secure browser session...")
    driver = webdriver.Chrome(options=chrome_options)
    driver.maximize_window()
    # Execute CDP commands to avoid detection
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            })
        """
    })
    print("🛡️ Browser anti-detection measures activated")
    
    return driver

def fetch_org_id_from_site(driver, org_name, max_retries=5):
    """Open new tab and fetch organization site ID with retry mechanism"""
    main_window = driver.current_window_handle
    driver.execute_script("window.open('');")
    driver.switch_to.window(driver.window_handles[-1])
    
    org_id = None
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Load the organization search page
            driver.get("https://web.pcc.gov.tw/prkms/tender/common/orgName/search")
            
            # Handle CAPTCHA check
            handle_captcha(driver, False)
            
            # Wait for search input to be available
            search_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/div/div[2]/div/div[2]/div/form/table/tbody/tr/td[1]/input'))
            )
            
            # Enter organization name and submit form
            search_input.clear()  # Clear any existing text
            search_input.send_keys(org_name)
            form = driver.find_element(By.XPATH, '/html/body/div/div[2]/div/div[2]/div/form')
            driver.execute_script("arguments[0].submit();", form)
            
            # Wait for results and get org ID
            org_id_cell = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/div/div[2]/div/div[2]/div/table/tbody/tr[2]/td[1]'))
            )
            org_id = org_id_cell.text.strip()
            
            # If we successfully got the org ID, break the loop
            if org_id:
                print(f"✅ Found organization ID on attempt {retry_count + 1}/{max_retries}")
                break
                
        except Exception as e:
            # Increment retry counter
            retry_count += 1
            
            if retry_count < max_retries:
                print(f"⚠️ Failed to find organization ID on attempt {retry_count}/{max_retries}: {e}")
                print(f"🔄 Reloading page and trying again...")
                time.sleep(2)  # Add a short delay before retrying
            else:
                print(f"❌ Failed to fetch org ID for '{org_name}' after {max_retries} attempts: {e}")
    
    # Close the tab and switch back to main window
    driver.close()
    driver.switch_to.window(main_window)
    return org_id

def fetch_tender_details(driver, pk_pms_main):
    """Fetch tender details from detail page"""
    main_window = driver.current_window_handle
    driver.execute_script("window.open('');")
    driver.switch_to.window(driver.window_handles[-1])
    detail_url = f"https://web.pcc.gov.tw/tps/QueryTender/query/searchTenderDetail?pkPmsMain={pk_pms_main}"
    driver.get(detail_url)
    time.sleep(1)  # Wait for page to load

    # Handle CAPTCHA check on detail page
    handle_captcha(driver, False)

    # Define a mapping from the Chinese field labels (as shown on the page)
    # to column names we want in our database.
    fields_mapping = {
        "單位名稱": "org_name",
        "機關地址": "agency_address",
        "聯絡人": "contact_person",
        "聯絡電話": "contact_phone",
        "傳真號碼": "fax_number",
        "電子郵件信箱": "email",
        "採購資料": "procurement_data",
        "標案案號": "tender_id",
        "標案名稱": "tender_title",
        "標的分類": "item_category",  # This will be processed by get_or_create_category
        "財物採購性質": "nature_of_procurement",
        "採購金額級距": "procurement_amount_range",
        "辦理方式": "handling_method",
        "依據法條": "according_to_laws",
        "採購法第49": "procurement_act_49",
        "本採購是否屬「具敏感性或國安(含資安)疑慮之業務範疇」採購": "sensitive_procurement",
        "本採購是否屬「涉及國家安全」採購": "national_security_procurement",
        "預算金額": "budget_amount",
        "預算金額是否公開": "budget_public",
        "後續擴充": "subsequent_expansion",
        "是否受機關補助": "agency_subsidy",
        "是否為政策及業務宣導業務": "promotional_service",
        "招標方式": "tender_method",
        "決標方式": "awarding_method",
        "參考最有利標精神": "most_advantageous_bid_reference",
        "是否電子報價": "e_quotation",
        "新增公告傳輸次數": "announcement_transmission_count",
        "招標狀態": "tender_status",
        "是否複數決標": "multiple_awards",
        "是否訂有底價": "base_price_set",
        "價格是否納入評選": "price_included_in_evaluation",
        "所占配分或權重是否為20%以上": "weight_above_20_percent",
        "是否屬特殊採購": "special_procurement",
        "是否已辦理公開閱覽": "public_inspection_done",
        "是否屬統包": "package_tender",
        "是否屬共同供應契約採購": "joint_supply_contract",
        "是否屬二以上機關之聯合採購(不適用共同供應契約規定)": "joint_procurement",
        "是否應依公共工程專業技師簽證規則實施技師簽證": "engineer_certification",
        "是否採行協商措施": "negotiation_measures",
        "是否適用採購法第104條或105條或招標期限標準第10條或第4條之1": "applicable_procurement_law",
        "是否依據採購法第106條第1項第1款辦理": "processed_according_to_procurement_act",
        "是否提供電子領標": "e_tender",
        "是否提供電子投標": "e_bidding",
        "截止投標": "bid_deadline",
        "開標時間": "bid_opening_time",
        "開標地點": "bid_opening_location",
        "是否須繳納押標金": "bid_bond_required",
        "是否須繳納履約保證金": "performance_bond_required",
        "投標文字": "bid_text",
        "收受投標文件地點": "bid_document_collection_location",
    }

    detail_data = {}
    try:
        # Assuming that the detail information is presented in a table where field labels and values are in adjacent <td> elements.
        cells = driver.find_elements(By.XPATH, "//table//td")
        for i, cell in enumerate(cells):
            text = cell.text.strip()
            if text in fields_mapping:
                try:
                    # The next cell is assumed to contain the value.
                    value = cells[i+1].text.strip()
                except IndexError:
                    value = ""
                
                # Store the raw value
                detail_data[fields_mapping[text]] = value
                
                # Debug log for item_category field
                if text == "標的分類" and value:
                    print(f"Found item_category: {value}")
    except Exception as e:
        print("Error extracting tender details:", e)

    driver.close()
    driver.switch_to.window(main_window)
    return detail_data

def check_page_data_loaded(driver, page_size, base_url, current_url, query_sentence, time_range, headless=False, max_retries=5):
    """Check if the data is loaded correctly with browser restart mechanism"""
    current_retry = 0
    last_row_count = 0
    consistent_count = 0  # Track how many times we get the same count
    more_pages = True

    while current_retry < max_retries:
        # Get the rows
        rows = driver.find_elements(By.CSS_SELECTOR, "#bulletion > tbody > tr")
        row_count = len(rows)
        
        # If we have a full page, we're good - continue to next page
        if row_count >= page_size:
            print(f"📊 Found {row_count} tenders - will continue to next page after processing")
            break
        
        # If we have some data but less than the page size, it's the last page - don't retry
        if 0 < row_count < page_size:
            print(f"📑 Found {row_count} tenders which is less than page size - this is the last page")
            more_pages = False  # Signal this is the last page
            break
            
        # If no data found (row_count == 0), proceed with retry logic
        
        # If this is our last retry with zero rows, accept that result
        if current_retry == max_retries - 1:
            print(f"📑 Found 0 tenders after {max_retries} retries - this may be the last page")
            print("⚠️ No tenders found after maximum retries. Signaling to restart browser without advancing page.")
            more_pages = True  # Continue the loop, but...
            return rows, more_pages, driver, False  # Add a flag to indicate "don't advance page"
            
        # Otherwise, check if we have consistent counts for zero rows
        elif row_count == last_row_count and row_count == 0:
            # Only count consistency for zero results
            consistent_count += 1
            if consistent_count >= 3:  # Require 3 consistent zero counts to be sure
                print(f"📑 Found 0 tenders consistently (verified after {current_retry+1} retries) - need to restart browser")
                # We don't break here - proceed with browser restart
                pass
        else:
            # Reset the consistency counter if count changed
            consistent_count = 0
        
        # BROWSER RESTART APPROACH with multi-stage loading:
        print(f"🔄 Found only {row_count} tenders, which is less than page size {page_size}.")
        print(f"🔄 Restarting browser and establishing fresh session (retry {current_retry+1}/{max_retries})...")
        
        # Close the current browser
        driver.quit()
        
        # Set up a new browser instance
        driver = setup_selenium_driver(headless=headless)
        
        # STEP 1: First navigate to the base search page (without pagination)
        print(f"🔍 Establishing new session with base search URL...")
        driver.get("https://web.pcc.gov.tw/prkms/tender/common/bulletion/readBulletion")
        time.sleep(3)  # Wait for the page to load
        
        # Handle CAPTCHA if present on the base page
        handle_captcha(driver, False)
        
        # STEP 2: Perform the initial search to establish a fresh session
        print(f"🔍 Performing initial search with parameters: query='{query_sentence}', year={time_range}")
        driver.get(base_url)
        time.sleep(3)  # Wait for search results to load
        
        # Handle CAPTCHA if it appears after search
        handle_captcha(driver, False)
        
        # Check if this initial search was successful
        initial_rows = driver.find_elements(By.CSS_SELECTOR, "#bulletion > tbody > tr")
        if len(initial_rows) > 0:
            print(f"✅ Initial search successful, found {len(initial_rows)} tenders")
            
            # STEP 3: Only then navigate to the specific pagination page that was failing
            # But only if we're not already on the first page
            if current_url != base_url:
                print(f"📄 Now navigating to the specific page that failed: {current_url}")
                driver.get(current_url)
                time.sleep(3)  # Wait for the paginated results to load
                
                # Handle CAPTCHA if it appears after pagination
                handle_captcha(driver, False)
        else:
            print(f"⚠️ Initial search failed to load any results, trying direct navigation to failed page")
            driver.get(current_url)
            time.sleep(3)
            handle_captcha(driver, False)
        
        last_row_count = row_count
        current_retry += 1
    
    return rows, more_pages, driver, True  # Return the driver and a flag indicating "advance page"

def extract_tender_info(row):
    """Extract tender information from a table row"""
    cells = row.find_elements(By.TAG_NAME, "td")
    if len(cells) < 10:
        return None
    
    org_name = cells[2].text.strip()
    tender_info = cells[3].text.strip().split("\n")
    tender_no = tender_info[0].strip()
    project_name = tender_info[1].strip() if len(tender_info) > 1 else ""
    a_tag = cells[3].find_element(By.TAG_NAME, "a")
    detail_link = a_tag.get_attribute("href")
    
    # We still need these for fetching details, but won't store them permanently
    pk = detail_link.split("pk=")[-1]
    pk_pms_main = pk
    
    # Get the ROC date strings from the table cells
    roc_pub_date = cells[4].text.strip()
    roc_deadline = cells[6].text.strip()
    
    # For reference, also parse the dates into Gregorian format
    # but we'll use the ROC string format for storage
    gregorian_pub_date = parse_roc_date(roc_pub_date)
    gregorian_deadline = parse_roc_date(roc_deadline)
    
    return {
        "org_name": org_name,
        "tender_no": tender_no,
        "project_name": project_name,
        "detail_link": detail_link,
        "pk_pms_main": pk_pms_main,
        "pub_date": roc_pub_date,  # Use the ROC date string
        "deadline": roc_deadline,  # Use the ROC date string
        "gregorian_pub_date": gregorian_pub_date,  # For reference only
        "gregorian_deadline": gregorian_deadline   # For reference only
    }