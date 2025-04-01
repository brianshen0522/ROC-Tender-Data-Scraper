import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from captcha_solver import handle_captcha
from utils import parse_roc_date

def setup_selenium_driver(headless=False):
    """Set up and return a configured Selenium WebDriver"""
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.add_argument("--disable-cache")
    
    if headless:
        chrome_options.add_argument("--headless")
    
    print("🔐 Setting up secure browser session...")
    driver = webdriver.Chrome(options=chrome_options)
    
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

def fetch_org_id_from_site(driver, org_name):
    """Open new tab and fetch organization site ID"""
    main_window = driver.current_window_handle
    driver.execute_script("window.open('');")
    driver.switch_to.window(driver.window_handles[-1])
    driver.get("https://web.pcc.gov.tw/prkms/tender/common/orgName/search")

    # Handle CAPTCHA check
    handle_captcha(driver, False)

    try:
        search_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '/html/body/div/div[2]/div/div[2]/div/form/table/tbody/tr/td[1]/input'))
        )
        search_input.send_keys(org_name)
        form = driver.find_element(By.XPATH, '/html/body/div/div[2]/div/div[2]/div/form')
        driver.execute_script("arguments[0].submit();", form)
        org_id_cell = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '/html/body/div/div[2]/div/div[2]/div/table/tbody/tr[2]/td[1]'))
        )
        org_id = org_id_cell.text.strip()
    except Exception as e:
        print(f"⚠️ Failed to fetch org ID for '{org_name}':", e)
        org_id = None

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
        "標的分類": "item_category",
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
                detail_data[fields_mapping[text]] = value
    except Exception as e:
        print("Error extracting tender details:", e)

    driver.close()
    driver.switch_to.window(main_window)
    return detail_data

def check_page_data_loaded(driver, page_size, max_retries=5):
    """Check if the data is loaded correctly with retry mechanism"""
    current_retry = 0
    last_row_count = 0
    consistent_count = 0  # Track how many times we get the same count
    more_pages = True

    while current_retry < max_retries:
        # Get the rows
        rows = driver.find_elements(By.CSS_SELECTOR, "#bulletion > tbody > tr")
        row_count = len(rows)
        
        # If we have a full page, we're good
        if row_count >= page_size:
            print(f"📊 Found {row_count} tenders - will continue to next page after processing")
            break
        
        # If this is our last retry, accept whatever we have
        if current_retry == max_retries - 1:
            print(f"📑 Found {row_count} tenders after {max_retries} retries - this may be the last page")
            more_pages = False if row_count == 0 else True  # Only stop if we found zero rows
            break
            
        # Otherwise, check if we have consistent counts
        elif row_count == last_row_count and row_count > 0:
            # Only count consistency if we have actual rows (don't trust zero results)
            consistent_count += 1
            if consistent_count >= 3:  # Require 3 consistent counts to be sure
                print(f"📑 Found {row_count} tenders (verified after {current_retry+1} retries) - this may be the last page")
                more_pages = True  # Still process this page
                break
        else:
            # Reset the consistency counter if count changed
            consistent_count = 0
        
        # Retry
        print(f"🔄 Found only {row_count} tenders, which is less than page size {page_size}. Reloading to verify (retry {current_retry+1}/{max_retries})...")
        driver.refresh()
        time.sleep(5)  # Wait longer for the page to fully load
        
        # Handle CAPTCHA if it appears after refresh
        handle_captcha(driver, False)
        
        last_row_count = row_count
        current_retry += 1
    
    return rows, more_pages

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
    
    pub_date = parse_roc_date(cells[4].text.strip())
    deadline = parse_roc_date(cells[6].text.strip())
    
    return {
        "org_name": org_name,
        "tender_no": tender_no,
        "project_name": project_name,
        "detail_link": detail_link,
        "pk_pms_main": pk_pms_main,
        "pub_date": pub_date,
        "deadline": deadline
    }