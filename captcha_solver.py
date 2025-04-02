# test_captcha_solver.py - Standalone test script for PCC Taiwan CAPTCHA solver

import os
import sys
import time
import cv2
import numpy as np
import traceback
from datetime import datetime
from contextlib import contextmanager
import glob
import io
from PIL import Image
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import UnexpectedAlertPresentException, TimeoutException, NoSuchElementException

# Create necessary directories
def setup_directories():
    """Create necessary directories for the script to run"""
    os.makedirs("debug_images", exist_ok=True)
    print("✓ Debug directory created")

# Utility functions
@contextmanager
def suppress_output():
    """Context manager to suppress standard output"""
    # Save original stdout and stderr
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    
    try:
        # Redirect stdout and stderr to devnull
        with open(os.devnull, 'w') as devnull:
            sys.stdout = devnull
            sys.stderr = devnull
            yield
    finally:
        # Restore original stdout and stderr
        sys.stdout = old_stdout
        sys.stderr = old_stderr

def cleanup_debug_images(keep_files=False):
    """Delete all debug images unless keep_files is True"""
    if not keep_files:
        for f in glob.glob("debug_images/*.png"):
            try:
                os.remove(f)
            except:
                pass

# Functions from the original code
def capture_image_from_element(driver, xpath):
    """Capture an image from an element identified by XPath"""
    try:
        element = driver.find_element(By.XPATH, xpath)
        # Get the element's location and size
        location = element.location
        size = element.size
        
        # Take screenshot
        screenshot = driver.get_screenshot_as_png()
        image = Image.open(io.BytesIO(screenshot))
        
        # Calculate the boundaries of the element in the screenshot
        left = location['x']
        top = location['y']
        right = location['x'] + size['width']
        bottom = location['y'] + size['height']
        
        # Crop the image to the element's boundaries
        image = image.crop((left, top, right, bottom))
        
        # Convert PIL Image to numpy array (RGB)
        image_np = np.array(image)
        
        # Convert RGB to BGR for OpenCV
        image_bgr = cv2.cvtColor(image_np, cv2.COLOR_RGB2BGR)
        
        return image_bgr
    except Exception as e:
        print(f"Error capturing image from element: {str(e)}")
        print(traceback.format_exc())
        return None

def identify_color(image):
    """Identify if the card is red or black based on simple color analysis"""
    try:
        # Convert to HSV for better color detection
        hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
        
        # Define ranges for red color (hearts/diamonds)
        # Red wraps around the HSV color wheel, so we need two ranges
        lower_red1 = np.array([0, 70, 50])
        upper_red1 = np.array([10, 255, 255])
        lower_red2 = np.array([160, 70, 50])
        upper_red2 = np.array([180, 255, 255])
        
        # Create masks for red color
        mask1 = cv2.inRange(hsv, lower_red1, upper_red1)
        mask2 = cv2.inRange(hsv, lower_red2, upper_red2)
        red_mask = mask1 + mask2
        
        # Get gray image for overall content
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        ret, content_mask = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY_INV)
        
        # Count pixels
        red_pixels = np.sum(red_mask > 0)
        total_content_pixels = np.sum(content_mask > 0)
        
        # Calculate red ratio
        red_ratio = red_pixels / total_content_pixels if total_content_pixels > 0 else 0
        print(f"Red ratio: {red_ratio:.3f}")
        
        # Determine if it's a red card or black card
        if red_ratio > 0.2:
            return "red"
        else:
            return "black"
    except Exception as e:
        print(f"Error in color identification: {str(e)}")
        print(traceback.format_exc())
        return "unknown"

def calculate_overlap_ratio(image1, image2):
    """Calculate the similarity/overlap ratio between two card images"""
    try:
        # Convert both images to grayscale for better comparison
        gray1 = cv2.cvtColor(image1, cv2.COLOR_BGR2GRAY)
        gray2 = cv2.cvtColor(image2, cv2.COLOR_BGR2GRAY)
        
        # Apply thresholding to convert to binary images (black and white)
        ret, thresh1 = cv2.threshold(gray1, 127, 255, cv2.THRESH_BINARY)
        ret, thresh2 = cv2.threshold(gray2, 127, 255, cv2.THRESH_BINARY)
        
        # Make sure both images are the same size for comparison
        h1, w1 = thresh1.shape
        h2, w2 = thresh2.shape
        
        # Resize the second image to match the first if they're different sizes
        if h1 != h2 or w1 != w2:
            thresh2 = cv2.resize(thresh2, (w1, h1))
        
        # Calculate the difference between the two binary images
        diff = cv2.bitwise_xor(thresh1, thresh2)
        
        # Count the number of matching pixels (where the images are the same)
        non_zero_pixels = np.count_nonzero(diff)
        total_pixels = thresh1.shape[0] * thresh1.shape[1]
        
        # Calculate the similarity ratio (higher means more similar)
        similarity_ratio = 1 - (non_zero_pixels / total_pixels)
        
        return similarity_ratio
    except Exception as e:
        print(f"Error calculating overlap ratio: {str(e)}")
        print(traceback.format_exc())
        return 0.0

def solve_card_captcha(driver, attempt=1, max_attempts=10):
    """Solve the card CAPTCHA by using overlap ratio to match patterns between areas A and B"""
    # Only log attempt when retrying
    if attempt > 1:
        print(f"🔄 CAPTCHA retry attempt {attempt}/{max_attempts}")
    
    # Silently identify the CAPTCHA elements and images
    try:
        # Wait for the CAPTCHA to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(text(), '驗證碼檢核')]"))
        )
        
        # Wait for area A image to be visible
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, "/html/body/div/div[2]/div/div/div[3]/div/div[4]/form/table[1]/tbody/tr[1]/td/table/tbody/tr/td[2]/img"))
        )
        
        # Capture the question image
        question_xpath = "/html/body/div/div[2]/div/div/div[3]/div/div[4]/form/table[1]/tbody/tr[1]/td/table/tbody/tr/td[2]/img"
        question_image = capture_image_from_element(driver, question_xpath)
        
        # Split the question image
        height, width = question_image.shape[:2]
        left_half = question_image[0:height, 0:width//2]
        right_half = question_image[0:height, width//2:width]
        
        # Save the halves for debugging
        cv2.imwrite('debug_images/left_half_question.png', left_half)
        cv2.imwrite('debug_images/right_half_question.png', right_half)
        
        # Get all card images from area B
        card_xpaths = [
            "/html/body/div/div[2]/div/div/div[3]/div/div[4]/form/table[1]/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr/td[1]/label/img",
            "/html/body/div/div[2]/div/div/div[3]/div/div[4]/form/table[1]/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr/td[2]/label/img",
            "/html/body/div/div[2]/div/div/div[3]/div/div[4]/form/table[1]/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr/td[3]/label/img",
            "/html/body/div/div[2]/div/div/div[3]/div/div[4]/form/table[1]/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr/td[4]/label/img",
            "/html/body/div/div[2]/div/div/div[3]/div/div[4]/form/table[1]/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr/td[5]/label/img",
            "/html/body/div/div[2]/div/div/div[3]/div/div[4]/form/table[1]/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr/td[6]/label/img"
        ]
        
        # Create lists to store overlap ratios for each card with left and right templates
        card_elements = []
        left_overlap_ratios = []
        right_overlap_ratios = []
        
        # Analyze each card in area B
        for i, xpath in enumerate(card_xpaths):
            card_element = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, xpath))
            )
            
            card_elements.append(card_element)
            card_image = capture_image_from_element(driver, xpath)
            cv2.imwrite(f'debug_images/card_{i+1}.png', card_image)
            
            # Calculate overlap ratios
            left_ratio = calculate_overlap_ratio(left_half, card_image)
            right_ratio = calculate_overlap_ratio(right_half, card_image)
            
            left_overlap_ratios.append(left_ratio)
            right_overlap_ratios.append(right_ratio)
            
            print(f"Card {i+1}: Left ratio: {left_ratio:.3f}, Right ratio: {right_ratio:.3f}")
        
        # Find the best matches based on overlap ratios
        best_left_match_idx = left_overlap_ratios.index(max(left_overlap_ratios))
        best_right_match_idx = right_overlap_ratios.index(max(right_overlap_ratios))
        
        # Avoid duplicate selections if the same card matches both left and right
        if best_left_match_idx == best_right_match_idx:
            # If they're the same, find the second-best match for right
            temp_right_ratios = right_overlap_ratios.copy()
            temp_right_ratios[best_left_match_idx] = -1  # Exclude the left match
            best_right_match_idx = temp_right_ratios.index(max(temp_right_ratios))
        
        print(f"Best matches: Left -> Card {best_left_match_idx+1}, Right -> Card {best_right_match_idx+1}")
        
        # Click the matching cards
        card_elements[best_left_match_idx].click()
        card_elements[best_right_match_idx].click()
            
        confirm_button_xpath = "//input[@value='確認送出']"
        confirm_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, confirm_button_xpath))
        )
        
        try:
            confirm_button.click()
            
            # Handle alerts if they appear
            try:
                alert = WebDriverWait(driver, 3).until(EC.alert_is_present())
                alert_text = alert.text
                print(f"Alert message: {alert_text}")
                alert.accept()  # Press OK on the alert
                
                # If alert and have attempts left, try again
                if attempt < max_attempts:
                    print(f"🔄 CAPTCHA retry needed")
                    # Wait for the page to refresh
                    time.sleep(1)
                    return solve_card_captcha(driver, attempt + 1, max_attempts)
                else:
                    print(f"❌ Verification failed! Maximum attempts reached.")
                    return False
            except TimeoutException:
                # No alert, success!
                print("✅ Verification submitted!")
                return True
        except UnexpectedAlertPresentException as e:
            # Handle alert that appears during click
            try:
                alert = driver.switch_to.alert
                alert_text = alert.text
                print(f"Alert message: {alert_text}")
                alert.accept()
                
                # If alert and have attempts left, try again
                if attempt < max_attempts:
                    print(f"🔄 CAPTCHA retry needed")
                    # Wait for the page to refresh
                    time.sleep(1)
                    return solve_card_captcha(driver, attempt + 1, max_attempts)
                else:
                    print(f"❌ Verification failed! Maximum attempts reached.")
                    return False
            except Exception:
                print(f"❌ Verification failed! Unable to handle alert.")
                return False
    
    except Exception as e:
        print(f"❌ Verification failed! Error: {str(e)}")
        # Log the error to a debug file for investigation
        with open('debug_captcha_errors.log', 'a') as f:
            f.write(f"{datetime.now()} - CAPTCHA Error: {str(e)}\n")
            f.write(traceback.format_exc())
            f.write("\n---\n")
        
        # If we have attempts left, try again
        if attempt < max_attempts:
            print(f"🔄 CAPTCHA retry needed")
            time.sleep(1)
            return solve_card_captcha(driver, attempt + 1, max_attempts)
        return False
    
    return True

def handle_captcha(driver, keep_debug_files=False):
    """Check for CAPTCHA and solve it if needed"""
    if "驗證碼檢核" in driver.page_source:
        print("🛑 CAPTCHA detected — attempting to solve automatically...")
        
        # Get the current URL to determine where to redirect after solving
        current_url = driver.current_url
        
        # Special handling for redirect to validation page
        if "validate" not in current_url:
            # Save the current URL silently
            pass
        
        # Solve the CAPTCHA
        success = solve_card_captcha(driver)
        
        # Clean up debug image files
        cleanup_debug_images(keep_debug_files)
        
        # Check if we need to return to the previous page
        if success and "validate" not in current_url and current_url != driver.current_url:
            print(f"🔙 Returning to previous page")
            driver.get(current_url)
            time.sleep(0.5)
        
        return success
    
    return True  # No CAPTCHA detected

def check_dependencies():
    """Check if required dependencies are installed"""
    missing_deps = []
    
    # Check required packages
    try:
        import selenium
        print("✓ Selenium is installed")
    except ImportError:
        missing_deps.append("selenium")
        print("✗ Selenium is not installed")
    
    try:
        import cv2
        print("✓ OpenCV is installed")
    except ImportError:
        missing_deps.append("opencv-python")
        print("✗ OpenCV is not installed")
    
    try:
        from PIL import Image
        print("✓ Pillow is installed")
    except ImportError:
        missing_deps.append("pillow")
        print("✗ Pillow is not installed")
    
    try:
        import numpy
        print("✓ NumPy is installed")
    except ImportError:
        missing_deps.append("numpy")
        print("✗ NumPy is not installed")
    
    if missing_deps:
        print("\n❌ Missing dependencies. Please install them with:")
        print(f"pip install {' '.join(missing_deps)}")
        return False
    
    return True

def test_captcha_solver():
    """Run the test for the CAPTCHA solver"""
    print("\nSetting up Chrome WebDriver...")
    
    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")  # Maximize window
    chrome_options.add_argument("--log-level=3")  # Suppress logs
    
    # Initialize Chrome WebDriver directly
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.maximize_window()
    except Exception as e:
        print(f"❌ Failed to initialize Chrome WebDriver: {str(e)}")
        print("Make sure you have Chrome installed on your system and ChromeDriver is in your PATH.")
        return False
    
    try:
        print(f"Navigating to the CAPTCHA test page...")
        driver.get("https://web.pcc.gov.tw/tps/validate/init")
        
        # Wait for page to load
        time.sleep(2)
        
        # Check if the page loaded correctly
        if "驗證碼檢核" not in driver.page_source:
            print("❌ Test failed: Could not find CAPTCHA on the page.")
            print("Please check your internet connection or if the website has changed.")
            return False
        
        print("Starting CAPTCHA solving test...")
        # Keep debug files for this test run
        success = handle_captcha(driver, keep_debug_files=True)
        
        if success:
            print("\n✅ CAPTCHA test PASSED - Successfully solved the CAPTCHA!")
        else:
            print("\n❌ CAPTCHA test FAILED - Could not solve the CAPTCHA.")
        
        # Wait a bit to see the result
        time.sleep(3)
        
        return success
    
    except Exception as e:
        print(f"❌ Test failed with error: {str(e)}")
        print(traceback.format_exc())
        return False
    
    finally:
        print("Closing browser...")
        driver.quit()

if __name__ == "__main__":
    print("=" * 70)
    print("PCC Taiwan CAPTCHA Solver Test Script")
    print("=" * 70)
    print("This script tests the CAPTCHA solver on https://web.pcc.gov.tw/tps/validate/init")
    print("\nChecking dependencies...")
    
    # Check if dependencies are installed
    if not check_dependencies():
        sys.exit(1)
    
    # Set up directories
    setup_directories()
    
    print("\nStarting test...")
    result = test_captcha_solver()
    
    print("\n" + "=" * 70)
    if result:
        print("Test completed successfully!")
    else:
        print("Test failed. Check the messages above for details.")
    print("=" * 70)
    
    # Exit with appropriate code
    sys.exit(0 if result else 1)