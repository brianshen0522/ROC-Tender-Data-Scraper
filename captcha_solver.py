import os
import sys
import time
import cv2
import numpy as np
import traceback
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import UnexpectedAlertPresentException, TimeoutException, NoSuchElementException
from PIL import Image
import io
from ultralytics import YOLO
from utils import suppress_output, setup_debug_directory, cleanup_debug_images

# Initialize debug directory
setup_debug_directory()

# Load the YOLO model (suppress detailed output)
@suppress_output
def load_yolo_model():
    try:
        model = YOLO(r".\\poker\\model\\best.pt")  # Load the trained model
        print("YOLO model loaded successfully")
        return model
    except Exception as e:
        print(f"Error loading YOLO model: {str(e)}")
        print(traceback.format_exc())
        return None

# Global YOLO model instance
model = load_yolo_model()

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

@suppress_output
def identify_card_with_yolo(image):
    """Identify the card using the YOLO classification model"""
    if model is None:
        return "unknown"
        
    # Create a copy of the image for debugging
    debug_img = image.copy()
    
    # Save the original image for debugging
    timestamp = int(time.time() * 1000)
    cv2.imwrite(f'debug_images/orig_{timestamp}.png', debug_img)
    
    # Save the image temporarily
    temp_image_path = f'debug_images/temp_{timestamp}.png'
    cv2.imwrite(temp_image_path, image)
    
    try:
        # Run prediction on the image
        results = model(temp_image_path)
        print(results)
        # Check if results is not empty
        if results and len(results) > 0:
            # Get the first result
            result = results[0]
            
            # For classification models, we need to access the probs attribute
            if hasattr(result, 'probs') and result.probs is not None:
                # Get class with highest probability
                class_idx = int(result.probs.top1)
                confidence = float(result.probs.top1conf)
                
                # Get the class name
                if hasattr(result, 'names') and result.names is not None:
                    class_name = result.names[class_idx]
                    print(f"Predicted card: {class_name}, Confidence: {confidence:.3f}")
                    return class_name
                else:
                    print("Names dictionary not found in result")
                    return "unknown"
            # Alternative access method if the above doesn't work
            elif hasattr(result, 'names') and result.names is not None:
                # Try to directly get the predicted class
                if hasattr(result, 'verbose') and isinstance(result.verbose, bool):
                    # Parse without printing full results
                    for line in str(results).split('\n'):
                        if line.strip() and ' ' in line:
                            parts = line.split(' ')
                            if len(parts) >= 2 and parts[1].strip():
                                class_name = parts[0].strip()
                                confidence = float(parts[1].strip())
                                print(f"Card: {class_name}, Confidence: {confidence:.3f}")
                                return class_name
                
                print("Could not extract classification result")
                return "unknown"
            else:
                print("No probs attribute or names dictionary found")
                return "unknown"
        else:
            print("Empty results from model")
            return "unknown"
    except Exception as e:
        print(f"Error in YOLO prediction: {str(e)}")
        print(traceback.format_exc())
        return "unknown"

def solve_card_captcha(driver, attempt=1, max_attempts=10):
    """Solve the card CAPTCHA by matching patterns between areas A and B"""
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
        
        # Analyze cards using YOLO model
        left_card = identify_card_with_yolo(left_half)
        right_card = identify_card_with_yolo(right_half)
            
        # Fall back to color-based identification if YOLO failed
        if left_card == "unknown" or right_card == "unknown":
            left_color = identify_color(left_half)
            right_color = identify_color(right_half)
            
            # If we got at least one card from YOLO, use it
            if left_card == "unknown":
                left_card = left_color
            if right_card == "unknown":
                right_card = right_color
        
        # Cards to match in area B
        cards_to_match = [left_card, right_card]
        
        # Get all card images from area B
        card_xpaths = [
            "/html/body/div/div[2]/div/div/div[3]/div/div[4]/form/table[1]/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr/td[1]/label/img",
            "/html/body/div/div[2]/div/div/div[3]/div/div[4]/form/table[1]/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr/td[2]/label/img",
            "/html/body/div/div[2]/div/div/div[3]/div/div[4]/form/table[1]/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr/td[3]/label/img",
            "/html/body/div/div[2]/div/div/div[3]/div/div[4]/form/table[1]/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr/td[4]/label/img",
            "/html/body/div/div[2]/div/div/div[3]/div/div[4]/form/table[1]/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr/td[5]/label/img",
            "/html/body/div/div[2]/div/div/div[3]/div/div[4]/form/table[1]/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr/td[6]/label/img"
        ]
        
        # Create a list to store which cards we'll click
        cards_to_click = []
        
        # Analyze each card in area B
        for i, xpath in enumerate(card_xpaths):
            card_element = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, xpath))
            )
            
            card_image = capture_image_from_element(driver, xpath)
            cv2.imwrite(f'debug_images/card_{i+1}.png', card_image)
            
            card_name = identify_card_with_yolo(card_image)
            
            if card_name in cards_to_match:
                cards_to_click.append((i+1, card_element))
                cards_to_match.remove(card_name)
                
                if not cards_to_match:
                    break
        
        # Click the matching cards
        for idx, card_element in cards_to_click:
            card_element.click()
        confirm_button_xpath = "//input[@value='確認送出']"
        confirm_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, confirm_button_xpath))
        )
        
        try:
            confirm_button.click()
            
            # Handle alerts if they appear
            try:
                alert = WebDriverWait(driver, 3).until(EC.alert_is_present())
                alert.accept()  # Press OK on the alert
                
                # If alert and have attempts left, try again
                if attempt < max_attempts:
                    print(f"🔄 CAPTCHA retry needed")
                    # Wait for the page to refresh
                    time.sleep(1)
                    solve_card_captcha(driver, attempt + 1, max_attempts)
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
                alert.accept()
                
                # If alert and have attempts left, try again
                if attempt < max_attempts:
                    print(f"🔄 CAPTCHA retry needed")
                    # Wait for the page to refresh
                    time.sleep(1)
                    solve_card_captcha(driver, attempt + 1, max_attempts)
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