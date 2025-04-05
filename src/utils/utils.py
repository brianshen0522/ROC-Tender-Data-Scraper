import os
import sys
import time
import glob
from datetime import datetime, date
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def setup_debug_directory():
    """Create a directory to save debug images if it doesn't exist"""
    os.makedirs('debug_images', exist_ok=True)

def suppress_output(func):
    """Decorator to suppress stdout during function execution"""
    def wrapper(*args, **kwargs):
        # Temporarily redirect stdout to suppress messages
        with open(os.devnull, 'w') as f:
            original_stdout = sys.stdout
            sys.stdout = f
            result = func(*args, **kwargs)
            sys.stdout = original_stdout
        return result
    return wrapper

def parse_roc_date(date_str):
    """Convert ROC date (e.g., 113/10/30) to Gregorian date"""
    try:
        parts = date_str.strip().split("/")
        roc_year = int(parts[0])
        year = roc_year + 1911
        return datetime.strptime(f"{year}/{parts[1]}/{parts[2]}", "%Y/%m/%d").date()
    except Exception as e:
        print("Error parsing date:", e)
        return None

def convert_to_roc_date(gregorian_date):
    """Convert Gregorian date to ROC date string (e.g., '113/10/30')"""
    if not gregorian_date:
        return None
        
    if isinstance(gregorian_date, str):
        try:
            gregorian_date = datetime.strptime(gregorian_date, "%Y-%m-%d").date()
        except:
            return None
    
    roc_year = gregorian_date.year - 1911
    return f"{roc_year}/{gregorian_date.month:02d}/{gregorian_date.day:02d}"

def cleanup_debug_images(keep_debug_files=False):
    """Remove debug image files from the debug_images directory"""
    if not keep_debug_files and os.path.exists('debug_images'):
        files = glob.glob('debug_images/*.png')
        for file in files:
            try:
                os.remove(file)
                print(f"Removed debug file: {file}")
            except Exception as e:
                print(f"Error removing file {file}: {e}")
        print(f"Cleaned up {len(files)} debug image files")