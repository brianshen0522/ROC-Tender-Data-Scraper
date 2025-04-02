# Tender Data Scraper

A web scraper for collecting tender data from a Taiwanese government procurement website, with automated CAPTCHA solving capabilities and a Textual-based TUI (Text User Interface).

## Project Structure

```
.
├── .env                    # Environment variables and configuration
├── main.py                 # Main entry point for the scraper
├── tui.py                  # Textual-based TUI for running the scraper
├── database.py             # Database connection and operations
├── scraper.py              # Web scraping functions
├── captcha_solver.py       # CAPTCHA solving functionality
├── utils.py                # Utility functions
├── requirements.txt        # Python dependencies
└── debug_images/           # Directory for debug images (created automatically)
```

## Setup

1. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Set up the `.env` file with your database credentials and default settings:
   ```env
   # Database connection settings
   DB_NAME=tender
   DB_USER=yourusername
   DB_PASSWORD=yourpassword
   DB_HOST=localhost
   DB_PORT=5432

   # Scraper settings
   DEFAULT_QUERY=案
   DEFAULT_TIME_RANGE=113
   DEFAULT_PAGE_SIZE=100
   ```

## Usage

### Running the TUI

The project includes a Textual-based TUI for an interactive experience. To launch the TUI:
```
python tui.py
```

The TUI allows you to:
- Enter scraper parameters (query, time range, page size, etc.)
- Enable or disable headless mode and debug file retention
- View real-time logs of the scraping process
- Start and stop the scraper interactively

### Running the Scraper Directly

Run the scraper with default settings from the `.env` file:
```
python main.py
```

Or specify command-line arguments to override the defaults:
```
python main.py --query "關鍵字" --time "113" --size 50 --headless
```

### Command Line Arguments

- `--query`: Query sentence for tender search (default: from `.env` or '案')
- `--time`: Republic of China era year to search (default: from `.env` or '113')
- `--size`: Page size for results (default: from `.env` or 100, max: 100)
- `--headless`: Run browser in headless mode (default: off)
- `--keep-debug`: Keep debug images after CAPTCHA solving (default: off)

## Features

- **Web Scraping**: Automatically navigates paginated search results
- **CAPTCHA Solving**: Solves card-based CAPTCHAs using image processing techniques
- **Database Storage**: Stores data in PostgreSQL with proper schema
- **Error Handling**: Robust error handling and retry mechanisms
- **Configurable**: Easily configure settings via `.env` file or command-line arguments
- **Interactive TUI**: Provides a user-friendly interface for running the scraper

## Database Schema

The scraper stores data in two PostgreSQL tables:

1. **organizations**: Stores information about organizations
   - `site_id` (TEXT, PRIMARY KEY): Organization's site ID
   - `name` (TEXT, UNIQUE NOT NULL): Organization's name

2. **tenders**: Stores tender information
   - `organization_id` (TEXT, FOREIGN KEY): Reference to organizations table
   - `tender_no` (TEXT): Tender number
   - `url` (TEXT, PRIMARY KEY): URL of the tender
   - `project_name` (TEXT): Name of the tender project
   - `publication_date` (DATE): Publication date
   - `deadline` (DATE): Tender deadline
   - `scrap_status` (TEXT): Status of scraping ('finished', 'failed')
   - ... (many more fields from the tender details)

## Notes

- The CAPTCHA solving functionality uses OpenCV and image processing techniques.
- Browser automation is handled by Selenium with Chrome WebDriver.
- The TUI is built using the Textual framework for a modern terminal-based interface.