# Tender Data Scraper

A web scraper for collecting tender data from a Taiwanese government procurement website, with automated CAPTCHA solving capabilities and a Textual-based TUI (Text User Interface).

## Project Structure

```
.
├── .env                        # Environment variables and configuration
├── src/                        # Source code directory
│   ├── scraper/                # Scraping functionality
│   │   ├── main.py            # Main entry point for the scraper
│   │   ├── scraper.py         # Web scraping functions
│   │   └── captcha_solver.py  # CAPTCHA solving functionality
│   ├── db/                     # Database operations
│   │   ├── database.py        # Database connection and operations
│   │   ├── check_categories.py # Verification for tender categories
│   │   └── check_organizations.py # Verification for organizations
│   ├── ui/                     # User interface
│   │   └── tui.py             # Textual-based TUI for running the scraper
│   └── utils/                  # Utility functions
│       └── utils.py           # Utility functions
├── data/                       # Directory for JSON configuration files
│   ├── organizations.json     # Organizations data
│   └── tender_categories.json # Tender categories data
├── requirements.txt            # Python dependencies
└── debug_images/               # Directory for debug images (created automatically)
```

## Setup

1. Install the required dependencies:
   ```bash
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
```bash
python -m src.ui.tui
```

The TUI allows you to:
- Enter scraper parameters (query, time range, page size, etc.)
- Enable or disable headless mode and debug file retention
- Select which phase(s) of the scraper to run (discovery, detail, or both)
- View real-time logs of the scraping process
- Start and stop the scraper interactively

### Running the Scraper Directly

Run the scraper with default settings from the `.env` file:
```bash
python -m src.scraper.main
```

Or specify command-line arguments to override the defaults:
```bash
python -m src.scraper.main --query "關鍵字" --time "113" --size 50 --headless --phase both
```

### Command Line Arguments

- `--query`: Query sentence for tender search (default: from `.env` or '案')
- `--time`: Republic of China era year to search (default: from `.env` or '113')
- `--size`: Page size for results (default: from `.env` or 100, max: 100)
- `--headless`: Run browser in headless mode (default: off)
- `--keep-debug`: Keep debug images after CAPTCHA solving (default: off)
- `--phase`: Run only the discovery phase, only the detail phase, or both (default: both)

### Verification Tools

The project includes tools to verify database content:
```bash
# Check organization data
python -m src.db.check_organizations

# Check tender categories
python -m src.db.check_categories
```

## Features

- **Web Scraping**: Automatically navigates paginated search results
- **CAPTCHA Solving**: Solves card-based CAPTCHAs using image processing techniques
- **Database Storage**: Stores data in PostgreSQL with proper schema
- **Error Handling**: Robust error handling and retry mechanisms
- **Configurable**: Easily configure settings via `.env` file or command-line arguments
- **Interactive TUI**: Provides a user-friendly interface for running the scraper

## Database Schema

The scraper stores data in three PostgreSQL tables:

1. **organizations**: Stores information about organizations
   - `site_id` (TEXT, PRIMARY KEY): Organization's site ID
   - `name` (TEXT, UNIQUE NOT NULL): Organization's name

2. **tender_categories**: Stores tender category information
   - `id` (TEXT, PRIMARY KEY): Category ID
   - `name` (TEXT NOT NULL): Category name
   - `category` (TEXT NOT NULL): Category type

3. **tenders**: Stores tender information
   - `organization_id` (TEXT, FOREIGN KEY): Reference to organizations table
   - `tender_no` (TEXT): Tender number
   - `url` (TEXT, UNIQUE): URL of the tender
   - `project_name` (TEXT): Name of the tender project
   - `publication_date` (TEXT): Publication date in ROC format (e.g., '113/04/01')
   - `deadline` (TEXT): Tender deadline in ROC format
   - `scrap_status` (TEXT): Status of scraping ('found', 'finished', 'failed')
   - `pk_pms_main` (TEXT): Unique identifier for fetching tender details
   - `item_category` (TEXT, FOREIGN KEY): Reference to tender_categories table
   - ... (many more fields from the tender details)

## Notes

- The CAPTCHA solving functionality uses OpenCV and image processing techniques.
- Browser automation is handled by Selenium with Chrome WebDriver.
- The TUI is built using the Textual framework for a modern terminal-based interface.
- The scraper uses a two-phase approach:
  1. **Discovery Phase**: Finds tenders and saves basic information.
  2. **Detail Phase**: Fetches detailed information for tenders with a "found" status.
- The project requires Chrome/Chromium and ChromeDriver installed for Selenium to work.