"""
Configuration for the real estate scraper.
"""
import os
from pathlib import Path
from typing import List, Dict, Any

# Base URLs and API endpoints
BASE_URL = "https://home.ss.ge/"
API_URL = "https://api-gateway.ss.ge/v1/RealEstate/LegendSearch"

# Headers for API requests
HEADERS = {
    "Content-Type": "application/json",
    "Accept-Language": "ka",
    "Accept": "application/json, text/plain, */*",
    "Origin": BASE_URL,
    "Referer": BASE_URL,
    "authority": "api-gateway.ss.ge",
}

# User agent rotation for requests
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    "Mozilla/5.0 (iPad; CPU OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0"
]

# Search parameters
CITY_ID = 95
SUB_DISTRICT_IDS = [
    2, 3, 4, 5, 26, 27, 44, 45, 46, 47, 48, 49, 50, 6, 7, 8, 9, 10, 11,
    13, 14, 15, 16, 17, 18, 19, 24, 32, 33, 34, 35, 36, 37, 38, 39, 40,
    41, 42, 43, 53, 1, 28, 29, 30, 31, 20, 21, 22, 23, 51, 52,
]
PAGE_SIZE = 16
CURRENCY_ID = 1
REAL_ESTATE_TYPE = 5
DEAL_TYPE = 4

# File paths
DEFAULT_OUTPUT_DIR = "output"
CHECKPOINT_FILE = "checkpoint.json"
DATA_FILE = "all_properties.json"
PROCESSED_FILE = "properties_cleaned.csv"
CACHE_DIR = "cache"  # Directory to store cached data
FAILED_PAGES_FILE = "failed_pages.json"  # Track failed pages

# Scraping parameters
DEFAULT_START_PAGE = 1
DEFAULT_END_PAGE = 15972
DEFAULT_BATCH_SIZE = 100

# Delay parameters (increased to avoid rate limiting)
MIN_DELAY = 2.0  # Minimum delay between requests in seconds
MAX_DELAY = 4.0  # Maximum delay between requests in seconds
RETRY_MIN_DELAY = 4.0  # Minimum delay for retries
RETRY_MAX_DELAY = 8.0  # Maximum delay for retries
CHECKPOINT_INTERVAL = 22  # Save checkpoint every N pages

# Request parameters
CONNECT_TIMEOUT = 15  # Connection timeout in seconds
READ_TIMEOUT = 45  # Read timeout in seconds
MAX_RETRIES = 3  # Maximum number of retry attempts per page
RETRY_MAX_RETRIES = 5  # Maximum number of retry attempts for failed pages

# Multiprocessing parameters
DEFAULT_WORKERS = 2  # Reduced default number of workers to avoid overwhelming the server
DEFAULT_RATE_LIMIT = 0.25  # Default rate limit in pages per second across all workers

# Caching parameters
CACHE_EXPIRY = 86400  # Cache expiry in seconds (24 hours)