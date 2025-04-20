"""
Web scraping service for the real estate data.
"""
import random
import time
import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Set

import requests
from requests.adapters import HTTPAdapter, Retry

from config import (
    API_URL, BASE_URL, HEADERS, USER_AGENTS, CITY_ID, SUB_DISTRICT_IDS,
    PAGE_SIZE, CURRENCY_ID, REAL_ESTATE_TYPE, DEAL_TYPE, 
    MIN_DELAY, MAX_DELAY, RETRY_MIN_DELAY, RETRY_MAX_DELAY,
    CONNECT_TIMEOUT, READ_TIMEOUT, MAX_RETRIES, RETRY_MAX_RETRIES,
    CHECKPOINT_INTERVAL, CACHE_DIR, FAILED_PAGES_FILE
)
from utils.logging_utils import setup_logger
from utils.file_utils import (
    load_checkpoint, save_checkpoint, save_properties, load_properties,
    save_failed_pages, load_failed_pages
)
from utils.cache_utils import (
    ensure_cache_dir, get_cache_key, get_from_cache, save_to_cache, clear_expired_cache
)
from utils.pagination_utils import is_last_page, estimate_last_page  # Add this import
from utils.benchmark_utils import benchmark

logger = setup_logger(__name__)

class RealEstateScraper:
    """
    Scraper for real estate listings.
    """
    
    def __init__(self, use_cache: bool = True):
        """
        Initialize the scraper with a requests session.
        
        Args:
            use_cache: Whether to use caching
        """
        self.session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,  # Maximum number of retries
            backoff_factor=1,  # Backoff factor for retries
            status_forcelist=[429, 500, 502, 503, 504],  # Retry on these status codes
            allowed_methods=["GET", "POST"]  # Allow retries for these methods
        )
        
        # Optimize connection pooling
        adapter = HTTPAdapter(
            pool_connections=20,  # Number of connection pools
            pool_maxsize=50,      # Max connections per pool
            max_retries=retry_strategy
        )
        self.session.mount('https://', adapter)
        self.session.headers.update({"User-Agent": random.choice(USER_AGENTS)})
        
        self.token = None
        self.use_cache = use_cache
        self.cache_dir = Path(CACHE_DIR)
        if use_cache:
            ensure_cache_dir(self.cache_dir)
            clear_expired_cache(self.cache_dir)  # Clear expired cache on startup
    
    def get_auth_token(self) -> Optional[str]:
        """
        Get an authentication token from the website.
        
        Returns:
            Authentication token string or None if retrieval fails.
        """
        try:
            logger.info("Requesting session token from homepage...")
            # Rotate user agent
            self.session.headers.update({"User-Agent": random.choice(USER_AGENTS)})
            response = self.session.get(BASE_URL, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            response.raise_for_status()
            cookies = self.session.cookies.get_dict()
            token = cookies.get("ss-session-token")
            if token:
                logger.info("Successfully obtained new auth token")
                return token
            else:
                logger.error("No token found in cookies")
                return None
        except requests.RequestException as e:
            logger.error(f"Error getting auth token: {e}")
            return None
    
    def fetch_page_data(self, page: int, max_retries: int = MAX_RETRIES, check_last_page: bool = False) -> Tuple[Optional[List[Dict[str, Any]]], bool, bool]:
        """
        Fetch property data for a specific page with robust error handling and retries.
        
        Args:
            page: Page number to fetch
            max_retries: Maximum number of retry attempts
            check_last_page: Whether to check if this is the last page
            
        Returns:
            Tuple containing:
            - List of property data or None if retrieval fails
            - Boolean indicating if auth token needs refresh
            - Boolean indicating if this is the last page (only if check_last_page is True)
        """
        # Prepare request payload
        payload = {
            "realEstateType": REAL_ESTATE_TYPE,
            "realEstateDealType": DEAL_TYPE,
            "cityIdList": [CITY_ID],
            "subDistrictIds": SUB_DISTRICT_IDS,
            "currencyId": CURRENCY_ID,
            "page": page,
            "pageSize": PAGE_SIZE
        }
        
        # Check cache first if enabled
        if self.use_cache:
            cache_key = get_cache_key(page, payload)
            cached_data = get_from_cache(self.cache_dir, cache_key)
            if cached_data:
                is_last = is_last_page(cached_data) if check_last_page else False
                return cached_data.get("realStateItemModel", []), False, is_last
        
        # Ensure we have a valid token
        if not self.token:
            self.token = self.get_auth_token()
            if not self.token:
                logger.critical("Unable to get auth token.")
                return None, False, False
        
        # Initialize retry counter
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Rotate user agent for each attempt
                user_agent = random.choice(USER_AGENTS)
                
                # Prepare headers with current token
                headers = HEADERS.copy()
                headers["Authorization"] = f"Bearer {self.token}"
                headers["User-Agent"] = user_agent
                
                logger.debug(f"Fetching page {page} (attempt {retry_count + 1}/{max_retries})")
                
                # Add timeout to avoid hanging requests
                response = self.session.post(
                    API_URL, 
                    headers=headers, 
                    json=payload,
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
                )
                
                # Handle different response status codes
                if response.status_code == 200:
                    try:
                        response_data = response.json()
                        
                        # Validate response structure
                        if "realStateItemModel" not in response_data:
                            logger.warning(f"Invalid response structure for page {page}, missing 'realStateItemModel'")
                            retry_count += 1
                            time.sleep(random.uniform(RETRY_MIN_DELAY, RETRY_MAX_DELAY))
                            continue
                        
                        # Check if this is the last page
                        is_last = is_last_page(response_data) if check_last_page else False
                        
                        # Cache the response if caching is enabled
                        if self.use_cache:
                            cache_key = get_cache_key(page, payload)
                            save_to_cache(self.cache_dir, cache_key, response_data)
                        
                        return response_data.get("realStateItemModel", []), False, is_last
                        
                    except json.JSONDecodeError as e:
                        logger.warning(f"JSON parsing error on page {page}: {e}")
                        retry_count += 1
                        time.sleep(random.uniform(RETRY_MIN_DELAY, RETRY_MAX_DELAY))
                        continue
                        
                elif response.status_code in (401, 403):
                    logger.warning(f"Auth issue on page {page}, status code: {response.status_code}")
                    return None, True, False  # Need token refresh
                    
                elif response.status_code == 429:
                    # Rate limited - use exponential backoff
                    wait_time = (2 ** retry_count) * 5  # Exponential backoff: 5s, 10s, 20s...
                    logger.warning(f"Rate limited on page {page}, waiting {wait_time}s before retry")
                    time.sleep(wait_time)
                    retry_count += 1
                    
                elif 500 <= response.status_code < 600:
                    # Server error - retry with backoff
                    wait_time = random.uniform(2, 5) * (retry_count + 1)
                    logger.warning(f"Server error {response.status_code} on page {page}, waiting {wait_time}s before retry")
                    time.sleep(wait_time)
                    retry_count += 1
                    
                else:
                    logger.error(f"Failed to fetch page {page}, status: {response.status_code}")
                    retry_count += 1
                    time.sleep(random.uniform(RETRY_MIN_DELAY, RETRY_MAX_DELAY))
                    
            except requests.Timeout as e:
                logger.warning(f"Request timeout on page {page}: {e}")
                retry_count += 1
                time.sleep(random.uniform(RETRY_MIN_DELAY, RETRY_MAX_DELAY) * (retry_count + 1))
                
            except requests.ConnectionError as e:
                logger.warning(f"Connection error on page {page}: {e}")
                retry_count += 1
                time.sleep(random.uniform(RETRY_MIN_DELAY, RETRY_MAX_DELAY) * (retry_count + 1))
                
            except Exception as e:
                logger.error(f"Unexpected error on page {page}: {e}")
                retry_count += 1
                time.sleep(random.uniform(RETRY_MIN_DELAY, RETRY_MAX_DELAY) * (retry_count + 1))
        
        # If we've exhausted all retries
        logger.error(f"Failed to fetch page {page} after {max_retries} retries")
        return None, False, False
    
   
    @benchmark
    def scrape_properties(
        self, 
        output_path: Path, 
        checkpoint_path: Path, 
        data_path: Path,
        failed_pages_path: Path,
        start_page: int, 
        end_page: int = None,  # Make end_page optional
        batch_size: int = 100,
        retry_failed: bool = True,
        detect_last_page: bool = True  # New parameter to enable auto-detection
    ) -> List[Dict[str, Any]]:
        """
        Scrape property listings and save them to a file.
        
        Args:
            output_path: Directory to save output files.
            checkpoint_path: Path to the checkpoint file.
            data_path: Path to the data file.
            failed_pages_path: Path to save failed pages.
            start_page: First page to scrape.
            end_page: Last page to scrape. If None, will be auto-detected.
            batch_size: Number of properties to collect before saving.
            retry_failed: Whether to retry failed pages after initial pass.
            detect_last_page: Whether to auto-detect the last page.
            
        Returns:
            List of all collected property data.
        """
        # Load existing progress if any
        last_page = load_checkpoint(checkpoint_path)
        all_properties = load_properties(data_path)
        
        # Determine ending page
        if end_page is None:
            if detect_last_page:
                # Try to get the last page from sitemap first
                from utils.pagination_utils import estimate_last_page
                end_page = estimate_last_page()
                logger.info(f"Auto-detected last page: {end_page}")
            else:
                # Use default from config
                from config import DEFAULT_END_PAGE
                end_page = DEFAULT_END_PAGE
                logger.info(f"Using default last page: {end_page}")
        
        # If we already finished
        if last_page >= end_page:
            logger.info("Already finished scraping. No further scraping needed.")
            return all_properties
        
        # Initialize token
        self.token = self.get_auth_token()
        if not self.token:
            logger.critical("Unable to proceed without token.")
            return all_properties
        
        # Start scraping from the last checkpoint
        current_batch = []
        failed_pages = set()
        
        current_page = max(last_page + 1, start_page)
        reached_last_page = False
        
        while current_page <= end_page and not reached_last_page:
            logger.info(f"Scraping page {current_page} of {end_page}")
            
            # Request data with last page detection
            result, need_refresh, is_last_page = self.fetch_page_data(
                current_page, 
                check_last_page=detect_last_page
            )
            
            # Handle token refresh if needed
            if need_refresh:
                logger.info("Refreshing authentication token")
                self.token = self.get_auth_token()
                if not self.token:
                    logger.critical("Failed to refresh token after auth error.")
                    break
                # Retry the request
                time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
                result, _, is_last_page = self.fetch_page_data(current_page, check_last_page=detect_last_page)
            
            # Check if we're done
            if is_last_page:
                logger.info(f"Detected last page at {current_page}")
                reached_last_page = True
            
            # Handle results
            if result:
                if len(result) == 0 and detect_last_page:
                    logger.info(f"No properties found on page {current_page}, likely reached the end")
                    reached_last_page = True
                else:
                    current_batch.extend(result)
                    all_properties.extend(result)
                    logger.info(f"Successfully fetched page {current_page} with {len(result)} properties")
            else:
                logger.warning(f"Skipping page {current_page} due to failure.")
                failed_pages.add(current_page)
            
            # Save checkpoints periodically
            if len(current_batch) >= batch_size or current_page % CHECKPOINT_INTERVAL == 0:
                save_properties(data_path, all_properties)
                save_checkpoint(checkpoint_path, current_page)
                save_failed_pages(failed_pages_path, failed_pages)
                logger.info(f"Saved checkpoint at page {current_page}")
                current_batch.clear()
            
            # Increment page counter
            current_page += 1
            
            # Add random delay to avoid rate limiting
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        
        # Final save
        if current_batch:
            save_properties(data_path, all_properties)
            save_checkpoint(checkpoint_path, min(current_page - 1, end_page))
            save_failed_pages(failed_pages_path, failed_pages)
        
        # Update the actual end page if we found the last page
        if reached_last_page and current_page < end_page:
            end_page = current_page
            logger.info(f"Updated end page to {end_page} based on detection")
        
        # Retry failed pages
        if retry_failed and failed_pages:
            # [Rest of the retry code remains the same]
            
            return all_properties

def scrape_range(range_info: Tuple[int, int, Path, bool]) -> List[Dict[str, Any]]:
    """
    Function to be executed by each worker process to scrape a range of pages.
    
    Args:
        range_info: Tuple containing (start_page, end_page, output_dir, use_cache)
        
    Returns:
        List of property data from the scraped range
    """
    start_page, end_page, output_dir, use_cache = range_info
    logger.info(f"Worker process starting: pages {start_page} to {end_page}")
    
    # Create paths
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    worker_id = f"worker_{start_page}_{end_page}"
    checkpoint_path = output_path / f"checkpoint_{worker_id}.json"
    data_path = output_path / f"properties_{worker_id}.json"
    failed_pages_path = output_path / f"failed_pages_{worker_id}.json"
    
    # Initialize scraper
    scraper = RealEstateScraper(use_cache=use_cache)
    
    # Scrape the range
    properties = scraper.scrape_properties(
        output_path=output_path,
        checkpoint_path=checkpoint_path,
        data_path=data_path,
        failed_pages_path=failed_pages_path,
        start_page=start_page,
        end_page=end_page,
        batch_size=50,  # Smaller batch size for workers
        retry_failed=True
    )
    
    logger.info(f"Worker process completed: pages {start_page} to {end_page}, fetched {len(properties)} properties")
    return properties