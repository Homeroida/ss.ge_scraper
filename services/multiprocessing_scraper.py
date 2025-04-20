"""
Multiprocessing scraper for real estate data.
"""
import os
import time
import multiprocessing as mp
from pathlib import Path
from typing import Dict, List, Any, Optional
from functools import reduce
import multiprocessing.synchronize

from config import DEFAULT_WORKERS, DEFAULT_RATE_LIMIT
from utils.logging_utils import setup_logger
from utils.file_utils import ensure_directory, save_properties
from services.scraper import scrape_range
from utils.benchmark_utils import benchmark

logger = setup_logger(__name__)

class RateLimiter:
    """
    Rate limiter to control request frequency across multiple processes.
    """
    def __init__(self, rate_limit: float):
        """
        Initialize rate limiter.
        
        Args:
            rate_limit: Maximum requests per second
        """
        self.rate_limit = rate_limit
        self.interval = 1.0 / rate_limit if rate_limit > 0 else 0
        self.last_request = mp.Value('d', 0.0)
        self.lock = mp.Lock()
    
    def wait(self):
        """
        Wait if necessary to maintain the rate limit.
        """
        with self.lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request.value
            
            if time_since_last < self.interval:
                sleep_time = self.interval - time_since_last
                time.sleep(sleep_time)
            
            self.last_request.value = time.time()

def scrape_with_rate_limit(args: tuple) -> List[Dict[str, Any]]:
    """
    Wrapper function for scrape_range with rate limiting.
    
    Args:
        args: Tuple containing (range_info, rate_limiter)
        
    Returns:
        List of property data
    """
    range_info, rate_limiter = args
    
    # Override the fetch_page_data method to use rate limiting
    original_scrape_range = scrape_range
    
    def rate_limited_scrape_range(range_info):
        start_page, end_page, output_dir, use_cache = range_info
        logger.info(f"Rate-limited worker process starting: pages {start_page} to {end_page}")
        
        # Create paths
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        worker_id = f"worker_{start_page}_{end_page}"
        checkpoint_path = output_path / f"checkpoint_{worker_id}.json"
        data_path = output_path / f"properties_{worker_id}.json"
        failed_pages_path = output_path / f"failed_pages_{worker_id}.json"
        
        # Initialize scraper
        from services.scraper import RealEstateScraper
        scraper = RealEstateScraper(use_cache=use_cache)
        
        # Store the original fetch_page_data method
        original_fetch = scraper.fetch_page_data
        
        # Replace with rate-limited version
        def rate_limited_fetch(page, max_retries=3):
            rate_limiter.wait()  # Wait based on rate limit
            return original_fetch(page, max_retries)
        
        # Apply the rate-limited version
        scraper.fetch_page_data = rate_limited_fetch
        
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
    
    return rate_limited_scrape_range(range_info)

class MultiprocessingScraper:
    """
    Scraper that uses multiple processes to fetch data in parallel.
    """
    
    @staticmethod
    def merge_property_lists(list1: List[Dict[str, Any]], list2: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Merge two lists of properties, removing duplicates based on applicationId.
        
        Args:
            list1: First list of properties
            list2: Second list of properties
            
        Returns:
            Merged list with duplicates removed
        """
        # Create a dictionary with applicationId as keys to remove duplicates
        merged_dict = {}
        
        # Add properties from list1
        for prop in list1:
            app_id = prop.get("applicationId")
            if app_id:
                merged_dict[app_id] = prop
        
        # Add properties from list2
        for prop in list2:
            app_id = prop.get("applicationId")
            if app_id:
                merged_dict[app_id] = prop
        
        # Convert back to list
        return list(merged_dict.values())
    
    @benchmark
    def scrape_with_multiprocessing(
        self, 
        output_dir: str, 
        start_page: int, 
        end_page: int,
        num_workers: int = DEFAULT_WORKERS,
        use_cache: bool = True,
        rate_limit: float = DEFAULT_RATE_LIMIT
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Scrape property listings using multiple processes.
        
        Args:
            output_dir: Directory to save output files
            start_page: First page to scrape
            end_page: Last page to scrape
            num_workers: Number of worker processes to use
            use_cache: Whether to use caching
            rate_limit: Maximum requests per second across all workers
            
        Returns:
            List of all collected property data
        """
        logger.info(f"Starting multiprocessing scraper with {num_workers} workers and rate limit of {rate_limit} req/sec")
        
        # Ensure output directory exists
        output_path = ensure_directory(output_dir)
        
        # Calculate the range for each worker
        total_pages = end_page - start_page + 1
        pages_per_worker = max(1, total_pages // num_workers)
        
        # Create ranges for each worker
        ranges = []
        for i in range(num_workers):
            worker_start = start_page + (i * pages_per_worker)
            worker_end = min(end_page, worker_start + pages_per_worker - 1)
            
            # Skip if start > end (can happen in edge cases)
            if worker_start <= worker_end:
                ranges.append((worker_start, worker_end, output_dir, use_cache))
        
        # Create a shared rate limiter
        rate_limiter = RateLimiter(rate_limit)
        
        # Prepare arguments with rate limiter
        args = [(range_info, rate_limiter) for range_info in ranges]
        
        # Create a process pool and run the workers
        try:
            with mp.Pool(processes=num_workers) as pool:
                results = pool.map(scrape_with_rate_limit, args)
            
            # Merge results from all workers
            all_properties = reduce(self.merge_property_lists, results, [])
            
            # Save the combined results
            combined_path = output_path / "all_properties_combined.json"
            save_properties(combined_path, all_properties)
            logger.info(f"Combined {len(all_properties)} properties from all workers")
            
            return all_properties
            
        except Exception as e:
            logger.error(f"Error in multiprocessing scraper: {e}")
            return None