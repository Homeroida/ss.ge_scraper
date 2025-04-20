"""
Main entry point for the real estate scraper application.
"""
import argparse
import time
from pathlib import Path

from config import (
    DEFAULT_OUTPUT_DIR, CHECKPOINT_FILE, DATA_FILE, PROCESSED_FILE, FAILED_PAGES_FILE,
    DEFAULT_START_PAGE, DEFAULT_END_PAGE, DEFAULT_BATCH_SIZE, 
    DEFAULT_WORKERS, DEFAULT_RATE_LIMIT, CACHE_DIR
)
from utils.logging_utils import setup_logger
from utils.file_utils import ensure_directory, load_failed_pages
from utils.benchmark_utils import compare_performance
from services.scraper import RealEstateScraper
from services.multiprocessing_scraper import MultiprocessingScraper
from services.data_processor import RealEstateDataProcessor

logger = setup_logger(__name__)

def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        Parsed arguments object.
    """
    parser = argparse.ArgumentParser(description="Real Estate Scraper")
    parser.add_argument(
        "--output", 
        type=str, 
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})"
    )
    parser.add_argument(
        "--start-page", 
        type=int, 
        default=DEFAULT_START_PAGE,
        help=f"Starting page number (default: {DEFAULT_START_PAGE})"
    )
    parser.add_argument(
        "--end-page", 
        type=int, 
        default=None,
        help=f"Ending page number (default: {DEFAULT_END_PAGE})"
    )
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=DEFAULT_BATCH_SIZE,
        help=f"Number of properties to collect before saving (default: {DEFAULT_BATCH_SIZE})"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of worker processes for multiprocessing (default: {DEFAULT_WORKERS})"
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=DEFAULT_RATE_LIMIT,
        help=f"Rate limit in requests per second across all workers (default: {DEFAULT_RATE_LIMIT})"
    )
    parser.add_argument(
        "--use-multiprocessing",
        action="store_true",
        help="Use multiprocessing for faster scraping (default: False)"
    )
    parser.add_argument(
        "--use-cache",
        action="store_true",
        help="Use cache to avoid redundant requests (default: False)"
    )
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Run benchmarking to compare performance (default: False)"
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear the cache before starting (default: False)"
    )
    parser.add_argument(
        "--retry-failed-only",
        action="store_true",
        help="Only retry previously failed pages (default: False)"
    )
    parser.add_argument(
        "--disable-auto-detect",
        action="store_true",
        help="Disable automatic last page detection (default: False)"
    )
    parser.add_argument(
        "--no-retry",
        action="store_true",
        help="Skip retrying failed pages (default: False)"
    )
    return parser.parse_args()

def run_single_process(args):
    """
    Run scraper in a single process.
    
    Args:
        args: Command line arguments
        
    Returns:
        List of properties
    """
    output_path = ensure_directory(args.output)
    checkpoint_path = output_path / CHECKPOINT_FILE
    data_path = output_path / DATA_FILE
    failed_pages_path = output_path / FAILED_PAGES_FILE
    
    scraper = RealEstateScraper(use_cache=args.use_cache)
    return scraper.scrape_properties(
        output_path=output_path,
        checkpoint_path=checkpoint_path,
        data_path=data_path,
        failed_pages_path=failed_pages_path,
        start_page=args.start_page,
        end_page=args.end_page,
        batch_size=args.batch_size,
        retry_failed=not args.no_retry
    )

def run_multiprocess(args):
    """
    Run scraper with multiple processes.
    
    Args:
        args: Command line arguments
        
    Returns:
        List of properties
    """
    mp_scraper = MultiprocessingScraper()
    return mp_scraper.scrape_with_multiprocessing(
        output_dir=args.output,
        start_page=args.start_page,
        end_page=args.end_page,
        num_workers=args.workers,
        use_cache=args.use_cache,
        rate_limit=args.rate_limit
    )

def retry_failed_pages(args):
    """
    Retry only previously failed pages.
    
    Args:
        args: Command line arguments
        
    Returns:
        List of properties
    """
    output_path = ensure_directory(args.output)
    data_path = output_path / DATA_FILE
    failed_pages_path = output_path / FAILED_PAGES_FILE
    
    # Load failed pages
    failed_pages = load_failed_pages(failed_pages_path)
    if not failed_pages:
        logger.info("No failed pages to retry")
        return []
    
    logger.info(f"Retrying {len(failed_pages)} failed pages")
    
    # Create temporary checkpoint file for failed pages
    retry_checkpoint_path = output_path / "retry_checkpoint.json"
    
    # Initialize scraper
    scraper = RealEstateScraper(use_cache=args.use_cache)
    
    # Retry each failed page individually
    all_properties = []
    for page in sorted(failed_pages):
        logger.info(f"Retrying page {page}")
        
        # Try to fetch the page with more retries
        scraper.token = scraper.get_auth_token()
        if not scraper.token:
            logger.critical("Failed to get auth token for retries.")
            break
        
        result, need_refresh = scraper.fetch_page_data(page, max_retries=5)
        
        if need_refresh:
            scraper.token = scraper.get_auth_token()
            if not scraper.token:
                logger.critical("Failed to refresh token during retries.")
                break
            time.sleep(4.0)
            result, _ = scraper.fetch_page_data(page, max_retries=5)
        
        if result:
            logger.info(f"Successfully fetched page {page} on retry with {len(result)} properties")
            all_properties.extend(result)
            failed_pages.remove(page)
        else:
            logger.warning(f"Failed to fetch page {page} again")
        
        # Save progress after each page
        save_failed_pages(failed_pages_path, failed_pages)
        time.sleep(random.uniform(3.0, 6.0))  # Longer delay between retries
    
    logger.info(f"Retry results: {len(all_properties)} properties fetched, {len(failed_pages)} pages still failed")
    return all_properties

def clear_cache():
    """
    Clear the cache directory.
    """
    cache_path = Path(CACHE_DIR)
    if cache_path.exists():
        for file in cache_path.glob("*.json"):
            file.unlink()
        logger.info(f"Cleared cache directory: {CACHE_DIR}")
    else:
        logger.info(f"Cache directory does not exist: {CACHE_DIR}")

def run_benchmark(args):
    """
    Run benchmark comparing single-process and multi-process scraping.
    
    Args:
        args: Command line arguments
    """
    logger.info("Running benchmark comparison...")
    
    # Define smaller range for benchmarking
    benchmark_args = argparse.Namespace(
        output=args.output,
        start_page=args.start_page,
        end_page=min(args.start_page + 19, args.end_page),  # 20 pages for benchmark
        batch_size=args.batch_size,
        workers=args.workers,
        use_cache=args.use_cache,
        rate_limit=args.rate_limit,
        no_retry=True  # Don't retry during benchmark
    )
    
    # Compare methods
    methods = {
        "Single Process": lambda: run_single_process(benchmark_args),
        "Multiprocessing": lambda: run_multiprocess(benchmark_args)
    }
    
    # Run benchmark
    compare_performance(methods)

def main():
    """
    Main function to run the scraper and data processor.
    """
    logger.info("Starting real estate scraper")
    
    # Parse arguments
    args = parse_arguments()
    
    # Clear cache if requested
    if args.clear_cache:
        clear_cache()
    
    # Run benchmark if requested
    if args.benchmark:
        run_benchmark(args)
        return
    
    # Decide which scraping method to use
    if args.retry_failed_only:
        logger.info("Retrying only previously failed pages")
        properties = retry_failed_pages(args)
    elif args.use_multiprocessing:
        logger.info(f"Using multiprocessing with {args.workers} workers and rate limit of {args.rate_limit} req/sec")
        properties = run_multiprocess(args)
    else:
        logger.info("Using single process")
        properties = run_single_process(args)
    
    # Process data
    if properties:
        output_path = ensure_directory(args.output)
        processed_path = output_path / PROCESSED_FILE
        
        processor = RealEstateDataProcessor()
        df = processor.process_data(properties, processed_path)
        
        if df is not None:
            logger.info(f"Successfully processed {len(df)} properties")
        else:
            logger.warning("Data processing failed")
    else:
        logger.warning("No properties were scraped")
    
    logger.info("Scraping and processing completed")

if __name__ == "__main__":
    import random  # Import needed for retry_failed_pages
    main()