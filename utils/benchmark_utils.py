"""
Benchmarking utilities for the real estate scraper.
"""
import time
import functools
from typing import Any, Callable, Dict, Tuple
from utils.logging_utils import setup_logger

logger = setup_logger(__name__)

def benchmark(func: Callable) -> Callable:
    """
    Decorator to benchmark a function's execution time.
    
    Args:
        func: Function to benchmark
        
    Returns:
        Wrapped function that logs execution time
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"{func.__name__} executed in {execution_time:.2f} seconds")
        return result
    return wrapper

def compare_performance(methods: Dict[str, Callable], *args, **kwargs) -> Dict[str, float]:
    """
    Compare performance of multiple methods.
    
    Args:
        methods: Dictionary mapping method names to functions
        *args, **kwargs: Arguments to pass to each method
        
    Returns:
        Dictionary mapping method names to execution times
    """
    results = {}
    
    for name, method in methods.items():
        logger.info(f"Benchmarking {name}...")
        start_time = time.time()
        method(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        results[name] = execution_time
        logger.info(f"{name} executed in {execution_time:.2f} seconds")
    
    # Print comparison summary
    logger.info("Performance comparison:")
    for name, execution_time in sorted(results.items(), key=lambda x: x[1]):
        logger.info(f"{name}: {execution_time:.2f} seconds")
    
    return results