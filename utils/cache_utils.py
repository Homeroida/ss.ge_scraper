"""
Caching utilities for the real estate scraper.
"""
import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

from utils.logging_utils import setup_logger
from config import CACHE_EXPIRY

logger = setup_logger(__name__)

def ensure_cache_dir(cache_dir: Path) -> Path:
    """
    Ensure the cache directory exists.
    
    Args:
        cache_dir: Path to the cache directory
        
    Returns:
        Path to the cache directory
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir

def get_cache_key(page: int, params: Dict[str, Any]) -> str:
    """
    Generate a cache key based on request parameters.
    
    Args:
        page: Page number
        params: Request parameters
        
    Returns:
        Cache key string
    """
    # Use only relevant parts of the params to create the key
    key_parts = {
        "page": page,
        "realEstateType": params.get("realEstateType"),
        "realEstateDealType": params.get("realEstateDealType"),
        "cityIdList": str(params.get("cityIdList")),
        "currencyId": params.get("currencyId")
    }
    return f"page_{page}_{hash(frozenset(key_parts.items()))}"

def get_from_cache(cache_dir: Path, cache_key: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve data from cache if available and not expired.
    
    Args:
        cache_dir: Path to the cache directory
        cache_key: Cache key string
        
    Returns:
        Cached data or None if not found or expired
    """
    cache_file = cache_dir / f"{cache_key}.json"
    
    if not cache_file.exists():
        return None
    
    # Check if cache is expired
    file_age = time.time() - cache_file.stat().st_mtime
    if file_age > CACHE_EXPIRY:
        logger.debug(f"Cache expired for {cache_key}")
        return None
    
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            logger.debug(f"Cache hit for {cache_key}")
            return data
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Error reading cache for {cache_key}: {e}")
        return None

def save_to_cache(cache_dir: Path, cache_key: str, data: Dict[str, Any]) -> bool:
    """
    Save data to cache.
    
    Args:
        cache_dir: Path to the cache directory
        cache_key: Cache key string
        data: Data to cache
        
    Returns:
        True if successful, False otherwise
    """
    cache_file = cache_dir / f"{cache_key}.json"
    
    try:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        logger.debug(f"Cached data for {cache_key}")
        return True
    except IOError as e:
        logger.warning(f"Error caching data for {cache_key}: {e}")
        return False

def clear_expired_cache(cache_dir: Path) -> int:
    """
    Clear expired cache files.
    
    Args:
        cache_dir: Path to the cache directory
        
    Returns:
        Number of deleted cache files
    """
    if not cache_dir.exists():
        return 0
    
    count = 0
    current_time = time.time()
    
    for cache_file in cache_dir.glob("*.json"):
        file_age = current_time - cache_file.stat().st_mtime
        if file_age > CACHE_EXPIRY:
            cache_file.unlink()
            count += 1
    
    logger.info(f"Cleared {count} expired cache files")
    return count