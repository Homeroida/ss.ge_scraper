"""
Pagination utilities for detecting the last page of results.
"""
import re
import xml.etree.ElementTree as ET
import requests
from pathlib import Path
from typing import Optional, Tuple
import random

from config import BASE_URL, USER_AGENTS
from utils.logging_utils import setup_logger

logger = setup_logger(__name__)

def detect_last_page_from_api(api_response: dict) -> Optional[int]:
    """
    Try to detect the last page number from API response metadata.
    
    Args:
        api_response: The JSON response from the API
        
    Returns:
        The last page number if found, None otherwise
    """
    # Check if response contains pagination metadata
    if "meta" in api_response and "totalPages" in api_response["meta"]:
        return api_response["meta"]["totalPages"]
    
    # Check for total count and items per page
    if "meta" in api_response and "totalCount" in api_response["meta"] and "pageSize" in api_response["meta"]:
        total_count = api_response["meta"]["totalCount"]
        page_size = api_response["meta"]["pageSize"]
        if page_size > 0:
            return (total_count + page_size - 1) // page_size
    
    # Other possible pagination indicators in the response
    if "pagination" in api_response:
        pagination = api_response["pagination"]
        if "totalPages" in pagination:
            return pagination["totalPages"]
        if "lastPage" in pagination:
            return pagination["lastPage"]
    
    return None

def is_last_page(api_response: dict) -> bool:
    """
    Determine if we've reached the last page based on API response.
    
    Args:
        api_response: The JSON response from the API
        
    Returns:
        True if this appears to be the last page, False otherwise
    """
    # Check if response contains empty items
    if "realStateItemModel" in api_response and len(api_response["realStateItemModel"]) == 0:
        return True
    
    # Check if response indicates it's the last page
    if "meta" in api_response and "isLastPage" in api_response["meta"]:
        return api_response["meta"]["isLastPage"]
    
    # Check if current page equals total pages
    if "meta" in api_response and "currentPage" in api_response["meta"] and "totalPages" in api_response["meta"]:
        return api_response["meta"]["currentPage"] >= api_response["meta"]["totalPages"]
    
    return False

def fetch_sitemap(sitemap_url: str = None) -> Optional[str]:
    """
    Fetch sitemap content from URL or local file.
    
    Args:
        sitemap_url: URL of the sitemap or None to use local file
        
    Returns:
        Sitemap content as string or None if fetch fails
    """
    if sitemap_url:
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            response = requests.get(sitemap_url, headers=headers, timeout=(10, 30))
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Failed to fetch sitemap from URL: {e}")
            return None
    
    # Try to use a local sitemap file
    sitemap_path = Path("sitemap.xml")
    if sitemap_path.exists():
        try:
            with open(sitemap_path, "r", encoding="utf-8") as f:
                return f.read()
        except IOError as e:
            logger.error(f"Failed to read local sitemap: {e}")
            return None
    
    return None

def parse_page_numbers_from_sitemap(sitemap_content: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Parse sitemap to extract the highest page number.
    
    Args:
        sitemap_content: XML content of the sitemap
        
    Returns:
        Tuple containing (highest listing page, total pages if found)
    """
    try:
        root = ET.fromstring(sitemap_content)
        namespace = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        highest_page = 0
        for sitemap in root.findall('.//sm:sitemap', namespace):
            loc = sitemap.find('sm:loc', namespace)
            if loc is not None and loc.text:
                # Look for listing page patterns
                listing_match = re.search(r'sitemap-listing-(\d+)\.xml', loc.text)
                if listing_match:
                    page_num = int(listing_match.group(1))
                    highest_page = max(highest_page, page_num)
        
        # Try to find total pages from other indicators in the sitemap
        total_sitemaps = len(root.findall('.//sm:sitemap', namespace))
        
        return highest_page, total_sitemaps
    except (ET.ParseError, ValueError) as e:
        logger.error(f"Failed to parse sitemap: {e}")
        return None, None

def estimate_last_page(base_url: str = BASE_URL) -> int:
    """
    Estimate the last page number using sitemap data and fallbacks.
    
    Args:
        base_url: Base URL of the website
        
    Returns:
        Estimated last page number or fallback default
    """
    # Try to get sitemap from the root domain
    sitemap_url = f"{base_url.rstrip('/')}/sitemap.xml"
    sitemap_content = fetch_sitemap(sitemap_url)
    
    # If we couldn't get from URL, try local file
    if not sitemap_content:
        sitemap_content = fetch_sitemap()
    
    if sitemap_content:
        highest_listing, total_sitemaps = parse_page_numbers_from_sitemap(sitemap_content)
        
        if highest_listing is not None and highest_listing > 0:
            # Listing pages typically contain 1000 property entries per page
            # If we found highest_listing is 2, there could be 3000 property pages (0, 1, 2)
            estimated_properties = (highest_listing + 1) * 1000
            # With 16 properties per page, calculate total pages
            page_size = 16  # From config
            estimated_pages = (estimated_properties + page_size - 1) // page_size
            logger.info(f"Estimated last page from sitemap: {estimated_pages}")
            return estimated_pages
    
    # Fallback to default
    fallback = 15972  # Original hardcoded value
    logger.warning(f"Couldn't detect last page, using fallback: {fallback}")
    return fallback