"""
File utilities for the real estate scraper.
"""
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Set

def load_checkpoint(checkpoint_path: Path) -> int:
    """
    Load the last processed page from a checkpoint file.
    
    Args:
        checkpoint_path: Path to the checkpoint file.
        
    Returns:
        The last processed page number or 0 if no checkpoint exists.
    """
    if checkpoint_path.exists():
        with open(checkpoint_path, "r") as f:
            data = json.load(f)
            return data.get("last_page", 0)
    return 0

def save_checkpoint(checkpoint_path: Path, page: int) -> None:
    """
    Save the current processing state to a checkpoint file.
    
    Args:
        checkpoint_path: Path to the checkpoint file.
        page: Current page number.
    """
    with open(checkpoint_path, "w") as f:
        json.dump({
            "last_page": page,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }, f)

def save_properties(filepath: Path, properties: List[Dict[str, Any]]) -> None:
    """
    Save property data to a JSON file.
    
    Args:
        filepath: Path to save the data.
        properties: List of property data dictionaries.
    """
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(properties, f, ensure_ascii=False)

def load_properties(filepath: Path) -> List[Dict[str, Any]]:
    """
    Load property data from a JSON file.
    
    Args:
        filepath: Path to the data file.
        
    Returns:
        List of property data dictionaries or an empty list if file doesn't exist.
    """
    if filepath.exists():
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_failed_pages(filepath: Path, failed_pages: Set[int]) -> None:
    """
    Save failed pages to a JSON file.
    
    Args:
        filepath: Path to save the data.
        failed_pages: Set of failed page numbers.
    """
    with open(filepath, "w") as f:
        json.dump({
            "failed_pages": sorted(list(failed_pages)),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "count": len(failed_pages)
        }, f)

def load_failed_pages(filepath: Path) -> Set[int]:
    """
    Load failed pages from a JSON file.
    
    Args:
        filepath: Path to the failed pages file.
        
    Returns:
        Set of failed page numbers or an empty set if file doesn't exist.
    """
    if filepath.exists():
        with open(filepath, "r") as f:
            data = json.load(f)
            return set(data.get("failed_pages", []))
    return set()

def ensure_directory(directory: Union[str, Path]) -> Path:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        directory: Directory path.
        
    Returns:
        Path object of the directory.
    """
    dir_path = Path(directory)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path