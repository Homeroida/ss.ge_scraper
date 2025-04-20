"""
Logging utilities for the real estate scraper.
"""
import logging
import sys
from typing import Optional

def setup_logger(name: str = None, level: int = logging.INFO) -> logging.Logger:
    """
    Set up and configure a logger.
    
    Args:
        name: Name of the logger. If None, returns the root logger.
        level: Logging level. Default is INFO.
        
    Returns:
        A configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Check if the logger already has handlers to avoid duplicates
    if not logger.handlers:
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        
        # Format
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        console_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(console_handler)
    
    return logger