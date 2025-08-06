"""
Logging utilities for Amazon Jobs Scraper
"""

import logging
import sys
from pathlib import Path
from typing import Optional
import time

def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    log_format: Optional[str] = None
) -> logging.Logger:
    """
    Setup logging configuration for the scraper.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (optional)
        log_format: Log message format (optional)
        
    Returns:
        Configured logger instance
    """
    
    # Default format
    if log_format is None:
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Create logs directory if it doesn't exist
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=log_format,
        handlers=[
            # Console handler for immediate feedback
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Add file handler if log_file is specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(log_format))
        logging.getLogger().addHandler(file_handler)
    
    # Create logger for this module
    logger = logging.getLogger(__name__)
    logger.info("=== Amazon Jobs Scraper Logging Setup ===")
    
    return logger

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the specified name.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)

def log_execution_time(func):
    """
    Decorator to log function execution time.
    
    Args:
        func: Function to decorate
        
    Returns:
        Decorated function
    """
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"{func.__name__} completed in {execution_time:.2f} seconds")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"{func.__name__} failed after {execution_time:.2f} seconds: {e}")
            raise
    
    return wrapper

def log_scraper_stats(
    total_jobs: int,
    active_jobs: int,
    execution_time: float,
    logger: Optional[logging.Logger] = None
):
    """
    Log scraper execution statistics.
    
    Args:
        total_jobs: Total number of jobs processed
        active_jobs: Number of active jobs
        execution_time: Execution time in seconds
        logger: Logger instance (optional)
    """
    if logger is None:
        logger = get_logger(__name__)
    
    logger.info("=== SCRAPER STATISTICS ===")
    logger.info(f"Total jobs processed: {total_jobs}")
    logger.info(f"Active jobs: {active_jobs}")
    logger.info(f"Inactive jobs: {total_jobs - active_jobs}")
    logger.info(f"Execution time: {execution_time:.2f} seconds")
    logger.info(f"Jobs per second: {total_jobs/execution_time:.2f}" if execution_time > 0 else "N/A")
    logger.info("=" * 30) 