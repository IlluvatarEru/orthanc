"""
Shared logging configuration for all Orthanc launch scripts.

Usage:
    from common.src.logging_config import setup_logging
    logger = setup_logging("my_script")
"""

import logging
import os


def setup_logging(
    name: str,
    log_file: str = None,
    level: int = logging.INFO,
    log_dir: str = "logs",
) -> logging.Logger:
    """
    Configure logging with consistent format across all scripts.

    :param name: str, logger name (typically __name__ or script name)
    :param log_file: str, optional log file name (e.g., "scraper.log"). If None, only logs to console.
    :param level: int, logging level (default: INFO)
    :param log_dir: str, directory for log files (default: "logs")
    :return: logging.Logger, configured logger
    """
    handlers = [logging.StreamHandler()]

    if log_file:
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, log_file)
        handlers.append(logging.FileHandler(log_path))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
        force=True,
    )

    return logging.getLogger(name)
