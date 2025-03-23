import os
import logging
import sys
from pathlib import Path


def setup_logger():
    # Determine the project root and ensure logs directory exists
    if "src" in os.getcwd():
        # Running from src
        project_root = Path(os.getcwd()).parent
    else:
        # Running from project root
        project_root = Path(os.getcwd())

    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)

    # File paths for different log levels
    debug_log_file = logs_dir / "debug.log"
    info_log_file = logs_dir / "info.log"

    # Create a specific logger
    logger = logging.getLogger('soundcharts_logger')

    # Ensure no duplicate logs in the console
    if logger.hasHandlers():
        logger.handlers.clear()

    # Handlers
    debug_handler = logging.FileHandler(debug_log_file, encoding='utf-8')
    info_handler = logging.FileHandler(info_log_file, encoding='utf-8')
    console_handler = logging.StreamHandler(sys.stdout)

    # Set logging levels for handlers
    debug_handler.setLevel(logging.DEBUG)
    info_handler.setLevel(logging.INFO)
    console_handler.setLevel(logging.DEBUG)

    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s [Line: %(lineno)d in %(filename)s]')
    debug_handler.setFormatter(formatter)
    info_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(debug_handler)
    logger.addHandler(info_handler)
    logger.addHandler(console_handler)

    # Set logger level
    logger.setLevel(logging.DEBUG)

    return logger


# Initialize the logger
logger = setup_logger()
