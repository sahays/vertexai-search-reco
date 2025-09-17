"""Utility functions for Media Data Store."""

import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from functools import wraps


class MediaDataStoreError(Exception):
    """Base exception for Media Data Store operations."""
    pass


def setup_logging(log_dir: Optional[Path] = None, level: int = logging.INFO, subcommand: Optional[str] = None, module_name: str = "datastore") -> logging.Logger:
    """Setup logging with optional file output."""
    logger = logging.getLogger("media_data_store")
    logger.setLevel(level)
    
    # Clear existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create detailed formatter for file
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    
    # Create simple formatter for console
    console_formatter = logging.Formatter(
        '%(levelname)s - %(message)s'
    )
    
    # Console handler - show INFO and above
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    
    # File handler if log_dir specified - capture ALL levels including DEBUG
    if log_dir:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%H%M%S%f")[:9]  # HHMMSSF (includes first 3 microsecond digits)
        subcommand_part = f"-{subcommand}" if subcommand else ""
        log_file = log_dir / f"{timestamp}{subcommand_part}-{module_name}.log"
        
        file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)  # Capture everything in file
        logger.addHandler(file_handler)
        
        # Also set root logger to capture from other modules
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        
        # Add file handler to root logger to catch all logging
        root_file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        root_file_handler.setFormatter(file_formatter)
        root_file_handler.setLevel(logging.DEBUG)
        root_logger.addHandler(root_file_handler)
        
        logger.info(f"Verbose logging enabled. Log file: {log_file}")
        logger.debug("Debug logging started")
    
    return logger


def handle_vertex_ai_error(func):
    """Decorator to handle Vertex AI errors."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger = logging.getLogger("media_data_store")
            logger.error(f"Vertex AI operation failed: {e}")
            raise MediaDataStoreError(f"Operation failed: {e}") from e
    return wrapper


def save_output(data: Any, output_dir: Path, filename: str, subcommand: Optional[str] = None) -> Path:
    """Save data to output directory with timestamp."""
    logger = logging.getLogger("media_data_store")
    
    # Add timestamp and subcommand to filename to avoid overwrites  
    timestamp = datetime.now().strftime("%H%M%S%f")[:9]  # HHMMSSF (includes first 3 microsecond digits)
    subcommand_part = f"-{subcommand}" if subcommand else ""
    
    name_parts = filename.rsplit('.', 1)
    if len(name_parts) == 2:
        timestamped_filename = f"{timestamp}{subcommand_part}-{name_parts[0]}.{name_parts[1]}"
    else:
        timestamped_filename = f"{timestamp}{subcommand_part}-{filename}"
    
    logger.debug(f"Saving output to: {output_dir / timestamped_filename}")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / timestamped_filename
    
    if isinstance(data, (dict, list)):
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        logger.debug(f"Saved JSON data ({type(data).__name__}) to {output_path}")
    else:
        with open(output_path, 'w') as f:
            f.write(str(data))
        logger.debug(f"Saved text data to {output_path}")
    
    return output_path


def load_json_file(file_path: Path) -> Dict[str, Any]:
    """Load JSON data from file."""
    logger = logging.getLogger("media_data_store")
    logger.debug(f"Loading JSON file: {file_path}")
    
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            logger.debug(f"Successfully loaded JSON with {len(data)} items" if isinstance(data, list) else f"Successfully loaded JSON object with {len(data)} fields")
            return data
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {file_path}: {e}")
        raise MediaDataStoreError(f"Invalid JSON in {file_path}: {e}")
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise MediaDataStoreError(f"File not found: {file_path}")


def validate_media_data(data: Dict[str, Any], required_fields: list) -> bool:
    """Validate media data contains required fields."""
    missing_fields = [field for field in required_fields if field not in data]
    if missing_fields:
        raise MediaDataStoreError(f"Missing required fields: {missing_fields}")
    return True