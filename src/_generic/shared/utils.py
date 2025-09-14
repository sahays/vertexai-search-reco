"""Utility functions for logging and error handling."""

import logging
import sys
from pathlib import Path
from typing import Optional
from rich.logging import RichHandler
from rich.console import Console

console = Console()


def setup_logging(
    level: str = "INFO",
    log_file: Optional[Path] = None,
    include_rich: bool = True
) -> logging.Logger:
    """Set up logging configuration."""
    
    # Convert string level to logging level
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Create logger
    logger = logging.getLogger("vertex_search")
    logger.setLevel(numeric_level)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Add file handler if specified
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    # Add console handler
    if include_rich:
        # Use Rich handler for colored output
        console_handler = RichHandler(
            console=console,
            show_time=False,
            show_path=False
        )
    else:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(file_formatter)
    
    console_handler.setLevel(numeric_level)
    logger.addHandler(console_handler)
    
    return logger


class VertexSearchError(Exception):
    """Base exception for Vertex Search operations."""
    pass


class ConfigurationError(VertexSearchError):
    """Raised when there's a configuration error."""
    pass


class SchemaValidationError(VertexSearchError):
    """Raised when schema validation fails."""
    pass


class DataStoreError(VertexSearchError):
    """Raised when data store operations fail."""
    pass


class SearchError(VertexSearchError):
    """Raised when search operations fail."""
    pass


def handle_vertex_ai_error(func):
    """Decorator to handle common Vertex AI errors."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_msg = str(e)
            
            # Map common Google Cloud errors to more user-friendly messages
            if "403" in error_msg or "Forbidden" in error_msg:
                raise VertexSearchError(
                    "Permission denied. Please check your Google Cloud credentials and permissions."
                ) from e
            elif "404" in error_msg or "Not Found" in error_msg:
                raise VertexSearchError(
                    "Resource not found. Please check if the data store or engine exists."
                ) from e
            elif "429" in error_msg or "Quota" in error_msg:
                raise VertexSearchError(
                    "Rate limit exceeded. Please try again later."
                ) from e
            elif "INVALID_ARGUMENT" in error_msg:
                raise VertexSearchError(
                    f"Invalid request parameters: {error_msg}"
                ) from e
            else:
                raise VertexSearchError(f"Vertex AI operation failed: {error_msg}") from e
    
    return wrapper


def validate_required_fields(data: dict, required_fields: list, context: str = ""):
    """Validate that required fields are present in data."""
    missing_fields = []
    
    for field in required_fields:
        if '.' in field:
            # Handle nested fields like 'rating.mpaa_rating'
            parts = field.split('.')
            current = data
            
            for part in parts:
                if not isinstance(current, dict) or part not in current:
                    missing_fields.append(field)
                    break
                current = current[part]
        else:
            if field not in data:
                missing_fields.append(field)
    
    if missing_fields:
        context_msg = f" in {context}" if context else ""
        raise ConfigurationError(
            f"Missing required fields{context_msg}: {', '.join(missing_fields)}"
        )


def safe_get_nested(data: dict, path: str, default=None):
    """Safely get a nested value from a dictionary using dot notation."""
    try:
        current = data
        for part in path.split('.'):
            current = current[part]
        return current
    except (KeyError, TypeError):
        return default


def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


def truncate_text(text: str, max_length: int = 100) -> str:
    """Truncate text to specified length with ellipsis."""
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."