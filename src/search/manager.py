"""Search engine management functionality."""

# Import search-related managers
from .search_manager import SearchManager

# Re-export for the search domain
__all__ = [
    'SearchManager'
]