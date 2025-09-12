"""Autocomplete management functionality."""

# Import autocomplete-related managers
from .autocomplete_manager import AutocompleteManager

# Re-export for the autocomplete domain
__all__ = [
    'AutocompleteManager'
]