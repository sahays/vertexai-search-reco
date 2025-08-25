"""Manager classes for Vertex AI Search operations.

This module provides backward compatibility by importing from the new modular structure.
The actual manager classes are now located in the managers/ directory.
"""

# Import all manager classes from the new modular structure
from .managers.dataset_manager import DatasetManager
from .managers.media_asset_manager import MediaAssetManager
from .managers.search_manager import SearchManager
from .managers.autocomplete_manager import AutocompleteManager
from .managers.recommendation_manager import RecommendationManager

__all__ = [
    'DatasetManager',
    'MediaAssetManager',
    'SearchManager', 
    'AutocompleteManager',
    'RecommendationManager'
]