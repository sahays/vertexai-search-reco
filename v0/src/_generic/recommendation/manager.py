"""Recommendation management functionality."""

# Import recommendation-related managers
from .recommendation_manager import RecommendationManager

# Re-export for the recommendation domain
__all__ = [
    'RecommendationManager'
]