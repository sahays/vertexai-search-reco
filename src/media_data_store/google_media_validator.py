"""Google Cloud media schema validation following official requirements."""

import re
from datetime import datetime
from typing import Dict, Any, List, Optional
from dateutil.parser import parse as parse_datetime

from .utils import MediaDataStoreError, setup_logging

logger = setup_logging()


class GoogleMediaValidator:
    """Validates media data against Google Cloud's official media schema requirements."""
    
    # Google's supported media categories/types
    SUPPORTED_MEDIA_TYPES = {
        "movie", "show", "podcast", "music", "news", "sports", "live",
        "educational", "documentary", "comedy", "drama", "action", "horror",
        "romance", "thriller", "animation", "family", "adventure", "fantasy",
        "crime", "mystery", "western", "war", "biography", "history"
    }
    
    # Duration format pattern (e.g., "5s", "1m", "2h", "1h30m")  
    DURATION_PATTERN = re.compile(r'^(\d+h)?(\d+m)?(\d+s)?$')
    
    @staticmethod
    def validate_required_fields(data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate Google's 5 required media fields."""
        errors = []
        warnings = []
        
        # 1. Title validation (required, max 1000 chars)
        title = data.get("title")
        if not title:
            errors.append("Missing required field: title")
        elif not isinstance(title, str):
            errors.append("Field 'title' must be a string")
        elif len(title) > 1000:
            errors.append(f"Field 'title' exceeds 1000 characters: {len(title)}")
        
        # 2. URI validation (required, max 5000 chars)
        uri = data.get("uri")
        if not uri:
            errors.append("Missing required field: uri")
        elif not isinstance(uri, str):
            errors.append("Field 'uri' must be a string")
        elif len(uri) > 5000:
            errors.append(f"Field 'uri' exceeds 5000 characters: {len(uri)}")
        elif not GoogleMediaValidator._is_valid_uri(uri):
            warnings.append(f"Field 'uri' may not be a valid URI: {uri}")
        
        # 3. Categories validation (required, string array, max 250)
        categories = data.get("categories")
        if not categories:
            errors.append("Missing required field: categories")
        elif not isinstance(categories, list):
            errors.append("Field 'categories' must be a string array")
        elif len(categories) > 250:
            errors.append(f"Field 'categories' exceeds 250 items: {len(categories)}")
        else:
            for i, category in enumerate(categories):
                if not isinstance(category, str):
                    errors.append(f"Category at index {i} must be a string")
                elif category.lower() not in GoogleMediaValidator.SUPPORTED_MEDIA_TYPES:
                    warnings.append(f"Category '{category}' not in supported media types")
        
        # 4. Available time validation (required, RFC 3339 datetime)
        available_time = data.get("available_time")
        if not available_time:
            errors.append("Missing required field: available_time")
        elif not isinstance(available_time, str):
            errors.append("Field 'available_time' must be a string in RFC 3339 format")
        else:
            try:
                parse_datetime(available_time)
                logger.debug(f"Valid RFC 3339 datetime: {available_time}")
            except Exception as e:
                errors.append(f"Field 'available_time' is not valid RFC 3339 datetime: {e}")
        
        # 5. Duration validation (required, string format like "5s", "1m", "2h30m")
        duration = data.get("duration")
        if not duration:
            errors.append("Missing required field: duration")
        elif not isinstance(duration, str):
            errors.append("Field 'duration' must be a string")
        elif not GoogleMediaValidator.DURATION_PATTERN.match(duration):
            errors.append(f"Field 'duration' must be in format like '5s', '1m', '2h30m': {duration}")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "required_fields_status": {
                "title": bool(title and len(str(title)) <= 1000),
                "uri": bool(uri and len(str(uri)) <= 5000),
                "categories": bool(categories and isinstance(categories, list) and len(categories) <= 250),
                "available_time": bool(available_time and GoogleMediaValidator._is_valid_datetime(available_time)),
                "duration": bool(duration and GoogleMediaValidator.DURATION_PATTERN.match(str(duration)))
            }
        }
    
    @staticmethod
    def validate_optional_fields(data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate optional Google media fields."""
        warnings = []
        
        # Language validation (BCP 47 tags)
        language = data.get("language")
        if language and not GoogleMediaValidator._is_valid_bcp47(language):
            warnings.append(f"Language '{language}' may not be valid BCP 47 tag")
        
        # Persons validation (array of objects with name, role)
        persons = data.get("persons", [])
        if persons and isinstance(persons, list):
            for i, person in enumerate(persons):
                if not isinstance(person, dict):
                    warnings.append(f"Person at index {i} should be an object")
                elif "name" not in person:
                    warnings.append(f"Person at index {i} missing 'name' field")
        
        # Organizations validation
        organizations = data.get("organizations", [])
        if organizations and isinstance(organizations, list):
            for i, org in enumerate(organizations):
                if not isinstance(org, dict):
                    warnings.append(f"Organization at index {i} should be an object")
                elif "name" not in org:
                    warnings.append(f"Organization at index {i} missing 'name' field")
        
        return {
            "warnings": warnings,
            "optional_fields_found": {
                "language": "language" in data,
                "persons": "persons" in data and len(data.get("persons", [])) > 0,
                "organizations": "organizations" in data and len(data.get("organizations", [])) > 0,
                "rating": "rating" in data
            }
        }
    
    @staticmethod
    def _is_valid_uri(uri: str) -> bool:
        """Basic URI validation."""
        return uri.startswith(('http://', 'https://', 'gs://', 'file://')) or '://' in uri
    
    @staticmethod
    def _is_valid_datetime(datetime_str: str) -> bool:
        """Validate RFC 3339 datetime string."""
        try:
            parse_datetime(datetime_str)
            return True
        except:
            return False
    
    @staticmethod
    def _is_valid_bcp47(language_tag: str) -> bool:
        """Basic BCP 47 language tag validation."""
        # Basic pattern: language[-script][-region]
        pattern = re.compile(r'^[a-z]{2,3}(-[A-Z][a-z]{3})?(-[A-Z]{2}|\d{3})?$')
        return pattern.match(language_tag) is not None
    
    @staticmethod
    def convert_duration_to_google_format(seconds: int) -> str:
        """Convert duration in seconds to Google's required string format."""
        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            minutes = seconds // 60
            remaining_seconds = seconds % 60
            if remaining_seconds:
                return f"{minutes}m{remaining_seconds}s"
            return f"{minutes}m"
        else:
            hours = seconds // 3600
            remaining_seconds = seconds % 3600
            minutes = remaining_seconds // 60
            remaining_seconds = remaining_seconds % 60
            
            result = f"{hours}h"
            if minutes:
                result += f"{minutes}m"
            if remaining_seconds:
                result += f"{remaining_seconds}s"
            return result
    
    @staticmethod
    def normalize_categories(categories: List[str]) -> List[str]:
        """Normalize categories to Google's supported media types."""
        normalized = []
        for category in categories:
            category_lower = category.lower().strip()
            if category_lower in GoogleMediaValidator.SUPPORTED_MEDIA_TYPES:
                normalized.append(category_lower)
            else:
                # Try to map common variations
                mappings = {
                    "film": "movie",
                    "series": "show", 
                    "tv": "show",
                    "television": "show",
                    "music video": "music",
                    "comedy special": "comedy",
                    "standup": "comedy",
                    "documentary film": "documentary",
                    "nature": "documentary"
                }
                mapped = mappings.get(category_lower)
                if mapped:
                    normalized.append(mapped)
                else:
                    normalized.append(category_lower)  # Keep original but warn
        
        return normalized[:250]  # Enforce max 250 limit