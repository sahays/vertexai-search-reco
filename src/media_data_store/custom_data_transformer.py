"""Customer-specific data transformations for media content."""

import json
import re
from typing import Dict, Any, List, Optional
from datetime import datetime
from dateutil.parser import parse as parse_datetime

from .utils import setup_logging, MediaDataStoreError
from .google_media_validator import GoogleMediaValidator

logger = setup_logging()


class CustomDataTransformer:
    """Transforms customer data to Google media schema format."""
    
    # Language code mappings to BCP 47
    LANGUAGE_MAPPINGS = {
        "hi": "hi-IN",
        "mr": "mr-IN", 
        "ta": "ta-IN",
        "bn": "bn-IN",
        "ml": "ml-IN",
        "kn": "kn-IN",
        "te": "te-IN",
        "en": "en-US",
        "NULL": None
    }
    
    # Genre mappings to Google's supported categories
    GENRE_MAPPINGS = {
        "Comedy": "comedy",
        "Romance": "romance",
        "Drama": "drama", 
        "Horror": "horror",
        "Action": "action",
        "Animation": "animation",
        "Entertainment": "show",
        "Family": "family",
        "Tragedy": "drama",
        "Teen Drama": "drama",
        "Talk Show": "show",
        "Educational": "educational",
        "Technology": "educational",
        "Documentary": "documentary",
        "Nature": "documentary"
    }
    
    @staticmethod
    def transform_customer_record(data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single customer record to Google media schema."""
        logger.debug(f"Transforming customer record ID: {data.get('id')}")
        
        transformed = {}
        
        # Required Google fields mapping
        transformed["title"] = CustomDataTransformer._transform_title(data.get("title", ""))
        transformed["uri"] = CustomDataTransformer._transform_image_to_uri(data.get("image", ""))
        transformed["categories"] = CustomDataTransformer._transform_genre_to_categories(data.get("genre", []))
        transformed["available_time"] = CustomDataTransformer._transform_release_date(data.get("release_date"))
        transformed["duration"] = CustomDataTransformer._transform_episode_count_to_duration(data.get("episode_count", 1))
        
        # Required _id field for custom schema imports
        if data.get("id"):
            transformed["id"] = str(data["id"])
            transformed["_id"] = str(data["id"])  # Required for custom schema imports
        else:
            # Generate a unique ID if missing - required for custom schema
            import uuid
            generated_id = str(uuid.uuid4())
            transformed["id"] = generated_id
            transformed["_id"] = generated_id
            logger.warning(f"Missing ID in source data, generated: {generated_id}")
        
        if data.get("desc"):
            transformed["description"] = data["desc"]
        
        if data.get("audio_lang"):
            transformed["language"] = CustomDataTransformer._normalize_language_code(data["audio_lang"])
            
        if data.get("age_rating") and data["age_rating"] != "NULL":
            # content_rating expects an array, not a string
            transformed["content_rating"] = [data["age_rating"]]
        
        # Transform persons (actors + directors)
        persons = CustomDataTransformer._transform_persons(data.get("actors", []), data.get("directors", []))
        if persons:
            transformed["persons"] = persons
        
        # Add additional metadata
        if data.get("original_title") and data["original_title"] != data.get("title"):
            transformed["original_title"] = data["original_title"]
            
        if data.get("keywords"):
            transformed["keywords"] = data["keywords"]
            
        if data.get("tags"):
            transformed["tags"] = data["tags"] if isinstance(data["tags"], list) else []
            
        # Extract from extended metadata
        if data.get("extended"):
            extended_data = CustomDataTransformer._extract_extended_metadata(data["extended"])
            transformed.update(extended_data)
        
        # Add licensing information with proper RFC3339 formatting
        if data.get("licensing_from") and data.get("licensing_until"):
            transformed["licensing"] = {
                "from": CustomDataTransformer._transform_release_date(data["licensing_from"]),
                "until": CustomDataTransformer._transform_release_date(data["licensing_until"])
            }
            
        if data.get("rights"):
            transformed["distribution_rights"] = data["rights"]
        
        logger.debug(f"Transformed record with {len(transformed)} fields")
        return transformed
    
    @staticmethod
    def _transform_title(title: str) -> str:
        """Transform and validate title field."""
        if not title or title == "NULL":
            raise MediaDataStoreError("Title field is required and cannot be empty")
            
        title = title.strip()
        if len(title) > 1000:
            logger.warning(f"Title exceeds 1000 chars, truncating: {title[:50]}...")
            title = title[:1000]
            
        return title
    
    @staticmethod
    def _transform_image_to_uri(image: str) -> str:
        """Transform image ID to media URI."""
        if not image or image == "NULL":
            raise MediaDataStoreError("Media URI (image) field is required")
            
        # Convert image ID to full GCS URI
        if not image.startswith(('http://', 'https://', 'gs://')):
            uri = f"gs://media-bucket/images/{image}.jpg"
        else:
            uri = image
            
        if len(uri) > 5000:
            raise MediaDataStoreError(f"URI exceeds 5000 characters: {len(uri)}")
            
        return uri
    
    @staticmethod
    def _transform_genre_to_categories(genres: List[str]) -> List[str]:
        """Transform genre array to Google's supported categories."""
        if not genres:
            return ["entertainment"]  # Default category
            
        categories = []
        for genre in genres:
            if isinstance(genre, str):
                mapped_genre = CustomDataTransformer.GENRE_MAPPINGS.get(genre, genre.lower())
                categories.append(mapped_genre)
            
        # Remove duplicates and limit to 250
        categories = list(set(categories))[:250]
        
        if not categories:
            categories = ["entertainment"]
            
        logger.debug(f"Transformed genres {genres} -> {categories}")
        return categories
    
    @staticmethod
    def _transform_release_date(release_date: str) -> str:
        """Transform release date to RFC 3339 format."""
        if not release_date or release_date == "NULL":
            # Use current timestamp as fallback
            return datetime.now().replace(microsecond=0).isoformat() + "Z"
            
        try:
            # Clean up the input format first
            cleaned_date = release_date.strip()
            
            # Handle .000Z format by removing microseconds
            if ".000Z" in cleaned_date:
                cleaned_date = cleaned_date.replace(".000Z", "Z")
            elif ".000+00:00" in cleaned_date:
                cleaned_date = cleaned_date.replace(".000+00:00", "+00:00")
            
            # Parse the date and ensure RFC 3339 format
            parsed_date = parse_datetime(cleaned_date)
            
            # Remove microseconds for cleaner format
            parsed_date = parsed_date.replace(microsecond=0)
            
            # Handle timezone formatting properly
            if parsed_date.tzinfo is None:
                # No timezone, assume UTC
                return parsed_date.isoformat() + "Z"
            else:
                # Convert to UTC and use Z format for consistency
                utc_date = parsed_date.utctimetuple()
                return datetime(*utc_date[:6]).isoformat() + "Z"
                
        except Exception as e:
            logger.warning(f"Invalid release date '{release_date}': {e}")
            return datetime.now().replace(microsecond=0).isoformat() + "Z"
    
    @staticmethod
    def _transform_episode_count_to_duration(episode_count: int) -> str:
        """Transform episode count to Google duration format."""
        try:
            episodes = int(episode_count) if episode_count else 1
        except (ValueError, TypeError):
            episodes = 1
            
        # Assume each episode is ~5 minutes for micro dramas
        total_minutes = episodes * 5
        
        if total_minutes < 60:
            return f"{total_minutes}m"
        else:
            hours = total_minutes // 60
            minutes = total_minutes % 60
            if minutes:
                return f"{hours}h{minutes}m"
            return f"{hours}h"
    
    @staticmethod
    def _normalize_language_code(lang_code: str) -> Optional[str]:
        """Normalize language code to BCP 47 format."""
        if not lang_code or lang_code == "NULL":
            return None
            
        # Handle comma-separated multiple languages
        if "," in lang_code:
            lang_code = lang_code.split(",")[0].strip()
            
        return CustomDataTransformer.LANGUAGE_MAPPINGS.get(lang_code, lang_code)
    
    @staticmethod
    def _transform_persons(actors: List[str], directors: List[str]) -> List[Dict[str, str]]:
        """Transform actors and directors to person objects."""
        persons = []
        
        # Add actors
        if actors:
            for actor in actors:
                if actor and actor != "NULL":
                    # Handle format like "Actor Name:Character"
                    if ":" in actor:
                        name = actor.split(":")[0].strip()
                    else:
                        name = actor.strip()
                        
                    if name:
                        persons.append({
                            "name": name,
                            "role": "actor"
                        })
        
        # Add directors
        if directors:
            for director in directors:
                if director and director != "NULL":
                    persons.append({
                        "name": director.strip(),
                        "role": "director"
                    })
        
        return persons
    
    @staticmethod
    def _extract_extended_metadata(extended: Dict[str, Any]) -> Dict[str, Any]:
        """Extract useful fields from extended metadata."""
        extracted = {}
        
        if extended.get("content_category"):
            extracted["content_category"] = extended["content_category"]
            
        if extended.get("digital_keywords"):
            extracted["digital_keywords"] = extended["digital_keywords"]
            
        if extended.get("content_version"):
            extracted["content_version"] = extended["content_version"]
            
        if extended.get("content_descriptors"):
            extracted["content_descriptors"] = extended["content_descriptors"]
            
        # Combine all person-related fields
        person_fields = [
            "producers", "executive_producers", "music_directors", 
            "lyricists", "narrators", "singers", "storywriters"
        ]
        
        additional_persons = []
        for field in person_fields:
            if extended.get(field):
                for person in extended[field]:
                    if person and person != "NULL":
                        additional_persons.append({
                            "name": person,
                            "role": field.rstrip('s')  # Remove 's' from plural
                        })
        
        if additional_persons:
            extracted["additional_crew"] = additional_persons
            
        return extracted
    
    @staticmethod
    def transform_batch(data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform a batch of customer records."""
        logger.info(f"Transforming batch of {len(data_list)} records")
        
        transformed_records = []
        errors = []
        
        for i, record in enumerate(data_list):
            try:
                transformed = CustomDataTransformer.transform_customer_record(record)
                
                # Validate against Google schema
                validation_result = GoogleMediaValidator.validate_required_fields(transformed)
                
                if validation_result["valid"]:
                    transformed_records.append(transformed)
                else:
                    logger.error(f"Record {i} failed Google validation: {validation_result['errors']}")
                    errors.append({
                        "record_index": i,
                        "record_id": record.get("id"),
                        "errors": validation_result["errors"]
                    })
                    
            except Exception as e:
                logger.error(f"Failed to transform record {i}: {e}")
                errors.append({
                    "record_index": i, 
                    "record_id": record.get("id"),
                    "errors": [str(e)]
                })
        
        logger.info(f"Successfully transformed {len(transformed_records)} records, {len(errors)} errors")
        
        if errors:
            logger.warning(f"Transformation errors: {errors}")
            
        return transformed_records