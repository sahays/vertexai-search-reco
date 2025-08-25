"""Dataset management functionality for Vertex AI Search."""

import json
from pathlib import Path
from typing import Dict, Any, List
import jsonschema

from ..config import ConfigManager
from ..interfaces import DatasetManagerInterface
from ..utils import setup_logging

logger = setup_logging()


class DatasetManager(DatasetManagerInterface):
    """Manages datasets with flexible schema support."""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
    
    def create_dataset(self, data: List[Dict[str, Any]], schema: Dict[str, Any]) -> bool:
        """Create a new dataset with the given data and schema."""
        try:
            # Validate data against schema
            errors = self.validate_data(data, schema)
            if errors:
                logger.error(f"Data validation failed: {errors}")
                return False
            
            # Save validated dataset
            output_path = self.config_manager.config.output_directory / "validated_dataset.json"
            return self.save_data_to_file(data, output_path)
            
        except Exception as e:
            logger.error(f"Failed to create dataset: {str(e)}")
            return False
    
    def validate_data(self, data: List[Dict[str, Any]], schema: Dict[str, Any]) -> List[str]:
        """Validate data against schema. Returns list of validation errors."""
        errors = []
        
        try:
            # Validate each record against the schema
            for i, record in enumerate(data):
                try:
                    jsonschema.validate(record, schema)
                except jsonschema.ValidationError as e:
                    errors.append(f"Record {i}: {e.message}")
                except jsonschema.SchemaError as e:
                    errors.append(f"Schema error: {e.message}")
                    break  # Don't continue if schema is invalid
                    
        except Exception as e:
            errors.append(f"Validation failed: {str(e)}")
        
        return errors
    
    def load_data_from_file(self, file_path: Path) -> List[Dict[str, Any]]:
        """Load data from JSON file."""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Ensure data is a list
            if isinstance(data, dict):
                data = [data]
            elif not isinstance(data, list):
                raise ValueError("Data must be a JSON object or array")
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to load data from {file_path}: {str(e)}")
            raise
    
    def save_data_to_file(self, data: List[Dict[str, Any]], file_path: Path) -> bool:
        """Save data to JSON file."""
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            
            logger.info(f"Saved {len(data)} records to {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save data to {file_path}: {str(e)}")
            return False