"""
This module contains the core logic for flattening and cleaning data records and schemas.
"""
import json
import logging
from typing import Dict, Any, List, Tuple

logger = logging.getLogger(__name__)

class DataProcessor:
    """
    A class to handle the flattening and cleaning of JSON data and schemas.
    """
    def __init__(self, schema: Dict[str, Any], flat_deep: bool = False, flat_array: bool = False, array_delimiter: str = ' '):
        self.schema = schema
        self.flat_deep = flat_deep
        self.flat_array = flat_array
        self.array_delimiter = array_delimiter
        self.schema_types = self._load_schema_types()

    def _load_schema_types(self) -> Dict[str, str]:
        """
        Parses a JSON schema and returns a flat dictionary mapping
        field names to their expected data type.
        """
        field_types = {}
        properties = self.schema.get("properties", {})
        for field_name, spec in properties.items():
            if "type" in spec:
                field_types[field_name] = spec["type"]
        return field_types

    def process_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Processes a single data record by cleaning and flattening it."""
        logger.info(f"--- Starting processing for record ---")
        cleaned_record = self._clean_record(record)
        flattened_record = self._flatten_record(cleaned_record)
        logger.info(f"--- Finished processing record ---")
        return flattened_record

    def generate_flattened_schema(self) -> Dict[str, Any]:
        """Generates a new, flattened JSON schema based on the original."""
        logger.info("--- Generating flattened schema ---")
        original_properties = self.schema.get("properties", {})
        flattened_properties = self._flatten_schema_properties(original_properties)
        
        new_schema = self.schema.copy()
        new_schema["properties"] = flattened_properties
        logger.info("--- Finished generating flattened schema ---")
        return new_schema

    def _flatten_record(self, record: Dict[str, Any], parent_key="", sep="_") -> Dict[str, Any]:
        """Recursively flattens a data record."""
        items = []
        for key, value in record.items():
            new_key = parent_key + sep + key if parent_key else key
            if self.flat_deep and isinstance(value, dict):
                logger.info(f"Flattening object: '{key}' -> '{new_key}_*'")
                items.extend(self._flatten_record(value, new_key, sep=sep).items())
            elif self.flat_array and isinstance(value, list):
                logger.info(f"Flattening array: '{key}' -> '{new_key}' with delimiter '{self.array_delimiter}'")
                items.append((new_key, self.array_delimiter.join(map(str, value))))
            else:
                items.append((new_key, value))
        return dict(items)

    def _flatten_schema_properties(self, properties: Dict[str, Any], parent_key="", sep="_") -> Dict[str, Any]:
        """Recursively flattens the 'properties' block of a JSON schema."""
        flattened = {}
        for key, spec in properties.items():
            new_key = parent_key + sep + key if parent_key else key
            prop_type = spec.get("type")

            if self.flat_deep and prop_type == "object" and "properties" in spec:
                logger.info(f"Flattening schema object: '{key}' -> '{new_key}_*'")
                flattened.update(self._flatten_schema_properties(spec["properties"], new_key, sep=sep))
            elif self.flat_array and prop_type == "array" and spec.get("items", {}).get("type") == "string":
                logger.info(f"Flattening schema array: '{key}' -> '{new_key}' (type: string)")
                new_spec = spec.copy()
                new_spec["type"] = "string"
                new_spec.pop("items", None)
                flattened[new_key] = new_spec
            else:
                flattened[new_key] = spec
        return flattened

    def _clean_record(self, obj: Any, parent_key="") -> Any:
        """
        Recursively cleans a dictionary, replacing "NULL" strings and fixing types.
        """
        if isinstance(obj, dict):
            # Iterate over a copy of the items to allow modification
            for key, value in list(obj.items()):
                full_key = f"{parent_key}.{key}" if parent_key else key
                expected_type = self.schema_types.get(key)

                # Rule 1: If schema expects an array but the value is not a list, fix it.
                if expected_type == "array" and not isinstance(value, list):
                    logger.info(f"Cleaning field '{full_key}': Replaced non-list value with [].")
                    obj[key] = []
                    continue

                # Rule 2: If the value is the string "NULL", fix it based on type.
                if value == "NULL":
                    if expected_type == "object":
                        logger.info(f"Cleaning field '{full_key}': Replaced 'NULL' string with {{}}.")
                        obj[key] = {}
                    else:
                        logger.info(f"Cleaning field '{full_key}': Replaced 'NULL' string with None.")
                        obj[key] = None
                    continue
                
                # Rule 3: Recurse into nested objects and lists
                if isinstance(value, dict):
                    obj[key] = self._clean_record(value, full_key)
                elif isinstance(value, list):
                    obj[key] = [self._clean_record(item, full_key) for item in value]
            return obj
        
        return obj
