"""
A schema-aware script to clean a JSON data file.

This script replaces invalid or "empty" values with their correct, type-safe
equivalents based on a provided JSON schema.
- A field defined as 'array' that isn't a list (e.g., "NULL", "") becomes []
- A field defined as 'object' that is "NULL" becomes {}
- Any other field that is "NULL" becomes null
- Automatically creates searchable "_text" fields for array fields configured as searchable
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

def load_schema_types(schema: Dict[str, Any]) -> Dict[str, str]:
    """
    Parses a JSON schema and returns a flat dictionary mapping
    field names to their expected data type.
    """
    field_types = {}
    properties = schema.get("properties", {})
    for field_name, spec in properties.items():
        if "type" in spec:
            field_types[field_name] = spec["type"]
    return field_types

def load_config(config_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """
    Load configuration file to get searchable fields list.
    """
    if not config_path or not config_path.exists():
        return None
    
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: Could not load config file {config_path}: {e}")
        return None

def get_searchable_array_fields(config: Optional[Dict[str, Any]], field_types: Dict[str, str]) -> List[str]:
    """
    Get list of array fields that are configured as searchable.
    """
    if not config:
        return []
    
    searchable_fields = config.get("schema", {}).get("searchable_fields", [])
    searchable_arrays = []
    
    for field_name in searchable_fields:
        if field_name in field_types and field_types[field_name] == "array":
            searchable_arrays.append(field_name)
    
    return searchable_arrays

def create_text_field(array_value: Any) -> str:
    """
    Convert an array to a searchable text string.
    """
    if not isinstance(array_value, list):
        return ""
    
    # Filter out None/empty values and convert to strings
    text_parts = []
    for item in array_value:
        if item is not None and str(item).strip():
            text_parts.append(str(item).strip())
    
    return ", ".join(text_parts)

def clean_record(obj: Any, field_types: Dict[str, str]) -> Any:
    """
    Recursively cleans a dictionary or list based on the types
    defined in the schema.
    """
    if not isinstance(obj, dict):
        return obj

    # Iterate over a copy of the items to allow modification
    for key, value in list(obj.items()):
        expected_type = field_types.get(key)

        # Rule 1: If schema expects an array but the value is not a list, fix it.
        if expected_type == "array" and not isinstance(value, list):
            obj[key] = []
            continue

        # Rule 2: If the value is the string "NULL", fix it based on type.
        if value == "NULL":
            if expected_type == "object":
                obj[key] = {}
            else:
                # For any other type (string, integer, etc.), use null.
                # This also covers array fields that were "NULL" and are now []
                # from the rule above, so this is safe.
                obj[key] = None
            continue

        # Rule 3: Recurse into nested objects and lists
        if isinstance(value, dict):
            obj[key] = clean_record(value, field_types)
        elif isinstance(value, list):
            obj[key] = [clean_record(item, field_types) for item in value]
            
    return obj

def main():
    """Main function to run the data cleaning script."""
    if len(sys.argv) not in [4, 5]:
        print("Usage: python clean_data.py <schema.json> <input_file.json> <output_file.json> [config.json]")
        print("  config.json is optional - if provided, will create _text fields for searchable arrays")
        return 1

    schema_path = Path(sys.argv[1])
    input_path = Path(sys.argv[2])
    output_path = Path(sys.argv[3])
    config_path = Path(sys.argv[4]) if len(sys.argv) == 5 else None

    if not schema_path.exists():
        print(f"Error: Schema file not found at {schema_path}")
        return 1
    if not input_path.exists():
        print(f"Error: Input file not found at {input_path}")
        return 1

    print(f"Loading schema from {schema_path}...")
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema = json.load(f)
    
    field_types = load_schema_types(schema)

    # Load config to identify searchable array fields
    config = load_config(config_path) if config_path else None
    searchable_arrays = get_searchable_array_fields(config, field_types)
    
    if searchable_arrays:
        print(f"Will create searchable text fields for arrays: {searchable_arrays}")
    
    # Debug: Print field types for troubleshooting
    print(f"Debug - Schema field types: {field_types}")
    if config:
        searchable_fields = config.get("schema", {}).get("searchable_fields", [])
        print(f"Debug - Config searchable fields: {searchable_fields}")
    else:
        print("Debug - No config provided")

    print(f"Reading data from {input_path}...")
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"Cleaning {len(data)} records based on schema...")
    cleaned_data = []
    
    for record in data:
        # Clean the record first
        cleaned_record = clean_record(record, field_types)
        
        # Add searchable text fields for array fields
        for array_field in searchable_arrays:
            if array_field in cleaned_record:
                text_field_name = f"{array_field}_text"
                text_value = create_text_field(cleaned_record[array_field])
                cleaned_record[text_field_name] = text_value
                
        cleaned_data.append(cleaned_record)

    print(f"Writing cleaned data to {output_path}...")
    # Ensure the output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(cleaned_data, f, indent=2)

    print("Data cleaning complete.")
    if searchable_arrays:
        text_fields_created = [f"{field}_text" for field in searchable_arrays]
        print(f"Created searchable text fields: {text_fields_created}")
    print(f"Cleaned file saved to: {output_path}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
