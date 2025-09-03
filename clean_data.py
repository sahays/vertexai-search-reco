"""
A schema-aware script to clean a JSON data file.

This script replaces invalid or "empty" values with their correct, type-safe
equivalents based on a provided JSON schema.
- A field defined as 'array' that isn't a list (e.g., "NULL", "") becomes []
- A field defined as 'object' that is "NULL" becomes {}
- Any other field that is "NULL" becomes null
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any

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
    if len(sys.argv) != 4:
        print("Usage: python clean_data.py <schema.json> <input_file.json> <output_file.json>")
        return 1

    schema_path = Path(sys.argv[1])
    input_path = Path(sys.argv[2])
    output_path = Path(sys.argv[3])

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

    print(f"Reading data from {input_path}...")
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"Cleaning {len(data)} records based on schema...")
    cleaned_data = [clean_record(record, field_types) for record in data]

    print(f"Writing cleaned data to {output_path}...")
    # Ensure the output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(cleaned_data, f, indent=2)

    print("Data cleaning complete.")
    print(f"Cleaned file saved to: {output_path}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
