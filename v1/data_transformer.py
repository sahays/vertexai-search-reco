#!/usr/bin/env python3
"""Data transformation module for cleaning and preparing data before BigQuery import"""

import json
import click
import os
from typing import Any, Dict, List, Union


class DataTransformer:
    """Transform and clean data for Vertex AI Search compatibility"""

    def __init__(self):
        pass

    def clean_array_field(self, value: Any) -> List[str]:
        """Clean and normalize array fields, handling empty strings and nulls"""
        if value is None:
            return []

        if isinstance(value, list):
            # Filter out empty strings, None values, and "NULL" strings
            return [str(item).strip() for item in value if item is not None and str(item).strip() and str(item).strip() != "NULL"]

        if isinstance(value, str):
            # Handle empty strings and "NULL"
            if not value or value.strip() == "" or value.strip() == "NULL":
                return []
            # Split comma-separated values and clean
            items = [item.strip() for item in value.split(',') if item.strip() and item.strip() != "NULL"]
            return items

        # Convert other types to string and treat as single item
        str_value = str(value).strip()
        return [str_value] if str_value and str_value != "NULL" else []

    def clean_string_field(self, value: Any) -> str:
        """Clean and normalize string fields"""
        if value is None:
            return ""

        if isinstance(value, (list, dict)):
            # Convert complex types to JSON string
            str_value = json.dumps(value) if value else ""
        else:
            str_value = str(value).strip()

        # Convert "NULL" strings to empty strings
        if str_value == "NULL":
            return ""

        return str_value

    def clean_nested_field(self, obj: Dict, field_path: str) -> Any:
        """Extract value from nested field path (e.g., 'extended.content_descriptors')"""
        if not obj or not field_path:
            return None

        parts = field_path.split('.')
        current = obj

        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None

        return current

    def clean_all_fields(self, obj: Any) -> Any:
        """Recursively clean all fields in the object, converting NULL strings to empty strings"""
        if isinstance(obj, dict):
            cleaned = {}
            for key, value in obj.items():
                cleaned[key] = self.clean_all_fields(value)
            return cleaned
        elif isinstance(obj, list):
            return [self.clean_all_fields(item) for item in obj if item != "NULL"]
        elif isinstance(obj, str) and obj == "NULL":
            return ""
        else:
            return obj

    def transform_record(self, record: Dict[str, Any], field_mappings: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single record according to field mappings"""
        # First, clean all NULL strings in the entire record
        transformed = self.clean_all_fields(record.copy())

        # Process custom fields defined in field mappings
        custom_fields = field_mappings.get('custom_fields', {})

        for field_key, field_info in custom_fields.items():
            source_field_name = field_info.get("name")
            field_type = field_info.get("type", "string")

            if not source_field_name:
                continue

            # Extract value from source field (handles nested paths)
            raw_value = self.clean_nested_field(transformed, source_field_name)

            # Transform based on declared type
            if field_type == "array":
                transformed_value = self.clean_array_field(raw_value)
            else:  # string or other types
                transformed_value = self.clean_string_field(raw_value)

            # Update the record with transformed value
            # For nested paths, we'll set it at the source location
            if '.' in source_field_name:
                # For nested fields, update in place
                parts = source_field_name.split('.')
                current = transformed
                for part in parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                current[parts[-1]] = transformed_value
            else:
                # For top-level fields
                transformed[source_field_name] = transformed_value

        return transformed

    def transform_json_file(self, input_file: str, output_file: str, field_mappings: Dict[str, Any]) -> bool:
        """Transform JSON file with data cleaning"""
        click.echo(f"🔄 Transforming data from {input_file} to {output_file}")

        try:
            # Read input file
            with open(input_file, 'r') as f:
                data = json.load(f)

            # Ensure data is a list
            if not isinstance(data, list):
                data = [data]

            # Transform each record
            transformed_data = []
            for i, record in enumerate(data):
                try:
                    transformed_record = self.transform_record(record, field_mappings)
                    transformed_data.append(transformed_record)
                except Exception as e:
                    click.echo(f"⚠️  Warning: Error transforming record {i}: {e}")
                    # Include original record with warning
                    transformed_data.append(record)

            # Write transformed data
            with open(output_file, 'w') as f:
                json.dump(transformed_data, f, indent=2, ensure_ascii=False)

            click.echo(f"✅ Transformed {len(transformed_data)} records")
            return True

        except Exception as e:
            click.echo(f"❌ Error transforming data: {e}")
            return False

    def validate_transformed_data(self, file_path: str, field_mappings: Dict[str, Any]) -> bool:
        """Validate transformed data meets requirements"""
        click.echo(f"🔍 Validating transformed data in {file_path}")

        try:
            with open(file_path, 'r') as f:
                data = json.load(f)

            if not isinstance(data, list):
                data = [data]

            validation_errors = []
            custom_fields = field_mappings.get('custom_fields', {})

            for i, record in enumerate(data[:10]):  # Check first 10 records
                for field_key, field_info in custom_fields.items():
                    source_field_name = field_info.get("name")
                    field_type = field_info.get("type", "string")

                    if not source_field_name:
                        continue

                    value = self.clean_nested_field(record, source_field_name)

                    # Validate based on type
                    if field_type == "array":
                        if value is not None and not isinstance(value, list):
                            validation_errors.append(f"Record {i}: {source_field_name} should be array, got {type(value)}")

            if validation_errors:
                click.echo("⚠️  Validation warnings:")
                for error in validation_errors[:5]:  # Show first 5 errors
                    click.echo(f"  - {error}")
                if len(validation_errors) > 5:
                    click.echo(f"  ... and {len(validation_errors) - 5} more")
            else:
                click.echo("✅ Data validation passed")

            return True

        except Exception as e:
            click.echo(f"❌ Error validating data: {e}")
            return False


@click.command()
@click.argument('input_file')
@click.argument('output_file')
@click.option('--custom-fields', required=True, help='JSON string of custom field mappings')
@click.option('--validate', is_flag=True, help='Validate transformed data')
def transform_data(input_file, output_file, custom_fields, validate):
    """Transform data file with proper type handling and cleaning"""

    try:
        field_mappings = {
            'custom_fields': json.loads(custom_fields) if custom_fields else {}
        }
    except json.JSONDecodeError as e:
        click.echo(f"❌ Error parsing custom fields JSON: {e}")
        return

    transformer = DataTransformer()

    # Ensure outputs directory exists and save there
    outputs_dir = 'outputs'
    os.makedirs(outputs_dir, exist_ok=True)

    # If output_file is just a filename, put it in outputs folder
    if not os.path.dirname(output_file):
        output_file = os.path.join(outputs_dir, output_file)
    else:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Transform the data
    if not transformer.transform_json_file(input_file, output_file, field_mappings):
        click.echo("❌ Data transformation failed")
        return

    # Validate if requested
    if validate:
        transformer.validate_transformed_data(output_file, field_mappings)

    click.echo(f"✅ Data transformation completed: {output_file}")


if __name__ == '__main__':
    transform_data()