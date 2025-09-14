"""BigQuery management for Media Data Store."""

from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime
import json

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from .config import ConfigManager
from .auth import get_credentials
from .utils import setup_logging, MediaDataStoreError, save_output, handle_vertex_ai_error


class MediaBigQueryManager:
    """Manages BigQuery operations for media data store."""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.logger = setup_logging()
        
        # Initialize BigQuery client
        credentials = get_credentials()
        self.client = bigquery.Client(
            project=config_manager.vertex_ai.project_id,
            credentials=credentials
        )
    
    @handle_vertex_ai_error
    def upload_data(self, data: List[Dict[str, Any]], dataset_id: str, table_id: str,
                   output_dir: Optional[Path] = None, subcommand: str = "upload") -> Dict[str, Any]:
        """Upload media data to BigQuery table."""
        self.logger.info(f"Starting upload to {dataset_id}.{table_id}")
        self.logger.debug(f"Upload parameters - Dataset: {dataset_id}, Table: {table_id}, Records: {len(data)}")
        self.logger.debug(f"Output directory: {output_dir}")
        
        if not data:
            raise MediaDataStoreError("No data provided for upload")
        
        # Ensure dataset exists
        dataset_ref = self.client.dataset(dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
            self.logger.info(f"Using existing dataset: {dataset_id}")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Default location for media data
            self.client.create_dataset(dataset)
            self.logger.info(f"Created dataset: {dataset_id}")
        
        # Create table reference
        table_ref = dataset_ref.table(table_id)
        
        # Configure load job
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        
        # Disable autodetect and generate schema dynamically to keep datetime fields as strings
        # This ensures Vertex AI receives RFC3339 strings, not converted timestamps
        job_config.autodetect = False
        job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
        
        # Note: schema_update_options not compatible with WRITE_TRUNCATE on non-partitioned tables
        
        # Analyze data for type inconsistencies and normalize fields that should be arrays.
        # This prevents errors when a field is sometimes a list and sometimes a single value.
        data = self._analyze_and_normalize_data(data)

        # Generate schema dynamically from the now-consistent data structure
        schema = self._generate_schema_from_data(data)
        job_config.schema = schema
        
        # Start load job
        job = self.client.load_table_from_json(
            data, table_ref, job_config=job_config
        )
        
        # Wait for job completion
        job.result()
        
        # Get job statistics
        stats = {
            "job_id": job.job_id,
            "rows_loaded": len(data),
            "table_id": f"{dataset_id}.{table_id}",
            "state": job.state,
            "input_bytes": getattr(job, 'input_bytes', None),
            "output_bytes": getattr(job, 'output_bytes', None),
            "creation_time": job.created.isoformat() if job.created else None,
            "start_time": job.started.isoformat() if job.started else None,
            "end_time": job.ended.isoformat() if job.ended else None,
            "completion_time": datetime.now().isoformat()
        }
        
        self.logger.info(f"Successfully uploaded {stats['rows_loaded']} rows to BigQuery")
        
        if output_dir:
            output_file = save_output(stats, output_dir, "bigquery_upload_stats.json", subcommand)
            self.logger.info(f"Upload statistics saved to: {output_file}")
        
        return stats
    
    @handle_vertex_ai_error
    def get_table_info(self, dataset_id: str, table_id: str) -> Dict[str, Any]:
        """Get information about a BigQuery table."""
        table_ref = self.client.dataset(dataset_id).table(table_id)
        
        try:
            table = self.client.get_table(table_ref)
            
            return {
                "table_id": f"{dataset_id}.{table_id}",
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "created": table.created.isoformat() if table.created else None,
                "modified": table.modified.isoformat() if table.modified else None,
                "schema_fields": len(table.schema),
                "schema": [
                    {
                        "name": field.name,
                        "field_type": field.field_type,
                        "mode": field.mode
                    }
                    for field in table.schema
                ]
            }
        except NotFound:
            raise MediaDataStoreError(f"Table not found: {dataset_id}.{table_id}")
    
    @handle_vertex_ai_error 
    def validate_table_schema(self, dataset_id: str, table_id: str, 
                            expected_fields: List[str]) -> Dict[str, Any]:
        """Validate that table contains expected fields."""
        self.logger.info(f"Validating schema for {dataset_id}.{table_id}")
        
        table_info = self.get_table_info(dataset_id, table_id)
        existing_fields = [field["name"] for field in table_info["schema"]]
        
        missing_fields = [field for field in expected_fields if field not in existing_fields]
        extra_fields = [field for field in existing_fields if field not in expected_fields]
        
        validation_result = {
            "valid": len(missing_fields) == 0,
            "missing_fields": missing_fields,
            "extra_fields": extra_fields,
            "total_fields": len(existing_fields),
            "expected_fields": len(expected_fields),
            "validation_timestamp": datetime.now().isoformat()
        }
        
        if validation_result["valid"]:
            self.logger.info("Table schema validation passed")
        else:
            self.logger.warning(f"Table schema validation failed. Missing: {missing_fields}")
        
        return validation_result
    
    @handle_vertex_ai_error
    def query_sample_data(self, dataset_id: str, table_id: str, 
                         limit: int = 10) -> List[Dict[str, Any]]:
        """Query sample data from table."""
        query = f"""
            SELECT *
            FROM `{self.config_manager.vertex_ai.project_id}.{dataset_id}.{table_id}`
            LIMIT {limit}
        """
        
        query_job = self.client.query(query)
        results = query_job.result()
        
        sample_data = [dict(row) for row in results]
        
        self.logger.info(f"Retrieved {len(sample_data)} sample rows from {dataset_id}.{table_id}")
        
        return sample_data
    
    def _analyze_and_normalize_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Analyzes data to find inconsistent array types and normalizes them.
        For any field key that ever appears as a list, this ensures it is always a list
        in every record.
        """
        array_field_keys = set()

        # First pass: recursively find all keys that are ever used for lists.
        def find_array_keys(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if isinstance(value, list):
                        array_field_keys.add(key)
                    find_array_keys(value)
            elif isinstance(obj, list):
                for item in obj:
                    find_array_keys(item)

        for record in data:
            find_array_keys(record)
        
        if array_field_keys:
            self.logger.debug(f"Identified keys that should always be arrays: {array_field_keys}")

        # Second pass: recursively normalize the data.
        def normalize_object(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if key in array_field_keys and not isinstance(value, list):
                        # This is the fix: wrap non-list values in a list.
                        obj[key] = [value] if value is not None else []
                    else:
                        normalize_object(value)
            elif isinstance(obj, list):
                for item in obj:
                    normalize_object(item)
            return obj

        return [normalize_object(record) for record in data]
    
    def _generate_schema_from_data(self, data: List[Dict[str, Any]]) -> List[bigquery.SchemaField]:
        """Generate BigQuery schema from data structure, keeping datetime fields as strings."""
        if not data:
            raise MediaDataStoreError("Cannot generate schema from empty data")
        
        # Collect all unique field names from all records
        all_fields = set()
        field_types = {}
        
        for record in data:
            all_fields.update(record.keys())
            
            # Track field types and examples
            for field_name, field_value in record.items():
                if field_name not in field_types:
                    field_types[field_name] = field_value
                elif field_value is not None:
                    # Update with non-None value if we had None before
                    if field_types[field_name] is None:
                        field_types[field_name] = field_value
        
        # Known datetime fields that should remain as STRING for RFC3339 compatibility
        datetime_fields = {
            'available_time', 'release_date', 'c_releasedate', 
            'licensing_from', 'licensing_until', 'from', 'until'
        }
        
        # Required fields for Vertex AI custom schema
        required_fields = {'_id'}
        
        schema_fields = []
        
        for field_name in sorted(all_fields):
            field_value = field_types.get(field_name)
            mode = "REQUIRED" if field_name in required_fields else "NULLABLE"
            
            # Determine field type based on value and known patterns
            if field_name in datetime_fields:
                # Force datetime fields to STRING to preserve RFC3339
                schema_fields.append(bigquery.SchemaField(field_name, "STRING", mode=mode))
            elif field_value is None:
                # Default to string for null values
                schema_fields.append(bigquery.SchemaField(field_name, "STRING", mode=mode))
            elif isinstance(field_value, str):
                schema_fields.append(bigquery.SchemaField(field_name, "STRING", mode=mode))
            elif isinstance(field_value, bool):
                schema_fields.append(bigquery.SchemaField(field_name, "BOOLEAN", mode=mode))
            elif isinstance(field_value, int):
                schema_fields.append(bigquery.SchemaField(field_name, "INTEGER", mode=mode))
            elif isinstance(field_value, float):
                schema_fields.append(bigquery.SchemaField(field_name, "FLOAT", mode=mode))
            elif isinstance(field_value, list):
                if field_value and isinstance(field_value[0], str):
                    # Array of strings
                    schema_fields.append(bigquery.SchemaField(field_name, "STRING", mode="REPEATED"))
                elif field_value and isinstance(field_value[0], dict):
                    # Array of objects - generate nested schema from all examples
                    nested_examples = []
                    for record in data:
                        if field_name in record and record[field_name]:
                            nested_examples.extend(record[field_name])
                    
                    if nested_examples:
                        nested_fields = self._generate_nested_schema_from_examples(nested_examples, datetime_fields)
                        schema_fields.append(bigquery.SchemaField(field_name, "RECORD", mode="REPEATED", fields=nested_fields))
                    else:
                        # Default to string array
                        schema_fields.append(bigquery.SchemaField(field_name, "STRING", mode="REPEATED"))
                else:
                    # Default to string array
                    schema_fields.append(bigquery.SchemaField(field_name, "STRING", mode="REPEATED"))
            elif isinstance(field_value, dict):
                # Nested object - collect all nested examples
                nested_examples = []
                for record in data:
                    if field_name in record and record[field_name]:
                        nested_examples.append(record[field_name])
                
                if nested_examples:
                    nested_fields = self._generate_nested_schema_from_examples(nested_examples, datetime_fields)
                    schema_fields.append(bigquery.SchemaField(field_name, "RECORD", mode=mode, fields=nested_fields))
                else:
                    # Default empty record
                    schema_fields.append(bigquery.SchemaField(field_name, "RECORD", mode=mode, fields=[]))
            else:
                # Default to string for unknown types
                schema_fields.append(bigquery.SchemaField(field_name, "STRING", mode=mode))
        
        self.logger.debug(f"Generated schema with {len(schema_fields)} fields from {len(data)} records")
        return schema_fields
    
    def _generate_nested_schema(self, nested_obj: Dict[str, Any], datetime_fields: set) -> List[bigquery.SchemaField]:
        """Generate schema for nested objects."""
        nested_schema = []
        
        for field_name, field_value in nested_obj.items():
            mode = "NULLABLE"
            
            if field_name in datetime_fields:
                # Force datetime fields to STRING
                nested_schema.append(bigquery.SchemaField(field_name, "STRING", mode=mode))
            elif isinstance(field_value, str):
                nested_schema.append(bigquery.SchemaField(field_name, "STRING", mode=mode))
            elif isinstance(field_value, bool):
                nested_schema.append(bigquery.SchemaField(field_name, "BOOLEAN", mode=mode))
            elif isinstance(field_value, int):
                nested_schema.append(bigquery.SchemaField(field_name, "INTEGER", mode=mode))
            elif isinstance(field_value, float):
                nested_schema.append(bigquery.SchemaField(field_name, "FLOAT", mode=mode))
            elif isinstance(field_value, list):
                if field_value and isinstance(field_value[0], str):
                    nested_schema.append(bigquery.SchemaField(field_name, "STRING", mode="REPEATED"))
                else:
                    nested_schema.append(bigquery.SchemaField(field_name, "STRING", mode="REPEATED"))
            else:
                nested_schema.append(bigquery.SchemaField(field_name, "STRING", mode=mode))
        
        return nested_schema
    
    def _generate_nested_schema_from_examples(self, nested_examples: List[Dict[str, Any]], datetime_fields: set) -> List[bigquery.SchemaField]:
        """Generate schema for nested objects from multiple examples."""
        if not nested_examples:
            return []
        
        # Collect all unique field names from all nested examples
        all_nested_fields = set()
        nested_field_types = {}
        
        for nested_obj in nested_examples:
            if isinstance(nested_obj, dict):
                all_nested_fields.update(nested_obj.keys())
                
                for field_name, field_value in nested_obj.items():
                    if field_name not in nested_field_types:
                        nested_field_types[field_name] = field_value
                    elif field_value is not None:
                        # Update with non-None value if we had None before
                        if nested_field_types[field_name] is None:
                            nested_field_types[field_name] = field_value
        
        nested_schema = []
        
        for field_name in sorted(all_nested_fields):
            field_value = nested_field_types.get(field_name)
            mode = "NULLABLE"
            
            if field_name in datetime_fields:
                # Force datetime fields to STRING
                nested_schema.append(bigquery.SchemaField(field_name, "STRING", mode=mode))
            elif field_value is None:
                nested_schema.append(bigquery.SchemaField(field_name, "STRING", mode=mode))
            elif isinstance(field_value, str):
                nested_schema.append(bigquery.SchemaField(field_name, "STRING", mode=mode))
            elif isinstance(field_value, bool):
                nested_schema.append(bigquery.SchemaField(field_name, "BOOLEAN", mode=mode))
            elif isinstance(field_value, int):
                nested_schema.append(bigquery.SchemaField(field_name, "INTEGER", mode=mode))
            elif isinstance(field_value, float):
                nested_schema.append(bigquery.SchemaField(field_name, "FLOAT", mode=mode))
            elif isinstance(field_value, list):
                if field_value and isinstance(field_value[0], str):
                    nested_schema.append(bigquery.SchemaField(field_name, "STRING", mode="REPEATED"))
                elif field_value and isinstance(field_value[0], dict):
                    # Array of objects - generate nested schema from all examples
                    sub_nested_examples = []
                    for nested_obj in nested_examples:
                        if isinstance(nested_obj, dict) and field_name in nested_obj and nested_obj[field_name]:
                            sub_nested_examples.extend(nested_obj[field_name])
                    
                    if sub_nested_examples:
                        nested_fields = self._generate_nested_schema_from_examples(sub_nested_examples, datetime_fields)
                        nested_schema.append(bigquery.SchemaField(field_name, "RECORD", mode="REPEATED", fields=nested_fields))
                    else:
                        # Default to string array
                        nested_schema.append(bigquery.SchemaField(field_name, "STRING", mode="REPEATED"))
                else:
                    nested_schema.append(bigquery.SchemaField(field_name, "STRING", mode="REPEATED"))
            elif isinstance(field_value, dict):
                # Nested object - collect all nested examples for recursion
                sub_nested_examples = []
                for nested_obj in nested_examples:
                    if isinstance(nested_obj, dict) and field_name in nested_obj and nested_obj[field_name]:
                        sub_nested_examples.append(nested_obj[field_name])
                
                if sub_nested_examples:
                    nested_fields = self._generate_nested_schema_from_examples(sub_nested_examples, datetime_fields)
                    nested_schema.append(bigquery.SchemaField(field_name, "RECORD", mode=mode, fields=nested_fields))
                else:
                    # Default empty record
                    nested_schema.append(bigquery.SchemaField(field_name, "RECORD", mode=mode, fields=[]))
            else:
                nested_schema.append(bigquery.SchemaField(field_name, "STRING", mode=mode))
        
        return nested_schema