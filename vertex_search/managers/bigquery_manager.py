"""BigQuery management functionality for data ingestion."""

import json
import time
from pathlib import Path
from typing import Dict, Any, List
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from ..config import ConfigManager
from ..utils import setup_logging

logger = setup_logging()

class BigQueryManager:
    """Manages data ingestion into Google BigQuery."""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.client = bigquery.Client(project=config_manager.vertex_ai.project_id)

    def _convert_to_jsonl(self, data: List[Dict[str, Any]]) -> str:
        """Converts a list of dictionaries to a JSONL string, ensuring id field is string type and not null."""
        processed_records = []
        id_field = self.config_manager.schema.id_field
        
        # Check if any records have null/empty id values
        records_with_missing_id = 0
        
        for record in data:
            # Create a copy of the record to avoid modifying the original
            processed_record = record.copy()
            
            # Ensure the id field exists, is a string, and is not null/empty
            id_value = processed_record.get(id_field)
            
            if id_value is None or id_value == "" or str(id_value).strip() == "":
                # Generate a unique ID if missing/empty/null
                import uuid
                processed_record[id_field] = str(uuid.uuid4())
                records_with_missing_id += 1
                logger.warning(f"Missing/empty '{id_field}' field in record, generated UUID: {processed_record[id_field]}")
            else:
                # Convert to string and ensure it's not just whitespace
                string_id = str(id_value).strip()
                if not string_id:
                    import uuid
                    processed_record[id_field] = str(uuid.uuid4())
                    records_with_missing_id += 1
                    logger.warning(f"Empty '{id_field}' field in record, generated UUID: {processed_record[id_field]}")
                else:
                    processed_record[id_field] = string_id
            
            # Handle data types for BigQuery/Vertex AI compatibility
            # Keep native types and let BigQuery handle schema detection
            for field_name, field_value in processed_record.items():
                if isinstance(field_value, list):
                    # Keep arrays as native arrays for BigQuery ARRAY detection
                    # Filter out None/empty values from arrays
                    processed_record[field_name] = [item for item in field_value if item is not None and str(item).strip() != ""]
                elif isinstance(field_value, dict):
                    # Keep objects as native objects for BigQuery RECORD detection
                    processed_record[field_name] = field_value
                elif field_value is None:
                    # Keep None as None for NULLABLE fields
                    processed_record[field_name] = None
                elif isinstance(field_value, str) and field_value.strip() == "":
                    # Convert empty strings to None for cleaner data
                    processed_record[field_name] = None
            
            # Wrap the record in Vertex AI Document format for BigQuery import
            # This ensures Vertex AI can properly parse the document structure
            vertex_ai_document = {
                "id": processed_record[id_field],
                "structData": processed_record  # Keep as object structure for protobuf.Struct compatibility
            }
            
            processed_records.append(vertex_ai_document)
        
        if records_with_missing_id > 0:
            logger.info(f"Generated UUIDs for {records_with_missing_id} records with missing/empty ID fields")
        
        logger.info(f"All {len(processed_records)} records have non-null string ID values")
        return "\n".join(json.dumps(record) for record in processed_records)
    
    def create_table_from_json_schema(self, dataset_id: str, table_id: str) -> bool:
        """Create a BigQuery table using Vertex AI Document format with the configured JSON schema."""
        try:
            dataset_ref = self.client.dataset(dataset_id)
            table_ref = dataset_ref.table(table_id)
            
            logger.info(f"Creating BigQuery table with Vertex AI Document format")
            
            # Create schema for Vertex AI Document format
            # This matches the expected format: {id: string, structData: object}
            schema = [
                bigquery.SchemaField("id", "STRING", mode="REQUIRED", description="Document ID (required by Vertex AI)"),
                bigquery.SchemaField("structData", "JSON", mode="NULLABLE", description="Document content as JSON object")
            ]
            
            # Create the table
            table = bigquery.Table(table_ref, schema=schema)
            table = self.client.create_table(table)
            
            logger.info(f"✓ Created table {dataset_id}.{table_id} with Vertex AI Document format")
            logger.info(f"  - id: STRING REQUIRED (Document identifier)")
            logger.info(f"  - structData: JSON NULLABLE (Document content as JSON object)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table with Vertex AI Document format: {str(e)}")
            return False

    def create_table_with_vertex_ai_schema(self, dataset_id: str, table_id: str) -> bool:
        """Create a BigQuery table with schema optimized for Vertex AI compatibility."""
        try:
            dataset_ref = self.client.dataset(dataset_id)
            table_ref = dataset_ref.table(table_id)
            
            # Check if table already exists
            try:
                self.client.get_table(table_ref)
                logger.info(f"Table {dataset_id}.{table_id} already exists")
                return True
            except Exception:
                pass  # Table doesn't exist, continue to create it
            
            # Create schema with id field as STRING REQUIRED
            id_field = self.config_manager.schema.id_field
            schema = [
                bigquery.SchemaField(id_field, "STRING", mode="REQUIRED", description=f"Unique identifier (required by Vertex AI)")
            ]
            
            # Create the table
            table = bigquery.Table(table_ref, schema=schema)
            table = self.client.create_table(table)
            
            logger.info(f"Created table {dataset_id}.{table_id} with Vertex AI compatible schema")
            logger.info(f"  - {id_field}: STRING REQUIRED")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table: {str(e)}")
            return False
    
    def fix_table_schema_for_vertex_ai(self, dataset_id: str, table_id: str) -> bool:
        """Fix table schema to ensure id field is STRING REQUIRED for Vertex AI compatibility."""
        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            table = self.client.get_table(table_ref)
            id_field = self.config_manager.schema.id_field
            
            # Check if id field needs fixing
            id_field_schema = None
            for field in table.schema:
                if field.name == id_field:
                    id_field_schema = field
                    break
            
            if not id_field_schema:
                logger.error(f"ID field '{id_field}' not found in table schema")
                return False
            
            if id_field_schema.field_type == "STRING" and id_field_schema.mode == "REQUIRED":
                logger.info(f"ID field '{id_field}' is already properly configured")
                return True
            
            # Create new schema with id field fixed
            new_schema = []
            for field in table.schema:
                if field.name == id_field:
                    # Force id field to be STRING REQUIRED
                    new_field = bigquery.SchemaField(
                        field.name, 
                        "STRING", 
                        mode="REQUIRED", 
                        description=field.description
                    )
                    logger.info(f"Fixing ID field '{id_field}': {field.field_type} {field.mode} -> STRING REQUIRED")
                else:
                    new_field = field
                new_schema.append(new_field)
            
            # Update table schema
            table.schema = new_schema
            updated_table = self.client.update_table(table, ["schema"])
            
            logger.info(f"Successfully updated table schema for Vertex AI compatibility")
            return True
            
        except Exception as e:
            error_str = str(e)
            if "changed mode from NULLABLE to REQUIRED" in error_str:
                logger.error("Cannot change field from NULLABLE to REQUIRED in existing BigQuery table")
                logger.info("Solution: Reload the table with 'bq load --replace' to create correct schema from start")
            else:
                logger.error(f"Failed to fix table schema: {error_str}")
            return False
    
    def get_table_schema(self, dataset_id: str, table_id: str) -> Dict[str, Any]:
        """Get the schema of a BigQuery table."""
        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            table = self.client.get_table(table_ref)
            
            schema_info = {
                'num_rows': table.num_rows,
                'fields': []
            }
            
            for field in table.schema:
                field_info = {
                    'name': field.name,
                    'field_type': field.field_type,
                    'mode': field.mode,
                    'description': field.description
                }
                schema_info['fields'].append(field_info)
            
            return schema_info
        except Exception as e:
            logger.error(f"Failed to get table schema: {str(e)}")
            return {'error': str(e)}

    def load_table_from_file(
        self, 
        dataset_id: str,
        table_id: str,
        file_path: Path,
        replace: bool = False
    ) -> bool:
        """
        Loads data from a local JSON file into a BigQuery table.

        This method handles the conversion from standard JSON to JSONL,
        creates the dataset if it doesn't exist, and runs a load job.
        """
        try:
            # 1. Create dataset if it doesn't exist
            dataset_ref = self.client.dataset(dataset_id)
            try:
                self.client.get_dataset(dataset_ref)
                logger.info(f"Dataset {dataset_id} already exists.")
            except NotFound:
                logger.info(f"Creating dataset {dataset_id}...")
                self.client.create_dataset(dataset_ref)
                logger.info(f"Dataset {dataset_id} created.")

            # 2. Read and convert data to JSONL
            logger.info(f"Reading data from {file_path}...")
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            jsonl_data = self._convert_to_jsonl(data)
            
            # 3. Configure and run the load job
            table_ref = dataset_ref.table(table_id)
            
            # Configure job - write disposition will be set below based on our approach
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            
            id_field = self.config_manager.schema.id_field
            
            if replace:
                # For replace mode, delete the table first, then create with complete schema
                try:
                    self.client.delete_table(table_ref)
                    logger.info(f"Deleted existing table {dataset_id}.{table_id} for replacement")
                except Exception:
                    pass  # Table doesn't exist
                
                # Create table using JSON schema
                if not self.create_table_from_json_schema(dataset_id, table_id):
                    raise Exception("Failed to create table from JSON schema")
                
                # Load data without schema options since table already exists with correct schema
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                logger.info("Loading data to pre-created table with Vertex AI compatible schema")
            else:
                # When appending, use explicit schema with field addition allowed
                # Create table if it doesn't exist
                self.create_table_with_vertex_ai_schema(dataset_id, table_id)
                
                # Use schema update to add fields as needed
                job_config.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

            logger.info(f"Starting BigQuery load job for table {dataset_id}.{table_id}...")
            
            # The BigQuery client can load from an in-memory file-like object
            from io import StringIO
            job = self.client.load_table_from_file(
                StringIO(jsonl_data), table_ref, job_config=job_config
            )

            job.result()  # Wait for the job to complete

            table = self.client.get_table(table_ref)
            logger.info(
                f"Successfully loaded {table.num_rows} rows into table {dataset_id}.{table_id}."
            )
            
            # Log schema information for debugging
            id_field = self.config_manager.schema.id_field
            id_field_info = None
            for field in table.schema:
                if field.name == id_field:
                    id_field_info = f"{field.name}: {field.field_type} ({field.mode})"
                    break
            
            if id_field_info:
                logger.info(f"Table schema - {id_field_info}")
                if field.field_type == "STRING" and field.mode == "REQUIRED":
                    logger.info("✓ ID field is properly configured for Vertex AI compatibility")
                else:
                    logger.warning(f"ID field '{id_field}' is not STRING REQUIRED as expected by Vertex AI: {id_field_info}")
                    
                    logger.info("Schema will need to be fixed for Vertex AI compatibility")
                    logger.info("You can use 'vertex-search bq fix-schema' command after data is loaded")
            else:
                logger.error(f"ID field '{id_field}' not found in table schema")
            
            return True

        except Exception as e:
            logger.error(f"Failed to load data into BigQuery: {str(e)}")
            return False
