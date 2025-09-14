"""Data store management for Vertex AI Search for Media."""

from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime

from google.cloud import discoveryengine_v1beta
from google.cloud.discoveryengine_v1beta.types import (
    DataStore,
    Schema,
    Document,
    ImportDocumentsRequest,
    BigQuerySource
)
from google.protobuf.json_format import MessageToDict

from .config import ConfigManager
from .auth import get_credentials
from .utils import setup_logging, MediaDataStoreError, save_output, handle_vertex_ai_error


class MediaDataStoreManager:
    """Manages Vertex AI Search for Media data stores."""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.logger = setup_logging()
        
        # Get credentials
        credentials = get_credentials()
        
        # Initialize clients
        self.data_store_client = discoveryengine_v1beta.DataStoreServiceClient(credentials=credentials)
        self.document_client = discoveryengine_v1beta.DocumentServiceClient(credentials=credentials)
        self.schema_client = discoveryengine_v1beta.SchemaServiceClient(credentials=credentials)
    
    @handle_vertex_ai_error
    def create_data_store(self, data_store_id: str, display_name: str,
                         content_config: str = "NO_CONTENT",
                         document_processing_config: Optional[Dict[str, Any]] = None,
                         output_dir: Optional[Path] = None, subcommand: str = "create") -> Dict[str, Any]:
        """Create a media-specific data store following Google's requirements."""
        self.logger.info(f"Creating media data store: {data_store_id}")
        self.logger.debug(f"Create parameters - ID: {data_store_id}, Name: {display_name}, Config: {content_config}")
        self.logger.debug(f"Project: {self.config_manager.vertex_ai.project_id}, Location: {self.config_manager.vertex_ai.location}")
        
        parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection"
        self.logger.debug(f"Parent resource: {parent}")
        
        # Validate that this is for media industry vertical
        if not self._validate_media_requirements():
            raise MediaDataStoreError("Data store creation requirements not met for media industry vertical")
        
        # Configure for media search with Google's requirements
        # For MEDIA industry vertical, use specified content config
        content_config_enum = getattr(discoveryengine_v1beta.DataStore.ContentConfig, content_config)
        
        data_store_config = {
            "display_name": display_name,
            "industry_vertical": discoveryengine_v1beta.IndustryVertical.MEDIA,
            "solution_types": [discoveryengine_v1beta.SolutionType.SOLUTION_TYPE_SEARCH],
            "content_config": content_config_enum
        }
        
        self.logger.debug(f"Using {content_config} config for MEDIA industry vertical")
        
        # Add document processing config if provided
        if document_processing_config:
            self.logger.debug(f"Adding document processing config: {document_processing_config}")
            # This would be used for advanced media processing features
        
        data_store = discoveryengine_v1beta.DataStore(**data_store_config)
        self.logger.debug(f"Created data store object with industry vertical: MEDIA")
        
        operation = self.data_store_client.create_data_store(
            parent=parent,
            data_store=data_store,
            data_store_id=data_store_id
        )
        
        # Get operation details safely
        operation_name = getattr(operation, 'name', None) or str(operation.operation.name if hasattr(operation, 'operation') else 'unknown')
        self.logger.info(f"Data store creation initiated. Operation: {operation_name}")
        
        # Wait for operation completion
        result = operation.result(timeout=300)
        
        creation_result = {
            "data_store_id": data_store_id,
            "name": result.name,
            "display_name": result.display_name,
            "industry_vertical": "MEDIA",
            "content_config": content_config,
            "creation_time": datetime.now().isoformat(),
            "operation_name": operation_name
        }
        
        self.logger.info(f"Media data store created successfully: {result.name}")
        
        if output_dir:
            output_file = save_output(creation_result, output_dir, f"datastore_{data_store_id}_created.json", subcommand)
            self.logger.info(f"Creation details saved to: {output_file}")
        
        return creation_result
    
    @handle_vertex_ai_error
    def import_bigquery_data(self, data_store_id: str, dataset_id: str, table_id: str,
                           output_dir: Optional[Path] = None, subcommand: str = "import") -> Dict[str, Any]:
        """Import data from BigQuery into the media data store."""
        self.logger.info(f"Starting BigQuery import for data store: {data_store_id}")
        
        parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{data_store_id}/branches/default_branch"
        
        # Configure BigQuery source
        # Use "custom" data schema since we have custom field mappings
        bigquery_source = discoveryengine_v1beta.BigQuerySource(
            project_id=self.config_manager.vertex_ai.project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            data_schema="custom"
        )
        
        import_config = discoveryengine_v1beta.ImportDocumentsRequest(
            parent=parent,
            bigquery_source=bigquery_source,
            reconciliation_mode=discoveryengine_v1beta.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL
        )
        
        operation = self.document_client.import_documents(import_config)
        
        # Get operation details safely
        operation_name = getattr(operation, 'name', None) or str(operation.operation.name if hasattr(operation, 'operation') else 'unknown')
        
        import_result = {
            "data_store_id": data_store_id,
            "source_table": f"{dataset_id}.{table_id}",
            "operation_name": operation_name,
            "import_started": datetime.now().isoformat(),
            "status": "IN_PROGRESS"
        }
        
        self.logger.info(f"BigQuery import initiated. Operation: {operation_name}")
        self.logger.info("Use 'status' command to check import progress")
        
        if output_dir:
            output_file = save_output(import_result, output_dir, f"import_{data_store_id}_started.json", subcommand)
            self.logger.info(f"Import details saved to: {output_file}")
        
        return import_result
    
    @handle_vertex_ai_error
    def get_import_status(self, operation_name: str, output_dir: Optional[Path] = None) -> Dict[str, Any]:
        """Get the status of an import operation."""
        self.logger.info(f"Checking status for operation: {operation_name}")
        
        # Create a GetOperationRequest using the google.longrunning operations
        from google.longrunning import operations_pb2
        request = operations_pb2.GetOperationRequest(name=operation_name)
        
        # Use the document client's get_operation method
        operation = self.document_client.get_operation(request=request)
        
        # Convert operation to dict for easier handling
        operation_dict = MessageToDict(operation)
        
        status_info = {
            "operation_name": operation_name,
            "done": operation.done,
            "status": "COMPLETED" if operation.done else "IN_PROGRESS",
            "checked_at": datetime.now().isoformat()
        }
        
        if operation.done:
            # Check if there's actually an error (not just an empty error object)
            has_error = hasattr(operation, 'error') and operation.error and (
                hasattr(operation.error, 'code') or 
                (hasattr(operation.error, 'message') and operation.error.message) or
                (isinstance(operation_dict.get("error"), dict) and operation_dict["error"])
            )
            
            if has_error:
                status_info["error"] = operation_dict.get("error", {})
                status_info["status"] = "FAILED"
                self.logger.error(f"Import operation failed: {operation.error}")
            else:
                status_info["result"] = operation_dict.get("response", {})
                status_info["status"] = "COMPLETED"
                self.logger.info("Import operation completed successfully")
        else:
            # Check metadata for progress
            if "metadata" in operation_dict:
                status_info["metadata"] = operation_dict["metadata"]
            self.logger.info("Import operation still in progress")
        
        if output_dir:
            output_file = save_output(status_info, output_dir, f"operation_status_{datetime.now().strftime('%H%M%S')}.json")
            self.logger.info(f"Status information saved to: {output_file}")
        
        return status_info
    
    @handle_vertex_ai_error
    def get_data_store_info(self, data_store_id: str) -> Dict[str, Any]:
        """Get information about a data store."""
        name = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{data_store_id}"
        
        data_store = self.data_store_client.get_data_store(name=name)
        
        return {
            "name": data_store.name,
            "display_name": data_store.display_name,
            "industry_vertical": str(data_store.industry_vertical),
            "content_config": str(data_store.content_config),
            "create_time": data_store.create_time.isoformat() if data_store.create_time else None,
        }
    
    @handle_vertex_ai_error
    def list_data_stores(self) -> List[Dict[str, Any]]:
        """List all data stores in the project."""
        parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection"
        
        data_stores = []
        for data_store in self.data_store_client.list_data_stores(parent=parent):
            data_stores.append({
                "name": data_store.name,
                "display_name": data_store.display_name,
                "industry_vertical": str(data_store.industry_vertical),
                "content_config": str(data_store.content_config),
            })
        
        return data_stores
    
    def _validate_media_requirements(self) -> bool:
        """Validate requirements for media data store creation."""
        self.logger.debug("Validating media data store requirements")
        
        # Check that location is supported for media
        supported_locations = ["global", "us-central1", "europe-west1"]
        if self.config_manager.vertex_ai.location not in supported_locations:
            self.logger.warning(f"Location '{self.config_manager.vertex_ai.location}' may not support media data stores")
            return False
        
        # Additional validations can be added here
        # - Check if project has required APIs enabled
        # - Validate quota limits
        # - Check permissions
        
        self.logger.debug("Media data store requirements validation passed")
        return True
    
    @handle_vertex_ai_error
    def create_custom_schema(self, data_store_id: str, schema_definition: Dict[str, Any], 
                           output_dir: Optional[Path] = None) -> Dict[str, Any]:
        """Create a custom schema for the media data store."""
        self.logger.info(f"Creating custom schema for data store: {data_store_id}")
        self.logger.debug(f"Schema definition: {schema_definition}")
        
        parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{data_store_id}"
        
        # Build schema using discoveryengine Schema class
        schema_fields = []
        
        # Add required Google fields with custom source mapping
        field_mappings = self.config_manager.schema.field_mappings
        
        # Title field (required)
        title_field = Schema.FieldConfig(
            field_path=field_mappings.title_source_field,
            field_type=Schema.FieldConfig.FieldType.STRING,
            indexable_option=Schema.FieldConfig.IndexableOption.INDEXABLE_ENABLED,
            dynamic_facetable_option=Schema.FieldConfig.DynamicFacetableOption.DYNAMIC_FACETABLE_DISABLED,
            searchable_option=Schema.FieldConfig.SearchableOption.SEARCHABLE_ENABLED,
            retrievable_option=Schema.FieldConfig.RetrievableOption.RETRIEVABLE_ENABLED,
            completable_option=Schema.FieldConfig.CompletableOption.COMPLETABLE_ENABLED
        )
        schema_fields.append(title_field)
        
        # URI field (required) 
        uri_field = Schema.FieldConfig(
            field_path=field_mappings.uri_source_field,
            field_type=Schema.FieldConfig.FieldType.STRING,
            indexable_option=Schema.FieldConfig.IndexableOption.INDEXABLE_DISABLED,
            searchable_option=Schema.FieldConfig.SearchableOption.SEARCHABLE_DISABLED,
            retrievable_option=Schema.FieldConfig.RetrievableOption.RETRIEVABLE_ENABLED,
            completable_option=Schema.FieldConfig.CompletableOption.COMPLETABLE_DISABLED
        )
        schema_fields.append(uri_field)
        
        # Categories field (required)
        categories_field = Schema.FieldConfig(
            field_path=field_mappings.categories_source_field,
            field_type=Schema.FieldConfig.FieldType.STRING,
            indexable_option=Schema.FieldConfig.IndexableOption.INDEXABLE_ENABLED,
            dynamic_facetable_option=Schema.FieldConfig.DynamicFacetableOption.DYNAMIC_FACETABLE_ENABLED,
            searchable_option=Schema.FieldConfig.SearchableOption.SEARCHABLE_ENABLED,
            retrievable_option=Schema.FieldConfig.RetrievableOption.RETRIEVABLE_ENABLED,
            completable_option=Schema.FieldConfig.CompletableOption.COMPLETABLE_DISABLED
        )
        schema_fields.append(categories_field)
        
        # Available time field (required)
        available_time_field = Schema.FieldConfig(
            field_path=field_mappings.available_time_source_field,
            field_type=Schema.FieldConfig.FieldType.DATETIME,
            indexable_option=Schema.FieldConfig.IndexableOption.INDEXABLE_ENABLED,
            searchable_option=Schema.FieldConfig.SearchableOption.SEARCHABLE_DISABLED,
            retrievable_option=Schema.FieldConfig.RetrievableOption.RETRIEVABLE_ENABLED,
            completable_option=Schema.FieldConfig.CompletableOption.COMPLETABLE_DISABLED
        )
        schema_fields.append(available_time_field)
        
        # Duration field (required)
        duration_field = Schema.FieldConfig(
            field_path=field_mappings.duration_source_field,
            field_type=Schema.FieldConfig.FieldType.STRING,
            indexable_option=Schema.FieldConfig.IndexableOption.INDEXABLE_DISABLED,
            searchable_option=Schema.FieldConfig.SearchableOption.SEARCHABLE_DISABLED,
            retrievable_option=Schema.FieldConfig.RetrievableOption.RETRIEVABLE_ENABLED,
            completable_option=Schema.FieldConfig.CompletableOption.COMPLETABLE_DISABLED
        )
        schema_fields.append(duration_field)
        
        # Add optional fields if configured
        if field_mappings.content_source_field:
            content_field = Schema.FieldConfig(
                field_path=field_mappings.content_source_field,
                field_type=Schema.FieldConfig.FieldType.STRING,
                indexable_option=Schema.FieldConfig.IndexableOption.INDEXABLE_ENABLED,
                searchable_option=Schema.FieldConfig.SearchableOption.SEARCHABLE_ENABLED,
                retrievable_option=Schema.FieldConfig.RetrievableOption.RETRIEVABLE_ENABLED,
                completable_option=Schema.FieldConfig.CompletableOption.COMPLETABLE_DISABLED
            )
            schema_fields.append(content_field)
        
        if field_mappings.language_source_field:
            language_field = Schema.FieldConfig(
                field_path=field_mappings.language_source_field,
                field_type=Schema.FieldConfig.FieldType.STRING,
                indexable_option=Schema.FieldConfig.IndexableOption.INDEXABLE_DISABLED,
                dynamic_facetable_option=Schema.FieldConfig.DynamicFacetableOption.DYNAMIC_FACETABLE_ENABLED,
                searchable_option=Schema.FieldConfig.SearchableOption.SEARCHABLE_DISABLED,
                retrievable_option=Schema.FieldConfig.RetrievableOption.RETRIEVABLE_ENABLED
            )
            schema_fields.append(language_field)
        
        if field_mappings.persons_source_field:
            persons_field = Schema.FieldConfig(
                field_path=field_mappings.persons_source_field,
                field_type=Schema.FieldConfig.FieldType.OBJECT,
                indexable_option=Schema.FieldConfig.IndexableOption.INDEXABLE_ENABLED,
                searchable_option=Schema.FieldConfig.SearchableOption.SEARCHABLE_ENABLED,
                retrievable_option=Schema.FieldConfig.RetrievableOption.RETRIEVABLE_ENABLED,
                completable_option=Schema.FieldConfig.CompletableOption.COMPLETABLE_ENABLED
            )
            schema_fields.append(persons_field)
        
        # Create the schema
        schema = Schema(
            struct_schema={"properties": schema_definition},
            field_configs=schema_fields
        )
        
        operation = self.schema_client.create_schema(
            parent=parent,
            schema=schema,
            schema_id="custom_media_schema"
        )
        
        self.logger.info("Custom schema creation initiated")
        result = operation.result(timeout=300)
        
        schema_result = {
            "schema_name": result.name,
            "field_count": len(schema_fields),
            "required_fields": [
                field_mappings.title_source_field,
                field_mappings.uri_source_field,
                field_mappings.categories_source_field,
                field_mappings.available_time_source_field,
                field_mappings.duration_source_field
            ],
            "optional_fields": [
                field for field in [
                    field_mappings.content_source_field,
                    field_mappings.language_source_field, 
                    field_mappings.persons_source_field,
                    field_mappings.organizations_source_field
                ] if field
            ],
            "creation_time": datetime.now().isoformat(),
            "operation_name": operation.name
        }
        
        self.logger.info(f"Custom schema created: {result.name}")
        
        if output_dir:
            output_file = save_output(schema_result, output_dir, f"custom_schema_{data_store_id}.json")
            self.logger.info(f"Schema details saved to: {output_file}")
        
        return schema_result