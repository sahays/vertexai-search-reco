"""Data store management for Vertex AI Search for Media."""

import json
from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime

from google.cloud import discoveryengine_v1beta
from google.cloud.discoveryengine_v1beta.types import (
    DataStore,
    Schema,
    ImportDocumentsRequest,
    BigQuerySource,
    GetSchemaRequest,
    UpdateSchemaRequest,
)
from google.protobuf.json_format import MessageToDict

from .config import ConfigManager
from .auth import get_credentials
from .utils import (
    setup_logging,
    MediaDataStoreError,
    save_output,
    handle_vertex_ai_error,
)


class MediaDataStoreManager:
    """Manages Vertex AI Search for Media data stores."""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.logger = setup_logging()

        # Get credentials
        credentials = get_credentials()

        # Initialize clients
        self.data_store_client = discoveryengine_v1beta.DataStoreServiceClient(
            credentials=credentials
        )
        self.document_client = discoveryengine_v1beta.DocumentServiceClient(
            credentials=credentials
        )
        self.schema_client = discoveryengine_v1beta.SchemaServiceClient(
            credentials=credentials
        )

    @handle_vertex_ai_error
    def create_data_store(
        self,
        data_store_id: str,
        display_name: str,
        content_config: str = "NO_CONTENT",
        output_dir: Optional[Path] = None,
        subcommand: str = "create",
    ) -> Dict[str, Any]:
        """Create a media-specific data store."""
        self.logger.info(f"Creating media data store: {data_store_id}")
        parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection"

        content_config_enum = getattr(
            discoveryengine_v1beta.DataStore.ContentConfig, content_config
        )

        data_store = discoveryengine_v1beta.DataStore(
            display_name=display_name,
            industry_vertical=discoveryengine_v1beta.IndustryVertical.MEDIA,
            solution_types=[discoveryengine_v1beta.SolutionType.SOLUTION_TYPE_SEARCH],
            content_config=content_config_enum,
        )

        operation = self.data_store_client.create_data_store(
            parent=parent, data_store=data_store, data_store_id=data_store_id
        )

        self.logger.info(
            f"Data store creation initiated. Operation: {operation.operation.name}"
        )
        result = operation.result(timeout=300)

        creation_result = {
            "data_store_id": data_store_id,
            "name": result.name,
            "display_name": result.display_name,
            "industry_vertical": "MEDIA",
            "content_config": content_config,
            "creation_time": datetime.now().isoformat(),
            "operation_name": operation.operation.name,
        }

        self.logger.info(f"Media data store created successfully: {result.name}")

        if output_dir:
            output_file = save_output(
                creation_result,
                output_dir,
                f"datastore_{data_store_id}_created.json",
                subcommand,
            )
            self.logger.info(f"Creation details saved to: {output_file}")

        return creation_result

    @handle_vertex_ai_error
    def import_bigquery_data(
        self,
        data_store_id: str,
        dataset_id: str,
        table_id: str,
        output_dir: Optional[Path] = None,
        subcommand: str = "import",
    ) -> Dict[str, Any]:
        """Import data from BigQuery into the media data store."""
        self.logger.info(f"Starting BigQuery import for data store: {data_store_id}")

        parent = self.document_client.branch_path(
            project=self.config_manager.vertex_ai.project_id,
            location=self.config_manager.vertex_ai.location,
            data_store=data_store_id,
            branch="default_branch",
        )

        bigquery_source = discoveryengine_v1beta.BigQuerySource(
            project_id=self.config_manager.vertex_ai.project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            data_schema="custom",
        )

        request = discoveryengine_v1beta.ImportDocumentsRequest(
            parent=parent,
            bigquery_source=bigquery_source,
            reconciliation_mode=discoveryengine_v1beta.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL,
        )

        operation = self.document_client.import_documents(request=request)

        import_result = {
            "data_store_id": data_store_id,
            "source_table": f"{dataset_id}.{table_id}",
            "operation_name": operation.operation.name,
            "import_started": datetime.now().isoformat(),
            "status": "IN_PROGRESS",
        }

        self.logger.info(
            f"BigQuery import initiated. Operation: {operation.operation.name}"
        )

        if output_dir:
            output_file = save_output(
                import_result,
                output_dir,
                f"import_{data_store_id}_started.json",
                subcommand,
            )
            self.logger.info(f"Import details saved to: {output_file}")

        return import_result

    @handle_vertex_ai_error
    def get_import_status(
        self, operation_name: str, output_dir: Optional[Path] = None
    ) -> Dict[str, Any]:
        """Get the status of an import operation."""
        self.logger.info(f"Checking status for operation: {operation_name}")

        operation = self.document_client.get_operation(name=operation_name)
        operation_dict = MessageToDict(operation)

        status_info = {
            "operation_name": operation_name,
            "done": operation.done,
            "status": "COMPLETED" if operation.done else "IN_PROGRESS",
            "checked_at": datetime.now().isoformat(),
        }

        if operation.done:
            if operation.error:
                status_info["error"] = operation_dict.get("error", {})
                status_info["status"] = "FAILED"
                self.logger.error(f"Import operation failed: {operation.error}")
            else:
                status_info["result"] = operation_dict.get("response", {})
                status_info["status"] = "COMPLETED"
                self.logger.info("Import operation completed successfully")
        else:
            if "metadata" in operation_dict:
                status_info["metadata"] = operation_dict["metadata"]
            self.logger.info("Import operation still in progress")

        if output_dir:
            output_file = save_output(
                status_info,
                output_dir,
                f"operation_status_{datetime.now().strftime('%H%M%S')}.json",
            )
            self.logger.info(f"Status information saved to: {output_file}")

        return status_info

    @handle_vertex_ai_error
    def get_data_store_info(self, data_store_id: str) -> Dict[str, Any]:
        """Get information about a data store."""
        name = self.data_store_client.data_store_path(
            project=self.config_manager.vertex_ai.project_id,
            location=self.config_manager.vertex_ai.location,
            data_store=data_store_id,
        )
        data_store = self.data_store_client.get_data_store(name=name)

        return MessageToDict(data_store)

    @handle_vertex_ai_error
    def list_data_stores(self) -> List[Dict[str, Any]]:
        """List all data stores in the project."""
        parent = self.data_store_client.collection_path(
            project=self.config_manager.vertex_ai.project_id,
            location=self.config_manager.vertex_ai.location,
            collection="default_collection",
        )

        return [
            MessageToDict(ds)
            for ds in self.data_store_client.list_data_stores(parent=parent)
        ]

    def _apply_settings_recursively(
        self, properties: Dict[str, Any], config_map: Dict[str, set]
    ):
        """Recursively apply schema settings from the config map."""
        for field_name, field_spec in properties.items():
            # Special handling for 'persons' field, checking against 'actors' and 'directors' from config
            is_person_field = field_name == "persons"

            for setting, config_fields in config_map.items():
                # Apply setting if the field name is directly in the config
                # OR if it's the 'persons' field and either 'actors' or 'directors' are in the config
                apply_setting = field_name in config_fields or (
                    is_person_field
                    and ("actors" in config_fields or "directors" in config_fields)
                )

                if apply_setting:
                    field_spec[setting] = True

            # Recurse into nested properties for object types
            if "properties" in field_spec:
                self._apply_settings_recursively(field_spec["properties"], config_map)

            # Recurse into nested properties for arrays of objects
            if (
                field_spec.get("type") == "array"
                and "items" in field_spec
                and "properties" in field_spec["items"]
            ):
                self._apply_settings_recursively(
                    field_spec["items"]["properties"], config_map
                )

    @handle_vertex_ai_error
    def apply_schema_from_config(
        self,
        data_store_id: str,
        output_dir: Optional[Path] = None,
        subcommand: str = "update-schema",
    ) -> Dict[str, Any]:
        """Update the data store schema based on the application configuration."""
        self.logger.info(f"Applying schema from config to data store: {data_store_id}")
        self.logger.warning(
            "This operation merges settings from the config file with the existing schema."
        )
        schema_config = self.config_manager.schema

        schema_name = self.schema_client.schema_path(
            project=self.config_manager.vertex_ai.project_id,
            location=self.config_manager.vertex_ai.location,
            data_store=data_store_id,
            schema="default_schema",
        )

        try:
            request = GetSchemaRequest(name=schema_name)
            raw_schema_response = self.schema_client.get_schema(request=request)
            self.logger.info(raw_schema_response)
            response_dict = MessageToDict(raw_schema_response._pb)
            self.logger.info(response_dict)
            schema_dict = response_dict.get("structSchema", {})
            self.logger.info("Successfully parsed schema from response.")
        except Exception as e:
            self.logger.error(f"Failed to fetch or parse schema: {e}")
            raise MediaDataStoreError(
                f"Could not retrieve schema for '{data_store_id}'."
            ) from e

        updated_schema_properties = schema_dict.get("properties", {})

        if not updated_schema_properties:
            self.logger.warning(
                "The fetched schema is empty. Building a new one from the config file."
            )

        all_config_fields_map = {
            "retrievable": set(schema_config.retrievable_fields),
            "searchable": set(schema_config.searchable_fields),
            "indexable": set(schema_config.indexable_fields),
            "completable": set(schema_config.completable_fields),
            "dynamicFacetable": set(schema_config.dynamic_facetable_fields),
        }
        all_config_field_names = set().union(*all_config_fields_map.values())

        # Add any fields from config that are not in the current schema
        for field_name in all_config_field_names:
            if field_name not in updated_schema_properties:
                self.logger.info(
                    f"Adding new field '{field_name}' to schema from config."
                )
                updated_schema_properties[field_name] = {
                    "type": "string"
                }  # Default type

        # Apply all settings recursively
        self._apply_settings_recursively(
            updated_schema_properties, all_config_fields_map
        )

        final_struct_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": updated_schema_properties,
        }

        schema_to_update = Schema(name=schema_name, struct_schema=final_struct_schema)

        self.logger.info(
            f"Constructed updated schema with {len(updated_schema_properties)} fields."
        )
        self.logger.info(f"Request Schema: {json.dumps(final_struct_schema, indent=2)}")

        request = UpdateSchemaRequest(schema=schema_to_update)
        operation = self.schema_client.update_schema(request=request)
        self.logger.info(
            f"Schema update operation started. Operation: {operation.operation.name}"
        )

        update_result = {
            "data_store_id": data_store_id,
            "operation_name": operation.operation.name,
            "fields_configured": len(updated_schema_properties),
            "update_started_time": datetime.now().isoformat(),
        }

        if output_dir:
            output_file = save_output(
                update_result,
                output_dir,
                f"update_schema_{data_store_id}_started.json",
                subcommand,
            )
            self.logger.info(f"Schema update details saved to: {output_file}")

        return update_result
