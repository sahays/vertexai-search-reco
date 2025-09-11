"""Manages Vertex AI Schema operations."""

import json
from typing import Dict, Any
from google.cloud import discoveryengine_v1beta
from ...utils import DataStoreError, setup_logging
from ...config import ConfigManager

logger = setup_logging()

class SchemaManager:
    """Handles getting and updating Vertex AI Schemas."""

    def __init__(self, config_manager: ConfigManager, client):
        self.config_manager = config_manager
        self.schema_client = client

    def _parse_schema_properties(self, schema_response) -> Dict[str, Any]:
        """
        Parses the raw schema response from the Google Cloud API by converting
        the entire protobuf message to JSON, which is the robust way to handle it.
        """
        from google.protobuf.json_format import MessageToJson

        if not schema_response:
            return {}
        
        try:
            # Convert the entire protobuf response to a JSON string.
            schema_json = MessageToJson(schema_response._pb)
            # Parse the JSON string into a standard Python dictionary.
            schema_dict = json.loads(schema_json)
            
            # The schema properties are nested under the 'structSchema' key.
            return schema_dict.get("structSchema", {}).get("properties", {})
        except Exception as e:
            logger.error(f"Failed to decode schema JSON: {e}")
            return {}

    def get_schema(self, data_store_id: str) -> Dict[str, Any]:
        """Get the current schema for a data store."""
        try:
            schema_name = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{data_store_id}/schemas/default_schema"
            request = discoveryengine_v1beta.GetSchemaRequest(name=schema_name)
            schema_response = self.schema_client.get_schema(request=request)
            
            properties = self._parse_schema_properties(schema_response)
            return {"properties": properties}
            
        except Exception as e:
            logger.error(f"Failed to get schema: {str(e)}")
            raise DataStoreError(f"Failed to get schema: {str(e)}") from e

    def apply_field_settings_from_config(self, data_store_id: str) -> bool:
        """Apply field settings from config to the data store schema."""
        try:
            schema_config = self.config_manager.schema
            field_settings = {
                'retrievable': set(schema_config.retrievable_fields),
                'searchable': set(schema_config.searchable_fields),
                'facetable': set(schema_config.facetable_fields),
                'completable': set(schema_config.completable_fields),
                'filterable': set(schema_config.filterable_fields)
            }

            if not any(field_settings.values()):
                logger.info("No field settings configured in config - skipping schema update")
                return True

            logger.info("Applying field settings from config to schema...")

            # 1. Fetch the current schema from the data store.
            try:
                schema_dict = self.get_schema(data_store_id)
                logger.info("Successfully fetched existing schema from data store.")
            except DataStoreError:
                logger.warning("Could not fetch existing schema. Using local schema file as a base.")
                schema_dict = self.config_manager.validate_schema_file()

            original_properties = schema_dict.get("properties", {})
            if not original_properties:
                logger.error("The schema for this data store is currently empty. Please import data first.")
                return False

            # Create a deep copy for modification.
            new_properties = json.loads(json.dumps(original_properties))

            # 2. Define a helper to apply settings to a single schema field spec.
            def _apply_annotations_to_spec(spec, full_field_name):
                field_type = spec.get("type", "")
                is_string_field = field_type == "string"
                is_title_field = spec.get("keyPropertyMapping") == "title"

                # --- Annotation Logic ---
                if full_field_name in field_settings['retrievable'] and field_type in ["string", "number", "boolean", "integer"]:
                    spec['retrievable'] = True
                else:
                    spec.pop('retrievable', None)

                if is_title_field:
                    spec.pop('searchable', None)
                    spec.pop('indexable', None)
                else:
                    if full_field_name in field_settings['searchable'] and is_string_field:
                        spec['searchable'] = True
                    else:
                        spec.pop('searchable', None)
                    if full_field_name in field_settings['filterable'] and field_type in ["string", "number", "boolean", "integer"]:
                        spec['indexable'] = True
                    else:
                        spec.pop('indexable', None)

                if full_field_name in field_settings['facetable'] and is_string_field:
                    spec['dynamicFacetable'] = True
                else:
                    spec.pop('dynamicFacetable', None)

                if full_field_name in field_settings['completable'] and is_string_field:
                    spec['completable'] = True
                else:
                    spec.pop('completable', None)

            # 3. Recursively traverse the schema and apply annotations.
            def recursive_traverse(properties, parent_key=""):
                for field_name, field_spec in properties.items():
                    full_field_name = f"{parent_key}.{field_name}" if parent_key else field_name
                    
                    # Apply annotations to the field itself (for non-array types)
                    _apply_annotations_to_spec(field_spec, full_field_name)
                    
                    # Recurse into nested objects and array items
                    field_type = field_spec.get("type", "")
                    if field_type == "object" and "properties" in field_spec:
                        recursive_traverse(field_spec["properties"], full_field_name)
                    elif field_type == "array" and "items" in field_spec:
                        # For arrays, the settings of the parent apply to the items.
                        _apply_annotations_to_spec(field_spec["items"], full_field_name)

            recursive_traverse(new_properties)

            # 4. Robustly compare the original schema with the newly constructed one.
            original_json = json.dumps(original_properties, sort_keys=True)
            new_json = json.dumps(new_properties, sort_keys=True)

            if original_json == new_json:
                logger.info("No schema changes to apply.")
                return True
            
            changes_made = [k for k in new_properties if new_properties.get(k) != original_properties.get(k)]

            # 5. Create the final, clean schema for the API call.
            final_properties = {}
            for field_name, field_spec in new_properties.items():
                clean_spec = field_spec.copy()
                field_type = clean_spec.get("type")
                if isinstance(field_type, list):
                    clean_spec["type"] = next((t for t in field_type if t != "null"), "string")
                final_properties[field_name] = clean_spec

            # --- API Update Call ---
            schema_name = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{data_store_id}/schemas/default_schema"
            vertex_ai_schema = {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "properties": final_properties}
            
            logger.info("Vertex AI schema being sent:")
            logger.info(json.dumps(vertex_ai_schema, indent=2))
            
            schema = discoveryengine_v1beta.Schema(name=schema_name, struct_schema=vertex_ai_schema)
            request = discoveryengine_v1beta.UpdateSchemaRequest(schema=schema)
            
            operation = self.schema_client.update_schema(request=request)
            logger.info(f"Schema update started: {operation.operation.name}")
            logger.info(f"Applied field settings to {len(changes_made)} fields: {changes_made}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to apply field settings from config: {str(e)}")
            if hasattr(e, 'details') and callable(getattr(e, 'details', None)):
                logger.error(f"Error details: {e.details()}")
            return False
