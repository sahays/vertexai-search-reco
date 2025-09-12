"""Manages Vertex AI Schema operations."""

import json
from typing import Dict, Any
from google.cloud import discoveryengine_v1beta
from ..shared.utils import DataStoreError, setup_logging
from ..shared.config import ConfigManager

logger = setup_logging()

class SchemaManager:
    """Handles getting and updating Vertex AI Schemas."""

    def __init__(self, config_manager: ConfigManager, client):
        self.config_manager = config_manager
        self.schema_client = client

    def _parse_schema_properties(self, schema_response, verbose: bool = False) -> Dict[str, Any]:
        """
        Parses the raw schema response from the Google Cloud API.
        Tries multiple approaches to extract schema properties from protobuf.
        """
        if not schema_response:
            if verbose:
                logger.warning("Schema response is None or empty")
            return {}
        
        try:
            # Method 1: Check for json_schema attribute (most likely to work)
            if hasattr(schema_response, 'json_schema') and schema_response.json_schema:
                json_schema = schema_response.json_schema
                if verbose:
                    logger.info("Method 1: Using json_schema attribute")
                    logger.info(f"json_schema type: {type(json_schema)}")
                
                # If it's already a dict
                if isinstance(json_schema, dict):
                    properties = json_schema.get('properties', {})
                    if properties:
                        if verbose:
                            logger.info(f"Found {len(properties)} properties via json_schema dict")
                        return properties
                
                # If it's a string, parse it
                elif isinstance(json_schema, str):
                    try:
                        schema_dict = json.loads(json_schema)
                        properties = schema_dict.get('properties', {})
                        if properties:
                            if verbose:
                                logger.info(f"Found {len(properties)} properties via json_schema string")
                            return properties
                    except json.JSONDecodeError as e:
                        if verbose:
                            logger.warning(f"Failed to parse json_schema string: {e}")
            
            # Method 2: Direct access to struct_schema (fallback)
            if hasattr(schema_response, 'struct_schema') and schema_response.struct_schema:
                struct_schema = schema_response.struct_schema
                if verbose:
                    logger.info("Method 2: Direct struct_schema access")
                    logger.info(f"struct_schema type: {type(struct_schema)}")
                
                # If struct_schema is a dict-like object
                if hasattr(struct_schema, 'get'):
                    properties = struct_schema.get('properties', {})
                    if properties:
                        if verbose:
                            logger.info(f"Found {len(properties)} properties via direct access")
                        return properties
                
                # If struct_schema has properties attribute
                if hasattr(struct_schema, 'properties'):
                    properties = struct_schema.properties
                    if verbose:
                        logger.info(f"Found properties attribute: {type(properties)}")
                    if properties:
                        # Convert protobuf map to dict if needed
                        if hasattr(properties, 'items'):
                            return dict(properties.items()) if callable(getattr(properties, 'items')) else dict(properties)
                        return properties
            
            # Method 2: MessageToJson conversion
            if verbose:
                logger.info("Method 2: Trying MessageToJson conversion...")
            
            from google.protobuf.json_format import MessageToJson
            schema_json = MessageToJson(schema_response)
            if verbose:
                logger.info("Raw schema JSON from protobuf:")
                logger.info(schema_json)
            
            schema_dict = json.loads(schema_json)
            if verbose:
                logger.info("Parsed schema dict keys:")
                logger.info(list(schema_dict.keys()))
            
            # Try different possible nested structures
            for key_path in [
                ["structSchema", "properties"],
                ["struct_schema", "properties"], 
                ["properties"],
                ["schema", "properties"]
            ]:
                current = schema_dict
                for key in key_path:
                    if isinstance(current, dict) and key in current:
                        current = current[key]
                    else:
                        current = None
                        break
                
                if current and isinstance(current, dict):
                    if verbose:
                        logger.info(f"Found properties via path {' -> '.join(key_path)}: {len(current)} fields")
                    return current
            
            if verbose:
                logger.warning("No properties found in any expected location")
                
            return {}
            
        except Exception as e:
            logger.error(f"Failed to parse schema properties: {e}")
            if verbose:
                import traceback
                logger.error(f"Full traceback: {traceback.format_exc()}")
            return {}

    def get_schema(self, data_store_id: str, verbose: bool = False) -> Dict[str, Any]:
        """Get the current schema for a data store."""
        try:
            schema_name = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{data_store_id}/schemas/default_schema"
            if verbose:
                logger.info(f"Requesting schema from: {schema_name}")
            
            request = discoveryengine_v1beta.GetSchemaRequest(name=schema_name)
            if verbose:
                logger.info(f"Making API request for schema...")
                
            schema_response = self.schema_client.get_schema(request=request)
            
            if verbose:
                logger.info("Raw schema response received:")
                logger.info(f"Response type: {type(schema_response)}")
                logger.info(f"Available attributes: {[attr for attr in dir(schema_response) if not attr.startswith('_')]}")
                
                # Only access attributes that exist
                if hasattr(schema_response, 'name'):
                    logger.info(f"Schema name: {schema_response.name}")
                
                logger.info(f"Has json_schema: {hasattr(schema_response, 'json_schema')}")
                logger.info(f"Has struct_schema: {hasattr(schema_response, 'struct_schema')}")
                
                if hasattr(schema_response, 'json_schema'):
                    json_schema = schema_response.json_schema
                    logger.info(f"json_schema is not None: {json_schema is not None}")
                    logger.info(f"json_schema type: {type(json_schema)}")
                    if json_schema:
                        if isinstance(json_schema, str):
                            logger.info(f"json_schema content (first 500 chars): {json_schema[:500]}...")
                        else:
                            logger.info(f"json_schema value: {json_schema}")
                
                if hasattr(schema_response, 'struct_schema'):
                    struct_schema = schema_response.struct_schema
                    logger.info(f"struct_schema is not None: {struct_schema is not None}")
                    logger.info(f"struct_schema type: {type(struct_schema)}")
                    
                    if struct_schema:
                        logger.info(f"struct_schema attributes: {[attr for attr in dir(struct_schema) if not attr.startswith('_')]}")
                        logger.info(f"struct_schema value: {struct_schema}")
                        
                        if hasattr(struct_schema, 'keys'):
                            logger.info(f"struct_schema keys: {list(struct_schema.keys())}")
                        elif hasattr(struct_schema, '__dict__'):
                            logger.info(f"struct_schema dict: {struct_schema.__dict__}")
            
            properties = self._parse_schema_properties(schema_response, verbose)
            
            if verbose:
                logger.info(f"Parsed properties count: {len(properties)}")
                if properties:
                    logger.info("Sample field names from parsed schema:")
                    for i, field_name in enumerate(list(properties.keys())[:5]):
                        logger.info(f"  {i+1}. {field_name}")
                        if i >= 4:  # Show max 5 fields
                            break
                    logger.info("Full downloaded schema:")
                    logger.info(json.dumps(properties, indent=2))
                else:
                    logger.warning("No properties found in parsed schema")
            
            return {"properties": properties}
            
        except Exception as e:
            logger.error(f"Failed to get schema: {str(e)}")
            if verbose:
                import traceback
                logger.error(f"Full traceback: {traceback.format_exc()}")
            raise DataStoreError(f"Failed to get schema: {str(e)}") from e

    def apply_field_settings_from_config(self, data_store_id: str, verbose: bool = False) -> bool:
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

            if verbose:
                logger.info("Configuration field settings from config:")
                for setting_type, fields in field_settings.items():
                    logger.info(f"  {setting_type}: {list(fields) if fields else 'None'}")

            if not any(field_settings.values()):
                logger.info("No field settings configured in config - skipping schema update")
                return True

            logger.info("Applying field settings from config to schema...")

            # 1. Fetch the current schema from the data store.
            try:
                if verbose:
                    logger.info(f"Attempting to fetch schema for data store: {data_store_id}")
                schema_dict = self.get_schema(data_store_id, verbose)
                logger.info("Successfully fetched existing schema from data store.")
                
                if verbose:
                    logger.info(f"Schema dict keys: {list(schema_dict.keys())}")
                    logger.info(f"Properties key exists: {'properties' in schema_dict}")
                    if 'properties' in schema_dict:
                        logger.info(f"Number of properties: {len(schema_dict['properties'])}")
                
            except DataStoreError as e:
                logger.error(f"DataStoreError fetching schema: {str(e)}")
                if verbose:
                    import traceback
                    logger.error(f"Full traceback: {traceback.format_exc()}")
                logger.error("Failed to fetch schema from data store. Cannot proceed with schema update.")
                return False
            except Exception as e:
                logger.error(f"Unexpected error fetching schema: {str(e)}")
                if verbose:
                    import traceback
                    logger.error(f"Full traceback: {traceback.format_exc()}")
                logger.error("Failed to fetch schema from data store. Cannot proceed with schema update.")
                return False

            original_properties = schema_dict.get("properties", {})
            if verbose:
                logger.info(f"Original properties count after fetch: {len(original_properties)}")
                
            if not original_properties:
                logger.error("The schema for this data store is currently empty. Please import data first.")
                if verbose:
                    logger.error("Debug: Schema dict content:")
                    logger.error(json.dumps(schema_dict, indent=2))
                return False

            # Create a deep copy for modification.
            new_properties = json.loads(json.dumps(original_properties))

            # 2. Define a helper to apply settings to a single schema field spec.
            def _apply_annotations_to_spec(spec, full_field_name):
                field_type = spec.get("type", "")
                is_string_field = field_type == "string"
                is_title_field = spec.get("keyPropertyMapping") == "title"
                
                # Valid types for different parameters based on Google Cloud docs
                indexable_types = ["string", "number", "integer", "boolean", "datetime", "geolocation"]
                retrievable_types = ["string", "number", "integer", "boolean", "datetime", "geolocation"]

                # Only modify boolean parameters, preserve everything else
                
                # --- Retrievable ---
                if full_field_name in field_settings['retrievable'] and field_type in retrievable_types:
                    spec['retrievable'] = True
                elif full_field_name not in field_settings['retrievable'] and field_type in retrievable_types:
                    spec['retrievable'] = False
                # If field type doesn't support retrievable, leave existing value or don't set

                # --- Indexable (skip for title fields as they're indexable by default) ---
                if not is_title_field and field_type in indexable_types:
                    if full_field_name in field_settings['filterable']:
                        spec['indexable'] = True
                    else:
                        spec['indexable'] = False
                # For title fields, preserve existing indexable setting

                # --- Searchable (only string fields, skip title fields) ---
                if not is_title_field and is_string_field:
                    if full_field_name in field_settings['searchable']:
                        spec['searchable'] = True
                    else:
                        spec['searchable'] = False
                # For non-string fields or title fields, preserve existing searchable setting

                # --- Dynamic Facetable ---
                if field_type in ["string", "number", "integer", "boolean"]:
                    if full_field_name in field_settings['facetable']:
                        spec['dynamicFacetable'] = True
                        # Ensure it's also indexable if it's dynamic facetable
                        if field_type in indexable_types and not is_title_field:
                            spec['indexable'] = True
                    else:
                        spec['dynamicFacetable'] = False

                # --- Completable (only string fields) ---
                if is_string_field:
                    if full_field_name in field_settings['completable']:
                        spec['completable'] = True
                    else:
                        spec['completable'] = False

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

            # 5. Use the modified properties directly (preserve all fields)
            final_properties = new_properties

            if verbose:
                logger.info("Schema changes to be applied:")
                logger.info(json.dumps(new_properties, indent=2))
                
                # Debug: Check for any invalid types
                logger.info("Checking for invalid field types:")
                valid_types = ["string", "number", "integer", "boolean", "datetime", "geolocation", "object", "array"]
                for field_name, field_spec in final_properties.items():
                    field_type = field_spec.get("type")
                    if field_type not in valid_types:
                        logger.warning(f"INVALID TYPE DETECTED - Field '{field_name}' has type: '{field_type}' (type: {type(field_type)})")
                    else:
                        logger.info(f"Field '{field_name}' has valid type: '{field_type}'")

            # --- API Update Call ---
            schema_name = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{data_store_id}/schemas/default_schema"
            vertex_ai_schema = {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "properties": final_properties}
            
            if verbose:
                logger.info("Final Vertex AI schema for update request:")
                logger.info(json.dumps(vertex_ai_schema, indent=2))
            else:
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
