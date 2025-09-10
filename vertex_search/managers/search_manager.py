"""Search management functionality for Vertex AI Search."""

import json
from typing import Dict, Any, List, Optional
from google.cloud import discoveryengine_v1beta
from google.protobuf.json_format import MessageToDict

from ..config import ConfigManager
from ..interfaces import SearchManagerInterface
from ..utils import handle_vertex_ai_error, SearchError, setup_logging
from ..auth import get_credentials, setup_client_options

logger = setup_logging()


class SearchManager(SearchManagerInterface):
    """Manages search functionality using Vertex AI Search."""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager

        # Get credentials and client options
        credentials = get_credentials(config_manager.vertex_ai)
        client_options = setup_client_options(config_manager.vertex_ai)

        # Initialize clients with credentials (only if not using API key)
        client_kwargs = {}
        if credentials is not None:
            client_kwargs["credentials"] = credentials
        if client_options is not None:
            client_kwargs["client_options"] = client_options

        self.engine_client = discoveryengine_v1beta.EngineServiceClient(**client_kwargs)
        self.search_client = discoveryengine_v1beta.SearchServiceClient(**client_kwargs)

    @handle_vertex_ai_error
    def create_search_engine(
        self,
        engine_id: str,
        display_name: str,
        data_store_ids: List[str],
        solution_type: str = "SEARCH",
    ) -> bool:
        """Create a search engine connected to data stores."""
        try:
            parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection"

            # Build data store IDs list - use just the data store IDs, not full names
            # The API expects just the data store IDs, not full resource names
            data_store_ids_list = list(data_store_ids)

            # Get industry vertical from config
            vertical_mapping = {
                "GENERIC": discoveryengine_v1beta.IndustryVertical.GENERIC,
                "MEDIA": discoveryengine_v1beta.IndustryVertical.MEDIA,
                "HEALTHCARE_FHIR": discoveryengine_v1beta.IndustryVertical.HEALTHCARE_FHIR,
            }

            industry_vertical = vertical_mapping.get(
                self.config_manager.vertex_ai.industry_vertical,
                discoveryengine_v1beta.IndustryVertical.GENERIC,
            )

            # Configure engine based on solution type
            if solution_type.upper() == "RECOMMENDATION":
                # For recommendation engines
                engine = discoveryengine_v1beta.Engine(
                    display_name=display_name,
                    solution_type=discoveryengine_v1beta.SolutionType.SOLUTION_TYPE_RECOMMENDATION,
                    industry_vertical=industry_vertical,
                    data_store_ids=data_store_ids_list,
                )
            else:
                # For search engines
                search_config = discoveryengine_v1beta.Engine.SearchEngineConfig()
                engine = discoveryengine_v1beta.Engine(
                    display_name=display_name,
                    solution_type=discoveryengine_v1beta.SolutionType.SOLUTION_TYPE_SEARCH,
                    industry_vertical=industry_vertical,
                    data_store_ids=data_store_ids_list,
                    search_engine_config=search_config,
                )

            operation = self.engine_client.create_engine(
                parent=parent, engine=engine, engine_id=engine_id
            )

            logger.info(f"Creating search engine {engine_id}...")
            result = operation.result(timeout=300)
            logger.info(f"Search engine created: {result.name}")
            return True

        except Exception as e:
            logger.error(f"Failed to create search engine: {str(e)}")
            raise SearchError(f"Failed to create search engine: {str(e)}") from e

    def search(
        self,
        query: str,
        engine_id: str,
        filters: Optional[Dict[str, Any]] = None,
        facets: Optional[List[str]] = None,
        page_size: int = 10,
        page_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Perform a search query."""
        try:
            serving_config = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/engines/{engine_id}/servingConfigs/{self.config_manager.vertex_ai.serving_config_id}"

            search_request = discoveryengine_v1beta.SearchRequest(
                serving_config=serving_config,
                query=query,
                page_size=page_size,
                page_token=page_token or "",
            )

            # Add facet specs if provided
            if facets:
                facet_specs = []
                for facet in facets:
                    facet_spec = discoveryengine_v1beta.SearchRequest.FacetSpec(
                        facet_key=discoveryengine_v1beta.SearchRequest.FacetSpec.FacetKey(
                            key=facet
                        ),
                        limit=20,  # Set facet limit between 1-300
                        excluded_filter_keys=[],
                    )
                    facet_specs.append(facet_spec)
                search_request.facet_specs = facet_specs
                logger.info(f"Added facet specs for fields: {', '.join(facets)}")

            # Add filter if provided
            if filters:
                # Try different filter approaches if the first one fails
                filter_approaches = [
                    lambda f: self._build_filter_string_direct(f),  # Direct field names
                    lambda f: self._build_filter_string(f),  # structData prefix
                ]

                for approach in filter_approaches:
                    try:
                        filter_string = approach(filters)
                        search_request.filter = filter_string
                        logger.info(f"Trying search filter: {filter_string}")

                        # Test the filter by doing a quick search
                        test_request = discoveryengine_v1beta.SearchRequest(
                            serving_config=serving_config,
                            query=query,
                            page_size=1,
                            filter=filter_string,
                        )
                        test_response = self.search_client.search(test_request)

                        # If we get here, the filter worked
                        logger.info(f"Filter syntax validated: {filter_string}")
                        break

                    except Exception as filter_error:
                        logger.warning(f"Filter approach failed: {filter_error}")
                        if approach == filter_approaches[-1]:  # Last approach
                            raise SearchError(
                                f"All filter approaches failed. Last error: {filter_error}"
                            )
                        continue

            response = self.search_client.search(search_request)

            # Convert the entire protobuf response to a dictionary.
            # This is the most robust way to handle all nested complex objects
            # and ensure the result is fully JSON serializable.
            response_dict = MessageToDict(response._pb)

            # Parse JSON string fields back to proper objects/arrays for BigQuery data
            response_dict = self._parse_json_fields(response_dict)

            logger.debug(
                f"Search returned {len(response_dict.get('results', []))} results."
            )

            return response_dict

        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            raise SearchError(f"Search operation failed: {str(e)}") from e

    def get_serving_config(
        self, engine_id: str, serving_config_id: str
    ) -> Dict[str, Any]:
        """Get serving configuration for an engine."""
        try:
            name = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/engines/{engine_id}/servingConfigs/{serving_config_id}"

            config = self.search_client.get_serving_config(name=name)
            return {
                "name": config.name,
                "display_name": config.display_name,
                "solution_type": config.solution_type,
            }

        except Exception as e:
            logger.error(f"Failed to get serving config: {str(e)}")
            raise

    def _build_filter_string(self, filters: Dict[str, Any]) -> str:
        """Build filter string from filter dictionary using Vertex AI Discovery Engine syntax."""
        filter_parts = []

        # Get schema to identify datetime fields
        schema_dict = None
        try:
            schema_dict = self.config_manager.validate_schema_file()
        except Exception:
            logger.warning("Could not load schema for datetime field detection")

        for field, value in filters.items():
            # Try both with and without structData prefix based on import method
            field_paths = []

            if field.startswith("structData."):
                field_paths = [field]
            else:
                # Try both direct field name and structData prefixed
                field_paths = [field, f"structData.{field}"]

            # Use the first field path (we'll try different approaches)
            field_path = field_paths[0]  # Start with direct field name

            # Check if this is a datetime field
            is_datetime_field = False
            if schema_dict and 'properties' in schema_dict:
                field_spec = schema_dict['properties'].get(field, {})
                is_datetime_field = field_spec.get('format') == 'date-time'

            if isinstance(value, str):
                if is_datetime_field:
                    # For datetime fields, use equality comparison instead of ANY()
                    escaped_value = value.replace('"', '\\"')
                    filter_parts.append(f'{field_path} = "{escaped_value}"')
                    logger.info(f"Using datetime equality filter for field '{field}': {field_path} = \"{escaped_value}\"")
                else:
                    # For regular string fields, use ANY()
                    escaped_value = value.replace('"', '\\"')
                    filter_parts.append(f'{field_path}: ANY("{escaped_value}")')
            elif isinstance(value, dict) and is_datetime_field:
                # Handle datetime comparison operators: {">=": "2025-01-01", "<": "2025-12-31"}
                for operator, datetime_value in value.items():
                    if operator in ['<=', '<', '>=', '>', '=']:
                        escaped_value = str(datetime_value).replace('"', '\\"')
                        filter_parts.append(f'{field_path} {operator} "{escaped_value}"')
                        logger.info(f"Using datetime comparison filter for field '{field}': {field_path} {operator} \"{escaped_value}\"")
                    else:
                        logger.warning(f"Unsupported datetime comparison operator '{operator}' for field '{field}'")
            elif isinstance(value, (int, float)):
                filter_parts.append(f"{field_path} = {value}")
            elif isinstance(value, list):
                # Handle list values - each item in ANY() needs to be quoted if string
                if not value:  # Skip empty lists
                    continue
                value_strs = []
                for v in value:
                    if isinstance(v, str):
                        escaped_v = v.replace('"', '\\"')
                        value_strs.append(f'"{escaped_v}"')
                    else:
                        value_strs.append(str(v))
                filter_parts.append(f'{field_path}: ANY({", ".join(value_strs)})')
            elif isinstance(value, bool):
                filter_parts.append(f"{field_path} = {str(value).lower()}")

        result = " AND ".join(filter_parts)
        logger.debug(f"Built filter string: {result}")
        return result

    def _build_filter_string_direct(self, filters: Dict[str, Any]) -> str:
        """Build filter string using direct field names (without structData prefix)."""
        filter_parts = []

        # Get schema to identify datetime fields
        schema_dict = None
        try:
            schema_dict = self.config_manager.validate_schema_file()
        except Exception:
            logger.warning("Could not load schema for datetime field detection")

        for field, value in filters.items():
            # Use field name directly without prefix
            field_path = field

            # Check if this is a datetime field
            is_datetime_field = False
            if schema_dict and 'properties' in schema_dict:
                field_spec = schema_dict['properties'].get(field, {})
                is_datetime_field = field_spec.get('format') == 'date-time'

            if isinstance(value, str):
                if is_datetime_field:
                    # For datetime fields, use equality comparison instead of ANY()
                    escaped_value = value.replace('"', '\\"')
                    filter_parts.append(f'{field_path} = "{escaped_value}"')
                    logger.info(f"Using datetime equality filter for field '{field}': {field_path} = \"{escaped_value}\"")
                else:
                    # For regular string fields, use ANY()
                    escaped_value = value.replace('"', '\\"')
                    filter_parts.append(f'{field_path}: ANY("{escaped_value}")')
            elif isinstance(value, dict) and is_datetime_field:
                # Handle datetime comparison operators: {">=": "2025-01-01", "<": "2025-12-31"}
                for operator, datetime_value in value.items():
                    if operator in ['<=', '<', '>=', '>', '=']:
                        escaped_value = str(datetime_value).replace('"', '\\"')
                        filter_parts.append(f'{field_path} {operator} "{escaped_value}"')
                        logger.info(f"Using datetime comparison filter for field '{field}': {field_path} {operator} \"{escaped_value}\"")
                    else:
                        logger.warning(f"Unsupported datetime comparison operator '{operator}' for field '{field}'")
            elif isinstance(value, (int, float)):
                filter_parts.append(f"{field_path} = {value}")
            elif isinstance(value, list):
                if not value:
                    continue
                value_strs = []
                for v in value:
                    if isinstance(v, str):
                        escaped_v = v.replace('"', '\\"')
                        value_strs.append(f'"{escaped_v}"')
                    else:
                        value_strs.append(str(v))
                filter_parts.append(f'{field_path}: ANY({", ".join(value_strs)})')
            elif isinstance(value, bool):
                filter_parts.append(f"{field_path} = {str(value).lower()}")

        result = " AND ".join(filter_parts)
        logger.debug(f"Built direct filter string: {result}")
        return result
    
    def _parse_json_fields(self, response_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Parse JSON string fields back to proper objects/arrays for BigQuery data."""
        import json
        
        if 'results' not in response_dict:
            return response_dict
        
        # Fields that are commonly stored as JSON strings in BigQuery but should be objects/arrays
        json_fields = [
            'genre', 'category', 'content_language', 'actors', 'tags', 
            'subtitle_lang', 'rights', 'directors', 'country_meta', 'extended'
        ]
        
        for search_result in response_dict['results']:
            if 'document' in search_result and 'structData' in search_result['document']:
                struct_data = search_result['document']['structData']
                
                for field in json_fields:
                    if field in struct_data and isinstance(struct_data[field], str):
                        try:
                            # Parse JSON string back to object/array
                            parsed_value = json.loads(struct_data[field])
                            struct_data[field] = parsed_value
                            logger.info(f"Parsed JSON field '{field}': '{struct_data[field]}' -> {parsed_value}")
                        except (json.JSONDecodeError, TypeError) as e:
                            # Keep original value if parsing fails
                            logger.debug(f"Could not parse field '{field}' as JSON: {e}")
                            pass
        
        return response_dict
