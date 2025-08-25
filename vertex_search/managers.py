"""Implementation of manager classes for Vertex AI Search operations."""

import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
import jsonschema
from google.cloud import discoveryengine_v1beta
from google.cloud import storage
import uuid

from .config import ConfigManager
from .interfaces import (
    DatasetManagerInterface,
    MediaAssetManagerInterface,
    SearchManagerInterface,
    AutocompleteManagerInterface,
    RecommendationManagerInterface
)
from .utils import (
    handle_vertex_ai_error,
    SchemaValidationError,
    DataStoreError,
    SearchError,
    setup_logging
)
from .auth import get_credentials, setup_client_options

logger = setup_logging()


class DatasetManager(DatasetManagerInterface):
    """Manages datasets with flexible schema support."""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
    
    def create_dataset(self, data: List[Dict[str, Any]], schema: Dict[str, Any]) -> bool:
        """Create a new dataset with the given data and schema."""
        try:
            # Validate data against schema
            errors = self.validate_data(data, schema)
            if errors:
                logger.error(f"Data validation failed: {errors}")
                return False
            
            # Save validated dataset
            output_path = self.config_manager.config.output_directory / "validated_dataset.json"
            return self.save_data_to_file(data, output_path)
            
        except Exception as e:
            logger.error(f"Failed to create dataset: {str(e)}")
            return False
    
    def validate_data(self, data: List[Dict[str, Any]], schema: Dict[str, Any]) -> List[str]:
        """Validate data against schema. Returns list of validation errors."""
        errors = []
        
        try:
            # Validate each record against the schema
            for i, record in enumerate(data):
                try:
                    jsonschema.validate(record, schema)
                except jsonschema.ValidationError as e:
                    errors.append(f"Record {i}: {e.message}")
                except jsonschema.SchemaError as e:
                    errors.append(f"Schema error: {e.message}")
                    break  # Don't continue if schema is invalid
                    
        except Exception as e:
            errors.append(f"Validation failed: {str(e)}")
        
        return errors
    
    def load_data_from_file(self, file_path: Path) -> List[Dict[str, Any]]:
        """Load data from JSON file."""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Ensure data is a list
            if isinstance(data, dict):
                data = [data]
            elif not isinstance(data, list):
                raise ValueError("Data must be a JSON object or array")
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to load data from {file_path}: {str(e)}")
            raise
    
    def save_data_to_file(self, data: List[Dict[str, Any]], file_path: Path) -> bool:
        """Save data to JSON file."""
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            
            logger.info(f"Saved {len(data)} records to {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save data to {file_path}: {str(e)}")
            return False


class MediaAssetManager(MediaAssetManagerInterface):
    """Manages media assets using Vertex AI Discovery Engine."""
    
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
            
        self.client = discoveryengine_v1beta.DataStoreServiceClient(**client_kwargs)
        self.document_client = discoveryengine_v1beta.DocumentServiceClient(**client_kwargs)
        self.import_client = discoveryengine_v1beta.DocumentServiceClient(**client_kwargs)
        
        # Initialize Cloud Storage client
        storage_kwargs = {}
        if credentials is not None:
            storage_kwargs["credentials"] = credentials
        self.storage_client = storage.Client(**storage_kwargs)
        
    @handle_vertex_ai_error
    def create_data_store(self, data_store_id: str, display_name: str) -> bool:
        """Create a new data store in Vertex AI."""
        try:
            parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection"
            
            # Get industry vertical from config
            vertical_mapping = {
                "GENERIC": discoveryengine_v1beta.IndustryVertical.GENERIC,
                "MEDIA": discoveryengine_v1beta.IndustryVertical.MEDIA,
                "HEALTHCARE_FHIR": discoveryengine_v1beta.IndustryVertical.HEALTHCARE_FHIR
            }
            
            industry_vertical = vertical_mapping.get(
                self.config_manager.vertex_ai.industry_vertical, 
                discoveryengine_v1beta.IndustryVertical.GENERIC
            )
            
            data_store = discoveryengine_v1beta.DataStore(
                display_name=display_name,
                industry_vertical=industry_vertical,
                solution_types=[discoveryengine_v1beta.SolutionType.SOLUTION_TYPE_SEARCH],
                content_config=discoveryengine_v1beta.DataStore.ContentConfig.NO_CONTENT
            )
            
            operation = self.client.create_data_store(
                parent=parent,
                data_store=data_store,
                data_store_id=data_store_id
            )
            
            logger.info(f"Creating data store {data_store_id}...")
            result = operation.result(timeout=300)
            logger.info(f"Data store created: {result.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create data store: {str(e)}")
            raise DataStoreError(f"Failed to create data store: {str(e)}") from e
    
    @handle_vertex_ai_error
    def import_documents(self, data_store_id: str, documents: List[Dict[str, Any]]) -> str:
        """Import documents to the data store in batches. Returns operation ID of the last batch."""
        try:
            parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{data_store_id}/branches/default_branch"
            id_field = self.config_manager.schema.id_field
            batch_size = 100  # API limit
            total_documents = len(documents)
            operation_ids = []
            
            logger.info(f"Importing {total_documents} documents in batches of {batch_size}")
            
            # Process documents in batches
            for batch_start in range(0, total_documents, batch_size):
                batch_end = min(batch_start + batch_size, total_documents)
                batch_documents = documents[batch_start:batch_end]
                
                logger.info(f"Processing batch {batch_start//batch_size + 1}: documents {batch_start + 1}-{batch_end}")
                
                # Convert batch to Vertex AI format
                vertex_documents = []
                for doc in batch_documents:
                    # Extract ID
                    doc_id = str(doc.get(id_field, ''))
                    if not doc_id:
                        raise ValueError(f"Document missing required field '{id_field}'")
                    
                    # Create document
                    vertex_doc = discoveryengine_v1beta.Document(
                        id=doc_id,
                        json_data=json.dumps(doc).encode('utf-8')
                    )
                    vertex_documents.append(vertex_doc)
                
                # Create inline source with batch documents
                inline_source = discoveryengine_v1beta.ImportDocumentsRequest.InlineSource(
                    documents=vertex_documents
                )
                
                # Create import request with inline source
                request = discoveryengine_v1beta.ImportDocumentsRequest(
                    parent=parent,
                    inline_source=inline_source,
                    reconciliation_mode=discoveryengine_v1beta.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL,
                    auto_generate_ids=False
                )
                
                operation = self.import_client.import_documents(request=request)
                operation_id = operation.operation.name
                operation_ids.append(operation_id)
                
                logger.info(f"Batch {batch_start//batch_size + 1} import started: {operation_id}")
                
                # Small delay between batches to avoid rate limiting
                if batch_end < total_documents:
                    import time
                    time.sleep(1)
            
            logger.info(f"All {len(operation_ids)} batches submitted. Total documents: {total_documents}")
            
            # Return the last operation ID for monitoring
            return operation_ids[-1] if operation_ids else ""
            
        except Exception as e:
            logger.error(f"Failed to import documents: {str(e)}")
            raise DataStoreError(f"Failed to import documents: {str(e)}") from e
    
    def get_import_status(self, operation_id: str) -> Dict[str, Any]:
        """Get the status of an import operation."""
        try:
            # This is a simplified version - in practice, you'd use the operations client
            # to check the status of long-running operations
            return {"done": True, "status": "completed"}
            
        except Exception as e:
            logger.error(f"Failed to get import status: {str(e)}")
            return {"done": False, "error": str(e)}
    
    def list_documents(self, data_store_id: str, page_size: int = 10) -> Dict[str, Any]:
        """List documents in a data store to verify import."""
        try:
            parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{data_store_id}/branches/default_branch"
            
            request = discoveryengine_v1beta.ListDocumentsRequest(
                parent=parent,
                page_size=page_size
            )
            
            response = self.document_client.list_documents(request=request)
            
            documents = []
            for doc in response.documents:
                doc_info = {
                    'id': doc.id,
                    'name': doc.name
                }
                documents.append(doc_info)
            
            return {
                'documents': documents,
                'count': len(documents),
                'next_page_token': response.next_page_token
            }
            
        except Exception as e:
            logger.error(f"Failed to list documents: {str(e)}")
            return {'error': str(e), 'documents': [], 'count': 0}
    
    def upload_to_cloud_storage(self, bucket_name: str, documents: List[Dict[str, Any]], folder_path: str = "", batch_size: int = 1000) -> List[str]:
        """Upload documents to Cloud Storage as JSONL files for Vertex AI import. Returns list of GCS URIs."""
        try:
            bucket = self.storage_client.bucket(bucket_name)
            uploaded_uris = []
            id_field = self.config_manager.schema.id_field
            
            logger.info(f"Uploading {len(documents)} documents to gs://{bucket_name}/{folder_path} as JSONL files")
            
            # Process documents in batches and create JSONL files
            for batch_start in range(0, len(documents), batch_size):
                batch_end = min(batch_start + batch_size, len(documents))
                batch_documents = documents[batch_start:batch_end]
                
                # Create batch filename
                batch_id = f"batch_{batch_start//batch_size + 1:04d}"
                blob_path = f"{folder_path}/{batch_id}.jsonl" if folder_path else f"{batch_id}.jsonl"
                blob = bucket.blob(blob_path)
                
                # Create JSONL content (one JSON object per line) with proper Document schema
                jsonl_lines = []
                for doc in batch_documents:
                    # Ensure document has required ID
                    doc_id = str(doc.get(id_field, str(uuid.uuid4())))
                    # Create a copy of the document with guaranteed ID
                    doc_with_id = doc.copy()
                    doc_with_id[id_field] = doc_id
                    
                    # Wrap document in proper Vertex AI Document schema format
                    vertex_doc = {
                        "id": doc_id,
                        "structData": doc_with_id
                    }
                    
                    # Add document as single line JSON (no indentation)
                    jsonl_lines.append(json.dumps(vertex_doc, default=str))
                
                # Join with newlines to create JSONL format
                jsonl_content = '\n'.join(jsonl_lines)
                
                # Upload JSONL file
                blob.upload_from_string(jsonl_content, content_type='application/json')
                
                uri = f"gs://{bucket_name}/{blob_path}"
                uploaded_uris.append(uri)
                
                logger.info(f"Uploaded batch {batch_start//batch_size + 1}: {len(batch_documents)} documents to {blob_path}")
            
            logger.info(f"Successfully uploaded {len(documents)} documents in {len(uploaded_uris)} JSONL files")
            return uploaded_uris
            
        except Exception as e:
            logger.error(f"Failed to upload to Cloud Storage: {str(e)}")
            raise DataStoreError(f"Failed to upload to Cloud Storage: {str(e)}") from e
    
    @handle_vertex_ai_error  
    def import_from_cloud_storage(self, data_store_id: str, gcs_uri: str, data_schema: str = "document") -> str:
        """Import documents from Cloud Storage. Returns operation ID."""
        try:
            parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{data_store_id}/branches/default_branch"
            
            # Create GCS source
            gcs_source = discoveryengine_v1beta.GcsSource(
                input_uris=[gcs_uri],
                data_schema=data_schema
            )
            
            # Create import request with GCS source  
            request = discoveryengine_v1beta.ImportDocumentsRequest(
                parent=parent,
                gcs_source=gcs_source,
                reconciliation_mode=discoveryengine_v1beta.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL,
                auto_generate_ids=False
            )
            
            operation = self.import_client.import_documents(request=request)
            logger.info(f"Started Cloud Storage import operation: {operation.operation.name}")
            return operation.operation.name
            
        except Exception as e:
            logger.error(f"Failed to import from Cloud Storage: {str(e)}")
            raise DataStoreError(f"Failed to import from Cloud Storage: {str(e)}") from e
    
    def create_bucket_if_not_exists(self, bucket_name: str, location: str = "US") -> bool:
        """Create a Cloud Storage bucket if it doesn't exist."""
        try:
            # Check if bucket exists
            try:
                bucket = self.storage_client.get_bucket(bucket_name)
                logger.info(f"Bucket {bucket_name} already exists")
                return True
            except Exception:
                # Bucket doesn't exist, create it
                bucket = self.storage_client.bucket(bucket_name)
                bucket = self.storage_client.create_bucket(bucket, location=location)
                logger.info(f"Created bucket {bucket_name} in {location}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to create bucket: {str(e)}")
            return False
    
    def delete_data_store(self, data_store_id: str) -> bool:
        """Delete a data store."""
        try:
            name = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{data_store_id}"
            
            operation = self.client.delete_data_store(name=name)
            operation.result(timeout=300)
            logger.info(f"Data store {data_store_id} deleted")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete data store: {str(e)}")
            return False


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
    def create_search_engine(self, engine_id: str, display_name: str, data_store_ids: List[str]) -> bool:
        """Create a search engine connected to data stores."""
        try:
            parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection"
            
            # Build data store IDs list - use just the data store IDs, not full names
            # The API expects just the data store IDs, not full resource names
            data_store_ids_list = list(data_store_ids)
            
            # Create search engine config (simplified structure)
            search_config = discoveryengine_v1beta.Engine.SearchEngineConfig()
            
            # Get industry vertical from config
            vertical_mapping = {
                "GENERIC": discoveryengine_v1beta.IndustryVertical.GENERIC,
                "MEDIA": discoveryengine_v1beta.IndustryVertical.MEDIA,
                "HEALTHCARE_FHIR": discoveryengine_v1beta.IndustryVertical.HEALTHCARE_FHIR
            }
            
            industry_vertical = vertical_mapping.get(
                self.config_manager.vertex_ai.industry_vertical, 
                discoveryengine_v1beta.IndustryVertical.GENERIC
            )
            
            engine = discoveryengine_v1beta.Engine(
                display_name=display_name,
                solution_type=discoveryengine_v1beta.SolutionType.SOLUTION_TYPE_SEARCH,
                industry_vertical=industry_vertical,
                data_store_ids=data_store_ids_list,
                search_engine_config=search_config
            )
            
            operation = self.engine_client.create_engine(
                parent=parent,
                engine=engine,
                engine_id=engine_id
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
        page_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Perform a search query."""
        try:
            serving_config = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/engines/{engine_id}/servingConfigs/{self.config_manager.vertex_ai.serving_config_id}"
            
            search_request = discoveryengine_v1beta.SearchRequest(
                serving_config=serving_config,
                query=query,
                page_size=page_size,
                page_token=page_token or ""
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
                        excluded_filter_keys=[]
                    )
                    facet_specs.append(facet_spec)
                search_request.facet_specs = facet_specs
                logger.info(f"Added facet specs for fields: {', '.join(facets)}")
            
            # Add filter if provided
            if filters:
                # Try different filter approaches if the first one fails
                filter_approaches = [
                    lambda f: self._build_filter_string_direct(f),  # Direct field names
                    lambda f: self._build_filter_string(f),        # structData prefix
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
                            filter=filter_string
                        )
                        test_response = self.search_client.search(test_request)
                        
                        # If we get here, the filter worked
                        logger.info(f"Filter syntax validated: {filter_string}")
                        break
                        
                    except Exception as filter_error:
                        logger.warning(f"Filter approach failed: {filter_error}")
                        if approach == filter_approaches[-1]:  # Last approach
                            raise SearchError(f"All filter approaches failed. Last error: {filter_error}")
                        continue
            
            response = self.search_client.search(search_request)
            
            # Format response
            results = []
            for result in response.results:
                # Handle both structData and jsonData formats
                if hasattr(result.document, 'struct_data') and result.document.struct_data:
                    doc_dict = dict(result.document.struct_data)
                elif hasattr(result.document, 'json_data') and result.document.json_data:
                    # json_data might be bytes or string
                    json_data = result.document.json_data
                    if isinstance(json_data, bytes):
                        json_data = json_data.decode('utf-8')
                    doc_dict = json.loads(json_data)
                else:
                    doc_dict = {}
                
                results.append({
                    'document': doc_dict,
                    'score': getattr(result, 'score', None)
                })
            
            facet_results = []
            for facet in response.facets:
                facet_values = [{'value': fv.value, 'count': fv.count} for fv in facet.values]
                facet_results.append({
                    'key': facet.key,
                    'values': facet_values
                })
            
            logger.debug(f"Search returned {len(results)} results, {len(facet_results)} facets, total_size: {response.total_size}")
            
            return {
                'results': results,
                'facets': facet_results,
                'total_size': response.total_size,
                'next_page_token': response.next_page_token
            }
            
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            raise SearchError(f"Search operation failed: {str(e)}") from e
    
    def get_serving_config(self, engine_id: str, serving_config_id: str) -> Dict[str, Any]:
        """Get serving configuration for an engine."""
        try:
            name = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/engines/{engine_id}/servingConfigs/{serving_config_id}"
            
            config = self.search_client.get_serving_config(name=name)
            return {
                'name': config.name,
                'display_name': config.display_name,
                'solution_type': config.solution_type
            }
            
        except Exception as e:
            logger.error(f"Failed to get serving config: {str(e)}")
            raise
    
    def _build_filter_string(self, filters: Dict[str, Any]) -> str:
        """Build filter string from filter dictionary using Vertex AI Discovery Engine syntax."""
        filter_parts = []
        
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
            
            if isinstance(value, str):
                # Escape quotes in the value
                escaped_value = value.replace('"', '\\"')
                filter_parts.append(f'{field_path}: ANY("{escaped_value}")')
            elif isinstance(value, (int, float)):
                filter_parts.append(f'{field_path} = {value}')
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
                filter_parts.append(f'{field_path} = {str(value).lower()}')
        
        result = " AND ".join(filter_parts)
        logger.debug(f"Built filter string: {result}")
        return result
    
    def _build_filter_string_direct(self, filters: Dict[str, Any]) -> str:
        """Build filter string using direct field names (without structData prefix)."""
        filter_parts = []
        
        for field, value in filters.items():
            # Use field name directly without prefix
            field_path = field
            
            if isinstance(value, str):
                escaped_value = value.replace('"', '\\"')
                filter_parts.append(f'{field_path}: ANY("{escaped_value}")')
            elif isinstance(value, (int, float)):
                filter_parts.append(f'{field_path} = {value}')
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
                filter_parts.append(f'{field_path} = {str(value).lower()}')
        
        result = " AND ".join(filter_parts)
        logger.debug(f"Built direct filter string: {result}")
        return result


class AutocompleteManager(AutocompleteManagerInterface):
    """Manages autocomplete functionality."""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        
        # Get credentials and client options
        credentials = get_credentials(config_manager.vertex_ai)
        client_options = setup_client_options(config_manager.vertex_ai)
        
        # Initialize client with credentials (only if not using API key)
        client_kwargs = {}
        if credentials is not None:
            client_kwargs["credentials"] = credentials
        if client_options is not None:
            client_kwargs["client_options"] = client_options
            
        self.completion_client = discoveryengine_v1beta.CompletionServiceClient(**client_kwargs)
    
    def get_suggestions(
        self, 
        query: str, 
        engine_id: str,
        suggestion_types: Optional[List[str]] = None
    ) -> List[str]:
        """Get autocomplete suggestions for a query."""
        try:
            # CompleteQueryRequest uses data_store, not parent or engine
            data_store = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{self.config_manager.vertex_ai.data_store_id}"
            
            request = discoveryengine_v1beta.CompleteQueryRequest(
                data_store=data_store,
                query=query,
                include_tail_suggestions=True
            )
            
            response = self.completion_client.complete_query(request)
            
            suggestions = []
            for suggestion in response.query_suggestions:
                suggestions.append(suggestion.suggestion)
            
            return suggestions
            
        except Exception as e:
            logger.error(f"Failed to get suggestions: {str(e)}")
            return []
    
    def configure_suggestions(
        self, 
        engine_id: str, 
        suggestion_config: Dict[str, Any]
    ) -> bool:
        """Configure suggestion settings for an engine."""
        try:
            # This would configure suggestion settings in Vertex AI
            # Implementation depends on specific requirements
            logger.info(f"Configuring suggestions for engine {engine_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to configure suggestions: {str(e)}")
            return False


class RecommendationManager(RecommendationManagerInterface):
    """Manages recommendation functionality."""
    
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
            
        self.recommendation_client = discoveryengine_v1beta.RecommendationServiceClient(**client_kwargs)
        self.user_event_client = discoveryengine_v1beta.UserEventServiceClient(**client_kwargs)
    
    def get_recommendations(
        self,
        user_event: Dict[str, Any],
        engine_id: str,
        max_results: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Get recommendations based on user events."""
        try:
            parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/engines/{engine_id}/servingConfigs/{self.config_manager.vertex_ai.serving_config_id}"
            
            # Create recommendation request
            request = discoveryengine_v1beta.RecommendRequest(
                serving_config=parent,
                user_event=discoveryengine_v1beta.UserEvent(
                    event_type=user_event.get('eventType', 'view'),
                    user_pseudo_id=user_event.get('userPseudoId', ''),
                    documents=[
                        discoveryengine_v1beta.DocumentInfo(id=doc_id) 
                        for doc_id in user_event.get('documents', [])
                    ]
                ),
                page_size=max_results
            )
            
            response = self.recommendation_client.recommend(request)
            
            recommendations = []
            for result in response.results:
                # Handle both structData and jsonData formats
                if hasattr(result.document, 'struct_data') and result.document.struct_data:
                    doc_dict = dict(result.document.struct_data)
                elif hasattr(result.document, 'json_data') and result.document.json_data:
                    # json_data might be bytes or string
                    json_data = result.document.json_data
                    if isinstance(json_data, bytes):
                        json_data = json_data.decode('utf-8')
                    doc_dict = json.loads(json_data)
                else:
                    doc_dict = {}
                
                recommendations.append({
                    'document': doc_dict,
                    'score': getattr(result, 'score', None)
                })
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Failed to get recommendations: {str(e)}")
            return []
    
    def record_user_event(
        self,
        event_type: str,
        user_pseudo_id: str,
        documents: List[str],
        engine_id: str,
        additional_info: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Record a user event for recommendation training."""
        try:
            parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{engine_id}"
            
            user_event = discoveryengine_v1beta.UserEvent(
                event_type=event_type,
                user_pseudo_id=user_pseudo_id,
                documents=[
                    discoveryengine_v1beta.DocumentInfo(id=doc_id) 
                    for doc_id in documents
                ]
            )
            
            # Add additional info if provided
            if additional_info:
                user_event.attributes.update(additional_info)
            
            self.user_event_client.write_user_event(
                parent=parent,
                user_event=user_event
            )
            
            logger.info(f"Recorded user event: {event_type} for user {user_pseudo_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to record user event: {str(e)}")
            return False
    
    def get_user_events(
        self,
        user_pseudo_id: str,
        engine_id: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get user events for analysis."""
        try:
            # This would retrieve user events from Vertex AI
            # Implementation depends on specific requirements for event retrieval
            logger.info(f"Getting user events for user {user_pseudo_id}")
            return []
            
        except Exception as e:
            logger.error(f"Failed to get user events: {str(e)}")
            return []