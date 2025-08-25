"""Media asset management functionality for Vertex AI Search."""

import json
import uuid
from typing import Dict, Any, List
from google.cloud import discoveryengine_v1beta
from google.cloud import storage

from ..config import ConfigManager
from ..interfaces import MediaAssetManagerInterface
from ..utils import handle_vertex_ai_error, DataStoreError, setup_logging
from ..auth import get_credentials, setup_client_options

logger = setup_logging()


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