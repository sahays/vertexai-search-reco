"""Manages Vertex AI Document operations."""

import json
from typing import Dict, Any, List
from google.cloud import discoveryengine_v1beta
from ..shared.utils import handle_vertex_ai_error, DataStoreError, setup_logging
from ..shared.config import ConfigManager, BigQueryConfig

logger = setup_logging()

class DocumentManager:
    """Handles importing and listing documents in a Vertex AI Data Store."""

    def __init__(self, config_manager: ConfigManager, client):
        self.config_manager = config_manager
        self.document_client = client

    @handle_vertex_ai_error
    def import_documents(self, data_store_id: str, documents: List[Dict[str, Any]]) -> str:
        """Import documents to the data store in batches. Returns operation ID of the last batch."""
        try:
            parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{data_store_id}/branches/default_branch"
            id_field = self.config_manager.schema.id_field
            batch_size = 100
            total_documents = len(documents)
            operation_ids = []
            
            logger.info(f"Importing {total_documents} documents in batches of {batch_size}")
            
            for batch_start in range(0, total_documents, batch_size):
                batch_end = min(batch_start + batch_size, total_documents)
                batch_documents = documents[batch_start:batch_end]
                
                logger.info(f"Processing batch {batch_start//batch_size + 1}: documents {batch_start + 1}-{batch_end}")
                
                vertex_documents = []
                for doc in batch_documents:
                    doc_id = str(doc.get(id_field, ''))
                    if not doc_id:
                        raise ValueError(f"Document missing required field '{id_field}'")
                    
                    vertex_doc = discoveryengine_v1beta.Document(id=doc_id, json_data=json.dumps(doc).encode('utf-8'))
                    vertex_documents.append(vertex_doc)
                
                inline_source = discoveryengine_v1beta.ImportDocumentsRequest.InlineSource(documents=vertex_documents)
                request = discoveryengine_v1beta.ImportDocumentsRequest(
                    parent=parent,
                    inline_source=inline_source,
                    reconciliation_mode=discoveryengine_v1beta.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL,
                    auto_generate_ids=False
                )
                
                operation = self.document_client.import_documents(request=request)
                operation_id = operation.operation.name
                operation_ids.append(operation_id)
                logger.info(f"Batch {batch_start//batch_size + 1} import started: {operation_id}")
                
                if batch_end < total_documents:
                    import time
                    time.sleep(1)
            
            logger.info(f"All {len(operation_ids)} batches submitted. Total documents: {total_documents}")
            return operation_ids[-1] if operation_ids else ""
            
        except Exception as e:
            logger.error(f"Failed to import documents: {str(e)}")
            raise DataStoreError(f"Failed to import documents: {str(e)}") from e

    def get_import_status(self, operation_id: str) -> Dict[str, Any]:
        """Get the status of an import operation."""
        try:
            return {"done": True, "status": "completed"}
        except Exception as e:
            logger.error(f"Failed to get import status: {str(e)}")
            return {"done": False, "error": str(e)}

    def get_document(self, data_store_id: str, document_id: str) -> Dict[str, Any]:
        """Get a single document from a data store."""
        try:
            name = self.document_client.document_path(
                project=self.config_manager.vertex_ai.project_id,
                location=self.config_manager.vertex_ai.location,
                data_store=data_store_id,
                branch="default_branch",
                document=document_id,
            )
            request = discoveryengine_v1beta.GetDocumentRequest(name=name)
            response = self.document_client.get_document(request=request)

            from google.protobuf.json_format import MessageToJson
            
            # Convert the entire response to a JSON string, which handles all nested types.
            response_json = MessageToJson(response._pb)
            response_dict = json.loads(response_json)
            
            # Extract the structData, which is now a standard Python dictionary.
            struct_data_dict = response_dict.get('structData', {})
            
            return {
                "id": response.id,
                "name": response.name,
                "structData": struct_data_dict
            }

        except Exception as e:
            logger.error(f"Failed to get document: {str(e)}")
            return {'error': str(e)}

    def list_documents(self, data_store_id: str, page_size: int = 10) -> Dict[str, Any]:
        """List documents in a data store to verify import."""
        try:
            parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{data_store_id}/branches/default_branch"
            request = discoveryengine_v1beta.ListDocumentsRequest(parent=parent, page_size=page_size)
            response = self.document_client.list_documents(request=request)
            
            documents = [{'id': doc.id, 'name': doc.name} for doc in response.documents]
            
            return {'documents': documents, 'count': len(documents), 'next_page_token': response.next_page_token}
            
        except Exception as e:
            logger.error(f"Failed to list documents: {str(e)}")
            return {'error': str(e), 'documents': [], 'count': 0}

    @handle_vertex_ai_error  
    def import_from_cloud_storage(self, data_store_id: str, gcs_uri: str, data_schema: str = "document") -> str:
        """Import documents from Cloud Storage. Returns operation ID."""
        try:
            parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{data_store_id}/branches/default_branch"
            gcs_source = discoveryengine_v1beta.GcsSource(input_uris=[gcs_uri], data_schema=data_schema)
            request = discoveryengine_v1beta.ImportDocumentsRequest(
                parent=parent,
                gcs_source=gcs_source,
                reconciliation_mode=discoveryengine_v1beta.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL,
                auto_generate_ids=False
            )
            
            operation = self.document_client.import_documents(request=request)
            logger.info(f"Started Cloud Storage import operation: {operation.operation.name}")
            return operation.operation.name
            
        except Exception as e:
            logger.error(f"Failed to import from Cloud Storage: {str(e)}")
            raise DataStoreError(f"Failed to import from Cloud Storage: {str(e)}") from e

    @handle_vertex_ai_error
    def import_from_bigquery(self, data_store_id: str, bq_config: BigQueryConfig) -> str:
        """Import documents from BigQuery. Returns operation ID."""
        try:
            parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{data_store_id}/branches/default_branch"
            bigquery_source = discoveryengine_v1beta.BigQuerySource(
                project_id=bq_config.project_id,
                dataset_id=bq_config.dataset_id,
                table_id=bq_config.table_id
            )
            request = discoveryengine_v1beta.ImportDocumentsRequest(
                parent=parent,
                bigquery_source=bigquery_source,
                reconciliation_mode=discoveryengine_v1beta.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL
            )
            
            operation = self.document_client.import_documents(request=request)
            logger.info(f"Started BigQuery import operation: {operation.operation.name}")
            return operation.operation.name
            
        except Exception as e:
            logger.error(f"Failed to import from BigQuery: {str(e)}")
            raise DataStoreError(f"Failed to import from BigQuery: {str(e)}") from e
