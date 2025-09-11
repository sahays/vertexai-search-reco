"""Manages Google Cloud Storage operations."""

import json
import uuid
from typing import Dict, Any, List
from ...utils import DataStoreError, setup_logging
from ...config import ConfigManager

logger = setup_logging()

class GCSManager:
    """Handles interactions with Google Cloud Storage."""

    def __init__(self, config_manager: ConfigManager, client):
        self.config_manager = config_manager
        self.storage_client = client

    def upload_to_cloud_storage(self, bucket_name: str, documents: List[Dict[str, Any]], folder_path: str = "", batch_size: int = 1000) -> List[str]:
        """Upload documents to Cloud Storage as JSONL files for Vertex AI import. Returns list of GCS URIs."""
        try:
            bucket = self.storage_client.bucket(bucket_name)
            uploaded_uris = []
            id_field = self.config_manager.schema.id_field
            
            logger.info(f"Uploading {len(documents)} documents to gs://{bucket_name}/{folder_path} as JSONL files")
            
            for batch_start in range(0, len(documents), batch_size):
                batch_end = min(batch_start + batch_size, len(documents))
                batch_documents = documents[batch_start:batch_end]
                
                batch_id = f"batch_{batch_start//batch_size + 1:04d}"
                blob_path = f"{folder_path}/{batch_id}.jsonl" if folder_path else f"{batch_id}.jsonl"
                blob = bucket.blob(blob_path)
                
                jsonl_lines = []
                for doc in batch_documents:
                    doc_id = str(doc.get(id_field, str(uuid.uuid4())))
                    doc_with_id = doc.copy()
                    doc_with_id[id_field] = doc_id
                    
                    vertex_doc = {"id": doc_id, "structData": doc_with_id}
                    jsonl_lines.append(json.dumps(vertex_doc, default=str))
                
                jsonl_content = '\n'.join(jsonl_lines)
                blob.upload_from_string(jsonl_content, content_type='application/json')
                
                uri = f"gs://{bucket_name}/{blob_path}"
                uploaded_uris.append(uri)
                logger.info(f"Uploaded batch {batch_start//batch_size + 1}: {len(batch_documents)} documents to {blob_path}")
            
            logger.info(f"Successfully uploaded {len(documents)} documents in {len(uploaded_uris)} JSONL files")
            return uploaded_uris
            
        except Exception as e:
            logger.error(f"Failed to upload to Cloud Storage: {str(e)}")
            raise DataStoreError(f"Failed to upload to Cloud Storage: {str(e)}") from e

    def create_bucket_if_not_exists(self, bucket_name: str, location: str = "US") -> bool:
        """Create a Cloud Storage bucket if it doesn't exist."""
        try:
            try:
                self.storage_client.get_bucket(bucket_name)
                logger.info(f"Bucket {bucket_name} already exists")
                return True
            except Exception:
                bucket = self.storage_client.bucket(bucket_name)
                self.storage_client.create_bucket(bucket, location=location)
                logger.info(f"Created bucket {bucket_name} in {location}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to create bucket: {str(e)}")
            return False
