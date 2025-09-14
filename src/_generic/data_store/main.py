"""Facade for managing media assets in Vertex AI Search."""

from typing import Dict, Any, List
from ..shared.config import ConfigManager, BigQueryConfig
from ..shared.interfaces import MediaAssetManagerInterface
from .client_manager import ClientManager
from .datastore_manager import DataStoreManager
from .document_manager import DocumentManager
from .gcs_manager import GCSManager
from .schema_manager import SchemaManager

class MediaAssetManager(MediaAssetManagerInterface):
    """
    Acts as a facade, delegating media asset management tasks to specialized managers.
    """
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        
        clients = ClientManager(config_manager)
        
        self._datastore_manager = DataStoreManager(config_manager, clients.datastore_client)
        self._document_manager = DocumentManager(config_manager, clients.document_client)
        self._gcs_manager = GCSManager(config_manager, clients.storage_client)
        self._schema_manager = SchemaManager(config_manager, clients.schema_client)

    def create_data_store(self, data_store_id: str, display_name: str, solution_type: str = "SEARCH") -> bool:
        return self._datastore_manager.create_data_store(data_store_id, display_name, solution_type)

    def import_documents(self, data_store_id: str, documents: List[Dict[str, Any]]) -> str:
        return self._document_manager.import_documents(data_store_id, documents)

    def get_import_status(self, operation_id: str) -> Dict[str, Any]:
        return self._document_manager.get_import_status(operation_id)

    def get_document(self, data_store_id: str, document_id: str) -> Dict[str, Any]:
        return self._document_manager.get_document(data_store_id, document_id)

    def list_documents(self, data_store_id: str, page_size: int = 10) -> Dict[str, Any]:
        return self._document_manager.list_documents(data_store_id, page_size)

    def upload_to_cloud_storage(self, bucket_name: str, documents: List[Dict[str, Any]], folder_path: str = "", batch_size: int = 1000) -> List[str]:
        return self._gcs_manager.upload_to_cloud_storage(bucket_name, documents, folder_path, batch_size)

    def import_from_cloud_storage(self, data_store_id: str, gcs_uri: str, data_schema: str = "document") -> str:
        return self._document_manager.import_from_cloud_storage(data_store_id, gcs_uri, data_schema)

    def import_from_bigquery(self, data_store_id: str, bq_config: BigQueryConfig) -> str:
        return self._document_manager.import_from_bigquery(data_store_id, bq_config)

    def create_bucket_if_not_exists(self, bucket_name: str, location: str = "US") -> bool:
        return self._gcs_manager.create_bucket_if_not_exists(bucket_name, location)

    def get_schema(self, data_store_id: str) -> Dict[str, Any]:
        return self._schema_manager.get_schema(data_store_id)

    def apply_field_settings_from_config(self, data_store_id: str, verbose: bool = False) -> bool:
        return self._schema_manager.apply_field_settings_from_config(data_store_id, verbose)

    def delete_data_store(self, data_store_id: str) -> bool:
        return self._datastore_manager.delete_data_store(data_store_id)
