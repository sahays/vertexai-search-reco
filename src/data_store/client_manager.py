"""Manages initialization of Google Cloud clients."""

from google.cloud import discoveryengine_v1beta, storage
from ..shared.auth import get_credentials, setup_client_options
from ..shared.config import ConfigManager

class ClientManager:
    """Handles the creation and configuration of Google Cloud clients."""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self._credentials = get_credentials(self.config_manager.vertex_ai)
        self._client_options = setup_client_options(self.config_manager.vertex_ai)
        self._client_kwargs = self._build_client_kwargs()
        self._storage_kwargs = self._build_storage_kwargs()

        self.datastore_client = self._create_datastore_client()
        self.document_client = self._create_document_client()
        self.schema_client = self._create_schema_client()
        self.storage_client = self._create_storage_client()

    def _build_client_kwargs(self):
        kwargs = {}
        if self._credentials:
            kwargs["credentials"] = self._credentials
        if self._client_options:
            kwargs["client_options"] = self._client_options
        return kwargs

    def _build_storage_kwargs(self):
        kwargs = {}
        if self._credentials:
            kwargs["credentials"] = self._credentials
        return kwargs

    def _create_datastore_client(self):
        return discoveryengine_v1beta.DataStoreServiceClient(**self._client_kwargs)

    def _create_document_client(self):
        return discoveryengine_v1beta.DocumentServiceClient(**self._client_kwargs)

    def _create_schema_client(self):
        return discoveryengine_v1beta.SchemaServiceClient(**self._client_kwargs)

    def _create_storage_client(self):
        return storage.Client(**self._storage_kwargs)
