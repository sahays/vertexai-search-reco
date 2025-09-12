"""Manages Vertex AI Data Store operations."""

from google.cloud import discoveryengine_v1beta
from ..shared.utils import handle_vertex_ai_error, DataStoreError, setup_logging
from ..shared.config import ConfigManager

logger = setup_logging()

class DataStoreManager:
    """Handles creation and deletion of Vertex AI Data Stores."""

    def __init__(self, config_manager: ConfigManager, client):
        self.config_manager = config_manager
        self.client = client

    @handle_vertex_ai_error
    def create_data_store(self, data_store_id: str, display_name: str, solution_type: str = "SEARCH") -> bool:
        """Create a new data store in Vertex AI."""
        try:
            parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection"
            
            vertical_mapping = {
                "GENERIC": discoveryengine_v1beta.IndustryVertical.GENERIC,
                "MEDIA": discoveryengine_v1beta.IndustryVertical.MEDIA,
                "HEALTHCARE_FHIR": discoveryengine_v1beta.IndustryVertical.HEALTHCARE_FHIR
            }
            industry_vertical = vertical_mapping.get(
                self.config_manager.vertex_ai.industry_vertical, 
                discoveryengine_v1beta.IndustryVertical.GENERIC
            )
            
            solution_types = [
                discoveryengine_v1beta.SolutionType.SOLUTION_TYPE_RECOMMENDATION
                if solution_type.upper() == "RECOMMENDATION"
                else discoveryengine_v1beta.SolutionType.SOLUTION_TYPE_SEARCH
            ]
            
            data_store = discoveryengine_v1beta.DataStore(
                display_name=display_name,
                industry_vertical=industry_vertical,
                solution_types=solution_types,
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
