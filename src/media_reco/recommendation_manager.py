"""Manages Vertex AI Search Recommendation operations."""

import json
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime

from google.cloud import discoveryengine_v1beta
from google.cloud.discoveryengine_v1beta.types import (
    RecommendRequest,
    ServingConfig,
    UserEvent,
    DocumentInfo,
)
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import MessageToDict

from .config import ConfigManager
from media_data_store.auth import get_credentials
from media_data_store.utils import setup_logging, MediaDataStoreError, save_output, handle_vertex_ai_error

class RecommendationManager:
    """Handles Recommendation requests for Vertex AI Search for Media."""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.logger = setup_logging()
        credentials = get_credentials()
        
        self.recommendation_client = discoveryengine_v1beta.RecommendationServiceClient(credentials=credentials)
        self.engine_client = discoveryengine_v1beta.EngineServiceClient(credentials=credentials)

    @handle_vertex_ai_error
    async def recommend(self, serving_config_id: str, document_id: str, user_id: str,
                        page_size: int = 10, output_dir: Optional[Path] = None) -> Dict[str, Any]:
        """Performs a recommendation request."""
        self.logger.info(f"Fetching recommendations for doc '{document_id}' using serving config '{serving_config_id}'")

        # Recommendation serving configs are children of a collection and engine
        serving_config = self.engine_client.serving_config_path(
            project=self.config_manager.vertex_ai.project_id,
            location=self.config_manager.vertex_ai.location,
            collection="default_collection",
            engine="default_engine", # This should be parameterized if you support multiple engines
            serving_config=serving_config_id,
        )

        # Create a user event to provide context for the recommendation
        user_event = discoveryengine_v1beta.UserEvent(
            event_type="view-item",
            user_pseudo_id=user_id,
            documents=[discoveryengine_v1beta.DocumentInfo(id=document_id)],
            event_time=Timestamp(seconds=int(datetime.now().timestamp()))
        )

        request = discoveryengine_v1beta.RecommendRequest(
            serving_config=serving_config,
            user_event=user_event,
            page_size=page_size,
            params={"returnDocument": True}
        )

        response = self.recommendation_client.recommend(request)
        
        results = [MessageToDict(r._pb) for r in response.results]

        reco_response = {
            "document_id": document_id,
            "user_id": user_id,
            "results": results,
            "recommendation_time": datetime.now().isoformat()
        }

        if output_dir:
            output_file = save_output(reco_response, output_dir, f"reco_results_{document_id}_{datetime.now().strftime('%H%M%S')}.json", "recommend")
            self.logger.info(f"Full JSON response saved to: {output_file}")

        return reco_response