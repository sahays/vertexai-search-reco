"""Recommendation functionality for Vertex AI Search."""

import json
from typing import Dict, Any, List, Optional
from google.cloud import discoveryengine_v1beta

from ..config import ConfigManager
from ..interfaces import RecommendationManagerInterface
from ..utils import setup_logging
from ..auth import get_credentials, setup_client_options

logger = setup_logging()


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
                    event_type=user_event.get('eventType', 'media-play'),
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
        data_store_id: str,
        additional_info: Optional[Dict[str, Any]] = None,
        media_progress_duration: Optional[float] = None,
        media_progress_percentage: Optional[float] = None
    ) -> bool:
        """Record a user event for recommendation training."""
        try:
            parent = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{data_store_id}"
            
            user_event = discoveryengine_v1beta.UserEvent(
                event_type=event_type,
                user_pseudo_id=user_pseudo_id,
                documents=[
                    discoveryengine_v1beta.DocumentInfo(id=doc_id) 
                    for doc_id in documents
                ]
            )
            
            # Add media info for media events that require it
            if event_type in ['media-complete', 'media-progress'] or media_progress_duration is not None or media_progress_percentage is not None:
                from google.protobuf import duration_pb2
                media_info = discoveryengine_v1beta.MediaInfo()
                
                if media_progress_duration is not None:
                    # Convert to Google Duration format
                    duration = duration_pb2.Duration()
                    duration.seconds = int(media_progress_duration)
                    duration.nanos = int((media_progress_duration % 1) * 1e9)
                    media_info.media_progress_duration = duration
                elif event_type == 'media-complete':
                    # For complete events, assume 100% completion if not specified
                    duration = duration_pb2.Duration()
                    duration.seconds = 0  # Will be set by the service if not provided
                    media_info.media_progress_duration = duration
                
                if media_progress_percentage is not None:
                    # Ensure percentage is between 0 and 1 (convert from 0-100 if needed)
                    if media_progress_percentage > 1.0:
                        media_progress_percentage = media_progress_percentage / 100.0
                    media_info.media_progress_percentage = media_progress_percentage
                elif event_type == 'media-complete':
                    # For complete events, set to 1.0 (100%) if not specified
                    media_info.media_progress_percentage = 1.0
                
                user_event.media_info = media_info
            
            # Add additional info if provided
            if additional_info:
                user_event.attributes.update(additional_info)
            
            request = discoveryengine_v1beta.WriteUserEventRequest(
                parent=parent,
                user_event=user_event
            )
            
            self.user_event_client.write_user_event(request)
            
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