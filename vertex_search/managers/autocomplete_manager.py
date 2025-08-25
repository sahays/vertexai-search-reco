"""Autocomplete functionality for Vertex AI Search."""

from typing import Dict, Any, List, Optional
from google.cloud import discoveryengine_v1beta

from ..config import ConfigManager
from ..interfaces import AutocompleteManagerInterface
from ..utils import setup_logging
from ..auth import get_credentials, setup_client_options

logger = setup_logging()


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