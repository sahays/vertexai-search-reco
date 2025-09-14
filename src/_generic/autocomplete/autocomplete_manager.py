"""Autocomplete functionality for Vertex AI Search."""

from typing import Dict, Any, List, Optional
from google.cloud import discoveryengine_v1beta

from ..shared.config import ConfigManager
from ..shared.interfaces import AutocompleteManagerInterface
from ..shared.utils import setup_logging
from ..shared.auth import get_credentials, setup_client_options

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
        suggestion_types: Optional[List[str]] = None,
        max_suggestions: int = 10
    ) -> List[str]:
        """Get autocomplete suggestions for a query."""
        try:
            # Autocomplete uses data store, not engine - use configured data_store_id
            data_store = f"projects/{self.config_manager.vertex_ai.project_id}/locations/{self.config_manager.vertex_ai.location}/collections/default_collection/dataStores/{self.config_manager.vertex_ai.data_store_id}"
            logger.info(f"Using data store for autocomplete: {data_store}")
            
            # Try with query_model parameter first
            try:
                request = discoveryengine_v1beta.CompleteQueryRequest(
                    data_store=data_store,
                    query=query,
                    query_model="document-completable",  # Try document model first
                    user_pseudo_id="",  # Optional: can be used for personalization
                    include_tail_suggestions=True
                )
                
                response = self.completion_client.complete_query(request)
                
                suggestions = []
                for suggestion in response.query_suggestions:
                    suggestions.append(suggestion.suggestion)
                
                if suggestions:  # Return if we get results
                    logger.info(f"Found {len(suggestions)} autocomplete suggestions")
                    return suggestions[:max_suggestions]
                
            except Exception as model_error:
                logger.warning(f"Autocomplete with query_model failed: {model_error}")
            
            # Try without query_model parameter as fallback
            try:
                request = discoveryengine_v1beta.CompleteQueryRequest(
                    data_store=data_store,
                    query=query,
                    include_tail_suggestions=True
                )
                
                response = self.completion_client.complete_query(request)
                suggestions = [suggestion.suggestion for suggestion in response.query_suggestions]
                
                if suggestions:
                    logger.info(f"Found {len(suggestions)} autocomplete suggestions (fallback method)")
                    return suggestions[:max_suggestions]
            
            except Exception as fallback_error:
                logger.warning(f"Fallback autocomplete approach failed: {fallback_error}")
            
            logger.info("No autocomplete suggestions available - this is normal if:")
            logger.info("1. Data was recently imported (autocomplete needs 1-2 days to train)")
            logger.info("2. There's insufficient search traffic to generate suggestions")
            logger.info("3. The query doesn't match existing content patterns")
            return []
            
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