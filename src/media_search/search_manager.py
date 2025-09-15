"""Manages Vertex AI Search and Autocomplete operations."""

import json
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime

from google.cloud import discoveryengine_v1beta
from google.protobuf.json_format import MessageToDict

from .config import ConfigManager
from media_data_store.auth import get_credentials
from media_data_store.utils import setup_logging, MediaDataStoreError, save_output, handle_vertex_ai_error

class SearchManager:
    """Handles Search, Autocomplete, and Event Tracking for Vertex AI."""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.logger = setup_logging()
        credentials = get_credentials()
        
        # Initialize the three clients required for search, autocomplete, and events
        self.search_client = discoveryengine_v1beta.SearchServiceClient(credentials=credentials)
        self.completion_client = discoveryengine_v1beta.CompletionServiceClient(credentials=credentials)
        self.user_event_client = discoveryengine_v1beta.UserEventServiceClient(credentials=credentials)

    @handle_vertex_ai_error
    async def search(self, query: str, engine_id: str, filter_expression: Optional[str] = None,
                     facet_fields: List[str] = [], page_size: int = 10, offset: int = 0,
                     json_output: bool = False, output_dir: Optional[Path] = None) -> Dict[str, Any]:
        """Performs a search request."""
        self.logger.info(f"Performing search for query: '{query}' on data store: {engine_id}")

        serving_config = self.search_client.serving_config_path(
            project=self.config_manager.vertex_ai.project_id,
            location=self.config_manager.vertex_ai.location,
            data_store=engine_id,
            serving_config="default_serving_config",
        )

        # Build facet specs
        facet_specs = []
        for field in facet_fields:
            facet_specs.append(discoveryengine_v1beta.SearchRequest.FacetSpec(
                facet_key=discoveryengine_v1beta.SearchRequest.FacetSpec.FacetKey(key=field),
                limit=100
            ))

        request = discoveryengine_v1beta.SearchRequest(
            serving_config=serving_config,
            query=query,
            filter=filter_expression,
            page_size=page_size,
            offset=offset,
            facet_specs=facet_specs,
            content_search_spec=discoveryengine_v1beta.SearchRequest.ContentSearchSpec(
                snippet_spec=discoveryengine_v1beta.SearchRequest.ContentSearchSpec.SnippetSpec(
                    return_snippet=True
                ),
                summary_spec=discoveryengine_v1beta.SearchRequest.ContentSearchSpec.SummarySpec(
                    summary_result_count=5,
                    include_citations=True,
                ),
            ),
        )

        response = self.search_client.search(request)
        
        # Process results
        results = [MessageToDict(r._pb) for r in response.results]
        facets = [MessageToDict(f._pb) for f in response.facets]

        search_response = {
            "query": query,
            "total_size": response.total_size,
            "results": results,
            "facets": facets,
            "search_time": datetime.now().isoformat()
        }

        if json_output and output_dir:
            # Parse original_payload from string to JSON object
            for result in search_response.get('results', []):
                try:
                    doc_struct = result.get('document', {}).get('structData', {})
                    if 'original_payload' in doc_struct and isinstance(doc_struct['original_payload'], str):
                        doc_struct['original_payload'] = json.loads(doc_struct['original_payload'])
                except (json.JSONDecodeError, KeyError) as e:
                    self.logger.warning(f"Could not parse original_payload for document ID {result.get('id')}: {e}")

            output_file = save_output(search_response, output_dir, f"search_results_{datetime.now().strftime('%H%M%S')}.json", "search")
            self.logger.info(f"Full JSON response saved to: {output_file}")

        return search_response

    @handle_vertex_ai_error
    async def autocomplete(self, query: str, engine_id: str, output_dir: Optional[Path] = None) -> List[str]:
        """Performs an autocomplete request."""
        self.logger.info(f"Performing autocomplete for query: '{query}' on engine: {engine_id}")

        data_store = self.completion_client.data_store_path(
            project=self.config_manager.vertex_ai.project_id,
            location=self.config_manager.vertex_ai.location,
            data_store=engine_id,
        )

        request = discoveryengine_v1beta.CompleteQueryRequest(
            data_store=data_store,
            query=query,
            query_model="page-level",
            user_pseudo_id="user-123", # This should be a unique identifier for the user
        )

        response = self.completion_client.complete_query(request)
        
        suggestions = [s.suggestion for s in response.query_suggestions]

        if output_dir:
            output_data = {
                "query": query,
                "suggestions": suggestions,
                "completion_time": datetime.now().isoformat()
            }
            output_file = save_output(output_data, output_dir, f"autocomplete_results_{datetime.now().strftime('%H%M%S')}.json", "autocomplete")
            self.logger.info(f"Autocomplete results saved to: {output_file}")

        return suggestions

    # Placeholder for the event tracking method
    async def track_event(self, *args, **kwargs):
        pass
