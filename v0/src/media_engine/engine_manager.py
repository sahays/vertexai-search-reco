"""Search Engine management for Vertex AI Search for Media."""

from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime

from google.cloud import discoveryengine_v1beta
from google.cloud.discoveryengine_v1beta.types import Engine, SolutionType, IndustryVertical, CreateEngineRequest
from google.protobuf.json_format import MessageToDict

from .config import EngineConfig, EngineConfigManager
from media_data_store.auth import get_credentials
from media_data_store.utils import setup_logging, MediaDataStoreError, save_output, handle_vertex_ai_error


class MediaEngineManager:
    """Manages Vertex AI Search for Media engines."""
    
    def __init__(self, config: EngineConfig):
        self.config = config
        self.config_manager = EngineConfigManager(config)
        self.logger = setup_logging()
        
        # Validate media requirements
        self.config_manager.validate_media_requirements()
        
        # Get credentials
        credentials = get_credentials()
        
        # Initialize engine service client
        self.engine_client = discoveryengine_v1beta.EngineServiceClient(credentials=credentials)
        
        self.logger.debug(f"Initialized MediaEngineManager for project: {config.vertex_ai.project_id}")
    
    @handle_vertex_ai_error
    def create_engine(self, datastore_id: str, engine_id: str, display_name: str,
                     solution_type: str, description: Optional[str] = None,
                     output_dir: Optional[Path] = None, subcommand: str = "create") -> Dict[str, Any]:
        """Create a VAIS Media search or recommendation engine."""
        self.logger.info(f"Creating media {solution_type.lower()} engine: {engine_id}")
        self.logger.debug(f"Engine parameters - ID: {engine_id}, Name: {display_name}, Datastore: {datastore_id}")
        
        parent = self.config_manager.get_engine_parent()
        
        if solution_type == "SEARCH":
            solution_type_enum = SolutionType.SOLUTION_TYPE_SEARCH
        else:
            solution_type_enum = SolutionType.SOLUTION_TYPE_RECOMMENDATION

        engine_config = {
            "display_name": display_name,
            "data_store_ids": [datastore_id],
            "solution_type": solution_type_enum,
            "industry_vertical": IndustryVertical.MEDIA,
        }

        if solution_type == "RECOMMENDATION":
            # Note: Advanced MediaRecommendationEngineConfig not available in current SDK version
            # The engine will be created as a basic recommendation engine
            # Advanced features like "For You" and CTR optimization may need to be configured
            # through the Google Cloud Console or a newer SDK version
            self.logger.warning(
                "Creating basic recommendation engine. Advanced features like 'For You' "
                "recommendations and CTR optimization may need to be configured via Console."
            )

        engine = Engine(**engine_config)
        
        request = CreateEngineRequest(
            parent=parent,
            engine=engine,
            engine_id=engine_id,
        )

        # Create engine (this returns a long-running operation)
        self.logger.info(f"Creating engine with parent: {parent}")
        operation = self.engine_client.create_engine(request=request)
        
        # Wait for the operation to complete
        self.logger.info("Waiting for engine creation to complete...")
        created_engine = operation.result()
        
        # Extract engine details
        engine_info = {
            "engine_id": engine_id,
            "engine_name": created_engine.name,
            "display_name": created_engine.display_name,
            "description": description or f"Media {solution_type.lower()} engine for {datastore_id}",
            "datastore_ids": list(created_engine.data_store_ids),
            "solution_type": str(created_engine.solution_type),
            "create_time": created_engine.create_time.isoformat() if created_engine.create_time else None,
            "creation_timestamp": datetime.now().isoformat()
        }
        
        self.logger.info(f"Successfully created media {solution_type.lower()} engine: {engine_id}")
        
        if output_dir:
            output_file = save_output(engine_info, output_dir, f"engine_{engine_id}_created.json", subcommand)
            self.logger.info(f"Engine details saved to: {output_file}")
        
        return engine_info
    
    @handle_vertex_ai_error
    def get_engine(self, engine_id: str, output_dir: Optional[Path] = None, subcommand: str = "get") -> Dict[str, Any]:
        """Get detailed information about a search engine."""
        self.logger.info(f"Getting engine information: {engine_id}")
        
        engine_path = self.config_manager.get_engine_path(engine_id)
        
        try:
            engine = self.engine_client.get_engine(name=engine_path)
            
            # Convert search engine config safely if available
            search_config = {}
            if hasattr(engine, 'search_engine_config') and engine.search_engine_config:
                try:
                    search_config = {
                        "search_tier": str(getattr(engine.search_engine_config, 'search_tier', 'unknown')),
                        "search_add_ons": [str(addon) for addon in getattr(engine.search_engine_config, 'search_add_ons', [])]
                    }
                except Exception:
                    search_config = {"status": "unavailable"}
            
            engine_info = {
                "engine_id": engine_id,
                "engine_name": engine.name,
                "display_name": engine.display_name,
                "description": getattr(engine, 'description', 'No description available'),
                "datastore_ids": list(engine.data_store_ids),
                "solution_type": str(engine.solution_type),
                "industry_vertical": str(engine.industry_vertical),
                "search_engine_config": search_config,
                "create_time": engine.create_time.isoformat() if engine.create_time else None,
                "update_time": engine.update_time.isoformat() if engine.update_time else None,
                "retrieved_at": datetime.now().isoformat()
            }
            
            self.logger.info(f"Retrieved engine information for: {engine_id}")
            
            if output_dir:
                output_file = save_output(engine_info, output_dir, f"engine_{engine_id}_info.json", subcommand)
                self.logger.info(f"Engine information saved to: {output_file}")
            
            return engine_info
            
        except Exception as e:
            if "not found" in str(e).lower():
                raise MediaDataStoreError(f"Engine not found: {engine_id}")
            raise
    
    @handle_vertex_ai_error
    def list_engines(self, datastore_id: Optional[str] = None,
                    output_dir: Optional[Path] = None, subcommand: str = "list") -> List[Dict[str, Any]]:
        """List all search engines, optionally filtered by datastore."""
        self.logger.info("Listing media search engines")
        if datastore_id:
            self.logger.debug(f"Filtering by datastore: {datastore_id}")
        
        parent = self.config_manager.get_engine_parent()
        
        engines = []
        for engine in self.engine_client.list_engines(parent=parent):
            # Filter by datastore if specified
            if datastore_id and datastore_id not in engine.data_store_ids:
                continue
            
            # The industry vertical is not a direct property of the Engine object in the API.
            # Filtering should be done based on the associated data stores if necessary.
            
            engine_info = {
                "engine_id": engine.name.split("/")[-1],
                "engine_name": engine.name,
                "display_name": engine.display_name,
                "datastore_ids": list(engine.data_store_ids),
                "solution_type": str(engine.solution_type),
                "industry_vertical": str(engine.industry_vertical),
                "create_time": engine.create_time.isoformat() if engine.create_time else None,
                "update_time": engine.update_time.isoformat() if engine.update_time else None
            }
            engines.append(engine_info)
        
        result = {
            "engines": engines,
            "total_count": len(engines),
            "filtered_by_datastore": datastore_id,
            "listed_at": datetime.now().isoformat()
        }
        
        self.logger.info(f"Found {len(engines)} media search engines")
        
        if output_dir:
            filename = f"engines_list_{datastore_id}.json" if datastore_id else "engines_list_all.json"
            output_file = save_output(result, output_dir, filename, subcommand)
            self.logger.info(f"Engine list saved to: {output_file}")
        
        return result
    
    @handle_vertex_ai_error
    def delete_engine(self, engine_id: str, force: bool = False,
                     output_dir: Optional[Path] = None, subcommand: str = "delete") -> Dict[str, Any]:
        """Delete a search engine."""
        self.logger.info(f"Deleting search engine: {engine_id}")
        
        if not force:
            self.logger.warning("Use --force flag to confirm engine deletion")
            raise MediaDataStoreError("Engine deletion requires --force flag for confirmation")
        
        engine_path = self.config_manager.get_engine_path(engine_id)
        
        # Get engine info before deletion
        try:
            engine_info = self.get_engine(engine_id)
        except MediaDataStoreError:
            engine_info = {"engine_id": engine_id, "status": "not_found"}
        
        # Delete engine
        self.engine_client.delete_engine(name=engine_path)
        
        deletion_result = {
            "engine_id": engine_id,
            "engine_name": engine_path,
            "status": "deleted",
            "deleted_at": datetime.now().isoformat(),
            "previous_info": engine_info
        }
        
        self.logger.info(f"Successfully deleted search engine: {engine_id}")
        
        if output_dir:
            output_file = save_output(deletion_result, output_dir, f"engine_{engine_id}_deleted.json", subcommand)
            self.logger.info(f"Deletion details saved to: {output_file}")
        
        return deletion_result
    
    def _validate_datastore_compatibility(self, datastore_path: str) -> bool:
        """Validate that datastore exists and is compatible with MEDIA engines."""
        try:
            from google.cloud import discoveryengine_v1beta
            datastore_client = discoveryengine_v1beta.DataStoreServiceClient(
                credentials=get_credentials()
            )
            
            datastore = datastore_client.get_data_store(name=datastore_path)
            
            # Check industry vertical
            if datastore.industry_vertical != discoveryengine_v1beta.IndustryVertical.MEDIA:
                raise MediaDataStoreError(
                    f"Datastore {datastore_path} is not a MEDIA datastore. "
                    f"Found: {datastore.industry_vertical}"
                )
            
            self.logger.debug(f"Validated datastore compatibility: {datastore_path}")
            return True
            
        except Exception as e:
            if "not found" in str(e).lower():
                raise MediaDataStoreError(f"Datastore not found: {datastore_path}")
            raise MediaDataStoreError(f"Failed to validate datastore: {e}")