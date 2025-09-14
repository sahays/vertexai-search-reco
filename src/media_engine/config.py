"""Configuration management for Media Search Engine."""

from typing import Dict, Any, Optional
from pathlib import Path
import json
from pydantic import BaseModel, Field

from media_data_store.config import VertexAIConfig


class SearchEngineConfig(BaseModel):
    """Search engine specific configuration."""
    
    content_config: str = Field(default="NO_CONTENT", description="Content configuration for media search")
    industry_vertical: str = Field(default="MEDIA", description="Industry vertical - must be MEDIA")
    search_tier: str = Field(default="SEARCH_TIER_STANDARD", description="Search tier configuration")
    

class EngineConfig(BaseModel):
    """Configuration for Media Search Engine operations."""
    
    vertex_ai: VertexAIConfig
    search_engine: SearchEngineConfig = Field(default_factory=SearchEngineConfig)
    
    @classmethod
    def from_file(cls, config_path: Path) -> "EngineConfig":
        """Load configuration from JSON file."""
        with open(config_path, 'r') as f:
            config_data = json.load(f)
        
        return cls(**config_data)
    
    @classmethod
    def from_datastore_config(cls, datastore_config_path: Path) -> "EngineConfig":
        """Create engine config from existing datastore config."""
        with open(datastore_config_path, 'r') as f:
            datastore_config = json.load(f)
        
        # Extract vertex_ai config and add search engine config
        engine_config = {
            "vertex_ai": datastore_config["vertex_ai"],
            "search_engine": {
                "content_config": "NO_CONTENT",
                "industry_vertical": "MEDIA",
                "search_tier": "SEARCH_TIER_STANDARD"
            }
        }
        
        return cls(**engine_config)


class EngineConfigManager:
    """Manages engine configuration operations."""
    
    def __init__(self, config: EngineConfig):
        self.config = config
    
    def validate_media_requirements(self) -> bool:
        """Validate that configuration meets VAIS Media requirements."""
        if self.config.search_engine.industry_vertical != "MEDIA":
            raise ValueError("Industry vertical must be MEDIA for media search engines")
        
        # Validate supported content configurations for MEDIA
        valid_content_configs = ["NO_CONTENT", "CONTENT_REQUIRED", "PUBLIC_WEBSITE"]
        if self.config.search_engine.content_config not in valid_content_configs:
            raise ValueError(f"Content config must be one of: {valid_content_configs}")
        
        # Validate location supports MEDIA industry vertical
        supported_locations = ["global", "us-central1", "europe-west1"]
        if self.config.vertex_ai.location not in supported_locations:
            raise ValueError(f"Location must be one of: {supported_locations} for MEDIA engines")
        
        return True
    
    def get_engine_parent(self) -> str:
        """Get the parent resource path for engines."""
        return f"projects/{self.config.vertex_ai.project_id}/locations/{self.config.vertex_ai.location}/collections/default_collection"
    
    def get_datastore_path(self, datastore_id: str) -> str:
        """Get full datastore resource path."""
        parent = self.get_engine_parent()
        return f"{parent}/dataStores/{datastore_id}"
    
    def get_engine_path(self, engine_id: str) -> str:
        """Get full engine resource path."""
        parent = self.get_engine_parent()
        return f"{parent}/engines/{engine_id}"