"""Configuration management for Media Search."""

from pathlib import Path
import json
from pydantic import BaseModel, Field
from media_data_store.config import VertexAIConfig

class SearchCLIConfig(BaseModel):
    """Configuration for Media Search operations."""
    
    vertex_ai: VertexAIConfig
    
    @classmethod
    def from_file(cls, config_path: Path) -> "SearchCLIConfig":
        """Load configuration from JSON file."""
        with open(config_path, 'r') as f:
            config_data = json.load(f)
        
        # We only need the vertex_ai part of the config
        return cls(vertex_ai=config_data["vertex_ai"])

class ConfigManager:
    """Manages configuration for media search operations."""
    
    def __init__(self, config: SearchCLIConfig):
        self.config = config
    
    @property
    def vertex_ai(self) -> VertexAIConfig:
        return self.config.vertex_ai
