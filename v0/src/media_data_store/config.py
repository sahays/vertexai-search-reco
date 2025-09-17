"""Configuration management for Media Data Store."""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from pydantic import BaseModel


class VertexAIConfig(BaseModel):
    """Vertex AI configuration for media search."""
    project_id: str
    location: str = "global"
    data_store_id: Optional[str] = None



class CustomSchemaMapping(BaseModel):
    """Custom schema field mappings for media content."""
    # Source field mappings to Google's required fields
    title_source_field: str  # Maps to Google's 'title' field
    uri_source_field: str    # Maps to Google's 'uri' field
    categories_source_field: str  # Maps to Google's 'categories' field
    available_time_source_field: str  # Maps to Google's 'available_time' field
    duration_source_field: str  # Maps to Google's 'duration' field
    
    # Optional field mappings
    id_source_field: str = "id"
    content_source_field: Optional[str] = None
    language_source_field: Optional[str] = None
    rating_source_field: Optional[str] = None
    persons_source_field: Optional[str] = None
    organizations_source_field: Optional[str] = None


class MediaSchemaConfig(BaseModel):
    """Custom schema configuration for media content with field mappings."""
    schema_type: str = "custom"  # "custom" or "google_predefined"
    
    # Custom field mappings
    field_mappings: CustomSchemaMapping
    
    # Field behavior configuration  
    searchable_fields: List[str]  # Source field names that should be searchable
    retrievable_fields: List[str] # Source field names that should be retrievable
    indexable_fields: List[str]   # Source field names that should be indexed
    completable_fields: List[str] # Source field names for autocomplete
    dynamic_facetable_fields: List[str]  # Source field names for faceting


class SampleFilesConfig(BaseModel):
    """Sample files configuration."""
    data_file: str
    mapping_file: str


class MediaDataStoreConfig(BaseModel):
    """Main configuration for Media Data Store."""
    vertex_ai: VertexAIConfig
    media_schema: MediaSchemaConfig  # Renamed to avoid BaseModel.schema conflict
    sample_files: SampleFilesConfig

    @classmethod
    def from_file(cls, config_path: Path) -> "MediaDataStoreConfig":
        """Load configuration from file."""
        with open(config_path, "r") as f:
            config_data = json.load(f)
        return cls(**config_data)


class ConfigManager:
    """Manages configuration for media data store operations."""
    
    def __init__(self, config: MediaDataStoreConfig, config_dir: Path):
        self.config = config
        self.config_dir = config_dir
        
    @property
    def vertex_ai(self) -> VertexAIConfig:
        return self.config.vertex_ai
        
    @property
    def schema(self) -> MediaSchemaConfig:
        return self.config.media_schema
        
    @property
    def sample_files(self) -> SampleFilesConfig:
        return self.config.sample_files
    
    def get_data_file_path(self) -> Path:
        """Get path to sample data file."""
        return self.config_dir / self.sample_files.data_file
    
    def get_mapping_file_path(self) -> Path:
        """Get path to sample mapping file."""
        return self.config_dir / self.sample_files.mapping_file