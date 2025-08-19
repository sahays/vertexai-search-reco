"""Configuration management for Vertex AI Search."""

import os
from pathlib import Path
from typing import Optional, Dict, Any
import json
from pydantic import BaseModel, Field
from dotenv import load_dotenv


class VertexAIConfig(BaseModel):
    """Configuration for Vertex AI Search."""
    
    project_id: str = Field(..., description="Google Cloud Project ID")
    location: str = Field(default="global", description="Vertex AI location")
    data_store_id: Optional[str] = Field(None, description="Data store ID")
    engine_id: Optional[str] = Field(None, description="Search engine ID")
    serving_config_id: str = Field(default="default_config", description="Serving config ID")
    
    class Config:
        env_prefix = "VERTEX_"


class SchemaConfig(BaseModel):
    """Configuration for metadata schema."""
    
    schema_file: Path = Field(..., description="Path to JSON schema file")
    id_field: str = Field(default="id", description="Field name for unique ID")
    title_field: str = Field(default="title", description="Field name for title/name")
    content_field: Optional[str] = Field(None, description="Field name for main content")
    searchable_fields: list[str] = Field(default_factory=list, description="Fields to include in search")
    filterable_fields: list[str] = Field(default_factory=list, description="Fields to use for filtering")
    facetable_fields: list[str] = Field(default_factory=list, description="Fields to use for faceting")


class AppConfig(BaseModel):
    """Main application configuration."""
    
    vertex_ai: VertexAIConfig
    schema: SchemaConfig
    data_directory: Path = Field(default=Path("data"), description="Directory for data files")
    output_directory: Path = Field(default=Path("output"), description="Directory for output files")
    batch_size: int = Field(default=100, description="Batch size for operations")
    max_workers: int = Field(default=4, description="Maximum worker threads")
    
    @classmethod
    def from_file(cls, config_path: Path) -> "AppConfig":
        """Load configuration from JSON file."""
        with open(config_path) as f:
            config_data = json.load(f)
        return cls(**config_data)
    
    @classmethod
    def from_env(cls, schema_file: Path) -> "AppConfig":
        """Load configuration from environment variables."""
        load_dotenv()
        
        vertex_config = VertexAIConfig(
            project_id=os.getenv("VERTEX_PROJECT_ID", ""),
            location=os.getenv("VERTEX_LOCATION", "global"),
            data_store_id=os.getenv("VERTEX_DATA_STORE_ID"),
            engine_id=os.getenv("VERTEX_ENGINE_ID"),
            serving_config_id=os.getenv("VERTEX_SERVING_CONFIG_ID", "default_config")
        )
        
        schema_config = SchemaConfig(
            schema_file=schema_file,
            id_field=os.getenv("SCHEMA_ID_FIELD", "id"),
            title_field=os.getenv("SCHEMA_TITLE_FIELD", "title"),
            content_field=os.getenv("SCHEMA_CONTENT_FIELD"),
            searchable_fields=os.getenv("SCHEMA_SEARCHABLE_FIELDS", "").split(",") if os.getenv("SCHEMA_SEARCHABLE_FIELDS") else [],
            filterable_fields=os.getenv("SCHEMA_FILTERABLE_FIELDS", "").split(",") if os.getenv("SCHEMA_FILTERABLE_FIELDS") else [],
            facetable_fields=os.getenv("SCHEMA_FACETABLE_FIELDS", "").split(",") if os.getenv("SCHEMA_FACETABLE_FIELDS") else []
        )
        
        return cls(
            vertex_ai=vertex_config,
            schema=schema_config,
            data_directory=Path(os.getenv("DATA_DIRECTORY", "data")),
            output_directory=Path(os.getenv("OUTPUT_DIRECTORY", "output")),
            batch_size=int(os.getenv("BATCH_SIZE", "100")),
            max_workers=int(os.getenv("MAX_WORKERS", "4"))
        )


class ConfigManager:
    """Manager for application configuration."""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Ensure required directories exist."""
        self.config.data_directory.mkdir(parents=True, exist_ok=True)
        self.config.output_directory.mkdir(parents=True, exist_ok=True)
    
    @property
    def vertex_ai(self) -> VertexAIConfig:
        """Get Vertex AI configuration."""
        return self.config.vertex_ai
    
    @property
    def schema(self) -> SchemaConfig:
        """Get schema configuration."""
        return self.config.schema
    
    def validate_schema_file(self) -> Dict[str, Any]:
        """Validate and load the schema file."""
        if not self.config.schema.schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {self.config.schema.schema_file}")
        
        with open(self.config.schema.schema_file) as f:
            schema = json.load(f)
        
        # Basic validation
        if not isinstance(schema, dict):
            raise ValueError("Schema must be a JSON object")
        
        if "properties" not in schema:
            raise ValueError("Schema must have 'properties' field")
        
        return schema
    
    def get_searchable_fields(self, schema: Dict[str, Any]) -> list[str]:
        """Get list of searchable fields from schema."""
        if self.config.schema.searchable_fields:
            return self.config.schema.searchable_fields
        
        # Auto-detect searchable fields (string types)
        searchable = []
        properties = schema.get("properties", {})
        
        for field_name, field_spec in properties.items():
            field_type = field_spec.get("type")
            if field_type == "string" or (isinstance(field_type, list) and "string" in field_type):
                searchable.append(field_name)
        
        return searchable
    
    def get_filterable_fields(self, schema: Dict[str, Any]) -> list[str]:
        """Get list of filterable fields from schema."""
        if self.config.schema.filterable_fields:
            return self.config.schema.filterable_fields
        
        # Auto-detect filterable fields (enums, numbers, booleans)
        filterable = []
        properties = schema.get("properties", {})
        
        for field_name, field_spec in properties.items():
            field_type = field_spec.get("type")
            has_enum = "enum" in field_spec
            
            if has_enum or field_type in ["number", "integer", "boolean"]:
                filterable.append(field_name)
        
        return filterable