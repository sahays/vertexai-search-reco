"""Abstract interfaces for the Vertex AI Search system."""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Iterator
from pathlib import Path

from .config import BigQueryConfig


class DatasetManagerInterface(ABC):
    """Interface for managing datasets with custom schemas."""
    
    @abstractmethod
    def create_dataset(self, data: List[Dict[str, Any]], schema: Dict[str, Any]) -> bool:
        """Create a new dataset with the given data and schema."""
        pass
    
    @abstractmethod
    def validate_data(self, data: List[Dict[str, Any]], schema: Dict[str, Any]) -> List[str]:
        """Validate data against schema. Returns list of validation errors."""
        pass
    
    @abstractmethod
    def load_data_from_file(self, file_path: Path) -> List[Dict[str, Any]]:
        """Load data from JSON file."""
        pass
    
    @abstractmethod
    def save_data_to_file(self, data: List[Dict[str, Any]], file_path: Path) -> bool:
        """Save data to JSON file."""
        pass


class MediaAssetManagerInterface(ABC):
    """Interface for managing media assets using custom metadata."""
    
    @abstractmethod
    def create_data_store(self, data_store_id: str, display_name: str) -> bool:
        """Create a new data store in Vertex AI."""
        pass
    
    @abstractmethod
    def import_documents(self, data_store_id: str, documents: List[Dict[str, Any]]) -> str:
        """Import documents to the data store. Returns operation ID."""
        pass
    
    @abstractmethod
    def get_import_status(self, operation_id: str) -> Dict[str, Any]:
        """Get the status of an import operation."""
        pass
    
    @abstractmethod
    def delete_data_store(self, data_store_id: str) -> bool:
        """Delete a data store."""
        pass

    @abstractmethod
    def import_from_bigquery(self, data_store_id: str, bq_config: BigQueryConfig) -> str:
        """Import documents from BigQuery. Returns operation ID."""
        pass


class SearchManagerInterface(ABC):
    """Interface for search functionality."""
    
    @abstractmethod
    def create_search_engine(self, engine_id: str, display_name: str, data_store_ids: List[str]) -> bool:
        """Create a search engine connected to data stores."""
        pass
    
    @abstractmethod
    def search(
        self, 
        query: str, 
        engine_id: str,
        filters: Optional[Dict[str, Any]] = None,
        facets: Optional[List[str]] = None,
        page_size: int = 10,
        page_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Perform a search query."""
        pass
    
    @abstractmethod
    def get_serving_config(self, engine_id: str, serving_config_id: str) -> Dict[str, Any]:
        """Get serving configuration for an engine."""
        pass


class AutocompleteManagerInterface(ABC):
    """Interface for autocomplete functionality."""
    
    @abstractmethod
    def get_suggestions(
        self, 
        query: str, 
        engine_id: str,
        suggestion_types: Optional[List[str]] = None
    ) -> List[str]:
        """Get autocomplete suggestions for a query."""
        pass
    
    @abstractmethod
    def configure_suggestions(
        self, 
        engine_id: str, 
        suggestion_config: Dict[str, Any]
    ) -> bool:
        """Configure suggestion settings for an engine."""
        pass


class RecommendationManagerInterface(ABC):
    """Interface for recommendation functionality."""
    
    @abstractmethod
    def get_recommendations(
        self,
        user_event: Dict[str, Any],
        engine_id: str,
        max_results: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Get recommendations based on user events."""
        pass
    
    @abstractmethod
    def record_user_event(
        self,
        event_type: str,
        user_pseudo_id: str,
        documents: List[str],
        engine_id: str,
        additional_info: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Record a user event for recommendation training."""
        pass
    
    @abstractmethod
    def get_user_events(
        self,
        user_pseudo_id: str,
        engine_id: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get user events for analysis."""
        pass


class DataGeneratorInterface(ABC):
    """Interface for generating sample data based on schemas."""
    
    @abstractmethod
    def generate_sample_data(
        self, 
        schema: Dict[str, Any], 
        count: int,
        seed: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Generate sample data conforming to the given schema."""
        pass
    
    @abstractmethod
    def analyze_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze schema to understand field types and constraints."""
        pass
