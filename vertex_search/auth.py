"""Authentication utilities for Google Cloud APIs."""

import os
from typing import Optional
from google.auth import default
from google.auth.credentials import Credentials
from google.oauth2 import service_account
from google.auth.transport.requests import Request
import google.auth.transport.requests

from .config import VertexAIConfig
from .utils import ConfigurationError


def get_credentials(config: VertexAIConfig) -> Optional[Credentials]:
    """Get Google Cloud credentials based on configuration."""
    
    # Option 1: Use API Key if provided (will be handled in client options)
    if config.api_key:
        return None  # API key auth doesn't use credentials object
    
    # Option 2: Use Application Default Credentials (ADC)
    # This will check for:
    # - GOOGLE_APPLICATION_CREDENTIALS environment variable (service account key file)
    # - gcloud auth application-default login credentials
    # - Compute Engine/Cloud Run service account
    try:
        credentials, project = default()
        
        # Verify the project matches if specified
        if project and project != config.project_id:
            raise ConfigurationError(
                f"Default credentials project ({project}) doesn't match "
                f"configured project ({config.project_id})"
            )
        
        return credentials
        
    except Exception as e:
        raise ConfigurationError(
            f"Failed to get default credentials. Please set VERTEX_API_KEY "
            f"or configure Application Default Credentials: {str(e)}"
        )


def _create_api_key_credentials(api_key: str) -> "APIKeyCredentials":
    """Create credentials object that uses API key for authentication."""
    return APIKeyCredentials(api_key)


class APIKeyCredentials(Credentials):
    """Credentials implementation that uses an API key for authentication."""
    
    def __init__(self, api_key: str):
        super().__init__()
        self.api_key = api_key
        self._token = api_key
    
    @property
    def token(self) -> str:
        """Return the API key as the token."""
        return self._token
    
    def refresh(self, request: Request) -> None:
        """API keys don't need refreshing."""
        pass
    
    def expired(self) -> bool:
        """API keys don't expire in the traditional sense."""
        return False
    
    def valid(self) -> bool:
        """Check if the API key is present."""
        return bool(self.api_key)
    
    def apply(self, headers, token=None):
        """Apply the API key to request headers."""
        headers['X-Goog-Api-Key'] = self.api_key


def setup_client_options(config: VertexAIConfig):
    """Set up client options for Google Cloud clients."""
    from google.api_core import client_options
    
    options = {}
    
    # Configure API endpoint if needed
    if config.location and config.location != "global":
        options["api_endpoint"] = f"{config.location}-discoveryengine.googleapis.com"
    
    # Add API key if provided
    if config.api_key:
        options["api_key"] = config.api_key
    
    return client_options.ClientOptions(**options) if options else None


def validate_authentication(config: VertexAIConfig) -> bool:
    """Validate that authentication is properly configured."""
    try:
        # For API key, just check if key is present
        if config.api_key:
            return bool(config.api_key.strip())
        
        # For other credentials, try to get default credentials
        credentials = get_credentials(config)
        if credentials is None:
            return False
        
        # Try to refresh to validate
        if credentials.expired:
            request = google.auth.transport.requests.Request()
            credentials.refresh(request)
        
        return credentials.valid
        
    except Exception as e:
        print(f"Authentication validation failed: {str(e)}")
        return False