"""Authentication utilities for Media Data Store."""

from google.auth import default
from google.auth.credentials import Credentials


def get_credentials() -> Credentials:
    """Get Google Cloud credentials using gcloud auth."""
    try:
        credentials, _ = default()
        return credentials
    except Exception as e:
        raise RuntimeError(
            f"Failed to get credentials: {e}\n"
            "Please ensure you are authenticated with: gcloud auth login"
        )