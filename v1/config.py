"""Configuration and utilities for Vertex AI Search CLI"""

import json
from google.auth import default
import google.auth.transport.requests


class Config:
    """Global configuration class"""

    PROJECT_ID = None
    DATASET_ID = "media_dataset"
    DATASTORE_ID = "media-datastore"
    ENGINE_ID = "media-search-engine"
    LOCATION = "asia-south1"

    @classmethod
    def set_config(cls, project_id, dataset_id=None, datastore_id=None, engine_id=None, location=None):
        cls.PROJECT_ID = project_id
        if dataset_id:
            cls.DATASET_ID = dataset_id
        if datastore_id:
            cls.DATASTORE_ID = datastore_id
        if engine_id:
            cls.ENGINE_ID = engine_id
        if location:
            cls.LOCATION = location


def get_access_token():
    """Get OAuth2 access token for API calls"""
    credentials, project = default()
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    return credentials.token


def get_headers():
    """Standard headers for API calls"""
    return {
        "Authorization": f"Bearer {get_access_token()}",
        "Content-Type": "application/json",
        "X-Goog-User-Project": Config.PROJECT_ID,
    }


def get_media_schema(custom_fields=None):
    """Get the standard media schema, dynamically adding custom fields from a rich definition."""
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "datetime_detection": True,
        "geolocation_detection": True,
        "properties": {
            "title": {
                "type": "string",
                "retrievable": True,
                "keyPropertyMapping": "title",
            },
            "uri": {"type": "string", "retrievable": True, "keyPropertyMapping": "uri"},
            "categories": {
                "type": "array",
                "items": {
                    "type": "string",
                    "retrievable": True,
                    "keyPropertyMapping": "category",
                },
            },
            "available_time": {
                "type": "datetime",
                "retrievable": True,
                "keyPropertyMapping": "media_available_time",
            },
            "expire_time": {
                "type": "datetime",
                "retrievable": True,
                "keyPropertyMapping": "media_expire_time",
            },
            "media_type": {
                "type": "string",
                "retrievable": True,
                "keyPropertyMapping": "media_type",
            },
            "original_payload": {"type": "string", "retrievable": True},
        },
    }

    if custom_fields:
        for field_name, field_info in custom_fields.items():
            field_type = field_info.get("type", "string")
            if field_type == "array":
                schema["properties"][field_name] = {
                    "type": "array",
                    "items": {"type": "string"}
                }
            else: # Default to string
                schema["properties"][field_name] = {
                    "type": "string",
                    "retrievable": True
                }
    return schema
