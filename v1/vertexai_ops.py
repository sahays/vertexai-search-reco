"""Vertex AI Search operations for CLI"""

import json
import click
import requests
from config import Config, get_headers, get_media_schema


class VertexAIOperations:
    """Handle all Vertex AI Search related operations"""

    def __init__(self):
        self.headers = get_headers()

    def create_datastore(self, custom_fields=None):
        """Create Vertex AI Search datastore"""
        click.echo(f"Creating datastore {Config.DATASTORE_ID}")

        url = f"https://discoveryengine.googleapis.com/v1/projects/{Config.PROJECT_ID}/locations/global/collections/default_collection/dataStores"

        payload = {
            "displayName": "Media Search Datastore",
            "industryVertical": "MEDIA",
            "solutionTypes": ["SOLUTION_TYPE_RECOMMENDATION", "SOLUTION_TYPE_SEARCH"],
            "startingSchema": {
                "name": "media-schema",
                "jsonSchema": json.dumps(get_media_schema(custom_fields)),
            },
        }

        try:
            response = requests.post(
                url,
                json=payload,
                headers=self.headers,
                params={"dataStoreId": Config.DATASTORE_ID},
                timeout=60,
            )
            response.raise_for_status()
            click.echo("✅ Datastore creation operation initiated successfully")
            click.echo(json.dumps(response.json(), indent=2))
            return True
        except requests.exceptions.HTTPError as e:
            click.echo(f"❌ HTTP error: {e}")
            click.echo(f"Response: {e.response.text}")
            return False

    def import_documents(self, view_name):
        """Import documents from BigQuery view to datastore"""
        click.echo(f"Importing documents from view {view_name}")

        url = f"https://discoveryengine.googleapis.com/v1/projects/{Config.PROJECT_ID}/locations/global/collections/default_collection/dataStores/{Config.DATASTORE_ID}/branches/0/documents:import"

        payload = {
            "bigquerySource": {
                "projectId": Config.PROJECT_ID,
                "datasetId": Config.DATASET_ID,
                "tableId": view_name,
            }
        }

        try:
            response = requests.post(
                url, json=payload, headers=self.headers, timeout=60
            )
            response.raise_for_status()
            click.echo("✅ Document import operation initiated successfully")
            click.echo(json.dumps(response.json(), indent=2))
            return True
        except requests.exceptions.HTTPError as e:
            click.echo(f"❌ HTTP error: {e}")
            click.echo(f"Response: {e.response.text}")
            return False

    def create_search_engine(self):
        """Create search engine"""
        click.echo(f"Creating search engine {Config.ENGINE_ID}")

        url = f"https://discoveryengine.googleapis.com/v1/projects/{Config.PROJECT_ID}/locations/global/collections/default_collection/engines"

        payload = {
            "displayName": "Media Search Engine",
            "dataStoreIds": [Config.DATASTORE_ID],
            "solutionType": "SOLUTION_TYPE_SEARCH",
            "industryVertical": "MEDIA",
        }

        try:
            response = requests.post(
                url,
                json=payload,
                headers=self.headers,
                params={"engineId": Config.ENGINE_ID},
                timeout=60,
            )
            response.raise_for_status()
            click.echo("✅ Search engine creation operation initiated successfully")
            click.echo(json.dumps(response.json(), indent=2))
            return True
        except requests.exceptions.HTTPError as e:
            click.echo(f"❌ HTTP error: {e}")
            click.echo(f"Response: {e.response.text}")
            return False

    def import_user_events(self):
        """Import user events to datastore"""
        click.echo("Importing user events")

        url = f"https://discoveryengine.googleapis.com/v1alpha/projects/{Config.PROJECT_ID}/locations/global/collections/default_collection/dataStores/{Config.DATASTORE_ID}:importUserEvents"

        payload = {
            "bigquerySource": {
                "projectId": Config.PROJECT_ID,
                "datasetId": Config.DATASET_ID,
                "tableId": "user_events_view",
            }
        }

        try:
            response = requests.post(
                url, json=payload, headers=self.headers, timeout=60
            )
            response.raise_for_status()
            click.echo("✅ User events import operation initiated successfully")
            click.echo(json.dumps(response.json(), indent=2))
            return True
        except requests.exceptions.HTTPError as e:
            click.echo(f"❌ HTTP error: {e}")
            click.echo(f"Response: {e.response.text}")
            return False

    def create_recommendation_engine(self):
        """Create recommendation engine (optional)"""
        click.echo(f"Creating recommendation engine {Config.ENGINE_ID}-reco")

        url = f"https://discoveryengine.googleapis.com/v1/projects/{Config.PROJECT_ID}/locations/global/collections/default_collection/engines"

        payload = {
            "displayName": "Media Recommendation Engine",
            "dataStoreIds": [Config.DATASTORE_ID],
            "solutionType": "SOLUTION_TYPE_RECOMMENDATION",
            "mediaRecommendationEngineConfig": {
                "optimizationObjective": "ctr",
                "type": "others-you-may-like",
            },
            "industryVertical": "MEDIA",
        }

        try:
            response = requests.post(
                url,
                json=payload,
                headers=self.headers,
                params={"engineId": f"{Config.ENGINE_ID}-reco"},
                timeout=60,
            )
            response.raise_for_status()
            click.echo(
                "✅ Recommendation engine creation operation initiated successfully"
            )
            click.echo(json.dumps(response.json(), indent=2))
            return True
        except requests.exceptions.HTTPError as e:
            click.echo(f"❌ HTTP error: {e}")
            click.echo(f"Response: {e.response.text}")
            return False

    def check_operation_status(self, operation_name):
        """Check the status of a long-running operation"""
        url = f"https://discoveryengine.googleapis.com/v1/{operation_name}"

        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()

            result = response.json()
            if result.get("done", False):
                click.echo("✅ Operation completed successfully")
                if "error" in result:
                    click.echo(f"❌ Operation failed: {result['error']}")
                    return False
                return True
            else:
                click.echo("⏳ Operation still in progress...")
                return None
        except requests.exceptions.HTTPError as e:
            click.echo(f"❌ HTTP error: {e}")
            return False
