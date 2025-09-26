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

        url = f"https://discoveryengine.googleapis.com/v1/projects/{Config.PROJECT_ID}/locations/global/dataStores/{Config.DATASTORE_ID}/userEvents:import"

        payload = {
            "bigquerySource": {
                "projectId": Config.PROJECT_ID,
                "datasetId": Config.DATASET_ID,
                "tableId": "user_events_view",
                "dataSchema": "user_event"
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

    def import_user_events_from_view(self, view_name):
        """Import user events from a specific BigQuery view"""
        click.echo(f"Importing user events from view {view_name}")

        url = f"https://discoveryengine.googleapis.com/v1/projects/{Config.PROJECT_ID}/locations/global/dataStores/{Config.DATASTORE_ID}/userEvents:import"

        payload = {
            "bigquerySource": {
                "projectId": Config.PROJECT_ID,
                "datasetId": Config.DATASET_ID,
                "tableId": view_name,
                "dataSchema": "user_event"
            }
        }

        try:
            response = requests.post(
                url, json=payload, headers=self.headers, timeout=60
            )
            response.raise_for_status()
            click.echo(f"✅ User events import from {view_name} initiated successfully")
            click.echo(json.dumps(response.json(), indent=2))

            # Extract operation name for status checking
            operation_name = response.json().get('name', '')
            if operation_name:
                click.echo(f"📋 Operation name: {operation_name}")
                click.echo("💡 Use 'python vais.py check-operation --operation-name <name>' to check status")

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


def bulk_import_user_events_from_bigquery(project_id: str, location: str, data_store_id: str, bigquery_table: str):
    """
    Bulk imports user events directly from BigQuery using the import_user_events API.
    This is much more efficient than processing events individually.

    Args:
        project_id: The GCP Project ID.
        location: The location of the VAIS datastore.
        data_store_id: The ID of the target VAIS datastore.
        bigquery_table: Full BigQuery table/view name (project.dataset.table)

    Returns:
        Operation object for tracking the import progress
    """
    from google.cloud.discoveryengine import UserEventServiceClient
    from google.cloud.discoveryengine_v1beta.types import ImportUserEventsRequest, BigQuerySource

    client = UserEventServiceClient()
    parent = client.data_store_path(
        project=project_id, location=location, data_store=data_store_id
    )

    # Create BigQuery source configuration
    bigquery_source = BigQuerySource()
    bigquery_source.project_id = project_id
    bigquery_source.dataset_id = bigquery_table.split('.')[1]  # Extract dataset from project.dataset.table
    bigquery_source.table_id = bigquery_table.split('.')[2]    # Extract table from project.dataset.table

    # Create the import request
    request = ImportUserEventsRequest()
    request.parent = parent
    request.bigquery_source = bigquery_source

    # Start the bulk import operation
    operation = client.import_user_events(request=request)

    click.echo(f"✅ Bulk import operation started")
    click.echo("📊 This operation runs asynchronously. Monitor progress in the Google Cloud Console.")
    click.echo(f"🔗 Operation: {operation}")

    return operation


def write_user_events(project_id: str, location: str, data_store_id: str, user_events: list[dict]):
    """
    Writes a batch of formatted user events to the VAIS API (individual events).
    Note: For bulk operations, use bulk_import_user_events_from_bigquery() instead.

    Args:
        project_id: The GCP Project ID.
        location: The location of the VAIS datastore.
        data_store_id: The ID of the target VAIS datastore.
        user_events: A list of user event dictionaries.
    """
    from google.cloud.discoveryengine import UserEventServiceClient, UserEvent, WriteUserEventRequest

    client = UserEventServiceClient()
    parent = client.data_store_path(
        project=project_id, location=location, data_store=data_store_id
    )

    for user_event_dict in user_events:
        user_event = UserEvent.from_json(json.dumps(user_event_dict))
        request = WriteUserEventRequest(parent=parent, user_event=user_event)
        client.write_user_event(request=request)
