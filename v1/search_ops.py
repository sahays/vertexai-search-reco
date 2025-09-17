"""Search and recommendation operations for Vertex AI Search CLI"""

import json
import click
import requests
import time
import os
from datetime import datetime
from config import Config, get_headers

class SearchOperations:
    """Handle all search and recommendation related operations"""

    def __init__(self):
        self.headers = get_headers()

    def search(self, query, datastore_id=None, engine_id=None, filters=None, page_size=10, facets=True):
        """Semantic search with comprehensive filtering support"""
        click.echo(f"🔍 Semantic search for: {query}")

        # Use provided IDs or fall back to config
        ds_id = datastore_id or Config.DATASTORE_ID
        eng_id = engine_id or Config.ENGINE_ID

        click.echo(f"📊 Using datastore: {ds_id}, engine: {eng_id}")

        url = f"https://discoveryengine.googleapis.com/v1/projects/{Config.PROJECT_ID}/locations/global/collections/default_collection/engines/{eng_id}/servingConfigs/default_search:search"

        payload = {
            "query": query,
            "pageSize": page_size,
            "queryExpansionSpec": {"condition": "AUTO"},
            "spellCorrectionSpec": {"mode": "AUTO"}
        }

        if facets:
            payload["facetSpecs"] = [
                {"facetKey": {"key": "categories"}, "limit": 20},
                {"facetKey": {"key": "media_type"}, "limit": 20},
                {"facetKey": {"key": "directors"}, "limit": 10},
                {"facetKey": {"key": "actors"}, "limit": 10},
                {"facetKey": {"key": "primary_language"}, "limit": 10},
                {"facetKey": {"key": "age_rating"}, "limit": 5}
            ]

        if filters:
            payload["filter"] = filters

        try:
            response = requests.post(url, json=payload, headers=self.headers, timeout=30)
            response.raise_for_status()

            results = response.json()

            # Parse original_payload strings to JSON objects in the results
            if 'results' in results:
                for result in results['results']:
                    doc = result.get('document', {})
                    struct_data = doc.get('structData', {})
                    original_payload = struct_data.get('original_payload')

                    if original_payload and isinstance(original_payload, str):
                        try:
                            struct_data['original_payload'] = json.loads(original_payload)
                        except json.JSONDecodeError:
                            # Keep as string if parsing fails
                            pass

            # Print the full server response
            click.echo("🔍 Full Server Response:")
            click.echo(json.dumps(results, indent=2, ensure_ascii=False))

            # Save JSON result to outputs directory (overwrite)
            os.makedirs('outputs', exist_ok=True)
            filename = "outputs/search_results.json"

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)

            click.echo(f"\n💾 Results saved to: {filename}")

            self._display_search_results(results)
            return results

        except requests.exceptions.HTTPError as e:
            click.echo(f"❌ HTTP error: {e}")
            click.echo(f"Response: {e.response.text}")
            return None


    def get_recommendations(self, user_pseudo_id, document_id=None, page_size=10):
        """Get recommendations for a user"""
        click.echo(f"Getting recommendations for user: {user_pseudo_id}")

        url = f"https://discoveryengine.googleapis.com/v1/projects/{Config.PROJECT_ID}/locations/global/collections/default_collection/engines/{Config.ENGINE_ID}-reco/servingConfigs/default_serving_config:recommend"

        payload = {
            "userEvent": {
                "userPseudoId": user_pseudo_id,
                "eventType": "view-item"
            },
            "pageSize": page_size
        }

        if document_id:
            payload["userEvent"]["documents"] = [{"id": document_id}]

        try:
            response = requests.post(url, json=payload, headers=self.headers, timeout=30)
            response.raise_for_status()

            results = response.json()
            self._display_recommendation_results(results)
            return results

        except requests.exceptions.HTTPError as e:
            click.echo(f"❌ HTTP error: {e}")
            click.echo(f"Response: {e.response.text}")
            return None

    def create_search_controls(self, control_type="synonyms", values=None):
        """Create search controls (synonyms, boost/bury, etc.)"""
        if not values:
            click.echo("No values provided for control creation")
            return

        control_id = f"{control_type}-control-{int(time.time())}"
        click.echo(f"Creating {control_type} control: {control_id}")

        url = f"https://discoveryengine.googleapis.com/v1/projects/{Config.PROJECT_ID}/locations/global/collections/default_collection/engines/{Config.ENGINE_ID}/controls"

        if control_type == "synonyms":
            payload = {
                "displayName": f"Synonyms Control",
                "solutionType": "SOLUTION_TYPE_SEARCH",
                "synonymsAction": {
                    "synonyms": values  # List of synonym groups
                }
            }
        elif control_type == "boost":
            payload = {
                "displayName": f"Boost Control",
                "solutionType": "SOLUTION_TYPE_SEARCH",
                "boostAction": {
                    "boost": 2.0,
                    "filter": values  # Filter expression
                }
            }
        elif control_type == "bury":
            payload = {
                "displayName": f"Bury Control",
                "solutionType": "SOLUTION_TYPE_SEARCH",
                "filterAction": {
                    "filter": f"NOT ({values})"  # Negative filter
                }
            }

        try:
            response = requests.post(
                url,
                json=payload,
                headers=self.headers,
                params={"controlId": control_id},
                timeout=30
            )
            response.raise_for_status()
            click.echo(f"✅ {control_type.title()} control created successfully")
            return response.json()

        except requests.exceptions.HTTPError as e:
            click.echo(f"❌ HTTP error: {e}")
            click.echo(f"Response: {e.response.text}")
            return None

    def _display_search_results(self, results):
        """Display search results in a readable format"""
        click.echo("✅ Search Results:")

        if 'results' in results:
            for i, result in enumerate(results['results'][:5], 1):  # Show first 5
                doc = result.get('document', {})
                struct_data = doc.get('structData', {})

                # Parse original_payload if it exists and is a string
                original_payload = struct_data.get('original_payload')
                original_data = {}
                if original_payload and isinstance(original_payload, str):
                    try:
                        original_data = json.loads(original_payload)
                    except json.JSONDecodeError:
                        click.echo(f"   ⚠️ Failed to parse original_payload as JSON")

                click.echo(f"\n{i}. {struct_data.get('title', 'No title')}")
                click.echo(f"   ID: {doc.get('id', 'N/A')}")
                click.echo(f"   Categories: {struct_data.get('categories', [])}")
                click.echo(f"   Media Type: {struct_data.get('media_type', 'N/A')}")
                click.echo(f"   URI: {struct_data.get('uri', 'N/A')}")

                # Show additional fields from original_payload if available
                if original_data:
                    click.echo(f"   Description: {original_data.get('desc', 'N/A')}")
                    click.echo(f"   Directors: {original_data.get('directors', [])}")
                    click.echo(f"   Language: {original_data.get('primary_language', 'N/A')}")
                    click.echo(f"   Rating: {original_data.get('age_rating', 'N/A')}")

        # Display facets
        if 'facets' in results:
            click.echo("\n📊 Facets:")
            for facet in results['facets']:
                key = facet.get('key', 'Unknown')
                click.echo(f"\n{key}:")
                for value in facet.get('values', [])[:3]:  # Show first 3
                    click.echo(f"  - {value.get('value', 'N/A')} ({value.get('count', 0)})")

    def _display_recommendation_results(self, results):
        """Display recommendation results in a readable format"""
        click.echo("✅ Recommendation Results:")

        if 'results' in results:
            for i, result in enumerate(results['results'][:5], 1):  # Show first 5
                doc = result.get('document', {})
                struct_data = doc.get('structData', {})

                click.echo(f"\n{i}. {struct_data.get('title', 'No title')}")
                click.echo(f"   ID: {doc.get('id', 'N/A')}")
                click.echo(f"   Categories: {struct_data.get('categories', [])}")

    def list_search_engines(self):
        """List all search engines in the project"""
        url = f"https://discoveryengine.googleapis.com/v1/projects/{Config.PROJECT_ID}/locations/global/collections/default_collection/engines"

        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()

            results = response.json()
            click.echo("📋 Available Search Engines:")

            for engine in results.get('engines', []):
                click.echo(f"- {engine.get('name', 'Unknown')}")
                click.echo(f"  Display Name: {engine.get('displayName', 'N/A')}")
                click.echo(f"  Solution Type: {engine.get('solutionType', 'N/A')}")
                click.echo()

        except requests.exceptions.HTTPError as e:
            click.echo(f"❌ HTTP error: {e}")
            return None