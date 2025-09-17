#!/usr/bin/env python3

import click
import json
import os
from config import Config
from bigquery_ops import BigQueryOperations
from vertexai_ops import VertexAIOperations
from search_ops import SearchOperations
from data_transformer import DataTransformer

@click.group()
@click.option('--project-id', required=True, help='Google Cloud Project ID')
@click.option('--dataset-id', default='media_dataset', help='BigQuery dataset ID')
@click.option('--datastore-id', default='media-datastore', help='Vertex AI datastore ID')
@click.option('--engine-id', default='media-search-engine', help='Search engine ID')
@click.option('--location', default='asia-south1', help='GCP region/location (default: asia-south1)')
def cli(project_id, dataset_id, datastore_id, engine_id, location):
    """Vertex AI Search for Media CLI - Modular Version"""
    Config.set_config(project_id, dataset_id, datastore_id, location)
    Config.ENGINE_ID = engine_id

# BigQuery Commands
@cli.group()
def bigquery():
    """BigQuery data operations"""
    pass

@bigquery.command('upload-data')
@click.argument('data_file')
@click.argument('table_id')
def upload_data(data_file, table_id):
    """Upload data file (CSV or JSON) to BigQuery table"""
    bq_ops = BigQueryOperations()
    bq_ops.upload_data_to_table(data_file, table_id)

@bigquery.command('upload-csv')
@click.argument('csv_file')
@click.argument('table_id')
def upload_csv(csv_file, table_id):
    """Upload CSV file to BigQuery table"""
    bq_ops = BigQueryOperations()
    bq_ops.upload_csv_to_table(csv_file, table_id)

@bigquery.command('upload-json')
@click.argument('json_file')
@click.argument('table_id')
def upload_json(json_file, table_id):
    """Upload JSON/JSONL file to BigQuery table"""
    bq_ops = BigQueryOperations()
    bq_ops.upload_json_to_table(json_file, table_id)

@bigquery.command('create-transform-view')
@click.argument('raw_table')
@click.argument('view_name')
@click.option('--id-field', required=True, help='Field to use as document ID (required)')
@click.option('--title-field', required=True, help='Field to use as title (required)')
@click.option('--uri-field', help='Field to use as URI (optional)')
@click.option('--categories-field', help='Field to use as categories array')
@click.option('--available-time-field', help='Field to use as available_time (TIMESTAMP)')
@click.option('--expire-time-field', help='Field to use as expire_time (TIMESTAMP)')
@click.option('--media-type-field', help='Field to use as media_type')
@click.option('--custom-fields', help='JSON string of custom field mappings')
@click.option('--no-original-payload', is_flag=True, help='Disable original_payload field')
def create_transform_view(raw_table, view_name, id_field, title_field, uri_field, categories_field,
                         available_time_field, expire_time_field, media_type_field, custom_fields, no_original_payload):
    """Create BigQuery view to transform data to Vertex AI format"""
    field_mappings = {
        'id_field': id_field,
        'title_field': title_field,
        'uri_field': uri_field,
        'categories_field': categories_field,
        'available_time_field': available_time_field,
        'expire_time_field': expire_time_field,
        'media_type_field': media_type_field,
        'custom_fields': json.loads(custom_fields) if custom_fields else {},
        'include_original_payload': not no_original_payload
    }

    bq_ops = BigQueryOperations()
    bq_ops.create_documents_transform_view(raw_table, view_name, field_mappings)

@bigquery.command('upload-events')
@click.argument('csv_file')
def upload_events(csv_file):
    """Upload user events CSV to BigQuery"""
    bq_ops = BigQueryOperations()
    bq_ops.upload_events_csv(csv_file)

@bigquery.command('create-events-view')
def create_events_view():
    """Create events view for Vertex AI format"""
    bq_ops = BigQueryOperations()
    bq_ops.create_events_transform_view()

# Vertex AI Commands
@cli.group()
def vertexai():
    """Vertex AI Search operations"""
    pass

@vertexai.command('create-datastore')
def create_datastore():
    """Create Vertex AI Search datastore"""
    vai_ops = VertexAIOperations()
    vai_ops.create_datastore()

@vertexai.command('import-documents')
@click.argument('view_name')
def import_documents(view_name):
    """Import documents from BigQuery view to datastore"""
    vai_ops = VertexAIOperations()
    vai_ops.import_documents(view_name)

@vertexai.command('create-search-engine')
def create_search_engine():
    """Create search engine"""
    vai_ops = VertexAIOperations()
    vai_ops.create_search_engine()

@vertexai.command('create-recommendation-engine')
def create_recommendation_engine():
    """Create recommendation engine"""
    vai_ops = VertexAIOperations()
    vai_ops.create_recommendation_engine()

@vertexai.command('import-events')
def import_events():
    """Import user events to datastore"""
    vai_ops = VertexAIOperations()
    vai_ops.import_user_events()

@vertexai.command('check-status')
@click.argument('operation_name')
def check_status(operation_name):
    """Check the status of a long-running operation"""
    vai_ops = VertexAIOperations()
    vai_ops.check_operation_status(operation_name)

# Search Commands
@cli.group()
def search():
    """Search and recommendation operations"""
    pass

@search.command('query')
@click.argument('query_text')
@click.option('--filters', help='Search filters (e.g., "categories: ANY(\'action\')")')
@click.option('--page-size', default=10, help='Number of results to return')
@click.option('--no-facets', is_flag=True, help='Disable facets in results')
def search_query(query_text, filters, page_size, no_facets):
    """Search the media collection"""
    search_ops = SearchOperations()
    search_ops.search(query_text, filters=filters, page_size=page_size, facets=not no_facets)

@search.command('semantic')
@click.argument('query_text')
@click.option('--page-size', default=10, help='Number of results to return')
def semantic_search(query_text, page_size):
    """Perform semantic search using natural language"""
    search_ops = SearchOperations()
    search_ops.semantic_search(query_text, page_size=page_size)

@search.command('recommend')
@click.argument('user_pseudo_id')
@click.option('--document-id', help='Base document for recommendations')
@click.option('--page-size', default=10, help='Number of results to return')
def get_recommendations(user_pseudo_id, document_id, page_size):
    """Get recommendations for a user"""
    search_ops = SearchOperations()
    search_ops.get_recommendations(user_pseudo_id, document_id, page_size)

@search.command('create-synonyms')
@click.argument('synonyms_json')
def create_synonyms(synonyms_json):
    """Create synonyms control (JSON format: [["word1", "word2"], ["word3", "word4"]])"""
    search_ops = SearchOperations()
    synonyms = json.loads(synonyms_json)
    search_ops.create_search_controls("synonyms", synonyms)

@search.command('create-boost')
@click.argument('filter_expression')
def create_boost(filter_expression):
    """Create boost control with filter expression"""
    search_ops = SearchOperations()
    search_ops.create_search_controls("boost", filter_expression)

@search.command('list-engines')
def list_engines():
    """List all search engines in the project"""
    search_ops = SearchOperations()
    search_ops.list_search_engines()

# Data transformation command
@cli.command('transform-data')
@click.argument('input_file')
@click.argument('output_file')
@click.option('--custom-fields', required=True, help='JSON string of custom field mappings')
@click.option('--validate', is_flag=True, help='Validate transformed data')
def transform_data_cmd(input_file, output_file, custom_fields, validate):
    """Transform data file with proper type handling and cleaning"""
    try:
        field_mappings = {
            'custom_fields': json.loads(custom_fields) if custom_fields else {}
        }
    except json.JSONDecodeError as e:
        click.echo(f"❌ Error parsing custom fields JSON: {e}")
        return

    transformer = DataTransformer()

    # Ensure outputs directory exists
    outputs_dir = 'outputs'
    os.makedirs(outputs_dir, exist_ok=True)

    # If output_file is just a filename, put it in outputs folder
    if not os.path.dirname(output_file):
        output_file = os.path.join(outputs_dir, output_file)

    # Transform the data
    if not transformer.transform_json_file(input_file, output_file, field_mappings):
        click.echo("❌ Data transformation failed")
        return

    # Validate if requested
    if validate:
        transformer.validate_transformed_data(output_file, field_mappings)

    click.echo(f"✅ Data transformation completed: {output_file}")

# Convenience Commands (combining multiple operations)
@cli.command('quick-setup')
@click.argument('data_file')
@click.argument('table_id')
@click.argument('view_name')
@click.option('--id-field', required=True, help='Field to use as document ID (required)')
@click.option('--title-field', required=True, help='Field to use as title (required)')
@click.option('--uri-field', help='Field to use as URI')
@click.option('--categories-field', help='Field to use as categories array')
@click.option('--available-time-field', help='Field to use as available_time (TIMESTAMP)')
@click.option('--expire-time-field', help='Field to use as expire_time (TIMESTAMP)')
@click.option('--media-type-field', help='Field to use as media_type')
@click.option('--custom-fields', help='JSON string of custom field mappings')
@click.option('--no-original-payload', is_flag=True, help='Disable original_payload field')
@click.option('--datastore-id', help='Override the default datastore ID')
@click.option('--engine-id', help='Override the default search engine ID')
@click.option('--skip-transform', is_flag=True, help='Skip data transformation step')
def quick_setup(data_file, table_id, view_name, id_field, title_field, uri_field, categories_field,
               available_time_field, expire_time_field, media_type_field, custom_fields, no_original_payload,
               datastore_id, engine_id, skip_transform):
    """Quick setup: Transform data, upload to BigQuery, create view, create datastore, and import documents"""
    # Override default datastore and engine IDs if provided
    if datastore_id:
        Config.DATASTORE_ID = datastore_id
    if engine_id:
        Config.ENGINE_ID = engine_id

    click.echo("🚀 Starting quick setup...")
    click.echo(f"Using Datastore ID: {Config.DATASTORE_ID}")
    click.echo(f"Using Engine ID: {Config.ENGINE_ID}")

    field_mappings = {
        'id_field': id_field,
        'title_field': title_field,
        'uri_field': uri_field,
        'categories_field': categories_field,
        'available_time_field': available_time_field,
        'expire_time_field': expire_time_field,
        'media_type_field': media_type_field,
        'custom_fields': json.loads(custom_fields) if custom_fields else {},
        'include_original_payload': not no_original_payload
    }

    # Step 0: Transform data (optional)
    transformed_data_file = data_file
    if not skip_transform and custom_fields:
        click.echo("\n🔄 Step 0: Transforming data...")
        transformer = DataTransformer()
        outputs_dir = 'outputs'
        os.makedirs(outputs_dir, exist_ok=True)

        # Create transformed filename
        base_name = os.path.splitext(os.path.basename(data_file))[0]
        transformed_data_file = os.path.join(outputs_dir, f"{base_name}_transformed.json")

        if not transformer.transform_json_file(data_file, transformed_data_file, field_mappings):
            click.echo("❌ Step 0 failed. Aborting quick setup.")
            return

        # Validate transformed data
        transformer.validate_transformed_data(transformed_data_file, field_mappings)

    # Step 1: Upload data
    click.echo("\n📁 Step 1: Uploading data to BigQuery...")
    bq_ops = BigQueryOperations()
    if not bq_ops.upload_data_to_table(transformed_data_file, table_id):
        click.echo("❌ Step 1 failed. Aborting quick setup.")
        return

    # Step 2: Create transform view
    click.echo("\n🔄 Step 2: Creating transform view...")
    if not bq_ops.create_documents_transform_view(table_id, view_name, field_mappings):
        click.echo("❌ Step 2 failed. Aborting quick setup.")
        return

    # Step 3: Create datastore
    click.echo("\n🏗️  Step 3: Creating Vertex AI datastore...")
    vai_ops = VertexAIOperations()
    custom_fields_dict = field_mappings.get('custom_fields', {})
    if not vai_ops.create_datastore(custom_fields=custom_fields_dict):
        click.echo("❌ Step 3 failed. Aborting quick setup.")
        return

    # Step 4: Import documents
    click.echo("\n📥 Step 4: Importing documents...")
    if not vai_ops.import_documents(view_name):
        click.echo("❌ Step 4 failed. Aborting quick setup.")
        return

    # Step 5: Create search engine
    click.echo("\n🔍 Step 5: Creating search engine...")
    if not vai_ops.create_search_engine():
        click.echo("❌ Step 5 failed. Aborting quick setup.")
        return

    click.echo("\n✅ Quick setup completed! Wait for operations to finish, then you can start searching.")
    click.echo(f"\nTry: python vais.py --project-id {Config.PROJECT_ID} search query 'your search term'")

@cli.command('full-demo')
@click.argument('data_csv')
@click.argument('events_csv')
@click.option('--id-field', required=True)
@click.option('--title-field', required=True)
@click.option('--category-field')
def full_demo(data_csv, events_csv, id_field, title_field, category_field):
    """Full demo: Setup everything including events and recommendations"""
    click.echo("🎬 Starting full demo setup...")

    # Data setup
    bq_ops = BigQueryOperations()
    vai_ops = VertexAIOperations()

    # Upload and transform documents
    bq_ops.upload_csv_to_table(data_csv, "media_raw")
    field_mappings = {
        'id_field': id_field,
        'title_field': title_field,
        'category_field': category_field,
        'date_field': None,
        'custom_fields': {}
    }
    bq_ops.create_documents_transform_view("media_raw", "media_view", field_mappings)

    # Upload and transform events
    bq_ops.upload_events_csv(events_csv)
    bq_ops.create_events_transform_view()

    # Create Vertex AI resources
    vai_ops.create_datastore()
    vai_ops.import_documents("media_view")
    vai_ops.create_search_engine()
    vai_ops.create_recommendation_engine()
    vai_ops.import_user_events()

    click.echo("\n🎉 Full demo setup completed!")

if __name__ == '__main__':
    cli()