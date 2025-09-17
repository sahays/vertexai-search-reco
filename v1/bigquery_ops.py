"""BigQuery operations for Vertex AI Search CLI"""

import json
import os
import click
from google.cloud import bigquery
from config import Config

class BigQueryOperations:
    """Handle all BigQuery related operations"""

    def __init__(self):
        self.client = bigquery.Client(project=Config.PROJECT_ID)

    def ensure_dataset_exists(self):
        """Create dataset if it doesn't exist"""
        dataset_ref = self.client.dataset(Config.DATASET_ID)
        try:
            self.client.get_dataset(dataset_ref)
        except:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-south1"
            self.client.create_dataset(dataset)
            click.echo(f"Created dataset {Config.DATASET_ID}")

    def upload_csv_to_table(self, csv_file, table_id):
        """Upload CSV file to BigQuery table"""
        click.echo(f"Uploading {csv_file} to {Config.PROJECT_ID}.{Config.DATASET_ID}.{table_id}")
        try:
            self.ensure_dataset_exists()

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )

            with open(csv_file, "rb") as source_file:
                job = self.client.load_table_from_file(
                    source_file,
                    f"{Config.PROJECT_ID}.{Config.DATASET_ID}.{table_id}",
                    job_config=job_config
                )

            job.result()
            click.echo(f"✅ Data uploaded successfully to {table_id}")
            return True
        except Exception as e:
            click.echo(f"❌ Error uploading CSV: {e}")
            return False

    def upload_json_to_table(self, json_file, table_id):
        """Upload JSON file to BigQuery table - handles both JSON arrays and NDJSON"""
        click.echo(f"Uploading {json_file} to {Config.PROJECT_ID}.{Config.DATASET_ID}.{table_id}")
        try:
            self.ensure_dataset_exists()

            # Read and convert JSON array to NDJSON if needed
            with open(json_file, 'r') as f:
                data = json.load(f)

            def clean_for_bigquery(obj):
                """Clean JSON object for BigQuery compatibility"""
                if isinstance(obj, dict):
                    cleaned = {}
                    for key, value in obj.items():
                        # Replace special characters in field names
                        clean_key = key.replace('-', '_').replace(' ', '_').replace('.', '_')
                        cleaned[clean_key] = clean_for_bigquery(value)
                    return cleaned
                elif isinstance(obj, list):
                    # Ensure arrays are properly formatted
                    return [clean_for_bigquery(item) for item in obj]
                else:
                    return obj

            # Create JSONL file in outputs folder
            outputs_dir = 'outputs'
            os.makedirs(outputs_dir, exist_ok=True)
            jsonl_file_path = os.path.join(outputs_dir, f"{table_id}.jsonl")

            click.echo(f"💾 Saving JSONL file to {jsonl_file_path}")

            with open(jsonl_file_path, 'w') as jsonl_file:
                if isinstance(data, list):
                    # Convert JSON array to NDJSON
                    for item in data:
                        cleaned_item = clean_for_bigquery(item)
                        json.dump(cleaned_item, jsonl_file)
                        jsonl_file.write('\n')
                else:
                    # Single JSON object
                    cleaned_item = clean_for_bigquery(data)
                    json.dump(cleaned_item, jsonl_file)
                    jsonl_file.write('\n')

            # Upload the JSONL file
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                ignore_unknown_values=True,  # Ignore unknown fields
                max_bad_records=10  # Allow some bad records
            )

            with open(jsonl_file_path, "rb") as source_file:
                job = self.client.load_table_from_file(
                    source_file,
                    f"{Config.PROJECT_ID}.{Config.DATASET_ID}.{table_id}",
                    job_config=job_config
                )

            job.result()
            click.echo(f"✅ JSON data uploaded successfully to {table_id}")
            click.echo(f"📄 JSONL file saved at: {jsonl_file_path}")
            return True
        except Exception as e:
            click.echo(f"❌ Error uploading JSON: {e}")
            return False

    def upload_data_to_table(self, data_file, table_id):
        """Upload data file (CSV or JSON) to BigQuery table - auto-detects format"""
        if data_file.lower().endswith('.csv'):
            return self.upload_csv_to_table(data_file, table_id)
        elif data_file.lower().endswith('.json') or data_file.lower().endswith('.jsonl'):
            return self.upload_json_to_table(data_file, table_id)
        else:
            # Try to detect based on content
            try:
                with open(data_file, 'r') as f:
                    first_line = f.readline().strip()
                    if first_line.startswith('{'):
                        click.echo("Detected JSON format")
                        return self.upload_json_to_table(data_file, table_id)
                    else:
                        click.echo("Assuming CSV format")
                        return self.upload_csv_to_table(data_file, table_id)
            except Exception as e:
                click.echo(f"❌ Error detecting file format: {e}")
                return False

    def create_documents_transform_view(self, raw_table, view_name, field_mappings):
        """Create BigQuery view to transform data to Vertex AI format using declarative field definitions."""
        click.echo(f"Creating transform view {view_name} from {raw_table}")
        try:
            def escape_field_name(field_name, add_alias=True):
                """Escape field names for BigQuery SQL."""
                if not field_name: return None
                escaped_parts = '.'.join([f"`{part}`" for part in field_name.split('.')])
                return f"t.{escaped_parts}" if add_alias else escaped_parts

            def safe_array_transform(source_field_sql):
                """Transform field to array - pass through if already array, filtering out NULL strings."""
                return f"ARRAY(SELECT element FROM UNNEST(COALESCE({source_field_sql}, [])) AS element WHERE element != 'NULL')"

            def safe_string_transform(source_field_sql):
                """Transform field to string handling nulls and empty strings."""
                return f"CASE WHEN CAST({source_field_sql} AS STRING) = 'NULL' THEN NULL ELSE CAST({source_field_sql} AS STRING) END"

            # Build field references from field_mappings
            id_field = escape_field_name(field_mappings['id_field'])
            title_field = escape_field_name(field_mappings['title_field'])
            categories_field = escape_field_name(field_mappings.get('categories_field')) or "CAST(NULL AS ARRAY<STRING>)"

            available_time_expr = escape_field_name(field_mappings.get('available_time_field')) or "TIMESTAMP('2024-01-01T00:00:00Z')"
            expire_time_expr = escape_field_name(field_mappings.get('expire_time_field')) or "TIMESTAMP('2030-01-01T00:00:00Z')"

            # Create media_type mapping with proper content_category to media_type conversion
            raw_media_type_field = escape_field_name(field_mappings.get('media_type_field')) or "'video'"
            media_type_field = f"""CASE
                WHEN UPPER({raw_media_type_field}) LIKE '%MOVIE%' THEN 'movie'
                WHEN UPPER({raw_media_type_field}) LIKE '%MICRO DRAMA%' THEN 'show'
                WHEN UPPER({raw_media_type_field}) LIKE '%DRAMA%' THEN 'show'
                WHEN UPPER({raw_media_type_field}) LIKE '%SERIES%' THEN 'tv-series'
                WHEN UPPER({raw_media_type_field}) LIKE '%EPISODE%' THEN 'episode'
                WHEN UPPER({raw_media_type_field}) LIKE '%ORIGINAL%' THEN 'show'
                WHEN UPPER({raw_media_type_field}) LIKE '%CONCERT%' THEN 'concert'
                WHEN UPPER({raw_media_type_field}) LIKE '%EVENT%' THEN 'event'
                WHEN UPPER({raw_media_type_field}) LIKE '%LIVE%' THEN 'live-event'
                WHEN UPPER({raw_media_type_field}) LIKE '%BROADCAST%' THEN 'broadcast'
                WHEN UPPER({raw_media_type_field}) LIKE '%GAME%' THEN 'video-game'
                WHEN UPPER({raw_media_type_field}) LIKE '%CLIP%' THEN 'clip'
                WHEN UPPER({raw_media_type_field}) LIKE '%VLOG%' THEN 'vlog'
                WHEN UPPER({raw_media_type_field}) LIKE '%AUDIO%' THEN 'audio'
                WHEN UPPER({raw_media_type_field}) LIKE '%MUSIC%' THEN 'music'
                WHEN UPPER({raw_media_type_field}) LIKE '%ALBUM%' THEN 'album'
                WHEN UPPER({raw_media_type_field}) LIKE '%NEWS%' THEN 'news'
                WHEN UPPER({raw_media_type_field}) LIKE '%RADIO%' THEN 'radio'
                WHEN UPPER({raw_media_type_field}) LIKE '%PODCAST%' THEN 'podcast'
                WHEN UPPER({raw_media_type_field}) LIKE '%BOOK%' THEN 'book'
                WHEN UPPER({raw_media_type_field}) LIKE '%SPORT%' THEN 'sports-game'
                ELSE 'video'
            END"""

            uri_field = escape_field_name(field_mappings.get('uri_field')) or f"CONCAT('https://media.example.com/', CAST({id_field} AS STRING))"

            struct_fields = [
                f"{safe_string_transform(title_field)} AS title",
                f"{categories_field} AS categories",
                f"{uri_field} AS uri",
                f"FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', {available_time_expr}) AS available_time",
                f"FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', {expire_time_expr}) AS expire_time",
                f"({media_type_field}) AS media_type"
            ]

            # Add custom fields using declarative definitions (no schema introspection)
            custom_fields = field_mappings.get('custom_fields', {})
            for key, field_info in custom_fields.items():
                source_field_name = field_info.get("name")
                field_type = field_info.get("type", "string")

                source_field_sql = escape_field_name(source_field_name)
                alias = escape_field_name(key, add_alias=False)

                if source_field_sql:
                    if field_type == "array":
                        # Handle arrays with proper empty string and null handling
                        transformed_field = safe_array_transform(source_field_sql)
                        struct_fields.append(f"{transformed_field} AS {alias}")
                    else: # Handle string and other types
                        # Handle strings with proper empty string and null handling
                        transformed_field = safe_string_transform(source_field_sql)
                        struct_fields.append(f"{transformed_field} AS {alias}")

            if field_mappings.get('include_original_payload', True):
                struct_fields.append("TO_JSON_STRING(t) AS original_payload")

            json_data_expr = f"TO_JSON_STRING(STRUCT({', '.join(struct_fields)}))"

            sql_query = f"""
CREATE OR REPLACE VIEW `{Config.PROJECT_ID}.{Config.DATASET_ID}.{view_name}` AS
SELECT
  CAST({id_field} AS STRING) AS id,
  "default_schema" AS schemaId,
  NULL AS parentDocumentId,
  {json_data_expr} AS jsonData
FROM `{Config.PROJECT_ID}.{Config.DATASET_ID}.{raw_table}` AS t
WHERE {id_field} IS NOT NULL
"""

            outputs_dir = 'outputs'
            os.makedirs(outputs_dir, exist_ok=True)
            sql_file_path = os.path.join(outputs_dir, f"{view_name}_create_view.sql")
            with open(sql_file_path, 'w') as sql_file:
                sql_file.write(sql_query)

            click.echo("📝 Generated SQL Query:")
            click.echo("=" * 50)
            click.echo(sql_query)
            click.echo("=" * 50)
            click.echo(f"💾 SQL saved to: {sql_file_path}")

            query_job = self.client.query(sql_query)
            query_job.result()
            click.echo(f"✅ Transform view {view_name} created successfully")
            return True
        except Exception as e:
            click.echo(f"❌ Error creating transform view: {e}")
            return False

    def upload_events_csv(self, csv_file, append=False):
        """Upload user events CSV to BigQuery"""
        mode = "append" if append else "replace"
        click.echo(f"Uploading events from {csv_file} (mode: {mode})")

        self.ensure_dataset_exists()

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND if append else bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        with open(csv_file, "rb") as source_file:
            job = self.client.load_table_from_file(
                source_file,
                f"{Config.PROJECT_ID}.{Config.DATASET_ID}.user_events",
                job_config=job_config
            )

        job.result()
        click.echo(f"✅ Events uploaded successfully ({mode} mode)")

    def upload_events_csv_to_table(self, csv_file, table_name, append=False):
        """Upload user events CSV to a specific BigQuery table"""
        mode = "append" if append else "replace"
        click.echo(f"Uploading events from {csv_file} to {table_name} (mode: {mode})")

        self.ensure_dataset_exists()

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND if append else bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        with open(csv_file, "rb") as source_file:
            job = self.client.load_table_from_file(
                source_file,
                f"{Config.PROJECT_ID}.{Config.DATASET_ID}.{table_name}",
                job_config=job_config
            )

        job.result()
        click.echo(f"✅ Events uploaded successfully to {table_name} ({mode} mode)")

    def create_events_transform_view_from_table(self, source_table, view_name):
        """Create events view for Vertex AI format from a specific table"""
        click.echo(f"Creating user events view {view_name} from table {source_table}")

        sql_query = f"""
CREATE OR REPLACE VIEW `{Config.PROJECT_ID}.{Config.DATASET_ID}.{view_name}` AS
SELECT
  CAST(user_pseudo_id AS STRING) AS userPseudoId,
  event_type AS eventType,
  FORMAT_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ", event_timestamp) AS eventTime,
  [STRUCT(
    CAST(document_id AS STRING) AS id,
    CAST(NULL AS INT64) AS quantity
  )] AS documents,
  STRUCT(
    STRUCT([location] AS text) AS location
  ) AS attributes
FROM `{Config.PROJECT_ID}.{Config.DATASET_ID}.{source_table}`
WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
"""

        query_job = self.client.query(sql_query)
        query_job.result()
        click.echo(f"✅ Events view {view_name} created successfully")

        # Verify the view has data
        verify_query = f"SELECT COUNT(*) as event_count FROM `{Config.PROJECT_ID}.{Config.DATASET_ID}.{view_name}`"
        verify_job = self.client.query(verify_query)
        result = verify_job.result()
        count = list(result)[0].event_count
        click.echo(f"📊 Events view contains {count} events")

    def create_events_transform_view(self):
        """Create events view for Vertex AI format"""
        click.echo("Creating user events view")

        sql_query = f"""
CREATE OR REPLACE VIEW `{Config.PROJECT_ID}.{Config.DATASET_ID}.user_events_view` AS
SELECT
  CAST(user_pseudo_id AS STRING) AS userPseudoId,
  event_type AS eventType,
  FORMAT_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ", event_timestamp) AS eventTime,
  [STRUCT(
    CAST(document_id AS STRING) AS id,
    CAST(NULL AS INT64) AS quantity,
    CAST(document_id AS STRING) AS document_descriptor
  )] AS documents,
  STRUCT(
    STRUCT([location] AS text) AS location
  ) AS attributes
FROM `{Config.PROJECT_ID}.{Config.DATASET_ID}.user_events`
WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
"""

        query_job = self.client.query(sql_query)
        query_job.result()
        click.echo("✅ Events view created successfully")

        # Verify the view has data
        verify_query = f"SELECT COUNT(*) as event_count FROM `{Config.PROJECT_ID}.{Config.DATASET_ID}.user_events_view`"
        verify_job = self.client.query(verify_query)
        result = verify_job.result()
        count = list(result)[0].event_count
        click.echo(f"📊 Events view contains {count} events")