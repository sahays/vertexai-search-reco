# Media Data Store

Vertex AI Search for Media data pipeline - A specialized CLI for processing and managing media content in Google Cloud's
Vertex AI Search.

## Overview

This module provides a complete pipeline for ingesting media content into Vertex AI Search for Media, with support for
schema mapping, data validation, BigQuery integration, and data store management.

## Prerequisites

1. **Google Cloud Authentication**:

   ```bash
   gcloud auth login
   gcloud config set project YOUR-PROJECT-ID
   ```

2. **Installation**:
   ```bash
   pip install -e .
   ```

## Quick Start

1. **Create/Update Configuration**: Copy and modify `examples/customer_media_config.json` for your data structure with
   custom field mappings.

2. **Validate Your Data**:

   ```bash
   media-data-store --config examples/customer_media_config.json validate
   ```

3. **Transform and Upload**:

   ```bash
   media-data-store --config examples/customer_media_config.json transform
   media-data-store --config examples/customer_media_config.json upload-bq my_dataset content_table
   ```

4. **Create Data Store**:

   ```bash
   media-data-store --config examples/customer_media_config.json create my-media-store
   ```

5. **Import to Vertex AI**:
   ```bash
   media-data-store --config examples/customer_media_config.json import-bq my-media-store my_dataset content_table
   ```

## Commands

### `validate`

Validate media data against schema requirements.

```bash
media-data-store --config config.json validate [--data-file custom_data.json]
```

**Options**:

- `--data-file`: Override data file (uses config default if not specified)

**Output**: Validation results with errors, warnings, and statistics

### `transform`

Transform customer data to Google media schema format.

```bash
media-data-store --config config.json transform [--data-file data.json] [--mapping mapping.json]
```

**Options**:

- `--data-file`: Override data file
- `--mapping`: Override mapping file

**Output**: `HHMMSS-transform-transformed_customer_data.json` - Timestamped data ready for BigQuery upload

### `upload-bq`

Upload processed media data to BigQuery.

```bash
media-data-store --config config.json upload-bq DATA_FILE DATASET_ID TABLE_ID
```

**Arguments**:

- `DATA_FILE`: Path to transformed data file (e.g., `143052-transform-transformed_customer_data.json`)
- `DATASET_ID`: BigQuery dataset name
- `TABLE_ID`: BigQuery table name

**Output**: Upload statistics and job information

### `create`

Create a media data store in Vertex AI Search.

```bash
media-data-store --config config.json create DATA_STORE_ID [--display-name "Name"] [--content-config TYPE]
```

**Arguments**:

- `DATA_STORE_ID`: Unique identifier for the data store

**Options**:

- `--display-name`: Human-readable name (defaults to DATA_STORE_ID)
- `--content-config`: `NO_CONTENT` (default for MEDIA), `CONTENT_REQUIRED`, or `PUBLIC_WEBSITE`

**Output**: Data store creation details

### `import-bq`

Import data from BigQuery into the media data store.

```bash
media-data-store --config config.json import-bq DATA_STORE_ID DATASET_ID TABLE_ID
```

**Arguments**:

- `DATA_STORE_ID`: Target data store
- `DATASET_ID`: Source BigQuery dataset
- `TABLE_ID`: Source BigQuery table

**Output**: Import operation details

### `status`

Check the status of an import operation.

```bash
media-data-store --config config.json status OPERATION_NAME
```

**Arguments**:

- `OPERATION_NAME`: Operation name from import command

**Output**: Current status and progress information

### `list`

List all media data stores in the project.

```bash
media-data-store --config config.json list
```

**Output**: Table of all data stores with details

### `info`

Get detailed information about a specific data store.

```bash
media-data-store --config config.json info DATA_STORE_ID
```

**Arguments**:

- `DATA_STORE_ID`: Data store identifier

**Output**: Detailed data store information

## Global Options

All commands support these options:

- `--config`: Configuration file path (required)
- `--log-dir`: Directory for timestamped log files
- `--output-dir`: Directory for processed output files

Example with logging:

```bash
media-data-store --config config.json --log-dir logs --output-dir outputs validate
```

## Configuration File

The configuration file defines your media schema with custom field mappings:

```json
{
	"vertex_ai": {
		"project_id": "your-project-id",
		"location": "global"
	},
	"media_schema": {
		"schema_type": "custom",
		"field_mappings": {
			"title_source_field": "title",
			"uri_source_field": "image",
			"categories_source_field": "genre",
			"available_time_source_field": "release_date",
			"duration_source_field": "episode_count",
			"id_source_field": "id",
			"content_source_field": "desc",
			"language_source_field": "audio_lang",
			"rating_source_field": "age_rating",
			"persons_source_field": "actors",
			"organizations_source_field": "directors"
		},
		"searchable_fields": ["title", "desc", "genre", "actors", "directors"],
		"retrievable_fields": ["id", "title", "desc", "genre", "image"],
		"indexable_fields": ["title", "desc", "genre"],
		"completable_fields": ["title", "actors", "directors"],
		"dynamic_facetable_fields": ["genre", "asset_type", "age_rating"]
	},
	"sample_files": {
		"data_file": "customer_sample_data.json",
		"mapping_file": "customer_media_mapping.json"
	}
}
```

### Key Configuration Sections:

- **field_mappings**: Maps your source fields to Google's required media schema fields
  - `title_source_field`: Your field name that contains the media title
  - `uri_source_field`: Your field name that contains the media URI/URL
  - `categories_source_field`: Your field name that contains genre/categories
  - `available_time_source_field`: Your field name for release/available date
  - `duration_source_field`: Your field name for duration/episode count
- **searchable_fields**: Source field names that should be searchable
- **retrievable_fields**: Source field names returned in search results
- **indexable_fields**: Source field names used for search indexing
- **completable_fields**: Source field names used for autocomplete
- **dynamic_facetable_fields**: Source field names used for dynamic faceting

## Field Mapping File

Define transformations and mappings:

```json
{
	"field_mappings": {
		"original_field": "target_field"
	},
	"value_transformations": {
		"field_name": {
			"type": "to_array",
			"separator": ","
		}
	},
	"validation_rules": {
		"required_fields": ["id", "title"],
		"media_types": ["VIDEO", "AUDIO"],
		"max_title_length": 200
	}
}
```

### Transformation Types:

- `to_array`: Convert comma-separated string to array
- `to_string`: Convert value to string
- `normalize_media_type`: Normalize media type values

## Working with Custom Data Structures

The media data store supports custom data transformation through the `CustomDataTransformer` pipeline:

### Automatic Transformations Applied:

1. **Title Validation**: Ensures title field is present and within length limits
2. **Image to URI Conversion**: Transforms image IDs to full GCS URIs (`gs://media-bucket/images/{id}.jpg`)
3. **Genre to Categories Mapping**: Maps custom genres to Google's supported categories
4. **Release Date to RFC 3339**: Converts various date formats to Google's required format
5. **Episode Count to Duration**: Transforms episode count to duration format (e.g., "5m", "2h30m")
6. **Language Code Normalization**: Maps language codes to BCP 47 format (e.g., "hi" → "hi-IN")
7. **Person Object Creation**: Combines actors and directors into structured person objects
8. **Extended Metadata Extraction**: Extracts additional fields from nested metadata
9. **Custom Document ID**: Adds required `_id` field for custom schema imports (uses source `id` or generates UUID)

### Configuration Setup:

1. **Analyze your data** structure and identify key fields
2. **Update field_mappings** in configuration to map your source fields to Google's required fields
3. **Configure field behaviors** (searchable, retrievable, etc.) using your source field names
4. **Run the pipeline** - transformation happens automatically

Example for customer data:

```bash
# Validate with automatic transformation
media-data-store --config examples/customer_media_config.json validate

# Transform customer data to Google schema
media-data-store --config examples/customer_media_config.json transform
```

### Supported Source Data Formats:

- **Single JSON object**: `{"id": "123", "title": "Movie", ...}`
- **Array of objects**: `[{"id": "123", ...}, {"id": "124", ...}]`
- **Nested metadata**: Handles `extended` field with additional metadata
- **Multiple language fields**: Handles comma-separated language codes
- **Person data**: Supports "Name:Character" format for actors

## Complete Pipeline Example

```bash
# Set up directories
export LOG_DIR="logs/$(date +%Y%m%d)"
export OUTPUT_DIR="outputs/$(date +%Y%m%d)"
CONFIG="examples/customer_media_config.json"

# 1. Validate customer data
media-data-store --config $CONFIG --log-dir $LOG_DIR --output-dir $OUTPUT_DIR validate

# 2. Transform to Google schema (outputs: HHMMSS-transform-transformed_customer_data.json)
media-data-store --config $CONFIG --log-dir $LOG_DIR --output-dir $OUTPUT_DIR transform

# 3. Upload transformed data to BigQuery (use specific timestamped file)
# Note: Replace HHMMSS with actual timestamp from transform output
media-data-store --config $CONFIG upload-bq $OUTPUT_DIR/143052-transform-transformed_customer_data.json media_dataset content_table

# 4. Create Vertex AI data store
media-data-store --config $CONFIG create my-media-store

# 5. Import from BigQuery to Vertex AI
media-data-store --config $CONFIG import-bq my-media-store media_dataset content_table
```

## Error Handling

- **Validation errors**: Check field requirements and data types
- **Authentication errors**: Ensure `gcloud auth login` is completed
- **BigQuery errors**: Verify dataset/table permissions
- **Import failures**: Use `status` command to check operation details

## Output Files

When using `--output-dir`, the following timestamped files are generated:

- `HHMMSS-validate-validation_results.json`: Validation summary
- `HHMMSS-transform-transformed_customer_data.json`: Transformed data
- `HHMMSS-transform-transformation_log.json`: Applied transformations
- `HHMMSS-upload-bigquery_upload_stats.json`: Upload statistics
- `HHMMSS-create-datastore_created.json`: Data store creation details
- `HHMMSS-import-import_started.json`: Import operation details

**Log Files** (when using `--log-dir`):

- `HHMMSS-validate-datastore.log`: Validation command logs
- `HHMMSS-transform-datastore.log`: Transform command logs
- `HHMMSS-upload-datastore.log`: Upload command logs

## Support

For issues or questions:

- Check logs in the specified `--log-dir`
- Review output files for detailed error information
- Ensure all prerequisites are met
