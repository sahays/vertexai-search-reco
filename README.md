# Vertex AI Search for Media

A complete CLI toolkit for building Vertex AI Search applications with media content. Upload data, create search
engines, and import user events for personalized recommendations.

## Scripts

The `v1/scripts/` directory contains scripts to automate the setup and data import processes.

### Configuration Variables

Before running the scripts, you must configure the local variables within each file. These variables define
project-specific settings, resource names, and paths.

**`v1/shell/quick-setup.sh`**

This script has its configuration embedded in the `vais.py` command-line arguments. You will need to edit the script to
change these values.

- `--project-id`: Your Google Cloud project ID (e.g., `search-and-reco`).
- `--location`: The Google Cloud location for your resources (e.g., `asia-south1`).
- `"outputs/customer_sample_data_transformed.json"`: The output path for the transformed data.
- `"vais-dataset"`: The name for the Vertex AI Search dataset.
- `"view-v1"`: A version tag for the dataset.
- `--datastore-id`: The ID for the datastore (e.g., `media-datastore-${UNIQUE_ID}`).
- `--engine-id`: The ID for the search engine (e.g., `media-search-engine-${UNIQUE_ID}`).
- `--id-field`: The field in your JSON data to use as the document ID (e.g., `"zee_id"`).
- `--title-field`: The field to be used as the title (e.g., `"title"`).
- `--categories-field`: The field for categories (e.g., `"genre"`).
- `--media-type-field`: The field for the media type (e.g., `"extended.content_category"`).
- `--available-time-field`: The field for the available time (e.g., `"licensing_from"`).
- `--expire-time-field`: The field for the expiration time (e.g., `"licensing_until"`).
- `CUSTOM_FIELDS_JSON`: A JSON string defining the custom fields for your data.

**`v1/shell/import-events-csv.sh`**

- `PROJECT_ID`: Your Google Cloud project ID (e.g., `"search-and-reco"`).
- `DATASET_ID`: The BigQuery dataset ID (e.g., `"media_dataset"`).
- `LOCATION`: The BigQuery location (e.g., `"US"`).
- `GCS_BUCKET`: The GCS bucket for staging the CSV file (e.g., `"sahays-zbullet-samples"`).
- `SOURCE_CSV`: The path to your source CSV file (e.g., `"$SCRIPT_DIR/../sample_data/customer_userevents-sample.csv"`).
- `TABLE_ID`: The ID for the new BigQuery table (e.g., `"user_events_ingested"`).
- `SCHEMA`: The BigQuery table schema.

**`v1/shell/user-events.sh`**

- `PROJECT_ID`: Your Google Cloud project ID (e.g., `"search-and-reco"`).
- `DATASET_ID`: The BigQuery dataset ID (e.g., `"media_dataset"`).
- `LOCATION`: The BigQuery location (e.g., `"US"`).
- `DATASTORE_ID`: The ID of your Vertex AI Search datastore (e.g., `"media-datastore-1758866993"`).
- `SOURCE_TABLE`: The source table in BigQuery (e.g., `"user_events_ingested"`).
- `VIEW_NAME`: The name of the BigQuery view to be created (e.g., `"user_events_for_vais_view"`).

### `quick-setup.sh`

Complete pipeline setup from JSON to search engine.

```bash
cd v1 && ./shell/quick-setup.sh
```

### `import-events-csv.sh`

Upload user events CSV to BigQuery via GCS.

```bash
cd v1 && ./shell/import-events-csv.sh
```

### `user-events.sh`

Import user events from BigQuery to Vertex AI Search (bulk import).

```bash
cd v1 && ./shell/user-events.sh
```

## Quick Start

```bash
# Setup
pip install -r v1/requirements.txt
gcloud auth application-default login

# Run complete pipeline
cd v1 && ./shell/quick-setup.sh

# Import user events
cd v1 && ./shell/import-events-csv.sh
cd v1 && ./shell/user-events.sh
```

## Data Format

### Media Documents (JSON)

```json
[
	{
		"zee_id": "0-6-4z5769255",
		"title": "Movie Title",
		"genre": ["Comedy", "Drama"],
		"desc": "Movie description"
	}
]
```

### User Events (CSV)

```csv
ViewerID,show_id,StartTimeUnixMs,Browser,ConvivaSessionID
user123,0-6-4z5769255,1758445385041,Chrome,session123
```

## Configuration

Update shell scripts with your project details:

- `v1/shell/import-events-csv.sh` - CSV import settings
- `v1/shell/user-events.sh` - User events pipeline config

## Architecture

- **v1/vais.py** - Main CLI interface
- **v1/\*\_ops.py** - BigQuery, Vertex AI, and search operations
- **v1/shell/** - Automation scripts for data import
- **v1/sample_data/** - Example data files

Built for Vertex AI Search Media vertical with support for recommendations, search, and user behavior tracking.
