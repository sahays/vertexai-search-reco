# Vertex AI Search for Media CLI - Modular Version

A modular CLI tool to create a Vertex AI Search for Media application following the Google Cloud notebook pattern.
Upload your JSON/CSV data with field mappings, automatically create BigQuery datasets and tables, transform data to VAIS
Media format, and set up complete search and recommendation engines.

## Project Structure

```
├── vais.py               # Main CLI interface with command groups
├── config.py             # Configuration and utilities
├── bigquery_ops.py       # BigQuery operations class
├── vertexai_ops.py       # Vertex AI Search operations class
├── search_ops.py         # Search and recommendation operations class
├── requirements.txt      # Dependencies
└── README.md            # This file
```

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Authenticate with Google Cloud:

```bash
gcloud auth application-default login
```

## Command Structure

The CLI is organized into logical groups:

- **`bigquery`** - Data upload and transformation operations
- **`vertexai`** - Vertex AI datastore and engine operations
- **`search`** - Search and recommendation operations
- **Convenience commands** - Combined operations for quick setup

## Quick Start

The simplest way to get started is with the `quick-setup` command that handles the entire pipeline:

```bash
# Upload JSON data with field mappings and create complete VAIS Media setup
python vais.py --project-id YOUR_PROJECT quick-setup customer_data.json raw_media media_view \
  --id-field "id" \
  --title-field "title" \
  --categories-field "genre" \
  --media-type-field "asset_type" \
  --custom-fields '{"description": "desc", "duration": "runtime", "director": "directors"}'
```

This single command will:

1. 📁 Upload your JSON/CSV data to BigQuery (auto-detects format)
2. 🏗️ Create a new dataset and table if needed
3. 🔄 Transform data to Vertex AI Search for Media schema
4. 📊 Create a BigQuery view conforming to VAIS Media format
5. 🚀 Set up Vertex AI datastore and search engine
6. 📥 Import all documents for search and recommendations

## Usage Examples

### JSON Input with Field Mappings

For JSON data like this:

```json
[
	{
		"id": "movie-123",
		"title": "Action Hero",
		"desc": "An exciting action movie",
		"genre": ["Action", "Adventure"],
		"asset_type": "movie",
		"directors": ["John Director"],
		"runtime": 120
	}
]
```

Map your fields to VAIS Media schema:

```bash
python vais.py --project-id YOUR_PROJECT quick-setup movies.json media_raw media_view \
  --id-field "id" \
  --title-field "title" \
  --categories-field "genre" \
  --media-type-field "asset_type" \
  --custom-fields '{"description": "desc", "duration": "runtime", "director": "directors"}'
```

### CSV Input Example

For CSV files, the CLI automatically detects the format:

```bash
python vais.py --project-id YOUR_PROJECT quick-setup movies.csv media_raw media_view \
  --id-field "movieId" \
  --title-field "title" \
  --categories-field "genres"
```

## Individual Commands

If you prefer step-by-step control, you can use individual commands:

### BigQuery Operations

```bash
# Upload data (auto-detects JSON/CSV format)
python vais.py --project-id YOUR_PROJECT bigquery upload-data movies.json raw_movies

# Create transformation view with field mappings
python vais.py --project-id YOUR_PROJECT bigquery create-transform-view raw_movies movies_view \
  --id-field "id" \
  --title-field "title" \
  --categories-field "genre" \
  --custom-fields '{"description": "desc", "duration": "runtime"}'
```

### Vertex AI Operations

```bash
# Create datastore
python vais.py --project-id YOUR_PROJECT vertexai create-datastore

# Import documents from BigQuery view
python vais.py --project-id YOUR_PROJECT vertexai import-documents movies_view

# Create search engine
python vais.py --project-id YOUR_PROJECT vertexai create-search-engine

# Check operation status
python vais.py --project-id YOUR_PROJECT vertexai check-status OPERATION_NAME
```

### Search Operations

```bash
# Basic search with filters
python vais.py --project-id YOUR_PROJECT search query "action movies" --filters "categories: ANY('Action')"

# Semantic search
python vais.py --project-id YOUR_PROJECT search semantic "funny romantic comedies"

# Get recommendations
python vais.py --project-id YOUR_PROJECT search recommend user123

# Create search controls
python vais.py --project-id YOUR_PROJECT search create-synonyms '[["movie", "film"], ["action", "adventure"]]'
```

## Field Mapping Reference

### Required VAIS Media Fields

- `--id-field`: Unique document identifier (required)
- `--title-field`: Document title/name (required)

### Optional VAIS Media Fields

- `--categories-field`: Categories/genres array
- `--available-time-field`: When content becomes available (TIMESTAMP)
- `--expire-time-field`: When content expires (TIMESTAMP)
- `--media-type-field`: Type of media (movie, show, episode, etc.)
- `--uri-field`: URI/URL for the content

### Custom Fields

Use `--custom-fields` to map any additional data:

```bash
--custom-fields '{"description": "desc", "duration": "runtime", "rating": "imdb_score", "director": "directors"}'
```

## Data Format Examples

### JSON Input Format

```json
[
	{
		"id": "0-6-4z5476990",
		"title": "Rickshaw Romeo",
		"desc": "Cupid strikes when Aryan, a billionaire, accidentally becomes Diya's rickshaw driver. Can love win?",
		"genre": ["Comedy", "Romance"],
		"asset_type": 6,
		"directors": ["Samay Bhattacharya"],
		"runtime": 120
	}
]
```

### CSV Input Format

```csv
id,title,genre,asset_type,desc,runtime
1,Action Hero,"Action,Adventure",movie,An exciting movie,120
2,Comedy Gold,"Comedy",movie,A funny movie,95
```

### Search Examples

```bash
# Search with filters
python vais.py --project-id YOUR_PROJECT search query "comedy" --filters "categories: ANY('Comedy')"

# Semantic search
python vais.py --project-id YOUR_PROJECT search semantic "funny romantic movies"

# Get recommendations
python vais.py --project-id YOUR_PROJECT search recommend user123

# Advanced filters
python vais.py --project-id YOUR_PROJECT search query "action" --filters "categories: ANY('Action') AND media_type: ANY('movie')"
```

## Configuration Options

Set default values using CLI options:

```bash
python vais.py --project-id YOUR_PROJECT \
               --dataset-id custom_dataset \
               --datastore-id custom-datastore \
               --engine-id custom-search-engine \
               quick-setup data.json raw_table view_name [options...]
```

## Common Workflows

### 1. Basic Setup (JSON to Search)

```bash
# Single command to go from JSON file to working search
python vais.py --project-id YOUR_PROJECT quick-setup customer_data.json raw_media media_view \
  --id-field "id" --title-field "title" --categories-field "genre"
```

### 2. Custom Schema Mapping

```bash
# Map complex nested JSON to VAIS Media schema
python vais.py --project-id YOUR_PROJECT quick-setup movies.json media_raw media_view \
  --id-field "movieId" \
  --title-field "original_title" \
  --categories-field "genres" \
  --media-type-field "content_type" \
  --custom-fields '{"description": "short_desc", "director": "directors", "duration": "runtime_minutes"}'
```

### 3. Monitor Progress

```bash
# Check status of long-running operations
python vais.py --project-id YOUR_PROJECT vertexai check-status projects/YOUR_PROJECT/locations/global/operations/OPERATION_ID
```

## Notes

- ⚡ **Auto-detection**: Automatically detects JSON vs CSV format
- 🔄 **Async Operations**: All Vertex AI operations are asynchronous - monitor with `check-status`
- 📊 **BigQuery Integration**: Creates optimized BigQuery views conforming to VAIS Media schema
- 🔍 **Immediate Search**: Search functionality available as soon as documents are imported
- 📈 **Original Payload**: Complete original JSON preserved unless `--no-original-payload` is used
- 🎯 **Field Mapping**: Flexible field mapping from your schema to VAIS Media requirements

## Troubleshooting

- **Operation timeouts**: Use `vertexai check-status` to monitor long-running operations
- **Schema errors**: Ensure required fields (`--id-field`, `--title-field`) are present in your data
- **Search not working**: Wait for document import to complete before searching
- **Permission errors**: Ensure proper Google Cloud authentication with `gcloud auth application-default login`
