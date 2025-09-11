# Vertex AI Search and Recommendations for Media

A schema-agnostic CLI tool for creating, indexing, and searching media datasets using Google Cloud Vertex AI Search and
Recommendations for Media. Designed for easy integration with any frontend application.

## Features

- **Schema-Agnostic**: Works with any JSON schema - not limited to specific content types
- **Complete Search Solution**: Dataset creation, indexing, search, autocomplete, and recommendations
- **CLI Interface**: Easy-to-use command-line interface for all operations
- **Clean Architecture**: SOLID principles with well-defined interfaces for frontend integration
- **Sample Data Generation**: Automatic generation of realistic sample data based on your schema
- **Flexible Configuration**: Support for both config files and environment variables

## Table of Contents

1. [Environment Setup](#environment-setup)
2. [Data Preparation](#data-preparation)
3. [Search Workflow](#search-workflow)
4. [Autocomplete Workflow](#autocomplete-workflow)
5. [Recommendations Workflow](#recommendations-workflow)
6. [Troubleshooting](#troubleshooting)
7. [CLI Reference](#cli-reference)
8. [Frontend Integration](#frontend-integration)
9. [Architecture](#architecture)

## Environment Setup

### 1. Installation

```bash
# Clone the repository
git clone <repository-url>
cd vertexai-search-reco

# Install dependencies
pip install -r requirements.txt

# Install the package in development mode
pip install -e .
```

### 2. Google Cloud Prerequisites

Before starting, ensure you have:

1. **Google Cloud Project**: Create a new project or use an existing one
2. **Enable Required APIs**:
   ```bash
   gcloud services enable discoveryengine.googleapis.com
   gcloud services enable storage.googleapis.com
   ```
3. **IAM Permissions**: Your account needs:
   - Discovery Engine Admin or Editor role
   - Storage Admin role (for Cloud Storage operations)

### 3. Authentication Setup

Choose one of the following authentication methods:

#### Option A: Application Default Credentials (Recommended)

```bash
# Authenticate with gcloud
gcloud auth application-default login
export VERTEX_PROJECT_ID="your-gcp-project-id"
export VERTEX_LOCATION="global"
```

#### Option B: Service Account Key File

```bash
# Create service account and download JSON key
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
export VERTEX_PROJECT_ID="your-gcp-project-id"
export VERTEX_LOCATION="global"
```

**Note**: API keys are not supported by the Discovery Engine API. You must use OAuth2 credentials.

### 4. Configuration Files

Copy and customize the example configuration files:

```bash
cp examples/.env.example .env
cp examples/config.json my_config.json
```

Edit `.env` with your project details:

```bash
# .env
VERTEX_PROJECT_ID=your-gcp-project-id
VERTEX_LOCATION=global
```

Edit `my_config.json` for advanced configuration:

```json
{
	"vertex_ai": {
		"project_id": "your-project-id",
		"location": "global"
	},
	"schema": {
		"schema_file": "your_schema.json",
		"searchable_fields": ["title", "description"],
		"filterable_fields": ["genre", "language"],
		"facetable_fields": ["genre", "content_type"]
	}
}
```

## Data Preparation

### Option 1: Generate Sample Data

For testing and development, generate realistic sample data:

```bash
# Generate sample data using the example schema
python generate_sample_data.py

# Or generate custom amount
vertex-search --config my_config.json dataset generate --count 1000 --output sample_data.json
```

### Option 2: Bring Your Own Data

Prepare your own JSON data file following these requirements:

1. **JSON Format**: Each record should be a valid JSON object
2. **Required Fields**: Must include `id` and at least one searchable field
3. **Schema Compliance**: Data should match your defined JSON schema

**Example data structure:**

```json
[
	{
		"id": "doc1",
		"title": "Sample Movie",
		"description": "A great movie about...",
		"genre": ["action", "adventure"],
		"language": "en"
	}
]
```

### Option 3: Clean Your Data (Recommended)

Before uploading your data to Vertex AI, it is crucial to ensure it is clean and consistent. Inconsistent data types or
the use of placeholder strings like `"NULL"` can cause the import process to fail or lead to fields being dropped during
indexing.

This project includes a schema-aware script to automatically clean your data file.

**What the script does:**

- Replaces string `"NULL"` values with their correct, type-safe equivalents based on your schema (e.g., `[]` for arrays,
  `null` for other types).
- Fixes fields that should be arrays but are not (e.g., a field with a value of `""` that should be `[]`).

**How to use the script:**

1.  **Prepare your schema and data files.**
2.  Run the following command, providing the paths to your schema, your source data file, and the desired output file.

```bash
python clean_data.py path/to/your_schema.json path/to/your_data.json path/to/cleaned_data.json
```

Always use the cleaned data file for the upload and import steps that follow.

### Custom Schema Setup

To use your own content schema:

1. **Create Schema File**: Define your JSON schema in a `.json` file
2. **Configure Field Mappings**: Update config to specify searchable, filterable, and facetable fields
3. **Validate Data**: Ensure your data matches the schema structure

```json
{
	"$schema": "http://json-schema.org/draft-07/schema#",
	"title": "Your Content Schema",
	"type": "object",
	"required": ["id", "title"],
	"properties": {
		"id": { "type": "string" },
		"title": { "type": "string" },
		"your_custom_fields": { "type": "string" }
	}
}
```

## BigQuery Data Ingestion

The BigQuery data ingestion feature allows you to load your JSON data directly into Google BigQuery tables, which can
then be used as a data source for Vertex AI Search and Recommendations.

### Why Use BigQuery as a Data Source?

- **Scalability**: Handle massive datasets efficiently
- **Query Performance**: Fast SQL-based data analysis and transformation
- **Data Integration**: Combine multiple data sources easily
- **Cost Efficiency**: Pay only for data processed and stored
- **Real-time Updates**: Stream data updates for dynamic content

### Configuration Setup

First, add BigQuery configuration to your config file:

```json
{
	"vertex_ai": {
		"project_id": "your-gcp-project-id",
		"location": "global"
	},
	"bigquery": {
		"project_id": "your-bigquery-project-id",
		"dataset_id": "your_dataset_id",
		"table_id": "your_table_id"
	},
	"schema": {
		"schema_file": "your_schema.json"
	}
}
```

### Loading Data into BigQuery

#### Step 1: Load JSON Data to BigQuery Table

```bash
# Load JSON data file into BigQuery table
vertex-search --config my_config.json bq load dataset_id.table_id path/to/data.json

# Replace existing table data
vertex-search --config my_config.json bq load dataset_id.table_id path/to/data.json --replace
```

**What this does:**

- Automatically creates the dataset if it doesn't exist
- Converts JSON array to JSONL format (required by BigQuery)
- Uses auto-schema detection for table structure
- Loads data with write disposition (append or replace)

#### Step 2: Import from BigQuery to Vertex AI

```bash
# Create data store for BigQuery import
vertex-search --config my_config.json datastore create my-datastore-bq --display-name "BigQuery Data Store" --solution-type SEARCH

# Import data from BigQuery table (without field settings)
vertex-search --config my_config.json datastore import-bq my-datastore-bq --wait --skip-schema-update

# Field settings must be configured manually in Google Cloud Console
```

### BigQuery Data Workflow Example

Complete workflow from JSON file to searchable data store:

```bash
# 1. Load data into BigQuery with Vertex AI compatible schema
vertex-search --config my_config.json bq load media_content.movies path/to/movies.json --replace

# 2. Verify BigQuery table schema (optional)
vertex-search --config my_config.json bq schema media_content.movies

# 3. Create Vertex AI data store
vertex-search --config my_config.json datastore create movies-datastore --display-name "Movies Search Store"

# 4. Import from BigQuery to Vertex AI
vertex-search --config my_config.json datastore import-bq movies-datastore --wait --skip-schema-update

# 5. Configure field settings manually in Google Cloud Console

# 6. Create search engine
vertex-search --config my_config.json search create-engine movies-engine movies-datastore --display-name "Movies Search Engine"

# 7. Test search
vertex-search --config my_config.json search query "action movies" --engine-id movies-engine
```

### BigQuery vs Cloud Storage Import

| Feature                 | BigQuery Import                     | Cloud Storage Import        |
| ----------------------- | ----------------------------------- | --------------------------- |
| **Best for**            | Large datasets, SQL transformations | Simple file-based imports   |
| **Data transformation** | SQL queries, joins, aggregations    | File format conversion only |
| **Scalability**         | Petabyte scale                      | Limited by file size        |
| **Cost**                | Query-based pricing                 | Storage-based pricing       |
| **Real-time updates**   | Streaming inserts                   | Manual file uploads         |
| **Setup complexity**    | Moderate (requires BQ setup)        | Simple                      |

### BigQuery Best Practices

1. **Schema Design**: Use consistent field names and types across tables
2. **Partitioning**: Partition large tables by date or category for better performance
3. **Data Cleaning**: Use SQL to clean and transform data before Vertex AI import
4. **Cost Management**: Monitor query costs and optimize table structures
5. **Security**: Use proper IAM roles and dataset-level permissions

### Advanced BigQuery Integration

You can also use SQL transformations before importing to Vertex AI:

```sql
-- Example: Transform and clean data in BigQuery
SELECT
  id,
  title,
  ARRAY_TO_STRING(genres, ', ') as genre_string,
  CAST(duration_minutes * 60 AS INT64) as duration_seconds,
  STRUCT(
    mpaa_rating,
    viewer_rating
  ) as rating
FROM `your-project.dataset.raw_movies`
WHERE title IS NOT NULL
  AND LENGTH(title) > 0
```

Then update your BigQuery table with the transformed data before importing to Vertex AI.

## Schema Field Settings

The schema field settings feature allows you to automatically configure how Vertex AI Search uses your fields through
configuration files. This eliminates the need for manual field configuration in the Google Cloud Console.

### Overview

Field settings determine how Vertex AI Search processes and returns your data fields:

- **Retrievable**: Fields returned in search results (max 50 fields)
- **Searchable**: Fields included in search queries
- **Facetable**: Fields used for filtering and faceting
- **Completable**: Fields used for autocomplete suggestions
- **Indexable**: Fields included in the search index

### Configuration Format

Add field settings to your config file under the `schema` section:

```json
{
	"schema": {
		"schema_file": "your_schema.json",
		"retrievable_fields": [
			"id",
			"title",
			"description",
			"genre",
			"actors",
			"director",
			"duration_seconds",
			"rating",
			"language",
			"release_date"
		],
		"searchable_fields": ["title", "description", "actors", "director", "keywords", "genre"],
		"filterable_fields": ["genre", "language", "content_type", "rating", "year"],
		"facetable_fields": ["genre", "language", "content_type", "rating.mpaa_rating"],
		"completable_fields": ["title", "actors", "director"]
	}
}
```

### Automatic Field Setting Application

Field settings are automatically applied after successful data imports:

```bash
# Import with automatic field settings application
vertex-search --config my_config.json datastore import-gcs my-datastore gs://bucket/* --wait

# Output:
# ✓ Import completed successfully
# Applying field settings from config...
# ✓ Field settings applied from config
# Set 10 fields as retrievable: ['id', 'title', 'description', ...]
# Set 6 fields as searchable: ['title', 'description', 'actors', ...]
# Set 4 fields as facetable: ['genre', 'language', 'content_type', ...]
```

### Manual Field Settings Update

You can also apply field settings manually to existing data stores:

```bash
# Configure field settings manually in Google Cloud Console
# Navigate to: Vertex AI Search → Data Stores → [your-datastore] → Schema tab
# Check the appropriate boxes for Indexable, Searchable, etc. for each field
```

### Separating Import and Field Settings (Recommended)

It's recommended to separate data import and field settings application for better control:

```bash
# Import without automatic field settings (recommended)
vertex-search --config my_config.json datastore import-gcs my-datastore gs://bucket/* --wait --skip-schema-update

# Output:
# ✓ Import completed successfully
# ℹ Schema update skipped (configure field settings manually in Google Cloud Console)

# Configure field settings manually in Google Cloud Console when ready
```

**Benefits of separating:**

- **Better error handling**: Import success isn't affected by field setting issues
- **Flexible timing**: Apply field settings when convenient
- **Array field support**: Handle complex field types that may not support all annotations
- **Easier troubleshooting**: Debug import vs. schema configuration issues independently

### Field Settings Validation

The system validates your field settings configuration:

- **Field Existence**: Warns about configured fields not found in schema
- **Type Compatibility**: Ensures field types support the requested settings
- **Limits**: Respects Vertex AI limits (e.g., max 50 retrievable fields)
- **Changes Tracking**: Reports which fields were updated

### Best Practices for Field Settings

#### 1. **Retrievable Fields Strategy**

```json
{
	"retrievable_fields": [
		"id", // Always include ID
		"title", // Essential for display
		"description", // Content preview
		"thumbnail_url", // Visual elements
		"genre", // Category information
		"rating", // Quality indicators
		"duration" // User-relevant metadata
	]
}
```

#### 2. **Searchable Fields Optimization**

```json
{
	"searchable_fields": [
		"title", // Primary search target
		"description", // Content-based search
		"actors", // People search
		"keywords", // Tag-based search
		"genre" // Category search
	]
}
```

#### 3. **Facetable Fields for Filtering**

```json
{
	"facetable_fields": [
		"genre", // Category filters
		"language", // Language filters
		"content_type", // Type filters
		"rating.mpaa_rating", // Nested field filtering
		"release_year" // Time-based filtering
	]
}
```

### Environment Variable Configuration

You can also configure field settings via environment variables:

```bash
export SCHEMA_RETRIEVABLE_FIELDS="id,title,description,genre"
export SCHEMA_SEARCHABLE_FIELDS="title,description,actors"
export SCHEMA_FACETABLE_FIELDS="genre,language,content_type"
export SCHEMA_COMPLETABLE_FIELDS="title,actors"
export SCHEMA_INDEXABLE_FIELDS="title,description,keywords"
```

### Field Settings Impact

**Before Field Settings** (default behavior):

- Only basic fields returned in search results
- Limited filtering options
- Poor autocomplete experience
- Missing content in search responses

**After Field Settings** (configured):

- Rich search results with all relevant data
- Dynamic filtering UI with facets
- Intelligent autocomplete suggestions
- Complete field data in API responses

### Troubleshooting Field Settings

#### Common Issues:

1. **Fields Not Appearing in Results**

   ```bash
   # Check if fields are marked as retrievable
   # Solution: Add fields to retrievable_fields in config
   ```

2. **Search Not Finding Content**

   ```bash
   # Check if fields are marked as searchable
   # Solution: Add fields to searchable_fields in config
   ```

3. **Missing Filter Options**

   ```bash
   # Check if fields are marked as facetable
   # Solution: Add fields to facetable_fields in config
   ```

4. **Schema Update Failures**
   ```bash
   # Check field names match your JSON schema exactly
   # Check field types support the requested settings
   # Verify you haven't exceeded limits (max 50 retrievable fields)
   ```

## Search Workflow

The search workflow enables query-based content discovery with advanced filtering and faceting capabilities.

### Step 1: Create Search Infrastructure

```bash
# Create search data store
vertex-search --config my_config.json datastore create my-datastore --display-name "My Search Store" --solution-type SEARCH

# Upload data to Cloud Storage (recommended approach)
vertex-search --config my_config.json datastore upload-gcs examples/sample_data.json your-bucket-name --create-bucket --folder vertex-ai-search

# Import data from Cloud Storage
vertex-search --config my_config.json datastore import-gcs my-datastore gs://your-bucket-name/vertex-ai-search/* --wait

# Create search engine
vertex-search --config my_config.json search create-engine my-engine my-datastore --display-name "My Search Engine" --solution-type SEARCH
```

### Step 2: Verify Setup

```bash
# Verify documents were imported
vertex-search --config my_config.json datastore list my-datastore --count 5
```

### Step 3: Perform Searches

#### Basic Search

```bash
# Simple text search
vertex-search --config my_config.json search query "romantic drama" --engine-id my-engine

# Search specific fields
vertex-search --config my_config.json search query "family story" --engine-id my-engine
```

#### Advanced Search with Filters

```bash
# Search with genre filter
vertex-search --config my_config.json search query "love story" \
  --engine-id my-engine \
  --filters '{"genre": ["romantic_drama", "family_drama"]}'

# Search with multiple filters
vertex-search --config my_config.json search query "thriller" \
  --engine-id my-engine \
  --filters '{"genre": ["thriller"], "language": "en", "duration_seconds": 120}'
```

#### Search with Facets

```bash
# Get faceted results for better filtering UI
vertex-search --config my_config.json search query "drama" \
  --engine-id my-engine \
  --facets "genre,content_type,language" \
  --page-size 20
```

### What to Expect

- **Response Time**: Typically 100-500ms for most queries
- **Relevance**: AI-powered semantic search with keyword matching
- **Faceted Results**: Grouped counts for building filter UIs
- **Pagination**: Support for large result sets
- **Highlighting**: Search term highlighting in results

### Advanced Search Filtering

The search system supports powerful filtering capabilities for different field types. Filters must be applied to fields
that are configured as **indexable** in your schema.

#### String Field Filtering

```bash
# Single value - exact match using ANY() syntax
--filters '{"audio_lang": "hi"}'

# Multiple values - OR logic using ANY() syntax
--filters '{"genre": ["romantic_drama", "family_drama"]}'

# Combine multiple fields - AND logic
--filters '{"genre": ["drama"], "audio_lang": "en"}'
```

#### Datetime Field Filtering

For datetime fields (with `"format": "date-time"` in schema), use comparison operators:

```bash
# Exact date match
--filters '{"c_releasedate": "2025-06-16"}'

# Date range using multiple operators (between dates)
--filters '{"c_releasedate": {">=": "2025-01-01", "<=": "2025-12-31"}}'

# Greater than (after a date)
--filters '{"c_releasedate": {">": "2025-06-15T23:59:59Z"}}'

# Less than (before a date)
--filters '{"c_releasedate": {"<": "2025-07-01"}}'

# Greater than or equal to (on or after)
--filters '{"c_releasedate": {">=": "2025-06-01"}}'

# Less than or equal to (up to and including)
--filters '{"c_releasedate": {"<=": "2025-06-30T23:59:59Z"}}'
```

**Supported Date Formats:**

- `"2025"` - Any time in 2025
- `"2025-06-16"` - Any time on June 16, 2025
- `"2025-06-16T12:00:00Z"` - Specific time with UTC timezone
- `"2025-06-16T12:00:00-07:00"` - Specific time with timezone offset

**Datetime Comparison Operators:**

- `=` : Equal to (exact match)
- `>` : Greater than (after)
- `>=` : Greater than or equal to (on or after)
- `<` : Less than (before)
- `<=` : Less than or equal to (up to and including)

#### Numeric Field Filtering

```bash
# Exact numeric match
--filters '{"duration_seconds": 120}'

# Numeric comparison (if field supports it)
--filters '{"rating": {">": 7.0}}'
```

#### Boolean Field Filtering

```bash
# Boolean values
--filters '{"verified": true}'
--filters '{"premium_content": false}'
```

#### Complex Filter Examples

```bash
# Find Hindi dramas released in 2025
vertex-search --config my_config.json search query "love story" \
  --engine-id my-engine \
  --filters '{"audio_lang": "hi", "c_releasedate": {">=": "2025-01-01", "<": "2026-01-01"}}'

# Find content released between June 1-30, 2025 with multiple genres
vertex-search --config my_config.json search query "drama" \
  --engine-id my-engine \
  --filters '{"c_releasedate": {">=": "2025-06-01", "<": "2025-07-01"}, "genre": ["drama", "romantic_drama"]}'

# Find recent premium content
vertex-search --config my_config.json search query "thriller" \
  --engine-id my-engine \
  --filters '{"c_releasedate": {">": "2025-01-01"}, "premium": true, "audio_lang": "en"}'
```

#### Filter Syntax Reference

```json
{
	"string_field": "exact_value", // String exact match
	"string_array": ["value1", "value2"], // String array (OR logic)
	"datetime_field": "2025-06-16", // Datetime exact match
	"datetime_field": { ">=": "2025-01-01", "<": "2026-01-01" }, // Datetime range
	"numeric_field": 42, // Numeric exact match
	"boolean_field": true, // Boolean value
	"nested.field": "value", // Nested field access
	"nested.array": ["val1", "val2"] // Nested array field
}
```

#### Making Fields Filterable

To enable filtering on a field, it must be configured as **indexable** in your Vertex AI data store schema:

1. **Via Google Cloud Console** (Manual):
   - Navigate to Vertex AI Search and Conversation in Google Cloud Console
   - Go to Data Stores → Select your data store → Schema tab
   - Check the "Indexable" checkbox for fields you want to filter on

2. **Verify Configuration**: Fields must show as "Indexable" in the Vertex AI Search console under your data store's Schema tab to be filterable.

#### Filter Troubleshooting

**Common Issues:**

- `Unsupported field "fieldname" on ":" operator` → Field is not marked as indexable
- `Invalid filter syntax` → Check JSON syntax and operator usage
- No results → Verify data contains values matching your filter criteria

**Solutions:**

- Manually configure fields as "Indexable" in the Google Cloud Console under your data store's Schema tab
- Use exact field names as they appear in your data
- For datetime fields, ensure proper ISO 8601 format

## Autocomplete Workflow

Autocomplete provides intelligent query suggestions to enhance user search experience.

### How It Works

Vertex AI Search analyzes your indexed content to generate contextually relevant suggestions based on:

- Popular search terms from your data
- Semantic understanding of content
- User query patterns
- Content metadata

### Implementation

#### Basic Autocomplete

```bash
# Get suggestions for partial queries
vertex-search --config my_config.json autocomplete suggest "rom" --engine-id my-engine
# Returns: ["romantic", "romantic comedy", "romance", "romantic drama"]

# Domain-specific suggestions
vertex-search --config my_config.json autocomplete suggest "fam" --engine-id my-engine
# Returns: ["family", "family drama", "family comedy"]

# Multi-word query completion
vertex-search --config my_config.json autocomplete suggest "psychological thr" --engine-id my-engine
# Returns: ["psychological thriller"]
```

### Frontend Integration Example

```javascript
// Real-time search suggestions
async function getSuggestions(query) {
	const response = await fetch(`/api/autocomplete?q=${encodeURIComponent(query)}`);
	return await response.json();
}

// Debounced input handler
let searchTimeout;
document.getElementById("search-input").addEventListener("input", (e) => {
	clearTimeout(searchTimeout);
	searchTimeout = setTimeout(() => {
		if (e.target.value.length >= 2) {
			getSuggestions(e.target.value).then((suggestions) => {
				showSuggestions(suggestions);
			});
		}
	}, 300); // 300ms debounce
});
```

### What to Expect

- **Minimum Query Length**: Suggestions work best with 2+ characters
- **Response Time**: Usually under 100ms
- **Relevance**: Context-aware suggestions based on your content
- **Variety**: Mix of exact matches and semantic suggestions
- **Real-time**: Updates as you type

### Best Practices

- Implement debouncing to avoid excessive API calls
- Show suggestions in a dropdown or overlay
- Handle keyboard navigation (up/down arrows, enter)
- Highlight matching characters in suggestions
- Limit displayed suggestions to 5-10 items

## Recommendations Workflow

The recommendations system provides personalized content suggestions based on user behavior patterns.

### How It Works

Vertex AI Recommendations for Media uses machine learning models specifically optimized for media content to analyze:

- **Media Interactions**: Plays, completions, clicks, likes, shares, bookmarks
- **Content Relationships**: Similar content based on metadata and user behavior
- **Collaborative Filtering**: "Users who watched this also enjoyed..."
- **Behavioral Patterns**: Viewing duration, completion rates, time-based preferences

### Step 1: Create Recommendation Infrastructure

```bash
# Create recommendation data store (separate from search)
vertex-search --config my_config.json datastore create my-datastore-reco \
  --display-name "Recommendation Store" \
  --solution-type RECOMMENDATION

# Upload same data to recommendation data store
vertex-search --config my_config.json datastore upload-gcs examples/sample_data.json your-bucket-name --folder vertex-ai-reco
vertex-search --config my_config.json datastore import-gcs my-datastore-reco gs://your-bucket-name/vertex-ai-reco/* --wait

# Create recommendation engine
vertex-search --config my_config.json search create-engine my-engine-reco my-datastore-reco \
  --display-name "Recommendation Engine" \
  --solution-type RECOMMENDATION
```

### Step 2: Record User Interactions

Track user behavior to train the recommendation model:

```bash
# Record when user starts playing content
vertex-search --config my_config.json recommend record \
  --user-id user123 \
  --event-type media-play \
  --document-id drama-001 \
  --data-store-id my-datastore-reco

# Record when user completes watching content (with automatic 100% completion)
vertex-search --config my_config.json recommend record \
  --user-id user123 \
  --event-type media-complete \
  --document-id comedy-005 \
  --data-store-id my-datastore-reco

# Record when user completes watching content (with specific duration and percentage)
vertex-search --config my_config.json recommend record \
  --user-id user123 \
  --event-type media-complete \
  --document-id drama-002 \
  --data-store-id my-datastore-reco \
  --media-progress-duration 120.5 \
  --media-progress-percentage 1.0

# Record partial completion (50% watched)
vertex-search --config my_config.json recommend record \
  --user-id user123 \
  --event-type media-complete \
  --document-id drama-003 \
  --data-store-id my-datastore-reco \
  --media-progress-duration 60.0 \
  --media-progress-percentage 0.5

# Record when user views content details
vertex-search --config my_config.json recommend record \
  --user-id user123 \
  --event-type view-item \
  --document-id premium-content-001 \
  --data-store-id my-datastore-reco

# Record home page view (required for "Recommended for You")
vertex-search --config my_config.json recommend record \
  --user-id user123 \
  --event-type view-home-page \
  --data-store-id my-datastore-reco
```

### Step 3: Get Personalized Recommendations

```bash
# Get recommendations based on viewing history
vertex-search --config my_config.json recommend get \
  --user-id user123 \
  --event-type media-play \
  --document-ids "drama-001,drama-025" \
  --engine-id my-engine-reco \
  --max-results 10

# Get recommendations for new users (cold start)
vertex-search --config my_config.json recommend get \
  --user-id new-user456 \
  --engine-id my-engine-reco \
  --max-results 5
```

### Supported Media Event Types

Based on official Google Cloud documentation, these are the supported event types for Vertex AI Recommendations for
Media:

#### Core Event Types

- `view-item` - Views details of a document/media item
- `view-home-page` - Views home page (required for "Recommended for You" on home page context)
- `search` - Searches the data store
- `media-play` - Clicks play on a media item
- `media-complete` - Stops playing a media item, signifying the end of watching (requires `mediaProgressDuration`)

#### Event Requirements by Recommendation Type

**Others You May Like:**

- Click-through rate: `view-item` OR `media-play`
- Conversion rate: `media-complete` AND (`media-play` OR `view-item`)
- Watch duration: `media-complete` AND (`media-play` OR `view-item`)

**Recommended for You:**

- Click-through rate: `view-item`, `media-play`, and `view-home-page` (for home page)
- Conversion rate: (`media-play` OR `view-item`), `media-complete`, and `view-home-page` (for home page)
- Watch duration: (`media-play` OR `view-item`), `media-complete`, and `view-home-page` (for home page)

**More Like This:**

- Click-through rate: `view-item` OR `media-play`
- Conversion rate: (`media-play` OR `view-item`) AND `media-complete`
- Watch duration: (`media-play` OR `view-item`) AND `media-complete`

**Most Popular:**

- Click-through rate: `view-item` OR `media-play`
- Conversion rate: `media-complete`

### What to Expect

#### Initial Period (0-2 weeks)

- **Cold Start**: Limited personalization for new users
- **Popular Items**: System recommends generally popular content
- **Basic Patterns**: Simple content-based recommendations

#### After Training (2+ weeks with data)

- **Personalized Results**: Tailored to individual user preferences
- **Collaborative Filtering**: "Users like you also enjoyed..."
- **Sequence Awareness**: Recommendations based on viewing patterns
- **Improved Accuracy**: Better match to user tastes

#### Performance Characteristics

- **Response Time**: 200-800ms depending on complexity
- **Freshness**: New interactions reflected within hours
- **Quality Metrics**: Click-through rates typically improve over time
- **Scalability**: Handles millions of users and items

### Best Practices

1. **Track Multiple Event Types**: Combine plays, completions, and item views for richer signals
2. **Handle Cold Start**: Show popular or trending content for new users
3. **Regular Updates**: Record interactions in real-time or batch uploads
4. **Track Completion Rates**: Use both `media-play` and `media-complete` to measure engagement
5. **Monitor Performance**: Track click-through rates and completion rates
6. **Fallbacks**: Have backup content ready when recommendations aren't available

### Example Implementation Patterns

```python
# Record when user starts watching content
def track_media_play(user_id, content_id):
    recommendation_manager.record_user_event(
        event_type="media-play",
        user_pseudo_id=user_id,
        documents=[content_id],
        data_store_id="my-datastore-reco"
    )

# Record when user finishes watching content (automatic 100% completion)
def track_media_complete(user_id, content_id):
    recommendation_manager.record_user_event(
        event_type="media-complete",
        user_pseudo_id=user_id,
        documents=[content_id],
        data_store_id="my-datastore-reco"
    )

# Record when user finishes watching content with specific progress
def track_media_complete_with_progress(user_id, content_id, duration_seconds, percentage):
    recommendation_manager.record_user_event(
        event_type="media-complete",
        user_pseudo_id=user_id,
        documents=[content_id],
        data_store_id="my-datastore-reco",
        media_progress_duration=duration_seconds,
        media_progress_percentage=percentage  # 0.0-1.0 range
    )

# Get personalized recommendations based on viewing history
def get_user_recommendations(user_id, recently_played_ids, count=10):
    user_event = {
        'eventType': 'media-play',
        'userPseudoId': user_id,
        'documents': recently_played_ids
    }

    return recommendation_manager.get_recommendations(
        user_event=user_event,
        engine_id="my-engine-reco",
        max_results=count
    )
```

## Troubleshooting

### Common Issues and Solutions

#### 1. "Project was not passed and could not be determined from the environment"

**Error:**

```
OSError: Project was not passed and could not be determined from the environment.
```

**Solution:** This error occurs when the Google Cloud project ID is not properly configured. Fix by:

1. **Set the environment variable:**

   ```bash
   export VERTEX_PROJECT_ID="your-gcp-project-id"
   ```

2. **Or set it in your config file:**

   ```json
   {
   	"vertex_ai": {
   		"project_id": "your-gcp-project-id"
   	}
   }
   ```

3. **Or use gcloud default project:**
   ```bash
   gcloud config set project your-gcp-project-id
   ```

#### 2. Authentication Issues

**Error:** Permission denied or authentication failures

**Solutions:**

1. **Use Application Default Credentials (Recommended):**

   ```bash
   gcloud auth application-default login
   export VERTEX_PROJECT_ID="your-gcp-project-id"
   ```

2. **Enable required APIs:**

   ```bash
   gcloud services enable discoveryengine.googleapis.com
   gcloud services enable storage.googleapis.com
   ```

3. **Check IAM permissions:** Ensure your account has:
   - Discovery Engine Admin or Editor role
   - Storage Admin role (for Cloud Storage operations)

#### 3. Import/Upload Failures

**Issue:** Data import fails or documents don't appear in search

**Solutions:**

1. **Use Cloud Storage import (recommended):**

   ```bash
   # Upload first
   vertex-search --config my_config.json datastore upload-gcs data.json bucket-name --create-bucket
   # Then import
   vertex-search --config my_config.json datastore import-gcs datastore-id gs://bucket-name/vertex-ai-search/*
   ```

2. **Verify data format:** Ensure JSON data follows the expected schema
3. **Check import status:** Use `--wait` flag to wait for completion
4. **Verify import:** Use `datastore list` to check if documents were imported

#### 4. Search Returns No Results

**Solutions:**

1. **Check data was imported:**

   ```bash
   vertex-search --config my_config.json datastore list datastore-id --count 5
   ```

2. **Verify search engine is properly linked to datastore**
3. **Try broader search terms**
4. **Check if facet filters are too restrictive**

#### 5. Missing Fields in Search Results

**Issue:** Your search query is successful, but the returned documents are missing fields (e.g., `title`, `description`)
that you know are in the source data.

**Cause:** By default, Vertex AI Search **indexes** your data for searching but does not automatically **store** every
field for retrieval in the search response. This is an optimization to keep search results fast and lightweight.

**Solution:** You must manually configure which fields should be returned in the search results by marking them as
"Retrievable" in the Google Cloud Console.

1.  **Navigate to Vertex AI Search:**

    - Open the [Google Cloud Console](https://console.cloud.google.com/).
    - In the navigation menu, go to **Vertex AI Search and Conversation**.

2.  **Select Your Data Store:**

    - Go to the **Data Stores** section.
    - Click on the name of your data store.

3.  **Edit Schema and Enable Retrievability:**

    - You will see a list of the fields that Vertex AI has detected from your data.
    - For each field that you want to be returned in the search results, check the **"Retrievable"** checkbox.
    - It is also a good practice to verify that the "Searchable", "Filterable", and "Facetable" options are set
      correctly for your needs.

4.  **Save and Wait:**
    - Save your changes.
    - **Important:** It may take some time (from a few minutes to over an hour) for the changes to be applied and the
      data to be re-processed. You may need to wait before the fields start appearing in your search results.

#### 6. Configuration File Issues

**Error:** Invalid configuration or missing fields

**Solutions:**

1. **Copy from examples:**

   ```bash
   cp examples/config.json my_config.json
   cp examples/.env.example .env
   ```

2. **Validate JSON syntax:** Use a JSON validator
3. **Check required fields:** Ensure all required configuration is present

#### 6. Schema Validation Errors

**Solutions:**

1. **Validate your schema file:** Ensure it's valid JSON Schema format
2. **Check data matches schema:** Run dataset validation before import
3. **Use the example schema as reference:** See `examples/drama_shorts_schema.json`

#### 7. Recommendations Not Working

**Common Issues:**

1. **No user interactions recorded:**

   ```bash
   # Record some interactions first using media-specific event types
   vertex-search --config my_config.json recommend record --user-id user123 --event-type media-play --document-id doc1 --data-store-id my-datastore-reco
   vertex-search --config my_config.json recommend record --user-id user123 --event-type media-complete --document-id doc1 --data-store-id my-datastore-reco
   ```

2. **Wrong solution type:** Ensure recommendation datastore and engine use `RECOMMENDATION` solution type
3. **Insufficient data:** Recommendations need time and data to train (typically 2+ weeks)
4. **Invalid event types:** Use supported event types like `media-play`, `media-complete`, `view-item`, `search`,
   `view-home-page`
5. **Missing media info:** For `media-complete` events, ensure media progress information is included (automatically
   added if not specified)

#### 8. Autocomplete Not Returning Suggestions

**Solutions:**

1. **Check minimum query length:** Try with 2+ characters
2. **Verify engine is active:** Ensure search engine is properly created and linked
3. **Wait for indexing:** New data may take time to be indexed for autocomplete

### Performance Issues

#### Slow Search Response Times

1. **Check query complexity:** Simplify filters and facets
2. **Optimize data size:** Reduce document size if possible
3. **Use pagination:** Limit result set size with `--page-size`

#### High API Costs

1. **Implement caching:** Cache frequent queries
2. **Use autocomplete wisely:** Implement proper debouncing
3. **Optimize recommendation calls:** Batch user interactions when possible

### Debugging Tips

#### Enable Debug Logging

```python
from vertex_search.utils import setup_logging
logger = setup_logging(level="DEBUG", log_file=Path("debug.log"))
```

#### Check GCP Console

1. **Discovery Engine Console**: View your data stores and engines
2. **Cloud Storage Console**: Verify uploaded files
3. **Cloud Logging**: Check for API errors and warnings

#### Validate Data Format

```bash
# Test with small data file first
vertex-search --config my_config.json dataset create test_data.json --validate-only
```

### Getting Help

If you encounter issues not covered here:

1. Check the [examples/](examples/) directory for working configurations
2. Validate your data against the provided schema
3. Test with the sample data first to isolate issues
4. Check Google Cloud Console for detailed error messages

## CLI Reference

### Dataset Commands

```bash
# Create and validate dataset
vertex-search --config <config_file> dataset create <data_file> [--validate-only]
# OR using environment variables
vertex-search --schema <schema_file> --project-id <project_id> dataset create <data_file>

# Generate sample data
vertex-search --config <config_file> dataset generate [--count 1000] [--output file.json] [--seed 42]
```

### Data Store Commands

```bash
# Create search data store (default)
vertex-search --config <config_file> datastore create <data_store_id> [--display-name "Name"] [--solution-type SEARCH]

# Create recommendation data store
vertex-search --config <config_file> datastore create <data_store_id> [--display-name "Name"] --solution-type RECOMMENDATION

# Upload to Cloud Storage (RECOMMENDED)
vertex-search --config <config_file> datastore upload-gcs <data_file> <bucket_name> [--create-bucket] [--folder path]

# Import from Cloud Storage (RECOMMENDED)
vertex-search --config <config_file> datastore import-gcs <data_store_id> <gcs_uri> [--wait] [--skip-schema-update]

# Import from BigQuery
vertex-search --config <config_file> datastore import-bq <data_store_id> [--wait] [--skip-schema-update]

# List documents to verify import
vertex-search --config <config_file> datastore list <data_store_id> [--count 10]

# Configure field settings manually in Google Cloud Console

# Legacy inline import (NOT RECOMMENDED)
vertex-search --config <config_file> datastore import <data_store_id> <data_file> [--wait] [--skip-schema-update]
```

### BigQuery Commands

```bash
# Load JSON data into BigQuery table
vertex-search --config <config_file> bq load <dataset_id>.<table_id> <data_file> [--replace]
```

### Search Commands

```bash
# Create search engine (default)
vertex-search --config <config_file> search create-engine <engine_id> <data_store_id> [--display-name "Name"] [--solution-type SEARCH]

# Create recommendation engine
vertex-search --config <config_file> search create-engine <engine_id> <data_store_id> [--display-name "Name"] --solution-type RECOMMENDATION

# Search with options
vertex-search --config <config_file> search query <query> [--engine-id <id>] [--filters <json>] [--facets <list>]
```

### Autocomplete Commands

```bash
# Get suggestions
vertex-search --config <config_file> autocomplete suggest <query> [--engine-id <id>]
```

### Recommendation Commands

```bash
# Record user interactions for recommendation training
vertex-search --config <config_file> recommend record --user-id <user_id> --event-type <type> --document-id <id> [--data-store-id <id>] [--media-progress-duration <seconds>] [--media-progress-percentage <0.0-1.0>]

# Get personalized recommendations
vertex-search --config <config_file> recommend get --user-id <id> [--event-type media-play] [--document-ids <id1,id2>] [--engine-id <id>] [--max-results 10]
```

### Command Examples

#### Data Workflow Examples

```bash
# Cloud Storage workflow (recommended)
vertex-search --config my_config.json datastore upload-gcs examples/sample_data.json my-bucket --create-bucket
vertex-search --config my_config.json datastore import-gcs my-datastore gs://my-bucket/vertex-ai-search/* --wait --skip-schema-update
  # Configure field settings manually in Google Cloud Console
vertex-search --config my_config.json datastore list my-datastore --count 5

# BigQuery workflow
vertex-search --config my_config.json bq load dataset.table examples/sample_data.json --replace
vertex-search --config my_config.json datastore import-bq my-datastore --wait --skip-schema-update
  # Configure field settings manually in Google Cloud Console
vertex-search --config my_config.json datastore list my-datastore --count 5
```

#### Recommendation Examples

```bash
# Record user media interactions (builds recommendation model)
vertex-search --config my_config.json recommend record --user-id user123 --event-type media-play --document-id drama-001 --data-store-id my-datastore-reco

# Record completion with automatic 100% progress
vertex-search --config my_config.json recommend record --user-id user123 --event-type media-complete --document-id drama-001 --data-store-id my-datastore-reco

# Record completion with specific progress (2 minutes, 100%)
vertex-search --config my_config.json recommend record --user-id user123 --event-type media-complete --document-id drama-002 --data-store-id my-datastore-reco --media-progress-duration 120.0 --media-progress-percentage 1.0

# Record partial viewing (1 minute, 50%)
vertex-search --config my_config.json recommend record --user-id user123 --event-type media-complete --document-id drama-003 --data-store-id my-datastore-reco --media-progress-duration 60.0 --media-progress-percentage 0.5

# Record user viewing item details
vertex-search --config my_config.json recommend record --user-id user123 --event-type view-item --document-id drama-005 --data-store-id my-datastore-reco

# Record home page view
vertex-search --config my_config.json recommend record --user-id user123 --event-type view-home-page --data-store-id my-datastore-reco

# Get recommendations for a user based on their viewing history
vertex-search --config my_config.json recommend get --user-id user123 --event-type media-play --document-ids "drama-001,drama-005" --engine-id my-engine-reco --max-results 5
```

## Frontend Integration

The CLI is designed for easy integration with web applications through its manager classes.

### Python Integration

```python
from pathlib import Path
from vertex_search.config import AppConfig, ConfigManager
from vertex_search.managers import (
    SearchManager,
    AutocompleteManager,
    RecommendationManager
)

# Load configuration
config = AppConfig.from_env(schema_file=Path("your_schema.json"))
config_manager = ConfigManager(config)

# Create managers
search_manager = SearchManager(config_manager)
autocomplete_manager = AutocompleteManager(config_manager)
recommendation_manager = RecommendationManager(config_manager)

# Perform search with filters and facets
results = search_manager.search(
    query="romantic drama",
    engine_id="your-engine",
    filters={"genre": ["romantic_drama"], "content_type": "custom"},
    facets=["genre", "content_type", "language"],
    page_size=20
)

# Get autocomplete suggestions
suggestions = autocomplete_manager.get_suggestions(
    query="rom",
    engine_id="your-engine"
)

# Get personalized recommendations
user_event = {
    'eventType': 'media-play',
    'userPseudoId': 'user123',
    'documents': ['doc1', 'doc2', 'doc3']
}
recommendations = recommendation_manager.get_recommendations(
    user_event=user_event,
    engine_id="your-engine",
    max_results=10
)

# Record user interactions for better future recommendations
recommendation_manager.record_user_event(
    event_type="media-play",
    user_pseudo_id="user123",
    documents=["doc1"],
    engine_id="your-engine"
)
```

### REST API Wrapper Example

You can easily wrap the managers in a REST API using FastAPI, Flask, or Django:

```python
import json
from typing import Optional
from fastapi import FastAPI, Query
from vertex_search.managers import (
    SearchManager,
    AutocompleteManager,
    RecommendationManager
)

app = FastAPI()

# Initialize managers (do this once at startup)
# search_manager = SearchManager(config_manager)
# autocomplete_manager = AutocompleteManager(config_manager)
# recommendation_manager = RecommendationManager(config_manager)

@app.get("/search")
async def search(
    q: str,
    engine_id: str,
    filters: Optional[str] = None,
    facets: Optional[str] = None,
    page_size: int = 10
):
    """Perform search with optional filters and facets."""
    results = search_manager.search(
        query=q,
        engine_id=engine_id,
        filters=json.loads(filters) if filters else None,
        facets=facets.split(',') if facets else None,
        page_size=page_size
    )
    return results

@app.get("/autocomplete")
async def autocomplete(q: str, engine_id: str):
    """Get autocomplete suggestions."""
    suggestions = autocomplete_manager.get_suggestions(
        query=q,
        engine_id=engine_id
    )
    return {"suggestions": suggestions}

@app.get("/recommendations")
async def get_recommendations(
    user_id: str,
    engine_id: str,
    event_type: str = "media-play",
    document_ids: Optional[str] = None,
    max_results: int = 10
):
    """Get personalized recommendations for a user."""
    user_event = {
        'eventType': event_type,
        'userPseudoId': user_id,
        'documents': document_ids.split(',') if document_ids else []
    }

    recommendations = recommendation_manager.get_recommendations(
        user_event=user_event,
        engine_id=engine_id,
        max_results=max_results
    )
    return {"recommendations": recommendations}

@app.post("/events")
async def record_event(
    user_id: str,
    event_type: str,
    document_id: str,
    engine_id: str
):
    """Record a user interaction event."""
    success = recommendation_manager.record_user_event(
        event_type=event_type,
        user_pseudo_id=user_id,
        documents=[document_id],
        engine_id=engine_id
    )
    return {"success": success}
```

### Frontend JavaScript Integration

```javascript
// Real-time search with autocomplete
class VertexSearch {
	constructor(apiUrl) {
		this.apiUrl = apiUrl;
		this.searchTimeout = null;
	}

	// Debounced autocomplete
	setupAutocomplete(inputElement, suggestionsContainer) {
		inputElement.addEventListener("input", (e) => {
			clearTimeout(this.searchTimeout);
			this.searchTimeout = setTimeout(() => {
				if (e.target.value.length >= 2) {
					this.getSuggestions(e.target.value).then((suggestions) => {
						this.showSuggestions(suggestions, suggestionsContainer);
					});
				}
			}, 300);
		});
	}

	// Get autocomplete suggestions
	async getSuggestions(query) {
		const response = await fetch(`${this.apiUrl}/autocomplete?q=${encodeURIComponent(query)}&engine_id=my-engine`);
		return await response.json();
	}

	// Perform search
	async search(query, filters = {}, facets = []) {
		const params = new URLSearchParams({
			q: query,
			engine_id: "my-engine",
		});

		if (Object.keys(filters).length > 0) {
			params.append("filters", JSON.stringify(filters));
		}

		if (facets.length > 0) {
			params.append("facets", facets.join(","));
		}

		const response = await fetch(`${this.apiUrl}/search?${params}`);
		return await response.json();
	}

	// Get recommendations
	async getRecommendations(userId, eventType = "media-play", documentIds = []) {
		const params = new URLSearchParams({
			user_id: userId,
			engine_id: "my-engine-reco",
			event_type: eventType,
			max_results: "10",
		});

		if (documentIds.length > 0) {
			params.append("document_ids", documentIds.join(","));
		}

		const response = await fetch(`${this.apiUrl}/recommendations?${params}`);
		return await response.json();
	}

	// Track user interactions
	async trackEvent(userId, eventType, documentId) {
		const response = await fetch(`${this.apiUrl}/events`, {
			method: "POST",
			headers: {
				"Content-Type": "application/x-www-form-urlencoded",
			},
			body: new URLSearchParams({
				user_id: userId,
				event_type: eventType,
				document_id: documentId,
				engine_id: "my-engine-reco",
			}),
		});
		return await response.json();
	}
}

// Usage example
const searchClient = new VertexSearch("/api");
searchClient.setupAutocomplete(document.getElementById("search-input"), document.getElementById("suggestions"));
```

## Architecture

### Core Components

The system is built with clean architecture principles and SOLID design:

- **DatasetManager**: Schema-agnostic dataset creation and validation
- **MediaAssetManager**: Vertex AI data store management
- **SearchManager**: Search engine creation and query execution
- **AutocompleteManager**: Autocomplete functionality
- **RecommendationManager**: User event tracking and recommendations
- **DataGenerator**: Automatic sample data generation from schemas

### Design Principles

- **SOLID Principles**: Single responsibility, open/closed, interface segregation
- **DRY (Don't Repeat Yourself)**: Reusable components and utilities
- **Schema Flexibility**: No hardcoded assumptions about data structure
- **Clean Interfaces**: Easy integration with any frontend framework

### System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    Frontend Application                      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐│
│  │   Search UI     │  │  Autocomplete   │  │ Recommendations││
│  └─────────────────┘  └─────────────────┘  └──────────────┘│
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     REST API Layer                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐│
│  │ FastAPI/Flask   │  │   Search Routes │  │   Rec Routes ││
│  └─────────────────┘  └─────────────────┘  └──────────────┘│
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                  Vertex Search CLI Layer                    │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐│
│  │ SearchManager   │  │AutocompleteManager│ │RecommendationManager│
│  └─────────────────┘  └─────────────────┘  └──────────────┘│
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐│
│  │MediaAssetManager│  │ DatasetManager  │  │ConfigManager ││
│  └─────────────────┘  └─────────────────┘  └──────────────┘│
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 Google Cloud Vertex AI                      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐│
│  │  Search Engine  │  │   Data Stores   │  │ Cloud Storage││
│  └─────────────────┘  └─────────────────┘  └──────────────┘│
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

#### Search Flow

1. User enters query in frontend
2. Frontend calls REST API `/search` endpoint
3. API uses SearchManager to query Vertex AI
4. Results returned with facets and filters
5. Frontend displays results with filtering UI

#### Recommendation Flow

1. User interactions tracked via `/events` endpoint
2. RecommendationManager records events to Vertex AI
3. ML models train on user behavior patterns
4. `/recommendations` endpoint provides personalized suggestions
5. Frontend displays recommended content

### Custom Schema Support

The system works with any valid JSON schema through its flexible configuration:

```json
{
	"$schema": "http://json-schema.org/draft-07/schema#",
	"title": "Your Content Schema",
	"type": "object",
	"required": ["id", "title"],
	"properties": {
		"id": { "type": "string" },
		"title": { "type": "string" },
		"your_custom_fields": { "type": "string" }
	}
}
```

Configure field mappings in your config file:

```json
{
	"schema": {
		"schema_file": "your_schema.json",
		"searchable_fields": ["title", "description", "tags"],
		"filterable_fields": ["category", "status", "date"],
		"facetable_fields": ["category", "type"]
	}
}
```

## Example: Drama Shorts Use Case

The `examples/` directory contains a complete example for a drama shorts platform:

1. **Schema**: `drama_shorts_schema.json` - Rich metadata for 1-3 minute drama videos
2. **Sample Data**: Generated using the schema with realistic content
3. **Configuration**: Ready-to-use config files for the drama shorts domain

This demonstrates the system's flexibility while providing a concrete example for a media startup.

## Search vs Recommendations: Key Differences

### 🔍 **Search System** (Query-Based Discovery)

**Purpose**: Help users find specific content based on their search queries **Use Cases**:

- "Find dramas about family relationships"
- "Search for content by specific actors"
- "Filter by genre, rating, duration"

**Architecture**:

- Data Store: `SOLUTION_TYPE_SEARCH`
- Engine: `SOLUTION_TYPE_SEARCH`
- API: Uses SearchServiceClient

### 🎯 **Recommendation System** (Personalized Discovery)

**Purpose**: Suggest relevant content based on user behavior patterns **Use Cases**:

- "Users who watched this also watched..."
- "Recommended for you based on viewing history"
- "Similar content to what you just viewed"

**Architecture**:

- Data Store: `SOLUTION_TYPE_RECOMMENDATION`
- Engine: `SOLUTION_TYPE_RECOMMENDATION`
- API: Uses RecommendationServiceClient

### **Why Separate Systems?**

Vertex AI Search uses different machine learning models optimized for each use case:

- **Search Models**: Optimized for keyword matching, semantic understanding, and relevance ranking
- **Recommendation Models**: Optimized for collaborative filtering, content-based filtering, and user behavior analysis

**Important**: You need **separate data stores and engines** for search and recommendations, even if using the same
underlying data.

## Import Methods Comparison

### Cloud Storage Import (Recommended)

```bash
# Upload to Cloud Storage first
vertex-search --config my_config.json datastore upload-gcs data.json bucket-name --create-bucket
# Then import from Cloud Storage
vertex-search --config my_config.json datastore import-gcs datastore-id gs://bucket-name/vertex-ai-search/*
```

**Pros**: Full console visibility, reliable, easy debugging, source of truth **Best for**: Production use, large
datasets, when you need to see data in console

### Inline Import (Legacy)

```bash
vertex-search --config my_config.json datastore import datastore-id data.json
```

**Pros**: Single command **Cons**: Limited console visibility, less reliable for large datasets **Best for**: Quick
testing only

## Contributing

1. Follow SOLID principles when adding new features
2. Maintain schema agnosticism - don't hardcode specific content types
3. Add comprehensive error handling and logging
4. Include examples and documentation for new features

## License

MIT License - see LICENSE file for details.
