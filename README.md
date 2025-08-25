# Vertex AI Search for Media

A schema-agnostic CLI tool for creating, indexing, and searching media datasets using Google Cloud Vertex AI Search.
Designed for easy integration with any frontend application.

## Features

- **Schema-Agnostic**: Works with any JSON schema - not limited to specific content types
- **Complete Search Solution**: Dataset creation, indexing, search, autocomplete, and recommendations
- **CLI Interface**: Easy-to-use command-line interface for all operations
- **Clean Architecture**: SOLID principles with well-defined interfaces for frontend integration
- **Sample Data Generation**: Automatic generation of realistic sample data based on your schema
- **Flexible Configuration**: Support for both config files and environment variables

## Quick Start

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

### 2. Setup Google Cloud

1. Create a Google Cloud Project
2. Enable the Discovery Engine API
3. Choose an authentication method:

**Option A: Application Default Credentials (Recommended)**

```bash
# Use gcloud CLI authentication
gcloud auth application-default login
export VERTEX_PROJECT_ID="your-gcp-project-id"
```

**Option B: Service Account Key File**

```bash
# Create a service account and download the JSON key
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
export VERTEX_PROJECT_ID="your-gcp-project-id"
```

**Note**: API keys are not supported by the Discovery Engine API. You must use OAuth2 credentials (gcloud auth or
service account).

### 3. Configuration

Copy the example configuration:

```bash
cp examples/.env.example .env
cp examples/config.json my_config.json
```

Edit the configuration files with your project details:

```bash
# .env - Using Application Default Credentials (recommended)
VERTEX_PROJECT_ID=your-gcp-project-id
VERTEX_LOCATION=global
```

**Important**: Make sure to:

1. Enable the Discovery Engine API: `gcloud services enable discoveryengine.googleapis.com`
2. Set up proper IAM permissions (Discovery Engine Admin/Editor role)
3. Run `gcloud auth application-default login` for authentication

### 4. Generate Sample Data

```bash
# Generate 1000 sample records using the example schema
python generate_sample_data.py
```

### 5. Create Data Store and Search Engine

```bash
# Create data store
vertex-search --config my_config.json datastore create my-datastore --display-name "My Media Store"

# Create search engine
vertex-search --config my_config.json search create-engine my-engine my-datastore --display-name "My Search Engine"
```

### 6. Upload and Import Data (Cloud Storage Approach - Recommended)

```bash
# Step 1: Upload data to Cloud Storage
vertex-search --config my_config.json datastore upload-gcs examples/sample_data.json your-bucket-name --create-bucket --folder vertex-ai-search

# Step 2: Import from Cloud Storage to Vertex AI
vertex-search --config my_config.json datastore import-gcs my-datastore gs://your-bucket-name/vertex-ai-search/* --wait

# Step 3: Verify documents were imported
vertex-search --config my_config.json datastore list my-datastore --count 5
```

**Why Cloud Storage?**
- ✅ **Full Console Visibility**: See your data in GCP Console
- ✅ **Better Reliability**: More stable than inline import
- ✅ **Easy Debugging**: Files visible in Cloud Storage
- ✅ **Source of Truth**: Files remain for re-import if needed
- ✅ **Correct Document Format**: Uses proper Vertex AI Document schema with `structData`

### 7. Search Your Content

```bash
# Basic search
vertex-search --config my_config.json search query "romantic drama" --engine-id my-engine

# Search with filters and facets
vertex-search --config my_config.json search query "love story" --engine-id my-engine --filters '{"genre": ["romantic_drama"]}' --facets "genre,content_type"

# Get autocomplete suggestions
vertex-search --config my_config.json autocomplete suggest "rom" --engine-id my-engine

# Get recommendations
vertex-search --config my_config.json recommend get --user-id user123 --event-type view --document-ids "doc1,doc2" --engine-id my-engine
```

## Import Methods Comparison

### Cloud Storage Import (Recommended)
```bash
# Upload to Cloud Storage first
vertex-search --config my_config.json datastore upload-gcs data.json bucket-name --create-bucket
# Then import from Cloud Storage
vertex-search --config my_config.json datastore import-gcs datastore-id gs://bucket-name/vertex-ai-search/*
```
**Pros**: Full console visibility, reliable, easy debugging, source of truth
**Best for**: Production use, large datasets, when you need to see data in console

### Inline Import (Legacy)
```bash
vertex-search --config my_config.json datastore import datastore-id data.json
```
**Pros**: Single command
**Cons**: Limited console visibility, less reliable for large datasets
**Best for**: Quick testing only

## Architecture

### Core Components

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

## Custom Schema Usage

The system works with any valid JSON schema. Here's how to use your own schema:

### 1. Create Your Schema

```json
{
  \"$schema\": \"http://json-schema.org/draft-07/schema#\",
  \"title\": \"Your Content Schema\",
  \"type\": \"object\",
  \"required\": [\"id\", \"title\"],
  \"properties\": {
    \"id\": {\"type\": \"string\"},
    \"title\": {\"type\": \"string\"},
    \"your_custom_fields\": {\"type\": \"string\"}
  }
}
```

### 2. Configure Field Mappings

Update your configuration to specify which fields to use for search operations:

```json
{
  \"schema\": {
    \"schema_file\": \"your_schema.json\",
    \"id_field\": \"id\",
    \"title_field\": \"title\",
    \"searchable_fields\": [\"title\", \"description\", \"tags\"],
    \"filterable_fields\": [\"category\", \"status\", \"date\"],
    \"facetable_fields\": [\"category\", \"type\"]
  }
}
```

### 3. Generate Sample Data

```bash
# The data generator will automatically create realistic data based on your schema
vertex-search --config my_config.json dataset generate --count 1000 --output my_sample_data.json
```

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
# Create data store
vertex-search --config <config_file> datastore create <data_store_id> [--display-name "Name"]

# Upload to Cloud Storage (RECOMMENDED)
vertex-search --config <config_file> datastore upload-gcs <data_file> <bucket_name> [--create-bucket] [--folder path]

# Import from Cloud Storage (RECOMMENDED) 
vertex-search --config <config_file> datastore import-gcs <data_store_id> <gcs_uri> [--wait]

# List documents to verify import
vertex-search --config <config_file> datastore list <data_store_id> [--count 10]

# Legacy inline import (NOT RECOMMENDED)
vertex-search --config <config_file> datastore import <data_store_id> <data_file> [--wait]
```

**Examples:**
```bash
# Recommended workflow
vertex-search --config my_config.json datastore upload-gcs examples/sample_data.json my-bucket --create-bucket
vertex-search --config my_config.json datastore import-gcs my-datastore gs://my-bucket/vertex-ai-search/* --wait
vertex-search --config my_config.json datastore list my-datastore --count 5
```

### Search Commands

```bash
# Create search engine
vertex-search --config <config_file> search create-engine <engine_id> <data_store_id> [--display-name "Name"]

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
# Get recommendations
vertex-search --config <config_file> recommend get --user-id <id> [--event-type view] [--document-ids <id1,id2>] [--engine-id <id>] [--max-results 10]
```

**Examples:**
```bash
# Get recommendations for a user who viewed specific documents
vertex-search --config my_config.json recommend get --user-id user123 --event-type view --document-ids "doc1,doc2,doc3" --engine-id my-engine --max-results 5

# Get recommendations based on purchase event
vertex-search --config my_config.json recommend get --user-id user456 --event-type purchase --document-ids "product-123" --engine-id my-engine
```

## Advanced Search Features

### Autocomplete and Search Suggestions

The autocomplete functionality provides intelligent query suggestions based on your indexed content:

```bash
# Basic autocomplete - get suggestions for partial queries
vertex-search --config my_config.json autocomplete suggest "rom" --engine-id my-engine
# Returns: ["romantic", "romantic comedy", "romance", "romantic drama"]

# Autocomplete for specific domains
vertex-search --config my_config.json autocomplete suggest "fam" --engine-id my-engine  
# Returns: ["family", "family drama", "family comedy"]

# Autocomplete works with multi-word queries
vertex-search --config my_config.json autocomplete suggest "psychological thr" --engine-id my-engine
# Returns: ["psychological thriller"]
```

**Frontend Integration Pattern:**
```javascript
// Example: Real-time search suggestions in JavaScript
async function getSuggestions(query) {
  const response = await fetch(`/api/autocomplete?q=${encodeURIComponent(query)}`);
  return await response.json();
}

// Debounced search input handler
let searchTimeout;
document.getElementById('search-input').addEventListener('input', (e) => {
  clearTimeout(searchTimeout);
  searchTimeout = setTimeout(() => {
    getSuggestions(e.target.value).then(suggestions => {
      // Display suggestions in dropdown
      showSuggestions(suggestions);
    });
  }, 300);
});
```

### Recommendations and Personalization

Build personalized user experiences with the recommendation system:

```bash
# Get recommendations based on user viewing history
vertex-search --config my_config.json recommend get \
  --user-id user123 \
  --event-type view \
  --document-ids "drama-short-001,drama-short-025" \
  --engine-id my-engine \
  --max-results 10

# Get recommendations for different event types
vertex-search --config my_config.json recommend get \
  --user-id user456 \
  --event-type purchase \
  --document-ids "premium-drama-005" \
  --engine-id my-engine \
  --max-results 5

# Get recommendations for users who liked specific content
vertex-search --config my_config.json recommend get \
  --user-id user789 \
  --event-type like \
  --document-ids "comedy-drama-012,teen-drama-008" \
  --engine-id my-engine \
  --max-results 8
```

**Recommendation Event Types:**
- `view` - User viewed/watched content
- `purchase` - User purchased/subscribed to content  
- `like` - User liked/favorited content
- `share` - User shared content
- `click` - User clicked on content
- `add-to-cart` - User added to cart/watchlist
- `search` - User searched for content

**Building User Profiles:**
```bash
# Example: Track user interactions for better recommendations

# User viewed a romantic drama
vertex-search --config my_config.json recommend record \
  --user-id user123 \
  --event-type view \
  --document-id "romantic-drama-015" \
  --engine-id my-engine

# User purchased premium content
vertex-search --config my_config.json recommend record \
  --user-id user123 \
  --event-type purchase \
  --document-id "premium-series-001" \
  --engine-id my-engine

# Now get personalized recommendations
vertex-search --config my_config.json recommend get \
  --user-id user123 \
  --event-type view \
  --engine-id my-engine
```

### Advanced Search with Filters and Facets

Combine search, filters, and facets for powerful discovery experiences:

```bash
# Search with multiple filters
vertex-search --config my_config.json search query "family story" \
  --engine-id my-engine \
  --filters '{"genre": ["family_drama"], "duration_seconds": 120, "content_type": "custom"}' \
  --facets "genre,content_type,language" \
  --page-size 20

# Search for content in specific language with rating filter
vertex-search --config my_config.json search query "mystery" \
  --engine-id my-engine \
  --filters '{"language": "en", "rating.mpaa_rating": ["PG", "PG-13"]}' \
  --facets "genre,rating.mpaa_rating"

# Search recent content with creator filter
vertex-search --config my_config.json search query "thriller" \
  --engine-id my-engine \
  --filters '{"creator_info.creator_type": "studio", "created_at": "2024-01-01"}' \
  --facets "creator_info.creator_type,genre"
```

**Filter Syntax Examples:**
```json
{
  "genre": ["romantic_drama", "family_drama"],           // Array values (OR logic)
  "content_type": "custom",                              // String value
  "duration_seconds": 120,                               // Numeric value  
  "language": "en",                                      // String exact match
  "rating.mpaa_rating": ["PG", "PG-13"],               // Nested field array
  "creator_info.verified": true,                         // Boolean value
  "audience_metrics.view_count": 1000                    // Nested numeric field
}
```

## Integration with Frontend Applications

The CLI is designed for easy integration with web applications:

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
config = AppConfig.from_env(schema_file=Path(\"your_schema.json\"))
config_manager = ConfigManager(config)

# Create managers
search_manager = SearchManager(config_manager)
autocomplete_manager = AutocompleteManager(config_manager)
recommendation_manager = RecommendationManager(config_manager)

# Perform search with filters and facets
results = search_manager.search(
    query=\"romantic drama\",
    engine_id=\"your-engine\",
    filters={\"genre\": [\"romantic_drama\"], \"content_type\": \"custom\"},
    facets=[\"genre\", \"content_type\", \"language\"],
    page_size=20
)

# Get autocomplete suggestions
suggestions = autocomplete_manager.get_suggestions(
    query=\"rom\",
    engine_id=\"your-engine\"
)

# Get personalized recommendations
user_event = {
    'eventType': 'view',
    'userPseudoId': 'user123',
    'documents': ['doc1', 'doc2', 'doc3']
}
recommendations = recommendation_manager.get_recommendations(
    user_event=user_event,
    engine_id=\"your-engine\", 
    max_results=10
)

# Record user interactions for better future recommendations
recommendation_manager.record_user_event(
    event_type=\"view\",
    user_pseudo_id=\"user123\",
    documents=[\"doc1\"],
    engine_id=\"your-engine\"
)
```

### REST API Wrapper

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

@app.get(\"/search\")
async def search(
    q: str,
    engine_id: str,
    filters: Optional[str] = None,
    facets: Optional[str] = None,
    page_size: int = 10
):
    \"\"\"Perform search with optional filters and facets.\"\"\"
    results = search_manager.search(
        query=q,
        engine_id=engine_id,
        filters=json.loads(filters) if filters else None,
        facets=facets.split(',') if facets else None,
        page_size=page_size
    )
    return results

@app.get(\"/autocomplete\")
async def autocomplete(q: str, engine_id: str):
    \"\"\"Get autocomplete suggestions.\"\"\"
    suggestions = autocomplete_manager.get_suggestions(
        query=q,
        engine_id=engine_id
    )
    return {\"suggestions\": suggestions}

@app.get(\"/recommendations\") 
async def get_recommendations(
    user_id: str,
    engine_id: str,
    event_type: str = \"view\",
    document_ids: Optional[str] = None,
    max_results: int = 10
):
    \"\"\"Get personalized recommendations for a user.\"\"\"
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
    return {\"recommendations\": recommendations}

@app.post(\"/events\")
async def record_event(
    user_id: str,
    event_type: str,
    document_id: str,
    engine_id: str
):
    \"\"\"Record a user interaction event.\"\"\"
    success = recommendation_manager.record_user_event(
        event_type=event_type,
        user_pseudo_id=user_id,
        documents=[document_id],
        engine_id=engine_id
    )
    return {\"success\": success}

# Example usage:
# GET /search?q=romantic%20drama&engine_id=my-engine&filters={\"genre\":[\"romantic_drama\"]}
# GET /autocomplete?q=rom&engine_id=my-engine  
# GET /recommendations?user_id=user123&engine_id=my-engine&event_type=view&document_ids=doc1,doc2
# POST /events?user_id=user123&event_type=view&document_id=doc1&engine_id=my-engine
```

## Authentication

The system supports multiple authentication methods with automatic fallback:

### Method 1: API Key (Recommended)

```bash
export VERTEX_PROJECT_ID="your-gcp-project-id"
export VERTEX_API_KEY="your-google-cloud-api-key"
```

**To create an API Key:**

1. Go to Google Cloud Console → APIs & Services → Credentials
2. Click "Create Credentials" → "API Key"
3. Restrict the key to Discovery Engine API for security
4. Copy the key and set the environment variable

### Method 2: Service Account Key File

```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
export VERTEX_PROJECT_ID="your-gcp-project-id"
```

### Method 3: Application Default Credentials

```bash
gcloud auth application-default login
export VERTEX_PROJECT_ID="your-gcp-project-id"
```

### Method 4: Compute Engine/Cloud Run Service Account

No additional setup needed when running on Google Cloud services.

## Configuration Options

### Environment Variables

All configuration can be set via environment variables (see `examples/.env.example`):

**Authentication:**

- `VERTEX_PROJECT_ID`: Google Cloud Project ID (required)
- `VERTEX_API_KEY`: Google Cloud API Key (recommended method)
- `VERTEX_LOCATION`: Vertex AI location (default: global)

**Schema Configuration:**

- `SCHEMA_SEARCHABLE_FIELDS`: Comma-separated list of searchable fields
- `SCHEMA_FILTERABLE_FIELDS`: Comma-separated list of filterable fields
- `SCHEMA_FACETABLE_FIELDS`: Comma-separated list of facetable fields

### Configuration File

Use JSON configuration files for complex setups (see `examples/config.json`):

```json
{
  \"vertex_ai\": {
    \"project_id\": \"your-project\",
    \"api_key\": \"your-api-key\",
    \"location\": \"global\"
  },
  \"schema\": {
    \"schema_file\": \"your_schema.json\",
    \"searchable_fields\": [\"title\", \"content\"],
    \"filterable_fields\": [\"category\", \"date\"],
    \"facetable_fields\": [\"category\", \"type\"]
  }
}
```

## Error Handling

The system includes comprehensive error handling with user-friendly messages:

- **Configuration errors**: Missing required fields, invalid schema files
- **Vertex AI errors**: Permission issues, quota limits, invalid requests
- **Data validation errors**: Schema validation failures, missing required fields

## Logging

Structured logging with Rich formatting:

```python
from vertex_search.utils import setup_logging

logger = setup_logging(level=\"INFO\", log_file=Path(\"app.log\"))
```

## Example: Drama Shorts Use Case

The `examples/` directory contains a complete example for a drama shorts platform:

1. **Schema**: `drama_shorts_schema.json` - Rich metadata for 1-3 minute drama videos
2. **Sample Data**: Generated using the schema with realistic content
3. **Configuration**: Ready-to-use config files for the drama shorts domain

This demonstrates the system's flexibility while providing a concrete example for a media startup.

## Troubleshooting

### Common Issues and Solutions

#### 1. "Project was not passed and could not be determined from the environment"

**Error:**
```
OSError: Project was not passed and could not be determined from the environment.
```

**Solution:**
This error occurs when the Google Cloud project ID is not properly configured. Fix by:

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

#### 5. Configuration File Issues

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

## Contributing

1. Follow SOLID principles when adding new features
2. Maintain schema agnosticism - don't hardcode specific content types
3. Add comprehensive error handling and logging
4. Include examples and documentation for new features

## License

MIT License - see LICENSE file for details.
