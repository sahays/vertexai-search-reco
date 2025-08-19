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
3. Create a service account and download the JSON key
4. Set up authentication:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
```

### 3. Configuration

Copy the example configuration:

```bash
cp examples/.env.example .env
cp examples/config.json my_config.json
```

Edit the configuration files with your project details:

```bash
# .env
VERTEX_PROJECT_ID=your-gcp-project-id
VERTEX_LOCATION=global
```

### 4. Generate Sample Data

```bash
# Generate 1000 sample records using the example schema
python generate_sample_data.py
```

### 5. Create and Index Dataset

```bash
# Using environment variables
vertex-search --schema examples/drama_shorts_schema.json --project-id your-project-id dataset create examples/sample_data.json

# Or using config file
vertex-search --config my_config.json dataset create examples/sample_data.json
```

### 6. Create Data Store and Search Engine

```bash
# Create data store
vertex-search datastore create my-datastore --display-name "My Media Store"

# Import documents
vertex-search datastore import my-datastore examples/sample_data.json --wait

# Create search engine
vertex-search search create-engine my-engine my-datastore --display-name "My Search Engine"
```

### 7. Search Your Content

```bash
# Basic search
vertex-search search query "romantic drama" --engine-id my-engine

# Search with filters
vertex-search search query "love story" --engine-id my-engine --filters '{"genre": ["romantic_drama"]}'

# Get autocomplete suggestions
vertex-search autocomplete suggest "rom" --engine-id my-engine

# Get recommendations
vertex-search recommend get --user-id user123 --event-type view --engine-id my-engine
```

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
vertex-search dataset generate --count 1000 --output my_sample_data.json
```

## CLI Reference

### Dataset Commands

```bash
# Create and validate dataset
vertex-search dataset create <data_file> [--validate-only]

# Generate sample data
vertex-search dataset generate [--count 1000] [--output file.json] [--seed 42]
```

### Data Store Commands

```bash
# Create data store
vertex-search datastore create <data_store_id> [--display-name \"Name\"]

# Import documents
vertex-search datastore import <data_store_id> <data_file> [--wait]
```

### Search Commands

```bash
# Create search engine
vertex-search search create-engine <engine_id> <data_store_id> [--display-name \"Name\"]

# Search with options
vertex-search search query <query> [--engine-id <id>] [--filters <json>] [--facets <list>]
```

### Autocomplete Commands

```bash
# Get suggestions
vertex-search autocomplete suggest <query> [--engine-id <id>]
```

### Recommendation Commands

```bash
# Get recommendations
vertex-search recommend get --user-id <id> [--event-type view] [--engine-id <id>]
```

## Integration with Frontend Applications

The CLI is designed for easy integration with web applications:

### Python Integration

```python
from vertex_search.config import AppConfig, ConfigManager
from vertex_search.managers import SearchManager

# Load configuration
config = AppConfig.from_env(schema_file=Path(\"your_schema.json\"))
config_manager = ConfigManager(config)

# Create search manager
search_manager = SearchManager(config_manager)

# Perform search
results = search_manager.search(
    query=\"user query\",
    engine_id=\"your-engine\",
    filters={\"category\": \"videos\"},
    page_size=20
)
```

### REST API Wrapper

You can easily wrap the managers in a REST API using FastAPI, Flask, or Django:

```python
from fastapi import FastAPI
from vertex_search.managers import SearchManager

app = FastAPI()

@app.get(\"/search\")
async def search(q: str, filters: str = None):
    # Use SearchManager to handle the request
    results = search_manager.search(query=q, filters=json.loads(filters or \"{}\"))
    return results
```

## Configuration Options

### Environment Variables

All configuration can be set via environment variables (see `examples/.env.example`):

- `VERTEX_PROJECT_ID`: Google Cloud Project ID
- `VERTEX_LOCATION`: Vertex AI location (default: global)
- `SCHEMA_SEARCHABLE_FIELDS`: Comma-separated list of searchable fields
- `SCHEMA_FILTERABLE_FIELDS`: Comma-separated list of filterable fields

### Configuration File

Use JSON configuration files for complex setups (see `examples/config.json`):

```json
{
  \"vertex_ai\": {
    \"project_id\": \"your-project\",
    \"location\": \"global\"
  },
  \"schema\": {
    \"schema_file\": \"your_schema.json\",
    \"searchable_fields\": [\"title\", \"content\"],
    \"filterable_fields\": [\"category\", \"date\"]
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

## Contributing

1. Follow SOLID principles when adding new features
2. Maintain schema agnosticism - don't hardcode specific content types
3. Add comprehensive error handling and logging
4. Include examples and documentation for new features

## License

MIT License - see LICENSE file for details.
