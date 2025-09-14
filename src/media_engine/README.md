# Media Engine Module

Management of Vertex AI Search for Media engines for content discovery and semantic search.

## Overview

The Media Engine module provides lifecycle management for Vertex AI Search for Media engines. It handles creation,
configuration, monitoring, and deletion of search engines that are specifically designed for media content (videos,
movies, shows, etc.).

## Prerequisites

- Google Cloud Project with Vertex AI Search for Media API enabled
- Existing VAIS Media datastore (created using `media-data-store` module)
- Google Cloud authentication configured (`gcloud auth login`)
- Python 3.9+ with required dependencies installed

## Installation

```bash
# Install the package in development mode
pip install -e .
```

## Configuration

Use the same configuration file as the media-data-store module. The engine module will automatically extract the
necessary Vertex AI configuration:

```json
{
	"vertex_ai": {
		"project_id": "your-project-id",
		"location": "global"
	}
}
```

## CLI Commands

### Environment Setup

```bash
export CONFIG="examples/customer_media_config.json"
export LOG_DIR="logs"
export OUTPUT_DIR="outputs"
```

### Create Engine

Create a new VAIS Media search engine linked to an existing datastore:

```bash
media-engine --config $CONFIG \
  --log-dir $LOG_DIR \
  --output-dir $OUTPUT_DIR \
  create DATASTORE_ID ENGINE_ID \
  --display-name "Media Content Search Engine" \
  --description "Search engine for video and media content discovery"
```

**Example:**

```bash
media-engine --config $CONFIG \
  --log-dir $LOG_DIR \
  --output-dir $OUTPUT_DIR \
  create media-store-v2 content-search-v1 \
  --display-name "Movie & TV Search" \
  --description "Semantic search for movies, TV shows and media content"
```

### List Engines

List all VAIS Media engines or filter by datastore:

```bash
# List all engines
media-engine --config $CONFIG list --format table

# List engines for specific datastore
media-engine --config $CONFIG list media-store-v2 --format json
```

### Get Engine Information

Get detailed information about a specific engine:

```bash
media-engine --config $CONFIG get ENGINE_ID --format json
```

**Example:**

```bash
media-engine --config $CONFIG get content-search-v1 --format table
```

### Delete Engine

Delete a search engine (requires --force flag for confirmation):

```bash
media-engine --config $CONFIG delete ENGINE_ID --force
```

**Example:**

```bash
media-engine --config $CONFIG delete content-search-v1 --force
```

### Check Status (Placeholder)

```bash
media-engine --config $CONFIG status ENGINE_ID --verbose
```

## Output Files

The module generates timestamped output files in the specified output directory:

- `engine_{ENGINE_ID}_created.json` - Engine creation details
- `engine_{ENGINE_ID}_info.json` - Engine information
- `engines_list_{DATASTORE_ID}.json` - Engine listing results
- `engine_{ENGINE_ID}_deleted.json` - Engine deletion confirmation

## Engine Features

### VAIS Media Specific

- **Industry Vertical**: Automatically configured for MEDIA industry vertical
- **Content Configuration**: Optimized for media content (`NO_CONTENT` by default)
- **Search Tier**: Uses `SEARCH_TIER_STANDARD` with LLM add-ons
- **Datastore Integration**: Links to existing VAIS Media datastores

### Validation

- Validates datastore compatibility (must be MEDIA industry vertical)
- Ensures supported locations (global, us-central1, europe-west1)
- Checks required permissions and API access

## Example Workflow

1. **Create Engine**:

   ```bash
   media-engine --config $CONFIG create media-store-v2 movie-search \
     --display-name "Movie Discovery Engine" \
     --description "Semantic search for movie recommendations"
   ```

2. **Verify Creation**:

   ```bash
   media-engine --config $CONFIG get movie-search
   ```

3. **List All Engines**:

   ```bash
   media-engine --config $CONFIG list --format table
   ```

4. **Future**: Use engine for search queries (implemented in separate search module)

## Integration with Other Modules

### Media Data Store Integration

The engine module works with datastores created by the `media-data-store` module:

1. First, create and populate a datastore:

   ```bash
   media-data-store create media-store-v2 "Media Content Store"
   media-data-store transform examples/customer_sample_data.json
   media-data-store upload-bq transformed_data.json dataset table
   media-data-store import-bq media-store-v2 dataset table
   ```

2. Then create a search engine:
   ```bash
   media-engine create media-store-v2 content-search-v1 \
     --display-name "Content Search Engine"
   ```

### Future Search Module

Once engines are created, they will be used by the future search module for:

- Semantic search queries
- Content filtering and faceting
- Media-specific search features
- Search result ranking and relevance

## Error Handling

The module provides comprehensive error handling:

- **Configuration Validation**: Checks MEDIA industry vertical requirements
- **Resource Verification**: Validates datastore existence and compatibility
- **Permission Checks**: Ensures proper Google Cloud authentication
- **API Error Handling**: Graceful handling of Google Cloud API errors

## Logging

Detailed logging is available:

- **Console Output**: Summary of operations and results
- **Log Files**: Detailed debug information in `$LOG_DIR/HHMMSS-COMMAND-engine.log`
- **Verbose Mode**: Use `--verbose` flag for detailed console output

## Limitations

- Only supports VAIS Media engines (MEDIA industry vertical)
- Requires pre-existing VAIS Media datastores
- Currently supports standard search tier only
- Engine configuration updates not yet implemented

## Troubleshooting

### Common Issues

1. **"Datastore not found"**: Ensure datastore exists and is accessible
2. **"Industry vertical mismatch"**: Datastore must be MEDIA industry vertical
3. **"Permission denied"**: Check Google Cloud authentication and project access
4. **"Location not supported"**: Use global, us-central1, or europe-west1

### Debug Mode

Enable verbose logging for troubleshooting:

```bash
media-engine --config $CONFIG --verbose create ...
```

## Related Modules

- **media-data-store**: Creates and manages VAIS Media datastores
- **search** (future): Performs search queries using created engines
- **recommendations** (future): Content recommendation system
