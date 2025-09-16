# Media Engine

A CLI for creating and managing Vertex AI Search for Media engines.

## End-to-End Workflow

This tool is the second step in the pipeline, used after you have a data store with imported data. The primary purpose
is to create a serving engine linked to your data store.

```bash
# Define environment variables for convenience
export CONFIG="examples/customer_media_config.json"
export LOG_DIR="logs"
export OUTPUT_DIR="outputs"

# 1. List available data stores to find the one you want to use
media-data-store --config $CONFIG list

# 2. Create a new search engine linked to your data store
media-engine --config $CONFIG --output-dir $OUTPUT_DIR create \
  media-store-v1 \
  my-media-engine-v1 \
  --display-name "My Media Engine"

# 3. Check the status or get information about your new engine
media-engine --config $CONFIG get my-media-engine-v1
```

## Key Commands

- **`create <data-store-id> <engine-id>`**: Creates a new engine and links it to an existing data store.
- **`list`**: Lists all engines in the project.
- **`get <engine-id>`**: Retrieves detailed information about a specific engine.
- **`delete <engine-id>`**: Deletes an engine.
