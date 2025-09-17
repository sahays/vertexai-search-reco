# Media Search

A CLI for querying a Vertex AI Search for Media engine.

## End-to-End Workflow

This tool is the final step in the pipeline, used to test and interact with a serving engine.

```bash
# Define environment variables for convenience
export CONFIG="examples/customer_media_config.json"
export LOG_DIR="logs"
export OUTPUT_DIR="outputs"

# 1. List available engines to find the one you want to query
media-engine --config $CONFIG list

# 2. Execute a search query against your engine
media-search --config $CONFIG search my-media-engine-v1 "love story"

# 3. Execute a search with filters and facets
media-search --config $CONFIG search my-media-engine-v1 "love story" \
  --filter "categories:Comedy"
  --filter "language:hi"
  --facet-field "genre"

# 4. Get autocomplete suggestions for a partial query
media-search --config $CONFIG autocomplete my-media-engine-v1 "rom"
```

## Key Commands

- **`search <engine-id> <query>`**: Executes a search query.
  - `--filter`: Applies a filter (e.g., `"categories:Drama"`). Can be used multiple times.
  - `--facet-field`: Specifies a field to get facet counts for.
- `autocomplete <engine-id> <query>`\*\*: Gets autocomplete suggestions.
- `track`\*\*: (Placeholder) For tracking user events.
