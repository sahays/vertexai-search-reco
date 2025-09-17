# Media Data Store

A CLI for preparing and ingesting data into a Vertex AI Search for Media data store.

## End-to-End Workflow

This tool is the first step in the pipeline. A typical workflow involves transforming your local data, creating a data
store in Google Cloud, uploading your transformed data to BigQuery, importing it, and finally applying your schema
settings.

```bash
# Define environment variables for convenience
export CONFIG="examples/customer_media_config.json"
export LOG_DIR="logs"
export OUTPUT_DIR="outputs"

# 1. Transform local data file according to the mapping rules
media-data-store --config $CONFIG --output-dir $OUTPUT_DIR transform

# 2. Create a new data store in Vertex AI
media-data-store --config $CONFIG create media-store-v1

# 3. Upload the transformed data to a BigQuery table
# (Replace with your BQ dataset and table names)
media-data-store --config $CONFIG upload-bq \
  "$OUTPUT_DIR/111935084-transform-transformed_customer_data.json" \
  my_media_dataset my_media_table

# 4. Import the data from BigQuery into your data store
media-data-store --config $CONFIG import-bq \
  media-store-v1 my_media_dataset my_media_table

# 5. Apply your schema settings (searchable, facetable, etc.) to the data store
media-data-store --config $CONFIG update-schema media-store-v1
```

## Key Commands

- **`transform`**: Converts your source data into a Google-compliant format based on rules in your mapping file.
- **`create <data-store-id>`**: Creates a new, empty data store in Vertex AI.
- **`upload-bq <file> <dataset> <table>`**: Uploads a local JSON file to a BigQuery table.
- **`import-bq <data-store-id> <dataset> <table>`**: Starts the long-running import process from BigQuery to your data
  store.
- **`update-schema <data-store-id>`**: Configures the data store's schema based on the `media_schema` settings in your
  config file. This controls search and discovery features.
- **`status <operation-name>`**: Checks the status of a long-running operation like an import.
