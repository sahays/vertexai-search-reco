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
6. **Update Schema**:
   ```bash
   media-data-store --config examples/customer_media_config.json update-schema my-media-store
   ```

## Commands

### `validate`

Validate media data against schema requirements.

```bash
media-data-store --config config.json validate [--data-file custom_data.json]
```

### `transform`

Transform customer data to Google media schema format.

```bash
media-data-store --config config.json transform [--include-original]
```

**Options**:

- `--include-original`: Embeds the original data in a field named `original_payload`.

### `upload-bq`

Upload processed media data to BigQuery.

```bash
media-data-store --config config.json upload-bq DATA_FILE DATASET_ID TABLE_ID
```

### `create`

Create a media data store in Vertex AI Search.

```bash
media-data-store --config config.json create DATA_STORE_ID
```

### `import-bq`

Import data from BigQuery into the media data store.

```bash
media-data-store --config config.json import-bq DATA_STORE_ID DATASET_ID TABLE_ID
```

### `update-schema`

Apply schema settings from your configuration file to the data store. This is a crucial step after importing data to
control which fields are searchable, retrievable, etc.

```bash
media-data-store --config config.json update-schema DATA_STORE_ID
```

**Workflow**:

1.  Run the `import-bq` command to ingest your data.
2.  After the import is complete, run this `update-schema` command.
3.  The command will fetch the current schema, merge the settings from your config file (`searchable_fields`,
    `retrievable_fields`, etc.), and apply them.

For a detailed explanation of the rules and best practices for managing your schema, please refer to the guide:

- [**Best Practices for Updating a Vertex AI Search Schema**](./SCHEMA_UPDATE_GUIDE.md)

### `status`

Check the status of an import operation.

```bash
media-data-store --config config.json status OPERATION_NAME
```

### `list`

List all media data stores in the project.

```bash
media-data-store --config config.json list
```

### `info`

Get detailed information about a specific data store.

```bash
media-data-store --config config.json info DATA_STORE_ID
```
