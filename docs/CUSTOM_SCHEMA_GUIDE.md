# Using a Custom Schema and Data

This guide explains how to use the Vertex AI Search for Media CLI with your own custom JSON schema and data. The application is designed to be schema-agnostic, allowing you to adapt it to any data model without code changes.

## 1. Create a Valid JSON Schema

The first step is to define your data structure using a valid JSON Schema. The schema must be a `.json` file and should follow the [JSON Schema Draft 7](https://json-schema.org/specification-links.html#draft-7) specification.

**Key Requirements:**

*   The schema `type` must be `object`.
*   It must contain a `properties` field that defines the fields in your data.
*   Each property must have a standard JSON schema `type` (e.g., `string`, `integer`, `array`, `object`, `boolean`).
*   For date fields, use `type: "string"` with `format: "date"`.

### Example: Custom Schema

Here is an example of a custom schema for drama shorts metadata.

`examples/customer_schema.json`:
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Drama Shorts Metadata Schema",
  "description": "TV Show schema for micro_drama",
  "type": "object",
  "required": [
    "id",
    "title",
    "desc",
    "genre",
    "asset_type",
    "image",
    "status",
    "licensing_from",
    "licensing_until"
  ],
  "properties": {
    "id": {
        "type": "integer",
        "description": "unique id for the content"
    },
    "title": {
        "type": "string"
    },
    "genre": {
      "type": "array",
      "minItems": 1,
      "items": {
        "type": "string"
      }
    },
    "release_date": {
        "type": "string",
        "format": "date"
    }
  }
}
```

## 2. Create a Configuration File

Next, you need to create a configuration file that tells the application how to use your custom schema. This file maps the fields in your schema to the different features of Vertex AI Search (search, filtering, and faceting).

**Key Configuration Fields:**

*   `schema_file`: The path to your custom schema file.
*   `searchable_fields`: A list of fields that should be indexed for full-text search.
*   `filterable_fields`: A list of fields that can be used for filtering search results.
*   `facetable_fields`: A list of fields that can be used for faceting (i.e., grouping results by field values).

### Example: Custom Configuration

Here is an example configuration file that corresponds to the custom schema above.

`examples/customer_config.json`:
```json
{
	"vertex_ai": {
		"project_id": "your-gcp-project-id",
		"location": "global"
	},
	"schema": {
		"schema_file": "examples/customer_schema.json",
		"searchable_fields": [
			"title",
			"desc"
		],
		"filterable_fields": [
			"genre",
			"release_date"
		],
		"facetable_fields": [
			"genre"
		]
	}
}
```

## 3. Prepare Your Data

Your data must be in a JSON file, where each record is a JSON object that conforms to your custom schema.

If you don't have data, you can generate sample data from your schema using the following command:

```bash
vertex-search --config examples/customer_config.json dataset generate --count 1000 --output examples/customer_sample_data.json
```

## 4. Use the CLI with Your Custom Configuration

Once you have your schema, configuration, and data, you can use the CLI with your custom setup by providing your configuration file to the `--config` option.

### Example Workflow

Here is an example workflow using the custom schema and configuration:

```bash
# Set your GCS bucket name
export BUCKET_NAME="your-gcs-bucket-name"

# Create a search data store
vertex-search --config examples/customer_config.json datastore create customer-datastore --display-name "Customer Search Store" --solution-type SEARCH

# Upload your data to Cloud Storage
vertex-search --config examples/customer_config.json datastore upload-gcs examples/customer_sample_data.json $BUCKET_NAME --create-bucket --folder customer-search

# Import the data from Cloud Storage
vertex-search --config examples/customer_config.json datastore import-gcs customer-datastore gs://$BUCKET_NAME/customer-search/* --wait

# Create a search engine
vertex-search --config examples/customer_config.json search create-engine customer-engine customer-datastore --display-name "Customer Search Engine" --solution-type SEARCH

# Perform a search
vertex-search --config examples/customer_config.json search query "action movie" --engine-id customer-engine
```

By following these steps, you can adapt the application to any data model, making it a powerful and flexible tool for your search and recommendation needs.
