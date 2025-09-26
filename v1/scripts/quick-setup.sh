#!/bin/bash
# This script runs the full data transformation and Vertex AI Search setup process.

# --- Preamble: Find and use the correct Python interpreter ---
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT="$SCRIPT_DIR/../.."
VENV_PYTHON="$PROJECT_ROOT/.venv/bin/python"

if [ ! -f "$VENV_PYTHON" ]; then
    echo "❌ Error: Python interpreter not found at '$VENV_PYTHON'"
    echo "   Please ensure the virtual environment '.venv' exists in the project root."
    exit 1
fi

# --- Configuration ---
UNIQUE_ID=$(date +%s)
mkdir -p outputs

# Define the custom fields mapping in a variable to handle complex quoting.
CUSTOM_FIELDS_JSON='{
  "description": {"name": "desc", "type": "string"},
  "short_description": {"name": "short_desc", "type": "string"},
  "original_title": {"name": "original_title", "type": "string"},
  "directors": {"name": "directors", "type": "array"},
  "actors": {"name": "actors", "type": "array"},
  "age_rating": {"name": "age_rating", "type": "string"},
  "audio_language": {"name": "audio_lang", "type": "array"},
  "content_language": {"name": "content_language", "type": "array"},
  "subtitle_language": {"name": "subtitle_lang", "type": "array"},
  "primary_language": {"name": "primary_language", "type": "string"},
  "tags": {"name": "tags", "type": "array"},
  "content_category": {"name": "extended.content_category", "type": "string"},
  "content_version": {"name": "extended.content_version", "type": "string"},
  "keywords": {"name": "extended.digital_keywords", "type": "array"},
  "broadcast_state": {"name": "extended.broadcast_state", "type": "string"},
  "producers": {"name": "extended.producers", "type": "array"},
  "music_directors": {"name": "extended.music_directors", "type": "array"},
  "content_descriptors": {"name": "extended.content_descriptors", "type": "array"}
}'

# --- Execution ---
echo "🔄 Step 0: Transforming data with proper type handling..."

# First transform the data to handle empty strings and arrays properly
"$VENV_PYTHON" "$SCRIPT_DIR/../vais.py" --project-id search-and-reco \
    --location asia-south1 \
    transform-data "$SCRIPT_DIR/../sample_data/customer_sample.json" "outputs/customer_sample_data_transformed.json" \
    --custom-fields "$CUSTOM_FIELDS_JSON" --validate

if [ $? -ne 0 ]; then
    echo "❌ Data transformation failed. Aborting setup."
    exit 1
fi

echo "✅ Data transformation completed. Now running quick setup with transformed data..."

# Now run quick setup with the transformed data
"$VENV_PYTHON" "$SCRIPT_DIR/../vais.py" --project-id search-and-reco \
    --location asia-south1 \
    quick-setup "outputs/customer_sample_data_transformed.json" "vais-dataset" "view-v1" \
    --datastore-id "media-datastore-${UNIQUE_ID}" \
    --engine-id "media-search-engine-${UNIQUE_ID}" \
    --id-field "zee_id" \
    --title-field "title" \
    --categories-field "genre" \
    --media-type-field "extended.content_category" \
    --available-time-field "licensing_from" \
    --expire-time-field "licensing_until" \
    --skip-transform \
    --custom-fields "$CUSTOM_FIELDS_JSON" 2>&1 | tee outputs/quick-setup-output.log
