#!/bin/bash
# This script uploads a CSV file to a BigQuery table using a two-step process:
# 1. Upload the local CSV file to Google Cloud Storage (GCS).
# 2. Load the data from GCS into a new BigQuery table with a specific schema.
# This script is designed to be run from anywhere in the project.

# --- Configuration ---
# Get the directory where the script is located to resolve paths correctly.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# --- Preamble: Find and use the correct Python interpreter ---
# This ensures the script uses the Python from the virtual environment.
PROJECT_ROOT="$SCRIPT_DIR/../.."
VENV_PYTHON="$PROJECT_ROOT/.venv/bin/python"

if [ ! -f "$VENV_PYTHON" ]; then
    echo "❌ Error: Python interpreter not found at '$VENV_PYTHON'"
    echo "   Please ensure the virtual environment '.venv' exists in the project root."
    exit 1
fi

# Replace the placeholder values with your actual GCP configuration.
PROJECT_ID="search-and-reco"
DATASET_ID="media_dataset"
LOCATION="US" # bq load requires a region like US, EU, etc.
GCS_BUCKET="sahays-zbullet-samples" # IMPORTANT: Bucket to stage the CSV for BQ load.

# Paths are now relative to the script's location
SOURCE_CSV="$SCRIPT_DIR/../sample_data/customer_userevents-sample.csv"
TABLE_ID="user_events_ingested_v1"
GCS_URI="gs://$GCS_BUCKET/$(basename "$SOURCE_CSV")"

# --- Schema Definition ---
# The exact schema for the BigQuery table, derived from your CSV columns.
# All columns are defined as STRING for safe import.
SCHEMA="ViewerID:STRING,AssetName:STRING,DeviceOS:STRING,Country:STRING,State:STRING,City:STRING,StartTimeUnixMs:STRING,StartupTime:STRING,PlayingTime:STRING,ReBufferingTime:STRING,Browser:STRING,ConvivaSessionID:STRING,StreamURL:STRING,ErrorList:STRING,VPF:STRING,VPFErrorList:STRING,ContentLength:STRING,EndedStatus:STRING,SessionEndedStatus:STRING,EndTimeUnixMs:STRING,VSFBusiness:STRING,VSFBusinessErrorList:STRING,VSFTechnical:STRING,VSFTechnicalErrorList:STRING,VPFBusiness:STRING,VPFBusinessErrorList:STRING,VPFTechnical:STRING,VPFTechnicalErrorList:STRING,ExitDuringPreRoll:STRING,AdRelatedRebuffering:STRING,DeviceHardwareType:STRING,DeviceManufacture:STRING,DeviceName:STRING,DeviceOSVersion:STRING,BrowserVersion:STRING,dt:STRING,SessionTags:STRING,start_time_unix_ms_ts:STRING,end_time_unix_ms_ts:STRING,start_date:STRING,end_date:STRING,show_type:STRING,asset_id:STRING,c3_device_conn:STRING,c3_cm_genre:STRING,c3_cm_affiliate:STRING,business_type:STRING,c3_cm_episodeNumber:STRING,c3_cm_seriesName:STRING,c3_cm_showTitle:STRING,c3_cm_seasonNumber:STRING,c3_cm_contentType:STRING,c3_cm_categoryType:STRING,c3_cm_name:STRING,show_id:STRING,c3_cm_genreList:STRING,audio_language_at_start:STRING,audio_language_at_end:STRING,language:STRING"

# --- Execution ---
echo "▶️ Step 1/2: Uploading '$SOURCE_CSV' to '$GCS_URI'..."
gsutil cp "$SOURCE_CSV" "$GCS_URI"

if [ $? -ne 0 ]; then
    echo "❌ Error: Failed to upload CSV to GCS. Please check your gsutil configuration and bucket permissions."
    exit 1
fi
echo "✅ Step 1/2: Successfully uploaded to GCS."

echo "▶️ Step 2/2: Loading data from GCS into BigQuery table '$PROJECT_ID:$DATASET_ID.$TABLE_ID'..."
bq --location=$LOCATION load \
    --source_format=CSV \
    --skip_leading_rows=1 \
    --replace \
    "$PROJECT_ID:$DATASET_ID.$TABLE_ID" \
    "$GCS_URI" \
    "$SCHEMA"

if [ $? -ne 0 ]; then
    echo "❌ Error: BigQuery load job failed. Check the bq command output for details."
    exit 1
fi
echo "✅ Step 2/2: BigQuery load job initiated successfully."
echo "🎉 All done."
