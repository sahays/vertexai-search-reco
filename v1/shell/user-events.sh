#!/bin/bash
# This script automates the ingestion of user events by first ensuring a
# BigQuery view exists with the correct schema, and then running the ingestion script.
#
# PREREQUISITE: This script requires 'jq' to be installed for parsing the JSON mapping.
# (e.g., 'sudo apt-get install jq' or 'brew install jq')

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
LOCATION="US" # The location of your BigQuery dataset (e.g., US, EU, asia-south1)
DATASTORE_ID="media-datastore-1758866993" # The ID of your target VAIS datastore
SOURCE_TABLE="user_events_ingested" # The raw table uploaded previously
VIEW_NAME="user_events_for_vais_view" # The view to be created/used for ingestion

PYTHON_SCRIPT="$SCRIPT_DIR/../user_event_ops.py"
TARGET_VIEW_FULL_PATH="$PROJECT_ID:$DATASET_ID.$VIEW_NAME" # Use colon for bq tool
SOURCE_TABLE_FULL_PATH="\`$PROJECT_ID.$DATASET_ID.$SOURCE_TABLE\`" # Use backticks for SQL

# --- Step 1: Check for and Create BigQuery View ---
echo "▶️  Step 1/2: Checking for BigQuery view '$TARGET_VIEW_FULL_PATH' in location '$LOCATION'..."

# Check if the view exists by trying to show it. A non-zero exit code means it doesn't exist.
# The --location flag is crucial for this check.
bq --location=$LOCATION show "$TARGET_VIEW_FULL_PATH" &>/dev/null
if [ $? -ne 0 ]; then
    echo "   View not found. Generating SQL to create it..."

    # --- Mapping Documentation ---
    # The following table documents the mapping from the source CSV columns to the VAIS UserEvent schema.
    #
    # | VAIS User Event Field              | Source CSV Column         | Transformation/Logic                               |
    # |------------------------------------|---------------------------|----------------------------------------------------|
    # | eventType                          | (none)                    | Hardcoded to 'view-item'                           |
    # | eventTime                          | StartTimeUnixMs           | Converted from Unix milliseconds to RFC 3339 UTC   |
    # | sessionId                          | ConvivaSessionID          | Direct mapping                                     |
    # | userInfo.userId                    | ViewerID                  | Direct mapping                                     |
    # | userPseudoId                       | ConvivaSessionID          | Direct mapping (required field)                   |
    # | userInfo.userAgent                 | Browser, BrowserVersion   | Concatenated (e.g., "Chrome/123.0")                |
    # | documents[0].id                    | show_id                   | Direct mapping (matches zee_id in datastore)      |
    # | attributes.show_title              | c3_cm_showTitle           | Mapped as a text array attribute                   |
    # | attributes.series_name             | c3_cm_seriesName          | Mapped as a text array attribute                   |
    # | attributes.genres                  | c3_cm_genreList           | Mapped as a text array attribute                   |
    # | attributes.show_type               | show_type                 | Mapped as a text array attribute                   |
    # | attributes.season_number           | c3_cm_seasonNumber        | Mapped as a text array attribute                   |
    # | attributes.episode_number          | c3_cm_episodeNumber       | Mapped as a text array attribute                   |
    # | attributes.watch_duration_seconds  | PlayingTime               | Mapped as a number attribute                       |
    # | attributes.rebuffering_time_seconds| ReBufferingTime           | Mapped as a number attribute                       |
    # | attributes.content_length_seconds  | ContentLength             | Mapped as a number attribute                       |
    # | attributes.startup_time_ms         | StartupTime               | Mapped as a number attribute                       |
    # | attributes.ended_status            | EndedStatus               | Mapped as a text array attribute                   |
    # | attributes.device_os               | DeviceOS                  | Mapped as a text array attribute                   |
    # | attributes.device_manufacturer     | DeviceManufacture         | Mapped as a text array attribute                   |
    # | attributes.device_type             | DeviceHardwareType        | Mapped as a text array attribute                   |
    # | attributes.country                 | Country                   | Mapped as a text array attribute                   |
    # | attributes.city                    | City                      | Mapped as a text array attribute                   |
    # | attributes.language                | language                  | Mapped as a text array attribute                   |
    # | attributes.audio_language_at_start | audio_language_at_start   | Mapped as a text array attribute                   |

    # This query creates a sensible and curated view that maps the most important raw
    # data columns to the standard fields in the Vertex AI Search UserEvent schema.
    # The most valuable remaining columns are mapped into a clean 'attributes' struct.
    SQL_QUERY="
    CREATE OR REPLACE VIEW \`$PROJECT_ID.$DATASET_ID.$VIEW_NAME\` AS
    SELECT
        -- 1. Standard Fields
        'view-item' AS eventType, -- This is an assumption based on the data.
        FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', TIMESTAMP_MILLIS(SAFE_CAST(StartTimeUnixMs AS INT64))) AS eventTime,
        ConvivaSessionID AS sessionId,
        ConvivaSessionID AS userPseudoId,

        -- 2. Nested Standard Fields (Structs)
        STRUCT(
            ViewerID AS userId,
            CONCAT(Browser, '/', BrowserVersion) AS userAgent
        ) AS userInfo,
        [STRUCT(
            show_id AS id
        )] AS documents,

        -- 3. Curated Attributes: Only include attributes with non-empty values
        (
            SELECT AS STRUCT
                -- Only include attributes that have non-empty values
                CASE WHEN ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(SPLIT(c3_cm_showTitle, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '')) > 0
                     THEN STRUCT(ARRAY(SELECT x FROM UNNEST(SPLIT(c3_cm_showTitle, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '') AS text)
                     ELSE NULL END AS show_title,

                CASE WHEN ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(SPLIT(c3_cm_seriesName, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '')) > 0
                     THEN STRUCT(ARRAY(SELECT x FROM UNNEST(SPLIT(c3_cm_seriesName, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '') AS text)
                     ELSE NULL END AS series_name,

                CASE WHEN ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(SPLIT(c3_cm_genreList, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '')) > 0
                     THEN STRUCT(ARRAY(SELECT x FROM UNNEST(SPLIT(c3_cm_genreList, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '') AS text)
                     ELSE NULL END AS genres,

                CASE WHEN ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(SPLIT(show_type, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '')) > 0
                     THEN STRUCT(ARRAY(SELECT x FROM UNNEST(SPLIT(show_type, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '') AS text)
                     ELSE NULL END AS show_type,

                CASE WHEN ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(SPLIT(c3_cm_seasonNumber, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '')) > 0
                     THEN STRUCT(ARRAY(SELECT x FROM UNNEST(SPLIT(c3_cm_seasonNumber, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '') AS text)
                     ELSE NULL END AS season_number,

                CASE WHEN ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(SPLIT(c3_cm_episodeNumber, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '')) > 0
                     THEN STRUCT(ARRAY(SELECT x FROM UNNEST(SPLIT(c3_cm_episodeNumber, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '') AS text)
                     ELSE NULL END AS episode_number,

                -- Technical & Playback Attributes (numbers)
                CASE WHEN SAFE_CAST(PlayingTime AS NUMERIC) IS NOT NULL
                     THEN STRUCT([SAFE_CAST(PlayingTime AS NUMERIC)] AS numbers)
                     ELSE NULL END AS watch_duration_seconds,

                CASE WHEN SAFE_CAST(ReBufferingTime AS NUMERIC) IS NOT NULL
                     THEN STRUCT([SAFE_CAST(ReBufferingTime AS NUMERIC)] AS numbers)
                     ELSE NULL END AS rebuffering_time_seconds,

                CASE WHEN SAFE_CAST(ContentLength AS NUMERIC) IS NOT NULL
                     THEN STRUCT([SAFE_CAST(ContentLength AS NUMERIC)] AS numbers)
                     ELSE NULL END AS content_length_seconds,

                CASE WHEN SAFE_CAST(StartupTime AS NUMERIC) IS NOT NULL
                     THEN STRUCT([SAFE_CAST(StartupTime AS NUMERIC)] AS numbers)
                     ELSE NULL END AS startup_time_ms,

                CASE WHEN ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(SPLIT(EndedStatus, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '')) > 0
                     THEN STRUCT(ARRAY(SELECT x FROM UNNEST(SPLIT(EndedStatus, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '') AS text)
                     ELSE NULL END AS ended_status,

                -- Device & Location Attributes
                CASE WHEN ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(SPLIT(DeviceOS, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '')) > 0
                     THEN STRUCT(ARRAY(SELECT x FROM UNNEST(SPLIT(DeviceOS, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '') AS text)
                     ELSE NULL END AS device_os,

                CASE WHEN ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(SPLIT(DeviceManufacture, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '')) > 0
                     THEN STRUCT(ARRAY(SELECT x FROM UNNEST(SPLIT(DeviceManufacture, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '') AS text)
                     ELSE NULL END AS device_manufacturer,

                CASE WHEN ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(SPLIT(DeviceHardwareType, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '')) > 0
                     THEN STRUCT(ARRAY(SELECT x FROM UNNEST(SPLIT(DeviceHardwareType, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '') AS text)
                     ELSE NULL END AS device_type,

                CASE WHEN ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(SPLIT(Country, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '')) > 0
                     THEN STRUCT(ARRAY(SELECT x FROM UNNEST(SPLIT(Country, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '') AS text)
                     ELSE NULL END AS country,

                CASE WHEN ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(SPLIT(City, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '')) > 0
                     THEN STRUCT(ARRAY(SELECT x FROM UNNEST(SPLIT(City, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '') AS text)
                     ELSE NULL END AS city,

                -- Language Attributes
                CASE WHEN ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(SPLIT(language, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '')) > 0
                     THEN STRUCT(ARRAY(SELECT x FROM UNNEST(SPLIT(language, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '') AS text)
                     ELSE NULL END AS language,

                CASE WHEN ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(SPLIT(audio_language_at_start, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '')) > 0
                     THEN STRUCT(ARRAY(SELECT x FROM UNNEST(SPLIT(audio_language_at_start, ',')) AS x WHERE x IS NOT NULL AND TRIM(x) != '') AS text)
                     ELSE NULL END AS audio_language_at_start
        ) AS attributes
    FROM
        $SOURCE_TABLE_FULL_PATH
    WHERE
        ViewerID IS NOT NULL
        AND StartTimeUnixMs IS NOT NULL
        AND show_id IS NOT NULL;
    "

    echo "   Executing CREATE VIEW query..."
    bq query --project_id=$PROJECT_ID --use_legacy_sql=false "$SQL_QUERY"

    if [ $? -ne 0 ]; then
        echo "❌ Error: Failed to create BigQuery view. Aborting."
        exit 1
    fi
    echo "✅ View created successfully."
else
    echo "✅ View already exists."
fi

# --- Step 2: Run the Python Ingestion Script ---
echo "▶️  Step 2/2: Starting ingestion from view into Vertex AI Search..."

# The python script reads the pre-structured data directly from the view.
# The --mapping argument is no longer needed.
"$VENV_PYTHON" "$PYTHON_SCRIPT" \
    --project-id "$PROJECT_ID" \
    --location "global" \
    --datastore-id "$DATASTORE_ID" \
    --bq-source-table "$PROJECT_ID.$DATASET_ID.$VIEW_NAME"

if [ $? -ne 0 ]; then
    echo "❌ Error: Python ingestion script failed."
    exit 1
fi

echo "🎉 Ingestion process initiated successfully."
