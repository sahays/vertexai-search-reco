import argparse
import logging

from vertexai_ops import bulk_import_user_events_from_bigquery

logging.basicConfig(level=logging.INFO)

def ingest_user_events_cli():
    """
    Orchestrates the bulk ingestion pipeline from BigQuery to VAIS.
    Uses the optimized bulk import API for fast processing.
    """
    parser = argparse.ArgumentParser(description="Bulk import user events from a BigQuery view to VAIS.")
    parser.add_argument("--project-id", required=True, help="The GCP Project ID.")
    parser.add_argument("--location", required=True, help="The location of the VAIS datastore (e.g., 'global').")
    parser.add_argument("--datastore-id", required=True, help="The ID of the target VAIS datastore.")
    parser.add_argument("--bq-source-table", required=True, help="The full BigQuery view name in the format 'project.dataset.view'.")
    args = parser.parse_args()

    logging.info(f"Starting bulk import from BigQuery table: {args.bq_source_table}")
    logging.info(f"Target datastore: {args.datastore_id} in location: {args.location}")

    # Use the bulk import API - much faster than processing individual events
    operation = bulk_import_user_events_from_bigquery(
        project_id=args.project_id,
        location=args.location,
        data_store_id=args.datastore_id,
        bigquery_table=args.bq_source_table
    )

    logging.info(f"✅ Bulk import operation initiated successfully")
    logging.info("📊 The operation runs asynchronously. Check the Google Cloud Console for progress.")
    logging.info("🎯 User events will be available for search and recommendations once the import completes.")

if __name__ == "__main__":
    ingest_user_events_cli()