"""Command-line interface for Data Store Management."""

import click
import json
from pathlib import Path
from typing import Optional
from rich.console import Console
from rich.table import Table
from rich import print as rprint

# Import from shared modules  
from ..shared.config import AppConfig, ConfigManager
from ..shared.data_generator import DataGenerator

# Import domain-specific managers
from .manager import DatasetManager, MediaAssetManager, BigQueryManager

console = Console()


@click.group()
@click.option('--config', type=click.Path(exists=True, path_type=Path), help='Configuration file path')
@click.option('--schema', type=click.Path(exists=True, path_type=Path), help='JSON schema file path')
@click.option('--project-id', envvar='VERTEX_PROJECT_ID', help='Google Cloud Project ID')
@click.option('--output-dir', type=click.Path(path_type=Path), help='Output directory for generated files')
@click.option('--log', is_flag=True, help='Enable verbose logging')
@click.pass_context
def main(ctx, config: Optional[Path], schema: Optional[Path], project_id: Optional[str], output_dir: Optional[Path], log: bool):
    """Data Store Management - Dataset creation, ingestion, and document management."""
    
    # Set up logging if requested
    if log:
        import logging
        from pathlib import Path
        
        # Configure both console and file logging
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        # Create output directory if it doesn't exist
        if output_dir:
            from datetime import datetime
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%H%M%S")
            log_file = output_path / f"data_store_{timestamp}.log"
            
            # Configure logging to both file and console
            logging.basicConfig(
                level=logging.INFO,
                format=log_format,
                handlers=[
                    logging.FileHandler(log_file),
                    logging.StreamHandler()
                ]
            )
            console.print(f"[blue]Verbose logging enabled. Log file: {log_file}[/blue]")
        else:
            # Console only
            logging.basicConfig(level=logging.INFO, format=log_format)
    
    # Load configuration
    if config:
        app_config = AppConfig.from_file(config)
    elif schema and project_id:
        app_config = AppConfig.from_env(schema)
        app_config.vertex_ai.project_id = project_id
    else:
        console.print("[red]Error: Must provide either --config file or --schema + --project-id[/red]")
        ctx.exit(1)
    
    # Override output directory if specified
    if output_dir:
        app_config.output_directory = output_dir
    
    ctx.ensure_object(dict)
    ctx.obj['config_manager'] = ConfigManager(app_config)
    ctx.obj['log'] = log


# Dataset commands (from vertex-search dataset)
@main.command('create-dataset')
@click.argument('data_file', type=click.Path(exists=True, path_type=Path))
@click.option('--validate-only', is_flag=True, help='Only validate data without creating dataset')
@click.pass_context
def create_dataset(ctx, data_file: Path, validate_only: bool):
    """Create a dataset from JSON data file."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    dataset_manager = DatasetManager(config_manager)
    
    try:
        # Load schema and data
        schema = config_manager.validate_schema_file()
        data = dataset_manager.load_data_from_file(data_file)
        
        # Validate data
        console.print(f"Validating {len(data)} records against schema...")
        errors = dataset_manager.validate_data(data, schema)
        
        if errors:
            console.print(f"[red]Validation failed with {len(errors)} errors:[/red]")
            for error in errors[:10]:  # Show first 10 errors
                console.print(f"  - {error}")
            if len(errors) > 10:
                console.print(f"  ... and {len(errors) - 10} more errors")
            ctx.exit(1)
        
        console.print("[green]✓ Data validation successful[/green]")
        
        if validate_only:
            console.print("Validation complete. Use without --validate-only to create dataset.")
            return
        
        # Create dataset
        success = dataset_manager.create_dataset(data, schema)
        if success:
            console.print("[green]✓ Dataset created successfully[/green]")
        else:
            console.print("[red]✗ Failed to create dataset[/red]")
            ctx.exit(1)
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('generate-dataset')
@click.option('--count', default=1000, help='Number of records to generate')
@click.option('--output', type=click.Path(path_type=Path), help='Output file path')
@click.option('--seed', type=int, help='Random seed for reproducible data')
@click.pass_context
def generate_dataset(ctx, count: int, output: Optional[Path], seed: Optional[int]):
    """Generate sample data based on the schema."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    
    try:
        schema = config_manager.validate_schema_file()
        generator = DataGenerator()
        
        console.print(f"Generating {count} sample records...")
        data = generator.generate_sample_data(schema, count, seed)
        
        if output:
            output_path = output
        else:
            output_path = config_manager.config.output_directory / "generated_data.json"
        
        dataset_manager = DatasetManager(config_manager)
        dataset_manager.save_data_to_file(data, output_path)
        
        console.print(f"[green]✓ Generated {len(data)} records saved to {output_path}[/green]")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


# Datastore commands (from vertex-search datastore)
@main.command('create')
@click.argument('data_store_id')
@click.option('--display-name', help='Display name for the data store')
@click.option('--solution-type', default='SEARCH', type=click.Choice(['SEARCH', 'RECOMMENDATION']), help='Solution type for the data store')
@click.pass_context
def create_datastore(ctx, data_store_id: str, display_name: Optional[str], solution_type: str):
    """Create a new data store in Vertex AI."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    asset_manager = MediaAssetManager(config_manager)
    
    display_name = display_name or data_store_id
    
    try:
        success = asset_manager.create_data_store(data_store_id, display_name, solution_type)
        if success:
            console.print(f"[green]✓ {solution_type.title()} data store '{data_store_id}' created successfully[/green]")
        else:
            console.print(f"[red]✗ Failed to create {solution_type.lower()} data store '{data_store_id}'[/red]")
            ctx.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('list')
@click.argument('data_store_id')
@click.option('--count', default=10, help='Number of documents to list')
@click.pass_context
def list_documents(ctx, data_store_id: str, count: int):
    """List documents in a data store to verify import."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    asset_manager = MediaAssetManager(config_manager)
    
    try:
        result = asset_manager.list_documents(data_store_id, count)
        
        if 'error' in result:
            console.print(f"[red]Error: {result['error']}[/red]")
            ctx.exit(1)
        
        console.print(f"[green]Found {result['count']} documents in data store '{data_store_id}'[/green]")
        
        if result['documents']:
            table = Table(title=f"Documents in {data_store_id}")
            table.add_column("Document ID", style="cyan")
            table.add_column("Resource Name", style="green")
            
            for doc in result['documents']:
                table.add_row(doc['id'], doc['name'])
            
            console.print(table)
        else:
            console.print("[yellow]No documents found. Documents may still be indexing.[/yellow]")
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('get-document')
@click.argument('data_store_id')
@click.argument('document_id')
@click.option('--json', 'output_json', is_flag=True, help='Output the full document as JSON.')
@click.pass_context
def get_document(ctx, data_store_id: str, document_id: str, output_json: bool):
    """Get a single document from a data store to inspect its data."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    asset_manager = MediaAssetManager(config_manager)
    
    try:
        result = asset_manager.get_document(data_store_id, document_id)
        
        if 'error' in result:
            console.print(f"[red]Error: {result['error']}[/red]")
            ctx.exit(1)
        
        if output_json:
            # Using print to avoid rich formatting issues with JSON
            print(json.dumps(result, indent=2))
        else:
            console.print(f"[green]Document '{document_id}' from data store '{data_store_id}':[/green]")
            console.print(result)
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('upload-gcs')
@click.argument('data_file', type=click.Path(exists=True, path_type=Path))
@click.argument('bucket_name')
@click.option('--folder', default="vertex-ai-search", help='Folder path in the bucket')
@click.option('--create-bucket', is_flag=True, help='Create bucket if it does not exist')
@click.pass_context  
def upload_to_gcs(ctx, data_file: Path, bucket_name: str, folder: str, create_bucket: bool):
    """Upload documents to Cloud Storage for import."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    asset_manager = MediaAssetManager(config_manager)
    dataset_manager = DatasetManager(config_manager)
    
    try:
        # Create bucket if requested
        if create_bucket:
            console.print(f"Creating bucket '{bucket_name}' if it doesn't exist...")
            if not asset_manager.create_bucket_if_not_exists(bucket_name):
                console.print(f"[red]✗ Failed to create bucket '{bucket_name}'[/red]")
                ctx.exit(1)
        
        # Load data
        data = dataset_manager.load_data_from_file(data_file)
        console.print(f"Uploading {len(data)} documents to gs://{bucket_name}/{folder}/...")
        
        # Upload to Cloud Storage
        uris = asset_manager.upload_to_cloud_storage(bucket_name, data, folder)
        
        console.print(f"[green]✓ Uploaded {len(uris)} documents to Cloud Storage[/green]")
        console.print(f"[blue]GCS Path: gs://{bucket_name}/{folder}/[/blue]")
        console.print("\nNext steps:")
        console.print(f"1. Import from GCS: data-store --config my_config.json import-gcs {bucket_name} gs://{bucket_name}/{folder}/*")
        console.print(f"2. Check documents: data-store --config my_config.json list {bucket_name}")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('import-gcs')
@click.argument('data_store_id') 
@click.argument('gcs_uri')
@click.option('--data-schema', default='document', help='Data schema: document, content, or custom')
@click.option('--wait', is_flag=True, help='Wait for import to complete')
@click.option('--skip-schema-update', is_flag=True, help='Skip automatic field settings application from config')
@click.pass_context
def import_from_gcs(ctx, data_store_id: str, gcs_uri: str, data_schema: str, wait: bool, skip_schema_update: bool):
    """Import documents from Cloud Storage."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    asset_manager = MediaAssetManager(config_manager)
    
    try:
        console.print(f"Importing from {gcs_uri} to data store '{data_store_id}'...")
        console.print(f"Using data schema: {data_schema}")
        
        operation_id = asset_manager.import_from_cloud_storage(data_store_id, gcs_uri, data_schema)
        console.print(f"[green]✓ Import started with operation ID: {operation_id}[/green]")
        
        if wait:
            console.print("Waiting for import to complete...")
            import time
            while True:
                status = asset_manager.get_import_status(operation_id)
                if status.get('done', False):
                    if 'error' in status:
                        console.print(f"[red]✗ Import failed: {status['error']}[/red]")
                        ctx.exit(1)
                    else:
                        console.print("[green]✓ Import completed successfully[/green]")
                        
                        # Apply field settings from config after successful import
                        if not skip_schema_update:
                            console.print("Applying field settings from config...")
                            schema_success = asset_manager.apply_field_settings_from_config(data_store_id)
                            if schema_success:
                                console.print("[green]✓ Field settings applied from config[/green]")
                            else:
                                console.print("[yellow]⚠ Warning: Failed to apply field settings from config[/yellow]")
                        else:
                            console.print("[blue]ℹ Schema update skipped (use --skip-schema-update flag to apply field settings)[/blue]")
                        break
                time.sleep(10)
        else:
            console.print("Import is running in the background. Check the GCP console for progress.")
            console.print(f"Data should appear in the console at: Cloud Storage > gs://{gcs_uri.split('/')[2]}/")
            if not skip_schema_update:
                console.print("[blue]ℹ Use --wait flag to automatically apply field settings after import completion[/blue]")
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('import-bq')
@click.argument('data_store_id')
@click.option('--wait', is_flag=True, help='Wait for import to complete')
@click.option('--skip-schema-update', is_flag=True, help='Skip automatic field settings application from config')
@click.pass_context
def import_from_bq(ctx, data_store_id: str, wait: bool, skip_schema_update: bool):
    """Import documents from BigQuery."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    asset_manager = MediaAssetManager(config_manager)

    if not config_manager.config.bigquery:
        console.print("[red]Error: BigQuery configuration is missing from your config file.[/red]")
        ctx.exit(1)

    try:
        bq_config = config_manager.config.bigquery
        console.print(f"Importing from BigQuery table {bq_config.project_id}.{bq_config.dataset_id}.{bq_config.table_id}...")
        
        operation_id = asset_manager.import_from_bigquery(data_store_id, bq_config)
        console.print(f"[green]✓ Import started with operation ID: {operation_id}[/green]")
        
        if wait:
            console.print("Waiting for import to complete...")
            import time
            while True:
                status = asset_manager.get_import_status(operation_id)
                if status.get('done', False):
                    if 'error' in status:
                        console.print(f"[red]✗ Import failed: {status['error']}[/red]")
                        ctx.exit(1)
                    else:
                        console.print("[green]✓ Import completed successfully[/green]")
                        
                        # Apply field settings from config after successful import
                        if not skip_schema_update:
                            console.print("Applying field settings from config...")
                            schema_success = asset_manager.apply_field_settings_from_config(data_store_id)
                            if schema_success:
                                console.print("[green]✓ Field settings applied from config[/green]")
                            else:
                                console.print("[yellow]⚠ Warning: Failed to apply field settings from config[/yellow]")
                        else:
                            console.print("[blue]ℹ Schema update skipped (use --skip-schema-update flag to apply field settings)[/blue]")
                        break
                time.sleep(10)
        else:
            console.print("Import is running in the background. Check the GCP console for progress.")
            if not skip_schema_update:
                console.print("[blue]ℹ Use --wait flag to automatically apply field settings after import completion[/blue]")
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('import')  
@click.argument('data_store_id')
@click.argument('data_file', type=click.Path(exists=True, path_type=Path))
@click.option('--wait', is_flag=True, help='Wait for import to complete')
@click.option('--skip-schema-update', is_flag=True, help='Skip automatic field settings application from config')
@click.pass_context
def import_documents(ctx, data_store_id: str, data_file: Path, wait: bool, skip_schema_update: bool):
    """Import documents to a data store (DEPRECATED: Use upload-gcs + import-gcs instead)."""
    console.print("[yellow]WARNING: Inline import has limited console visibility.[/yellow]")
    console.print("[yellow]Recommended: Use 'upload-gcs' + 'import-gcs' for better reliability.[/yellow]")
    console.print("Continue with inline import? [y/N]: ", nl=False)
    
    import sys
    try:
        response = input().strip().lower()
        if response not in ['y', 'yes']:
            console.print("Import cancelled. Use the Cloud Storage workflow instead:")
            console.print(f"1. data-store --config my_config.json upload-gcs {data_file} your-bucket-name")
            console.print(f"2. data-store --config my_config.json import-gcs {data_store_id} gs://your-bucket-name/vertex-ai-search/*")
            ctx.exit(0)
    except (EOFError, KeyboardInterrupt):
        ctx.exit(0)
    
    config_manager: ConfigManager = ctx.obj['config_manager']
    asset_manager = MediaAssetManager(config_manager)
    dataset_manager = DatasetManager(config_manager)
    
    try:
        # Load and validate data
        data = dataset_manager.load_data_from_file(data_file)
        console.print(f"Importing {len(data)} documents to data store '{data_store_id}'...")
        
        operation_id = asset_manager.import_documents(data_store_id, data)
        console.print(f"Import started with operation ID: {operation_id}")
        
        if wait:
            console.print("Waiting for import to complete...")
            import time
            while True:
                status = asset_manager.get_import_status(operation_id)
                if status.get('done', False):
                    if 'error' in status:
                        console.print(f"[red]✗ Import failed: {status['error']}[/red]")
                        ctx.exit(1)
                    else:
                        console.print("[green]✓ Import completed successfully[/green]")
                        
                        # Apply field settings from config after successful import
                        if not skip_schema_update:
                            console.print("Applying field settings from config...")
                            schema_success = asset_manager.apply_field_settings_from_config(data_store_id)
                            if schema_success:
                                console.print("[green]✓ Field settings applied from config[/green]")
                            else:
                                console.print("[yellow]⚠ Warning: Failed to apply field settings from config[/yellow]")
                        else:
                            console.print("[blue]ℹ Schema update skipped (use --skip-schema-update flag to apply field settings)[/blue]")
                        break
                time.sleep(10)
        else:
            console.print("Use 'data-store status <operation_id>' to check progress")
            if not skip_schema_update:
                console.print("[blue]ℹ Use --wait flag to automatically apply field settings after import completion[/blue]")
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('update-schema')
@click.argument('data_store_id')
@click.pass_context
def update_schema_fields(ctx, data_store_id: str):
    """Apply field settings from config to data store schema."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    verbose = ctx.obj.get('log', False)
    asset_manager = MediaAssetManager(config_manager)
    
    try:
        console.print("Applying field settings from config to schema...")
        success = asset_manager.apply_field_settings_from_config(data_store_id, verbose)
        
        if success:
            console.print(f"[green]✓ Field settings applied to data store '{data_store_id}'[/green]")
            console.print("[blue]Note: Schema re-indexing will occur automatically. This may take time for large datasets.[/blue]")
        else:
            console.print(f"[red]✗ Failed to apply field settings to data store '{data_store_id}'[/red]")
            ctx.exit(1)
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


# BigQuery commands (from vertex-search bq)
@main.command('bq-load')
@click.argument('dataset_and_table')
@click.argument('file_path', type=click.Path(exists=True, path_type=Path))
@click.option('--replace', is_flag=True, help='Overwrite the table if it already exists.')
@click.pass_context
def bq_load(ctx, dataset_and_table: str, file_path: Path, replace: bool):
    """
    Loads a JSON file into a BigQuery table.

    DATASET_AND_TABLE: The destination table in 'dataset_id.table_id' format.
    FILE_PATH: The path to the local JSON data file.
    """
    config_manager: ConfigManager = ctx.obj['config_manager']
    bq_manager = BigQueryManager(config_manager)

    try:
        dataset_id, table_id = dataset_and_table.split('.')
    except ValueError:
        console.print(f"[red]Error: Invalid format for DATASET_AND_TABLE. Expected 'dataset_id.table_id'[/red]")
        ctx.exit(1)

    try:
        success = bq_manager.load_table_from_file(dataset_id, table_id, file_path, replace)
        if not success:
            ctx.exit(1)
    except Exception as e:
        console.print(f"[red]An unexpected error occurred: {str(e)}[/red]")
        ctx.exit(1)


@main.command('bq-schema')
@click.argument('dataset_and_table')
@click.pass_context
def bq_schema(ctx, dataset_and_table: str):
    """
    Display the schema of a BigQuery table.

    DATASET_AND_TABLE: The table in 'dataset_id.table_id' format.
    """
    config_manager: ConfigManager = ctx.obj['config_manager']
    bq_manager = BigQueryManager(config_manager)

    try:
        dataset_id, table_id = dataset_and_table.split('.')
    except ValueError:
        console.print(f"[red]Error: Invalid format for DATASET_AND_TABLE. Expected 'dataset_id.table_id'[/red]")
        ctx.exit(1)

    try:
        schema_info = bq_manager.get_table_schema(dataset_id, table_id)
        
        if 'error' in schema_info:
            console.print(f"[red]Error getting table schema: {schema_info['error']}[/red]")
            ctx.exit(1)
        
        console.print(f"[green]Table {dataset_id}.{table_id} Schema:[/green]")
        console.print(f"[blue]Rows: {schema_info['num_rows']}[/blue]")
        
        table = Table(title="Field Schema")
        table.add_column("Field Name", style="cyan")
        table.add_column("Type", style="green")
        table.add_column("Mode", style="yellow")
        table.add_column("Description", style="white")
        
        # Highlight id field
        id_field = config_manager.schema.id_field
        for field in schema_info['fields']:
            field_name = field['name']
            if field_name == id_field:
                # Highlight the id field
                field_name = f"[bold red]{field_name}[/bold red]"
                field_type = f"[bold red]{field['field_type']}[/bold red]"
                field_mode = f"[bold red]{field['mode']}[/bold red]"
            else:
                field_type = field['field_type']
                field_mode = field['mode']
            
            table.add_row(
                field_name,
                field_type,
                field_mode,
                field.get('description', '')
            )
        
        console.print(table)
        
        # Check if id field meets Vertex AI requirements
        id_field_schema = next((f for f in schema_info['fields'] if f['name'] == id_field), None)
        if id_field_schema:
            if id_field_schema['field_type'] == 'STRING' and id_field_schema['mode'] == 'REQUIRED':
                console.print(f"[green]✓ ID field '{id_field}' is properly configured for Vertex AI (STRING REQUIRED)[/green]")
            else:
                console.print(f"[red]✗ ID field '{id_field}' is not properly configured for Vertex AI[/red]")
                console.print(f"[red]  Expected: STRING REQUIRED, Found: {id_field_schema['field_type']} {id_field_schema['mode']}[/red]")
        else:
            console.print(f"[red]✗ ID field '{id_field}' not found in table[/red]")
            
    except Exception as e:
        console.print(f"[red]An unexpected error occurred: {str(e)}[/red]")
        ctx.exit(1)


@main.command('bq-fix-schema')
@click.argument('dataset_and_table')
@click.pass_context
def bq_fix_schema(ctx, dataset_and_table: str):
    """
    Fix BigQuery table schema to make id field STRING REQUIRED for Vertex AI compatibility.

    DATASET_AND_TABLE: The table in 'dataset_id.table_id' format.
    """
    config_manager: ConfigManager = ctx.obj['config_manager']
    bq_manager = BigQueryManager(config_manager)

    try:
        dataset_id, table_id = dataset_and_table.split('.')
    except ValueError:
        console.print(f"[red]Error: Invalid format for DATASET_AND_TABLE. Expected 'dataset_id.table_id'[/red]")
        ctx.exit(1)

    try:
        console.print(f"Fixing schema for table {dataset_id}.{table_id}...")
        success = bq_manager.fix_table_schema_for_vertex_ai(dataset_id, table_id)
        
        if success:
            console.print(f"[green]✓ Schema fixed for Vertex AI compatibility[/green]")
            console.print("You can now import this table to Vertex AI.")
        else:
            console.print(f"[red]✗ Failed to fix table schema[/red]")
            console.print("[yellow]BigQuery doesn't allow changing field mode from NULLABLE to REQUIRED.[/yellow]")
            console.print("[blue]Solution: Reload the table to create it with the correct schema:[/blue]")
            console.print(f"[blue]  data-store --config my_config.json bq-load {dataset_and_table} <data_file> --replace[/blue]")
            ctx.exit(1)
            
    except Exception as e:
        console.print(f"[red]An unexpected error occurred: {str(e)}[/red]")
        ctx.exit(1)


if __name__ == '__main__':
    main()