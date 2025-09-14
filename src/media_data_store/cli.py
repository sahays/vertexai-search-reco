"""Command-line interface for Media Data Store Management."""

import click
import json
from pathlib import Path
from typing import Optional, List
from rich.console import Console
from rich.table import Table

from .config import MediaDataStoreConfig, ConfigManager
from .schema_mapper import MediaSchemaMapper
from .bigquery_manager import MediaBigQueryManager
from .datastore_manager import MediaDataStoreManager
from .utils import setup_logging, load_json_file, MediaDataStoreError

console = Console()


@click.group()
@click.option('--config', type=click.Path(exists=True, path_type=Path), required=True, help='Configuration file path')
@click.option('--log-dir', type=click.Path(path_type=Path), help='Directory for log files')
@click.option('--output-dir', type=click.Path(path_type=Path), help='Directory for output files')
@click.pass_context
def main(ctx, config: Path, log_dir: Optional[Path], output_dir: Optional[Path]):
    """Media Data Store - Vertex AI Search for Media data pipeline."""
    
    # Load configuration
    try:
        app_config = MediaDataStoreConfig.from_file(config)
        config_dir = config.parent
    except Exception as e:
        console.print(f"[red]Error loading config: {e}[/red]")
        ctx.exit(1)
    
    # Note: Subcommand-specific logging will be set up in each command
    ctx.ensure_object(dict)
    ctx.obj['config_manager'] = ConfigManager(app_config, config_dir)
    ctx.obj['log_dir'] = log_dir
    ctx.obj['output_dir'] = Path(output_dir) if output_dir else None


@main.command('validate')
@click.option('--data-file', type=click.Path(exists=True, path_type=Path), help='Data file path (uses config default if not specified)')
@click.pass_context
def validate_data(ctx, data_file: Optional[Path]):
    """Validate media data against schema requirements."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    output_dir: Optional[Path] = ctx.obj['output_dir']
    log_dir: Optional[Path] = ctx.obj['log_dir']
    
    # Setup subcommand-specific logging
    logger = setup_logging(log_dir, subcommand='validate')
    logger.info("Media Data Store validate command started")
    
    # Use config default if no data file specified
    if not data_file:
        data_file = config_manager.get_data_file_path()
        logger.debug(f"Using config default data file: {data_file}")
    
    logger.info(f"Starting validation for data file: {data_file}")
    logger.debug(f"Output directory: {output_dir}")
    
    try:
        schema_mapper = MediaSchemaMapper(config_manager)
        data = load_json_file(data_file)
        
        if logger:
            logger.debug(f"Loaded {len(data) if isinstance(data, list) else 1} records from data file")
        
        # Handle both single record and array of records
        if isinstance(data, list):
            console.print(f"[blue]Validating {len(data)} records...[/blue]")
            validation_results = []
            for i, record in enumerate(data):
                result = schema_mapper.validate_data(record, output_dir, 'validate')
                result['record_index'] = i
                validation_results.append(result)
            
            valid_count = sum(1 for r in validation_results if r['valid'])
            console.print(f"[green]✓ {valid_count}/{len(data)} records are valid[/green]")
            
            # Show errors for invalid records
            for result in validation_results:
                if not result['valid']:
                    console.print(f"[red]Record {result['record_index']}: {result['errors']}[/red]")
                    
        else:
            result = schema_mapper.validate_data(data, output_dir, 'validate')
            if result['valid']:
                console.print("[green]✓ Data validation passed[/green]")
            else:
                console.print(f"[red]✗ Validation failed: {result['errors']}[/red]")
                ctx.exit(1)
                
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('transform')
@click.option('--data-file', type=click.Path(exists=True, path_type=Path), help='Data file path (uses config default if not specified)')
@click.option('--mapping', type=click.Path(exists=True, path_type=Path), help='Mapping file path (uses config default if not specified)')
@click.option('--include-original', is_flag=True, default=False, help='Include the original customer data in a field named "original_payload".')
@click.pass_context
def transform_data(ctx, data_file: Optional[Path], mapping: Optional[Path], include_original: bool):
    """Transform customer data to Google media schema format."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    output_dir: Optional[Path] = ctx.obj['output_dir']
    log_dir: Optional[Path] = ctx.obj['log_dir']
    
    # Setup subcommand-specific logging
    logger = setup_logging(log_dir, subcommand='transform')
    logger.info("Media Data Store transform command started")
    
    # Use config defaults if not specified
    if not data_file:
        data_file = config_manager.get_data_file_path()
    if not mapping:
        mapping = config_manager.get_mapping_file_path()
    
    try:
        schema_mapper = MediaSchemaMapper(config_manager)
        data = load_json_file(data_file)
        mapping_config = load_json_file(mapping)
        
        # Handle both single record and array of records
        if isinstance(data, list):
            console.print(f"[blue]Mapping {len(data)} records...[/blue]")
            mapped_data = schema_mapper.map_schema_fields(data, mapping_config, output_dir, 'transform', include_original)
            console.print(f"[green]✓ Successfully mapped {len(mapped_data) if isinstance(mapped_data, list) else 1} records[/green]")
        else:
            mapped_data = schema_mapper.map_schema_fields(data, mapping_config, output_dir, 'transform', include_original)
            console.print("[green]✓ Schema mapping completed[/green]")
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('upload-bq')
@click.argument('data_file', type=click.Path(exists=True, path_type=Path))
@click.argument('dataset_id')
@click.argument('table_id')
@click.pass_context
def upload_bigquery(ctx, data_file: Path, dataset_id: str, table_id: str):
    """Upload processed media data to BigQuery."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    output_dir: Optional[Path] = ctx.obj['output_dir']
    log_dir: Optional[Path] = ctx.obj['log_dir']
    
    # Setup subcommand-specific logging
    logger = setup_logging(log_dir, subcommand='upload')
    logger.info("Media Data Store upload command started")
    
    try:
        bq_manager = MediaBigQueryManager(config_manager)
        data = load_json_file(data_file)
        
        # Ensure data is a list
        if not isinstance(data, list):
            data = [data]
        
        console.print(f"[blue]Uploading {len(data)} records to {dataset_id}.{table_id}...[/blue]")
        
        stats = bq_manager.upload_data(data, dataset_id, table_id, output_dir, 'upload')
        
        console.print(f"[green]✓ Successfully uploaded {stats['rows_loaded']} rows[/green]")
        console.print(f"Job ID: {stats['job_id']}")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('create')
@click.argument('data_store_id')
@click.option('--display-name', help='Display name for the data store')
@click.option('--content-config', default='NO_CONTENT', 
              type=click.Choice(['NO_CONTENT', 'CONTENT_REQUIRED', 'PUBLIC_WEBSITE']),
              help='Content configuration type')
@click.pass_context
def create_datastore(ctx, data_store_id: str, display_name: Optional[str], content_config: str):
    """Create a media data store in Vertex AI Search."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    output_dir: Optional[Path] = ctx.obj['output_dir']
    log_dir: Optional[Path] = ctx.obj['log_dir']
    
    # Setup subcommand-specific logging
    logger = setup_logging(log_dir, subcommand='create')
    logger.info("Media Data Store create command started")
    
    try:
        ds_manager = MediaDataStoreManager(config_manager)
        display_name = display_name or data_store_id
        
        console.print(f"[blue]Creating media data store: {data_store_id}...[/blue]")
        
        result = ds_manager.create_data_store(data_store_id, display_name, content_config, None, output_dir, 'create')
        
        console.print(f"[green]✓ Media data store created: {result['name']}[/green]")
        console.print(f"Industry Vertical: {result['industry_vertical']}")
        console.print(f"Content Config: {result['content_config']}")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('import-bq')
@click.argument('data_store_id')
@click.argument('dataset_id')
@click.argument('table_id')
@click.pass_context
def import_bigquery(ctx, data_store_id: str, dataset_id: str, table_id: str):
    """Import data from BigQuery into the media data store."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    output_dir: Optional[Path] = ctx.obj['output_dir']
    log_dir: Optional[Path] = ctx.obj['log_dir']
    
    # Setup subcommand-specific logging
    logger = setup_logging(log_dir, subcommand='import')
    logger.info("Media Data Store import command started")
    
    try:
        ds_manager = MediaDataStoreManager(config_manager)
        
        console.print(f"[blue]Starting BigQuery import for {data_store_id}...[/blue]")
        
        result = ds_manager.import_bigquery_data(data_store_id, dataset_id, table_id, output_dir, 'import')
        
        console.print(f"[green]✓ Import initiated[/green]")
        console.print(f"Operation: {result['operation_name']}")
        console.print(f"Source: {result['source_table']}")
        console.print("[yellow]Use 'status' command to check progress[/yellow]")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('status')
@click.argument('operation_name')
@click.pass_context
def check_status(ctx, operation_name: str):
    """Check the status of an import operation."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    output_dir: Optional[Path] = ctx.obj['output_dir']
    
    try:
        ds_manager = MediaDataStoreManager(config_manager)
        
        status_info = ds_manager.get_import_status(operation_name, output_dir)
        
        console.print(f"[blue]Operation: {status_info['operation_name']}[/blue]")
        console.print(f"Status: {status_info['status']}")
        
        if status_info['status'] == 'COMPLETED':
            console.print("[green]✓ Import completed successfully[/green]")
            if 'result' in status_info:
                console.print("Result details available in output file")
        elif status_info['status'] == 'FAILED':
            console.print("[red]✗ Import failed[/red]")
            if 'error' in status_info:
                console.print(f"Error: {status_info['error']}")
        else:
            console.print("[yellow]⏳ Import in progress...[/yellow]")
            if 'metadata' in status_info:
                console.print("Progress details available in output file")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('list')
@click.pass_context
def list_datastores(ctx):
    """List all media data stores in the project."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    
    try:
        ds_manager = MediaDataStoreManager(config_manager)
        data_stores = ds_manager.list_data_stores()
        
        if not data_stores:
            console.print("[yellow]No data stores found[/yellow]")
            return
        
        table = Table(title="Media Data Stores")
        table.add_column("ID", style="cyan")
        table.add_column("Display Name", style="green")
        table.add_column("Industry", style="yellow")
        table.add_column("Content Config", style="blue")
        
        for ds in data_stores:
            ds_id = ds['name'].split('/')[-1]
            table.add_row(
                ds_id,
                ds['display_name'],
                ds['industry_vertical'],
                ds['content_config']
            )
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('info')
@click.argument('data_store_id')
@click.pass_context
def datastore_info(ctx, data_store_id: str):
    """Get detailed information about a specific data store."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    
    try:
        ds_manager = MediaDataStoreManager(config_manager)
        info = ds_manager.get_data_store_info(data_store_id)
        
        console.print(f"[bold]Data Store: {data_store_id}[/bold]")
        console.print(f"Display Name: {info['display_name']}")
        console.print(f"Industry Vertical: {info['industry_vertical']}")
        console.print(f"Content Config: {info['content_config']}")
        if info['create_time']:
            console.print(f"Created: {info['create_time']}")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('create-schema')
@click.argument('data_store_id')
@click.option('--schema-definition', type=click.Path(exists=True, path_type=Path), 
              help='JSON file with schema field definitions')
@click.pass_context
def create_custom_schema(ctx, data_store_id: str, schema_definition: Optional[Path]):
    """Create a custom schema for the media data store."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    output_dir: Optional[Path] = ctx.obj['output_dir']
    logger = ctx.obj.get('logger')
    
    try:
        ds_manager = MediaDataStoreManager(config_manager)
        
        # Load schema definition if provided, otherwise use default
        if schema_definition:
            schema_def = load_json_file(schema_definition)
            if logger:
                logger.debug(f"Loaded custom schema definition from: {schema_definition}")
        else:
            # Generate default schema based on config mappings
            field_mappings = config_manager.schema.field_mappings
            schema_def = {
                field_mappings.title_source_field: {"type": "string", "maxLength": 1000},
                field_mappings.uri_source_field: {"type": "string", "maxLength": 5000},
                field_mappings.categories_source_field: {"type": "array", "items": {"type": "string"}},
                field_mappings.available_time_source_field: {"type": "string", "format": "date-time"},
                field_mappings.duration_source_field: {"type": "string"}
            }
            if field_mappings.content_source_field:
                schema_def[field_mappings.content_source_field] = {"type": "string"}
            if field_mappings.language_source_field:
                schema_def[field_mappings.language_source_field] = {"type": "string"}
            if field_mappings.persons_source_field:
                schema_def[field_mappings.persons_source_field] = {"type": "array", "items": {"type": "object"}}
            if field_mappings.organizations_source_field:
                schema_def[field_mappings.organizations_source_field] = {"type": "array", "items": {"type": "object"}}
                
            if logger:
                logger.debug("Generated default schema definition from field mappings")
        
        console.print(f"[blue]Creating custom schema for data store: {data_store_id}...[/blue]")
        
        result = ds_manager.create_custom_schema(data_store_id, schema_def, output_dir)
        
        console.print(f"[green]✓ Custom schema created: {result['schema_name']}[/green]")
        console.print(f"Fields configured: {result['field_count']}")
        console.print(f"Required fields: {', '.join(result['required_fields'])}")
        if result['optional_fields']:
            console.print(f"Optional fields: {', '.join(result['optional_fields'])}")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('update-schema')
@click.argument('data_store_id')
@click.pass_context
def update_schema(ctx, data_store_id: str):
    """Update the data store schema based on the configuration file."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    output_dir: Optional[Path] = ctx.obj['output_dir']
    log_dir: Optional[Path] = ctx.obj['log_dir']
    
    # Setup subcommand-specific logging
    logger = setup_logging(log_dir, subcommand='update-schema')
    logger.info("Media Data Store update-schema command started")
    
    try:
        ds_manager = MediaDataStoreManager(config_manager)
        
        console.print(f"[blue]Updating schema for data store: {data_store_id}...[/blue]")
        
        result = ds_manager.apply_schema_from_config(data_store_id, output_dir, 'update-schema')
        
        console.print(f"[green]✓ Schema update operation started: {result['operation_name']}[/green]")
        console.print(f"Check the Google Cloud Console for the status of this long-running operation.")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


if __name__ == '__main__':
    main()