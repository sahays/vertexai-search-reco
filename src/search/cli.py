"""Command-line interface for Search Management."""

import click
import json
from pathlib import Path
from typing import Optional
from rich.console import Console
from rich.table import Table

# Import from shared modules  
from ..shared.config import AppConfig, ConfigManager

# Import domain-specific managers
from .manager import SearchManager

console = Console()


@click.group()
@click.option('--config', type=click.Path(exists=True, path_type=Path), help='Configuration file path')
@click.option('--schema', type=click.Path(exists=True, path_type=Path), help='JSON schema file path')
@click.option('--project-id', envvar='VERTEX_PROJECT_ID', help='Google Cloud Project ID')
@click.option('--output-dir', type=click.Path(path_type=Path), help='Output directory for generated files')
@click.option('--log', is_flag=True, help='Enable verbose logging')
@click.pass_context
def main(ctx, config: Optional[Path], schema: Optional[Path], project_id: Optional[str], output_dir: Optional[Path], log: bool):
    """Search Management - Search engine creation and querying."""
    
    # Set up logging if requested
    if log:
        import logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
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


@main.command('create-engine')
@click.argument('engine_id')
@click.argument('data_store_ids', nargs=-1, required=True)
@click.option('--display-name', help='Display name for the engine')
@click.option('--solution-type', default='SEARCH', type=click.Choice(['SEARCH', 'RECOMMENDATION']), help='Solution type for the engine')
@click.pass_context
def create_search_engine(ctx, engine_id: str, data_store_ids: tuple, display_name: Optional[str], solution_type: str):
    """Create a search engine connected to data stores."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    search_manager = SearchManager(config_manager)
    
    display_name = display_name or engine_id
    
    try:
        success = search_manager.create_search_engine(engine_id, display_name, list(data_store_ids), solution_type)
        if success:
            console.print(f"[green]✓ {solution_type.title()} engine '{engine_id}' created successfully[/green]")
        else:
            console.print(f"[red]✗ Failed to create {solution_type.lower()} engine '{engine_id}'[/red]")
            ctx.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('query')
@click.argument('query')
@click.option('--engine-id', help='Search engine ID')
@click.option('--filters', help='Search filters as JSON string')
@click.option('--facets', help='Facets to include (comma-separated)')
@click.option('--page-size', default=10, help='Number of results per page')
@click.option('--json', 'output_json', is_flag=True, help='Output results as JSON')
@click.pass_context
def search_query(ctx, query: str, engine_id: Optional[str], filters: Optional[str], 
                 facets: Optional[str], page_size: int, output_json: bool):
    """Perform a search query."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    search_manager = SearchManager(config_manager)
    
    engine_id = engine_id or config_manager.config.vertex_ai.engine_id
    if not engine_id:
        console.print("[red]Error: Engine ID is required[/red]")
        ctx.exit(1)
    
    try:
        # Parse filters and facets
        filter_dict = json.loads(filters) if filters else None
        facet_list = facets.split(',') if facets else None
        
        results = search_manager.search(
            query=query,
            engine_id=engine_id,
            filters=filter_dict,
            facets=facet_list,
            page_size=page_size
        )
        
        if output_json:
            # Using print instead of console.print to avoid rich formatting
            print(json.dumps(results, indent=2))
            return

        # Display results
        if 'results' in results and results['results']:
            table = Table(title=f"Search Results for: {query}")
            table.add_column("ID", style="cyan")
            table.add_column("Title", style="green")
            table.add_column("Score", style="yellow")
            
            id_field = config_manager.schema.id_field
            title_field = config_manager.schema.title_field

            for result in results['results']:
                doc = result.get('document', {})
                struct_data = doc.get('structData', {})
                score = result.get('score', 'N/A')
                
                # Correctly get the ID and Title from structData
                doc_id = struct_data.get(id_field, doc.get('id', 'N/A'))
                title = struct_data.get(title_field, 'N/A')
                
                table.add_row(str(doc_id), str(title), str(score))
            
            console.print(table)
            
            if 'facets' in results and results['facets']:
                console.print("\n[bold]Facets:[/bold]")
                for facet in results['facets']:
                    console.print(f"  {facet}")
        else:
            console.print("No results found")
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


if __name__ == '__main__':
    main()