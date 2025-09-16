"""Command-line interface for Media Search."""

import click
import asyncio
from pathlib import Path
from typing import Optional
from rich.console import Console

from .config import SearchCLIConfig, ConfigManager
from .search_manager import SearchManager
from media_data_store.utils import setup_logging

console = Console()

@click.group()
@click.option('--config', type=click.Path(exists=True, path_type=Path), required=True, help='Configuration file path (e.g., examples/customer_media_config.json)')
@click.option('--log-dir', type=click.Path(path_type=Path), help='Directory for log files')
@click.option('--output-dir', type=click.Path(path_type=Path), help='Directory for output files')
@click.pass_context
def main(ctx, config: Path, log_dir: Optional[Path], output_dir: Optional[Path]):
    """Media Search - A CLI for Vertex AI Search."""
    try:
        app_config = SearchCLIConfig.from_file(config)
    except Exception as e:
        console.print(f"[red]Error loading config: {e}[/red]")
        ctx.exit(1)
    
    ctx.ensure_object(dict)
    ctx.obj['config_manager'] = ConfigManager(app_config)
    ctx.obj['log_dir'] = log_dir
    ctx.obj['output_dir'] = Path(output_dir) if output_dir else None

@main.command("search")
@click.argument('engine_id')
@click.argument('query_text')
@click.option('--filter', 'filters', multiple=True, help='Filter expression (e.g., "categories:Drama"). Can be repeated.')
@click.option('--facet-field', 'facet_fields', multiple=True, help='Field to request facets for. Can be repeated.')
@click.option('--page-size', type=int, default=10, help='Number of results per page.')
@click.option('--offset', type=int, default=0, help='Starting offset for results.')
@click.option('--json', 'json_output', is_flag=True, help='Output the full JSON response to a file in --output-dir.')
@click.pass_context
def search(ctx, engine_id: str, query_text: str, filters: list[str], facet_fields: list[str], page_size: int, offset: int, json_output: bool):
    """Execute a search query."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    output_dir: Optional[Path] = ctx.obj['output_dir']
    log_dir: Optional[Path] = ctx.obj['log_dir']
    
    if json_output and not output_dir:
        console.print("[red]Error: --output-dir is required when using the --json flag.[/red]")
        ctx.exit(1)
        
    logger = setup_logging(log_dir, subcommand='search')
    logger.info("Media Search query command started")

    try:
        manager = SearchManager(config_manager)
        
        # Process filters into the required API format
        processed_filters = []
        operators = ['>=', '<=', '>', '<', '=']
        for f in filters:
            found_op = None
            for op in operators:
                if op in f:
                    found_op = op
                    break
            
            # Handle range/comparison filters (e.g., for dates or numbers)
            if found_op:
                parts = f.split(found_op, 1)
                key = parts[0].strip()
                value = parts[1].strip()
                # Values in comparisons must be quoted if they are not numbers.
                # It's safest to quote them to handle date-times correctly.
                processed_filters.append(f'{key} {found_op} "{value}"')
            # Handle simple key:value filters for array fields, which need the ANY() syntax.
            elif ":" in f:
                key, value = f.split(":", 1)
                processed_filters.append(f'{key}:ANY("{value}")')
            else:
                # Pass through any other filter format that the user provides manually
                processed_filters.append(f)
        
        filter_expression = " AND ".join(processed_filters) if processed_filters else None
        
        # Run the async search function
        results = asyncio.run(manager.search(
            query=query_text,
            engine_id=engine_id,
            filter_expression=filter_expression,
            facet_fields=list(facet_fields),
            page_size=page_size,
            offset=offset,
            json_output=json_output,
            output_dir=output_dir
        ))
        
        # Display results
        console.print(f"\n[bold green]Found {results['total_size']} results for '{query_text}'[/bold green]")
        
        for i, item in enumerate(results.get('results', [])):
            console.print(f"\n[bold cyan]Result {i+1}:[/bold cyan]")
            struct_data = item.get('document', {}).get('structData', {})
            console.print(f"  [bold]ID:[/bold] {item.get('id')}")
            if struct_data.get('title'):
                console.print(f"  [bold]Title:[/bold] {struct_data.get('title')}")
            if struct_data.get('desc'):
                console.print(f"  [bold]Description:[/bold] {struct_data.get('desc')[:200]}...")
        
        if results.get('facets'):
            console.print("\n[bold yellow]Facets:[/bold yellow]")
            for facet in results['facets']:
                console.print(f"  [bold]{facet['key']}:[/bold]")
                for value in facet.get('values', []):
                    console.print(f"    - {value['value']} ({value['count']})")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        ctx.exit(1)




@main.command()
@click.argument('engine_id')
@click.argument('query_text')
@click.option('--user-id', default='default-user', help='A unique identifier for the end user.')
@click.option('--json', 'json_output', is_flag=True, help='Output the full JSON response to a file in --output-dir.')
@click.pass_context
def autocomplete(ctx, engine_id: str, query_text: str, user_id: str, json_output: bool):
    """Get autocomplete suggestions."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    output_dir: Optional[Path] = ctx.obj['output_dir']
    log_dir: Optional[Path] = ctx.obj['log_dir']
    
    if json_output and not output_dir:
        console.print("[red]Error: --output-dir is required when using the --json flag.[/red]")
        ctx.exit(1)
        
    logger = setup_logging(log_dir, subcommand='autocomplete')
    logger.info("Media Search autocomplete command started")

    try:
        manager = SearchManager(config_manager)
        
        # Only pass the output_dir if the json flag is set
        effective_output_dir = output_dir if json_output else None
        
        suggestions = asyncio.run(manager.autocomplete(
            query=query_text,
            engine_id=engine_id,
            user_id=user_id,
            output_dir=effective_output_dir
        ))
        
        console.print(f"\n[bold green]Suggestions for '{query_text}':[/bold green]")
        for suggestion in suggestions:
            console.print(f"- {suggestion}")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        ctx.exit(1)




@main.group()
@click.pass_context
def track(ctx):
    """Track a user event."""
    pass

@track.command(name="search")
@click.pass_context
def track_search(ctx):
    """Track a search event."""
    console.print("[bold blue]Track search event placeholder[/bold blue]")

@track.command(name="view-item")
@click.pass_context
def track_view_item(ctx):
    """Track a view-item (click) event."""
    console.print("[bold blue]Track view-item event placeholder[/bold blue]")

if __name__ == '__main__':
    main()
