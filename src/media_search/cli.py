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

@main.command()
@click.argument('query_text')
@click.option('--engine-id', required=True, help='The ID of the search engine.')
@click.option('--filter', help='Filter expression JSON string (e.g., \'{\"genre\": \"drama\"}\'')
@click.option('--facets', help='Comma-separated list of fields for faceting.')
@click.option('--page-size', type=int, default=10, help='Number of results per page.')
@click.pass_context
def query(ctx, query_text: str, engine_id: str, filter: Optional[str], facets: Optional[str], page_size: int):
    """Execute a search query."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    output_dir: Optional[Path] = ctx.obj['output_dir']
    log_dir: Optional[Path] = ctx.obj['log_dir']
    
    logger = setup_logging(log_dir, subcommand='search')
    logger.info("Media Search query command started")

    try:
        manager = SearchManager(config_manager)
        
        # Parse facets if provided
        facet_list = facets.split(',') if facets else []
        
        # Run the async search function
        results = asyncio.run(manager.search(
            query=query_text,
            engine_id=engine_id,
            filter_expression=filter,
            facet_fields=facet_list,
            page_size=page_size,
            output_dir=output_dir
        ))
        
        # Display results
        console.print(f"\n[bold green]Found {results['total_size']} results for '{query_text}'[/bold green]")
        
        for i, item in enumerate(results.get('results', [])):
            console.print(f"\n[bold cyan]Result {i+1}:[/bold cyan]")
            console.print(f"  [bold]ID:[/bold] {item.get('id')}")
            if item.get('document', {}).get('title'):
                console.print(f"  [bold]Title:[/bold] {item['document']['title']}")
            if item.get('document', {}).get('plot_summary'):
                console.print(f"  [bold]Summary:[/bold] {item['document']['plot_summary'][:200]}...")
        
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
@click.argument('query_text')
@click.option('--engine-id', required=True, help='The ID of the search engine.')
@click.pass_context
def autocomplete(ctx, query_text: str, engine_id: str):
    """Get autocomplete suggestions."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    output_dir: Optional[Path] = ctx.obj['output_dir']
    log_dir: Optional[Path] = ctx.obj['log_dir']
    
    logger = setup_logging(log_dir, subcommand='autocomplete')
    logger.info("Media Search autocomplete command started")

    try:
        manager = SearchManager(config_manager)
        
        suggestions = asyncio.run(manager.autocomplete(
            query=query_text,
            engine_id=engine_id,
            output_dir=output_dir
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
