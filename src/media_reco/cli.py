"""Command-line interface for Media Recommendations."""

import click
import asyncio
from pathlib import Path
from typing import Optional
from rich.console import Console

from .config import RecoCLIConfig, ConfigManager
from .recommendation_manager import RecommendationManager
from media_data_store.utils import setup_logging

console = Console()

@click.group()
@click.option('--config', type=click.Path(exists=True, path_type=Path), required=True, help='Configuration file path (e.g., examples/customer_media_config.json)')
@click.option('--log-dir', type=click.Path(path_type=Path), help='Directory for log files')
@click.option('--output-dir', type=click.Path(path_type=Path), help='Directory for output files')
@click.pass_context
def main(ctx, config: Path, log_dir: Optional[Path], output_dir: Optional[Path]):
    """Media Recommendations - A CLI for Vertex AI Search for Media."""
    try:
        app_config = RecoCLIConfig.from_file(config)
    except Exception as e:
        console.print(f"[red]Error loading config: {e}[/red]")
        ctx.exit(1)
    
    ctx.ensure_object(dict)
    ctx.obj['config_manager'] = ConfigManager(app_config)
    ctx.obj['log_dir'] = log_dir
    ctx.obj['output_dir'] = Path(output_dir) if output_dir else None

@main.command()
@click.argument('serving_config_id')
@click.option('--document-id', required=True, help='The ID of the document to get recommendations for.')
@click.option('--user-id', required=True, help='A unique identifier for the end user.')
@click.option('--limit', type=int, default=10, help='Number of recommendations to return.')
@click.option('--json', 'json_output', is_flag=True, help='Output the full JSON response to a file in --output-dir.')
@click.pass_context
def recommend(ctx, serving_config_id: str, document_id: str, user_id: str, limit: int, json_output: bool):
    """Get 'More like this' recommendations for a document."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    output_dir: Optional[Path] = ctx.obj['output_dir']
    log_dir: Optional[Path] = ctx.obj['log_dir']

    if json_output and not output_dir:
        console.print(f"[red]Error: --output-dir is required when using the --json flag.[/red]")
        ctx.exit(1)

    logger = setup_logging(log_dir, subcommand='recommend')
    logger.info("Media Recommendations command started")

    try:
        manager = RecommendationManager(config_manager)
        
        effective_output_dir = output_dir if json_output else None

        results = asyncio.run(manager.recommend(
            serving_config_id=serving_config_id,
            document_id=document_id,
            user_id=user_id,
            page_size=limit,
            output_dir=effective_output_dir
        ))

        console.print(f"\n[bold green]Recommendations for document '{document_id}':[/bold green]")
        for i, item in enumerate(results.get('results', [])):
            console.print(f"\n[bold cyan]Recommendation {i+1}:[/bold cyan]")
            struct_data = item.get('document', {}).get('structData', {})
            console.print(f"  [bold]ID:[/bold] {item.get('document', {}).get('id')}")
            if struct_data.get('title'):
                console.print(f"  [bold]Title:[/bold] {struct_data.get('title')}")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        ctx.exit(1)

if __name__ == '__main__':
    main()
