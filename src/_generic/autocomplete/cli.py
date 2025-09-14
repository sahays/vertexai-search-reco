"""Command-line interface for Autocomplete Management."""

import click
from pathlib import Path
from typing import Optional
from rich.console import Console

# Import from shared modules  
from ..shared.config import AppConfig, ConfigManager

# Import domain-specific managers
from .manager import AutocompleteManager

console = Console()


@click.group()
@click.option('--config', type=click.Path(exists=True, path_type=Path), help='Configuration file path')
@click.option('--schema', type=click.Path(exists=True, path_type=Path), help='JSON schema file path')
@click.option('--project-id', envvar='VERTEX_PROJECT_ID', help='Google Cloud Project ID')
@click.option('--output-dir', type=click.Path(path_type=Path), help='Output directory for generated files')
@click.option('--log', is_flag=True, help='Enable verbose logging')
@click.pass_context
def main(ctx, config: Optional[Path], schema: Optional[Path], project_id: Optional[str], output_dir: Optional[Path], log: bool):
    """Autocomplete Management - Suggestion configuration and querying."""
    
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


@main.command('suggest')
@click.argument('query')
@click.option('--engine-id', help='Search engine ID')
@click.pass_context
def get_suggestions(ctx, query: str, engine_id: Optional[str]):
    """Get autocomplete suggestions."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    autocomplete_manager = AutocompleteManager(config_manager)
    
    engine_id = engine_id or config_manager.config.vertex_ai.engine_id
    if not engine_id:
        console.print("[red]Error: Engine ID is required[/red]")
        ctx.exit(1)
    
    try:
        suggestions = autocomplete_manager.get_suggestions(query, engine_id)
        
        if suggestions:
            console.print(f"[bold]Suggestions for '{query}':[/bold]")
            for suggestion in suggestions:
                console.print(f"  • {suggestion}")
        else:
            console.print("No suggestions found")
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


if __name__ == '__main__':
    main()