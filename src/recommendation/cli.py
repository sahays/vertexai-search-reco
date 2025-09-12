"""Command-line interface for Recommendation Management."""

import click
import json
from pathlib import Path
from typing import Optional
from rich.console import Console
from rich.table import Table

# Import from shared modules  
from ..shared.config import AppConfig, ConfigManager

# Import domain-specific managers
from .manager import RecommendationManager

console = Console()


@click.group()
@click.option('--config', type=click.Path(exists=True, path_type=Path), help='Configuration file path')
@click.option('--schema', type=click.Path(exists=True, path_type=Path), help='JSON schema file path')
@click.option('--project-id', envvar='VERTEX_PROJECT_ID', help='Google Cloud Project ID')
@click.option('--output-dir', type=click.Path(path_type=Path), help='Output directory for generated files')
@click.option('--log', is_flag=True, help='Enable verbose logging')
@click.pass_context
def main(ctx, config: Optional[Path], schema: Optional[Path], project_id: Optional[str], output_dir: Optional[Path], log: bool):
    """Recommendation Management - User events and recommendation engines."""
    
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


@main.command('get')
@click.option('--user-id', required=True, help='User pseudo ID')
@click.option('--event-type', default='view', help='Event type (view, purchase, etc.)')
@click.option('--document-ids', help='Document IDs (comma-separated)')
@click.option('--engine-id', help='Search engine ID')
@click.option('--max-results', default=10, help='Maximum number of recommendations')
@click.option('--json', 'output_json', is_flag=True, help='Output results as JSON')
@click.pass_context
def get_recommendations(ctx, user_id: str, event_type: str, document_ids: Optional[str], 
                       engine_id: Optional[str], max_results: int, output_json: bool):
    """Get recommendations for a user."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    recommendation_manager = RecommendationManager(config_manager)
    
    engine_id = engine_id or config_manager.config.vertex_ai.engine_id
    if not engine_id:
        console.print("[red]Error: Engine ID is required[/red]")
        ctx.exit(1)
    
    try:
        user_event = {
            'eventType': event_type,
            'userPseudoId': user_id,
            'documents': document_ids.split(',') if document_ids else []
        }
        
        recommendations = recommendation_manager.get_recommendations(
            user_event, engine_id, max_results
        )
        
        if recommendations:
            if output_json:
                # Using print instead of console.print to avoid rich formatting
                print(json.dumps(recommendations, indent=2))
            else:
                table = Table(title=f"Recommendations for user {user_id}")
                table.add_column("Document ID", style="cyan")
                table.add_column("Title", style="green")
                table.add_column("Score", style="yellow")
                
                for rec in recommendations:
                    doc = rec.get('document', {})
                    score = rec.get('score', 'N/A')
                    doc_id = doc.get('id', 'N/A')
                    # The title might be in a 'structData' sub-dictionary
                    struct_data = doc.get('structData', {})
                    title = struct_data.get('title', doc.get('title', 'N/A'))
                    table.add_row(str(doc_id), str(title), str(score))
                
                console.print(table)
        else:
            console.print("No recommendations found")
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


@main.command('record')
@click.option('--user-id', required=True, help='User pseudo ID')
@click.option('--event-type', required=True, help='Event type (media-play, media-complete, view-item, search, view-home-page)')
@click.option('--document-id', required=True, help='Document ID')
@click.option('--data-store-id', help='Data store ID (for recommendation data store)')
@click.option('--media-progress-duration', type=float, help='Media progress duration in seconds (for media-complete events)')
@click.option('--media-progress-percentage', type=float, help='Media progress percentage (0.0-1.0 or 0-100, for media-complete events)')
@click.pass_context
def record_event(ctx, user_id: str, event_type: str, document_id: str, data_store_id: Optional[str], 
                 media_progress_duration: Optional[float], media_progress_percentage: Optional[float]):
    """Record a user event for recommendation training."""
    config_manager: ConfigManager = ctx.obj['config_manager']
    recommendation_manager = RecommendationManager(config_manager)
    
    # Use configured data_store_id if not provided
    data_store_id = data_store_id or getattr(config_manager.config.vertex_ai, 'recommendation_data_store_id', config_manager.config.vertex_ai.data_store_id)
    
    try:
        success = recommendation_manager.record_user_event(
            event_type=event_type,
            user_pseudo_id=user_id,
            documents=[document_id],
            data_store_id=data_store_id,
            media_progress_duration=media_progress_duration,
            media_progress_percentage=media_progress_percentage
        )
        
        if success:
            console.print(f"[green]✓ User event recorded: {event_type} for user {user_id} on document {document_id}[/green]")
        else:
            console.print(f"[red]✗ Failed to record user event[/red]")
            ctx.exit(1)
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        ctx.exit(1)


if __name__ == '__main__':
    main()