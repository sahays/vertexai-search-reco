"""Command Line Interface for Media Engine management."""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Any

from .config import EngineConfig
from .engine_manager import MediaEngineManager
from media_data_store.utils import setup_logging, MediaDataStoreError


def create_engine_command(args: argparse.Namespace) -> int:
    """Handle create engine command."""
    try:
        config = EngineConfig.from_datastore_config(Path(args.config))
        manager = MediaEngineManager(config)
        
        result = manager.create_engine(
            datastore_id=args.datastore_id,
            engine_id=args.engine_id,
            display_name=args.display_name,
            description=args.description,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            subcommand="create"
        )
        
        print(f"✓ Engine created successfully")
        print(f"Engine ID: {result['engine_id']}")
        print(f"Display Name: {result['display_name']}")
        print(f"Datastore: {args.datastore_id}")
        print(f"Industry Vertical: {result['industry_vertical']}")
        
        return 0
    except MediaDataStoreError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


def get_engine_command(args: argparse.Namespace) -> int:
    """Handle get engine command."""
    try:
        config = EngineConfig.from_datastore_config(Path(args.config))
        manager = MediaEngineManager(config)
        
        result = manager.get_engine(
            engine_id=args.engine_id,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            subcommand="get"
        )
        
        if args.format == "json":
            print(json.dumps(result, indent=2))
        else:
            print(f"Engine ID: {result['engine_id']}")
            print(f"Display Name: {result['display_name']}")
            print(f"Description: {result.get('description', 'N/A')}")
            print(f"Datastore IDs: {', '.join(result['datastore_ids'])}")
            print(f"Industry Vertical: {result['industry_vertical']}")
            print(f"Solution Type: {result['solution_type']}")
            print(f"Created: {result.get('create_time', 'N/A')}")
            print(f"Updated: {result.get('update_time', 'N/A')}")
        
        return 0
    except MediaDataStoreError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


def list_engines_command(args: argparse.Namespace) -> int:
    """Handle list engines command."""
    try:
        config = EngineConfig.from_datastore_config(Path(args.config))
        manager = MediaEngineManager(config)
        
        result = manager.list_engines(
            datastore_id=args.datastore_id,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            subcommand="list"
        )
        
        if args.format == "json":
            print(json.dumps(result, indent=2))
        else:
            print(f"Found {result['total_count']} media search engines")
            if args.datastore_id:
                print(f"Filtered by datastore: {args.datastore_id}")
            print()
            
            for engine in result['engines']:
                print(f"Engine ID: {engine['engine_id']}")
                print(f"  Display Name: {engine['display_name']}")
                print(f"  Description: {engine.get('description', 'N/A')}")
                print(f"  Datastores: {', '.join(engine['datastore_ids'])}")
                print(f"  Created: {engine.get('create_time', 'N/A')}")
                print()
        
        return 0
    except MediaDataStoreError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


def delete_engine_command(args: argparse.Namespace) -> int:
    """Handle delete engine command."""
    try:
        config = EngineConfig.from_datastore_config(Path(args.config))
        manager = MediaEngineManager(config)
        
        result = manager.delete_engine(
            engine_id=args.engine_id,
            force=args.force,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            subcommand="delete"
        )
        
        print(f"✓ Engine deleted successfully")
        print(f"Engine ID: {result['engine_id']}")
        print(f"Deleted at: {result['deleted_at']}")
        
        return 0
    except MediaDataStoreError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Media Search Engine management for Vertex AI Search for Media",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Global arguments
    parser.add_argument("--config", required=True, help="Configuration file path")
    parser.add_argument("--log-dir", help="Directory for log files")
    parser.add_argument("--output-dir", help="Directory for output files")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Create engine command
    create_parser = subparsers.add_parser("create", help="Create a new search engine")
    create_parser.add_argument("datastore_id", help="Datastore ID to link engine to")
    create_parser.add_argument("engine_id", help="Unique engine ID")
    create_parser.add_argument("--display-name", required=True, help="Display name for the engine")
    create_parser.add_argument("--description", help="Engine description")
    create_parser.set_defaults(func=create_engine_command)
    
    # Get engine command
    get_parser = subparsers.add_parser("get", help="Get engine information")
    get_parser.add_argument("engine_id", help="Engine ID")
    get_parser.add_argument("--format", choices=["json", "table"], default="table", help="Output format")
    get_parser.set_defaults(func=get_engine_command)
    
    # List engines command
    list_parser = subparsers.add_parser("list", help="List search engines")
    list_parser.add_argument("datastore_id", nargs="?", help="Filter by datastore ID")
    list_parser.add_argument("--format", choices=["json", "table"], default="table", help="Output format")
    list_parser.set_defaults(func=list_engines_command)
    
    # Delete engine command
    delete_parser = subparsers.add_parser("delete", help="Delete a search engine")
    delete_parser.add_argument("engine_id", help="Engine ID to delete")
    delete_parser.add_argument("--force", action="store_true", help="Force deletion without confirmation")
    delete_parser.set_defaults(func=delete_engine_command)
    
    # Status command (placeholder for future implementation)
    status_parser = subparsers.add_parser("status", help="Check engine status")
    status_parser.add_argument("engine_id", help="Engine ID")
    status_parser.add_argument("--verbose", action="store_true", help="Show detailed status")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Setup logging with proper timestamps and subcommand
    from pathlib import Path
    log_dir = Path(args.log_dir) if args.log_dir else None
    subcommand = args.command if hasattr(args, 'command') else None
    
    if args.verbose:
        import logging
        logging.basicConfig(level=logging.DEBUG)
        logger = setup_logging(log_dir=log_dir, subcommand=subcommand, module_name="engine")
        logger.info("Verbose logging enabled")
        if args.log_dir:
            logger.info(f"Log directory: {args.log_dir}")
        if args.output_dir:
            logger.info(f"Output directory: {args.output_dir}")
    else:
        logger = setup_logging(log_dir=log_dir, subcommand=subcommand, module_name="engine")
        logger.info("Media Engine command started")
    
    # Execute command
    if hasattr(args, 'func'):
        return args.func(args)
    else:
        print(f"Command '{args.command}' not implemented yet", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())