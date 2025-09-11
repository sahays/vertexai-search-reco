"""Command-line interface for the Data Preparation Tool."""

import click
import json
import logging
from pathlib import Path
from datetime import datetime
from .processor import DataProcessor

def setup_logging(log_path: Path):
    """Configures logging to a file and the console."""
    # Remove all handlers associated with the root logger object.
    for handler in logging.getLogger().handlers[:]:
        logging.getLogger().removeHandler(handler)
        
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_path,
        filemode='w'
    )
    # Also add a handler to print messages to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(formatter)
    logging.getLogger().addHandler(console_handler)

@click.group()
@click.pass_context
def main(ctx):
    """A CLI tool for cleaning and flattening JSON data for search indexing."""
    ctx.obj = {'timestamp': datetime.now().strftime("%H%M%S")}

@main.command()
@click.argument('schema_file', type=click.Path(exists=True, path_type=Path))
@click.option('--flat-deep', is_flag=True, help='Recursively flatten nested objects.')
@click.option('--flat-array', is_flag=True, help='Flatten arrays of strings into a single string.')
@click.option('--array-delimiter', default=' ', help='Delimiter to use when flattening arrays. Defaults to a space.')
@click.option('--output-dir', type=click.Path(path_type=Path), help='Directory to save the output file.')
@click.option('--log', is_flag=True, help='Enable logging to a file in the output directory.')
@click.pass_context
def schema(ctx, schema_file: Path, flat_deep: bool, flat_array: bool, array_delimiter: str, output_dir: Path, log: bool):
    """
    Generates a new, flattened JSON schema based on an original schema.
    """
    timestamp = ctx.obj['timestamp']
    source_stem = schema_file.stem
    
    if log and not output_dir:
        raise click.UsageError("You must provide --output-dir to enable logging.")

    if log and output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        log_path = output_dir / f"{source_stem}_log_{timestamp}.log"
        setup_logging(log_path)

    logger = logging.getLogger(__name__)

    logger.info(f"Loading schema from {schema_file}...")
    with open(schema_file, 'r', encoding='utf-8') as f:
        original_schema = json.load(f)
    
    processor = DataProcessor(original_schema, flat_deep=flat_deep, flat_array=flat_array, array_delimiter=array_delimiter)
    new_schema = processor.generate_flattened_schema()
    
    if output_dir:
        output_path = output_dir / f"{source_stem}_schema_{timestamp}.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(new_schema, f, indent=2)
        logger.info(f"Flattened schema saved to: {output_path}")
    else:
        print(json.dumps(new_schema, indent=2))

@main.command()
@click.argument('schema_file', type=click.Path(exists=True, path_type=Path))
@click.argument('input_file', type=click.Path(exists=True, path_type=Path))
@click.argument('output_file', type=click.Path(path_type=Path), required=False)
@click.option('--flat-deep', is_flag=True, help='Recursively flatten nested objects.')
@click.option('--flat-array', is_flag=True, help='Flatten arrays of strings into a single string.')
@click.option('--array-delimiter', default=' ', help='Delimiter to use when flattening arrays. Defaults to a space.')
@click.option('--output-dir', type=click.Path(path_type=Path), help='Directory to save the output file (optional).')
@click.option('--log', is_flag=True, help='Enable logging to a file in the output directory.')
@click.pass_context
def process(ctx, schema_file: Path, input_file: Path, output_file: Path, flat_deep: bool, flat_array: bool, array_delimiter: str, output_dir: Path, log: bool):
    """
    Cleans and flattens a JSON data file based on a schema.
    """
    timestamp = ctx.obj['timestamp']
    source_stem = input_file.stem

    if not output_file and not output_dir:
        raise click.UsageError("You must provide either an OUTPUT_FILE or --output-dir.")

    if log and not output_dir:
        raise click.UsageError("You must provide --output-dir to enable logging.")

    if log and output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        log_path = output_dir / f"{source_stem}_log_{timestamp}.log"
        setup_logging(log_path)

    logger = logging.getLogger(__name__)

    if output_dir:
        output_path = output_dir / f"{source_stem}_data_{timestamp}.json"
    else:
        output_path = output_file

    logger.info(f"Loading schema from {schema_file}...")
    with open(schema_file, 'r', encoding='utf-8') as f:
        schema = json.load(f)
    
    processor = DataProcessor(schema, flat_deep=flat_deep, flat_array=flat_array, array_delimiter=array_delimiter)
    
    logger.info(f"Reading data from {input_file}...")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    logger.info(f"Processing {len(data)} records...")
    processed_data = [processor.process_record(record) for record in data]

    logger.info(f"Writing processed data to {output_path}...")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(processed_data, f, indent=2)

    logger.info(f"Data processing complete. Output saved to {output_path}")

if __name__ == '__main__':
    main()
