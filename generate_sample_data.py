#!/usr/bin/env python3
"""Generate sample data for testing."""

import json
import sys
from pathlib import Path

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent))

from vertex_search.data_generator import DataGenerator


def main():
    """Generate sample drama shorts data."""
    schema_path = Path("examples/drama_shorts_schema.json")
    output_path = Path("examples/sample_data.json")
    
    if not schema_path.exists():
        print(f"Error: Schema file not found at {schema_path}")
        return 1
    
    # Load schema
    with open(schema_path) as f:
        schema = json.load(f)
    
    # Generate sample data
    generator = DataGenerator()
    print("Generating 1000 sample drama shorts records...")
    
    data = generator.generate_sample_data(schema, count=1000, seed=42)
    
    # Save to file
    output_path.parent.mkdir(exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2, default=str)
    
    print(f"Generated {len(data)} records saved to {output_path}")
    
    # Show a sample record
    if data:
        print("\nSample record:")
        print(json.dumps(data[0], indent=2, default=str))
    
    return 0


if __name__ == "__main__":
    sys.exit(main())