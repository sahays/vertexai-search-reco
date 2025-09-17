#!/usr/bin/env python3
"""Simple utility to convert JSONL to JSON array format"""

import json
import sys
import argparse

def parse_nested_json_strings(obj):
    """Recursively parse JSON strings within object fields"""
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            if isinstance(value, str):
                # Try to parse string as JSON
                try:
                    parsed = json.loads(value)
                    result[key] = parse_nested_json_strings(parsed)
                except (json.JSONDecodeError, ValueError):
                    # If parsing fails, keep original string
                    result[key] = value
            else:
                result[key] = parse_nested_json_strings(value)
        return result
    elif isinstance(obj, list):
        return [parse_nested_json_strings(item) for item in obj]
    else:
        return obj

def jsonl_to_json(input_file, output_file, parse_nested=False):
    """Convert JSONL file to JSON array file"""
    data = []

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:  # Skip empty lines
                    try:
                        parsed_line = json.loads(line)
                        if parse_nested:
                            parsed_line = parse_nested_json_strings(parsed_line)
                        data.append(parsed_line)
                    except json.JSONDecodeError as e:
                        print(f"Error parsing line {line_num}: {e}", file=sys.stderr)
                        continue

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print(f"Converted {len(data)} lines from {input_file} to {output_file}")

    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='Convert JSONL file to JSON array format')
    parser.add_argument('input_file', help='Input JSONL file')
    parser.add_argument('output_file', help='Output JSON file')
    parser.add_argument('--parse-nested', action='store_true',
                       help='Parse JSON strings within fields into objects')

    args = parser.parse_args()
    jsonl_to_json(args.input_file, args.output_file, args.parse_nested)

if __name__ == '__main__':
    main()