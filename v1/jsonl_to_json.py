#!/usr/bin/env python3
"""Simple utility to convert JSONL to JSON array format"""

import json
import sys
import argparse

def jsonl_to_json(input_file, output_file):
    """Convert JSONL file to JSON array file"""
    data = []

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:  # Skip empty lines
                    try:
                        data.append(json.loads(line))
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

    args = parser.parse_args()
    jsonl_to_json(args.input_file, args.output_file)

if __name__ == '__main__':
    main()