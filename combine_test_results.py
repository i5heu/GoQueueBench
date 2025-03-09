#!/usr/bin/env python3
import os
import json
import argparse

def combine_json_files(directory, output_file):
    combined = []
    # Walk through the directory recursively
    for root, _, files in os.walk(directory):
        for file in files:
            # Check for the file names you expect.
            # Adjust this check if your file naming varies.
            if file.lower() in ["test-result.json", "test-results.json"]:
                filepath = os.path.join(root, file)
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        # If the JSON file contains a list, extend the combined list,
                        # otherwise append the single JSON object.
                        if isinstance(data, list):
                            combined.extend(data)
                        else:
                            combined.append(data)
                    print(f"Processed: {filepath}")
                except Exception as e:
                    print(f"Error reading {filepath}: {e}")
    # Write out the combined data as a valid JSON array.
    with open(output_file, "w", encoding="utf-8") as out:
        json.dump(combined, out, indent=2)
    print(f"\nCombined JSON saved to: {output_file}")
    print(f"Total records combined: {len(combined)}")

def main():
    parser = argparse.ArgumentParser(
        description="Recursively combine all test-result JSON files into one valid JSON file."
    )
    parser.add_argument("directory", help="Directory to search for JSON files")
    parser.add_argument(
        "-o",
        "--output",
        default="combined.json",
        help="Output file name (default: combined.json)",
    )
    args = parser.parse_args()
    combine_json_files(args.directory, args.output)

if __name__ == "__main__":
    main()
