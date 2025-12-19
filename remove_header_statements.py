import argparse
import json
import re
from typing import List, Dict, Any

import pandas as pd


def is_unwanted_statement(statement: str) -> bool:
    """
    Check if a statement matches unwanted header/boilerplate patterns.
    
    Patterns to match:
    - "Key Inclusion Criteria:"
    - "Key Exclusion Criteria:"
    - "Main Inclusion Criteria:"
    - "Main Exclusion Criteria:"
    - "Inclusion Criteria"
    - "Exclusion Criteria"
    - "Inclusion Criteria:"
    - "Exclusion Criteria:"
    - "INCLUSION CRITERIA"
    - "EXCLUSION CRITERIA"
    - "[Inclusion Criteria]"
    - "[Exclusion Criteria]"
    - "Key Inclusion criteria (Core phase)"
    - "Key Exclusion criteria (Core phase)"
    - "Additional Inclusion Criteria for..."
    - "Additional Exclusion Criteria for..."
    - "Index Case Investigation Inclusion Criteria:"
    - "Index Case Investigation Exclusion Criteria:"
    """
    if not statement or not isinstance(statement, str):
        return False
    
    # Normalize: strip whitespace
    normalized = statement.strip()
    if not normalized:
        return False
    
    lower = normalized.lower()
    
    # Pattern 1: "Key Inclusion Criteria:" or "Key Exclusion Criteria:" (with optional colon)
    if re.match(r'^key\s+(inclusion|exclusion)\s+criteria\s*:?\s*$', lower):
        return True
    
    # Pattern 1b: "Main Inclusion Criteria:" or "Main Exclusion Criteria:" (with optional colon)
    if re.match(r'^main\s+(inclusion|exclusion)\s+criteria\s*:?\s*$', lower):
        return True
    
    # Pattern 2: "Key Inclusion criteria (Core phase)" or "Key Exclusion criteria (Core phase)"
    if re.match(r'^key\s+(inclusion|exclusion)\s+criteria\s*\([^)]+\)\s*:?\s*$', lower):
        return True
    
    # Pattern 3: "Inclusion Criteria" or "Exclusion Criteria" (with optional colon)
    if re.match(r'^(inclusion|exclusion)\s+criteria\s*:?\s*$', lower):
        return True
    
    # Pattern 4: "[Inclusion Criteria]" or "[Exclusion Criteria]"
    if re.match(r'^\[(inclusion|exclusion)\s+criteria\]\s*:?\s*$', lower):
        return True
    
    # Pattern 5: "INCLUSION CRITERIA" or "EXCLUSION CRITERIA" (all caps)
    if normalized.isupper() and re.match(r'^(INCLUSION|EXCLUSION)\s+CRITERIA\s*:?\s*$', normalized):
        return True
    
    # Pattern 6: "Additional Inclusion Criteria for..." or "Additional Exclusion Criteria for..."
    if re.match(r'^additional\s+(inclusion|exclusion)\s+criteria\s+for\s+.*\s*:?\s*$', lower):
        return True
    
    # Pattern 7: "Index Case Investigation Inclusion Criteria:" or "Index Case Investigation Exclusion Criteria:"
    if re.match(r'^index\s+case\s+investigation\s+(inclusion|exclusion)\s+criteria\s*:?\s*$', lower):
        return True
    
    return False


def remove_unwanted_statements(data: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Remove rows with unwanted header statements.
    
    Returns:
        (cleaned_data, removed_rows)
    """
    cleaned = []
    removed = []
    
    for record in data:
        statement = record.get("statement", "")
        if is_unwanted_statement(statement):
            removed.append(record)
        else:
            cleaned.append(record)
    
    return cleaned, removed


def main():
    parser = argparse.ArgumentParser(
        description="Remove unwanted header/boilerplate statements from JSON file"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input JSON file path",
    )
    parser.add_argument(
        "--output-json",
        help="Output cleaned JSON file path (default: input filename with '_cleaned' suffix)",
    )
    parser.add_argument(
        "--output-csv",
        help="Output CSV file with removed rows (default: input filename with '_removed.csv' suffix)",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Preview removed statements without saving files",
    )
    
    args = parser.parse_args()
    
    # Read input JSON
    print(f"Reading JSON from: {args.input}")
    with open(args.input, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    print(f"Total records in input: {len(data)}")
    
    # Remove unwanted statements
    cleaned_data, removed_rows = remove_unwanted_statements(data)
    
    print(f"\nRemoved {len(removed_rows)} unwanted statements")
    print(f"Remaining records: {len(cleaned_data)}")
    
    # Preview removed statements
    if removed_rows:
        print("\nSample of removed statements:")
        for i, row in enumerate(removed_rows[:10], 1):
            print(f"  {i}. [{row.get('nctId', 'N/A')}] {row.get('statement', '')[:80]}")
        if len(removed_rows) > 10:
            print(f"  ... and {len(removed_rows) - 10} more")
    
    if args.preview:
        print("\nPreview mode - no files saved.")
        return
    
    # Determine output paths
    base_name = args.input.rsplit(".", 1)[0] if "." in args.input else args.input
    
    output_json = args.output_json or f"{base_name}_cleaned.json"
    output_csv = args.output_csv or f"{base_name}_removed.csv"
    
    # Save cleaned JSON
    print(f"\nSaving cleaned JSON to: {output_json}")
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(cleaned_data, f, indent=2, ensure_ascii=False)
    
    # Save removed rows to CSV
    if removed_rows:
        print(f"Saving removed rows to CSV: {output_csv}")
        df_removed = pd.DataFrame(removed_rows)
        df_removed.to_csv(output_csv, index=False, encoding="utf-8")
    else:
        print("No rows removed - skipping CSV creation")
    
    # Summary statistics
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Input records: {len(data)}")
    print(f"Removed records: {len(removed_rows)}")
    print(f"Output records: {len(cleaned_data)}")
    print(f"Removal rate: {len(removed_rows) / len(data) * 100:.2f}%")
    print("=" * 70)


if __name__ == "__main__":
    main()

