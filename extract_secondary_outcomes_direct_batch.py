import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any

from extract_secondary_outcomes_direct import fetch_secondary_outcomes_direct


def extract_nct_ids_from_ctg_file(file_path: str) -> List[str]:
    """
    Extract all NCT IDs from ctg-studies.json.

    The file structure is tricky (very long lines, sometimes multiple JSON
    objects per line), so instead of json.loads(line) we:
      - read the whole file as text
      - use a regex to find all occurrences of `"nctId":"..."`
    """
    with open(file_path, "r", encoding="utf-8") as f:
        text = f.read()

    # Find all nctId values
    ids = re.findall(r'"nctId"\s*:\s*"([^"]+)"', text)

    # Preserve order but remove duplicates
    seen = set()
    unique_ids: List[str] = []
    for nid in ids:
        if nid not in seen:
            seen.add(nid)
            unique_ids.append(nid)

    return unique_ids


def process_nct_id(nct_id: str) -> List[Dict[str, Any]]:
    """
    Wrapper to call fetch_secondary_outcomes_direct for a single NCT ID.
    """
    print(f"Processing {nct_id}...")
    outcomes = fetch_secondary_outcomes_direct(nct_id)
    if outcomes:
        print(f"  Found {len(outcomes)} secondary outcome(s)")
    else:
        print(f"  No secondary outcomes found")
    return outcomes


def main() -> None:
    input_file = "ctg-studies.json"
    output_file = "secondary_outcomes_direct_NSCLC_all.json"
    # Use multiple workers for parallel processing with a small delay to balance speed and rate limits
    max_workers = 5
    delay_between_ids = 0.3  # seconds between completed requests

    print(f"Reading NCT IDs from {input_file}...")
    nct_ids = extract_nct_ids_from_ctg_file(input_file)
    print(f"Found {len(nct_ids)} NCT IDs")

    if not nct_ids:
        print("No NCT IDs found. Exiting.")
        return

    all_outcomes: List[Dict[str, Any]] = []
    failed_nct_ids: List[str] = []
    processed_count = 0

    print(f"\nProcessing {len(nct_ids)} NCT IDs with {max_workers} worker(s)...")
    print("Using direct extraction (no HTML parsing or splitting)\n")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_nct = {
            executor.submit(process_nct_id, nct_id): nct_id for nct_id in nct_ids
        }

        for future in as_completed(future_to_nct):
            nct_id = future_to_nct[future]
            try:
                outcomes = future.result()
                if outcomes:
                    all_outcomes.extend(outcomes)
                else:
                    failed_nct_ids.append(nct_id)
            except Exception as e:
                print(f"Error processing {nct_id}: {e}")
                failed_nct_ids.append(nct_id)

            processed_count += 1
            print(f"Processed {processed_count}/{len(nct_ids)} NCT IDs")

            # Delay between each NCT ID to keep requests smooth
            time.sleep(delay_between_ids)

    print(f"\nSaving results to {output_file}...")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_outcomes, f, indent=2, ensure_ascii=False)

    print("\nSummary:")
    print(f"  Total NCT IDs processed: {len(nct_ids)}")
    print(f"  Total secondary outcomes extracted: {len(all_outcomes)}")
    print(f"  NCT IDs with no secondary outcomes: {len(failed_nct_ids)}")

    if failed_nct_ids:
        print("\nExample NCT IDs with no secondary outcomes:")
        for nid in failed_nct_ids[:10]:
            print(f"  - {nid}")
        if len(failed_nct_ids) > 10:
            print(f"  ... and {len(failed_nct_ids) - 10} more")


if __name__ == "__main__":
    main()

