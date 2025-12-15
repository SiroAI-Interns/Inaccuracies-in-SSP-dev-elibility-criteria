import argparse
import json
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple

import pandas as pd

# Reuse the heavy-duty extractors from compare_single_nct
from compare_single_nct import (
    fetch_via_api,
    fetch_page,
    find_section,
    extract_all_content_from_section,
    read_json,
)


def load_nct_ids_from_csv(csv_path: str) -> List[str]:
    """Read unique NCT IDs from the provided CSV."""
    df = pd.read_csv(csv_path)
    if "nctId" not in df.columns:
        # try common variants
        for col in ["nct_id", "NCT_ID", "NCTId", "NctId"]:
            if col in df.columns:
                df = df.rename(columns={col: "nctId"})
                break
    if "nctId" not in df.columns:
        raise ValueError("CSV must contain an nctId column")
    ncts = (
        df["nctId"]
        .astype(str)
        .str.strip()
        .loc[lambda s: s.str.startswith("NCT")]
        .unique()
        .tolist()
    )
    return ncts


def compare_sets(site_recs, json_recs) -> Tuple[int, int, List[Dict], List[Dict]]:
    """Return counts and lists of missing in json / missing on site."""
    site_set = {r["statement"].strip().lower() for r in site_recs}
    json_set = {r["statement"].strip().lower() for r in json_recs}

    missing_in_json = site_set - json_set
    missing_on_site = json_set - site_set

    site_map = {r["statement"].strip().lower(): r for r in site_recs}
    json_map = {r["statement"].strip().lower(): r for r in json_recs}

    missing_in_json_list = [site_map[s] for s in missing_in_json if s in site_map]
    missing_on_site_list = [json_map[s] for s in missing_on_site if s in json_map]

    return len(site_recs), len(json_recs), missing_in_json_list, missing_on_site_list


def process_single(
    nct_id: str,
    json_data: List[Dict],
    save_details: bool,
    details_dir: str,
    session_factory,
    include_site_records: bool = False,
) -> Dict:
    """Process one NCT ID: fetch site, extract bullets, compare with JSON."""
    json_recs = [r for r in json_data if r.get("nctId") == nct_id]

    session = session_factory()
    soup = fetch_via_api(nct_id, session)
    if not soup:
        soup = fetch_page(nct_id, session)
    if not soup:
        return {
            "nctId": nct_id,
            "status": "fetch_failed",
            "site_docs": 0,
            "json_docs": len(json_recs),
            "missing_in_json": 0,
            "missing_on_site": 0,
        }

    section = find_section(soup) or soup
    site_recs, _ = extract_all_content_from_section(section, nct_id, order_start=0)
    # Normalize site records to the requested output format (with empty tags)
    normalized_site_recs = [
        {
            "nctId": rec.get("nctId", nct_id),
            "statement": rec.get("statement", "").strip(),
            "type": rec.get("type", ""),
            "tags": {},
        }
        for rec in site_recs
        if rec.get("statement")
    ]

    site_count, json_count, missing_in_json_list, missing_on_site_list = compare_sets(
        site_recs, json_recs
    )

    # Optionally save details
    if save_details:
        os.makedirs(details_dir, exist_ok=True)
        with open(os.path.join(details_dir, f"site_{nct_id}.json"), "w", encoding="utf-8") as f:
            json.dump(site_recs, f, ensure_ascii=False, indent=2)
        if missing_in_json_list:
            with open(
                os.path.join(details_dir, f"missing_in_json_{nct_id}.json"), "w", encoding="utf-8"
            ) as f:
                json.dump(missing_in_json_list, f, ensure_ascii=False, indent=2)
        if missing_on_site_list:
            with open(
                os.path.join(details_dir, f"missing_on_site_{nct_id}.json"), "w", encoding="utf-8"
            ) as f:
                json.dump(missing_on_site_list, f, ensure_ascii=False, indent=2)

    result = {
        "nctId": nct_id,
        "status": "ok",
        "site_docs": site_count,
        "json_docs": json_count,
        "missing_in_json": len(missing_in_json_list),
        "missing_on_site": len(missing_on_site_list),
    }
    if include_site_records:
        result["site_records"] = normalized_site_recs
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Batch compare site bullets vs JSON for all NCT IDs in a CSV."
    )
    parser.add_argument(
        "--csv",
        default="validated_40plus_statements.csv",
        help="Input CSV containing nctId column",
    )
    parser.add_argument(
        "--input",
        "-i",
        default="SSP-dev.eligibility-criteria.json",
        help="Input JSON with eligibility bullets",
    )
    parser.add_argument(
        "--workers", type=int, default=8, help="Number of parallel workers (default: 8)"
    )
    parser.add_argument(
        "--limit", type=int, help="Process only the first N NCT IDs (for quick testing)"
    )
    parser.add_argument(
        "--save-details",
        action="store_true",
        help="Save per-NCT site bullets and missing lists to disk",
    )
    parser.add_argument(
        "--details-dir",
        default="batch_compare_details",
        help="Directory to save per-NCT details when --save-details is set",
    )
    parser.add_argument(
        "--combined-out",
        default="site_bullets_all.json",
        help="Path to write combined site bullets JSON (all NCTs)",
    )
    args = parser.parse_args()

    if not os.path.exists(args.csv):
        raise FileNotFoundError(f"CSV not found: {args.csv}")
    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Input JSON not found: {args.input}")

    nct_ids = load_nct_ids_from_csv(args.csv)
    if args.limit:
        nct_ids = nct_ids[: args.limit]

    print(f"Found {len(nct_ids)} NCT IDs to process.")

    json_data = read_json(args.input)

    # Thread-safe session factory (one session per task)
    session_lock = threading.Lock()

    def make_session():
        import requests

        with session_lock:
            return requests.Session()

    results: List[Dict] = []
    combined_site_records: List[Dict] = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_map = {
            executor.submit(
                process_single,
                nct_id,
                json_data,
                args.save_details,
                args.details_dir,
                make_session,
                True,  # include_site_records
            ): nct_id
            for nct_id in nct_ids
        }

        for future in as_completed(future_map):
            nct = future_map[future]
            try:
                res = future.result()
                results.append(res)
                if "site_records" in res:
                    combined_site_records.extend(res["site_records"])
                print(
                    f"[{nct}] status={res['status']} site={res['site_docs']} json={res['json_docs']} "
                    f"missing_in_json={res['missing_in_json']} missing_on_site={res['missing_on_site']}"
                )
            except Exception as e:
                print(f"[{nct}] error: {e}")
                results.append(
                    {
                        "nctId": nct,
                        "status": f"error: {e}",
                        "site_docs": 0,
                        "json_docs": len([r for r in json_data if r.get('nctId') == nct]),
                        "missing_in_json": 0,
                        "missing_on_site": 0,
                    }
                )

    # Save summary
    summary_df = pd.DataFrame(results)
    summary_df.to_csv("batch_compare_summary.csv", index=False)
    print("\nSaved summary to batch_compare_summary.csv")
    print(summary_df.head())

    # Save combined site bullets JSON (all NCTs)
    with open(args.combined_out, "w", encoding="utf-8") as f:
        json.dump(combined_site_records, f, ensure_ascii=False, indent=2)
    print(f"Saved combined site bullets to {args.combined_out}")


if __name__ == "__main__":
    main()

