import os
import re
import time
from typing import Optional, Tuple, Dict, Any, List

import pandas as pd
import requests
from bs4 import BeautifulSoup


# STATIC INPUT CSV FILE (same as other methods)
INPUT_CSV_PATH = "long_statements_25plus.csv"

# ClinicalTrials.gov internal API endpoint (per NCT ID)
API_TEMPLATE = "https://clinicaltrials.gov/api/int/studies/{nct_id}?history=true"

# HTTP settings
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 25
MAX_RETRIES = 3


def read_nct_ids_from_csv(csv_path: str) -> List[str]:
    df = pd.read_csv(csv_path)

    if "nctId" in df.columns:
        col = "nctId"
    elif "NCT_ID" in df.columns:
        col = "NCT_ID"
    elif "nct_id" in df.columns:
        col = "nct_id"
    else:
        col = df.columns[0]

    ncts = [str(v).strip() for v in df[col].dropna().unique().tolist()]
    return [n for n in ncts if n.startswith("NCT")]


def fetch_eligibility_html(nct_id: str, session: requests.Session) -> Optional[str]:
    """
    Call ClinicalTrials.gov internal API to get the EligibilityCriteria HTML
    for one study.

    Path in JSON:
      study.protocolSection.eligibilityModule.eligibilityCriteria
    (user example had a small typo; we check both keys defensively)
    """
    url = API_TEMPLATE.format(nct_id=nct_id)
    headers = {"User-Agent": USER_AGENT}

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()

            study = data.get("study", {})
            proto = study.get("protocolSection", {})

            # correct key
            elig_mod = proto.get("eligibilityModule")
            # fallback key if API ever exposes the typo
            if elig_mod is None:
                elig_mod = proto.get("elgibilityModule")

            if not elig_mod:
                raise RuntimeError("eligibilityModule not found in response")

            html = elig_mod.get("eligibilityCriteria")
            if not html:
                raise RuntimeError("eligibilityCriteria missing/empty")
            return str(html)
        except Exception as e:
            last_exc = e
            time.sleep(0.8 * attempt)

    print(f"ERROR API for {nct_id}: {last_exc}")
    return None


def find_inclusion_exclusion_lists_from_html(html: str) -> Tuple[Optional[BeautifulSoup], Optional[BeautifulSoup]]:
    """
    From eligibilityCriteria HTML, locate Inclusion and Exclusion lists.

    Typical structure in API HTML:
      <p>Inclusion Criteria:</p>
      <ol>...</ol>
      <p>Exclusion Criteria:</p>
      <ol>...</ol>
    """
    soup = BeautifulSoup(html, "html.parser")
    root = soup

    def _find_list_after_heading(pattern: str) -> Optional[BeautifulSoup]:
        heading = root.find(
            string=lambda t: isinstance(t, str) and re.search(pattern, t.strip(), re.I)
        )
        if not heading:
            return None
        parent = heading.parent  # often <p> or similar
        sib = parent
        while sib:
            sib = sib.find_next_sibling()
            if not sib:
                break
            if sib.name in ("ol", "ul"):
                return sib
        return None

    incl_list = _find_list_after_heading(r"Inclusion\s+Criteria")
    excl_list = _find_list_after_heading(r"Exclusion\s+Criteria")

    # Fallback: if still missing, just take first and second <ol>/<ul> in block
    if incl_list is None or excl_list is None:
        all_lists = root.find_all(["ol", "ul"])
        if all_lists:
            if incl_list is None and len(all_lists) >= 1:
                incl_list = all_lists[0]
            if excl_list is None and len(all_lists) >= 2:
                excl_list = all_lists[1]

    return incl_list, excl_list


def analyze_list(list_root: Optional[BeautifulSoup], criteria_type: str) -> Dict[str, Any]:
    """
    Analyze a single Inclusion/Exclusion list from HTML:
    - Top-level <li> are primary statements.
    - Any nested <ol>/<ul> marks a statement as having sub-bullets.
    """
    results = {
        "correct": [],
        "incorrect": [],
        "correct_count": 0,
        "incorrect_count": 0,
    }

    if not list_root:
        return results

    if list_root.name not in ("ol", "ul"):
        first_list = list_root.find(["ol", "ul"])
        if not first_list:
            return results
        list_root = first_list

    top_items = list_root.find_all("li", recursive=False)

    for item in top_items:
        text = " ".join(item.stripped_strings)
        if not text:
            continue

        nested = item.find(["ol", "ul"])
        has_sub = nested is not None

        record = {
            "text": text,
            "type": criteria_type,
            "has_sub_bullet": has_sub,
        }

        if has_sub:
            results["incorrect"].append(record)
            results["incorrect_count"] += 1
        else:
            results["correct"].append(record)
            results["correct_count"] += 1

    return results


def main():
    """
    API-based script:
    - Reads NCT IDs from INPUT_CSV_PATH
    - For each ID (up to 10), calls the JSON API to get eligibilityCriteria HTML
    - Parses Inclusion/Exclusion lists and detects nested bullets
    - Times each ID and total runtime
    """
    csv_path = INPUT_CSV_PATH
    limit = 2889  # Only first 10 NCT IDs

    if not os.path.exists(csv_path):
        print(f"ERROR: CSV file not found: {csv_path}")
        return

    nct_ids = read_nct_ids_from_csv(csv_path)
    if limit:
        nct_ids = nct_ids[:limit]

    if not nct_ids:
        print("ERROR: No NCT IDs found in CSV.")
        return

    print(f"Found {len(nct_ids)} NCT IDs (API mode, limit={limit}).")

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    all_rows = []
    incorrect_rows = []
    per_id_times: List[Tuple[str, float]] = []

    t0 = time.perf_counter()

    for idx, nct in enumerate(nct_ids, 1):
        print(f"\n[{idx}/{len(nct_ids)}] {nct}")
        start = time.perf_counter()

        html = fetch_eligibility_html(nct, session)
        if not html:
            end = time.perf_counter()
            per_id_times.append((nct, end - start))
            continue

        incl_list, excl_list = find_inclusion_exclusion_lists_from_html(html)
        inc_res = analyze_list(incl_list, "Inclusion")
        exc_res = analyze_list(excl_list, "Exclusion")

        total_correct = inc_res["correct_count"] + exc_res["correct_count"]
        total_incorrect = inc_res["incorrect_count"] + exc_res["incorrect_count"]
        total = total_correct + total_incorrect

        all_rows.append(
            {
                "nctId": nct,
                "total_statements": total,
                "correct_statements": total_correct,
                "incorrect_statements": total_incorrect,
            }
        )

        for rec in inc_res["incorrect"]:
            incorrect_rows.append(
                {
                    "nctId": nct,
                    "type": "Inclusion",
                    "statement": rec["text"],
                }
            )
        for rec in exc_res["incorrect"]:
            incorrect_rows.append(
                {
                    "nctId": nct,
                    "type": "Exclusion",
                    "statement": rec["text"],
                }
            )

        end = time.perf_counter()
        per_id_times.append((nct, end - start))

    t1 = time.perf_counter()
    total_runtime = t1 - t0

    results_df = pd.DataFrame(all_rows)
    incorrect_df = pd.DataFrame(incorrect_rows)

    results_df.to_csv("dynamic_eligibility_results_api.csv", index=False)
    incorrect_df.to_csv("dynamic_incorrect_statements_api.csv", index=False)

    timing_df = pd.DataFrame(
        [{"nctId": n, "seconds": secs} for (n, secs) in per_id_times]
    )
    timing_df.to_csv("dynamic_eligibility_timing_api.csv", index=False)

    total_statements = results_df["total_statements"].sum() if not results_df.empty else 0
    total_incorrect = results_df["incorrect_statements"].sum() if not results_df.empty else 0
    total_correct = results_df["correct_statements"].sum() if not results_df.empty else 0
    accuracy = (total_correct / total_statements * 100) if total_statements else 0.0

    avg_time_per_id = timing_df["seconds"].mean() if not timing_df.empty else 0.0

    print("\n" + "=" * 70)
    print("API FINAL SUMMARY")
    print("=" * 70)
    print(f"Total NCT IDs requested: {len(nct_ids)}")
    print(f"Total NCT IDs processed successfully: {len(results_df)}")
    print(f"Total statements: {total_statements}")
    print(f"  Correct statements: {total_correct}")
    print(f"  Incorrect statements (with sub-bullets): {total_incorrect}")
    print(f"Accuracy ratio: {accuracy:.2f}%")
    print(f"Total runtime (seconds): {total_runtime:.2f}")
    print(f"Average time per NCT (seconds): {avg_time_per_id:.2f}")
    print("=" * 70)

    summary_df = pd.DataFrame(
        [
            {
                "total_nct_ids": len(results_df),
                "requested_nct_ids": len(nct_ids),
                "total_statements": total_statements,
                "correct_statements": total_correct,
                "incorrect_statements": total_incorrect,
                "accuracy_ratio": accuracy,
                "total_runtime_seconds": total_runtime,
                "avg_time_per_nct_seconds": avg_time_per_id,
            }
        ]
    )
    summary_df.to_csv("dynamic_eligibility_summary_api.csv", index=False)


if __name__ == "__main__":
    main()


