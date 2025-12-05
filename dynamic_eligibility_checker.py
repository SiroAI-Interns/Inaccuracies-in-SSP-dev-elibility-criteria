import os
import re
import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


# STATIC INPUT CSV FILE
# Update this to the CSV you want to use. It must contain a column with NCT IDs:
# one of: nctId, NCT_ID, nct_id, or the first column will be used.
INPUT_CSV_PATH = "long_statements_40plus.csv"


def read_nct_ids_from_csv(csv_path: str):
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


def find_inclusion_exclusion_lists(criteria_section: BeautifulSoup):
    """
    Robustly locate the main Inclusion and Exclusion lists.
    Works for:
    - 'Inclusion Criteria' / 'Exclusion Criteria'
    - '[Inclusion Criteria]' / '[Exclusion Criteria]'
    and similar variants.
    """
    inclusion_list = None
    exclusion_list = None

    # Preferred container
    desc_div = criteria_section.find("div", {"id": "eligibility-criteria-description"})
    search_root = desc_div if desc_div else criteria_section

    def _find_list_after_heading(root, pattern):
        heading = root.find(
            True,
            string=lambda t: isinstance(t, str)
            and re.search(pattern, t.strip(), re.I),
        )
        if not heading:
            return None
        # Move forward to the next <ol> or <ul>, skipping other tags/text
        sib = heading
        while sib:
            sib = sib.find_next_sibling()
            if not sib:
                break
            if sib.name in ("ol", "ul"):
                return sib
        return None

    # Patterns allow optional brackets and flexible spacing
    incl_pattern = r"\[?\s*Inclusion\s+Criteria\s*\]?"
    excl_pattern = r"\[?\s*Exclusion\s+Criteria\s*\]?"

    inclusion_list = _find_list_after_heading(search_root, incl_pattern)
    exclusion_list = _find_list_after_heading(search_root, excl_pattern)

    # Fallbacks: older structures
    if criteria_section and inclusion_list is None:
        incl_div = criteria_section.find("div", {"id": "eligibility-criteria-inclusion"})
        if incl_div:
            inclusion_list = incl_div.find(["ol", "ul"]) or incl_div

    if criteria_section and exclusion_list is None:
        excl_div = criteria_section.find("div", {"id": "eligibility-criteria-exclusion"})
        if excl_div:
            exclusion_list = excl_div.find(["ol", "ul"]) or excl_div

    # Last resort: look for first two big lists inside the description block
    if (inclusion_list is None or exclusion_list is None) and search_root:
        lists = search_root.find_all(["ol", "ul"])
        if lists:
            if inclusion_list is None and len(lists) >= 1:
                inclusion_list = lists[0]
            if exclusion_list is None and len(lists) >= 2:
                exclusion_list = lists[1]

    return inclusion_list, exclusion_list


def analyze_list(list_root: BeautifulSoup, criteria_type: str):
    """
    Analyze a single Inclusion/Exclusion list.
    - Top-level <li> under the main <ol>/<ul> are considered primary statements.
    - If any descendant <ul>/<ol> is found inside that <li>, it is marked as having sub-bullets.
    Completely agnostic to ordered vs unordered nesting.
    """
    results = {
        "correct": [],
        "incorrect": [],
        "correct_count": 0,
        "incorrect_count": 0,
    }

    if not list_root:
        return results

    # If a container div was passed, drill down to first list
    if list_root.name not in ("ol", "ul"):
        first_list = list_root.find(["ol", "ul"])
        if not first_list:
            return results
        list_root = first_list

    # Only direct li children are primary bullets
    top_items = list_root.find_all("li", recursive=False)

    for item in top_items:
        # Full visible text including nested content
        text = " ".join(item.stripped_strings)
        if not text:
            continue

        # Any nested list of any kind counts as sub-bullets
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


def fetch_and_analyze_nct(nct_id: str, driver, delay: float = 2.5):
    """
    Fetch a single NCT page with Selenium and analyze eligibility criteria.
    """
    url = f"https://clinicaltrials.gov/study/{nct_id}?term={nct_id}&rank=1#participation-criteria"

    print(f"\n{'=' * 70}")
    print(f"Processing NCT ID: {nct_id}")
    print(f"URL: {url}")
    print(f"{'=' * 70}")

    try:
        driver.get(url)

        # Wait for participation section or any mention of Inclusion/Exclusion
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "participation-criteria"))
            )
        except Exception:
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located(
                        (By.XPATH, "//*[contains(text(), 'Inclusion') or contains(text(), 'Exclusion')]")
                    )
                )
            except Exception:
                print("Warning: eligibility section not clearly found, parsing full page.")

        time.sleep(delay)

        soup = BeautifulSoup(driver.page_source, "html.parser")

        criteria_section = soup.find("div", {"id": "participation-criteria"})
        if not criteria_section:
            criteria_section = soup.find("section", {"id": "participation-criteria"})

        if not criteria_section:
            heading = soup.find(string=re.compile("Eligibility Criteria", re.I))
            if heading:
                criteria_section = heading.find_parent("div")

        if not criteria_section:
            print("ERROR: participation-criteria section not found; skipping this NCT.")
            return None

        incl_list, excl_list = find_inclusion_exclusion_lists(criteria_section)

        incl_results = analyze_list(incl_list, "Inclusion")
        excl_results = analyze_list(excl_list, "Exclusion")

        return incl_results, excl_results

    except Exception as e:
        print(f"ERROR while processing {nct_id}: {e}")
        return None


def main():
    """
    Static batch script:
    - Reads NCT IDs from INPUT_CSV_PATH
    - Uses Selenium to load each ClinicalTrials.gov page
    - Dynamically detects ANY nested bullet structures (ordered/unordered)
    - Writes incorrect statements with NCT IDs
    - Prints and saves a final accuracy summary.
    """
    csv_path = INPUT_CSV_PATH
    delay = 2.5
    limit = 2100  # e.g. 10 for quick test, or None for all

    if not os.path.exists(csv_path):
        print(f"ERROR: CSV file not found: {csv_path}")
        return

    nct_ids = read_nct_ids_from_csv(csv_path)
    if limit:
        nct_ids = nct_ids[:limit]

    if not nct_ids:
        print("ERROR: No NCT IDs found in CSV.")
        return

    print(f"Found {len(nct_ids)} NCT IDs.")

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )

    driver = webdriver.Chrome(options=chrome_options)

    all_rows = []
    incorrect_rows = []

    try:
        for idx, nct in enumerate(nct_ids, 1):
            print(f"\n[{idx}/{len(nct_ids)}] {nct}")
            res = fetch_and_analyze_nct(nct, driver, delay=delay)
            if res is None:
                continue

            inc_res, exc_res = res

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
    finally:
        driver.quit()

    results_df = pd.DataFrame(all_rows)
    incorrect_df = pd.DataFrame(incorrect_rows)

    results_df.to_csv("dynamic_eligibility_results.csv", index=False)
    incorrect_df.to_csv("dynamic_incorrect_statements.csv", index=False)

    total_statements = results_df["total_statements"].sum()
    total_incorrect = results_df["incorrect_statements"].sum()
    total_correct = results_df["correct_statements"].sum()
    accuracy = (total_correct / total_statements * 100) if total_statements else 0.0

    print("\n" + "=" * 70)
    print("FINAL SUMMARY")
    print("=" * 70)
    print(f"Total NCT IDs processed: {len(results_df)}")
    print(f"Total statements: {total_statements}")
    print(f"  Correct statements: {total_correct}")
    print(f"  Incorrect statements (with sub-bullets): {total_incorrect}")
    print(f"Accuracy ratio: {accuracy:.2f}%")
    print("=" * 70)

    summary_df = pd.DataFrame(
        [
            {
                "total_nct_ids": len(results_df),
                "total_statements": total_statements,
                "correct_statements": total_correct,
                "incorrect_statements": total_incorrect,
                "accuracy_ratio": accuracy,
            }
        ]
    )
    summary_df.to_csv("dynamic_eligibility_summary.csv", index=False)


if __name__ == "__main__":
    main()


