import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def safe_filename_from_url(url: str, headers: dict) -> str:
    # Try Content-Disposition
    cd = headers.get("Content-Disposition") or headers.get("content-disposition")
    if cd and "filename=" in cd:
        name = cd.split("filename=")[1].strip().strip("\"'")
    else:
        parsed = urlparse(url)
        name = os.path.basename(parsed.path) or f"file_{abs(hash(url)) % 100000}"

    if "." not in name:
        name += ".bin"

    invalid = '<>:"/\\|?*'
    for ch in invalid:
        name = name.replace(ch, "_")
    return name


def ensure_unique_path(base_dir: Path, filename: str) -> Path:
    target = base_dir / filename
    if not target.exists():
        return target
    stem = target.stem
    suffix = target.suffix
    counter = 1
    while True:
        cand = base_dir / f"{stem}_{counter}{suffix}"
        if not cand.exists():
            return cand
        counter += 1


def download_one(url: str, out_dir: Path, timeout=(20, 120), chunk_size=64 * 1024) -> dict:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, stream=True)
        resp.raise_for_status()
        filename = safe_filename_from_url(url, resp.headers)
        target_path = ensure_unique_path(out_dir, filename)

        size_header = resp.headers.get("Content-Length") or resp.headers.get("content-length")
        if size_header:
            try:
                mb = int(size_header) / (1024 * 1024)
                size_info = f" ~{mb:.1f} MB"
            except Exception:
                size_info = ""
        else:
            size_info = ""

        print(f"[download] {filename}{size_info}", flush=True)

        with open(target_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)

        return {
            "url": url,
            "status": "success",
            "filename": target_path.name,
            "path": str(target_path),
        }

    except Exception as e:
        return {"url": url, "status": "error", "error": str(e), "filename": None, "path": None}


def main():
    parser = argparse.ArgumentParser(description="Download files from CSV in parallel (local).")
    parser.add_argument("--csv", "-c", default="argentina_files.csv", help="Input CSV with file_url column.")
    parser.add_argument("--out-dir", "-o", default="downloads", help="Output directory for files.")
    parser.add_argument("--workers", "-w", type=int, default=6, help="Number of parallel downloads.")
    parser.add_argument("--limit", "-l", type=int, help="Process only first N URLs (testing).")
    args = parser.parse_args()

    if not os.path.exists(args.csv):
        print(f"ERROR: CSV not found: {args.csv}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(args.csv)
    if "file_url" not in df.columns:
        print("ERROR: CSV must contain 'file_url' column.", file=sys.stderr)
        sys.exit(1)

    urls = [u for u in df["file_url"].dropna().tolist() if isinstance(u, str) and u.strip()]
    if args.limit:
        urls = urls[: args.limit]

    total = len(urls)
    print(f"Total URLs: {total}")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    start = time.time()
    results = []
    done = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_map = {executor.submit(download_one, url, out_dir): url for url in urls}
        for future in as_completed(future_map):
            res = future.result()
            results.append(res)
            done += 1
            status = res.get("status", "unknown")
            fname = res.get("filename") or os.path.basename(urlparse(res.get("url", "")).path)
            print(f"[done {done}/{total}] {fname} -> {status}", flush=True)

    elapsed = time.time() - start
    successes = sum(1 for r in results if r["status"] == "success")
    failures = total - successes

    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print(f"Total: {total}")
    print(f"Success: {successes}")
    print(f"Failed: {failures}")
    print(f"Elapsed: {elapsed:.2f}s")

    # Write a results CSV
    pd.DataFrame(results).to_csv("download_local_results.csv", index=False)
    print("Saved results to download_local_results.csv")


if __name__ == "__main__":
    main()
