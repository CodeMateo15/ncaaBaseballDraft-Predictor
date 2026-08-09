"""
Fetch MLB Draft data from StatsAPI and save the raw JSON response.
Usage:
    python fetch_draft_json.py                     # just 2026
    python fetch_draft_json.py --years 2024 2025   # multiple years
    python fetch_draft_json.py --output ./data/     # custom output directory
"""

import argparse
import json
import os
import time

import requests

STATSAPI_DRAFT_URL = "https://statsapi.mlb.com/api/v1/draft/{year}"
SLEEP_SEC = 0.15


def main():
    parser = argparse.ArgumentParser(description="Fetch raw MLB Draft JSON from StatsAPI")
    parser.add_argument(
        "--years", nargs="+", type=int, default=[2026],
        help="Draft year(s) to fetch (default: 2026)"
    )
    parser.add_argument(
        "--output", type=str, default=".",
        help="Output directory (default: current directory)"
    )
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": "MLB-draft-research/1.0 (academic use)"})

    for year in sorted(args.years):
        url = STATSAPI_DRAFT_URL.format(year=year)
        print(f"Fetching {year} draft from {url}...")

        try:
            r = session.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()

            # Count picks for summary
            total_picks = sum(
                len(rd.get("picks", []))
                for rd in data.get("drafts", {}).get("rounds", [])
            )

            outpath = os.path.join(args.output, f"statsapi_draft_{year}.json")
            with open(outpath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)

            print(f"  ✅ {total_picks} picks → {outpath}")

        except Exception as e:
            print(f"  ✗ Failed: {e}")

        time.sleep(SLEEP_SEC)


if __name__ == "__main__":
    main()