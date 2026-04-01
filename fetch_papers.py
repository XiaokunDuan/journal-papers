"""
Fetch all papers from MS / OR / MSOM / POM published in the last 5 years
via the OpenAlex API, and write a plain list of DOI URLs.

Output: papers.txt  (one URL per line)
"""

import time
import requests

BASE_URL = "https://api.openalex.org"

# UTD top-4 OM journals (OpenAlex Source IDs)
JOURNALS = {
    "MS":   "S33323087",
    "OR":   "S125775545",
    "MSOM": "S81410195",
    "POM":  "S149070780",
}

YEAR_START = 2021
YEAR_END   = 2026
OUTPUT_FILE = "papers.txt"


def fetch_all() -> list[str]:
    """Return a list of DOI URLs for all matching papers."""
    source_filter = "|".join(JOURNALS.values())
    base_filter   = (
        f"primary_location.source.id:{source_filter},"
        f"publication_year:{YEAR_START}-{YEAR_END}"
    )

    urls: list[str] = []
    cursor = "*"
    page   = 0

    session = requests.Session()
    session.headers.update({"User-Agent": "journal-papers/1.0"})

    while True:
        params = {
            "filter":   base_filter,
            "select":   "id,doi,publication_year,primary_location",
            "per_page": 200,
            "cursor":   cursor,
        }

        resp = session.get(f"{BASE_URL}/works", params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        if not results:
            break

        for work in results:
            doi = work.get("doi") or ""
            if doi:
                # doi field is already a full URL: https://doi.org/10.xxxx
                urls.append(doi)
            else:
                # fall back to OpenAlex URL
                urls.append(work.get("id", ""))

        page += 1
        total = data.get("meta", {}).get("count", "?")
        print(f"  page {page}: fetched {len(results)} papers (total so far: {len(urls)} / {total})")

        cursor = data.get("meta", {}).get("next_cursor")
        if not cursor:
            break

        time.sleep(0.12)   # polite rate limit

    return urls


def main():
    print(f"Fetching papers from {', '.join(JOURNALS)} ({YEAR_START}–{YEAR_END}) …")
    urls = fetch_all()
    print(f"\nTotal: {len(urls)} papers")

    with open(OUTPUT_FILE, "w") as f:
        for url in urls:
            f.write(url + "\n")

    print(f"Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
