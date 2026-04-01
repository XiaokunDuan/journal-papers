"""
Fetch all papers from MS / OR / MSOM / POM published in the last 5 years
via the OpenAlex API, including abstracts.

Output: papers.json  (list of {doi, title, journal, year, abstract})
"""

import json
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

# Reverse map: source ID -> abbreviation
SOURCE_TO_ABBR = {v: k for k, v in JOURNALS.items()}

YEAR_START  = 2021
YEAR_END    = 2026
OUTPUT_FILE = "papers.json"


def reconstruct_abstract(inverted_index: dict) -> str:
    """Rebuild abstract text from OpenAlex inverted index format."""
    if not inverted_index:
        return ""
    pairs: list[tuple[int, str]] = []
    for word, positions in inverted_index.items():
        for pos in positions:
            pairs.append((pos, word))
    pairs.sort()
    return " ".join(w for _, w in pairs)


def fetch_all() -> list[dict]:
    source_filter = "|".join(JOURNALS.values())
    base_filter = (
        f"primary_location.source.id:{source_filter},"
        f"publication_year:{YEAR_START}-{YEAR_END}"
    )

    papers: list[dict] = []
    cursor = "*"
    page = 0

    session = requests.Session()
    session.headers.update({"User-Agent": "journal-papers/1.0"})

    while True:
        params = {
            "filter":   base_filter,
            "select":   "id,doi,title,abstract_inverted_index,publication_year,primary_location",
            "per_page": 200,
            "cursor":   cursor,
        }

        for attempt in range(4):
            try:
                resp = session.get(f"{BASE_URL}/works", params=params, timeout=30)
                resp.raise_for_status()
                break
            except Exception as e:
                if attempt == 3:
                    raise
                wait = 2 ** attempt
                print(f"  [retry {attempt+1}] {e} — waiting {wait}s")
                time.sleep(wait)
                session = requests.Session()
                session.headers.update({"User-Agent": "journal-papers/1.0"})
        data = resp.json()

        results = data.get("results", [])
        if not results:
            break

        for work in results:
            doi = work.get("doi") or work.get("id", "")

            # Identify journal abbreviation from source ID
            source = (work.get("primary_location") or {}).get("source") or {}
            source_id = (source.get("id") or "").split("/")[-1]  # e.g. "S33323087"
            journal_abbr = SOURCE_TO_ABBR.get(f"S{source_id.lstrip('S')}", source.get("display_name", ""))

            abstract = reconstruct_abstract(work.get("abstract_inverted_index") or {})

            papers.append({
                "doi":      doi,
                "title":    work.get("title") or "",
                "journal":  journal_abbr,
                "year":     work.get("publication_year"),
                "abstract": abstract,
            })

        page += 1
        total = data.get("meta", {}).get("count", "?")
        print(f"  page {page}: +{len(results)} papers  (total: {len(papers)} / {total})")

        cursor = data.get("meta", {}).get("next_cursor")
        if not cursor:
            break

        time.sleep(0.12)

    return papers


def main():
    print(f"Fetching papers from {', '.join(JOURNALS)} ({YEAR_START}–{YEAR_END}) …\n")
    papers = fetch_all()

    # Stats
    from collections import Counter
    dist = Counter(p["journal"] for p in papers)
    has_abstract = sum(1 for p in papers if p["abstract"])
    print(f"\nTotal: {len(papers)} papers  |  with abstract: {has_abstract}")
    for j, n in sorted(dist.items(), key=lambda x: -x[1]):
        print(f"  {j}: {n}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(papers, f, ensure_ascii=False, indent=2)

    print(f"\nSaved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
