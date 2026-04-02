"""
Fetch all papers from MS / OR / MSOM / POM via OpenAlex.

Modes:
  python3 fetch_papers.py           # full re-fetch (overwrites papers.json)
  python3 fetch_papers.py --update  # incremental: only fetch papers newer than
                                    # the latest year already in papers.json,
                                    # plus re-check the current year

Filters out non-article content (issue records, textbook chapters, editorials,
service awards, "In This Issue" notices, etc.).
"""

import json
import re
import sys
import time
import requests
from collections import defaultdict

BASE_URL = "https://api.openalex.org"

JOURNALS = {
    "MS":   "S33323087",
    "OR":   "S125775545",
    "MSOM": "S81410195",
    "POM":  "S149070780",
}
SOURCE_TO_ABBR = {v: k for k, v in JOURNALS.items()}

YEAR_START  = 2021
OUTPUT_FILE = "papers.json"

# ── Patterns that identify non-article junk ───────────────────────────────────
_JUNK_TITLE = re.compile(
    r"^(in this issue|front matter|back matter|issue information|"
    r"call for papers?|acknowledgment to referees?|"
    r"service award|meritorious service|best ae award|"
    r"software and data for|erratum|corrigendum|correction to|"
    r"introduction to (the )?(special|focused)|introduction:\s|"
    r"from the editor|editorial statement|reinforcing research|"
    r"tributes? for|pom journal biennial|journal biennial)",
    re.IGNORECASE,
)

_JUNK_DOI = re.compile(
    r"10\.1142/9789811239359|"   # OR textbook (World Scientific)
    r"10\.1007/978-3-662-|"      # German OR textbook (Springer)
    r"10\.1007/978-3-658-|"      # another German OR textbook
    r"10\.1111/poms\.v\d+\.\d+|" # POM volume/issue records
    r"10\.1287/opre$",           # bare journal DOI
    re.IGNORECASE,
)

def is_junk(work: dict) -> bool:
    title = (work.get("title") or "").strip()
    doi   = work.get("doi") or ""
    if not title:
        return True
    if _JUNK_TITLE.match(title):
        return True
    if _JUNK_DOI.search(doi):
        return True
    return False


def reconstruct_abstract(inv: dict) -> str:
    if not inv:
        return ""
    pairs = [(pos, w) for w, positions in inv.items() for pos in positions]
    pairs.sort()
    return " ".join(w for _, w in pairs)


def _get(session: requests.Session, endpoint: str, params: dict) -> dict:
    for attempt in range(4):
        try:
            resp = session.get(f"{BASE_URL}/{endpoint}", params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt == 3:
                raise
            wait = 2 ** attempt
            print(f"  [retry {attempt+1}] {e} — waiting {wait}s")
            time.sleep(wait)
            session.headers.update({"User-Agent": "journal-papers/1.0"})
    return {}


def fetch_range(year_start: int, year_end: int) -> list[dict]:
    source_filter = "|".join(JOURNALS.values())
    base_filter = (
        f"primary_location.source.id:{source_filter},"
        f"publication_year:{year_start}-{year_end}"
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
        data    = _get(session, "works", params)
        results = data.get("results", [])
        if not results:
            break

        for work in results:
            if is_junk(work):
                continue

            doi = work.get("doi") or work.get("id", "")
            source = (work.get("primary_location") or {}).get("source") or {}
            source_id = (source.get("id") or "").split("/")[-1]
            journal_abbr = SOURCE_TO_ABBR.get(source_id, source.get("display_name", ""))

            papers.append({
                "doi":      doi,
                "title":    work.get("title") or "",
                "journal":  journal_abbr,
                "year":     work.get("publication_year"),
                "abstract": reconstruct_abstract(work.get("abstract_inverted_index") or {}),
            })

        page += 1
        total = data.get("meta", {}).get("count", "?")
        print(f"  page {page}: +{len(results)} raw → kept {len(papers)} so far  (API total: {total})")

        cursor = data.get("meta", {}).get("next_cursor")
        if not cursor:
            break
        time.sleep(0.12)

    return papers


def print_stats(papers: list[dict]):
    dist = defaultdict(int)
    for p in papers:
        dist[p["journal"]] += 1
    has_abstract = sum(1 for p in papers if p["abstract"])
    print(f"\nTotal: {len(papers)}  |  with abstract: {has_abstract}")
    for j, n in sorted(dist.items(), key=lambda x: -x[1]):
        print(f"  {j}: {n}")


def main():
    incremental = "--update" in sys.argv
    import datetime
    current_year = datetime.date.today().year

    if incremental:
        # Load existing data
        try:
            existing = json.load(open(OUTPUT_FILE))
        except FileNotFoundError:
            print("No existing papers.json found, running full fetch.")
            existing = []

        # Find the max year we already have fully covered
        existing_years = {p["year"] for p in existing if p.get("year")}
        if existing_years:
            max_existing = max(existing_years)
            # Re-fetch from (max_existing - 1) to catch late additions
            fetch_from = max(max_existing - 1, YEAR_START)
        else:
            fetch_from = YEAR_START

        print(f"Incremental update: fetching {fetch_from}–{current_year} …\n")
        new_papers = fetch_range(fetch_from, current_year)

        # Build index of existing papers by DOI, remove those in re-fetched range
        existing_keep = [
            p for p in existing
            if p.get("year") and p["year"] < fetch_from
        ]
        # Preserve manual abstract patches for papers that overlap
        manual_patches = {
            p["doi"]: p["abstract"]
            for p in existing
            if p.get("abstract") and p.get("year", 0) >= fetch_from
        }
        for p in new_papers:
            if not p["abstract"] and p["doi"] in manual_patches:
                p["abstract"] = manual_patches[p["doi"]]

        papers = existing_keep + new_papers
        print(f"\nKept {len(existing_keep)} old + fetched {len(new_papers)} new = {len(papers)} total")

    else:
        print(f"Full fetch: {YEAR_START}–{current_year} …\n")
        papers = fetch_range(YEAR_START, current_year)

    print_stats(papers)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(papers, f, ensure_ascii=False, indent=2)
    print(f"\nSaved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
