from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
PAPERS_FILE = ROOT.parent / "papers.json"

_BAD_CHARS_RE = re.compile(r'[\\/*?:"<>|]')


def sanitize_filename(value: str, limit: int = 140) -> str:
    value = _BAD_CHARS_RE.sub("", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value[:limit].rstrip(". ") or "untitled"


def paper_key(paper: dict[str, Any]) -> str:
    doi = str(paper.get("doi", "")).strip()
    if doi:
        return doi
    return f"{paper.get('year', '')}::{paper.get('title', '')}"


def extract_doi_suffix(doi_url: str) -> str:
    doi_url = doi_url.strip()
    if doi_url.startswith("https://doi.org/"):
        doi_url = doi_url.removeprefix("https://doi.org/")
    return sanitize_filename(doi_url.replace("/", "_"), 80)


def legacy_target_path(download_dir: Path, paper: dict[str, Any]) -> Path:
    journal = sanitize_filename(str(paper.get("journal", "OTHER")).upper(), 20)
    year = str(paper.get("year", "0000"))
    title = sanitize_filename(str(paper.get("title", "untitled")))
    folder = download_dir / journal
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{year} - {title}.pdf"


@lru_cache(maxsize=1)
def collision_keys() -> set[tuple[str, str, str]]:
    papers = json.loads(PAPERS_FILE.read_text())
    grouped: dict[tuple[str, str, str], int] = {}
    for paper in papers:
        journal = sanitize_filename(str(paper.get("journal", "OTHER")).upper(), 20)
        year = str(paper.get("year", "0000"))
        title = sanitize_filename(str(paper.get("title", "untitled")))
        key = (journal, year, title)
        grouped[key] = grouped.get(key, 0) + 1
    return {key for key, count in grouped.items() if count > 1}


def target_path(download_dir: Path, paper: dict[str, Any]) -> Path:
    legacy = legacy_target_path(download_dir, paper)
    journal = sanitize_filename(str(paper.get("journal", "OTHER")).upper(), 20)
    year = str(paper.get("year", "0000"))
    title = sanitize_filename(str(paper.get("title", "untitled")))
    key = (journal, year, title)
    doi = str(paper.get("doi", "")).strip()
    if key not in collision_keys() or not doi:
        return legacy
    return legacy.with_name(f"{year} - {title} - {extract_doi_suffix(doi)}.pdf")


def candidate_target_paths(download_dir: Path, paper: dict[str, Any]) -> list[Path]:
    primary = target_path(download_dir, paper)
    legacy = legacy_target_path(download_dir, paper)
    if primary == legacy:
        return [primary]
    return [primary, legacy]


def target_exists(download_dir: Path, paper: dict[str, Any]) -> bool:
    return any(path.exists() for path in candidate_target_paths(download_dir, paper))
