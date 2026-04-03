#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus
from urllib.request import Request, build_opener

from playwright.async_api import (
    Browser,
    BrowserContext,
    Error as PlaywrightError,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

ROOT = Path(__file__).resolve().parent
DOWNLOAD_DIR = ROOT / "downloads"
PROGRESS_FILE = ROOT / "progress_wiley.json"
PAPERS_FILE = ROOT.parent / "papers.json"
DEFAULT_CHROME_DOWNLOADS = Path.home() / "Downloads"
SUPPORTED_JOURNALS = {"POM"}
WILEY_HOST = "onlinelibrary.wiley.com"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Automate Wiley POM PDF downloads from papers.json."
    )
    parser.add_argument("--mode", choices=["run"], required=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--journal", type=str, default="POM")
    parser.add_argument("--title-contains", type=str, default=None)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--slow-ms", type=int, default=250)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--shard-index", type=int, default=None)
    parser.add_argument("--shard-count", type=int, default=None)
    parser.add_argument("--cdp-url", type=str, required=True)
    parser.add_argument("--progress-file", type=str, default=None)
    parser.add_argument("--todo-file", type=str, default=None)
    parser.add_argument("--downloads-dir", type=str, default=str(DEFAULT_CHROME_DOWNLOADS))
    return parser.parse_args()


def sanitize_filename(value: str, limit: int = 140) -> str:
    value = re.sub(r'[\\/*?:"<>|]', "", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value[:limit].rstrip(". ") or "untitled"


def normalize_title(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value)
    value = value.casefold()
    value = value.replace("‐", "-").replace("–", "-").replace("—", "-")
    value = re.sub(r"[“”\"'`]", "", value)
    value = re.sub(r"[^0-9a-z]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def paper_key(paper: dict[str, Any]) -> str:
    doi = str(paper.get("doi", "")).strip()
    if doi:
        return doi
    return f"{paper.get('year', '')}::{paper.get('title', '')}"


def target_path(paper: dict[str, Any]) -> Path:
    journal = sanitize_filename(str(paper.get("journal", "OTHER")).upper(), 20)
    year = str(paper.get("year", "0000"))
    title = sanitize_filename(str(paper.get("title", "untitled")))
    folder = DOWNLOAD_DIR / journal
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{year} - {title}.pdf"


def load_papers(args: argparse.Namespace) -> list[dict[str, Any]]:
    papers = json.loads(PAPERS_FILE.read_text())
    if args.journal:
        papers = [p for p in papers if str(p.get("journal", "")).upper() == args.journal.upper()]
    if args.title_contains:
        needle = args.title_contains.casefold()
        papers = [p for p in papers if needle in str(p.get("title", "")).casefold()]
    if args.start:
        papers = papers[args.start :]
    if args.limit is not None:
        papers = papers[: args.limit]
    papers = [p for p in papers if str(p.get("journal", "")).upper() in SUPPORTED_JOURNALS]
    return dedupe_papers(papers)


def load_progress(progress_file: Path) -> dict[str, Any]:
    if not progress_file.exists():
        return {"done": [], "failed": [], "skipped": []}
    return json.loads(progress_file.read_text())


def save_progress(progress_file: Path, progress: dict[str, Any]) -> None:
    progress_file.write_text(json.dumps(progress, ensure_ascii=False, indent=2))


def dedupe_papers(papers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for paper in papers:
        key = paper_key(paper)
        if key in seen:
            continue
        seen.add(key)
        result.append(paper)
    return result


def load_or_create_todo_snapshot(
    todo_file: Path | None,
    papers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if todo_file is None:
        return papers
    if todo_file.exists():
        return json.loads(todo_file.read_text())
    todo_file.parent.mkdir(parents=True, exist_ok=True)
    todo_file.write_text(json.dumps(papers, ensure_ascii=False, indent=2))
    return papers


def split_items_evenly[T](items: list[T], part_count: int) -> list[list[T]]:
    part_count = max(1, part_count)
    total = len(items)
    base = total // part_count
    remainder = total % part_count
    result: list[list[T]] = []
    start = 0
    for part_index in range(part_count):
        size = base + (1 if part_index < remainder else 0)
        end = start + size
        result.append(items[start:end])
        start = end
    return result


def apply_shard(
    items: list[dict[str, Any]],
    shard_index: int | None,
    shard_count: int | None,
) -> tuple[list[dict[str, Any]], tuple[int, int] | None]:
    if shard_index is None and shard_count is None:
        return items, None
    if shard_index is None or shard_count is None:
        raise SystemExit("--shard-index 和 --shard-count 必须一起传")
    if shard_count < 1:
        raise SystemExit("--shard-count 必须 >= 1")
    if shard_index < 1 or shard_index > shard_count:
        raise SystemExit("--shard-index 必须在 1..--shard-count 范围内")
    shards = split_items_evenly(items, shard_count)
    selected = shards[shard_index - 1]
    if not selected:
        return selected, None
    start = sum(len(shard) for shard in shards[: shard_index - 1]) + 1
    end = start + len(selected) - 1
    return selected, (start, end)


def reconcile_existing_downloads(
    progress_file: Path,
    progress: dict[str, Any],
    papers: list[dict[str, Any]],
) -> None:
    changed = False
    progress.setdefault("done", [])
    progress.setdefault("failed", [])
    progress.setdefault("skipped", [])
    for paper in papers:
        if not target_path(paper).exists():
            continue
        key = paper_key(paper)
        if key not in progress["done"]:
            progress["done"].append(key)
            changed = True
        new_failed = [item for item in progress["failed"] if item.get("key") != key]
        if len(new_failed) != len(progress["failed"]):
            progress["failed"] = new_failed
            changed = True
        new_skipped = [item for item in progress["skipped"] if item.get("key") != key]
        if len(new_skipped) != len(progress["skipped"]):
            progress["skipped"] = new_skipped
            changed = True
    if changed:
        save_progress(progress_file, progress)


def already_done(progress: dict[str, Any], paper: dict[str, Any]) -> bool:
    key = paper_key(paper)
    skipped_keys = {item.get("key") for item in progress.get("skipped", []) if isinstance(item, dict)}
    return key in progress.get("done", []) or key in skipped_keys or target_path(paper).exists()


def mark_done(progress_file: Path, progress: dict[str, Any], paper: dict[str, Any]) -> None:
    progress.setdefault("done", [])
    key = paper_key(paper)
    if key not in progress["done"]:
        progress["done"].append(key)
    progress["failed"] = [item for item in progress.get("failed", []) if item.get("key") != key]
    progress["skipped"] = [item for item in progress.get("skipped", []) if item.get("key") != key]
    save_progress(progress_file, progress)


def mark_failed(progress_file: Path, progress: dict[str, Any], paper: dict[str, Any], reason: str) -> None:
    progress.setdefault("failed", [])
    key = paper_key(paper)
    progress["failed"] = [item for item in progress["failed"] if item.get("key") != key]
    progress["failed"].append(
        {
            "key": key,
            "title": paper.get("title", ""),
            "reason": reason,
        }
    )
    save_progress(progress_file, progress)


def build_search_url(title: str) -> str:
    return f"https://onlinelibrary.wiley.com/action/doSearch?AllField={quote_plus(title)}"


async def create_cdp_context(pw: Any, cdp_url: str) -> tuple[Browser, BrowserContext]:
    browser = await pw.chromium.connect_over_cdp(cdp_url)
    if browser.contexts:
        context = browser.contexts[0]
    else:
        context = await browser.new_context(accept_downloads=True)
    return browser, context


async def pick_wiley_page(context: BrowserContext) -> Page:
    for page in reversed(context.pages):
        if WILEY_HOST in page.url:
            await page.bring_to_front()
            return page
    page = await context.new_page()
    await page.goto("https://onlinelibrary.wiley.com/", wait_until="domcontentloaded")
    await page.bring_to_front()
    return page


async def ensure_page_alive(context: BrowserContext, page: Page | None) -> Page:
    if page is not None and not page.is_closed():
        return page
    return await pick_wiley_page(context)


async def open_search_results(page: Page, title: str) -> None:
    await page.goto(build_search_url(title), wait_until="domcontentloaded")
    first_result = page.locator('a[href*="/doi/"]').first
    await first_result.wait_for(timeout=30000)


async def get_first_result(page: Page) -> tuple[str, str]:
    first_result = page.locator('a[href*="/doi/"]').first
    title = (await first_result.inner_text()).strip()
    href = await first_result.get_attribute("href")
    if not href:
        raise RuntimeError("first Wiley result has no href")
    return title, href


async def open_first_result_if_match(page: Page, expected_title: str) -> None:
    actual_title, href = await get_first_result(page)
    if normalize_title(actual_title) != normalize_title(expected_title):
        raise RuntimeError(
            f"first Wiley result mismatch: expected={expected_title!r}, actual={actual_title!r}, href={href}"
        )
    await page.locator('a[href*="/doi/"]').first.click()
    await page.wait_for_load_state("domcontentloaded")
    await page.locator("h1").first.wait_for(timeout=30000)


async def open_epdf_reader(page: Page) -> Page:
    pdf_button = page.locator("a.coolBar__ctrl.pdf-download:visible").first
    await pdf_button.wait_for(timeout=30000)
    await pdf_button.click()
    await page.wait_for_url(re.compile(r"/doi/epdf/"), timeout=30000)
    await page.locator('a[aria-label^="Download PDF"]').first.wait_for(timeout=30000)
    return page


async def build_cookie_header(context: BrowserContext) -> str:
    cookies = await context.cookies(["https://onlinelibrary.wiley.com"])
    if not cookies:
        raise RuntimeError("no Wiley cookies available in attached browser context")
    return "; ".join(f"{item['name']}={item['value']}" for item in cookies)


def download_via_http(url: str, cookie_header: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_name(f".{destination.name}.part")
    if temp_path.exists():
        temp_path.unlink()
    req = Request(
        url,
        headers={
            "Cookie": cookie_header,
            "Referer": "https://onlinelibrary.wiley.com/",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
            ),
            "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        },
    )
    opener = build_opener()
    with opener.open(req, timeout=60) as resp:
        content_type = resp.headers.get("Content-Type", "")
        data = resp.read()
    if not data.startswith(b"%PDF-"):
        preview = data[:200].decode("utf-8", errors="ignore")
        raise RuntimeError(f"pdfdirect did not return PDF: content-type={content_type!r}, preview={preview!r}")
    temp_path.write_bytes(data)
    temp_path.replace(destination)


async def trigger_reader_download(page: Page, destination: Path) -> None:
    download_anchor = page.locator('a[aria-label^="Download PDF"]').first
    await download_anchor.wait_for(timeout=30000)
    href = await download_anchor.get_attribute("href")
    if not href:
        raise RuntimeError("reader download anchor has no href")
    if href.startswith("/"):
        href = f"https://{WILEY_HOST}{href}"
    cookie_header = await build_cookie_header(page.context)
    if destination.exists():
        destination.unlink()
    await asyncio.to_thread(download_via_http, href, cookie_header, destination)


async def process_one_paper(page: Page, paper: dict[str, Any]) -> Page:
    title = str(paper.get("title", "")).strip()
    destination = target_path(paper)
    await open_search_results(page, title)
    await open_first_result_if_match(page, title)
    page = await open_epdf_reader(page)
    await trigger_reader_download(page, destination)
    return page


async def run_mode(args: argparse.Namespace) -> None:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    progress_file = Path(args.progress_file).expanduser() if args.progress_file else PROGRESS_FILE
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    todo_file = Path(args.todo_file).expanduser() if args.todo_file else None
    papers = load_papers(args)
    progress = load_progress(progress_file)
    reconcile_existing_downloads(progress_file, progress, papers)
    papers = load_or_create_todo_snapshot(todo_file, papers)
    todo = [paper for paper in papers if not already_done(progress, paper)]
    todo, shard_range = apply_shard(todo, args.shard_index, args.shard_count)

    print(f"待处理: {len(todo)} / 可跑: {len(papers)}")
    if shard_range is not None:
        print(f"分片: {args.shard_index}/{args.shard_count} -> {shard_range[0]}-{shard_range[1]}")
    if todo_file is not None:
        print(f"todo快照: {todo_file}")
    if not todo:
        return

    async with async_playwright() as pw:
        _browser, context = await create_cdp_context(pw, args.cdp_url)
        page = await pick_wiley_page(context)
        for index, paper in enumerate(todo, start=1):
            title = str(paper.get("title", "")).strip()
            destination = target_path(paper)
            print(f"[{index}/{len(todo)}] {title}")
            try:
                if destination.exists():
                    mark_done(progress_file, progress, paper)
                    print(f"  saved -> {destination}")
                    continue
                page = await ensure_page_alive(context, page)
                page = await process_one_paper(page, paper)
                mark_done(progress_file, progress, paper)
                print(f"  saved -> {destination}")
            except (PlaywrightTimeoutError, PlaywrightError, RuntimeError) as exc:
                print(f"  failed -> {type(exc).__name__}: {exc}")
                mark_failed(progress_file, progress, paper, f"{type(exc).__name__}: {exc}")
                page = await pick_wiley_page(context)


async def main() -> None:
    args = parse_args()
    await run_mode(args)


if __name__ == "__main__":
    asyncio.run(main())
