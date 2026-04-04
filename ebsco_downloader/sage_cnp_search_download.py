#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import re
import ssl
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import HTTPSHandler, Request, build_opener

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
PROGRESS_FILE = ROOT / "progress_sage_cnp_search_download.json"
PAPERS_FILE = ROOT.parent / "papers.json"
SUPPORTED_JOURNALS = {"POM"}
CNP_HOST = "sage.cnpereading.g.sjuku.top"
SEARCH_URL = f"https://{CNP_HOST}/search/search"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Automate SAGE POM PDF downloads via CNPeReading search/search."
    )
    parser.add_argument("--mode", choices=["run"], required=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--journal", type=str, default="POM")
    parser.add_argument("--title-contains", type=str, default=None)
    parser.add_argument("--shard-index", type=int, default=None)
    parser.add_argument("--shard-count", type=int, default=None)
    parser.add_argument("--cdp-url", type=str, required=True)
    parser.add_argument("--progress-file", type=str, default=None)
    parser.add_argument("--todo-file", type=str, default=None)
    return parser.parse_args()


def sanitize_filename(value: str, limit: int = 140) -> str:
    value = re.sub(r'[\\/*?:"<>|]', "", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value[:limit].rstrip(". ") or "untitled"


def normalize_search_text(value: str) -> str:
    value = (
        value.replace("–", "-")
        .replace("—", "-")
        .replace("’", "'")
        .replace("“", '"')
        .replace("”", '"')
    )
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def compact_text(value: str) -> str:
    value = normalize_search_text(value).casefold()
    value = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", value)
    return value


def build_search_queries(title: str) -> list[str]:
    base = normalize_search_text(title)
    queries: list[str] = []
    candidates = [
        base,
        re.sub(r"[\"“”'’]", "", base),
        re.sub(r"[^0-9A-Za-z\u4e00-\u9fff\s-]", " ", base),
        re.split(r"[:?]", base, maxsplit=1)[0].strip(),
    ]
    for item in candidates:
        item = re.sub(r"\s+", " ", item).strip()
        if item and item not in queries:
            queries.append(item)
    return queries


def href_to_absolute(href: str) -> str:
    if href.startswith("/"):
        return f"https://{CNP_HOST}{href}"
    return href


def extract_doi_suffix(doi_url: str) -> str:
    doi_url = doi_url.strip()
    if doi_url.startswith("https://doi.org/"):
        return doi_url.removeprefix("https://doi.org/")
    return doi_url


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
        papers = [
            p for p in papers if str(p.get("journal", "")).upper() == args.journal.upper()
        ]
    if args.title_contains:
        needle = args.title_contains.casefold()
        papers = [p for p in papers if needle in str(p.get("title", "")).casefold()]
    if args.start:
        papers = papers[args.start :]
    if args.limit is not None:
        papers = papers[: args.limit]
    papers = [p for p in papers if str(p.get("journal", "")).upper() in SUPPORTED_JOURNALS]
    papers = [p for p in papers if str(p.get("doi", "")).strip().startswith("https://doi.org/10.1177/")]
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


def load_or_create_todo_snapshot(todo_file: Path | None, papers: list[dict[str, Any]]) -> list[dict[str, Any]]:
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


def reconcile_existing_downloads(progress_file: Path, progress: dict[str, Any], papers: list[dict[str, Any]]) -> None:
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
    skipped_keys = {
        item.get("key") for item in progress.get("skipped", []) if isinstance(item, dict)
    }
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
    progress["failed"].append({"key": key, "title": paper.get("title", ""), "reason": reason})
    save_progress(progress_file, progress)


def mark_skipped(progress_file: Path, progress: dict[str, Any], paper: dict[str, Any], reason: str) -> None:
    progress.setdefault("skipped", [])
    key = paper_key(paper)
    progress["skipped"] = [item for item in progress["skipped"] if item.get("key") != key]
    progress["skipped"].append({"key": key, "title": paper.get("title", ""), "reason": reason})
    save_progress(progress_file, progress)


async def create_cdp_context(pw: Any, cdp_url: str) -> tuple[Browser, BrowserContext]:
    browser = await pw.chromium.connect_over_cdp(cdp_url)
    if browser.contexts:
        context = browser.contexts[0]
    else:
        context = await browser.new_context()
    return browser, context


async def build_cookie_header(context: BrowserContext) -> str:
    cookies = await context.cookies([f"https://{CNP_HOST}/"])
    if not cookies:
        raise RuntimeError("no CNPeReading cookies available in attached browser context")
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
            "Referer": SEARCH_URL,
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
            ),
            "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        },
    )
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    opener = build_opener(HTTPSHandler(context=ssl_context))
    with opener.open(req, timeout=30) as resp:
        content_type = resp.headers.get("Content-Type", "")
        data = resp.read()
    if not data.startswith(b"%PDF-"):
        preview = data[:200].decode("utf-8", errors="ignore")
        raise RuntimeError(
            f"PDF_NOT_RETURNED: content-type={content_type!r}, preview={preview!r}"
        )
    temp_path.write_bytes(data)
    temp_path.replace(destination)


async def open_search_page(context: BrowserContext) -> Page:
    page = await context.new_page()
    await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=45000)
    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
    except PlaywrightTimeoutError:
        pass
    await page.locator('input[placeholder="Search Journals"]').first.wait_for(state="visible", timeout=15000)
    return page


async def find_result_item(page: Page, title: str) -> Any:
    target = compact_text(title)
    items = page.locator("li")
    count = min(await items.count(), 40)
    for index in range(count):
        item = items.nth(index)
        try:
            text = await item.inner_text()
        except PlaywrightError:
            continue
        if not text:
            continue
        if target in compact_text(text):
            return item
    return None


async def result_item_download_link(item: Any, doi_url: str) -> str | None:
    doi_suffix = extract_doi_suffix(doi_url)
    direct = item.locator(f'a[href*="/paragraph/download/?doi={doi_suffix}"]')
    if await direct.count():
        href = await direct.first.get_attribute("href")
        if href:
            return href_to_absolute(href)
    any_download = item.locator('a[href*="/paragraph/download/"]')
    if await any_download.count():
        href = await any_download.first.get_attribute("href")
        if href:
            return href_to_absolute(href)
    direct_article = item.locator(f'a[href*="/paragraph/article/?doi={doi_suffix}"]')
    if await direct_article.count():
        href = await direct_article.first.get_attribute("href")
        if href:
            return href_to_absolute(href.replace("/paragraph/article/", "/paragraph/download/"))
    any_article = item.locator('a[href*="/paragraph/article/"]')
    if await any_article.count():
        href = await any_article.first.get_attribute("href")
        if href:
            return href_to_absolute(href.replace("/paragraph/article/", "/paragraph/download/"))
    return None


async def search_download_link(page: Page, title: str, doi_url: str) -> str:
    search_box = page.locator('input[placeholder="Search Journals"]').first
    search_button = page.locator("""a[href*="searchMethod('generalSearch')"]""").first
    last_body = ""
    for query in build_search_queries(title):
        await search_box.fill(query)
        await search_button.click()
        await page.wait_for_load_state("domcontentloaded")
        try:
            await page.wait_for_load_state("networkidle", timeout=8000)
        except PlaywrightTimeoutError:
            pass
        item = await find_result_item(page, title)
        if item is None:
            last_body = (await page.locator("body").inner_text())[:1200]
            continue
        href = await result_item_download_link(item, doi_url)
        if href:
            return href
        try:
            last_body = (await item.inner_text())[:1200]
        except PlaywrightError:
            last_body = (await page.locator("body").inner_text())[:1200]
    raise RuntimeError(f"NO_RESULT_LINK: doi={extract_doi_suffix(doi_url)}; page={last_body!r}")


async def process_one_paper(context: BrowserContext, paper: dict[str, Any]) -> None:
    destination = target_path(paper)
    if destination.exists():
        return
    doi = str(paper.get("doi", "")).strip()
    title = str(paper.get("title", "")).strip()
    page = await open_search_page(context)
    try:
        download_url = await search_download_link(page, title, doi)
    finally:
        await page.close()
    cookie_header = await build_cookie_header(context)
    await asyncio.wait_for(
        asyncio.to_thread(download_via_http, download_url, cookie_header, destination),
        timeout=35,
    )


async def process_with_retry(context: BrowserContext, paper: dict[str, Any], retries: int = 1) -> None:
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            await process_one_paper(context, paper)
            return
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                raise
            await asyncio.sleep(1.0 * (attempt + 1))
    assert last_exc is not None
    raise last_exc


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
        for index, paper in enumerate(todo, start=1):
            title = str(paper.get("title", "")).strip()
            destination = target_path(paper)
            print(f"[{index}/{len(todo)}] {title}")
            try:
                if destination.exists():
                    mark_done(progress_file, progress, paper)
                    print(f"  saved -> {destination}")
                    continue
                await process_with_retry(context, paper)
                mark_done(progress_file, progress, paper)
                print(f"  saved -> {destination}")
            except (PlaywrightTimeoutError, PlaywrightError, RuntimeError, HTTPError, URLError, Exception) as exc:
                print(f"  failed -> {type(exc).__name__}: {exc}")
                mark_failed(progress_file, progress, paper, f"{type(exc).__name__}: {exc}")
                mark_skipped(progress_file, progress, paper, f"{type(exc).__name__}: {exc}")


async def main() -> None:
    args = parse_args()
    await run_mode(args)


if __name__ == "__main__":
    asyncio.run(main())
