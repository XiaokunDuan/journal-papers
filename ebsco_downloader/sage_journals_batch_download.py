#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import re
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
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

from download_naming import extract_doi_suffix, paper_key, target_exists, target_path


ROOT = Path(__file__).resolve().parent
DOWNLOAD_DIR = ROOT / "downloads"
PROGRESS_FILE = ROOT / "progress_sage_journals.json"
PAPERS_FILE = ROOT.parent / "papers.json"
SUPPORTED_JOURNALS = {"POM"}
SAGE_HOST = "journals.sagepub.com"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Automate SAGE POM PDF downloads through journals.sagepub.com search + epub reader."
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


def normalize_search_text(value: str) -> str:
    value = (
        value.replace("‐", "-")
        .replace("–", "-")
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


def build_search_url(query: str) -> str:
    return f"https://{SAGE_HOST}/action/doSearch?AllField={quote_plus(query)}"


def href_to_absolute(href: str) -> str:
    if href.startswith("/"):
        return f"https://{SAGE_HOST}{href}"
    return href


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
    papers = [
        p for p in papers if str(p.get("doi", "")).strip().startswith("https://doi.org/10.1177/")
    ]
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
        if not target_exists(DOWNLOAD_DIR, paper):
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
    return key in progress.get("done", []) or key in skipped_keys or target_exists(DOWNLOAD_DIR, paper)


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
    source_context = browser.contexts[0] if browser.contexts else await browser.new_context()
    cookies = await source_context.cookies([f"https://{SAGE_HOST}/"])
    context = await browser.new_context()
    if cookies:
        await context.add_cookies(cookies)
    return browser, context


async def build_cookie_header(context: BrowserContext) -> str:
    cookies = await context.cookies([f"https://{SAGE_HOST}/"])
    if not cookies:
        raise RuntimeError("no SAGE cookies available in attached browser context")
    return "; ".join(f"{item['name']}={item['value']}" for item in cookies)


def download_via_http(url: str, cookie_header: str, referer: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_name(f".{destination.name}.part")
    if temp_path.exists():
        temp_path.unlink()
    req = Request(
        url,
        headers={
            "Cookie": cookie_header,
            "Referer": referer,
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
            ),
            "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        },
    )
    opener = build_opener()
    with opener.open(req, timeout=45) as resp:
        content_type = resp.headers.get("Content-Type", "")
        data = resp.read()
    if not data.startswith(b"%PDF-"):
        preview = data[:200].decode("utf-8", errors="ignore")
        raise RuntimeError(
            f"PDF_NOT_RETURNED: content-type={content_type!r}, preview={preview!r}"
        )
    temp_path.write_bytes(data)
    temp_path.replace(destination)


async def open_search_results(page: Page, query: str) -> None:
    await page.goto(build_search_url(query), wait_until="domcontentloaded", timeout=45000)
    try:
        await page.wait_for_load_state("networkidle", timeout=8000)
    except PlaywrightTimeoutError:
        pass
    await page.locator('a[href*="/doi/abs/"]').first.wait_for(timeout=15000)


async def find_result_doi(page: Page, title: str) -> str | None:
    target = compact_text(title)
    links = page.locator('a[href*="/doi/abs/"]')
    count = min(await links.count(), 20)
    for index in range(count):
        link = links.nth(index)
        try:
            text = await link.inner_text()
        except PlaywrightError:
            continue
        if not text or compact_text(text) != target:
            continue
        href = await link.get_attribute("href")
        if not href:
            continue
        match = re.search(r"/doi/abs/(10\.1177/[^/?#]+)", href)
        if match:
            return match.group(1)
    return None


async def search_result_doi(page: Page, title: str, doi_url: str) -> str:
    expected = extract_doi_suffix(doi_url)
    last_body = ""
    for query in build_search_queries(title):
        await open_search_results(page, query)
        doi_suffix = await find_result_doi(page, title)
        if doi_suffix:
            return doi_suffix
        body = await page.locator("body").inner_text()
        last_body = body[:1200]
    raise RuntimeError(f"NO_RESULT_LINK: doi={expected}; page={last_body!r}")


async def wait_briefly(page: Page, timeout_ms: int = 8000) -> None:
    try:
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)
    except PlaywrightTimeoutError:
        pass


async def article_to_reader_url(page: Page, doi_suffix: str) -> str:
    full_url = f"https://{SAGE_HOST}/doi/full/{doi_suffix}"
    await page.goto(full_url, wait_until="domcontentloaded", timeout=45000)
    await wait_briefly(page)
    reader = page.locator('a[data-id="article-toolbar-pdf-epub"]').first
    await reader.wait_for(timeout=15000)
    href = await reader.get_attribute("href")
    if not href:
        raise RuntimeError(f"no PDF/EPUB href on article page for {doi_suffix}")
    return href_to_absolute(href)


async def resolve_download_url(page: Page, doi_suffix: str) -> tuple[str, str]:
    reader_url = await article_to_reader_url(page, doi_suffix)
    await page.goto(reader_url, wait_until="domcontentloaded", timeout=45000)
    await wait_briefly(page)
    if "/doi/epub/" not in page.url and "/doi/reader/" in page.url:
        await page.goto(f"https://{SAGE_HOST}/doi/epub/{doi_suffix}", wait_until="domcontentloaded", timeout=45000)
        await wait_briefly(page)
    download = page.locator('a[aria-label="Download PDF"]').first
    await download.wait_for(timeout=20000)
    href = await download.get_attribute("href")
    if not href:
        raise RuntimeError(f"no Download PDF href on epub page for {doi_suffix}")
    return href_to_absolute(href), page.url


async def process_one_paper(context: BrowserContext, paper: dict[str, Any]) -> None:
    destination = target_path(DOWNLOAD_DIR, paper)
    if destination.exists():
        return
    doi = str(paper.get("doi", "")).strip()
    title = str(paper.get("title", "")).strip()
    page = await context.new_page()
    try:
        doi_suffix = await search_result_doi(page, title, doi)
        download_url, referer = await resolve_download_url(page, doi_suffix)
    finally:
        await page.close()
    cookie_header = await build_cookie_header(context)
    await asyncio.wait_for(
        asyncio.to_thread(download_via_http, download_url, cookie_header, referer, destination),
        timeout=60,
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
            await asyncio.sleep(1.5 * (attempt + 1))
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
            destination = target_path(DOWNLOAD_DIR, paper)
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
                if "400" in str(exc):
                    mark_skipped(progress_file, progress, paper, f"{type(exc).__name__}: {exc}")


async def main() -> None:
    args = parse_args()
    await run_mode(args)


if __name__ == "__main__":
    asyncio.run(main())
