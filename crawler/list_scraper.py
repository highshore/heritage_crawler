"""
Phase 1 — List scraper for digital.khs.go.kr image archive.

Paginates through:
  GET /record/recordImg.do?locationYn=N&pageSe=Img&searchClick=N&page={N}

Each page contains 12 <article class="heritage-item"> elements.
Extracts ichDataUid, bizId, title, heritage type, and thumbnail URL.

Checkpoint: output/list_checkpoint.json  tracks completed page numbers.
Output:     output/list_items.jsonl       one JSON record per line.
"""
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup, Tag
from loguru import logger
from tenacity import (
    retry, retry_if_exception_type,
    stop_after_attempt, wait_exponential,
)
from tqdm import tqdm

from .config import (
    BASE_URL, LIST_PATH, THUMB_PATH, LIST_PARAMS, HEADERS,
    LIST_WORKERS, REQUEST_TIMEOUT,
    RETRY_ATTEMPTS, RETRY_WAIT_MIN, RETRY_WAIT_MAX,
    INTER_PAGE_DELAY, ITEMS_PER_PAGE,
    OUT_DIR, TOTAL_PAGES,
)

# ── Checkpoint / output paths ─────────────────────────────────────────────────
LIST_CHECKPOINT = OUT_DIR / "list_checkpoint.json"
LIST_OUTPUT     = OUT_DIR / "list_items.jsonl"

# ── Shared session ─────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


# ── HTTP helper ────────────────────────────────────────────────────────────────

@retry(
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=wait_exponential(min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX),
    reraise=True,
)
def _fetch_page(session: requests.Session, page_num: int) -> Optional[str]:
    params = {**LIST_PARAMS, "page": page_num}
    url    = BASE_URL + LIST_PATH
    resp   = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


# ── HTML parsing ───────────────────────────────────────────────────────────────

# Matches: javascript:goRecordDetailPage('123...', 'BIZ...')
_RE_HREF = re.compile(
    r"goRecordDetailPage\(\s*'(\d+)'\s*,\s*'([^']+)'\s*\)"
)


def _parse_article(article: Tag, page_num: int) -> Optional[dict]:
    """
    Extract one item from <article class="heritage-item">.

    Confirmed structure (April 2026):
      <article class="heritage-item" role="row">
        <a class="heritage-link"
           href="javascript:goRecordDetailPage('ichDataUid', 'bizId')">
          <p class="heritage-type ...">국가민속문화유산</p>
          <div class="img-box">
            <img src="/record/imageViewObjectStorageFile.do?ichDataUid=...&type=T">
          </div>
          <h3 itemprop="name" title="...">title text</h3>
        </a>
        <div class="heritage-content">
          <dl class="heritage-details">
            <div class="detail-item"><dt>파일형태</dt><dd>tif</dd></div>
            <div class="detail-item"><dt>데이터용량</dt><dd>17.2 MB</dd></div>
            <div class="detail-item"><dt>생산연도</dt><dd>2020</dd></div>
          </dl>
        </div>
      </article>
    """
    uid = biz = None

    # Primary: parse href of <a class="heritage-link">
    a_tag = article.find("a", class_="heritage-link")
    if a_tag:
        href = a_tag.get("href", "")
        m = _RE_HREF.search(href)
        if m:
            uid, biz = m.group(1), m.group(2)

    # Fallback: scan raw article HTML for the JS call pattern
    if not uid or not biz:
        m = _RE_HREF.search(str(article))
        if m:
            uid, biz = m.group(1), m.group(2)

    if not uid or not biz:
        logger.debug(f"Could not extract ichDataUid/bizId from article on page {page_num}")
        return None

    # ── Title ─────────────────────────────────────────────────────────────────
    # <h3 itemprop="name" title="...">text</h3>
    h3 = article.find("h3", attrs={"itemprop": "name"}) or article.find("h3")
    title = h3.get_text(strip=True) if h3 else ""

    # If no dedicated title element, use alt text from thumbnail img
    if not title:
        img = article.find("img")
        if img:
            title = img.get("alt", "").strip()

    # ── Heritage type badge ───────────────────────────────────────────────────
    # <p class="heritage-type deep-pink">국가민속문화유산</p>
    badge_tag = article.find("p", class_=re.compile(r"heritage.?type", re.I))
    heritage_type = badge_tag.get_text(strip=True) if badge_tag else ""

    # ── Thumbnail URL ─────────────────────────────────────────────────────────
    # Always build from ichDataUid — the img src in the raw HTML is the same
    thumb_url = f"{BASE_URL}{THUMB_PATH}?ichDataUid={uid}&type=T"

    # ── Additional metadata from list (format / size / year) ─────────────────
    # <dl class="heritage-details">
    #   <div class="detail-item"><dt>파일형태</dt><dd>tif</dd></div>
    meta_pairs: dict[str, str] = {}
    dl = article.find("dl", class_=re.compile(r"heritage.?details", re.I))
    if dl:
        for div in dl.find_all("div", class_="detail-item"):
            dt = div.find("dt")
            dd = div.find("dd")
            if dt and dd:
                meta_pairs[dt.get_text(strip=True)] = dd.get_text(strip=True)

    # Category type (구축유형)
    cat_tag = article.find("strong", class_="category-value")
    if cat_tag:
        meta_pairs["구축유형"] = cat_tag.get_text(strip=True)

    return {
        "ich_data_uid":    uid,
        "biz_id":          biz,
        "title":           title,
        "heritage_type":   heritage_type,
        "thumbnail_url":   thumb_url,
        "list_meta":       meta_pairs,
        "list_page":       page_num,
        "detail_page_url": f"{BASE_URL}/record/recordDetailImg.do?ichDataUid={uid}&bizId={biz}",
    }


def parse_list_page(html: str, page_num: int) -> list[dict]:
    """Parse all items from one list page's HTML."""
    soup     = BeautifulSoup(html, "lxml")
    articles = soup.find_all("article", class_=re.compile(r"heritage.?item", re.I))

    # Broader fallback: any article with a goRecordDetailPage call
    if not articles:
        articles = [a for a in soup.find_all("article")
                    if "goRecordDetailPage" in str(a)]

    # Even broader: li elements with the JS call (in case markup changed)
    if not articles:
        articles = [li for li in soup.find_all("li")
                    if "goRecordDetailPage" in str(li)]

    items = []
    for art in articles:
        item = _parse_article(art, page_num)
        if item:
            items.append(item)

    if not items:
        # Log a snippet to help debug
        logger.warning(f"Page {page_num}: 0 items parsed. Snippet: {html[:500]!r}")

    return items


# ── Checkpoint helpers ─────────────────────────────────────────────────────────

def _load_list_checkpoint() -> set[int]:
    if LIST_CHECKPOINT.exists():
        try:
            return set(json.loads(LIST_CHECKPOINT.read_text()))
        except Exception:
            pass
    return set()


def _save_list_checkpoint(done: set[int]) -> None:
    LIST_CHECKPOINT.write_text(json.dumps(sorted(done)))


# ── Public API ─────────────────────────────────────────────────────────────────

def scrape_list_pages(
    session: requests.Session,
    page_range: range,
    *,
    use_checkpoint: bool = True,
) -> list[dict]:
    """
    Scrape the given range of list pages and return all item stubs.

    If use_checkpoint=True (full mode), already-completed pages are skipped
    and results are appended to LIST_OUTPUT.jsonl for immediate persistence.
    """
    done_pages = _load_list_checkpoint() if use_checkpoint else set()

    pending = [p for p in page_range if p not in done_pages]
    if not pending:
        logger.info("All list pages already scraped (checkpoint).")
        # Reload from file
        if LIST_OUTPUT.exists():
            return [json.loads(l) for l in LIST_OUTPUT.read_text().splitlines() if l.strip()]
        return []

    if use_checkpoint and done_pages:
        logger.info(f"Resuming list scrape — {len(done_pages)} pages done, {len(pending)} remaining")

    all_items: list[dict] = []
    errors:    list[int]  = []

    def _fetch_and_parse(page_num: int) -> tuple[int, list[dict]]:
        time.sleep(INTER_PAGE_DELAY)
        html = _fetch_page(session, page_num)
        return page_num, parse_list_page(html or "", page_num)

    logger.info(f"List scraping {len(pending)} pages ({min(pending)}–{max(pending)}) "
                f"with {LIST_WORKERS} workers…")

    # Open output file for append (creates if absent)
    out_fh = LIST_OUTPUT.open("a", encoding="utf-8") if use_checkpoint else None

    try:
        with ThreadPoolExecutor(max_workers=LIST_WORKERS) as pool:
            futs = {pool.submit(_fetch_and_parse, p): p for p in pending}
            with tqdm(total=len(pending), desc="List pages", unit=" pg",
                      dynamic_ncols=True, colour="cyan", smoothing=0) as pbar:
                for fut in as_completed(futs):
                    try:
                        pg, items = fut.result()
                    except Exception as e:
                        pg = futs[fut]
                        logger.error(f"Page {pg} failed: {e}")
                        errors.append(pg)
                        pbar.update(1)
                        continue

                    all_items.extend(items)
                    done_pages.add(pg)

                    if use_checkpoint:
                        for item in items:
                            out_fh.write(json.dumps(item, ensure_ascii=False) + "\n")
                        out_fh.flush()
                        _save_list_checkpoint(done_pages)

                    pbar.update(1)
    finally:
        if out_fh:
            out_fh.close()

    if errors:
        logger.warning(f"List scrape: {len(errors)} pages failed: {errors}")

    logger.success(
        f"List scrape done | {len(all_items):,} items from {len(pending)} pages"
        + (f" | {len(errors)} page errors" if errors else "")
    )
    return all_items
