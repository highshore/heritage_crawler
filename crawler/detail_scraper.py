"""
Phase 2 — Detail scraper + KOGL 제1유형 filter for digital.khs.go.kr.

For each item stub from Phase 1, fetches:
  GET /record/recordDetailImg.do?ichDataUid={id}&bizId={bizId}

Then:
  1. Checks for <img src="...KOGL_1.png"> → keeps item if present, discards otherwise.
  2. Extracts all slider image ichDataUids (the item may have multiple images).
  3. Extracts full metadata (title, heritage name, year, material, size, description, etc.).

Checkpoint: output/detail_checkpoint.json  tracks completed ichDataUids.
Output:     output/detail_items.jsonl       one record per kept item.
"""
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    BASE_URL, DETAIL_PATH, THUMB_PATH, HEADERS,  # BASE_URL used in _extract_metadata links
    DETAIL_WORKERS, REQUEST_TIMEOUT,
    RETRY_ATTEMPTS, RETRY_WAIT_MIN, RETRY_WAIT_MAX,
    INTER_PAGE_DELAY, KOGL_TYPE1_IMG,
    OUT_DIR,
    SESSION_JSESSIONID, SESSION_SCOUTER,
)

# ── Checkpoint / output paths ─────────────────────────────────────────────────
DETAIL_CHECKPOINT = OUT_DIR / "detail_checkpoint.json"
DETAIL_OUTPUT     = OUT_DIR / "detail_items.jsonl"

# ── Shared session ─────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    # Load session cookies so the detail page shows download buttons
    # (download section is hidden for anonymous/unauthenticated requests)
    #
    # On EC2: read from environment variables (KHS_JSESSIONID / KHS_SCOUTER).
    # On local Mac: fall back to reading from Chrome via browser_cookie3.
    if SESSION_JSESSIONID:
        s.cookies.set(
            "PROJECT2_JSESSIONID", SESSION_JSESSIONID,
            domain="digital.khs.go.kr", path="/",
        )
        if SESSION_SCOUTER:
            s.cookies.set(
                "scouter", SESSION_SCOUTER,
                domain="digital.khs.go.kr", path="/",
            )
        logger.debug("detail_scraper: using env-var session cookies (EC2 mode)")
    else:
        try:
            import browser_cookie3
            jar = browser_cookie3.chrome(domain_name="digital.khs.go.kr")
            for c in jar:
                s.cookies.set(c.name, c.value, domain=c.domain, path=c.path)
            logger.debug("detail_scraper: loaded cookies from Chrome")
        except Exception:
            pass  # cookies are optional for detail scraping; fallback handled in downloader
    return s


# ── HTTP helper ────────────────────────────────────────────────────────────────

@retry(
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=wait_exponential(min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX),
    reraise=True,
)
def _fetch_detail(session: requests.Session, uid: str, biz_id: str) -> Optional[str]:
    params = {"ichDataUid": uid, "bizId": biz_id}
    url    = BASE_URL + DETAIL_PATH
    resp   = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


# ── HTML parsing ───────────────────────────────────────────────────────────────

_RE_UID = re.compile(r"(?:ichDataUid|uid)[='\"\s:]+(\d{10,})", re.I)
_RE_SLIDER_UID = re.compile(r"goSlide[^(]*\(\s*['\"]?(\d{10,})['\"]?")


def _extract_kogl_type(soup: BeautifulSoup) -> Optional[str]:
    """Return '1' if KOGL_1.png found, or None."""
    for img in soup.find_all("img"):
        src = img.get("src", "")
        if "KOGL_1" in src:
            return "1"
        # Also check KOGL_2, _3, _4 to record actual type
        m = re.search(r"KOGL_(\d+)", src)
        if m:
            return m.group(1)
    # Check text for KOGL mentions
    kogl_tag = soup.find(string=re.compile(r"공공누리.*[1-4]유형", re.I))
    if kogl_tag:
        m = re.search(r"([1-4])유형", kogl_tag)
        if m:
            return m.group(1)
    return None


def _extract_slider_uids(soup: BeautifulSoup, primary_uid: str) -> list[str]:
    """
    Return only the primary item's UID.

    The '관련 이미지' slider on the detail page shows OTHER records from the
    same bizId project.  Those are separate records that appear independently
    on the list pages and will be crawled naturally through pagination.
    Each list item = 1 record = 1 primary image, so we always return a
    single-element list here.
    """
    return [primary_uid]


_RE_DOWNLOAD_POPUP = re.compile(
    r"openDownloadPopup\(\s*'([^']+)'\s*,\s*'(\d+)'\s*,\s*'([^']+)'"
)

def _extract_download_options(soup: BeautifulSoup, primary_uid: str) -> list[dict]:
    """
    Find all openDownloadPopup(...) calls on the detail page and return
    a list of {type, ichDataUid, downFileSe} dicts for this item.

    Example button onclick:
      openDownloadPopup('L', '13898237364257494156', 'SOn', '경주 불국사')
      openDownloadPopup('H', '13898237364257494156', 'OOn', '경주 불국사')

    Only entries whose ichDataUid matches the primary item are returned
    (slider/related items are ignored).
    """
    options = []
    seen = set()
    for tag in soup.find_all(onclick=True):
        href = tag.get("onclick", "")
        for m in _RE_DOWNLOAD_POPUP.finditer(href):
            dl_type, dl_uid, dl_se = m.group(1), m.group(2), m.group(3)
            if dl_uid != primary_uid:
                continue
            key = (dl_type, dl_se)
            if key in seen:
                continue
            seen.add(key)
            options.append({"type": dl_type, "downFileSe": dl_se})
    # Also search plain text / script blocks
    for script in soup.find_all("script"):
        text = script.get_text()
        for m in _RE_DOWNLOAD_POPUP.finditer(text):
            dl_type, dl_uid, dl_se = m.group(1), m.group(2), m.group(3)
            if dl_uid != primary_uid:
                continue
            key = (dl_type, dl_se)
            if key in seen:
                continue
            seen.add(key)
            options.append({"type": dl_type, "downFileSe": dl_se})
    return options


def _extract_metadata(soup: BeautifulSoup) -> dict:
    """
    Extract structured metadata from the detail page.

    Primary source: div.detail-info
      ├── div.d-top
      │     ├── p.deep-teal  →  이미지유형  (일반이미지 / 도면 / …)
      │     ├── li > p + strong  →  분류 (상단)
      │     └── h3  →  제목
      └── div.d-bottom > ul > li
            Each li has p.bold (label) + p or div.more>p (value)
            Fields: 국가유산명, 사업명, 분류, 데이터용량, 생산연도, 파일형태, 내용
    """
    meta: dict = {}

    # ── 공공누리 (copyright) ────────────────────────────────────────────────────
    # Located in div.detail-visual > div.public-tip
    # e.g. "공공누리 제 1유형 : 출처 표시" / "출처표시 조건 하에 자유이용 가능"
    public_tip = soup.select_one("div.public-tip")
    if public_tip:
        lis = public_tip.select("li")
        if len(lis) >= 1:
            meta["공공누리"] = lis[0].get_text(" ", strip=True)
        if len(lis) >= 2:
            meta["공공누리_조건"] = lis[1].get_text(strip=True)
        kogl_link = public_tip.select_one("a[href]")
        if kogl_link:
            meta["공공누리_url"] = kogl_link["href"]

    detail = soup.select_one("div.detail-info")
    if not detail:
        # Fallback: generic dl/dt/dd scrape
        for dl in soup.select("dl"):
            for dt, dd in zip(dl.select("dt"), dl.select("dd")):
                k, v = dt.get_text(strip=True), dd.get_text(" ", strip=True)
                if k and v:
                    meta[k] = v
        return meta

    # ── d-top ──────────────────────────────────────────────────────────────────
    d_top = detail.select_one(".d-top")
    if d_top:
        # 이미지유형 badge  e.g. "일반이미지"
        badge = d_top.select_one("p.deep-teal")
        if badge:
            meta["이미지유형"] = badge.get_text(strip=True)

        # 분류 (상단 li)  label=<p>분류</p> value=<strong>이미지</strong>
        for li in d_top.select("li"):
            label_tag = li.select_one("p:not(.deep-teal)")
            value_tag = li.select_one("strong")
            if label_tag and value_tag:
                k = label_tag.get_text(strip=True)
                v = value_tag.get_text(strip=True)
                if k and v:
                    meta[k] = v

        # 제목
        h3 = d_top.select_one("h3")
        if h3:
            meta["제목"] = h3.get_text(strip=True)

    # ── d-bottom ───────────────────────────────────────────────────────────────
    d_bottom = detail.select_one(".d-bottom")
    if d_bottom:
        for li in d_bottom.select("ul > li"):
            label_tag = li.select_one("p.bold")
            if not label_tag:
                continue
            key = label_tag.get_text(strip=True)

            # Value: first <p> inside div.more, or direct sibling <p>
            more_div = li.select_one("div.more")
            if more_div:
                val_tag = more_div.select_one("p")
                val = val_tag.get_text(strip=True) if val_tag else more_div.get_text(" ", strip=True)
                # Also capture the detail link if present
                btn = more_div.select_one("button[onclick]")
                if btn:
                    m = re.search(r"location\.href='([^']+)'", btn.get("onclick", ""))
                    if m:
                        meta[key + "_link"] = BASE_URL + m.group(1)
            else:
                # Sibling <p> (not .bold)
                val_tags = [p for p in li.select("p") if "bold" not in (p.get("class") or [])]
                val = val_tags[0].get_text(strip=True) if val_tags else ""

            if key and val:
                meta[key] = val

    return meta


def parse_detail_page(html: str, stub: dict) -> Optional[dict]:
    """
    Parse one detail page HTML. Returns enriched item dict if KOGL type 1,
    otherwise returns None.
    """
    soup      = BeautifulSoup(html, "lxml")
    kogl_type = _extract_kogl_type(soup)

    if kogl_type != "1":
        return None  # discard non-제1유형

    uid             = stub["ich_data_uid"]
    slider          = _extract_slider_uids(soup, uid)
    metadata        = _extract_metadata(soup)
    download_options = _extract_download_options(soup, uid)

    # Build thumbnail URLs for all slider images
    thumb_urls = [
        f"{BASE_URL}{THUMB_PATH}?ichDataUid={u}&type=T"
        for u in slider
    ]

    return {
        **stub,                          # carry over all list-phase fields
        "kogl_type":        kogl_type,
        "slider_uids":      slider,
        "thumbnail_urls":   thumb_urls,
        "thumbnail_url":    thumb_urls[0] if thumb_urls else stub.get("thumbnail_url", ""),
        "download_options": download_options,  # e.g. [{"type":"L","downFileSe":"SOn"}]
        "detail_meta":      metadata,
    }


# ── Checkpoint helpers ─────────────────────────────────────────────────────────

def _load_detail_checkpoint() -> set[str]:
    if DETAIL_CHECKPOINT.exists():
        try:
            return set(json.loads(DETAIL_CHECKPOINT.read_text()))
        except Exception:
            pass
    return set()


def _save_detail_checkpoint(done: set[str]) -> None:
    DETAIL_CHECKPOINT.write_text(json.dumps(sorted(done)))


# ── Public API ─────────────────────────────────────────────────────────────────

def scrape_details(
    session: requests.Session,
    stubs: list[dict],
    *,
    use_checkpoint: bool = True,
    max_items: Optional[int] = None,
) -> list[dict]:
    """
    Fetch detail pages for all stubs, filter to KOGL 제1유형, return kept items.

    use_checkpoint=True persists done UIDs so the run can be resumed.
    max_items: stop early once this many KOGL-1 items have been collected.
    """
    done_uids = _load_detail_checkpoint() if use_checkpoint else set()

    pending = [s for s in stubs if s["ich_data_uid"] not in done_uids]
    if not pending:
        logger.info("All detail pages already processed (checkpoint).")
        if DETAIL_OUTPUT.exists():
            existing = [json.loads(l) for l in DETAIL_OUTPUT.read_text().splitlines() if l.strip()]
            if max_items:
                existing = existing[:max_items]
            return existing
        return []

    if use_checkpoint and done_uids:
        logger.info(
            f"Resuming detail scrape — {len(done_uids)} done, {len(pending)} remaining"
        )

    kept:      list[dict]  = []
    errors:    list[str]   = []
    discarded  = 0
    stop_event = threading.Event()   # set when max_items is reached

    def _process(stub: dict) -> tuple[str, Optional[dict]]:
        if stop_event.is_set():
            return stub["ich_data_uid"], None
        time.sleep(INTER_PAGE_DELAY)
        try:
            html = _fetch_detail(session, stub["ich_data_uid"], stub["biz_id"])
            item = parse_detail_page(html or "", stub)
            return stub["ich_data_uid"], item
        except Exception as e:
            logger.debug(f"Detail fetch failed {stub['ich_data_uid']}: {e}")
            return stub["ich_data_uid"], {"_error": str(e), **stub}

    limit_str = f" (stopping at {max_items:,} KOGL-1 items)" if max_items else ""
    logger.info(
        f"Detail scraping {len(pending):,} items with {DETAIL_WORKERS} workers "
        f"(filtering for KOGL 제1유형){limit_str}…"
    )

    out_fh = DETAIL_OUTPUT.open("a", encoding="utf-8") if use_checkpoint else None

    try:
        with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as pool:
            futs = {pool.submit(_process, s): s for s in pending}
            with tqdm(total=len(pending), desc="Detail pages", unit=" items",
                      dynamic_ncols=True, colour="yellow", smoothing=0) as pbar:
                for fut in as_completed(futs):
                    if stop_event.is_set():
                        pbar.update(1)
                        continue

                    uid, result = fut.result()
                    done_uids.add(uid)

                    if result is None:
                        discarded += 1
                    elif "_error" in result:
                        errors.append(uid)
                    else:
                        kept.append(result)
                        if use_checkpoint and out_fh:
                            out_fh.write(json.dumps(result, ensure_ascii=False) + "\n")
                            out_fh.flush()
                        # Early-stop once we have enough KOGL-1 items
                        if max_items and len(kept) >= max_items:
                            stop_event.set()
                            logger.info(
                                f"Reached max_items={max_items:,} — stopping detail scrape early"
                            )

                    if use_checkpoint:
                        _save_detail_checkpoint(done_uids)

                    pbar.set_postfix(kept=len(kept), disc=discarded, err=len(errors))
                    pbar.update(1)
    finally:
        if out_fh:
            out_fh.close()

    logger.success(
        f"Detail scrape done | {len(kept):,} kept (제1유형) | "
        f"{discarded:,} discarded | {len(errors)} errors"
    )
    return kept
