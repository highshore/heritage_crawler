"""
Orchestrates all three crawl phases for digital.khs.go.kr.

Phase 1: List scraping  — paginate recordImg.do, extract item stubs
Phase 2: Detail scraping — fetch recordDetailImg.do, filter KOGL 제1유형
Phase 3: Asset download  — download thumbnails, write metadata.json

Resume support:
  Checkpoints are written after each phase so any phase can be restarted
  without repeating earlier work. Pilot mode never writes checkpoints.
"""
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger

from .config import (
    OUT_DIR, TOTAL_PAGES, PILOT_PAGES,
    MAX_LIST_PAGES, MAX_DOWNLOADS,
)
from .list_scraper   import scrape_list_pages,  make_session as list_session
from .detail_scraper import scrape_details,     make_session as detail_session
from .downloader     import download_assets


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(mode: str = "pilot") -> tuple[list[dict], dict]:
    """
    mode: "pilot"  — PILOT_PAGES list pages, all three phases
          "full"   — all 44,496 list pages, all three phases with checkpoints
    Returns (kept_items, stats).
    """
    use_checkpoint = (mode == "full")
    is_pilot       = (mode == "pilot")

    # Page range
    if is_pilot:
        page_range = range(1, PILOT_PAGES + 1)
    else:
        # Cap at MAX_LIST_PAGES (≈ 8,334) — 2× safety factor vs MAX_DOWNLOADS (50,000)
        # so we collect enough stubs even if <100% pass the KOGL filter.
        page_range = range(1, MAX_LIST_PAGES + 1)

    wall_t0 = time.perf_counter()
    start   = _now()

    # ── Phase 1: List scraping ─────────────────────────────────────────────────
    logger.info(f"=== Phase 1: List scraping ({len(page_range)} pages) ===")
    t1 = time.perf_counter()
    stubs = scrape_list_pages(
        list_session(),
        page_range,
        use_checkpoint=use_checkpoint,
    )
    phase1_sec = round(time.perf_counter() - t1, 1)
    logger.success(f"Phase 1 complete | {len(stubs):,} stubs | {phase1_sec}s")

    # ── Phase 2: Detail scraping + KOGL filter ─────────────────────────────────
    logger.info(f"=== Phase 2: Detail scraping + KOGL filter ({len(stubs):,} items) ===")
    t2 = time.perf_counter()
    kept = scrape_details(
        detail_session(),
        stubs,
        use_checkpoint=use_checkpoint,
        max_items=(MAX_DOWNLOADS if not is_pilot else None),
    )
    phase2_sec = round(time.perf_counter() - t2, 1)
    kogl_rate  = len(kept) / max(len(stubs), 1) * 100
    logger.success(
        f"Phase 2 complete | {len(kept):,} kept ({kogl_rate:.1f}% are 제1유형) "
        f"| {phase2_sec}s"
    )

    # ── Phase 3: S3 upload ────────────────────────────────────────────────────
    logger.info(f"=== Phase 3: S3 upload ({len(kept):,} items) ===")
    t3 = time.perf_counter()
    asset_stats = download_assets(kept)
    phase3_sec  = round(time.perf_counter() - t3, 1)
    logger.success(f"Phase 3 complete | {phase3_sec}s")

    total_sec = round(time.perf_counter() - wall_t0, 1)

    stats = {
        "mode":                mode,
        "start_utc":           start,
        "end_utc":             _now(),
        "total_elapsed_sec":   total_sec,
        "phase1_list_pages":   len(page_range),
        "phase1_stubs":        len(stubs),
        "phase1_elapsed_sec":  phase1_sec,
        "phase2_kept":         len(kept),
        "phase2_discarded":    len(stubs) - len(kept),
        "phase2_kogl_rate_pct": round(kogl_rate, 2),
        "phase2_elapsed_sec":  phase2_sec,
        "phase3_elapsed_sec":  phase3_sec,
        "asset_download":      asset_stats,
    }

    return kept, stats
