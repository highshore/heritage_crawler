"""
CLI entry point for the 국가유산 디지털 서비스 crawler.

Usage:
  uv run python main.py pilot     # 5 list pages (~60 items), full pipeline
  uv run python main.py full      # all 44,496 pages (~533k items), with checkpoints

The pilot run exercises all three phases end-to-end:
  Phase 1 → scrape list pages → item stubs
  Phase 2 → fetch detail pages → filter KOGL 제1유형
  Phase 3 → download thumbnails → assets/ + metadata.json
"""
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from loguru import logger
from tqdm import tqdm

from crawler.config import LOGS_DIR, OUT_DIR
from crawler.orchestrator import run


def _setup_logging(mode: str) -> str:
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOGS_DIR / f"crawl_{mode}_{ts}.log"
    logger.remove()
    # Route through tqdm.write so the progress bar stays pinned at bottom
    logger.add(
        lambda msg: tqdm.write(msg, end=""),
        level="INFO",
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}",
    )
    logger.add(
        str(log_file),
        level="DEBUG",
        rotation="100 MB",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<7} | {message}",
    )
    return str(log_file)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="국가유산 디지털 서비스 crawler (공공누리 제1유형 images)"
    )
    p.add_argument(
        "mode",
        choices=["pilot", "full"],
        help="pilot = 5 list pages for testing; full = all 44,496 pages",
    )
    return p.parse_args()


def main() -> None:
    args    = _parse_args()
    log_f   = _setup_logging(args.mode)
    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = OUT_DIR / f"heritage_{args.mode}_{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"heritage_crawler | mode={args.mode}")

    items, stats = run(mode=args.mode)

    # ── Write outputs ──────────────────────────────────────────────────────────
    results_path = out_dir / "results.json"
    results_path.write_text(
        json.dumps(items, ensure_ascii=False, indent=2)
    )
    logger.success(f"  results:   {results_path}  ({len(items):,} items)")

    stats_path = out_dir / "stats.json"
    stats_path.write_text(
        json.dumps(stats, ensure_ascii=False, indent=2)
    )
    logger.success(f"  stats:     {stats_path}")

    error_log   = stats.get("asset_download", {}).get("error_log", [])
    errors_path = out_dir / "errors.json"
    errors_path.write_text(
        json.dumps({"total_errors": len(error_log), "errors": error_log},
                   ensure_ascii=False, indent=2)
    )
    if error_log:
        logger.warning(f"  errors:    {errors_path}  ({len(error_log)} download failures)")
    else:
        logger.success(f"  errors:    {errors_path}  (0 errors ✓)")

    # ── Summary ────────────────────────────────────────────────────────────────
    logger.success(
        f"\n{'='*55}\n"
        f"  Run complete ({args.mode})\n"
        f"  Stubs scraped :  {stats['phase1_stubs']:,}\n"
        f"  KOGL 제1유형 :   {stats['phase2_kept']:,} "
        f"({stats['phase2_kogl_rate_pct']:.1f}%)\n"
        f"  Downloads ok  :  {stats['asset_download']['ok']:,}\n"
        f"  Total time    :  {stats['total_elapsed_sec']}s\n"
        f"  Log file      :  {log_f}\n"
        f"{'='*55}"
    )


if __name__ == "__main__":
    main()
