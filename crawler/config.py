"""Centralised configuration for the 국가유산 디지털 서비스 crawler."""
import os
from pathlib import Path

# ── Base URL ──────────────────────────────────────────────────────────────────
BASE_URL = "https://digital.khs.go.kr"

# Endpoints (relative to BASE_URL)
LIST_PATH            = "/record/recordImg.do"
DETAIL_PATH          = "/record/recordDetailImg.do"
THUMB_PATH           = "/record/imageViewObjectStorageFile.do"     # fallback only
DOWNLOAD_PATH        = "/record/downloadObjectStorageFile.do"      # Step 1: register (POST)
DOWNLOAD_EXEC_PATH   = "/record/downloadObjectStorageFileExecute.do"  # Step 2: fetch file (GET)

# Fixed params for the image list
LIST_PARAMS = {
    "locationYn":  "N",
    "pageSe":      "Img",
    "searchClick": "N",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer":         "https://digital.khs.go.kr/record/recordImg.do",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── Crawl settings ─────────────────────────────────────────────────────────────
ITEMS_PER_PAGE   = 12       # server always returns 12 items per page
TOTAL_PAGES      = 44496    # as of April 2026 (~533,952 items total)

LIST_WORKERS   = 8          # parallel threads for list-page fetching
DETAIL_WORKERS = 20         # parallel threads for detail-page fetching
ASSET_WORKERS  = 20         # parallel threads for image downloads

REQUEST_TIMEOUT  = 20       # seconds
RETRY_ATTEMPTS   = 4
RETRY_WAIT_MIN   = 1.0
RETRY_WAIT_MAX   = 30.0
INTER_PAGE_DELAY = 0.1      # courtesy pause between page requests (sec)

# Pilot mode: stop after this many *list pages*
PILOT_PAGES = 5             # 5 pages × 12 items = 60 items

# ── Download limit ────────────────────────────────────────────────────────────
# The full catalog is ~533k items; we only want the first 50k qualifying images.
MAX_DOWNLOADS  = 50_000
# For full mode, list-scrape this many pages (2× safety factor at ~100% KOGL rate).
# Adjust upward if fewer than MAX_DOWNLOADS items pass the KOGL filter.
MAX_LIST_PAGES = MAX_DOWNLOADS * 2 // ITEMS_PER_PAGE   # ≈ 8,334 pages → ~100k stubs

# ── Session cookies (EC2 / headless environments) ─────────────────────────────
# On EC2 there is no Chrome browser, so browser_cookie3 cannot read cookies.
# Export these env vars before running:
#   export KHS_JSESSIONID=4839778598C01DDDA28EB4AFDE3DD22F.tomcat2
#   export KHS_SCOUTER=z66kb894ng5ups
#
# On a local Mac the crawler falls back to reading them from Chrome automatically.
SESSION_JSESSIONID = os.environ.get("KHS_JSESSIONID", "")
SESSION_SCOUTER    = os.environ.get("KHS_SCOUTER", "")

# ── Download form payload ─────────────────────────────────────────────────────
# Fields submitted with the Step 1 POST (usage registration).
DOWNLOAD_AGE         = "30"
DOWNLOAD_EMAIL       = "sookyum.kim@flitto.com"
DOWNLOAD_BRANCH_CD   = "A"          # 활용기관 구분 (A = 기타)
DOWNLOAD_BRANCH_ETC  = ""
DOWNLOAD_SUPPLY_CD   = "A"          # 활용목적 구분 (A = 기타)
DOWNLOAD_SUPPLY_ETC  = ""
DOWNLOAD_INTENT      = "연구목적으로 사용예정입니다"
DOWNLOAD_TYPE        = "L"          # L = original JPEG
DOWNLOAD_DOWN_SE     = "SOn"        # single-file download

# ── S3 storage ────────────────────────────────────────────────────────────────
S3_BUCKET = "flitto-upstage-wbl-data"
S3_PREFIX = "phase_2/raw/국가유산디지털서비스"   # key prefix — no trailing slash

# ── KOGL filter ───────────────────────────────────────────────────────────────
KOGL_TYPE1_IMG = "/img/icon/KOGL_1.png"

# ── Local paths (checkpoints, logs) ──────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
LOGS_DIR     = PROJECT_ROOT / "logs"
OUT_DIR      = PROJECT_ROOT / "output"
ASSETS_DIR   = PROJECT_ROOT / "assets"   # used only in pilot mode

LOGS_DIR.mkdir(exist_ok=True)
OUT_DIR.mkdir(exist_ok=True)
ASSETS_DIR.mkdir(exist_ok=True)
