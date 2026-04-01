"""
Phase 3 — Original-image downloader for digital.khs.go.kr.

Download flow (login required):
  1. POST /record/downloadObjectStorageFile.do   — register intent (X-CSRF-TOKEN)
  2. GET  /record/downloadObjectStorageFileExecute.do?type=…&ichDataUid=… — stream file

Each image is written to a NamedTemporaryFile, then uploaded to S3:
  s3://{S3_BUCKET}/{S3_PREFIX}/{ichDataUid}/image.{ext}
  s3://{S3_BUCKET}/{S3_PREFIX}/{ichDataUid}/metadata.json

A local upload checkpoint (output/upload_checkpoint.json) tracks completed UIDs
so the run can be safely interrupted and resumed.

Session management:
  • Cookies loaded from KHS_JSESSIONID env var (EC2) or Chrome browser_cookie3 (local).
  • Background keepalive thread pings the site every 10 min to prevent JSESSIONID expiry.
  • On SessionExpiredError the session is rebuilt (env var on EC2, Chrome on local) and
    the item is retried once.
"""
import json
import mimetypes
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests
from bs4 import BeautifulSoup
from botocore.exceptions import ClientError
from loguru import logger
from tenacity import (
    retry, retry_if_exception_type,
    stop_after_attempt, wait_exponential,
)
from tqdm import tqdm

from .config import (
    BASE_URL, DETAIL_PATH, LIST_PATH, LIST_PARAMS,
    DOWNLOAD_PATH, DOWNLOAD_EXEC_PATH, HEADERS,
    ASSET_WORKERS, ASSETS_DIR, OUT_DIR,
    REQUEST_TIMEOUT, RETRY_ATTEMPTS, RETRY_WAIT_MIN, RETRY_WAIT_MAX,
    DOWNLOAD_AGE, DOWNLOAD_EMAIL,
    DOWNLOAD_BRANCH_CD, DOWNLOAD_BRANCH_ETC,
    DOWNLOAD_SUPPLY_CD, DOWNLOAD_SUPPLY_ETC,
    DOWNLOAD_INTENT, DOWNLOAD_TYPE, DOWNLOAD_DOWN_SE,
    SESSION_JSESSIONID, SESSION_SCOUTER,
    S3_BUCKET, S3_PREFIX,
)

UPLOAD_CHECKPOINT = OUT_DIR / "upload_checkpoint.json"


# ── Custom exceptions ──────────────────────────────────────────────────────────

class SessionExpiredError(Exception):
    """Server returned an HTML login-redirect instead of a file."""


# ── S3 helpers ─────────────────────────────────────────────────────────────────

def make_s3_client():
    return boto3.client("s3")


def check_s3_prefix(s3) -> None:
    """Log whether the target S3 prefix already has objects."""
    try:
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX + "/", MaxKeys=1)
        if resp.get("KeyCount", 0) > 0:
            logger.info(f"S3 path s3://{S3_BUCKET}/{S3_PREFIX}/ already exists — resuming")
        else:
            logger.info(f"S3 path s3://{S3_BUCKET}/{S3_PREFIX}/ is new — will be created on first upload")
    except ClientError as e:
        logger.error(f"S3 prefix check failed: {e}")


def _s3_upload_file(s3, local_path: Path, key: str) -> None:
    s3.upload_file(str(local_path), S3_BUCKET, key)


def _s3_put_json(s3, data: dict, key: str) -> None:
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )


# ── Upload checkpoint ──────────────────────────────────────────────────────────

def _load_upload_checkpoint() -> set[str]:
    if UPLOAD_CHECKPOINT.exists():
        try:
            return set(json.loads(UPLOAD_CHECKPOINT.read_text()))
        except Exception:
            pass
    return set()


def _save_upload_checkpoint(done: set[str]) -> None:
    UPLOAD_CHECKPOINT.write_text(json.dumps(sorted(done)))


# ── Session helpers ────────────────────────────────────────────────────────────

def _load_cookies(session: requests.Session) -> bool:
    """Load cookies from KHS_JSESSIONID env var (EC2) or Chrome (local).
    Returns True if PROJECT2_JSESSIONID was found."""
    if SESSION_JSESSIONID:
        session.cookies.clear(domain="digital.khs.go.kr")
        session.cookies.set("PROJECT2_JSESSIONID", SESSION_JSESSIONID,
                            domain="digital.khs.go.kr", path="/")
        if SESSION_SCOUTER:
            session.cookies.set("SCOUTER", SESSION_SCOUTER,
                                domain="digital.khs.go.kr", path="/")
        logger.info("Session cookies loaded from KHS_JSESSIONID env var")
        return True

    try:
        import browser_cookie3
        jar = browser_cookie3.chrome(domain_name="digital.khs.go.kr")
        session.cookies.clear(domain="digital.khs.go.kr")
        for c in jar:
            session.cookies.set(c.name, c.value, domain=c.domain, path=c.path)
        names = [c.name for c in jar]
        if "PROJECT2_JSESSIONID" in names:
            logger.info("Session cookies loaded from Chrome (PROJECT2_JSESSIONID found)")
            return True
        logger.warning("Chrome cookies loaded but PROJECT2_JSESSIONID missing — log in first")
        return False
    except Exception as e:
        logger.error(f"Could not load cookies: {e}")
        return False


def _fetch_csrf(session: requests.Session, uid: str, biz_id: str) -> tuple[str, str]:
    """Fetch CSRF token from a detail page. Returns (header_name, token)."""
    try:
        r = session.get(
            f"{BASE_URL}{DETAIL_PATH}",
            params={"ichDataUid": uid, "bizId": biz_id},
            timeout=REQUEST_TIMEOUT,
        )
        soup = BeautifulSoup(r.text, "lxml")
        t = soup.find("meta", {"name": "_csrf"})
        h = soup.find("meta", {"name": "_csrf_header"})
        token  = t["content"] if t else ""
        header = h["content"] if h else "X-CSRF-TOKEN"
        if token:
            logger.info(f"CSRF token obtained ({token[:8]}…)")
        else:
            logger.warning("CSRF token not found — registration may be silently ignored")
        return header, token
    except Exception as e:
        logger.warning(f"Could not fetch CSRF token: {e}")
        return "X-CSRF-TOKEN", ""


# ── SessionManager ─────────────────────────────────────────────────────────────

class SessionManager:
    """Thread-safe session holder with keepalive and auto-refresh."""

    KEEPALIVE_INTERVAL = 600   # ping every 10 min
    REFRESH_COOLDOWN   = 60    # max one refresh per 60 s

    def __init__(self, seed_uid: str, seed_biz_id: str):
        self._lock         = threading.Lock()
        self._seed_uid     = seed_uid
        self._seed_biz_id  = seed_biz_id
        self._session: requests.Session | None = None
        self._csrf_header  = "X-CSRF-TOKEN"
        self._csrf_token   = ""
        self._last_refresh = 0.0
        self._stop_evt     = threading.Event()
        self._build()

    def get(self) -> tuple[requests.Session, str, str]:
        return self._session, self._csrf_header, self._csrf_token

    def refresh(self) -> None:
        with self._lock:
            if time.time() - self._last_refresh < self.REFRESH_COOLDOWN:
                return
            logger.warning("Session expired — rebuilding from cookies…")
            self._build()

    def start_keepalive(self) -> None:
        self._stop_evt.clear()
        t = threading.Thread(target=self._keepalive_loop, daemon=True, name="keepalive")
        t.start()
        logger.info(f"Keepalive thread started (ping every {self.KEEPALIVE_INTERVAL}s)")

    def stop_keepalive(self) -> None:
        self._stop_evt.set()

    def _build(self) -> None:
        s = requests.Session()
        s.headers.update(HEADERS)
        ok = _load_cookies(s)
        header, token = _fetch_csrf(s, self._seed_uid, self._seed_biz_id) if ok else ("X-CSRF-TOKEN", "")
        self._session     = s
        self._csrf_header = header
        self._csrf_token  = token
        self._last_refresh = time.time()

    def _keepalive_loop(self) -> None:
        while not self._stop_evt.wait(timeout=self.KEEPALIVE_INTERVAL):
            try:
                r = self._session.get(
                    f"{BASE_URL}{LIST_PATH}", params=LIST_PARAMS, timeout=10
                )
                logger.debug(f"Keepalive ping → HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"Keepalive ping failed: {e}")


# ── Image helpers ──────────────────────────────────────────────────────────────

def _ext_from_content_type(content_type: str) -> str:
    if content_type:
        g = mimetypes.guess_extension(content_type.split(";")[0].strip())
        if g and g not in (".jpe", ""):
            return g
        if g == ".jpe":
            return ".jpg"
    return ".jpg"


def _is_valid_image(raw: bytes) -> bool:
    if not raw:
        return False
    head = raw[:16]
    if head[:2] == b'\xff\xd8':                                           # JPEG
        return True
    if head[:8] == b'\x89PNG\r\n\x1a\n':                                  # PNG
        return True
    if head[:4] in (b'MM\x00*', b'II*\x00', b'II\x2b\x00', b'MM\x00\x2b'):  # TIFF
        return True
    if head[:4] == b'%PDF':                                               # PDF
        return True
    return False


def _is_login_redirect(raw: bytes) -> bool:
    sample = raw[:512].lower()
    return b"snsloginm" in sample or (b"<html" in sample and b"login" in sample)


# ── Two-step download ──────────────────────────────────────────────────────────

def _build_register_payload(uid: str) -> dict:
    return {
        "age":          DOWNLOAD_AGE,
        "email":        DOWNLOAD_EMAIL,
        "useBranchCd":  DOWNLOAD_BRANCH_CD,
        "useBranchEtc": DOWNLOAD_BRANCH_ETC,
        "useSupplyCd":  DOWNLOAD_SUPPLY_CD,
        "useSupplyEtc": DOWNLOAD_SUPPLY_ETC,
        "useIntent":    DOWNLOAD_INTENT,
        "ichDataUid":   uid,
        "type":         DOWNLOAD_TYPE,
        "downSe":       DOWNLOAD_DOWN_SE,
    }


def _download_one(
    session: requests.Session,
    uid: str,
    dl_type: str,
    down_file_se: str,
    csrf_header: str,
    csrf_token: str,
) -> tuple[bytes, str]:
    # Step 1: register
    payload = _build_register_payload(uid)
    payload["type"]   = dl_type
    payload["downSe"] = down_file_se
    session.post(
        f"{BASE_URL}{DOWNLOAD_PATH}",
        data=payload,
        headers={csrf_header: csrf_token} if csrf_token else {},
        timeout=REQUEST_TIMEOUT,
    )

    # Step 2: execute — GET (JS downFile() has typo "mtthod", form defaults to GET)
    with session.get(
        f"{BASE_URL}{DOWNLOAD_EXEC_PATH}",
        params={"type": dl_type, "ichDataUid": uid,
                "downFileSe": down_file_se, "downSe": ""},
        headers={"Referer": f"{BASE_URL}{DETAIL_PATH}?ichDataUid={uid}"},
        timeout=REQUEST_TIMEOUT,
        stream=True,
    ) as r:
        r.raise_for_status()
        ct  = r.headers.get("Content-Type", "")
        raw = b"".join(r.iter_content(65536))

    if _is_login_redirect(raw):
        raise SessionExpiredError(f"Login redirect for uid={uid}")
    if not _is_valid_image(raw):
        raise ValueError(f"Non-image response ({len(raw)}B, ct={ct}) uid={uid} type={dl_type}")
    return raw, ct


@retry(
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=wait_exponential(min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX),
    reraise=True,
)
def _fetch_original(
    session, uid, download_options, csrf_header, csrf_token
) -> tuple[bytes, str, str]:
    order   = {"L": 0, "H": 1}
    options = sorted(download_options, key=lambda o: order.get(o["type"], 99))
    if not options:
        options = [{"type": "L", "downFileSe": "SOn"},
                   {"type": "H", "downFileSe": "OOn"}]

    last_err = ""
    for opt in options:
        try:
            raw, ct = _download_one(session, uid, opt["type"], opt["downFileSe"],
                                    csrf_header, csrf_token)
            return raw, ct, opt["type"]
        except SessionExpiredError:
            raise
        except Exception as e:
            last_err = str(e)
            logger.debug(f"type={opt['type']} failed for {uid}: {e}")

    raise ValueError(f"All options failed for uid={uid}: {last_err}")


# ── Per-item processing ────────────────────────────────────────────────────────

def _process_item(
    mgr: SessionManager,
    item: dict,
    s3,
    done_uids: set[str],
    done_lock: threading.Lock,
) -> dict:
    uid              = item["ich_data_uid"]
    download_options = item.get("download_options") or []

    # Skip if already uploaded (checkpoint)
    with done_lock:
        if uid in done_uids:
            item["asset_status"] = "ok:cached"
            return item

    s3_image_key = ""
    dl_type_used = ""
    first_error  = ""

    for attempt in range(2):
        session, csrf_header, csrf_token = mgr.get()
        try:
            raw, ct, dl_type_used = _fetch_original(
                session, uid, download_options, csrf_header, csrf_token
            )
            ext = _ext_from_content_type(ct)

            # Write to temp file, upload to S3, then delete
            with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tf:
                tmp = Path(tf.name)
            try:
                tmp.write_bytes(raw)
                image_filename = f"image{ext}"
                s3_image_key   = f"{S3_PREFIX}/{uid}/{image_filename}"
                _s3_upload_file(s3, tmp, s3_image_key)
            finally:
                tmp.unlink(missing_ok=True)

            # Upload metadata
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            _s3_put_json(s3, {
                "ich_data_uid":       uid,
                "biz_id":             item.get("biz_id", ""),
                "title":              item.get("title", ""),
                "heritage_type":      item.get("heritage_type", ""),
                "kogl_type":          item.get("kogl_type", "1"),
                "detail_page_url":    item.get("detail_page_url", ""),
                "download_options":   download_options,
                "download_type_used": dl_type_used,
                "s3_image_key":       f"s3://{S3_BUCKET}/{s3_image_key}",
                "download_timestamp": ts,
                "list_meta":          item.get("list_meta", {}),
                "detail_meta":        item.get("detail_meta", {}),
            }, key=f"{S3_PREFIX}/{uid}/metadata.json")

            item["asset_status"] = "ok"
            item["s3_image_key"] = f"s3://{S3_BUCKET}/{s3_image_key}"

            # Mark done in checkpoint
            with done_lock:
                done_uids.add(uid)
                _save_upload_checkpoint(done_uids)
            break

        except SessionExpiredError as e:
            first_error = str(e)
            if attempt == 0:
                mgr.refresh()
        except Exception as e:
            first_error = f"{type(e).__name__}: {e}"
            logger.debug(f"Download failed {uid}: {first_error}")
            break

    if not item.get("asset_status", "").startswith("ok"):
        item["asset_status"] = f"error:{first_error[:80]}"

    return item


# ── Public API ─────────────────────────────────────────────────────────────────

def download_assets(items: list[dict]) -> dict:
    """Download originals and upload to S3 for all KOGL 제1유형 items."""
    if not items:
        return {"total": 0, "ok": 0, "skipped": 0, "errors": 0,
                "elapsed_sec": 0, "error_log": []}

    s3  = make_s3_client()
    check_s3_prefix(s3)

    done_uids = _load_upload_checkpoint()
    done_lock = threading.Lock()
    if done_uids:
        logger.info(f"Upload checkpoint: {len(done_uids):,} items already uploaded")

    seed = items[0]
    mgr  = SessionManager(seed_uid=seed["ich_data_uid"], seed_biz_id=seed.get("biz_id", ""))
    mgr.start_keepalive()

    ok = skipped = errors = 0
    error_log: list[dict] = []
    t0 = time.perf_counter()

    logger.info(
        f"Uploading {len(items):,} items to s3://{S3_BUCKET}/{S3_PREFIX}/ "
        f"({ASSET_WORKERS} workers)"
    )

    try:
        with ThreadPoolExecutor(max_workers=ASSET_WORKERS) as pool:
            futs = {
                pool.submit(_process_item, mgr, item, s3, done_uids, done_lock): item
                for item in items
            }
            with tqdm(total=len(items), desc="S3 uploads", unit=" items",
                      dynamic_ncols=True, colour="green", smoothing=0) as pbar:
                for fut in as_completed(futs):
                    item   = fut.result()
                    status = item.get("asset_status", "")
                    if status.startswith("ok"):
                        ok += 1
                    elif status.startswith("skipped"):
                        skipped += 1
                    else:
                        errors += 1
                        error_log.append({
                            "ich_data_uid": item.get("ich_data_uid", ""),
                            "title":        item.get("title", ""),
                            "asset_status": status,
                        })
                    pbar.update(1)
    finally:
        mgr.stop_keepalive()

    elapsed = round(time.perf_counter() - t0, 1)
    logger.success(
        f"Done | {ok:,} uploaded | {skipped:,} skipped | {errors:,} errors | {elapsed}s"
    )
    return {
        "total": len(items), "ok": ok, "skipped": skipped,
        "errors": errors, "elapsed_sec": elapsed, "error_log": error_log,
    }
