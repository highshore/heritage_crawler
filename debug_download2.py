"""
Debug v2: Test the full two-step download with session cookies + CSRF token.
Usage: uv run python debug_download2.py
"""
import requests
from bs4 import BeautifulSoup

BASE_URL         = "https://digital.khs.go.kr"
LIST_URL         = f"{BASE_URL}/record/recordImg.do?locationYn=N&pageSe=Img&searchClick=N"
DETAIL_URL       = f"{BASE_URL}/record/recordDetailImg.do"
REGISTER_URL     = f"{BASE_URL}/record/downloadObjectStorageFile.do"
EXECUTE_URL      = f"{BASE_URL}/record/downloadObjectStorageFileExecute.do"
TEST_UID         = "13898237364257494156"
TEST_BIZ_ID      = "2021116053"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer":         BASE_URL,
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Accept":          "*/*",
}

s = requests.Session()
s.headers.update(HEADERS)

# ── Step 1: Load Chrome cookies ────────────────────────────────────────────────
print("=" * 60)
print("STEP 1: Loading Chrome cookies")
try:
    import browser_cookie3
    jar = browser_cookie3.chrome(domain_name="digital.khs.go.kr")
    loaded = []
    for c in jar:
        s.cookies.set(c.name, c.value, domain=c.domain, path=c.path)
        loaded.append(c.name)
    print(f"  Cookies loaded: {loaded}")
    if "PROJECT2_JSESSIONID" not in loaded:
        print("  ⚠️  PROJECT2_JSESSIONID not found — are you logged in to Chrome?")
    else:
        print(f"  ✓ PROJECT2_JSESSIONID = {s.cookies.get('PROJECT2_JSESSIONID', '')[:16]}...")
except Exception as e:
    print(f"  ✗ browser_cookie3 failed: {e}")

# ── Step 2: Fetch CSRF token from detail page ──────────────────────────────────
print("\nSTEP 2: Fetching CSRF token from detail page")
try:
    r = s.get(DETAIL_URL, params={"ichDataUid": TEST_UID, "bizId": TEST_BIZ_ID}, timeout=20)
    soup = BeautifulSoup(r.text, "lxml")
    token_tag  = soup.find("meta", {"name": "_csrf"})
    header_tag = soup.find("meta", {"name": "_csrf_header"})
    csrf_token  = token_tag["content"]  if token_tag  else ""
    csrf_header = header_tag["content"] if header_tag else "X-CSRF-TOKEN"
    print(f"  csrf_header: {csrf_header}")
    print(f"  csrf_token:  {csrf_token[:12]}..." if csrf_token else "  ✗ CSRF token NOT FOUND")

    import re
    m = re.search(r"loginMberNm\s*=\s*['\"]([^'\"]*)['\"]", r.text)
    if m:
        print(f"  loginMberNm: '{m.group(1)}' ({'✓ logged in' if m.group(1) else '✗ NOT logged in'})")
except Exception as e:
    print(f"  ✗ Failed: {e}")
    csrf_token, csrf_header = "", "X-CSRF-TOKEN"

# ── Step 3: Fetch detail page and extract download options ─────────────────────
print(f"\nSTEP 3: Fetching detail page for uid={TEST_UID}")
try:
    import re
    r = s.get(DETAIL_URL, params={"ichDataUid": TEST_UID, "bizId": TEST_BIZ_ID}, timeout=20)
    soup = BeautifulSoup(r.text, "lxml")

    # Find openDownloadPopup calls
    pattern = re.compile(r"openDownloadPopup\(\s*'([^']+)'\s*,\s*'(\d+)'\s*,\s*'([^']+)'")
    found = []
    # Check onclick attributes
    for tag in soup.find_all(True):
        for attr in ("onclick", "href"):
            val = tag.get(attr, "")
            for m in pattern.finditer(val):
                found.append({"type": m.group(1), "uid": m.group(2), "downFileSe": m.group(3), "source": attr})
    # Check script tags
    for script in soup.find_all("script"):
        for m in pattern.finditer(script.get_text()):
            found.append({"type": m.group(1), "uid": m.group(2), "downFileSe": m.group(3), "source": "script"})

    print(f"  Download options found: {found}")
    if not found:
        print("  ⚠️  No openDownloadPopup calls found on detail page!")
        # Search raw HTML
        raw_matches = pattern.findall(r.text)
        print(f"  Raw HTML search: {raw_matches[:5]}")
except Exception as e:
    print(f"  ✗ Failed: {e}")
    found = []

# ── Step 4: Register download ─────────────────────────────────────────────────
print(f"\nSTEP 4: POST to register endpoint (type=L)")
try:
    payload = {
        "age": "30", "email": "sookyum.kim@flitto.com",
        "useBranchCd": "A", "useBranchEtc": "",
        "useSupplyCd": "A", "useSupplyEtc": "",
        "useIntent": "연구목적으로 사용예정입니다",
        "ichDataUid": TEST_UID, "type": "L", "downSe": "SOn",
    }
    r = s.post(REGISTER_URL, data=payload,
               headers={csrf_header: csrf_token} if csrf_token else {},
               timeout=20)
    print(f"  Status: {r.status_code}  Body: {len(r.content)} bytes  CT: {r.headers.get('Content-Type','')}")
except Exception as e:
    print(f"  ✗ Failed: {e}")

# ── Step 5: Execute download ───────────────────────────────────────────────────
print(f"\nSTEP 5: GET to execute endpoint (type=L)")
try:
    exec_params = {"type": "L", "ichDataUid": TEST_UID, "downFileSe": "SOn", "downSe": ""}
    r = s.get(EXECUTE_URL, params=exec_params,
              headers={"Referer": f"{BASE_URL}/record/recordDetailImg.do?ichDataUid={TEST_UID}"},
              timeout=30)
    ct  = r.headers.get("Content-Type", "")
    raw = r.content
    first8 = raw[:8].hex()
    print(f"  Status: {r.status_code}  Body: {len(raw)} bytes  CT: {ct}")
    print(f"  First 8 bytes hex: {first8}")

    if raw[:2] == b'\xff\xd8':
        print("  ✓ Valid JPEG!")
        with open("/tmp/test_image.jpg", "wb") as f:
            f.write(raw)
        print("  Saved to /tmp/test_image.jpg")
    elif b"<html" in raw[:200].lower() or b"<script" in raw[:200].lower():
        print("  ✗ Server returned HTML/script — likely not authenticated or session expired")
        print(f"  Preview: {raw[:300].decode('utf-8','replace')}")
    else:
        print(f"  ? Unknown format")
        print(f"  Preview: {raw[:200]}")
except Exception as e:
    print(f"  ✗ Failed: {e}")

# ── Step 6: Try type=H as well ─────────────────────────────────────────────────
print(f"\nSTEP 6: GET to execute endpoint (type=H, downFileSe=OOn)")
try:
    exec_params = {"type": "H", "ichDataUid": TEST_UID, "downFileSe": "OOn", "downSe": ""}
    r = s.get(EXECUTE_URL, params=exec_params,
              headers={"Referer": f"{BASE_URL}/record/recordDetailImg.do?ichDataUid={TEST_UID}"},
              timeout=30)
    ct  = r.headers.get("Content-Type", "")
    raw = r.content
    first8 = raw[:8].hex()
    print(f"  Status: {r.status_code}  Body: {len(raw)} bytes  CT: {ct}")
    print(f"  First 8 bytes hex: {first8}")
    if raw[:2] == b'\xff\xd8':
        print("  ✓ Valid JPEG!")
    elif b"<html" in raw[:200].lower() or b"<script" in raw[:200].lower():
        print("  ✗ HTML response")
        print(f"  Preview: {raw[:300].decode('utf-8','replace')}")
    else:
        print(f"  ? Unknown: {raw[:100]}")
except Exception as e:
    print(f"  ✗ Failed: {e}")
