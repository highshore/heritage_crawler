"""
Debug script: POST to downloadObjectStorageFile.do and print what we get back.
Usage: uv run python debug_download.py <ichDataUid>
       uv run python debug_download.py 13898237364257494156
"""
import sys
import requests

BASE_URL      = "https://digital.khs.go.kr"
DOWNLOAD_PATH = "/record/downloadObjectStorageFile.do"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer":         "https://digital.khs.go.kr/record/recordImg.do",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Accept":          "*/*",
}

uid = sys.argv[1] if len(sys.argv) > 1 else "13898237364257494156"

payload = {
    "age":          "30",
    "email":        "sookyum.kim@flitto.com",
    "useBranchCd":  "A",
    "useBranchEtc": "",
    "useSupplyCd":  "A",
    "useSupplyEtc": "",
    "useIntent":    "연구목적으로 사용예정입니다",
    "ichDataUid":   uid,
    "type":         "L",
    "downSe":       "SOn",
}

print(f"POST {BASE_URL}{DOWNLOAD_PATH}")
print(f"Payload: {payload}\n")

s = requests.Session()
s.headers.update(HEADERS)
r = s.post(f"{BASE_URL}{DOWNLOAD_PATH}", data=payload, timeout=30)

print(f"Status:        {r.status_code}")
print(f"Content-Type:  {r.headers.get('Content-Type', '(none)')}")
print(f"Content-Disp:  {r.headers.get('Content-Disposition', '(none)')}")
print(f"Content-Length:{r.headers.get('Content-Length', '(none)')}")
print(f"Body length:   {len(r.content)} bytes")
print(f"First 200 bytes (raw): {r.content[:200]}")
print()

# Is it an image?
if r.content[:2] == b'\xff\xd8':
    print("✓ Starts with FFD8 — valid JPEG")
elif r.content[:8] == b'\x89PNG\r\n\x1a\n':
    print("✓ Starts with PNG magic bytes")
elif r.content[:4] == b'%PDF':
    print("✓ This is a PDF")
elif r.content[:15].lower().lstrip().startswith(b'<!doctype') or b'<html' in r.content[:200].lower():
    print("✗ Server returned HTML (probably an error page or login redirect)")
    print("\n--- HTML preview ---")
    print(r.text[:1000])
else:
    print(f"? Unknown format. First bytes hex: {r.content[:16].hex()}")
