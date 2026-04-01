"""
Print the KHS session cookies from Chrome so you can paste them into EC2.

Usage (on your Mac):
    python get_cookies.py

Then copy the export lines and paste them into your EC2 tmux session.
"""
try:
    import browser_cookie3
except ImportError:
    print("Run:  pip install browser-cookie3")
    raise SystemExit(1)

jar = browser_cookie3.chrome(domain_name="digital.khs.go.kr")

cookies = {c.name: c.value for c in jar}

jsessionid = cookies.get("PROJECT2_JSESSIONID", "")
scouter    = cookies.get("scouter", "")

if not jsessionid:
    print("❌  PROJECT2_JSESSIONID not found.")
    print("   Make sure you are logged into https://digital.khs.go.kr in Chrome first.")
    raise SystemExit(1)

print("\n✅  Cookies found. Paste these into your EC2 tmux session:\n")
print(f"export KHS_JSESSIONID={jsessionid}")
if scouter:
    print(f"export KHS_SCOUTER={scouter}")
print()
