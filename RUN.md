# heritage_crawler — Run Instructions

## Prerequisites

Python 3.11+ and `uv` (or `pip`).

```bash
cd ~/Desktop/heritage_crawler
pip install requests beautifulsoup4 lxml loguru tenacity tqdm
```

## Pilot test (5 pages = ~60 items, all 3 phases)

Clear any stale checkpoints before a fresh pilot run:

```bash
cd ~/Desktop/heritage_crawler
rm -f output/list_checkpoint.json output/list_items.jsonl \
       output/detail_checkpoint.json output/detail_items.jsonl
python main.py pilot
```

This will:
1. Scrape list pages 1–5 (~60 item stubs)
2. Fetch each detail page and filter for KOGL 제1유형
3. Download **one thumbnail per item** → `assets/{ichDataUid}/`
   - Each list record = 1 image; '관련 이미지' (other records in the same
     BIZ project) are separate records crawled naturally through pagination
4. Write results to `output/heritage_pilot_*/`

Expected output:
```
output/
  heritage_pilot_YYYYMMDD_HHMMSS/
    results.json      ← kept items (KOGL 제1유형 only)
    stats.json        ← phase timings, counts, KOGL rate
    errors.json       ← any download failures
assets/
  {ichDataUid}/
    thumb_1.jpg       ← single thumbnail per item
    metadata.json     ← full item record
```

> **Note on image resolution**: `type=T` (thumbnail, ~30 KB) is the only
> publicly accessible endpoint on digital.khs.go.kr.  Original TIF/JPG
> files require an authenticated account + application form.

## Full run (all 44,496 pages ≈ 533k items)

```bash
tmux new -s heritage
cd ~/Desktop/heritage_crawler
python main.py full
# Ctrl+B D to detach
```

Full run checkpoints after every page/item — safe to interrupt and resume.

## Note on network access

`digital.khs.go.kr` may be blocked on the corporate proxy.
Run from home network, VPN, or a machine without the proxy restriction.
