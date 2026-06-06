"""justhodl-brain-sync — mirrors the user's "brain" (investing philosophy, rules,
theses, reminders) from the Cloudflare KV store to S3 data/brain.json, so the
platform's engines (morning intelligence, AI brief, ask) can read what's on
Khalid's mind and weight the system toward what matters to him.

The brain is authored on /brain.html → stored in KV via the worker /brain route.
This Lambda fetches the public GET /brain and writes a clean S3 copy + a
pre-formatted text block the AI engines can drop straight into a prompt.

SCHEDULE: every 15 min.
"""
import json, time
import urllib.request
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/brain.json"
BRAIN_URL = "https://justhodl-data-proxy.raafouis.workers.dev/brain"
s3 = boto3.client("s3", region_name=REGION)

CAT_LABEL = {"philosophy": "Philosophy", "rule": "Rule", "thesis": "Thesis",
             "macro": "Macro View", "watchlist": "Watchlist", "lesson": "Lesson",
             "reminder": "Reminder"}


def lambda_handler(event=None, context=None):
    t0 = time.time()
    try:
        req = urllib.request.Request(BRAIN_URL + "?sync=1", headers={"User-Agent": "JustHodl-BrainSync/1.0"})
        d = json.loads(urllib.request.urlopen(req, timeout=15).read().decode())
    except Exception as e:
        print(f"[brain-sync] fetch err: {str(e)[:80]}")
        return {"statusCode": 502, "body": "fetch failed"}

    notes = d.get("notes") or []
    # newest first; pinned float to top
    notes = sorted(notes, key=lambda n: (0 if n.get("pinned") else 1, -(n.get("created") or 0)))

    # Pre-format a compact prompt block the AI engines can inject directly.
    pinned = [n for n in notes if n.get("pinned")]
    lines = []
    if pinned:
        lines.append("PINNED PRINCIPLES (highest priority — always honor these):")
        for n in pinned:
            lines.append(f"  • [{CAT_LABEL.get(n.get('cat'), n.get('cat'))}] {n.get('text','').strip()}")
        lines.append("")
    by_cat = {}
    for n in notes:
        if n.get("pinned"):
            continue
        by_cat.setdefault(n.get("cat", "reminder"), []).append(n)
    for cat in ["philosophy", "rule", "thesis", "macro", "watchlist", "lesson", "reminder"]:
        items = by_cat.get(cat)
        if items:
            lines.append(f"{CAT_LABEL.get(cat, cat).upper()}:")
            for n in items[:12]:
                lines.append(f"  • {n.get('text','').strip()}")
            lines.append("")
    prompt_block = "\n".join(lines).strip()

    # Extract watchlist-ish tickers mentioned (UPPERCASE 1-5 char tokens) for
    # engines that want to bias toward what Khalid is watching.
    import re
    tickers = set()
    for n in notes:
        for m in re.findall(r"\b[A-Z]{1,5}\b", n.get("text", "")):
            if m not in {"QT", "QE", "RRP", "SOFR", "IORB", "EFFR", "TGA", "AI", "USD", "FED", "CPI", "PPI", "ETF", "I", "A", "US", "GDP", "BTC", "DCF"}:
                tickers.add(m)

    out = {
        "engine": "brain-sync", "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_updated_at": d.get("updated_at"),
        "n_notes": len(notes), "n_pinned": len(pinned),
        "notes": notes,
        "prompt_block": prompt_block,
        "mentioned_tickers": sorted(tickers),
        "categories": {CAT_LABEL.get(k, k): len(v) for k, v in by_cat.items()},
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=300")
    print(f"[brain-sync] DONE {round(time.time()-t0,1)}s — {len(notes)} notes ({len(pinned)} pinned)")
    return {"statusCode": 200, "body": json.dumps({"n_notes": len(notes), "n_pinned": len(pinned)})}
