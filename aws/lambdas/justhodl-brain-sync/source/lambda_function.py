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
import urllib.request, os
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/brain.json"
BRAIN_URL = "https://justhodl-data-proxy.raafouis.workers.dev/brain"
MODEL = "claude-haiku-4-5-20251001"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "") or os.environ.get("ANTHROPIC_API_KEY", "")
s3 = boto3.client("s3", region_name=REGION)

CAT_LABEL = {"philosophy": "Philosophy", "rule": "Rule", "thesis": "Thesis",
             "macro": "Macro View", "watchlist": "Watchlist", "lesson": "Lesson",
             "reminder": "Reminder"}


def _ai_extract(notes_text):
    """Have Claude READ the brain and produce a structured directive layer the
    engines can act on — real understanding, not keyword matching. Returns a dict
    with sector tilts, themes, hard rules, avoid-list, watched tickers, risk
    posture, and a one-paragraph 'investor profile'. Cached; only re-runs when
    the brain content hash changes (handled by caller)."""
    if not ANTHROPIC_KEY or not notes_text.strip():
        return None
    system = (
        "You are distilling an investor's personal notes into a STRUCTURED PROFILE that "
        "automated trading-signal engines will use to bias toward what this investor cares "
        "about. Read their notes and extract their actual investing worldview. "
        "Return STRICT JSON only, no prose: {"
        "\"investor_profile\": \"<2-3 sentence summary of how they think about markets>\", "
        "\"hard_rules\": [\"<imperative rules they live by, e.g. 'Do not treat QT ending as bullish'>\"], "
        "\"sector_tilts\": {\"<sector>\": \"<overweight|underweight|avoid> — <why>\"}, "
        "\"themes\": [\"<themes they're focused on, e.g. 'AI capex buildout', 'energy scarcity'>\"], "
        "\"watched_tickers\": [\"<tickers they explicitly track>\"], "
        "\"avoid\": [\"<things/sectors/setups they want to avoid>\"], "
        "\"risk_posture\": \"<aggressive|balanced|defensive> — <one line>\", "
        "\"signal_emphasis\": [\"<which signal types matter most to them: e.g. insider, buyback, capex, dislocation, macro>\"]}"
    )
    try:
        body = {"model": MODEL, "max_tokens": 900, "system": system,
                "messages": [{"role": "user", "content": "INVESTOR NOTES:\n" + notes_text[:9000]}]}
        req = urllib.request.Request("https://api.anthropic.com/v1/messages",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01"})
        r = json.loads(urllib.request.urlopen(req, timeout=30).read().decode())
        txt = "".join(b.get("text", "") for b in r.get("content", []) if b.get("type") == "text")
        import re as _re
        txt = _re.sub(r"^```(?:json)?\s*|\s*```$", "", txt.strip())
        return json.loads(txt)
    except Exception as e:
        print(f"[brain-sync] AI extract err: {str(e)[:80]}")
        return None


def read_prev():
    """Previous brain.json output (for hash-gating the AI extraction)."""
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
    except Exception:
        return {}


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

    # ── SMART layer: AI-extracted structured directive (hash-gated so we only
    # spend a Claude call when the brain actually changes) ──
    import hashlib
    all_text = "\n".join(f"[{'PINNED ' if n.get('pinned') else ''}{n.get('cat')}] {n.get('text','')}" for n in notes)
    content_hash = hashlib.sha256(all_text.encode()).hexdigest()[:16]
    directive = None
    prev = read_prev()
    if notes and (prev.get("content_hash") != content_hash or not prev.get("directive")):
        directive = _ai_extract(all_text)
    else:
        directive = prev.get("directive")  # reuse — content unchanged

    out = {
        "engine": "brain-sync", "version": "2.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_updated_at": d.get("updated_at"),
        "content_hash": content_hash,
        "n_notes": len(notes), "n_pinned": len(pinned),
        "notes": notes,
        "prompt_block": prompt_block,
        "directive": directive,                # ← the smart, structured worldview
        "mentioned_tickers": sorted(tickers | set((directive or {}).get("watched_tickers", []) or [])),
        "categories": {CAT_LABEL.get(k, k): len(v) for k, v in by_cat.items()},
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=300")
    print(f"[brain-sync] DONE {round(time.time()-t0,1)}s — {len(notes)} notes ({len(pinned)} pinned), "
          f"directive={'fresh' if (directive and prev.get('content_hash') != content_hash) else 'cached' if directive else 'none'}")
    return {"statusCode": 200, "body": json.dumps({"n_notes": len(notes), "n_pinned": len(pinned)})}
