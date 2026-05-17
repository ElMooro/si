"""
justhodl-crypto-narratives — Crypto Narrative / Sector Rotation

═══════════════════════════════════════════════════════════════════════
WHY THIS EXISTS
───────────────
Crypto trades by NARRATIVE — AI, RWA, DePIN, L2s, memecoins, gaming,
restaking — not by individual coin. The CMC `categories` endpoint
(confirmed unlocked, ops/735) gives each narrative's aggregate size
and 24h momentum, which the platform was not pulling.

This engine ranks narratives by 24h average price change, surfaces
what's rotating in and out, measures narrative breadth, and pairs it
with the crypto Fear & Greed reading for risk context.

OUTPUT: data/crypto-narratives.json   SCHEDULE: every 6h
═══════════════════════════════════════════════════════════════════════
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/crypto-narratives.json"
CMC_KEY = os.environ.get("CMC_KEY", "17ba8e87-53f0-46f4-abe5-014d9cd99597")

MIN_MCAP = 50_000_000      # ignore micro categories — noise
MIN_TOKENS = 4

s3 = boto3.client("s3", region_name="us-east-1")


def cmc(path):
    url = f"https://pro-api.coinmarketcap.com{path}"
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={
                "X-CMC_PRO_API_KEY": CMC_KEY, "Accept": "application/json",
                "User-Agent": "JustHodl/1.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < 2:
                time.sleep(2 ** attempt)
                continue
            return None
        except Exception:
            if attempt < 2:
                time.sleep(1)
                continue
            return None
    return None


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def lambda_handler(event, context):
    t0 = time.time()

    cats_raw = (cmc("/v1/cryptocurrency/categories") or {}).get("data") or []
    cats = []
    for c in cats_raw:
        mcap = num(c.get("market_cap"))
        ntok = c.get("num_tokens") or 0
        apc = num(c.get("avg_price_change"))
        if mcap is None or apc is None:
            continue
        if mcap < MIN_MCAP or ntok < MIN_TOKENS:
            continue
        cats.append({
            "name": c.get("name") or c.get("title"),
            "num_tokens": int(ntok),
            "avg_price_change_24h": round(apc, 2),
            "market_cap": round(mcap),
            "market_cap_change_24h": round(num(c.get("market_cap_change")) or 0, 2),
            "volume_change_24h": round(num(c.get("volume_change")) or 0, 2),
        })

    cats.sort(key=lambda x: x["avg_price_change_24h"], reverse=True)
    n_up = sum(1 for c in cats if c["avg_price_change_24h"] > 0)
    breadth = round(n_up / len(cats) * 100, 1) if cats else None

    hot = cats[:12]
    cold = cats[-8:][::-1] if len(cats) > 8 else []

    fg = (cmc("/v3/fear-and-greed/latest") or {}).get("data") or {}
    fg_val = fg.get("value")
    fg_class = fg.get("value_classification")

    # decisive read
    if breadth is None:
        read, stance = "Category data unavailable.", "UNKNOWN"
    elif breadth >= 65:
        stance = "RISK-ON ROTATION"
        read = (f"{breadth}% of narratives are green — broad risk-on. "
                f"Leadership: {hot[0]['name']} ({hot[0]['avg_price_change_24h']:+}%).")
    elif breadth <= 35:
        stance = "RISK-OFF"
        read = (f"Only {breadth}% of narratives are green — broad risk-off. "
                f"Even the leader, {hot[0]['name']}, is "
                f"{hot[0]['avg_price_change_24h']:+}%.")
    else:
        stance = "SELECTIVE"
        read = (f"{breadth}% of narratives green — selective tape, money is "
                f"rotating not chasing. Hot: {hot[0]['name']} "
                f"({hot[0]['avg_price_change_24h']:+}%); cold: "
                f"{cold[0]['name'] if cold else 'n/a'}.")

    out = {
        "schema_version": "1.0",
        "method": "cmc_category_rotation",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "stance": stance,
        "read": read,
        "narrative_breadth_pct": breadth,
        "n_categories": len(cats),
        "fear_greed": {"value": fg_val, "classification": fg_class},
        "hot": hot,
        "cold": cold,
        "note": ("Crypto narrative rotation from CoinMarketCap categories — "
                 "24h average price change aggregated per sector. Breadth is "
                 "the share of narratives in the green. A momentum read, not "
                 "advice; micro categories below $50M are filtered out."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=1800")
    print(f"[crypto-narratives] stance={stance} breadth={breadth}% "
          f"{len(cats)} categories {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "stance": stance, "narrative_breadth_pct": breadth,
        "n_categories": len(cats)})}
