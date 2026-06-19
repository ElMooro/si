"""
justhodl-stocktwits — RETAIL ATTENTION & SENTIMENT (free, no key)
=================================================================
Retail attention often front-runs small-cap pumps. This pulls Stocktwits' trending
symbols and, for the trending names + your AI-infra universe, the recent message-level
bull/bear sentiment and message velocity. Surfaces where retail crowding is building —
useful as a momentum confirm AND as an overheating caution (extreme bullishness on a
microcap is a fade risk, not a green light).

OUTPUT data/stocktwits.json   SCHEDULE daily 12:15 UTC. Real data, research only.
"""
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/stocktwits.json"
MAX_SYMBOLS = 34
THROTTLE = 0.7
s3 = boto3.client("s3", region_name="us-east-1")


def _get(url):
    try:
        return json.loads(urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 jh-st"}), timeout=15).read())
    except Exception:
        return None
    finally:
        time.sleep(THROTTLE)


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def symbol_sentiment(sym):
    d = _get(f"https://api.stocktwits.com/api/2/streams/symbol/{urllib.parse.quote(sym)}.json")
    msgs = (d or {}).get("messages", []) if isinstance(d, dict) else []
    bull = bear = 0
    for m in msgs:
        b = (((m.get("entities") or {}).get("sentiment") or {}) or {}).get("basic")
        if b == "Bullish":
            bull += 1
        elif b == "Bearish":
            bear += 1
    n = len(msgs)
    tagged = bull + bear
    return {"n_msgs": n, "bullish": bull, "bearish": bear,
            "bull_pct": round(bull / tagged * 100, 1) if tagged else None}


def lambda_handler(event, context):
    t0 = time.time()
    trend = _get("https://api.stocktwits.com/api/2/trending/symbols.json")
    trending = []
    for s in (trend or {}).get("symbols", []) if isinstance(trend, dict) else []:
        sym = s.get("symbol")
        # skip crypto/non-equity (e.g. CHIP.X) trending entries
        if sym and "." not in sym and (s.get("exchange") or "").upper() != "CRYPTO":
            trending.append(sym)

    # universe to gauge: trending + your AI-infra names (bounded)
    syms, seen = [], set()
    for s in trending:
        if s not in seen:
            seen.add(s); syms.append(s)
    stack = _read("data/ai-infra-stack.json") or {}
    for layer in stack.get("stack", []):
        for n in (layer.get("names", []) or [])[:4]:
            s = n.get("symbol")
            if s and s not in seen:
                seen.add(s); syms.append(s)
    syms = syms[:MAX_SYMBOLS]

    sentiment = {}
    for sym in syms:
        sentiment[sym] = symbol_sentiment(sym)
        if time.time() - t0 > 180:
            break

    # bullish buzz = high message volume + high bull%
    buzz = [{"symbol": k, **v} for k, v in sentiment.items()
            if (v["n_msgs"] or 0) >= 5 and v["bull_pct"] is not None]
    buzz.sort(key=lambda x: ((x["bull_pct"] or 0) * (x["n_msgs"] or 0)), reverse=True)
    out = {
        "engine": "stocktwits", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Retail attention/sentiment from Stocktwits — builds before small-cap pumps; extreme "
                  "readings also flag overheating to fade.",
        "trending_equities": trending[:20],
        "top_bullish_buzz": buzz[:15],
        "sentiment": sentiment,
        "source": "Stocktwits API v2 (free)",
        "caveats": "Retail sentiment is attention/crowding, not edge — extreme bullishness on a microcap is "
                   "a fade risk as often as a signal. Use as a crowding gauge alongside fundamentals, not alone. "
                   "Research only, not investment advice.",
        "elapsed_s": round(time.time() - t0, 1),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json")
    print(f"[stocktwits] trending={len(trending)} gauged={len(sentiment)} buzz={len(buzz)} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "trending": len(trending),
            "gauged": len(sentiment), "buzz": len(buzz)})}
