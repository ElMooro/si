"""justhodl-political-ai-investigation

For the top conviction politician buys, generate a deep AI investigation of
WHY the name might be a great buy — cross-referencing committee jurisdiction,
the legislative/contract catalysts that committee could influence, the cluster
of buyers, sector tailwinds, and JustHodl's own cascade/options signals.

Runs daily after political-intel. Output: data/political-ai-investigation.json
keyed by ticker. Consumed by chart-pro (POLITICIAN watchlist info panel) and
can feed the per-user notes-aware AI.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/political-ai-investigation.json"
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def _read_json(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _anthropic_key():
    if os.environ.get("ANTHROPIC_API_KEY"):
        return os.environ["ANTHROPIC_API_KEY"]
    for p in ["/justhodl/anthropic/api_key", "/anthropic/api_key"]:
        try:
            return ssm.get_parameter(Name=p, WithDecryption=True)["Parameter"]["Value"]
        except Exception:
            continue
    return ""


def call_claude(prompt, system="", max_tokens=500):
    key = _anthropic_key()
    if not key:
        return ""
    body = {"model": ANTHROPIC_MODEL, "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}]}
    if system:
        body["system"] = system
    req = urllib.request.Request("https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "anthropic-version": "2023-06-01", "x-api-key": key},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            data = json.loads(r.read().decode())
        for b in data.get("content", []):
            if b.get("type") == "text":
                return b.get("text", "").strip()
    except Exception as e:
        print(f"[claude] {e}")
    return ""


def build_context(ticker, prec, cascade, options):
    lines = [f"TICKER: {ticker} ({prec.get('asset','')})"]
    lines.append(f"Politician conviction score: {prec.get('conviction_score')}")
    lines.append(f"Distinct congressional buyers: {prec.get('n_buyers')} | net buys {prec.get('n_buys')} vs sells {prec.get('n_sells')}")
    lines.append(f"Cluster: {prec.get('cluster')} | Committee-relevant: {prec.get('committee_relevant')}")
    for m in (prec.get("committee_matches") or [])[:4]:
        lines.append(f"  • {m.get('member')} sits on {m.get('committee')} (jurisdiction match: {m.get('match')})")
    for b in (prec.get("buy_members") or [])[:5]:
        cm = " [COMMITTEE MATCH]" if b.get("committee_match") else ""
        lines.append(f"  • {b.get('member')} ({b.get('party')}-{b.get('state')}, {b.get('chamber')}) bought {b.get('amount')} on {b.get('tx_date')}{cm}")
    # JustHodl signal cross-reference
    for tier in ["alert_tier", "laggards_hot_themes", "medium_tier"]:
        for c in (cascade.get(tier) or []):
            if c.get("ticker") == ticker:
                lines.append(f"JustHodl cascade: {tier} score {c.get('combined_score')}, theme accel {c.get('theme_acceleration')}%, hot ETF {c.get('hot_etf')}")
                break
    for arr in [options.get("extreme_call_flow") or [], options.get("bullish_call_flow") or []]:
        for c in arr:
            if c.get("ticker") == ticker:
                lines.append(f"Options flow: C/P {c.get('cv_pv_ratio')}, smart-money blocks {c.get('n_smart_money_blocks')}")
                break
    return "\n".join(lines)


def lambda_handler(event, context):
    t0 = time.time()
    political = _read_json("data/political-intel.json") or {}
    cascade = _read_json("data/theme-cascade.json") or {}
    options = _read_json("data/polygon-options-flow.json") or {}

    # Investigate the union of top conviction + committee-relevant names
    names = {}
    for r in (political.get("top_conviction_buys") or [])[:20]:
        names[r["ticker"]] = r
    for r in (political.get("committee_relevant_buys") or [])[:15]:
        names[r["ticker"]] = r
    print(f"[pol-ai] investigating {len(names)} names")

    system = ("You are a hedge-fund analyst specializing in political-intelligence "
              "alpha. The edge in congressional trading is that COMMITTEE MEMBERS "
              "have non-public visibility into upcoming legislation, contracts, and "
              "regulation in their jurisdiction. Given the data, investigate WHY this "
              "stock might be a great buy. Be specific and decisive. If a committee "
              "member with relevant jurisdiction bought it, reason explicitly about "
              "what legislation/contracts/regulatory catalyst they might foresee. "
              "If it's just clustered buying without committee relevance, say so and "
              "weight it lower. 3-4 sentences. End with a one-line conviction verdict.")

    cache = _read_json(OUTPUT_KEY) or {"by_ticker": {}}
    by_ticker = cache.get("by_ticker") or {}
    cutoff = datetime.now(timezone.utc).timestamp() - 6 * 3600  # 6h cache

    n_gen = 0
    for ticker, prec in list(names.items())[:25]:
        # Skip if cached recently
        cached = by_ticker.get(ticker)
        if cached:
            try:
                if datetime.fromisoformat(cached.get("generated_at", "")).timestamp() > cutoff:
                    continue
            except Exception:
                pass
        ctx = build_context(ticker, prec, cascade, options)
        prompt = f"Investigate {ticker} as a buy idea based on congressional/committee trading:\n\n{ctx}\n\nWhy might this be a great buy?"
        thesis = call_claude(prompt, system=system, max_tokens=420)
        if thesis:
            by_ticker[ticker] = {
                "thesis": thesis,
                "conviction_score": prec.get("conviction_score"),
                "committee_relevant": prec.get("committee_relevant"),
                "n_buyers": prec.get("n_buyers"),
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
            n_gen += 1
            print(f"  ✓ {ticker}: {thesis[:90]}")

    out = {
        "schema_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model": ANTHROPIC_MODEL,
        "n_investigated": len(by_ticker),
        "n_generated_this_run": n_gen,
        "by_ticker": by_ticker,
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=600")
    print(f"[pol-ai] DONE {round(time.time()-t0,1)}s — generated {n_gen}, total {len(by_ticker)}")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "generated": n_gen, "total": len(by_ticker)})}
