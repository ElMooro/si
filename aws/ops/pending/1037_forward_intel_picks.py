#!/usr/bin/env python3
"""Step 1037 — pull the live top picks from the forward-intelligence layer
and write a human-readable picks summary."""
import json, os, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1037_forward_intel_picks.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)


def fetch(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        return {"_err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    fwd  = fetch("data/forward-orders.json")
    rot  = fetch("data/rotation-chains.json")
    buzz = fetch("data/buzz-velocity.json")
    fi   = fetch("data/future-intelligence.json")
    
    # ── 1. Forward orders top picks (filter US-only)
    out["forward_orders_top_10"] = []
    for r in (fwd.get("top_25_by_score") or [])[:25]:
        ticker = r.get("ticker", "")
        if "." in ticker:  # filter foreign-listed (e.g. .BA = Buenos Aires)
            continue
        out["forward_orders_top_10"].append({
            "ticker":         ticker,
            "composite":      r.get("composite"),
            "name":           (r.get("name") or "")[:60],
            "rpo_usd_bn":     round((r.get("data", {}).get("rpo_latest_usd") or 0) / 1e9, 2),
            "rpo_yield_pct":  r.get("data", {}).get("rpo_yield_pct"),
            "rpo_growth_yoy": r.get("data", {}).get("rpo_growth_yoy_pct"),
            "book_to_bill":   r.get("data", {}).get("book_to_bill_spread_pct"),
            "thesis":         r.get("thesis"),
        })
        if len(out["forward_orders_top_10"]) >= 10:
            break
    
    # ── 2. Rotation chains
    out["rotation_chains"] = []
    for name, c in (rot.get("chains") or {}).items():
        if c.get("current_leader_tier") is None:
            continue
        out["rotation_chains"].append({
            "chain":             name,
            "leader_tier":       c.get("current_leader_tier"),
            "leader_perf_30d":   c.get("leader_perf_30d_pct"),
            "next_tier_perf":    c.get("next_tier_perf_30d"),
            "expected_catchup":  c.get("expected_catchup_pct"),
            "state":             c.get("rotation_state"),
            "next_up_tickers":   [
                {"ticker": t["ticker"], "lag_pct": t["lag_pct"], "score": t["score"]}
                for t in (c.get("next_up_tickers") or [])[:5]
                if "." not in t.get("ticker", "")
            ],
        })
    
    # ── 3. Buzz stealth (US only)
    out["buzz_stealth_us_only"] = []
    for r in (buzz.get("stealth_picks") or [])[:20]:
        ticker = r.get("ticker", "")
        if "." in ticker:
            continue
        out["buzz_stealth_us_only"].append({
            "ticker":        ticker,
            "score":         r.get("score"),
            "velocity":      r.get("composite_velocity"),
            "price_7d_pct":  r.get("price_perf_7d_pct"),
            "reddit_interp": (r.get("reddit_velocity") or {}).get("interpretation"),
            "news_interp":   (r.get("news_velocity") or {}).get("interpretation"),
            "thesis":        r.get("thesis"),
        })
        if len(out["buzz_stealth_us_only"]) >= 10:
            break
    
    # ── 4. Future intelligence highlights (US only)
    out["future_intel"] = {
        "n_scored":         fi.get("n_scored"),
        "generated_at":     fi.get("generated_at"),
        "top_10_overall":   [],
        "multi_signal":     [],
        "locked_future":    [],
        "next_up_rotation": [],
        "stealth_buzz":     [],
    }
    
    def clean(r):
        if "." in r.get("ticker", ""):
            return None
        return {
            "ticker":      r["ticker"],
            "score":       r["future_intel_score"],
            "n_signals":   r.get("n_independent_signals"),
            "subscores":   r.get("subscores"),
            "thesis":      r.get("thesis"),
        }
    
    for r in (fi.get("top_25") or [])[:25]:
        c = clean(r)
        if c:
            out["future_intel"]["top_10_overall"].append(c)
        if len(out["future_intel"]["top_10_overall"]) >= 10:
            break
    
    highlights = fi.get("highlights") or {}
    for src, dst in (("multi_signal", "multi_signal"),
                       ("locked_future_value", "locked_future"),
                       ("next_up_rotation", "next_up_rotation"),
                       ("stealth_buzz", "stealth_buzz")):
        for r in (highlights.get(src) or [])[:15]:
            c = clean(r)
            if c:
                out["future_intel"][dst].append(c)
            if len(out["future_intel"][dst]) >= 8:
                break
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
