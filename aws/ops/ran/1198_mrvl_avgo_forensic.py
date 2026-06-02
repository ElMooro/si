"""1198 — FORENSIC INVESTIGATION: MRVL 29% pump + AVGO 5% pump

The user reports:
  - MRVL pumped 29% TODAY (2026-06-02)
  - AVGO pumped 5% TODAY
  - AVGO was on pre-pump-radar YESTERDAY (caught it)
  - MRVL only appeared TODAY (too late)

Question: did our system have signals on MRVL DAYS BEFORE today's pump?

Investigation plan:
  1. List ALL historical S3 archives we have under various prefixes
  2. For each historical snapshot, check if MRVL / AVGO appeared
  3. Check across ALL data sources:
     - data/convergence-radar.json (pre-pump-radar feed)
     - data/pump-positioning.json
     - data/momentum-leaders.json
     - data/velocity-acceleration.json
     - data/radar-backtest.json
     - etf-flows/history/{date}.json
     - any history archives
  4. Build a timeline showing when each ticker first appeared
"""
import json
import os
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1198_mrvl_avgo_forensic.json"
BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=120, retries={"max_attempts": 1})
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

TICKERS_OF_INTEREST = ["MRVL", "AVGO", "NVDA", "QCOM", "TXN", "AMD"]  # add semis for context

out = {
    "started": datetime.now(timezone.utc).isoformat(),
    "question": "When did MRVL + AVGO first show signals in our system?",
    "today_actual_pumps": {
        "MRVL": "+29%",
        "AVGO": "+5%",
    },
}


def list_keys_under(prefix, max_keys=2000):
    """List S3 keys under a prefix."""
    keys = []
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in (page.get("Contents") or []):
            keys.append({
                "key": obj["Key"],
                "size": obj["Size"],
                "modified": obj["LastModified"].isoformat(),
            })
            if len(keys) >= max_keys:
                return keys
    return keys


def read_json_safe(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        return {"_error": str(e)[:200]}


# Step 1: Discover all data archive locations
print("[1198] 1. Discover S3 history archive locations")
prefixes_to_scan = [
    "etf-flows/history/",       # ETF flow history (we know this exists)
    "etf-flows/ai-history/",    # AI analysis history
    "data/",                    # All the convergence radar data
    "macro/history/",           # Phase 2 macro
    "anomalies/history/",       # Anomaly history
    "flow-anomalies/history/",  # New flow anomaly history
    "pump-radar-history/",      # If exists
    "convergence-history/",     # If exists
    "radar-history/",           # If exists
    "history/",                 # Generic
]

archive_inventory = {}
for prefix in prefixes_to_scan:
    keys = list_keys_under(prefix, max_keys=200)
    if keys:
        archive_inventory[prefix] = {
            "n_keys": len(keys),
            "sample_keys": [k["key"] for k in keys[:5]],
            "newest": max(keys, key=lambda x: x["modified"])["modified"] if keys else None,
            "oldest": min(keys, key=lambda x: x["modified"])["modified"] if keys else None,
        }
        print(f"  ✓ {prefix:30s} {len(keys):4d} files (newest: {archive_inventory[prefix]['newest'][:16]})")
    else:
        archive_inventory[prefix] = {"n_keys": 0}

out["archive_inventory"] = archive_inventory


# Step 2: Look at CURRENT main data files to see MRVL/AVGO mentions
print(f"\n[1198] 2. Search CURRENT main data files for {TICKERS_OF_INTEREST}")
main_files = [
    "data/convergence-radar.json",
    "data/pump-positioning.json",
    "data/momentum-leaders.json",
    "data/velocity-acceleration.json",
    "data/pump-mechanics.json",
    "data/portfolio-analytics.json",
    "data/themes.json",
    "data/catalysts.json",
    "data/catalyst-clusters.json",
    "etf-flows/daily.json",
    "etf-flows/constituent-pressure.json",
    "etf-flows/stock-exposure-lookup.json",
    "etf-flows/ai-analysis.json",
]

current_signals = {}
for f in main_files:
    doc = read_json_safe(f)
    if doc.get("_error"):
        current_signals[f] = {"error": doc["_error"]}
        continue
    text = json.dumps(doc, default=str)
    mentions = {t: text.count(t) for t in TICKERS_OF_INTEREST if t in text}
    if mentions:
        # Get last modified
        try:
            head = s3.head_object(Bucket=BUCKET, Key=f)
            modified = head["LastModified"].isoformat()
        except Exception:
            modified = None
        current_signals[f] = {
            "modified": modified,
            "size_kb": round(len(text) / 1024, 1),
            "mentions": mentions,
        }
        print(f"  ✓ {f}  mentions: {mentions}  (modified {modified[:16] if modified else '?'})")

out["current_signals"] = current_signals


# Step 3: Deep dive into convergence-radar.json (main pre-pump feed)
print(f"\n[1198] 3. Deep dive into CURRENT convergence-radar.json")
radar = read_json_safe("data/convergence-radar.json")
if not radar.get("_error"):
    # Try multiple shapes (may have items, tickers, results, signals)
    items = radar.get("items") or radar.get("tickers") or radar.get("results") or radar.get("signals") or []
    if isinstance(items, list):
        relevant = [i for i in items if any(t == i.get("ticker") or t == i.get("symbol") for t in TICKERS_OF_INTEREST)]
        out["current_radar_relevant"] = relevant
        for r in relevant:
            t = r.get("ticker") or r.get("symbol")
            print(f"  {t}: tier={r.get('tier')} pump_score={r.get('pump_score')} category={r.get('pump_category')}")
            print(f"    keys present: {list(r.keys())[:15]}")
    else:
        out["current_radar_shape"] = {"top_keys": list(radar.keys())[:30]}


# Step 4: Walk through ETF flow history for signals on holding ETFs
print(f"\n[1198] 4. ETF flow history — track SMH/SOXX/MTUM/QQQ/XLK z-scores")
# These are the key ETFs that contain MRVL/AVGO. If their z-scores were high
# in days leading up to today, our constituent system WOULD have flagged
# MRVL had it existed yet.
holding_etfs_to_track = ["SMH", "SOXX", "MTUM", "QQQ", "XLK", "SPY", "IVV", "VOO"]
flow_history_keys = sorted([k["key"] for k in list_keys_under("etf-flows/history/", max_keys=200)], reverse=True)
print(f"  Found {len(flow_history_keys)} flow history archives")

etf_timeline = {}
for k in flow_history_keys[:30]:  # last 30 days
    date = k.split("/")[-1].replace(".json", "")
    doc = read_json_safe(k)
    if doc.get("_error"):
        continue
    metrics = doc.get("metrics", [])
    for m in metrics:
        t = m.get("ticker")
        if t in holding_etfs_to_track:
            etf_timeline.setdefault(t, {})[date] = {
                "z": m.get("flow_zscore_90d"),
                "5d": (m.get("flow_5d_usd") or 0) / 1e6,
                "21d": (m.get("flow_21d_usd") or 0) / 1e6,
                "label": m.get("signal_label"),
                "persistence": m.get("persistence_days"),
            }

out["etf_timeline"] = etf_timeline
for t, days in sorted(etf_timeline.items()):
    print(f"\n  {t}:")
    for date in sorted(days.keys(), reverse=True)[:10]:
        d = days[date]
        if d['z'] is not None:
            print(f"    {date}  z={d['z']:>+6.2f}σ  5d=${d['5d']:>+8.0f}M  21d=${d['21d']:>+8.0f}M  {d['label']:14s} persist={d['persistence']}d")


# Step 5: Check pump-positioning + momentum-leaders for historical signals
print(f"\n[1198] 5. Check pump-positioning + momentum-leaders for MRVL/AVGO")
for key_pattern in ["data/pump-positioning.json", "data/momentum-leaders.json",
                     "data/velocity-acceleration.json", "data/pump-radar-brief.json"]:
    doc = read_json_safe(key_pattern)
    if doc.get("_error"):
        continue
    text = json.dumps(doc, default=str)
    for t in TICKERS_OF_INTEREST:
        if t in text:
            # Find context around the ticker
            idx = text.find(t)
            ctx_start = max(0, idx - 80)
            ctx_end = min(len(text), idx + 200)
            ctx = text[ctx_start:ctx_end].replace("\n", " ")
            out.setdefault("ticker_context", {}).setdefault(key_pattern, {})[t] = ctx
            print(f"  {key_pattern}  [{t}]: {ctx[:200]}")


# Step 6: Are there other Lambda outputs we should check?
# Look at the data/* directory comprehensively
print(f"\n[1198] 6. All data/* files inventory")
data_files = list_keys_under("data/", max_keys=200)
out["data_files_inventory"] = [
    {"key": d["key"], "size_kb": round(d["size"] / 1024, 1), "modified": d["modified"]}
    for d in data_files
]
for d in data_files[:50]:
    print(f"  {d['key']:50s} {round(d['size']/1024,1):>8.1f} KB  {d['modified'][:16]}")


out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1198] DONE — report at {REPORT}")
