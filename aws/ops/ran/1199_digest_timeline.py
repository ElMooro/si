"""1199 — Trace MRVL/AVGO appearances through date-stamped archives.

Found in 1198:
  - data/_alerts/digest-{date}.json + digest-{date}-close.json exist back to 2026-05-30
  - data/ has 200 files (overwritten daily, not date-stamped)
  - history/ has 200 files (May 6-7 only)

This ops:
  1. List ALL date-stamped digest files
  2. For each, search for MRVL + AVGO + their context
  3. Show the timeline of when each ticker first appeared
  4. Show what signals were present on each day
"""
import json
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1199_digest_timeline.json"
BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=120, retries={"max_attempts": 1})
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}


def list_keys(prefix, max_n=500):
    pag = s3.get_paginator("list_objects_v2")
    keys = []
    for page in pag.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in (page.get("Contents") or []):
            keys.append({
                "key": obj["Key"],
                "size": obj["Size"],
                "modified": obj["LastModified"].isoformat(),
            })
            if len(keys) >= max_n:
                return keys
    return keys


def read_safe(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        return {"_error": str(e)[:150]}


# 1. List ALL date-stamped digest files
print("[1199] 1. Discover all date-stamped digest archives")
digest_keys = list_keys("data/_alerts/digest-", max_n=200)
digest_keys.sort(key=lambda x: x["key"])
for k in digest_keys:
    print(f"  {k['key']:50s} {round(k['size']/1024,1):>6.1f} KB  {k['modified'][:16]}")
out["digest_archives"] = digest_keys


# 2. Read each + search for our tickers
TARGETS = ["MRVL", "AVGO", "NVDA", "QCOM", "TXN", "AMD"]
print(f"\n[1199] 2. Search each digest for {TARGETS}")
timeline = {}
for k in digest_keys:
    key = k["key"]
    doc = read_safe(key)
    if doc.get("_error"):
        timeline[key] = {"error": doc["_error"]}
        continue
    text = json.dumps(doc, default=str)
    mentions = {t: text.count(t) for t in TARGETS if t in text}
    # Find context for each
    contexts = {}
    for t in TARGETS:
        if t in text:
            # Find first occurrence + grab 250 char context
            idx = text.find(t)
            start = max(0, idx - 100)
            end = min(len(text), idx + 250)
            contexts[t] = text[start:end].replace("\n", " ")
    timeline[key] = {
        "modified": k["modified"],
        "size_kb": round(k["size"] / 1024, 1),
        "mentions": mentions,
        "contexts": contexts,
        # Try to surface structured fields
        "top_keys": list(doc.keys())[:20] if isinstance(doc, dict) else None,
    }

out["timeline"] = timeline


# Print findings
print(f"\n[1199] TICKER TIMELINE ANALYSIS:")
print(f"\n  Date-by-date mentions of MRVL and AVGO:")
print(f"  {'DATE':12s} {'TYPE':8s} {'MRVL':>6s} {'AVGO':>6s} {'NVDA':>6s} {'QCOM':>6s} {'TXN':>6s} {'AMD':>6s}")
for key in sorted(timeline.keys()):
    info = timeline[key]
    if "error" in info:
        continue
    m = info.get("mentions") or {}
    filename = key.split("/")[-1].replace(".json", "")
    # Parse date from filename like "digest-2026-05-30-close" or "digest-2026-05-30"
    parts = filename.split("-")
    if len(parts) >= 4:
        date = f"{parts[1]}-{parts[2]}-{parts[3]}"
        typ = "CLOSE" if "close" in filename.lower() else "INTRADAY"
    elif "latest" in filename.lower():
        date = "LATEST"
        typ = ""
    elif "index" in filename.lower():
        date = "INDEX"
        typ = ""
    else:
        date = filename
        typ = ""
    print(f"  {date:12s} {typ:8s} {m.get('MRVL',0):>6d} {m.get('AVGO',0):>6d} {m.get('NVDA',0):>6d} {m.get('QCOM',0):>6d} {m.get('TXN',0):>6d} {m.get('AMD',0):>6d}")


# 3. For each digest that mentions MRVL, show full context
print(f"\n[1199] 3. FULL MRVL contexts across digests:")
mrvl_contexts = {}
for key, info in sorted(timeline.items()):
    if info.get("contexts", {}).get("MRVL"):
        date_part = key.split("/")[-1].replace(".json", "")
        mrvl_contexts[date_part] = info["contexts"]["MRVL"]
out["mrvl_contexts"] = mrvl_contexts
for d, ctx in mrvl_contexts.items():
    print(f"\n  📅 {d}:")
    print(f"      {ctx[:400]}")


# 4. For each digest that mentions AVGO, show full context  
print(f"\n[1199] 4. FULL AVGO contexts across digests:")
avgo_contexts = {}
for key, info in sorted(timeline.items()):
    if info.get("contexts", {}).get("AVGO"):
        date_part = key.split("/")[-1].replace(".json", "")
        avgo_contexts[date_part] = info["contexts"]["AVGO"]
out["avgo_contexts"] = avgo_contexts
for d, ctx in avgo_contexts.items():
    print(f"\n  📅 {d}:")
    print(f"      {ctx[:400]}")


# 5. Also check the history/ prefix and convergence radar state alerts
print(f"\n[1199] 5. Check history/ prefix + convergence-radar-state.json")
state = read_safe("data/_alerts/convergence-radar-state.json")
if not state.get("_error"):
    text = json.dumps(state, default=str)
    state_mentions = {t: text.count(t) for t in TARGETS if t in text}
    print(f"  convergence-radar-state.json mentions: {state_mentions}")
    # Show shape
    if isinstance(state, dict):
        print(f"  top-level keys: {list(state.keys())[:20]}")
        # Look for ticker-keyed structure
        for k, v in state.items():
            if isinstance(v, dict) and any(t in str(v) for t in ["MRVL", "AVGO"]):
                out.setdefault("state_structure", {})[k] = (
                    {"keys": list(v.keys())[:20]} if isinstance(v, dict) else str(v)[:200]
                )

# Also try alerted state
alerted = read_safe("data/_alerts/convergence-radar-alerted.json")
if not alerted.get("_error"):
    out["alerted_state"] = alerted


out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1199] DONE")
