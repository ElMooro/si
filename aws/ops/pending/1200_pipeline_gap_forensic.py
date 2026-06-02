"""1200 — Deep-dive forensic: convergence radar alerted MRVL on 2026-06-01
        at 19:00 UTC but no digest picked it up. Where did the pipeline break?

Findings so far:
  - convergence-radar-alerted.json: MRVL timestamp 2026-06-01T19:00:12 UTC
  - digest-2026-06-01.json (20:01 UTC) & -close.json (21:00 UTC): 0 MRVL mentions
  - digest-2026-06-02.json (today, 16:01 UTC): 0 MRVL mentions (also missed!)

This ops:
  1. Read FULL digest content for June 01 + June 02 to see what tickers it DID surface
  2. Read convergence-radar-state.json — does it have MRVL with a timestamp?
  3. Find the Lambda that builds convergence-radar.json
  4. Find the Lambda that builds digest files (might be different)
  5. Check pump-positioning.json + brief content for MRVL
  6. Check if there's historical convergence-radar.json archived anywhere
"""
import json
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1200_pipeline_gap_forensic.json"
BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=120, retries={"max_attempts": 1})
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}


def read_safe(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        return {"_error": str(e)[:200]}


# Step 1: Full digest contents for June 1 + June 2
print("[1200] 1. Full digest contents")
for date in ["2026-06-01", "2026-06-02"]:
    for stage in ["", "-close"]:
        key = f"data/_alerts/digest-{date}{stage}.json"
        doc = read_safe(key)
        if not doc.get("_error"):
            out[f"digest_{date}{stage}"] = doc
            print(f"\n  📄 {key}:")
            # Show structure
            if isinstance(doc, dict):
                print(f"     keys: {list(doc.keys())[:30]}")
                # Show each section's content compactly
                for k, v in doc.items():
                    if isinstance(v, str):
                        print(f"     {k}: {v[:300]}")
                    elif isinstance(v, list):
                        print(f"     {k}: [{len(v)} items] {json.dumps(v[:3], default=str)[:300]}")
                    elif isinstance(v, dict):
                        print(f"     {k}: keys={list(v.keys())[:10]}")
                        # Look for tickers in dict values
                        for subk, subv in v.items():
                            if isinstance(subv, list):
                                print(f"       └─ {subk}: [{len(subv)}] {json.dumps(subv[:5], default=str)[:200]}")


# Step 2: convergence-radar-state.json
print(f"\n\n[1200] 2. convergence-radar-state.json full content")
state = read_safe("data/_alerts/convergence-radar-state.json")
if not state.get("_error"):
    out["radar_state"] = state
    s = json.dumps(state, default=str)
    print(f"  Size: {len(s)/1024:.1f} KB · keys: {list(state.keys())[:30] if isinstance(state, dict) else '?'}")
    if isinstance(state, dict):
        # Look for MRVL specifically
        for k, v in state.items():
            if k == "MRVL" or (isinstance(v, dict) and "MRVL" in str(v)):
                print(f"  📍 {k}: {json.dumps(v, default=str)[:400]}")
            elif k in ["AVGO", "NVDA", "QCOM", "TXN", "AMD"]:
                print(f"  📍 {k}: {json.dumps(v, default=str)[:300]}")
        # Print all if structure is ticker-keyed
        all_keys = list(state.keys())
        if all(len(k) <= 6 and k.isupper() for k in all_keys[:10]):
            print(f"\n  Structure is ticker-keyed, showing MRVL + AVGO + AMD + NVDA:")
            for t in ["MRVL", "AVGO", "AMD", "NVDA", "QCOM", "TXN"]:
                if t in state:
                    print(f"\n  📌 {t}:")
                    print(f"     {json.dumps(state[t], default=str)[:600]}")


# Step 3: Find Lambdas producing these files
print(f"\n\n[1200] 3. Search Lambdas for convergence-radar producers")
out["lambdas_writing_files"] = {}
try:
    funcs = lam.list_functions()
    convergence_lambdas = []
    digest_lambdas = []
    for f in funcs.get("Functions", []):
        name = f["FunctionName"]
        # Heuristic: name contains "convergence", "radar", "pump", "digest", "alert"
        if any(x in name.lower() for x in ["convergence", "radar", "pump", "digest", "alert"]):
            convergence_lambdas.append(name)
    out["lambdas_writing_files"]["convergence"] = convergence_lambdas
    print(f"  Convergence/radar/digest Lambdas: {convergence_lambdas}")
except Exception as e:
    print(f"  ❌ {e}")


# Step 4: pump-positioning content (had 3 AVGO mentions)
print(f"\n\n[1200] 4. pump-positioning.json full content")
pos = read_safe("data/pump-positioning.json")
if not pos.get("_error"):
    out["pump_positioning_full"] = pos
    # Find ticker entries
    items = pos.get("items") or pos.get("tickers") or pos.get("positions") or []
    if isinstance(items, list):
        # Show all entries with AVGO/MRVL
        for i in items:
            t = i.get("ticker") or i.get("symbol")
            if t in ["MRVL", "AVGO", "NVDA", "QCOM", "TXN", "AMD"]:
                print(f"\n  📌 {t}: {json.dumps(i, default=str)[:600]}")
    # Top-level keys
    if isinstance(pos, dict):
        print(f"\n  Top keys: {list(pos.keys())[:25]}")
        # Look for items/list lists at top
        for k, v in pos.items():
            if isinstance(v, list) and len(v) > 0 and len(v) < 200:
                # show what tickers are in it
                tickers_in_list = []
                for item in v[:50]:
                    t = (item.get("ticker") if isinstance(item, dict) else None)
                    if t:
                        tickers_in_list.append(t)
                if tickers_in_list:
                    tickers_str = ", ".join(tickers_in_list[:30])
                    mrvl_present = "MRVL" in tickers_str
                    avgo_present = "AVGO" in tickers_str
                    print(f"  [{k}] {len(v)} items — MRVL? {mrvl_present} AVGO? {avgo_present} — first 15: {tickers_str[:300]}")


# Step 5: Check pump-radar-brief
print(f"\n\n[1200] 5. pump-radar-brief.json key surfaces")
brief = read_safe("data/pump-radar-brief.json")
if not brief.get("_error"):
    out["radar_brief_summary"] = {
        "top_keys": list(brief.keys()) if isinstance(brief, dict) else None,
        "has_mrvl": "MRVL" in json.dumps(brief, default=str),
        "has_avgo": "AVGO" in json.dumps(brief, default=str),
    }
    # Show narrative & key fields
    for k in ["narrative", "summary", "whats_changed_narrative", "pump_candidates", "new_signals", "removed_signals", "conviction_grade"]:
        if k in brief:
            v = brief[k]
            if isinstance(v, list):
                print(f"  {k}: {json.dumps(v[:5], default=str)[:300]}")
            else:
                print(f"  {k}: {str(v)[:400]}")


# Step 6: Compare CURRENT convergence-radar entries (MRVL + AVGO) to see if MRVL was just added today
print(f"\n\n[1200] 6. CURRENT convergence-radar entries for MRVL + AVGO")
rad = read_safe("data/convergence-radar.json")
if not rad.get("_error"):
    items = rad.get("items") or rad.get("tickers") or rad.get("results") or []
    if isinstance(items, list):
        for i in items:
            t = i.get("ticker")
            if t in ["MRVL", "AVGO"]:
                out.setdefault("current_radar_full", {})[t] = i
                print(f"\n  📌 {t}:")
                print(f"     {json.dumps(i, default=str, indent=2)[:1500]}")


out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1200] DONE")
