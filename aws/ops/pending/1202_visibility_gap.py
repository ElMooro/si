"""1202 — Why MRVL was caught but invisible to the user.

Hypothesis: MRVL was at ULTRA tier with 5 engines, alerted at 2026-06-01
19:00 UTC, but USER didn't see it because:
  1. UI sorts by n_engines or convergence_score — MRVL (5 eng) buried by AVGO (13 eng), AMD (8), NVDA (10)
  2. Pump category was PUMP_POSSIBLE (yellow), not the more visible PUMP_PRIMED (red)
  3. No standalone alert (was alerted in state but no Telegram push)
  4. is_ultra_new=False so no "NEW ULTRA" callout

This ops:
  1. Show ALL current ULTRA-tier tickers sorted by convergence_score so we can
     see where MRVL falls in the list
  2. Show pre-pump-radar.html filtering/sorting logic
  3. Examine the recent alert delivery pipeline
  4. Find what the user's view would look like with current data
"""
import json
import re
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1202_visibility_gap.json"
BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=120, retries={"max_attempts": 1})
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}


def read_safe(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        return {"_error": str(e)[:200]}


# 1. All ULTRA-tier tickers sorted as user would see them
print("[1202] 1. ALL ULTRA tickers — what the user sees on pre-pump-radar.html")
rad = read_safe("data/convergence-radar.json")
if not rad.get("_error"):
    items = rad.get("items") or rad.get("tickers") or rad.get("results") or []
    if isinstance(items, list):
        ultra = [i for i in items if i.get("tier") == "ULTRA"]
        # Sort by convergence_score desc (most likely UI default)
        ultra.sort(key=lambda x: -(x.get("convergence_score") or 0))
        print(f"\n  Total ULTRA tickers: {len(ultra)}")
        print(f"  {'RANK':>5} {'TICKER':<8} {'TIER':<8} {'CAT':<15} {'CONV':>6} {'N_ENG':>6} {'PRIOR':>6} {'NEW_HIGH':>9} {'ACCEL':>6} {'ULTRA_NEW':>9}")
        print(f"  {'─'*5} {'─'*8} {'─'*8} {'─'*15} {'─'*6} {'─'*6} {'─'*6} {'─'*9} {'─'*6} {'─'*9}")
        for rank, i in enumerate(ultra[:30], 1):
            marker = ""
            if i.get("ticker") in ["MRVL", "AVGO"]:
                marker = "  ← " + i.get("ticker")
            print(f"  {rank:>5} {i.get('ticker','?'):<8} {i.get('tier','?'):<8} "
                  f"{(i.get('pump_category') or '?'):<15} "
                  f"{(i.get('convergence_score') or 0):>6.1f} "
                  f"{(i.get('n_engines') or 0):>6} "
                  f"{(i.get('prior_n_engines') or 0):>6} "
                  f"{str(i.get('is_new_high')):>9} "
                  f"{str(i.get('is_accelerating')):>6} "
                  f"{str(i.get('is_ultra_new')):>9}{marker}")

        out["all_ultra"] = ultra[:30]

        # Find MRVL rank
        for rank, i in enumerate(ultra, 1):
            if i.get("ticker") == "MRVL":
                out["mrvl_rank_among_ultra"] = rank
                print(f"\n  📌 MRVL ranks #{rank} of {len(ultra)} ULTRA tickers")
                break


# 2. Pre-pump-radar.html sorting/filtering logic
print(f"\n\n[1202] 2. pre-pump-radar.html filtering logic")
try:
    with open("/root/work/si/pre-pump-radar.html") as f:
        html = f.read()
    # Find any sort/filter logic
    sort_matches = re.findall(r"\.sort\([^)]+\)", html)[:10]
    filter_matches = re.findall(r"\.filter\([^)]+\)", html)[:10]
    out["html_sort_logic"] = sort_matches
    out["html_filter_logic"] = filter_matches
    print(f"\n  Sort patterns found ({len(sort_matches)}):")
    for s in sort_matches[:8]:
        print(f"    {s[:200]}")
    print(f"\n  Filter patterns found ({len(filter_matches)}):")
    for f in filter_matches[:8]:
        print(f"    {f[:200]}")

    # Check the rendering logic for category cutoffs
    if "PUMP_PRIMED" in html:
        idx = html.find("PUMP_PRIMED")
        ctx = html[max(0,idx-300):idx+300]
        out["html_pump_primed_context"] = ctx
        print(f"\n  PUMP_PRIMED render context: {ctx[:400]}")
except Exception as e:
    print(f"  ❌ {e}")


# 3. Find the Lambda that writes radar + check schedule
print(f"\n\n[1202] 3. Check who writes convergence-radar.json")
# Recent generation time
head = s3.head_object(Bucket=BUCKET, Key="data/convergence-radar.json")
print(f"  Last modified: {head['LastModified'].isoformat()}")
# Generated_at from content
if not rad.get("_error"):
    print(f"  generated_at in doc: {rad.get('generated_at')}")
    print(f"  total items in doc: {len(rad.get('items', []) or rad.get('tickers', []) or rad.get('results', []) or [])}")
    out["radar_meta"] = {
        "last_modified": head['LastModified'].isoformat(),
        "generated_at": rad.get('generated_at'),
        "n_items": len(rad.get('items', []) or rad.get('tickers', []) or rad.get('results', []) or []),
    }


# 4. Check alert delivery (anomaly-detector alert history)
print(f"\n\n[1202] 4. Alert history check — was MRVL ever in alert history?")
for k in ["data/_alerts/alert-history.json",
           "data/_alerts/convergence-radar-alerted.json",
           "alert-history.json"]:
    doc = read_safe(k)
    if not doc.get("_error"):
        text = json.dumps(doc, default=str)
        if "MRVL" in text:
            # Find context
            idx = text.find("MRVL")
            ctx = text[max(0,idx-100):idx+200]
            out.setdefault("alert_history", {})[k] = {
                "modified": s3.head_object(Bucket=BUCKET, Key=k)["LastModified"].isoformat() if k.startswith("data/") else None,
                "mrvl_ctx": ctx,
            }
            print(f"  ✓ {k}: MRVL found")
            print(f"    {ctx[:300]}")


# 5. Show MRVL's engines in detail — what specific signals fired
print(f"\n\n[1202] 5. MRVL engines breakdown (what specifically detected the pump)")
if not rad.get("_error"):
    items = rad.get("items") or rad.get("tickers") or rad.get("results") or []
    for item in items if isinstance(items, list) else []:
        if item.get("ticker") == "MRVL":
            out["mrvl_full"] = item
            engines = item.get("engines") or []
            print(f"\n  MRVL has {len(engines)} engines flagging:")
            for e in engines:
                if isinstance(e, dict):
                    print(f"    • {e.get('name','?'):35s} signal={e.get('signal_strength','?')}  dir={e.get('direction','?')}  {e.get('detail','')[:90]}")
                else:
                    print(f"    • {e}")
            # bullish/bearish lists
            bull = item.get("bullish_engines") or []
            bear = item.get("bearish_engines") or []
            print(f"\n  Bullish engines ({len(bull)}): {bull}")
            print(f"  Bearish engines ({len(bear)}): {bear}")
            # pump components
            pc = item.get("pump_components") or {}
            if pc:
                print(f"\n  Pump components: {json.dumps(pc, default=str, indent=2)[:600]}")
            break


out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1202] DONE")
