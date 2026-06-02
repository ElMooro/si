"""1204 — Verify the new 3-tier velocity detection and surface tomorrow's
        potential pump candidates.

After the EMERGING/WATCH tier expansion, re-invoke the Lambda and check:
  1. Are EMERGING (45-59) and WATCH (30-44) candidates surfaced?
  2. Are any of today's pumpers (MRVL/AVGO) listed in EMERGING/WATCH
     proving the new logic captures pre-pump candidates?
  3. What NAMES are in EMERGING tier right now — these are tomorrow's
     potential pumps with the new detection logic
"""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1204_emerging_tier_verify.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-velocity-acceleration"

cfg = Config(read_timeout=420, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}


# Step 1: Sync invoke Lambda
print(f"[1204] 1. Sync invoke {LAMBDA} with new EMERGING/WATCH tiers")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["invoke"] = {
        "elapsed_s": elapsed,
        "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "body": payload[:600],
    }
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:500]}")
except Exception as e:
    out["invoke"] = {"error": str(e)[:300]}


# Step 2: Read full velocity-acceleration.json output
print(f"\n[1204] 2. Read data/velocity-acceleration.json output")
try:
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/velocity-acceleration.json")["Body"].read())
    out["doc_summary"] = {
        "schema_version": doc.get("schema_version"),
        "generated_at": doc.get("generated_at"),
        "trading_date": doc.get("trading_date"),
        "universe_size": doc.get("universe_size"),
        "n_fired": doc.get("n_fired"),
        "n_emerging": doc.get("n_emerging"),
        "n_watch": doc.get("n_watch"),
        "n_confirmed_today": doc.get("n_confirmed_today"),
        "n_fresh": doc.get("n_fresh"),
        "config": doc.get("config"),
    }
    print(f"  schema: {doc.get('schema_version')}  trading_date: {doc.get('trading_date')}")
    print(f"  universe: {doc.get('universe_size')}  fired(>=60): {doc.get('n_fired')}")
    print(f"  emerging(45-59): {doc.get('n_emerging')}  watch(30-44): {doc.get('n_watch')}")

    # Show CONFIRMED (still in old logic)
    confirmed = doc.get("confirmed_today") or []
    print(f"\n  ── ✅ CONFIRMED TODAY ({len(confirmed)}) ──")
    for c in confirmed:
        print(f"    {c.get('ticker'):6s}  composite={c.get('current_score'):>5.1f}  theme={c.get('theme_label','?'):20s}  tier={c.get('tier_label','?')}")
    out["confirmed_today"] = confirmed

    # FRESH
    fresh = doc.get("fresh_fires") or []
    print(f"\n  ── 🆕 FRESH FIRES ({len(fresh)}) ──")
    for f in fresh:
        print(f"    {f.get('ticker'):6s}  composite={f.get('current_score'):>5.1f}")
    out["fresh_fires"] = fresh

    # EMERGING (NEW)
    emerging = doc.get("emerging") or []
    print(f"\n  ── ⚡ EMERGING (45-59 composite, 1-3 days before fire) ──")
    print(f"  These are tomorrow's potential MRVL/AVGO candidates:")
    print(f"  {'TICKER':<8s} {'CS':>5s} {'SLP':>4s} {'ACC':>4s} {'FLR':>4s} {'VOL':>5s}  THEME")
    print(f"  {'-'*8} {'-'*5} {'-'*4} {'-'*4} {'-'*4} {'-'*5}  {'-'*30}")
    for e in emerging[:25]:
        print(f"  {e.get('ticker','?'):<8s} {e.get('composite_score',0):>5.1f} "
              f"{e.get('slope_score',0):>4.0f} {e.get('accum_score',0):>4.0f} "
              f"{e.get('floor_score',0):>4.0f} "
              f"{e.get('current_vol_ratio',0):>4.2f}x  {e.get('theme_label','?')}")
    out["emerging"] = emerging

    # WATCH (NEW)
    watch = doc.get("watch") or []
    print(f"\n  ── 👁️ WATCH (30-44 composite, very early) ──")
    print(f"  {'TICKER':<8s} {'CS':>5s} {'SLP':>4s} {'ACC':>4s} {'FLR':>4s} {'VOL':>5s}  THEME")
    for w in watch[:20]:
        print(f"  {w.get('ticker','?'):<8s} {w.get('composite_score',0):>5.1f} "
              f"{w.get('slope_score',0):>4.0f} {w.get('accum_score',0):>4.0f} "
              f"{w.get('floor_score',0):>4.0f} "
              f"{w.get('current_vol_ratio',0):>4.2f}x  {w.get('theme_label','?')}")
    out["watch"] = watch

    # Is MRVL or AVGO in any tier?
    print(f"\n  ── 🔍 SEARCH FOR MRVL + AVGO IN NEW TIERS ──")
    for ticker_check in ["MRVL", "AVGO", "NVDA", "PLTR", "AMD", "MU", "ORCL"]:
        for tier_name, tier in [("CONFIRMED", confirmed), ("FRESH", fresh),
                                  ("EMERGING", emerging), ("WATCH", watch)]:
            for item in tier:
                if item.get("ticker") == ticker_check:
                    score = item.get("composite_score") or item.get("current_score")
                    print(f"    ✓ {ticker_check} is in {tier_name} with composite={score}")
                    out.setdefault("ticker_locations", {})[ticker_check] = {
                        "tier": tier_name,
                        "composite_score": score,
                        "theme": item.get("theme_label"),
                    }
                    break
except Exception as e:
    out["doc_summary"] = {"error": str(e)[:300]}


out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1204] DONE")
