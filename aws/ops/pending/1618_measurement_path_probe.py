"""Probe the shortest path to cross-sectional factor IC:
1) is signal-backtest accruing real-n forward returns?
2) do daily snapshots carry per-ticker FACTOR scores?
3) does alpha-score.json carry per-ticker components (panel source)?"""
import json, boto3
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"

def load(k):
    try: return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())
    except Exception as e: return {"_err": str(e)[:120]}

# 1) signal-backtest maturity
sb = load("data/signal-backtest.json")
print("=== signal-backtest.json ===")
print("maturity:", sb.get("maturity"), "| n_observations:", sb.get("n_observations"))
print("keys:", list(sb.keys())[:12])

# 2) latest opportunity snapshot — what fields per ticker?
print("\n=== daily snapshots ===")
r = s3.list_objects_v2(Bucket=B, Prefix="data/track-record/snapshots/", MaxKeys=400)
keys = sorted([o["Key"] for o in r.get("Contents", [])])
print(f"snapshot count: {len(keys)}; span: {keys[0].split('/')[-1] if keys else '-'} .. {keys[-1].split('/')[-1] if keys else '-'}")
if keys:
    snap = load(keys[-1])
    # find the per-ticker list
    rows = snap if isinstance(snap, list) else (snap.get("items") or snap.get("opportunities") or snap.get("rows") or snap.get("data") or [])
    if isinstance(rows, dict): rows = list(rows.values())
    print("latest snapshot ticker-row fields:", list(rows[0].keys())[:25] if rows and isinstance(rows[0], dict) else type(rows))
    if rows and isinstance(rows[0], dict):
        carries_factors = any(k.lower() in ("components","factors","alpha","quality","growth","momentum","value","valuation") for k in rows[0].keys())
        print("carries per-ticker FACTOR scores?:", carries_factors)

# 3) alpha-score.json per-ticker components
print("\n=== alpha-score.json ===")
a = load("screener/alpha-score.json")
arows = a.get("stocks") or a.get("rows") or a.get("results") or (a if isinstance(a, list) else [])
print("alpha-score keys:", list(a.keys())[:12] if isinstance(a, dict) else "list")
if arows and isinstance(arows[0], dict):
    print("per-ticker fields:", list(arows[0].keys())[:20])
    print("has 'components'?:", "components" in arows[0])
