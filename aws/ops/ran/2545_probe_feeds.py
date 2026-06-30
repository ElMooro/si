"""ops 2545 — probe candidate feeds for the risk-regime page upgrade.
Dump existence + top-level keys + a compact sample of headline/regime/score-ish
fields so the page renders REAL field names, not assumptions."""
import boto3, json
s3 = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"

CANDIDATES = [
    "data/cross-asset-flow-state.json", "data/cross-asset-regime.json",
    "data/dollar-radar.json", "data/gold-equity-rotation.json",
    "data/capital-inflows.json", "data/tic-flows.json",
    "data/eurodollar-stress.json", "data/eurodollar-plumbing.json",
    "data/vol-regime.json", "data/crisis-composite.json", "data/global-stress.json",
    "data/cycle-clock.json", "data/dark-pool.json", "data/net-liquidity.json",
    "data/fed-liquidity.json", "data/regime-map.json", "data/sahm.json",
    "data/settlement-fails.json", "data/sovereign-fiscal.json",
]

def rd(k):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())
    except Exception as e:
        return {"_MISSING": str(e)[:50]}

def compact(d, depth=0):
    """one-line preview of a dict's scalar fields + nested keys"""
    if not isinstance(d, dict):
        return str(d)[:80]
    out = []
    for k, v in list(d.items())[:14]:
        if isinstance(v, (int, float, str, bool)) or v is None:
            sv = str(v)
            if len(sv) > 38:
                sv = sv[:38] + "…"
            out.append(f"{k}={sv}")
        elif isinstance(v, list):
            out.append(f"{k}=[{len(v)}]")
        elif isinstance(v, dict):
            out.append(f"{k}={{{','.join(list(v)[:5])}}}")
    return " · ".join(out)

for key in CANDIDATES:
    d = rd(key)
    if "_MISSING" in d:
        print(f"✗ {key}  ({d['_MISSING']})")
        continue
    print(f"\n✓ {key}")
    print("   topkeys:", list(d)[:18])
    print("   sample :", compact(d)[:300])
    # show one representative nested list/dict shape
    for k, v in d.items():
        if isinstance(v, list) and v and isinstance(v[0], dict):
            print(f"   {k}[0]:", compact(v[0])[:200]); break
print("\nDONE 2545")
