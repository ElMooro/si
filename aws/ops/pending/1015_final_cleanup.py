#!/usr/bin/env python3
"""Step 1015 — Probe opportunities.json structure + samples for 13 remaining unknowns."""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1015_final_cleanup.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)


def grab(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        return {"err": str(e)[:200]}


def peek_structure(obj, depth=2, max_keys=20):
    """Return a structural view of a dict/list."""
    if isinstance(obj, dict):
        out = {}
        for k in list(obj.keys())[:max_keys]:
            v = obj[k]
            if isinstance(v, list):
                if v and isinstance(v[0], dict):
                    out[k] = f"<list[{len(v)}] of dicts with keys: {list(v[0].keys())[:10]}>"
                    if depth > 0:
                        out[f"_{k}[0]"] = {kk: vv for kk, vv in list(v[0].items())[:8] if not isinstance(vv, (list, dict))}
                else:
                    out[k] = f"<list[{len(v)}]>"
            elif isinstance(v, dict):
                out[k] = f"<dict with {len(v)} keys: {list(v.keys())[:8]}>"
            else:
                out[k] = repr(v)[:80]
        return out
    elif isinstance(obj, list):
        return [peek_structure(o, depth-1, max_keys) for o in obj[:3]]
    else:
        return repr(obj)[:80]


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # opportunities.json — find the ranked-items array
    op = grab("data/opportunities.json")
    if "err" not in op:
        out["opportunities_top_keys"] = list(op.keys())
        out["opportunities_structure"] = peek_structure(op, depth=1, max_keys=20)
        # Find the FIRST array of dicts
        for k, v in op.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                out["opportunities_first_array"] = {
                    "key": k,
                    "len": len(v),
                    "first_item_keys": list(v[0].keys()),
                    "first_item_sample": {kk: vv for kk, vv in list(v[0].items())[:15]
                                           if not isinstance(vv, (list, dict))},
                }
                break
    else:
        out["opportunities_err"] = op
    
    # near-misses-by-signal.json — current state
    nm = grab("data/near-misses-by-signal.json")
    if "err" not in nm:
        out["near_misses_current"] = nm.get("near_misses_by_signal")
        out["near_misses_diag"] = nm.get("diagnostics", [])[:6]
    
    # remaining unknown signal types
    em = grab("data/engine-signal-map.json")
    if "err" not in em:
        out["unknowns_remaining"] = em.get("unknown_signal_types", [])
        out["new_id_prefixes"] = em.get("new_id_prefixes", [])
    
    # crisis-composite.json — check structure for z-score field
    cc = grab("data/crisis-composite.json")
    if "err" not in cc and isinstance(cc, dict):
        out["crisis_composite_structure"] = {
            k: type(v).__name__ + (f" ({len(v)} items)" if isinstance(v, (list, dict)) else f" = {repr(v)[:60]}")
            for k, v in list(cc.items())[:25]
        }
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
