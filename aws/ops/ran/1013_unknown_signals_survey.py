#!/usr/bin/env python3
"""Step 1013 — Survey unknown signal_types + engine snapshot patterns
for near-miss monitoring design.

Outputs:
  - top 43 unknown signal_types with counts (for KNOWN_ENGINE_SIGNALS extension)
  - sample of which engines write to S3 with score-like fields (for near-miss
    monitor adapter design)
  - signal-scorecard top 10 promoted signals + their threshold patterns
"""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1013_unknown_signals_survey.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)


def grab(key, byte_limit=8000):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        body = obj["Body"].read().decode("utf-8", errors="replace")
        return json.loads(body)
    except Exception as e:
        return {"err": str(e)[:200]}


def list_engine_snapshots():
    """List S3 keys under data/ that look like engine output snapshots."""
    paginator = s3.get_paginator("list_objects_v2")
    snaps = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix="data/"):
        for obj in page.get("Contents") or []:
            k = obj["Key"]
            # Skip directories, ledgers, archives, large bulk files
            if k.endswith("/") or "/archive/" in k or "/misses/" in k:
                continue
            if obj["Size"] > 200_000:
                continue
            if obj["Size"] < 200:
                continue
            snaps.append({"key": k, "size": obj["Size"],
                          "modified": str(obj["LastModified"])})
    return snaps[:200]


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # 1. Pull the unknown_signal_types from engine-signal-map
    em = grab("data/engine-signal-map.json")
    if isinstance(em, dict) and "err" not in em:
        out["engine_map_totals"] = em.get("totals")
        out["unknown_signal_types"] = em.get("unknown_signal_types", [])
        out["new_id_prefixes"] = em.get("new_id_prefixes", [])
        out["known_families"] = list((em.get("by_family") or {}).keys())
    else:
        out["engine_map_err"] = em
    
    # 2. Scorecard top 10 promoted + their stats
    sc = grab("data/signal-scorecard.json")
    if isinstance(sc, dict) and "err" not in sc:
        out["scorecard_promoted"] = sc.get("promoted_signals", [])[:15]
        out["scorecard_deprecated"] = sc.get("deprecated_signals", [])[:15]
        # Top promoted signals' stats — show n, hit rate
        rows = sc.get("by_signal_type") or sc.get("signals") or []
        if isinstance(rows, list):
            out["scorecard_promoted_stats"] = []
            for r in rows:
                if isinstance(r, dict):
                    name = (r.get("signal_type") or r.get("name") or "").lower()
                    if name in (s.lower() for s in out["scorecard_promoted"]):
                        out["scorecard_promoted_stats"].append({
                            "signal_type": name,
                            "n_scored":    r.get("n_scored") or r.get("n"),
                            "wilson_lb":   r.get("wilson_lb") or r.get("hit_rate_lb"),
                            "avg_return":  r.get("avg_return") or r.get("mean_return"),
                            "grade":       r.get("grade"),
                        })
    
    # 3. List engine snapshot keys to understand the surface area
    snaps = list_engine_snapshots()
    out["s3_snapshots"] = snaps
    out["n_snapshots"] = len(snaps)
    
    # 4. For 5 likely-promising snapshots, peek at structure
    candidates = [s for s in snaps if any(
        kw in s["key"] for kw in ("screener", "edge", "opportunity", "momentum",
                                    "deepvalue", "epsvelocity", "earnings", "scorecard")
    )][:6]
    out["snapshot_samples"] = []
    for c in candidates:
        try:
            d = grab(c["key"], byte_limit=2000)
            if "err" in d: continue
            sample = {
                "key":         c["key"],
                "size":        c["size"],
                "top_keys":    list(d.keys())[:15] if isinstance(d, dict) else "<list>",
            }
            # Look for items with a 'score' or 'rank' or 'confidence' field
            for try_key in ("top_tickers", "ranked", "items", "stocks", "candidates",
                             "results", "picks", "promotions"):
                arr = d.get(try_key) if isinstance(d, dict) else None
                if isinstance(arr, list) and arr and isinstance(arr[0], dict):
                    sample["array_key"]  = try_key
                    sample["array_len"]  = len(arr)
                    sample["item_keys"]  = list(arr[0].keys())[:20]
                    sample["item_sample"] = {k: arr[0].get(k)
                        for k in list(arr[0].keys())[:8]
                        if not isinstance(arr[0].get(k), (list, dict))}
                    break
            out["snapshot_samples"].append(sample)
        except Exception as e:
            out["snapshot_samples"].append({"key": c["key"], "err": str(e)[:100]})
    
    # 5. Sample 5 DDB items where signal_type matches one of the unknowns
    if out.get("unknown_signal_types"):
        from boto3.dynamodb.conditions import Attr
        table = ddb.Table("justhodl-signals")
        top_unknown = [u["signal_type"] for u in out["unknown_signal_types"][:5]]
        out["ddb_unknown_samples"] = {}
        for st in top_unknown:
            try:
                r = table.scan(
                    FilterExpression=Attr("signal_type").eq(st),
                    Limit=2,
                )
                items = r.get("Items", [])
                if items:
                    i = items[0]
                    out["ddb_unknown_samples"][st] = {
                        "signal_id":    str(i.get("signal_id", ""))[:30],
                        "signal_value": str(i.get("signal_value", ""))[:30],
                        "confidence":   i.get("confidence"),
                        "predicted_direction": i.get("predicted_direction"),
                        "metadata_keys": list((i.get("metadata") or {}).keys())[:10],
                    }
            except Exception as e:
                out["ddb_unknown_samples"][st] = {"err": str(e)[:100]}
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
