"""
ops/843 - desk book structure probe (read-only).

Preflight for the Firm Book consolidated blotter. Dumps the exact JSON
shape of every strategy desk's book so the firm-book engine extracts
positions from documented field names, never guesses. For each desk:
top-level keys, and for every position array the field keys + a couple
of sample rows.

Writes aws/ops/reports/843_desk_book_probe.json.
"""
import json
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"

# desk -> (json key, [candidate position arrays to inspect])
DESKS = {
    "best-ideas": ("data/best-ideas.json",
                   ["stack", "titans", "high_conviction"]),
    "pairs-arb": ("data/pairs-arb.json",
                  ["pairs", "tradeable", "all_pairs"]),
    "trend-engine": ("data/trend-engine.json",
                     ["positions", "book", "signals"]),
    "merger-arb": ("data/merger-arb.json",
                   ["all_priced", "tight_carry", "deals"]),
    "spinoff-desk": ("data/spinoff-desk.json",
                     ["top_setups", "fresh_spinoffs", "seasoned_spinoffs"]),
    "index-recon": ("data/index-recon.json",
                    ["russell_2000_additions", "russell_2000_deletions",
                     "russell_graduations", "russell_demotions",
                     "sp500_candidates"]),
    "risk-radar": ("data/risk-radar.json",
                   ["stack", "shorts", "candidates"]),
}

cfg = Config(read_timeout=120, connect_timeout=20,
             retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 843,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Desk book structure probe - preflight for the Firm Book "
               "consolidated blotter",
    "desks": {},
}


def shape(v, depth=0):
    """Compact structural description of a value."""
    if isinstance(v, dict):
        return {k: shape(val, depth + 1) for k, val in list(v.items())[:40]}
    if isinstance(v, list):
        return f"list[{len(v)}]"
    if isinstance(v, str):
        return f"str({v[:48]})"
    return type(v).__name__


for desk, (key, arrays) in DESKS.items():
    entry = {"json_key": key}
    try:
        doc = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        entry["error"] = f"{type(e).__name__}: {e}"[:200]
        rep["desks"][desk] = entry
        continue

    entry["top_level_keys"] = sorted(doc.keys()) if isinstance(doc, dict) \
        else type(doc).__name__
    # summary block if present
    if isinstance(doc, dict) and isinstance(doc.get("summary"), dict):
        entry["summary"] = doc["summary"]
    entry["generated_at"] = doc.get("generated_at") \
        if isinstance(doc, dict) else None

    # for each candidate array, dump length + first rows' field shape
    found = {}
    for arr in arrays:
        v = doc.get(arr) if isinstance(doc, dict) else None
        if isinstance(v, list):
            info = {"length": len(v)}
            if v:
                first = v[0]
                if isinstance(first, dict):
                    info["item_keys"] = sorted(first.keys())
                    info["sample_row"] = {
                        k: shape(first[k]) for k in list(first.keys())[:30]}
                    if len(v) > 1 and isinstance(v[1], dict):
                        info["row2"] = {
                            k: (v[1][k] if isinstance(
                                v[1][k], (int, float, str, bool))
                                else shape(v[1][k]))
                            for k in list(v[1].keys())[:30]}
                else:
                    info["item_type"] = type(first).__name__
            found[arr] = info
    entry["arrays"] = found
    rep["desks"][desk] = entry

out = json.dumps(rep, indent=2, default=str)
print(out[:6000])
try:
    with open("aws/ops/reports/843_desk_book_probe.json", "w") as f:
        f.write(out)
except Exception as e:
    print("report write skipped:", e)
