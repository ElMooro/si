"""1201 — Final forensic: get MRVL/AVGO TIMELINE from radar-state correctly.

radar-state.json has shape: {snapshot_at, tickers: {...}}
Need to look INSIDE the tickers field. Also check is_ultra_new flag on
current MRVL/AVGO entries to confirm when each became ULTRA.

Also: look for any DAILY snapshot archive of convergence-radar.json itself.
"""
import json
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1201_final_mrvl_timeline.json"
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


def list_keys(prefix, max_n=300):
    pag = s3.get_paginator("list_objects_v2")
    keys = []
    for page in pag.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in (page.get("Contents") or []):
            keys.append({
                "key": obj["Key"], "size": obj["Size"],
                "modified": obj["LastModified"].isoformat(),
            })
            if len(keys) >= max_n:
                return keys
    return keys


# 1. Parse radar-state correctly
print("[1201] 1. Parse convergence-radar-state.json (inside 'tickers' field)")
state = read_safe("data/_alerts/convergence-radar-state.json")
if not state.get("_error"):
    out["state_snapshot_at"] = state.get("snapshot_at")
    tickers = state.get("tickers") or {}
    out["n_tickers_in_state"] = len(tickers)
    print(f"  snapshot_at: {state.get('snapshot_at')}")
    print(f"  n tickers tracked: {len(tickers)}")
    
    # Show MRVL, AVGO, NVDA, QCOM, TXN, AMD full state
    for t in ["MRVL", "AVGO", "NVDA", "QCOM", "TXN", "AMD"]:
        if t in tickers:
            v = tickers[t]
            out.setdefault("ticker_state", {})[t] = v
            print(f"\n  📌 {t}:")
            print(f"     {json.dumps(v, default=str, indent=2)[:1200]}")
        else:
            print(f"\n  📌 {t}: NOT in tickers state")


# 2. Look at current convergence-radar.json's MRVL/AVGO entries in FULL
print(f"\n\n[1201] 2. CURRENT convergence-radar.json — FULL MRVL/AVGO entries")
rad = read_safe("data/convergence-radar.json")
if not rad.get("_error"):
    items = rad.get("items") or rad.get("tickers") or rad.get("results") or []
    for item in items if isinstance(items, list) else []:
        t = item.get("ticker")
        if t in ["MRVL", "AVGO", "NVDA"]:
            out.setdefault("current_radar_entries", {})[t] = item
            print(f"\n  📌 {t} FULL:")
            print(f"     {json.dumps(item, default=str, indent=2)[:2500]}")


# 3. Look at justhodl-multi-tf-convergence Lambda config
print(f"\n\n[1201] 3. justhodl-multi-tf-convergence Lambda info")
try:
    c = lam.get_function_configuration(FunctionName="justhodl-multi-tf-convergence")
    out["multi_tf_lambda"] = {
        "runtime": c.get("Runtime"),
        "timeout": c.get("Timeout"),
        "memory": c.get("MemorySize"),
        "last_modified": c.get("LastModified"),
        "description": c.get("Description"),
    }
    print(f"  runtime: {c.get('Runtime')} · last_modified: {c.get('LastModified')}")
    print(f"  description: {c.get('Description','')[:200]}")
except Exception as e:
    out["multi_tf_lambda"] = {"error": str(e)[:200]}


# 4. Look for DAILY snapshots of convergence-radar.json
print(f"\n\n[1201] 4. Search for HISTORICAL convergence-radar archives")
candidates = [
    "convergence-radar/history/",
    "convergence-radar-history/",
    "data/convergence-radar-history/",
    "data/_archive/convergence-radar/",
    "data/_archive/radar/",
    "radar-history/",
    "pump-radar-history/",
]
for prefix in candidates:
    keys = list_keys(prefix, max_n=10)
    if keys:
        print(f"  ✓ {prefix}: {len(keys)} files")
        for k in keys[:5]:
            print(f"    {k['key']:60s} {round(k['size']/1024,1):>6.1f} KB  {k['modified'][:16]}")
        out.setdefault("history_archives", {})[prefix] = keys


# 5. List ALL lambdas — find ones related to convergence/radar/pump
print(f"\n\n[1201] 5. ALL Lambdas matching convergence/radar/pump/digest patterns")
try:
    all_lambdas = []
    pag = lam.get_paginator("list_functions")
    for page in pag.paginate():
        for f in page.get("Functions", []):
            n = f["FunctionName"]
            if any(x in n.lower() for x in ["convergence","radar","pump","digest","alert","velocity","momentum","frontrun","sniff","pre-pump","prepump"]):
                all_lambdas.append({
                    "name": n,
                    "last_modified": f.get("LastModified"),
                    "description": (f.get("Description") or "")[:120],
                })
    out["relevant_lambdas"] = all_lambdas
    print(f"  Found {len(all_lambdas)} relevant Lambdas:")
    for l in all_lambdas:
        print(f"    {l['name']:50s} {l['last_modified'][:16]}  {l['description'][:80]}")
except Exception as e:
    out["relevant_lambdas"] = {"error": str(e)[:200]}


# 6. Check generated_at on convergence-radar.json + radar-state.json
print(f"\n\n[1201] 6. Metadata on key files")
for key in ["data/convergence-radar.json", "data/_alerts/convergence-radar-state.json",
             "data/momentum-leaders.json", "data/velocity-acceleration.json",
             "data/pump-radar-brief.json", "data/pump-positioning.json"]:
    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
        out.setdefault("file_metadata", {})[key] = {
            "modified": head["LastModified"].isoformat(),
            "size_kb": round(head["ContentLength"] / 1024, 1),
        }
        print(f"  {key:55s} {round(head['ContentLength']/1024,1):>7.1f} KB  modified: {head['LastModified'].isoformat()[:16]}")
    except Exception as e:
        print(f"  ❌ {key}: {e}")


out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1201] DONE")
