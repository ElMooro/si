"""
ops 959 -- GROUND-TRUTH AUDIT of edges 1-4 Lambda names + S3 keys.

The repo directory names (justhodl-vix-backwardation-trigger,
justhodl-insider-buys-enriched, justhodl-breadth-thrust) don't match
the actual deployed AWS function names (e.g. justhodl-capitulation).

This ops:
  1. Lists ALL justhodl-* Lambdas on AWS, sorted by last_modified
  2. Lists all data/* S3 keys, sized and dated
  3. Maps each edge (1-4) to its real Lambda + S3 key by name-similarity
  4. Identifies duplicate functions created by the recent config-normalize
     push (so we can decide which to keep vs delete)
"""

import json
import os
import time
import boto3
import datetime as dt

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
REPORT_PATH = "aws/ops/reports/959_ground_truth_audit_1_4.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

# Keyword hints for edge mapping (1-4)
EDGE_KEYWORDS = {
    "1_vix_backwardation": ["vix", "capitulation", "backward"],
    "2_insider_buys":      ["insider", "buy", "cluster"],
    "3_breadth_thrust":    ["breadth", "thrust", "zweig"],
    "4_vol_target_unwind": ["vol-target", "vol_target", "voltarget", "unwind"],
}

# --- List ALL justhodl-* Lambdas ---
all_lams = []
paginator = lam.get_paginator("list_functions")
for page in paginator.paginate():
    for fn in page["Functions"]:
        name = fn["FunctionName"]
        if not name.startswith("justhodl-"):
            continue
        all_lams.append({
            "name": name,
            "runtime": fn.get("Runtime"),
            "mem": fn.get("MemorySize"),
            "timeout": fn.get("Timeout"),
            "last_modified": fn.get("LastModified", "")[:19],
            "code_size_kb": round(fn.get("CodeSize", 0) / 1024, 1),
        })

print(f"Total justhodl-* Lambdas: {len(all_lams)}")

# Sort by last_modified desc
all_lams.sort(key=lambda x: x["last_modified"], reverse=True)

# --- List all data/* S3 keys ---
all_s3 = []
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="data/"):
    for obj in page.get("Contents", []):
        if not obj["Key"].endswith(".json"):
            continue
        all_s3.append({
            "key": obj["Key"],
            "size_kb": round(obj["Size"] / 1024, 1),
            "last_modified": obj["LastModified"].isoformat()[:19],
        })
all_s3.sort(key=lambda x: x["last_modified"], reverse=True)
print(f"Total data/*.json S3 keys: {len(all_s3)}")

# --- Map edges 1-4 to candidate Lambdas + S3 keys ---
edge_mapping = {}
for edge_id, kws in EDGE_KEYWORDS.items():
    matching_lams = [l for l in all_lams
                     if any(kw in l["name"].lower() for kw in kws)]
    matching_s3 = [k for k in all_s3
                   if any(kw in k["key"].lower() for kw in kws)]
    edge_mapping[edge_id] = {
        "keywords": kws,
        "candidate_lambdas": [
            {"name": l["name"], "last_modified": l["last_modified"],
             "mem": l["mem"], "timeout": l["timeout"]}
            for l in matching_lams
        ],
        "candidate_s3_keys": matching_s3,
    }

# --- Detect duplicates from recent config-normalize push ---
# Look at Lambdas modified in last 30 minutes (the normalize push)
now = dt.datetime.utcnow()
recent_lams = []
for l in all_lams:
    try:
        lm = dt.datetime.fromisoformat(l["last_modified"].replace("Z",""))
        age_min = (now - lm).total_seconds() / 60
        if age_min < 60:  # last hour
            recent_lams.append({**l, "age_min": round(age_min, 1)})
    except Exception:
        pass

report = {
    "ops": 959,
    "title": "ground-truth audit: edges 1-4 real AWS Lambda names + S3 keys",
    "run_at": dt.datetime.utcnow().isoformat() + "Z",
    "n_all_justhodl_lambdas": len(all_lams),
    "n_all_data_s3_keys": len(all_s3),
    "edge_mapping": edge_mapping,
    "recently_modified_lambdas": recent_lams,
    "all_justhodl_lambdas_first_30": all_lams[:30],
    "all_data_s3_keys_first_30": all_s3[:30],
}

print("\n=== EDGE MAPPING SUMMARY ===")
for edge_id, m in edge_mapping.items():
    print(f"\n  {edge_id}:")
    print(f"    keywords: {m['keywords']}")
    print(f"    Lambdas: {[l['name'] for l in m['candidate_lambdas']]}")
    print(f"    S3 keys: {[k['key'] for k in m['candidate_s3_keys']]}")

print(f"\n=== RECENTLY-MODIFIED LAMBDAS (last hour) ===")
for l in recent_lams:
    print(f"  {l['name']} mod_age={l['age_min']}m mem={l['mem']} to={l['timeout']}")

os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
with open(REPORT_PATH, "w") as f:
    json.dump(report, f, indent=2, default=str)
print(f"\nreport written to {REPORT_PATH}")
