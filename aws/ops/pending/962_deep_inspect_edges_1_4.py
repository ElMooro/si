"""ops 962: list ALL Lambdas, find which justhodl- functions are deployed."""
import boto3, json, os, datetime as dt
from botocore.exceptions import ClientError

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
CHECKS = []
def add(n, ok, d): CHECKS.append({"name": n, "passed": ok, "detail": str(d)[:340]})

# List all justhodl- Lambdas
try:
    paginator = lam.get_paginator("list_functions")
    all_fns = []
    for page in paginator.paginate():
        for f in page["Functions"]:
            if "justhodl" in f["FunctionName"].lower() or "capit" in f["FunctionName"].lower():
                all_fns.append({
                    "name": f["FunctionName"],
                    "runtime": f.get("Runtime"),
                    "mem": f.get("MemorySize"),
                    "timeout": f.get("Timeout"),
                    "modified": f.get("LastModified", "")[:19]
                })
    add("all_justhodl_lambdas_count", True, f"n={len(all_fns)}")
    # Group by edge
    for fn in sorted(all_fns, key=lambda x: x["name"]):
        add(f"fn.{fn['name']}", True,
            f"runtime={fn['runtime']} mem={fn['mem']} timeout={fn['timeout']} mod={fn['modified']}")
except ClientError as e:
    add("list_failed", False, str(e)[:200])

# For each EXPECTED edge-1-4 Lambda, attempt explicit GetFunction
for fn in ["justhodl-vix-backwardation-trigger", "justhodl-insider-buys-enriched",
           "justhodl-breadth-thrust", "justhodl-vol-target-unwind",
           "justhodl-capitulation"]:
    try:
        info = lam.get_function(FunctionName=fn)
        cfg = info.get("Configuration", {})
        env = cfg.get("Environment", {}).get("Variables", {})
        add(f"target.{fn}", True,
            f"runtime={cfg.get('Runtime')} mem={cfg.get('MemorySize')} mod={cfg.get('LastModified','')[:19]} n_env={len(env)}")
    except ClientError as e:
        if "ResourceNotFoundException" in str(e):
            add(f"target.{fn}", False, "NOT DEPLOYED")
        else:
            add(f"target.{fn}", False, str(e)[:200])

# List S3 keys at data/
try:
    lst = s3.list_objects_v2(Bucket="justhodl-dashboard-live", Prefix="data/", MaxKeys=200)
    keys_recent = []
    for it in lst.get("Contents", []):
        name = it["Key"]
        if any(k in name.lower() for k in ["vix-back","capit","insider-buy","insider-cluster","breadth","vol-target"]):
            keys_recent.append(f"{name} ({it['Size']}B, mod={str(it['LastModified'])[:19]})")
    add("s3_keys_matching_edges_1_4", True, " | ".join(keys_recent[:20]))
except ClientError as e:
    add("s3_keys_matching_edges_1_4", False, str(e)[:200])

rep = {"ops": 962, "run_at": dt.datetime.utcnow().isoformat()+"Z", "checks": CHECKS,
       "summary": {"total": len(CHECKS), "passed": sum(1 for c in CHECKS if c["passed"])}}
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/962_deep_inspect_edges_1_4.json", "w") as f:
    json.dump(rep, f, indent=2)
print(f"\n=== TOTAL {len(CHECKS)} checks, {sum(1 for c in CHECKS if c['passed'])} passed ===\n")
for c in CHECKS:
    print(f"  [{'OK' if c['passed'] else 'X '}] {c['name']:45} {c['detail'][:130]}")
