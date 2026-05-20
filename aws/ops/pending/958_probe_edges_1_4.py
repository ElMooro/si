"""ops 958: probe deployed Lambda names for edges 1-4."""
import json, os, datetime as dt
import boto3
from botocore.exceptions import ClientError

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
CHECKS=[]
def add(name,ok,det): CHECKS.append({"name":name,"passed":ok,"detail":str(det)[:280]})

# List ALL Lambdas matching edge-1-4 keywords
try:
    paginator = lam.get_paginator("list_functions")
    matches = []
    for page in paginator.paginate():
        for f in page["Functions"]:
            n = f["FunctionName"]
            if any(k in n.lower() for k in ["vix-back","vix-capit","insider-buy","breadth-thrust","vol-target","vol_target","capitulation"]):
                matches.append({"name": n, "runtime": f.get("Runtime"),
                                "mem": f.get("MemorySize"), "timeout": f.get("Timeout"),
                                "last_modified": f.get("LastModified", "")[:16]})
    add("lambdas_found", True, json.dumps(matches, default=str)[:500])
except ClientError as e:
    add("lambdas_found", False, str(e)[:200])

# Check S3 outputs
for key in ["data/vix-backwardation-trigger.json","data/vix-capitulation.json",
            "data/insider-buys.json","data/insider-buys-enriched.json",
            "data/insider-clusters.json","data/breadth-thrust.json",
            "data/vol-target-unwind.json","data/vol-target.json"]:
    try:
        h = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
        add(f"s3.{key}", True, f"size={h['ContentLength']}B mod={h['LastModified']}")
    except ClientError as e:
        if "NoSuchKey" in str(e) or "404" in str(e):
            add(f"s3.{key}", False, "not found")
        else:
            add(f"s3.{key}", False, str(e)[:120])

rep={"ops":958,"title":"probe deployed Lambda names + S3 outputs for edges 1-4",
     "run_at":dt.datetime.utcnow().isoformat()+"Z","checks":CHECKS,
     "summary":{"total":len(CHECKS),"passed":sum(1 for c in CHECKS if c["passed"])}}
os.makedirs("aws/ops/reports",exist_ok=True)
open("aws/ops/reports/958_probe_edges_1_4.json","w").write(json.dumps(rep,indent=2))
for c in CHECKS:
    print(f"  [{'OK' if c['passed'] else 'X '}] {c['name']:40} {c['detail'][:140]}")
