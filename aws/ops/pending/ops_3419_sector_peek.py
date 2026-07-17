"""ops 3419 — what does a setup row's sector actually say?"""
import json, sys
from pathlib import Path
import boto3
from ops_report import report
S3C=boto3.client("s3","us-east-1")
with report("3419_sector_peek") as rep:
    j=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/best-setups.json")["Body"].read())
    rows=(j.get("top_setups") or [])[:6]
    secs=[{ "ticker":r.get("ticker"),"sector":r.get("sector"),"keys_sample":[k for k in r.keys() if "sec" in k.lower()][:4]} for r in rows]
    line=json.dumps(secs); print(line); rep.log(line[:400])
    Path("aws/ops/reports/3419.json").write_text(json.dumps({"rows":secs},indent=2)); sys.exit(0)
