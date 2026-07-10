#!/usr/bin/env python3
"""ops 3049 -- breadth-thrust.html 'isn't working' (Khalid). Evidence
pass: (a) feed exists/fresh on S3? (b) reachable via justhodl.ai/data/
relative route? (c) bucket CORS allows the page's direct-S3 fetch?
(d) engine + schedule alive? No fixes here -- evidence only."""
import json
import sys
import urllib.request
from datetime import datetime, timezone

import boto3
from ops_report import report

S3 = boto3.client("s3", region_name="us-east-1")
LAM = boto3.client("lambda", region_name="us-east-1")
EVT = boto3.client("events", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "data/breadth-thrust.json"


def fetch(url):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 ops-3049",
            "Origin": "https://justhodl.ai"})
        r = urllib.request.urlopen(req, timeout=20)
        body = r.read()
        return {"code": r.status, "len": len(body),
                "cors": r.headers.get("Access-Control-Allow-Origin"),
                "head": body[:120].decode("utf-8", "replace")}
    except Exception as e:
        return {"err": str(e)[:160]}


with report("3049_breadth_diag") as rep:
    rep.section("1. S3 object")
    try:
        h = S3.head_object(Bucket=BUCKET, Key=KEY)
        age_h = (datetime.now(timezone.utc)
                 - h["LastModified"]).total_seconds() / 3600
        rep.kv(s3_exists=True, age_h=round(age_h, 1),
               size=h["ContentLength"])
        body = json.loads(S3.get_object(Bucket=BUCKET,
                                        Key=KEY)["Body"].read())
        rep.kv(top_keys=json.dumps(sorted(body.keys())[:14]),
               as_of=body.get("as_of"),
               state=body.get("state") or (body.get("zweig") or
                                           {}).get("state"))
    except Exception as e:
        rep.kv(s3_exists=False, err=str(e)[:140])

    rep.section("2. Routes (runner-side)")
    rep.kv(rel_route=json.dumps(fetch(
        "https://justhodl.ai/" + KEY)))
    rep.kv(s3_route=json.dumps(fetch(
        "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/"
        + KEY)))
    try:
        cors = S3.get_bucket_cors(Bucket=BUCKET)
        rep.kv(bucket_cors=json.dumps(cors.get("CORSRules"))[:400])
    except Exception as e:
        rep.kv(bucket_cors="NONE (%s)" % str(e)[:80])

    rep.section("3. Engine + schedule")
    try:
        c = LAM.get_function_configuration(
            FunctionName="justhodl-breadth-thrust")
        rep.kv(fn_modified=c["LastModified"],
               state=c.get("State"), status=c.get("LastUpdateStatus"))
    except Exception as e:
        rep.kv(fn="MISSING: %s" % str(e)[:80])
    try:
        r = EVT.describe_rule(Name="justhodl-breadth-thrust-daily")
        rep.kv(rule=r.get("State"), cron=r.get("ScheduleExpression"))
    except Exception as e:
        rep.kv(rule="MISSING: %s" % str(e)[:80])
    rep.log("evidence-only pass done")
    if False:
        sys.exit(1)   # preflight contract: diagnostic never fails
sys.exit(0)
