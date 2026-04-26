#!/usr/bin/env python3
"""Step 202 — re-probe bonds (was 236B, suspicious) + check macro-brief root cause."""
import io, json, time, zipfile
from ops_report import report
import boto3
from botocore.exceptions import ClientError

PROBE_NAME = "justhodl-tmp-probe-202"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

PROBE_CODE = '''
import json, urllib.request, urllib.error
def lambda_handler(event, context):
    try:
        req = urllib.request.Request(event["url"], method=event.get("method","GET"),
                                     headers=event.get("headers", {}))
        with urllib.request.urlopen(req, timeout=30) as r:
            data = r.read()
            return {"ok": True, "status": r.status, "len": len(data),
                    "preview": data[:5000].decode("utf-8", errors="replace")}
    except urllib.error.HTTPError as e:
        b = e.read()[:300] if hasattr(e,"read") else b""
        return {"ok": False, "status": e.code, "body": b.decode("utf-8", errors="replace")}
    except Exception as e:
        return {"ok": False, "error": str(e)}
'''


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", PROBE_CODE)
    buf.seek(0); return buf.read()


with report("reprobe_bonds_and_macrobrief") as r:
    r.heading("Re-probe bonds + diagnose macro-brief")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    lam.create_function(
        FunctionName=PROBE_NAME, Runtime="python3.11",
        Role=ROLE_ARN, Handler="lambda_function.lambda_handler",
        Code={"ZipFile": build_zip()},
        Timeout=60, MemorySize=512, Architectures=["x86_64"],
    )
    time.sleep(3)

    # ── A. /agent/bonds — re-probe to bust 60s CF cache (use ?t= to vary URL)
    r.section("A. /agent/bonds — re-probe with cache-buster")
    cache_buster = int(time.time())
    resp = lam.invoke(
        FunctionName=PROBE_NAME, InvocationType="RequestResponse",
        Payload=json.dumps({
            "url": f"https://api.justhodl.ai/agent/bonds?t={cache_buster}",
            "method": "GET",
            "headers": {"Origin": "https://justhodl.ai", "User-Agent": UA},
        }),
    )
    result = json.loads(resp["Payload"].read())
    if result.get("ok"):
        r.log(f"  ✅ HTTP {result['status']} len={result['len']}")
        try:
            data = json.loads(result["preview"])
            if data.get("statusCode") and isinstance(data.get("body"), str):
                inner = json.loads(data["body"])
                r.log(f"  inner keys: {list(inner.keys())}")
                bi = inner.get("bond_indices", {})
                r.log(f"  bond_indices ({len(bi)} entries):")
                for k, v in list(bi.items())[:5]:
                    if isinstance(v, dict):
                        r.log(f"    {k}: current={v.get('current')} signal={v.get('signal')}")
            else:
                r.log(f"  body preview: {result['preview'][:1000]}")
        except Exception as e:
            r.log(f"  parse err: {e}")
            r.log(f"  raw: {result['preview'][:800]}")
    else:
        r.warn(f"  fail: {result}")

    # ── B. /agent/macro-brief — call directly via Function URL
    r.section("B. macro-brief direct Function URL probe")
    resp = lam.invoke(
        FunctionName="macro-financial-intelligence",
        InvocationType="RequestResponse",
        Payload=json.dumps({"queryStringParameters": {"action": "latest"}}),
    )
    payload = resp["Payload"].read().decode("utf-8", errors="replace")
    r.log(f"  status: {resp.get('StatusCode')} fnError: {resp.get('FunctionError','none')}")
    r.log(f"  payload first 1500B: {payload[:1500]}")

    # ── C. List /daily_briefs/ in S3 (this Lambda writes there per earlier probe)
    r.section("C. S3 /daily_briefs/ listing")
    s3 = boto3.client("s3", region_name="us-east-1")
    resp = s3.list_objects_v2(Bucket="justhodl-dashboard-live", Prefix="daily_briefs/", MaxKeys=10)
    objs = resp.get("Contents", [])
    r.log(f"  {len(objs)} briefs found:")
    from datetime import datetime, timezone
    for o in sorted(objs, key=lambda x: x["LastModified"], reverse=True)[:5]:
        age_h = (datetime.now(timezone.utc) - o["LastModified"]).total_seconds() / 3600
        r.log(f"    {o['Key']:50} {o['Size']:>8}B  {age_h:>5.1f}h")

    if objs:
        # Sample most recent
        latest = sorted(objs, key=lambda x: x["LastModified"], reverse=True)[0]
        r.log(f"\n  Sampling {latest['Key']}...")
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=latest["Key"])
        try:
            data = json.loads(obj["Body"].read())
            r.log(f"  Top-level keys: {list(data.keys())[:12]}")
            for k, v in list(data.items())[:8]:
                if isinstance(v, dict):
                    r.log(f"    {k}: dict({len(v)})")
                elif isinstance(v, list):
                    r.log(f"    {k}: list[{len(v)}]")
                else:
                    r.log(f"    {k}: {repr(v)[:120]}")
        except Exception as e:
            r.log(f"  parse err: {e}")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    r.log("Done")
