#!/usr/bin/env python3
"""Step 236 — diagnose FRED silent failure + re-seed correlation-breaks.

Two questions:
  1. Why did the crisis-plumbing manual invoke produce 0 populated series?
     (elapsed=0.6s suggests fail-fast — FRED key, network, or new series IDs?)
  2. Re-invoke justhodl-correlation-breaks now that it's past Pending state,
     so data/correlation-breaks.json gets seeded for correlation.html.
"""
import io
import json
import sys
import time
import urllib.parse
import urllib.request
import zipfile
from ops_report import report
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
PROBE_NAME = "justhodl-tmp-fred-probe-236"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300))
s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)

# Probe Lambda — runs a single FRED call from inside AWS to confirm key/network
PROBE_CODE = '''
import json, os, urllib.request, urllib.parse
def lambda_handler(event, context):
    series = event.get("series_id", "DGS10")
    api_key = event.get("api_key", os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989"))
    params = {"series_id": series, "api_key": api_key, "file_type": "json", "limit": 5, "sort_order": "desc"}
    url = "https://api.stlouisfed.org/fred/series/observations?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            body = r.read().decode()
            data = json.loads(body)
            return {"ok": True, "status": r.status,
                    "n_obs": len(data.get("observations", [])),
                    "first_3": data.get("observations", [])[:3],
                    "error_message": data.get("error_message"),
                    "raw_excerpt": body[:500]}
    except Exception as e:
        return {"ok": False, "error": str(e), "type": type(e).__name__}
'''


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", PROBE_CODE)
    buf.seek(0)
    return buf.read()


with report("diagnose_fred_and_seed_corr") as r:
    r.heading("Diagnose FRED silent failure + re-seed correlation-breaks")

    # 1. Build probe Lambda
    r.section("1. Test FRED API directly from AWS")
    try:
        lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError:
        pass
    lam.create_function(
        FunctionName=PROBE_NAME,
        Runtime="python3.11",
        Role=ROLE_ARN,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": build_zip()},
        Timeout=30,
        MemorySize=256,
        Architectures=["x86_64"],
    )
    time.sleep(4)  # wait past Pending state

    # Test 3 series: one well-known (DGS10), one Phase 9.3 new (WGMMNS), one funding+credit (SOFR)
    for sid in ("DGS10", "WGMMNS", "SOFR", "BUSLOANS", "INTGSBJPM193N", "DTWEXBGS"):
        resp = lam.invoke(FunctionName=PROBE_NAME, InvocationType="RequestResponse",
                          Payload=json.dumps({"series_id": sid}))
        result = json.loads(resp["Payload"].read())
        if result.get("ok"):
            n = result.get("n_obs", 0)
            err = result.get("error_message")
            if n > 0:
                latest = result["first_3"][0] if result["first_3"] else {}
                r.log(f"  ✅ {sid:18s}  n_obs={n}  latest={latest.get('date')}={latest.get('value')}")
            else:
                r.log(f"  ⚠ {sid:18s}  n_obs=0  fred_error={err!r}")
                r.log(f"     raw: {result.get('raw_excerpt','')[:200]}")
        else:
            r.warn(f"  ✗ {sid:18s}  exception: {result.get('error')}")

    try:
        lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError:
        pass

    # 2. Look at CloudWatch logs from the failed crisis-plumbing invoke
    r.section("2. CloudWatch tail from latest justhodl-crisis-plumbing invoke")
    log_group = "/aws/lambda/justhodl-crisis-plumbing"
    try:
        # Get latest log stream
        streams = logs.describe_log_streams(
            logGroupName=log_group, orderBy="LastEventTime", descending=True, limit=2
        )["logStreams"]
        for stream in streams[:1]:
            r.log(f"  stream: {stream['logStreamName']}")
            events = logs.get_log_events(
                logGroupName=log_group,
                logStreamName=stream["logStreamName"],
                limit=80,
                startFromHead=False,
            )["events"]
            r.log(f"  last {len(events)} events:")
            for e in events[-50:]:
                msg = e["message"].rstrip()
                if any(k in msg for k in ("[fred]", "error", "Error", "exception", "[crisis-plumbing]")):
                    r.log(f"    {msg[:300]}")
    except ClientError as e:
        r.warn(f"  ✗ logs error: {e}")

    # 3. Re-invoke crisis-plumbing now to verify if FRED is recovered
    r.section("3. Re-invoke justhodl-crisis-plumbing (fresh attempt)")
    t0 = time.time()
    try:
        resp = lam.invoke(FunctionName="justhodl-crisis-plumbing", InvocationType="RequestResponse")
        payload = json.loads(resp["Payload"].read())
        dur = round(time.time() - t0, 1)
        r.log(f"  invoke ({dur}s): {json.dumps(payload)[:400]}")
        # Read fresh S3 to check
        body = s3.get_object(Bucket="justhodl-dashboard-live",
                             Key="data/crisis-plumbing.json")["Body"].read()
        d = json.loads(body)
        ci = d.get("crisis_indices", {})
        n_ci = sum(1 for k, v in ci.items() if v.get("available"))
        plumb = d.get("plumbing_tier2", {})
        n_pl = sum(1 for k, v in plumb.items() if v.get("available"))
        fcs = d.get("funding_credit_signals", {})
        n_fc = sum(1 for k, v in fcs.items() if v.get("available"))
        xcc = d.get("xcc_basis_proxy", {})
        n_xcc = sum(1 for k, v in xcc.items() if v.get("available"))
        r.log(f"  fresh S3: crisis={n_ci}/5  plumbing={n_pl}/7  funding+credit={n_fc}/6  xcc={n_xcc}/4")
    except Exception as e:
        r.warn(f"  ✗ {e}")

    # 4. Re-seed correlation-breaks Lambda (was in Pending state during step 235)
    r.section("4. Re-seed justhodl-correlation-breaks")
    # Wait to be sure Lambda is ready
    cfg = lam.get_function_configuration(FunctionName="justhodl-correlation-breaks")
    r.log(f"  state: {cfg['State']}  reason: {cfg.get('StateReason','—')}")
    if cfg["State"] != "Active":
        r.log("  waiting for Active state...")
        for i in range(20):
            time.sleep(3)
            cfg = lam.get_function_configuration(FunctionName="justhodl-correlation-breaks")
            if cfg["State"] == "Active":
                r.log(f"  → Active after {(i+1)*3}s")
                break
    if cfg["State"] != "Active":
        r.warn(f"  ⚠ Still not Active: {cfg['State']}")
        sys.exit(0)
    t0 = time.time()
    try:
        resp = lam.invoke(FunctionName="justhodl-correlation-breaks", InvocationType="RequestResponse")
        payload = json.loads(resp["Payload"].read())
        dur = round(time.time() - t0, 1)
        if resp.get("FunctionError"):
            r.warn(f"  ✗ FunctionError ({dur}s): {payload}")
        else:
            r.log(f"  ✅ OK ({dur}s)")
            r.log(f"  payload: {json.dumps(payload)[:600]}")
    except Exception as e:
        r.warn(f"  ✗ {e}")

    # 5. Read correlation-breaks.json to confirm
    r.section("5. data/correlation-breaks.json status")
    try:
        body = s3.get_object(Bucket="justhodl-dashboard-live",
                             Key="data/correlation-breaks.json")["Body"].read()
        d = json.loads(body)
        r.log(f"  bytes: {len(body)}")
        r.log(f"  schema_version: {d.get('schema_version')}")
        r.log(f"  status: {d.get('status', 'ok')}")
        if d.get("status") != "warming_up":
            r.log(f"  signal: {d.get('signal')}")
            r.log(f"  frobenius_z: {d.get('frobenius_z_score_1y')}")
            r.log(f"  n_pairs_2sigma: {d.get('n_pairs_above_2sigma')}")
            tops = d.get("top_breaking_pairs", [])
            r.log(f"  top breaking pairs ({len(tops)}):")
            for p in tops[:5]:
                r.log(f"    {p['pair'][0]}↔{p['pair'][1]}  curr={p['current_corr']:.3f}  "
                      f"baseline={p['baseline_corr']:.3f}  z={p['z_score']:+.2f}")
        else:
            r.log(f"  warming_up: {d.get('message')}")
    except Exception as e:
        r.warn(f"  ✗ {e}")

    r.section("DONE")
    r.log("  Done")
