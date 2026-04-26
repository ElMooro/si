#!/usr/bin/env python3
"""Step 213 — re-verify calibrator after fix in commit 56acd0f.

After deploy-lambdas.yml redeploys justhodl-calibrator with the
corrected ordering (SSM puts → add_ka_aliases → S3 puts), force-
invoke and check S3 output for ka_* aliases.
"""
import json, time
from datetime import datetime, timezone
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def find_keys_in_obj(obj, prefix, _seen=None, _depth=0):
    if _seen is None: _seen = set()
    if _depth > 12: return _seen
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and k.startswith(prefix):
                _seen.add(k)
            find_keys_in_obj(v, prefix, _seen, _depth + 1)
    elif isinstance(obj, list):
        for x in obj:
            find_keys_in_obj(x, prefix, _seen, _depth + 1)
    return _seen


with report("verify_calibrator_fixed") as r:
    r.heading("Verify calibrator fix — ka_* aliases in S3 after SSM-ordering fix")

    # Wait for deploy if not yet
    r.section("A. Confirm deployed code has SSM-puts-before-aliasing")
    import urllib.request, zipfile, io
    info = lam.get_function(FunctionName="justhodl-calibrator")
    with urllib.request.urlopen(info["Code"]["Location"], timeout=30) as resp:
        zb = resp.read()
    with zipfile.ZipFile(io.BytesIO(zb)) as zf:
        src = zf.read("lambda_function.py").decode("utf-8", errors="replace")

    # Find positions of SSM puts vs add_ka_aliases call
    import re
    ssm_pos = [m.start() for m in re.finditer(r'ssm\.put_parameter', src)]
    alias_pos = [m.start() for m in re.finditer(r'add_ka_aliases\(report\)', src)]
    s3_pos = [m.start() for m in re.finditer(r's3\.put_object', src)]

    r.log(f"  SSM put positions: {ssm_pos[:5]}")
    r.log(f"  add_ka_aliases positions: {alias_pos}")
    r.log(f"  S3 put positions: {s3_pos[:5]}")

    if alias_pos and ssm_pos and s3_pos:
        last_ssm = max(ssm_pos)
        first_s3 = min(s3_pos)
        first_alias = min(alias_pos)
        if last_ssm < first_alias < first_s3:
            r.log(f"  ✅ ordering correct: SSM ({last_ssm}) → alias ({first_alias}) → S3 ({first_s3})")
        else:
            r.warn(f"  ⚠ ordering wrong: SSM={last_ssm} alias={first_alias} S3={first_s3}")
            r.warn(f"    deploy may not have completed yet")

    # Force-invoke calibrator
    r.section("B. Force-invoke calibrator")
    try:
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-calibrator", InvocationType="RequestResponse",
                          Payload=json.dumps({}))
        elapsed = time.time() - t0
        err = resp.get("FunctionError")
        payload = resp["Payload"].read().decode("utf-8", errors="replace")
        if err:
            r.warn(f"  ✗ {elapsed:.1f}s err={err}")
            r.log(f"  payload: {payload[:600]}")
        else:
            r.log(f"  ✅ {elapsed:.1f}s err=none")
            r.log(f"  payload preview: {payload[:300]}")
    except Exception as e:
        r.warn(f"  invoke fail: {e}")

    time.sleep(5)

    # Check S3 output
    r.section("C. Verify calibration/latest.json now has ka_* aliases")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="calibration/latest.json")
        content = obj["Body"].read()
        age_s = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds()
        r.log(f"  size: {len(content)}B  age: {age_s:.1f}s")
        data = json.loads(content)

        khalid_found = find_keys_in_obj(data, "khalid_")
        ka_found = find_keys_in_obj(data, "ka_")
        r.log(f"  khalid_* keys ({len(khalid_found)}): {sorted(khalid_found)}")
        r.log(f"  ka_* keys     ({len(ka_found)}): {sorted(ka_found)}")

        missing = [k for k in khalid_found if "ka_" + k[len("khalid_"):] not in ka_found]
        if not missing and ka_found:
            r.log(f"  ▸ ✅ DUAL-WRITE-OK — all {len(khalid_found)} aliased")
        elif missing:
            r.warn(f"  ⚠ {len(missing)} missing aliases: {missing}")
        else:
            r.warn(f"  ✗ no aliases found")
    except Exception as e:
        r.warn(f"  err: {e}")

    r.log("Done")
