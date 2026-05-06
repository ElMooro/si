"""Probe justhodl-asymmetric-scorer and justhodl-risk-sizer:
  1. Inspect Lambda source code (handler, output keys)
  2. Find where they actually write in S3 (vs the 120b stubs at canonical paths)
  3. Show what real outputs exist if any
  4. Smoke invoke each to capture the live response
"""
import io
import json
import os
import time
import zipfile
import boto3
from datetime import datetime, timezone
from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def main():
    with report("probe_asymmetric_and_risk_sizer") as r:
        for name in ["justhodl-asymmetric-scorer", "justhodl-risk-sizer"]:
            r.heading(f"=== {name} ===")
            # 1. Lambda config
            try:
                cfg = lam.get_function_configuration(FunctionName=name)
                r.log(f"  state: {cfg['State']}  mem={cfg['MemorySize']}MB  timeout={cfg['Timeout']}s")
                r.log(f"  last modified: {cfg.get('LastModified')}")
                r.log(f"  handler: {cfg.get('Handler')}")
                env = (cfg.get("Environment") or {}).get("Variables") or {}
                r.log(f"  env: {list(env.keys())}")
            except Exception as e:
                r.log(f"  ✗ {e}")
                continue

            # 2. Pull the Lambda code and look for S3 put_object calls to find write paths
            try:
                code_resp = lam.get_function(FunctionName=name)
                code_url = code_resp["Code"]["Location"]
                # download via urllib (avoid signed-url auth issues with boto)
                import urllib.request
                with urllib.request.urlopen(code_url, timeout=20) as resp_code:
                    z = zipfile.ZipFile(io.BytesIO(resp_code.read()))
                # Find lambda_function.py
                for n in z.namelist():
                    if n.endswith("lambda_function.py") or (n.endswith(".py") and "test" not in n.lower()):
                        src = z.read(n).decode("utf-8", errors="replace")
                        # Find put_object calls
                        import re
                        keys = re.findall(r'(?:Key|key)\s*=\s*["\']([^"\']+\.json)["\']', src)
                        keys = sorted(set(keys))
                        r.log(f"  source file: {n}  ({len(src):,} chars)")
                        r.log(f"  S3 keys referenced in code:")
                        for k in keys:
                            r.log(f"    • {k}")
                        # Output dict
                        out_match = re.search(r'OUTPUT_KEY\s*=\s*["\']([^"\']+)["\']', src)
                        if out_match:
                            r.log(f"  OUTPUT_KEY constant: {out_match.group(1)}")
                        # The first put_object key (most likely output)
                        put_calls = re.findall(r'put_object\([^)]*Key\s*=\s*["\']([^"\']+)["\']', src)
                        if put_calls:
                            r.log(f"  put_object writes to: {put_calls}")
                        break
            except Exception as e:
                r.log(f"  ⚠ couldn't fetch code: {e}")

            # 3. List S3 objects matching the Lambda name
            r.log("")
            r.log("  S3 keys matching pattern:")
            for prefix in [f"data/{name.replace('justhodl-','')}",
                           name.replace('justhodl-','')+"/",
                           f"data/{name.replace('justhodl-','').replace('-','_')}"]:
                try:
                    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=15)
                    for obj in resp.get("Contents", []) or []:
                        r.log(f"    {obj['Key']:55s} {obj['Size']:>9,}b  {obj['LastModified'].isoformat()}")
                except Exception:
                    pass

            # 4. Smoke invoke
            r.log("")
            r.log(f"  Smoke invoke {name}:")
            try:
                t0 = time.time()
                resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
                body = resp["Payload"].read().decode()
                r.log(f"    status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
                r.log(f"    response: {body[:500]}")
            except Exception as e:
                r.log(f"    ✗ {e}")

            r.log("")


if __name__ == "__main__":
    main()
