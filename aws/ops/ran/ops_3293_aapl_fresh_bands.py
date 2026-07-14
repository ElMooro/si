"""ops 3293 — finish 3292 verification. AAPL had a 24h-cached pre-3292
doc (risk fields None); MARA already proved the new module flags serial
diluters. Here: force_refresh AAPL (engine honors refresh=1 via async
self-invoke + S3 cache write), poll the cache doc until the new fields
land, then run the clean-name truth bands: risk_flag=False,
ten_year_multiple in 0.5-1.0 (buyback machine), >=8 annual points."""
import json
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-equity-research"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 0}))


def cache_doc(tkr):
    for key in ("equity-research/%s.json" % tkr,):
        try:
            return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                              ["Body"].read()), key
        except Exception:
            continue
    return None, None


with report("3293_aapl_fresh_bands") as rep:
    fails = []
    LAM.invoke(FunctionName=FN, InvocationType="Event",
               Payload=json.dumps({"queryStringParameters":
                                   {"ticker": "AAPL",
                                    "refresh": "1"}}).encode())
    dil, key = {}, None
    for i in range(36):
        time.sleep(10)
        d, key = cache_doc("AAPL")
        dil = (d or {}).get("dilution") or {}
        if dil.get("risk_flag") is not None:
            break
    rep.kv(cache_key=key, verdict=dil.get("verdict"),
           risk=dil.get("risk_flag"),
           mult10=dil.get("ten_year_multiple"),
           n_annual=len(dil.get("annual_series") or []),
           latest=dil.get("latest_shares"))
    if dil.get("risk_flag") is not False:
        fails.append("AAPL risk_flag: %r" % dil.get("risk_flag"))
    m10 = dil.get("ten_year_multiple")
    if not (m10 and 0.5 <= m10 < 1.0):
        fails.append("AAPL 10y multiple: %s" % m10)
    if len(dil.get("annual_series") or []) < 8:
        fails.append("annual series thin")

    rep.kv(fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3293 PASS — fresh AAPL carries the v2 dilution "
            "record; 3292 arc fully proven.")
sys.exit(0)
