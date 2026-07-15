"""ops 3359 — read the ACTUAL error strings sovereign-stress emits, so we fix the right
data sources instead of guessing. Reads data/sovereign-stress.json (already written) and
also does a direct probe of the ECB CISS endpoint it uses. Read-only diagnosis.
"""
import json
import urllib.request
import boto3
from ops_report import report

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def gj(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode())
    except Exception as e:
        return {"__err__": type(e).__name__}


with report("3359_sovereign_errors") as r:
    r.section("sovereign-stress emitted errors")
    ss = gj("data/sovereign-stress.json")
    if ss.get("__err__"):
        r.fail(f"no output: {ss['__err__']}")
    else:
        errs = ss.get("errors") or []
        r.log(f"total errors: {len(errs)}")
        for e in errs:
            r.log(f"  ✗ {e}")
        r.log(f"sources that DID work: {ss.get('sources')}")
        r.log(f"europe_stress: {ss.get('europe_stress')}")
        sp = ss.get("sovereign_spreads") or {}
        r.log(f"sovereign_spreads keys: {list(sp.keys()) if isinstance(sp, dict) else sp}")

    # direct probe of the ECB CISS endpoint to see the real HTTP failure
    r.section("Direct ECB CISS endpoint probe")
    tests = {
        "CISS EA (CI.CISS.EA)": "https://data-api.ecb.europa.eu/service/data/CISS/D.U2.Z0Z.4F.EC.SS_CI.IDX?format=csvdata&lastNObservations=5",
        "CISS composite alt": "https://data-api.ecb.europa.eu/service/data/CISS/D.U2.Z0Z.4F.EC.SS_CIN.IDX?format=csvdata&lastNObservations=5",
    }
    for name, url in tests.items():
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                body = resp.read().decode()[:300]
                r.ok(f"{name}: HTTP {resp.status} — {body[:150]}")
        except Exception as e:
            r.fail(f"{name}: {type(e).__name__}: {str(e)[:120]}")
