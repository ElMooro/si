#!/usr/bin/env python3
"""Step 341 — Re-invoke implied-prob 3x with delays + check QQQ IV + Fed stance.

If still failing after 3 invocations, those FRED series may have known issues
(VXNCLS discontinuation, DGS3MO publish-lag) — we'll patch.
"""
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

REPORT = "aws/ops/reports/341_reverify.json"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
FRED_KEY = "2f057499936072679d8843d7fce99989"


def http_get_json(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "verify/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        return {"_err": str(e)[:200]}


def check_fred_series(series_id):
    """Direct check: does this FRED series return data?"""
    qs = urllib.parse.urlencode({
        "series_id": series_id, "api_key": FRED_KEY,
        "file_type": "json", "limit": 5, "sort_order": "desc",
    })
    d = http_get_json(f"https://api.stlouisfed.org/fred/series/observations?{qs}")
    if d.get("_err"):
        return {"err": d["_err"]}
    obs = d.get("observations") or []
    valid = [o for o in obs if o.get("value") not in (None, ".")]
    return {
        "n_returned": len(obs),
        "n_valid": len(valid),
        "latest": valid[0] if valid else None,
    }


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "iterations": [], "fred_direct": {}}

    # 1. Direct FRED check first — does VXNCLS, DGS3MO, DGS6MO actually return data?
    for sid in ("VXNCLS", "VIXCLS", "RVXCLS", "VXDCLS", "GVZCLS",
                 "DGS3MO", "DGS6MO", "DGS1", "DGS1MO",
                 "DFEDTARU", "DFEDTARL", "FEDFUNDS"):
        out["fred_direct"][sid] = check_fred_series(sid)
        time.sleep(0.3)

    # 2. Force-invoke implied-prob 3x with 5s gaps
    for i in range(3):
        print(f"\n── Invocation #{i+1} ──")
        started = time.time()
        resp = lam.invoke(FunctionName="justhodl-implied-prob",
                           InvocationType="RequestResponse", Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        try:
            inv_body = json.loads(body)
        except Exception:
            inv_body = {"raw": body[:200]}
        time.sleep(2)
        try:
            output = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                               Key="data/implied-prob.json")["Body"].read())
            iter_data = {
                "iter": i + 1,
                "duration_s": round(time.time() - started, 1),
                "invoke": inv_body,
                "spy_iv_30d": (output.get("spy") or {}).get("iv_30d"),
                "spy_iv_90d": (output.get("spy") or {}).get("iv_90d"),
                "qqq_iv_30d": (output.get("qqq") or {}).get("iv_30d"),
                "qqq_iv_90d": (output.get("qqq") or {}).get("iv_90d"),
                "btc_iv_30d": (output.get("btc") or {}).get("iv_30d"),
                "fed_stance": (output.get("fed") or {}).get("near_term_stance"),
                "fed_bill_3m": (output.get("fed") or {}).get("bill_3m"),
                "fed_bill_6m": (output.get("fed") or {}).get("bill_6m"),
                "fed_implied_3m_bp": (output.get("fed") or {}).get("implied_3m_change_bp"),
                "rec_label": (output.get("recession") or {}).get("composite_label"),
            }
        except Exception as e:
            iter_data = {"iter": i + 1, "err": str(e)[:200]}
        out["iterations"].append(iter_data)
        time.sleep(5)

    # 3. Final state from S3
    output = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                       Key="data/implied-prob.json")["Body"].read())
    out["final_state"] = {
        "spy": output.get("spy"),
        "qqq": output.get("qqq"),
        "btc": output.get("btc"),
        "fed": output.get("fed"),
    }

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:6000])


if __name__ == "__main__":
    main()
