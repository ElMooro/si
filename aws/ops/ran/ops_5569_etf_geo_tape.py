"""ops_5569 — harvest etf-derived.json v1.6.0 (USA/DM/EM + country specialists).

Invokes justhodl-etf-global-desk after the 1.6.0 deploy so EWT/EWY/ECH/EPU/EFNL
are on the desk and geo_rotation is written (wrappers vs ports vs exports vs BOP).
"""
from __future__ import annotations

import json
import time

import boto3
from botocore.config import Config

FN = "justhodl-etf-global-desk"
BUCKET = "justhodl-dashboard-live"
LAM = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=900, retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")


def _get(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        return {"_err": str(e)}


def main():
    t0 = time.time()
    last = None
    for i in range(40):
        cfg = LAM.get_function_configuration(FunctionName=FN)
        state = cfg.get("State")
        upd = cfg.get("LastUpdateStatus")
        print("try", i, "state", state, "update", upd)
        if state == "Active" and upd in (None, "Successful"):
            print("invoke", FN)
            resp = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
            raw = resp["Payload"].read()
            try:
                last = json.loads(raw)
            except Exception:
                last = {"raw": raw[:400].decode("utf-8", "replace")}
            print("lambda", json.dumps(last, default=str)[:900])
            der = _get("data/etf-derived.json")
            ver = str(der.get("version") or "")
            print("derived version", ver, "status", der.get("status"))
            print("verdicts", der.get("verdicts"))
            geo = der.get("geo_rotation") or {}
            print("geo", geo.get("verdict"), geo.get("specialist"), "usa", geo.get("usa_5d"), "dm", geo.get("dm_5d"), "em", geo.get("em_5d"))
            by = der.get("by_ticker") or {}
            for t in ("EWT", "EWY", "ECH", "EPU", "EFNL", "EWJ", "EFA", "EEM"):
                r = by.get(t) or {}
                print(t, "flow_5d", r.get("flow_5d"), "sleeve", r.get("sleeve"), "ok", r.get("ok"))
            if ver >= "1.6.0" and geo.get("verdict") and "EWT" in by:
                print("elapsed", round(time.time() - t0, 1))
                return
        time.sleep(8)
    raise SystemExit("etf-derived 1.6.0 geo tape never landed")


if __name__ == "__main__":
    main()
