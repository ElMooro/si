"""ops_5567 — harvest etf-derived.json from the paid desk (v1.4.0).

Invokes justhodl-etf-global-desk after the 1.4.0 deploy so data/etf-derived.json
is written: crowding, price-vs-flow, confirmed/disagreed, thematic vs index,
factor dollars, credit/rates/EM stack, BTC/ETH wrapper, bond look-through,
leverage flags, HHI / top-holding hit.
"""
from __future__ import annotations

import json
import time
import urllib.request

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
    print("invoke", FN)
    resp = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    raw = resp["Payload"].read()
    try:
        body = json.loads(raw)
    except Exception:
        body = {"raw": raw[:400].decode("utf-8", "replace")}
    print("lambda", json.dumps(body, default=str)[:1200])
    der = _get("data/etf-derived.json")
    verdicts = der.get("verdicts") or {}
    print("derived version", der.get("version"), "status", der.get("status"))
    print("verdicts", verdicts)
    print("n_by_ticker", len(der.get("by_ticker") or {}))
    print("n_crowding", len(der.get("crowding") or []))
    print("n_confirmed", len(der.get("confirmed") or []))
    print("n_disagreed", len(der.get("disagreed") or []))
    print("n_leverage", len(der.get("leverage") or []))
    print("bond_leaders", len((der.get("bond_lookthrough") or {}).get("leaders") or []))
    spy = (der.get("by_ticker") or {}).get("SPY") or {}
    ibit = (der.get("by_ticker") or {}).get("IBIT") or {}
    print("SPY hhi", spy.get("hhi"), "px_flow", spy.get("px_flow"), "levered", spy.get("levered"))
    print("IBIT flow_5d", ibit.get("flow_5d"), "sleeve", ibit.get("sleeve"))
    print("elapsed", round(time.time() - t0, 1))
    if der.get("_err") or der.get("version") != "1.4.0":
        raise SystemExit("derived harvest missing or old version")


if __name__ == "__main__":
    main()
