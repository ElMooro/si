"""ops_5568 — harvest etf-derived.json v1.5.1 (cross-asset risk/fear tape).

Invokes justhodl-etf-global-desk after the 1.5.1 deploy so data/etf-derived.json
has mega/large/small, duration vs T-bills vs junk vs fallen, wrapper cashing
in/out, levered sentiment, and SPY/VOO/IVV plumbing. Harvests TQQQ/SQQQ/UPRO.
"""
from __future__ import annotations

import json
import time

import boto3
from botocore.config import Config

FN = "justhodl-etf-global-desk"
BUCKET = "justhodl-dashboard-live"
LAM = boto3.client(
    "lambda",
    region_name="us-east-1",
    config=Config(read_timeout=900, retries={"max_attempts": 0}),
)
S3 = boto3.client("s3", region_name="us-east-1")
NEED = "1.5.1"
LEVERED = ("TQQQ", "SQQQ", "UPRO", "SOXL", "TNA", "SH", "UVXY")


def _get(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        return {"_err": str(e)}


def main():
    t0 = time.time()
    print("wait for", FN, "Active +", NEED)
    doc = None
    for i in range(40):
        try:
            c = LAM.get_function_configuration(FunctionName=FN)
            print("lambda", c.get("LastUpdateStatus"), c.get("State"), "timeout", c.get("Timeout"))
            if c.get("LastUpdateStatus") in (None, "Successful") and c.get("State") == "Active":
                print("invoke", FN, "try", i)
                resp = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
                raw = resp["Payload"].read()
                try:
                    body = json.loads(raw)
                except Exception:
                    body = {"raw": raw[:400].decode("utf-8", "replace")}
                print("lambda body", json.dumps(body, default=str)[:1500])
                der = _get("data/etf-derived.json")
                print("derived version", der.get("version"), "status", der.get("status"))
                if str(der.get("version") or "") >= NEED and not der.get("_err"):
                    doc = der
                    break
        except Exception as e:
            print("wait err", type(e).__name__, str(e)[:240])
        time.sleep(8)
    if not doc:
        raise SystemExit("derived harvest missing or old version")
    v = doc.get("verdicts") or {}
    x = doc.get("cross_asset") or {}
    w = doc.get("wrapper_intensity") or {}
    L = doc.get("levered_sentiment") or {}
    print("verdicts", v)
    print("risk", v.get("risk"), v.get("fear_greed"), v.get("rotation"), v.get("size"))
    print("equity_5d", x.get("equity_5d"), "gov_5d", x.get("gov_5d"), "credit_5d", x.get("credit_5d"))
    print("mega", (x.get("mega") or {}).get("flow_5d"), "large", (x.get("large") or {}).get("flow_5d"),
          "small", (x.get("small") or {}).get("flow_5d"))
    print("duration", (x.get("duration") or {}).get("flow_5d"), "t_bills", (x.get("t_bills") or {}).get("flow_5d"))
    print("hy", (x.get("hy") or {}).get("flow_5d"), "fallen", (x.get("fallen") or {}).get("flow_5d"))
    print("spx plumbing", (x.get("spx_family") or {}).get("plumbing"), (x.get("spx_family") or {}).get("net"))
    print("wrapper", w.get("verdict"), "in", w.get("gross_in_1d"), "out", w.get("gross_out_1d"))
    print("heavy", [(h.get("t"), h.get("flow_1d")) for h in (w.get("heavy_1d") or [])[:8]])
    print("levered", L.get("verdict"), "bull", (L.get("bull") or {}).get("flow_5d"),
          "bear", (L.get("bear") or {}).get("flow_5d"))
    by = doc.get("by_ticker") or {}
    for t in LEVERED:
        row = by.get(t) or {}
        print(t, "flow_5d", row.get("flow_5d"), "levered", row.get("levered"), "in_by", t in by)
    desk = _get("data/etf-desk.json")
    print("desk version", desk.get("version"), "n", desk.get("n"), "n_ok", desk.get("n_ok"))
    print("elapsed", round(time.time() - t0, 1))
    if str(desk.get("version") or "") < NEED:
        raise SystemExit("desk version old")
    missing = [t for t in LEVERED if t not in (desk.get("by_etf") or {})]
    if missing:
        print("WARN levered not on desk", missing)
    print("OK", NEED, v.get("risk"), v.get("fear_greed"), v.get("wrapper"), v.get("levered"))


if __name__ == "__main__":
    main()
