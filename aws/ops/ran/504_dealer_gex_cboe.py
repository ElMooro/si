#!/usr/bin/env python3
"""504 — Redeploy GEX with CBOE source, verify ALL underlyings now produce data."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/504_dealer_gex_cboe.json"
SOURCE = "aws/lambdas/justhodl-dealer-gex/source/lambda_function.py"
NAME = "justhodl-dealer-gex"

lam = boto3.client("lambda", region_name="us-east-1")


def zip_source(path):
    with open(path, "rb") as f: code = f.read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    zb = zip_source(SOURCE)
    try:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        out["deploy"] = "ok"
    except Exception as e:
        out["deploy_err"] = str(e)[:300]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str); return

    _time.sleep(2)
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["invoke_response"] = json.loads(p["body"]) if p.get("body") else p
        except: out["invoke_raw"] = body[:2000]
        if resp.get("LogResult"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8","replace")[-3500:]
    except Exception as e:
        out["invoke_err"] = str(e)[:400]

    # Read sidecar
    try:
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/dealer-gex.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "elapsed": p.get("elapsed_seconds"),
            "market_composite": p.get("market_composite"),
            "squeeze_top_5": (p.get("squeeze_candidates") or [])[:5],
            "underlyings": {
                sym: {
                    "spot": r.get("spot"),
                    "total_gex_b": r.get("total_dealer_gex_billions"),
                    "flip": r.get("zero_gamma_flip_level"),
                    "pct_to_flip": r.get("spot_pct_to_flip"),
                    "regime": r.get("regime"),
                    "trading_bias": r.get("trading_bias"),
                    "pcr_oi": r.get("pcr_oi"),
                    "pcr_vol": r.get("pcr_volume"),
                    "n_contracts": r.get("n_contracts_modeled"),
                    "call_oi": r.get("total_call_oi"),
                    "put_oi": r.get("total_put_oi"),
                    "max_pain": dict(list((r.get("max_pain_by_expiry") or {}).items())[:3]),
                    "zero_dte_pct": (r.get("zero_dte") or {}).get("vol_pct"),
                    "skew_30d": (r.get("iv_skew_30d") or {}).get("skew"),
                    "top_call_wall": (r.get("call_walls_top5") or [{}])[0],
                    "top_put_wall": (r.get("put_walls_top5") or [{}])[0],
                    "top_gamma": (r.get("top_gamma_strikes") or [{}])[0],
                    "err": r.get("err"),
                } for sym, r in (p.get("underlyings") or {}).items()
            },
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
