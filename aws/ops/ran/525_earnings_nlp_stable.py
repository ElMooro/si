#!/usr/bin/env python3
"""525 — Redeploy earnings-nlp v1.1.0 (FMP /stable/ endpoints) + invoke + verify."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/525_earnings_nlp_stable.json"
NAME = "justhodl-earnings-nlp"
SOURCE = "aws/lambdas/justhodl-earnings-nlp/source/lambda_function.py"

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


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
        out["update"] = "ok"
    except Exception as e:
        out["update_err"] = str(e)[:300]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str); return

    _time.sleep(3)
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["response"] = json.loads(p["body"]) if p.get("body") else p
        except: out["raw"] = body[:2500]
        if resp.get("LogResult"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8","replace")[-4500:]
    except Exception as e:
        out["invoke_err"] = str(e)[:400]

    # Read sidecar — sample 5 scored tickers
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/earnings-nlp.json")
        body = obj["Body"].read()
        p = json.loads(body)
        by_t = p.get("by_ticker") or {}
        scored = [(k, v) for k, v in by_t.items() if v.get("management_tone") is not None]
        errored = [(k, v) for k, v in by_t.items() if v.get("err")]
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "model": p.get("model"),
            "n_tickers": p.get("n_tickers"),
            "n_with_data": p.get("n_with_data"),
            "n_with_err": p.get("n_with_err"),
            "market_summary": p.get("market_summary"),
            "n_improvers": len((p.get("ranked") or {}).get("biggest_improvers") or []),
            "n_deteriorators": len((p.get("ranked") or {}).get("biggest_deteriorators") or []),
            "sample_scored": [
                {"ticker": k, "period": v.get("period"),
                  "tone": v.get("management_tone"),
                  "guidance": v.get("guidance_direction"),
                  "confidence": v.get("confidence"),
                  "demand": v.get("demand_signal"),
                  "margin": v.get("margin_signal"),
                  "themes": v.get("key_themes"),
                  "summary": (v.get("summary") or "")[:240]}
                for k, v in scored[:5]
            ],
            "first_5_errors": [{"ticker": k, "err": v.get("err")} for k, v in errored[:5]],
            "top_3_bullish": [
                {"ticker": x.get("ticker"), "tone": x.get("tone"), "guidance": x.get("guidance")}
                for x in ((p.get("ranked") or {}).get("most_bullish_tone") or [])[:3]
            ],
            "top_3_bearish": [
                {"ticker": x.get("ticker"), "tone": x.get("tone"), "guidance": x.get("guidance")}
                for x in ((p.get("ranked") or {}).get("most_bearish_tone") or [])[:3]
            ],
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
