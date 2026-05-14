#!/usr/bin/env python3
"""557 — Re-deploy morning-intel (alerts fix) + verify meta_* metrics."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/557_morning_intel_alerts_fix.json"
NAME = "justhodl-morning-intelligence"
SOURCE = "aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py"

lam = boto3.client("lambda", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    with open(SOURCE, "rb") as f: code = f.read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", code)
    zb = buf.getvalue()

    try:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        out["deployed"] = "OK"
    except Exception as e:
        out["deploy_err"] = str(e)[:200]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str); return

    _time.sleep(2)

    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail",
                           Payload=json.dumps({"dry_run": True, "return_metrics": True}).encode("utf-8"))
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        out["response_size"] = len(body)
        try:
            p = json.loads(body)
            inner = p
            if isinstance(p, dict) and "body" in p:
                try: inner = json.loads(p["body"])
                except: pass
            metrics = (inner or {}).get("metrics") if isinstance(inner, dict) else None
            if metrics and isinstance(metrics, dict):
                meta_keys = {k: v for k, v in metrics.items() if k.startswith("meta_")}
                out["meta_keys_found"] = meta_keys
                out["n_meta_keys"] = len(meta_keys)
                out["n_total_metrics"] = len(metrics)
                # Surface a few other key metrics for sanity
                out["khalid_regime"] = metrics.get("khalid_regime")
                out["edge_regime"] = metrics.get("edge_regime")
                out["fed_regime"] = metrics.get("fed_regime")
                out["lce_regime"] = metrics.get("lce_regime")
            else:
                out["response_excerpt"] = body[:600]
        except Exception as e:
            out["parse_err"] = str(e)[:150]
            out["raw"] = body[:600]
        if resp.get("LogResult") and resp.get("FunctionError"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-2500:]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
