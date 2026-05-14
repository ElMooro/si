#!/usr/bin/env python3
"""553 — Deploy regime-composite v1.0.1 (DIX parallel-arrays + insider thresholds)
+ invoke + verify DIX_NEUTRAL/HIGH/LOW and INSIDER_BUYING_PRESENT now appear."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/553_regime_composite_v101.json"
NAME = "justhodl-regime-composite"
SOURCE = "aws/lambdas/justhodl-regime-composite/source/lambda_function.py"

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
        out["update"] = "OK"
    except Exception as e:
        out["update_err"] = str(e)[:300]
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
            out["response_body"] = json.loads(p["body"]) if p.get("body") else p
        except Exception: pass
        if resp.get("LogResult"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-2000:]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]

    _time.sleep(2)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/regime-composite.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "meta_regime": p.get("meta_regime"),
            "meta_class": p.get("meta_class"),
            "meta_narrative": p.get("meta_narrative"),
            "composite_score": p.get("composite_score"),
            "n_modules_with_data": p.get("n_modules_with_data"),
            "n_modules_missing": p.get("n_modules_missing"),
            "duration_s": p.get("duration_s"),
            "dimensions": p.get("dimensions"),
        }
        # Pull out DIX + insider specifically
        for m in (p.get("modules") or []):
            if "DIX" in m.get("label", ""):
                out["dix_module"] = m
            if "Insider" in m.get("label", ""):
                out["insider_module"] = m
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
