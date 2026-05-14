#!/usr/bin/env python3
"""556 — Force-deploy justhodl-morning-intelligence (auto-deploy missed it).
Then invoke + extract meta_* metrics."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/556_morning_intel_meta_redeploy.json"
NAME = "justhodl-morning-intelligence"
SOURCE = "aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py"

lam = boto3.client("lambda", region_name="us-east-1")


def zip_source(path):
    with open(path, "rb") as f: code = f.read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Check pre-state
    try:
        pre = lam.get_function(FunctionName=NAME)["Configuration"]
        out["pre_last_modified"] = pre.get("LastModified")
        out["pre_state"] = pre.get("State")
    except Exception as e:
        out["pre_err"] = str(e)[:150]

    # Deploy
    zb = zip_source(SOURCE)
    try:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        post = lam.get_function(FunctionName=NAME)["Configuration"]
        out["post_last_modified"] = post.get("LastModified")
        out["deployed"] = "OK"
    except Exception as e:
        out["deploy_err"] = str(e)[:300]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str); return

    _time.sleep(2)

    # Invoke with dry-run to capture metrics output
    try:
        payload = json.dumps({"dry_run": True, "return_metrics": True}).encode("utf-8")
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=payload)
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
            else:
                out["response_excerpt"] = body[:600]
        except Exception as e:
            out["parse_err"] = str(e)[:150]
            out["raw"] = body[:600]
        if resp.get("LogResult") and resp.get("FunctionError"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-2000:]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
