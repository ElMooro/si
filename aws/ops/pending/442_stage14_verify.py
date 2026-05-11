#!/usr/bin/env python3
"""Step 442 — After deploy completes:
  1. Force-invoke COT v2, verify fresh date in S3
  2. Force-invoke earnings-sentiment Lambda, see how many transcripts scored
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/442_stage14_verify.json"
NAME = "justhodl-tmp-442"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, time, urllib.request
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    # ── 1. COT v2 verify ──
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-cot-tracker")
        out["cot_lambda"] = {"last_modified": cfg["LastModified"], "code_size": cfg["CodeSize"]}
        resp = lam.invoke(
            FunctionName="justhodl-cot-tracker",
            InvocationType="RequestResponse",
            Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        parsed = json.loads(body)
        inner = json.loads(parsed["body"]) if parsed.get("body") else parsed
        out["cot_invoke"] = inner
    except Exception as e:
        out["cot_err"] = str(e)[:300]

    # Read S3 file to see fresh dates + sample contracts
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/cot-latest.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["cot_s3"] = {
            "size_kb": round(len(body)/1024, 1),
            "generated_at": p.get("generated_at"),
            "latest_report_date": p.get("latest_report_date"),
            "n_contracts": len(p.get("contracts") or []),
            "summary": p.get("summary", {}),
        }
        # Sample top 5 contracts (most extreme by z-score)
        out["cot_top5"] = [{
            "sym": c.get("symbol"), "name": c.get("name"), "sec": c.get("sector"),
            "date": c.get("latest_date"),
            "long_pct": c.get("noncomm_long_pct"),
            "short_pct": c.get("noncomm_short_pct"),
            "net_pct_oi": c.get("net_pct_oi"),
            "z": c.get("z_score"),
            "n_weeks": c.get("n_weeks"),
            "signal": c.get("extreme_signal"),
        } for c in (p.get("contracts") or [])[:8]]
    except Exception as e:
        out["cot_s3_err"] = str(e)[:200]

    # ── 2. Earnings sentiment Lambda verify ──
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-earnings-sentiment")
        out["es_lambda"] = {
            "last_modified": cfg["LastModified"],
            "code_size": cfg["CodeSize"],
            "memory": cfg["MemorySize"],
            "timeout": cfg["Timeout"],
            "env_keys": list((cfg.get("Environment") or {}).get("Variables", {}).keys()),
        }
    except Exception as e:
        out["es_lambda_err"] = str(e)[:300]
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # Force-invoke earnings sentiment (synchronous to see result)
    try:
        resp = lam.invoke(
            FunctionName="justhodl-earnings-sentiment",
            InvocationType="RequestResponse",
            Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        parsed = json.loads(body)
        inner = json.loads(parsed["body"]) if parsed.get("body") else parsed
        out["es_invoke"] = inner
    except Exception as e:
        out["es_invoke_err"] = str(e)[:300]

    # Read earnings sentiment S3 file
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/earnings-sentiment.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["es_s3"] = {
            "size_kb": round(len(body)/1024, 1),
            "generated_at": p.get("generated_at"),
            "n_new_this_run": p.get("n_new_this_run"),
            "n_candidates": p.get("n_candidates"),
            "summary": p.get("summary", {}),
        }
        out["es_recent"] = [{
            "sym": r.get("symbol"), "name": r.get("name"),
            "date": r.get("transcript_date"),
            "sentiment": r.get("overall_sentiment"),
            "confidence": r.get("confidence_score"),
            "guidance": r.get("forward_guidance"),
            "summary": (r.get("one_line_summary") or "")[:100],
            "themes": r.get("themes"),
            "tokens_in": r.get("tokens_in"),
            "tokens_out": r.get("tokens_out"),
        } for r in (p.get("transcripts") or [])[:5]]
    except Exception as e:
        out["es_s3_err"] = str(e)[:200]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 90s for deploy-lambdas to publish both Lambdas...")
    time.sleep(90)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=900, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:10000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
