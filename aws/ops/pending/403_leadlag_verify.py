#!/usr/bin/env python3
"""Step 403 — Verify lead/lag ranking deployed end-to-end."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
REPORT = "aws/ops/reports/403_leadlag_verify.json"
NAME = "justhodl-tmp-leadlag"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request, time
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3  = boto3.client("s3",     region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
TARGET = "justhodl-global-business-cycle"

def fetch(url, t=15):
    req = urllib.request.Request(url, headers={"User-Agent":"JH/1.0"})
    with urllib.request.urlopen(req, timeout=t) as r: return r.read().decode(), r.status

def lambda_handler(event, context):
    out = {}
    cfg = lam.get_function_configuration(FunctionName=TARGET)
    out["lambda_last_modified"] = cfg["LastModified"]
    out["code_size"] = cfg["CodeSize"]

    resp = lam.invoke(FunctionName=TARGET, InvocationType="RequestResponse", Payload=b"{}")
    out["invoke_body"] = resp["Payload"].read().decode("utf-8")[:300]

    time.sleep(4)

    # History JSON
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/global-business-cycle-history.json")
        h = json.loads(obj["Body"].read())
        ranking = h.get("lead_lag_ranking", [])
        # Trim each entry's curve for report (just keep top-level fields)
        slim_ranking = []
        for r in ranking:
            slim_ranking.append({
                "iso3": r["iso3"],
                "country_name": r["country_name"],
                "region": r["region"],
                "gdp_weight": r["gdp_weight"],
                "peak_lag_weeks": r["peak_lag_weeks"],
                "peak_correlation": r["peak_correlation"],
                "n_overlap": r["n_overlap"],
                "boundary_flag": r.get("boundary_flag", False),
                "curve_n_points": len(r.get("curve_lags", [])),
            })
        # Sample one curve in full for shape check
        sample_curve = None
        if ranking:
            r0 = ranking[0]
            sample_curve = {
                "iso3": r0["iso3"],
                "curve_lags_first5": r0.get("curve_lags", [])[:5],
                "curve_corrs_first5": r0.get("curve_corrs", [])[:5],
                "curve_lags_last5": r0.get("curve_lags", [])[-5:],
                "curve_corrs_last5": r0.get("curve_corrs", [])[-5:],
            }
        out["history"] = {
            "schema": h.get("schema_version"),
            "lead_lag_count": h.get("lead_lag_count"),
            "transitions_count": h.get("transitions_count"),
            "ranking": slim_ranking,
            "sample_curve": sample_curve,
        }
    except Exception as e:
        out["history"] = {"error": str(e)[:300]}

    # Page check
    try:
        page, status = fetch("https://justhodl.ai/global-cycle/?cb=" + str(int(time.time())))
        out["page"] = {
            "status": status,
            "size": len(page),
            "has_renderLeadLagRanking": "renderLeadLagRanking" in page,
            "has_drawCorrelationCurve": "drawCorrelationCurve" in page,
            "has_leadLagTable": "leadLagTable" in page,
            "has_corrCurveChart": "corrCurveChart" in page,
            "has_section_title": "Country Lead/Lag Ranking" in page,
            "has_caveat": "self-influence" in page,
        }
    except Exception as e:
        out["page"] = {"error": str(e)[:200]}

    # CloudWatch
    try:
        lg = f"/aws/lambda/{TARGET}"
        streams = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime",
                                              descending=True, limit=1)
        if streams.get("logStreams"):
            stream = streams["logStreams"][0]["logStreamName"]
            ev = logs.get_log_events(logGroupName=lg, logStreamName=stream,
                                      startFromHead=False, limit=80)
            lines = [e["message"].strip() for e in ev.get("events", [])]
            out["log_leadlag"] = [l for l in lines if "lead/lag" in l.lower() or "[gbc-history]" in l][-20:]
    except Exception as e:
        out["log_err"] = str(e)[:200]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
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
        out["raw"] = body[:8000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
