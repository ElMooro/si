#!/usr/bin/env python3
"""Step 404 — Verify phase-conditional return distributions deployed."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
REPORT = "aws/ops/reports/404_pcr_verify.json"
NAME = "justhodl-tmp-pcr"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request, time
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3  = boto3.client("s3",     region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")

def fetch(url, t=15):
    req = urllib.request.Request(url, headers={"User-Agent":"JH/1.0"})
    with urllib.request.urlopen(req, timeout=t) as r: return r.read().decode(), r.status

def lambda_handler(event, context):
    out = {}
    cfg = lam.get_function_configuration(FunctionName="justhodl-global-business-cycle")
    out["lambda_last_modified"] = cfg["LastModified"]
    out["code_size"] = cfg["CodeSize"]
    resp = lam.invoke(FunctionName="justhodl-global-business-cycle",
                       InvocationType="RequestResponse", Payload=b"{}")
    out["invoke_body"] = resp["Payload"].read().decode("utf-8")[:300]

    time.sleep(5)

    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/global-business-cycle-history.json")
        body = obj["Body"].read()
        h = json.loads(body)
        out["size_bytes"] = len(body)
        out["schema"] = h.get("schema_version")
        summary = h.get("phase_summary", {})
        # Strip out full stats for report compactness
        out["phase_summary_top5"] = {}
        for phase, arr in summary.items():
            out["phase_summary_top5"][phase] = [{
                "iso3": r["iso3"], "name": r["country_name"], "region": r["region"],
                "mean": r["mean"], "median": r["median"],
                "hit_rate": r["hit_rate"], "n": r["n"],
                "p10": r["p10"], "p90": r["p90"],
            } for r in arr[:8]]
            out["phase_summary_top5"][phase + "_bottom"] = [{
                "iso3": r["iso3"], "name": r["country_name"],
                "mean": r["mean"], "hit_rate": r["hit_rate"], "n": r["n"],
            } for r in arr[-3:]]
        # Spot-check one country's full breakdown (USA)
        usa = (h.get("by_country") or {}).get("USA", {}).get("phase_returns") or {}
        usa_summary = {}
        for phase, stats in usa.items():
            usa_summary[phase] = {
                "n": stats.get("n"),
                "mean": stats.get("mean"),
                "median": stats.get("median"),
                "hit_rate": stats.get("hit_rate"),
                "p10": stats.get("p10"),
                "p90": stats.get("p90"),
                "bins_n": len(stats.get("bins", [])),
            }
        out["usa_phase_returns"] = usa_summary
        # Also CHN as second sanity check
        chn = (h.get("by_country") or {}).get("CHN", {}).get("phase_returns") or {}
        out["chn_phase_returns"] = {phase: {"n": s.get("n"), "mean": s.get("mean"),
                                              "hit_rate": s.get("hit_rate")}
                                       for phase, s in chn.items()}
    except Exception as e:
        out["history_err"] = str(e)[:300]

    try:
        page, status = fetch("https://justhodl.ai/global-cycle/?cb=" + str(int(time.time())))
        out["page"] = {
            "status": status, "size": len(page),
            "has_renderPhaseConditional": "renderPhaseConditional" in page,
            "has_drawPCRHistogram": "drawPCRHistogram" in page,
            "has_pcr_summary": "pcrSummaryGrid" in page,
            "has_pcr_country_select": "pcrCountrySelect" in page,
            "has_section_title": "Phase-Conditional Forward Returns" in page,
        }
    except Exception as e:
        out["page_err"] = str(e)[:200]

    try:
        lg = "/aws/lambda/justhodl-global-business-cycle"
        streams = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime",
                                              descending=True, limit=1)
        if streams.get("logStreams"):
            stream = streams["logStreams"][0]["logStreamName"]
            ev = logs.get_log_events(logGroupName=lg, logStreamName=stream,
                                      startFromHead=False, limit=80)
            lines = [e["message"].strip() for e in ev.get("events", [])]
            out["log_pcr"] = [l for l in lines if "phase" in l.lower() and ("[gbc-history]" in l or "top →" in l)][-15:]
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
        out["raw"] = body[:10000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
