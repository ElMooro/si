#!/usr/bin/env python3
"""Step 494 — Verify #18 v3.0 deploy + /anomaly/ page live + homepage wire-in."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/494_anomaly_v3_verify.json"
NAME = "justhodl-tmp-494"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, base64, urllib.request
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    # 1. Verify anomaly-detector Lambda updated
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-anomaly-detector")
        out["lambda"] = {"deployed": True, "modified": cfg["LastModified"][:19],
                          "memory": cfg["MemorySize"], "timeout": cfg["Timeout"]}
    except Exception as e:
        out["lambda"] = {"err": str(e)[:300]}
    # 2. Invoke the Lambda to write v3.0 sidecar
    try:
        resp = lam.invoke(FunctionName="justhodl-anomaly-detector",
                            InvocationType="RequestResponse", LogType="Tail",
                            Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        try:
            parsed = json.loads(body)
            out["invoke_response"] = json.loads(parsed["body"]) if parsed.get("body") else parsed
        except: out["invoke_raw"] = body[:1000]
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        if resp.get("LogResult"):
            tail = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")
            out["log_tail"] = tail[-2000:]
    except Exception as e:
        out["invoke_err"] = str(e)[:400]
    # 3. Read v3.0 sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="signals/anomalies.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body)/1024, 1),
            "version": p.get("version"),
            "generated_at": p.get("generated_at"),
            "macro_stress_score": p.get("macro_stress_score"),
            "stress_interpretation": p.get("stress_interpretation"),
            "detectors_count": p.get("detectors_count"),
            "anomalies_count": p.get("anomalies_count"),
            "high_or_extreme_count": p.get("high_or_extreme_count"),
            "by_severity": p.get("by_severity"),
            "stress_contributions": p.get("stress_contributions"),
            "categories": p.get("categories"),
            "n_metrics": len(p.get("metrics") or {}),
            "first_anomalies": (p.get("anomalies") or [])[:5],
            # Sample 3 new metrics to confirm they came through
            "new_metric_NFCI": (p.get("metrics") or {}).get("NFCI"),
            "new_metric_DFII10": (p.get("metrics") or {}).get("DFII10"),
            "new_metric_SAHMREALTIME": (p.get("metrics") or {}).get("SAHMREALTIME"),
        }
    except Exception as e:
        out["sidecar"] = {"err": str(e)[:300]}
    # 4. Test public pages
    out["pages"] = {}
    for url in ["https://justhodl.ai/", "https://justhodl.ai/anomaly/"]:
        try:
            req = urllib.request.Request(url + "?_t=" + str(int(__import__("time").time())),
                headers={"User-Agent": "JustHodl-Verify/1.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                body = r.read().decode("utf-8", errors="replace")
            o = {"status": r.status, "size_kb": round(len(body)/1024, 1)}
            if url == "https://justhodl.ai/":
                o["has_anomaly_nav"] = \'href="/anomaly/"\' in body
                o["n_anomaly_links"] = body.count(\'/anomaly/\')
                o["has_anomaly_card"] = "ANOMALY & BLACK-SWAN" in body
            else:
                import re
                m = re.search(r"<title>([^<]+)</title>", body)
                o["title"] = m.group(1) if m else None
                o["has_mss_hero"] = "Macro Stress Score" in body
                o["has_detector_grid"] = "detector-grid" in body
                o["has_41_mention"] = "41 institutional" in body
            out["pages"][url] = o
        except Exception as e:
            out["pages"][url] = {"err": str(e)[:200]}
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 200s for deploy...")
    _time.sleep(200)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=300, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    _time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:30000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
