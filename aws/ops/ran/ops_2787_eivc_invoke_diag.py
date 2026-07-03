"""ops 2787 — test-invoke earnings-iv-crush, capture tail log + whether it writes fresh output."""
import os, json, base64, time
from datetime import datetime, timezone
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1"); B = "justhodl-dashboard-live"
now = datetime.now(timezone.utc); fn = "justhodl-earnings-iv-crush"
R = {"ops": 2787, "ts": now.isoformat()}
# freshness before
def age(k):
    try:
        h = s3.head_object(Bucket=B, Key=k); return (datetime.now(timezone.utc)-h["LastModified"].astimezone(timezone.utc)).total_seconds()/3600
    except Exception as e: return None
R["age_before_h"] = round(age("data/earnings-iv-crush.json") or -1, 1)
print("earnings-iv-crush.json age BEFORE: %.1fh" % R["age_before_h"])
print("invoking %s (RequestResponse, tail logs)…" % fn)
try:
    resp = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    R["status_code"] = resp["StatusCode"]; R["function_error"] = resp.get("FunctionError")
    tail = base64.b64decode(resp.get("LogResult","")).decode("utf-8","ignore")
    payload = resp["Payload"].read().decode("utf-8","ignore")[:800]
    R["payload"] = payload
    print("StatusCode:", resp["StatusCode"], "| FunctionError:", resp.get("FunctionError"))
    print("── payload (first 800c) ──"); print(payload)
    print("── tail log (last lines) ──")
    for ln in tail.strip().splitlines()[-25:]: print("  ", ln)
    R["tail_last"] = tail.strip().splitlines()[-25:]
except Exception as e:
    R["invoke_err"] = str(e)[:200]; print("INVOKE ERR:", str(e)[:200])
time.sleep(4)
R["age_after_h"] = round(age("data/earnings-iv-crush.json") or -1, 1)
print("earnings-iv-crush.json age AFTER: %.1fh %s" % (R["age_after_h"], "(WROTE FRESH!)" if R["age_after_h"]>=0 and R["age_after_h"]<1 else "(still stale — did NOT write)"))
R["wrote_fresh"] = (R["age_after_h"] is not None and 0 <= R["age_after_h"] < 1)
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2787_eivc_invoke_diag.json","w"), indent=1, default=str)
print("OPS 2787 COMPLETE · wrote_fresh=%s" % R["wrote_fresh"])
