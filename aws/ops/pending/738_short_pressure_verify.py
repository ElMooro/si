"""ops/738 — verify justhodl-short-pressure end-to-end."""
import json, os, time, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config

BUCKET = "justhodl-dashboard-live"
cfg = Config(read_timeout=320, connect_timeout=20, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1")

report = {"ops": 738, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "short-pressure engine verify"}

try:
    r = lam.invoke(FunctionName="justhodl-short-pressure",
                   InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", "replace") if r.get("Payload") else ""
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"), "response": body[:300]}
except Exception as e:
    report["invoke"] = {"status": "error", "err": str(e)[:200]}

time.sleep(5)
d = None
try:
    d = json.loads(s3.get_object(Bucket=BUCKET,
                                 Key="data/short-pressure.json")["Body"].read())
except Exception as e:
    report["sidecar_error"] = str(e)[:180]

if d:
    ns = d.get("names", [])
    with_z = [n for n in ns if n.get("z_score") is not None]
    report["summary"] = {
        "schema": d.get("schema_version"), "n_covered": d.get("n_covered"),
        "n_failed": d.get("n_failed"),
        "n_pressure_building": d.get("n_pressure_building"),
        "n_shorts_covering": d.get("n_shorts_covering"),
        "n_with_z": len(with_z)}
    report["sample"] = [{k: n.get(k) for k in
                         ("ticker", "short_ratio_latest", "baseline_20d",
                          "z_score", "state")} for n in ns[:5]]

try:
    req = urllib.request.Request("https://justhodl.ai/short-pressure.html",
                                 headers={"User-Agent": "justhodl-ops/738"})
    with urllib.request.urlopen(req, timeout=25) as resp:
        report["frontend"] = {"status": resp.status,
                              "marker": "Short Pressure" in
                              resp.read().decode("utf-8", "replace")}
except Exception as e:
    report["frontend"] = {"status": "error", "err": str(e)[:140]}

checks = {
    "invoke_ok": report["invoke"].get("status") == 200
                 and report["invoke"].get("fn_error") is None,
    "data_ok": bool(d) and d.get("schema_version") == "1.0"
               and (d.get("n_covered") or 0) >= 5,
    "z_populated": bool(d) and len([n for n in d.get("names", [])
                                    if n.get("z_score") is not None]) >= 3,
    "frontend_live": isinstance(report.get("frontend"), dict)
                     and report["frontend"].get("marker") is True,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = ("VERIFIED — Short Pressure engine live and populated"
                     if report["all_pass"] else "REVIEW — see checks")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/738_short_pressure_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/738_short_pressure_verify.json")
