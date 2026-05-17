"""ops/741 — verify justhodl-signal-board end-to-end."""
import json, os, time, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config

BUCKET = "justhodl-dashboard-live"
cfg = Config(read_timeout=140, connect_timeout=20, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1")

report = {"ops": 741, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "signal-board verify"}

try:
    r = lam.invoke(FunctionName="justhodl-signal-board",
                   InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", "replace") if r.get("Payload") else ""
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"), "response": body[:300]}
except Exception as e:
    report["invoke"] = {"status": "error", "err": str(e)[:200]}

time.sleep(4)
d = None
try:
    d = json.loads(s3.get_object(Bucket=BUCKET,
                                 Key="data/signal-board.json")["Body"].read())
except Exception as e:
    report["sidecar_error"] = str(e)[:180]

if d:
    report["summary"] = {
        "schema": d.get("schema_version"),
        "composite_posture": d.get("composite_posture"),
        "composite_signal": d.get("composite_signal"),
        "n_engines": d.get("n_engines"), "n_live": d.get("n_live"),
        "n_stale": d.get("n_stale"),
        "categories": d.get("categories")}
    report["engines"] = [{"engine": e.get("engine"),
                          "signal_label": e.get("signal_label"),
                          "read": e.get("read"), "stale": e.get("stale")}
                         for e in d.get("engines", [])]

try:
    req = urllib.request.Request("https://justhodl.ai/signal-board.html",
                                 headers={"User-Agent": "justhodl-ops/741"})
    with urllib.request.urlopen(req, timeout=25) as resp:
        report["frontend"] = {"status": resp.status,
                              "marker": "Signal Board" in
                              resp.read().decode("utf-8", "replace")}
except Exception as e:
    report["frontend"] = {"status": "error", "err": str(e)[:140]}

checks = {
    "invoke_ok": report["invoke"].get("status") == 200
                 and report["invoke"].get("fn_error") is None,
    "sidecar_valid": bool(d) and d.get("schema_version") == "1.0",
    "engines_aggregated": bool(d) and (d.get("n_engines") or 0) >= 7,
    "composite_computed": bool(d) and d.get("composite_posture") not in
                          (None, "NO SIGNAL"),
    "frontend_live": isinstance(report.get("frontend"), dict)
                     and report["frontend"].get("marker") is True,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = ("VERIFIED — Signal Board live: 7 engines aggregated "
                     "into a composite cross-asset posture"
                     if report["all_pass"] else "REVIEW — see checks")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/741_signal_board_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/741_signal_board_verify.json")
