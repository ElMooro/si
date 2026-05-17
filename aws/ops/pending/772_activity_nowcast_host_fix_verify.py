"""ops/772 — redeploy activity-nowcast (FRED host fix) + final verify (#3)."""
import json, os, io, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

FN = "justhodl-activity-nowcast"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
report = {"ops": 772, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Activity Nowcast redeploy (FRED host fix) + final verify"}

try:
    code = open(SRC, encoding="utf-8").read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", code)
    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
    report["host_in_code"] = ("api.stlouisfed.org" in code
                              and "api.fred.stlouisfed.org" not in code)
    report["update"] = "ok"
except Exception as e:
    report["update"] = f"{type(e).__name__}: {str(e)[:240]}"

for _ in range(30):
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
    except Exception:
        pass
    time.sleep(3)

try:
    lam.update_function_configuration(FunctionName=FN, Timeout=90)
    for _ in range(20):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    report["timeout_set"] = c.get("Timeout")
except Exception as e:
    report["timeout_err"] = str(e)[:160]

an = {}
for attempt in range(3):
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=b"{}")
        payload = json.loads(r["Payload"].read() or b"{}")
        report[f"invoke_{attempt+1}"] = {"status": r.get("StatusCode"),
                                         "fn_error": r.get("FunctionError"),
                                         "body": payload.get("body")}
    except Exception as e:
        report[f"invoke_{attempt+1}"] = {"err": str(e)[:200]}
    try:
        an = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                        Key="data/activity-nowcast.json")["Body"].read())
    except Exception as e:
        report["read_err"] = str(e)[:200]
    if an.get("ok") is True:
        break
    time.sleep(6)

comps = an.get("components", []) or []
report["output"] = {"ok": an.get("ok"), "activity_index": an.get("activity_index"),
                    "activity_z": an.get("activity_z"), "regime": an.get("regime"),
                    "momentum": an.get("momentum"), "headline": an.get("headline"),
                    "n_ok": an.get("n_ok"), "errors": an.get("errors")}
report["divergence"] = an.get("divergence")
report["components"] = [
    {"series": c.get("series"), "name": c.get("name"), "latest": c.get("latest"),
     "latest_date": c.get("latest_date"), "level_z": c.get("level_z"),
     "momentum_z": c.get("momentum_z"), "contribution": c.get("contribution"),
     "signal_label": c.get("signal_label")} for c in comps]

idx = an.get("activity_index")
checks = {
    "host_corrected": report.get("host_in_code") is True,
    "update_ok": report.get("update") == "ok",
    "no_fn_error": not any(report.get(f"invoke_{i}", {}).get("fn_error")
                           for i in (1, 2, 3) if report.get(f"invoke_{i}")),
    "output_ok": an.get("ok") is True,
    "index_in_range": isinstance(idx, (int, float)) and 0 <= idx <= 100,
    "has_regime": an.get("regime") in ("ACCELERATING", "EXPANDING", "STEADY",
                                       "SLOWING", "CONTRACTING"),
    "all_series_ok": (an.get("n_ok") or 0) >= 5,
    "real_fred_data": len(comps) >= 5
                      and all(c.get("latest") is not None for c in comps),
    "divergence_works": isinstance(an.get("divergence"), dict)
                        and an.get("divergence", {}).get("available") is True,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "ACTIVITY NOWCAST LIVE & VERIFIED — FRED host corrected, all "
    "high-frequency series resolving on real data, 0-100 activity index + "
    "regime + divergence flag all computing. Roadmap #3 COMPLETE."
    if report["all_pass"] else "REVIEW — see checks[]/output/components")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/772_activity_nowcast_host_fix_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/772_activity_nowcast_host_fix_verify.json")
