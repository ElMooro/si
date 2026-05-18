"""ops/809 — redeploy + verify micro-cap targets now populate.

The bagger multibagger multiple was nested at twin_engine.yr10.
with_rerating_x; the engine now reads it. Confirms micro-cap rockets carry
a 10-year bagger price target and a thesis.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-opportunity-screener"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"

report = {"ops": 809, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Verify micro-cap bagger price targets populate"}

buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
try:
    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
    for _ in range(40):
        if lam.get_function_configuration(
                FunctionName=FN).get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    report["deploy"] = "updated"
except Exception as e:
    report["deploy"] = f"ERROR {type(e).__name__}: {str(e)[:200]}"

time.sleep(3)
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError")}
except Exception as e:
    report["invoke"] = {"error": str(e)[:200]}

time.sleep(3)
ob = {}
try:
    ob = json.loads(s3.get_object(
        Bucket=BUCKET,
        Key="screener/opportunity-screener.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:200]

micro = ob.get("microcap_rockets") or []
mt = sum(1 for b in micro if b.get("price_target") is not None)
report["microcaps"] = {
    "n": len(micro), "with_target": mt,
    "sample": [{"sym": b.get("symbol"), "price": b.get("price"),
                "target": b.get("price_target"),
                "upside": b.get("upside_pct"),
                "horizon": b.get("target_horizon"),
                "why": (b.get("why") or "")[:160]} for b in micro[:4]],
}
checks = {
    "deploy_ok": report.get("deploy") == "updated",
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "microcaps_present": len(micro) >= 5,
    "microcaps_have_targets": mt >= max(5, int(0.6 * len(micro))),
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"MICRO-CAP TARGETS LIVE — {mt}/{len(micro)} micro-cap rockets now "
    "carry a 10-year bagger multibagger price target + thesis. Boom Board "
    "complete."
    if report["all_pass"] else "REVIEW — see checks[]/microcaps")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/809_microcap_targets.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/809_microcap_targets.json")
