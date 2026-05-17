"""ops/787 — redeploy refined justhodl-yen-carry + verify the positioning leg.

ops 785 shipped the engine but the CFTC positioning leg fell back to
graceful-degrade. The jpy_positioning() parser was rewritten to read the
real 6J / Japanese-Yen structure from data/cftc-all-cache.json and z-score
the net speculator position. This redeploys and verifies that the
positioning block and the crowded_positioning unwind-risk component now
resolve with live data.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

FN = "justhodl-yen-carry"
BASE = f"aws/lambdas/{FN}"
BUCKET = "justhodl-dashboard-live"
report = {"ops": 787, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Redeploy refined yen-carry + verify positioning leg"}

code = open(f"{BASE}/source/lambda_function.py", encoding="utf-8").read()
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", code)

try:
    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
    report["deploy"] = "updated"
except Exception as e:
    report["deploy"] = f"ERROR {type(e).__name__}: {str(e)[:200]}"

for _ in range(40):
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        if (c.get("State") == "Active"
                and c.get("LastUpdateStatus") == "Successful"):
            break
    except Exception:
        pass
    time.sleep(3)

try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": json.loads(r["Payload"].read() or b"{}")}
except Exception as e:
    report["invoke"] = {"err": str(e)[:240]}

time.sleep(3)
out = {}
try:
    out = json.loads(s3.get_object(Bucket=BUCKET,
                     Key="data/yen-carry.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:200]

pos = out.get("positioning") or {}
comps = out.get("unwind_risk_components") or {}
fx = out.get("fx_detonator") or {}
report["output"] = {
    "ok": out.get("ok"),
    "headline": out.get("headline"),
    "carry_regime": out.get("carry_regime"),
    "unwind_risk_score": out.get("unwind_risk_score"),
    "unwind_risk_label": out.get("unwind_risk_label"),
    "unwind_risk_components": comps,
    "positioning": pos,
    "fx_vol_regime": fx.get("vol_regime"),
    "fx_realized_vol_20d": fx.get("realized_vol_20d_pct"),
    "errors": out.get("errors"),
}

pos_resolved = isinstance(pos.get("net_speculator"), (int, float))
checks = {
    "deploy_ok": "ERROR" not in str(report.get("deploy", "")),
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": out.get("ok") is True,
    "positioning_resolved": pos_resolved,
    "positioning_zscore_present": pos.get("net_zscore_vs_history") is not None,
    "crowded_component_in_score": "crowded_positioning" in comps,
    "all_five_legs": ("crowded_positioning" in comps
                      and "fx_vol" in comps and "boj_hawkish" in comps),
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "POSITIONING LEG LIVE — yen-carry now reads real CFTC JPY positioning: "
    f"net speculator {pos.get('net_speculator')} "
    f"({'net short yen' if pos.get('crowded_short') else 'net long yen'}), "
    f"z-score {pos.get('net_zscore_vs_history')}sd vs "
    f"{pos.get('history_weeks')}w. All five legs now feed the unwind-risk "
    f"score ({out.get('unwind_risk_score')}/100, {out.get('unwind_risk_label')})."
    if report["all_pass"] else
    "REVIEW — positioning leg did not fully resolve; see output.positioning")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/787_yen_carry_positioning.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/787_yen_carry_positioning.json")
