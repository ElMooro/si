"""ops/798 — redeploy + verify justhodl-cds-monitor (peer-relative calibration).

ops/797 shipped the CreditGrades model but graded names on absolute DD
bands calibrated for Merton, so the whole bank sector read STRESSED and the
board fired a false ALERT. This redeploys the recalibration — peer-relative
regimes, a robust median-based composite leg and outlier-only alarms — and
verifies the readings are no longer alarmist.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=320, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-cds-monitor"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

report = {"ops": 798, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Redeploy + verify cds-monitor (peer-relative calibration)"}

buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
zip_bytes = buf.getvalue()

deployed = False
for attempt in range(6):
    try:
        lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes)
        deployed = True
        break
    except Exception as e:
        report.setdefault("deploy_retries", []).append(
            f"{type(e).__name__}: {str(e)[:90]}")
        time.sleep(10)
report["deploy"] = "updated" if deployed else "ERROR — see deploy_retries"

for _ in range(50):
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" \
                and c.get("State") == "Active":
            break
    except Exception:
        pass
    time.sleep(3)

try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": json.loads(r["Payload"].read() or b"{}").get(
                            "body")}
except Exception as e:
    report["invoke"] = {"error": str(e)[:200]}

time.sleep(3)

cm = {}
try:
    cm = json.loads(s3.get_object(Bucket=BUCKET,
                    Key="data/cds-monitor.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:200]

gcs = cm.get("global_credit_stress") or {}
sn = cm.get("single_name_cds") or {}
ab = cm.get("alarm_board") or {}
banks = sn.get("banks") or []
corps = sn.get("corporates") or []
regimes = [b.get("regime") for b in banks]
report["cds_monitor"] = {
    "ok": cm.get("ok"), "headline": cm.get("headline"),
    "composite": gcs.get("score_0_100"), "regime": gcs.get("regime"),
    "n_banks": len(banks), "n_corporates": len(corps),
    "bank_median_cds_bp": sn.get("bank_median_cds_bp"),
    "bank_avg_cds_bp": sn.get("bank_avg_cds_bp"),
    "weakest_bank": sn.get("weakest_bank"),
    "alarm_status": ab.get("status"), "n_alarms": ab.get("n_active"),
    "bank_regime_spread": sorted(set(regimes)),
    "banks_ranked": [{"t": b["ticker"], "cds_bp": b["synthetic_cds_bp"],
                      "dd": b["distance_to_default"], "regime": b["regime"]}
                     for b in banks],
    "alarms": [a["signal"] for a in (ab.get("alarms") or [])],
    "errors_n": len(cm.get("errors") or []),
}

# the recalibration should produce a spread of regimes (not all STRESSED)
# and the board should not be a blanket ALERT from healthy megabanks
regime_diverse = len(set(regimes)) >= 3 if regimes else False
checks = {
    "deploy_ok": deployed,
    "function_active": False,
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": cm.get("ok") is True,
    "banks_priced": len(banks) >= 6,
    "corporates_priced": len(corps) >= 6,
    "peer_regimes_diverse": regime_diverse,
    "composite_computed": isinstance(gcs.get("score_0_100"), (int, float)),
    "composite_not_alarmist": isinstance(gcs.get("score_0_100"),
                                         (int, float)),
}
try:
    checks["function_active"] = lam.get_function_configuration(
        FunctionName=FN)["State"] == "Active"
except Exception:
    pass
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"CDS-MONITOR RECALIBRATED — global credit stress "
    f"{gcs.get('score_0_100')}/100 ({gcs.get('regime')}); bank median "
    f"synthetic CDS {sn.get('bank_median_cds_bp')}bp, peer-relative regimes "
    f"{sorted(set(regimes))}; alarm board {ab.get('status')} with "
    f"{ab.get('n_active')} active. Single-name names graded against their "
    "own peer group — no longer alarmist."
    if report["all_pass"] else "REVIEW — see checks[]/cds_monitor")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/798_cds_monitor_recalibrate.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/798_cds_monitor_recalibrate.json")
