"""ops/796 — deploy + schedule + verify justhodl-cds-monitor.

Force-deploys the Merton-model Global Credit Default & Stress Monitor via
boto3, wires the daily EventBridge schedule, invokes it (allowing the long
single-name pricing pass), and confirms data/cds-monitor.json carries the
single-name synthetic CDS, the composite and the alarm board.
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
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"

report = {"ops": 796, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Deploy + verify justhodl-cds-monitor (Merton credit engine)"}

buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
zip_bytes = buf.getvalue()

try:
    lam.get_function(FunctionName=FN)
    exists = True
except lam.exceptions.ResourceNotFoundException:
    exists = False

try:
    if exists:
        lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes)
        for _ in range(40):
            if lam.get_function_configuration(
                    FunctionName=FN).get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        lam.update_function_configuration(
            FunctionName=FN, Handler=CONF["handler"], Runtime=CONF["runtime"],
            Role=ROLE, Timeout=CONF["timeout"], MemorySize=CONF["memory"],
            Environment={"Variables": CONF["environment"]},
            Description=CONF["description"])
        report["deploy"] = "updated"
    else:
        lam.create_function(
            FunctionName=FN, Runtime=CONF["runtime"], Role=ROLE,
            Handler=CONF["handler"], Timeout=CONF["timeout"],
            MemorySize=CONF["memory"], Architectures=CONF["architectures"],
            Description=CONF["description"],
            Environment={"Variables": CONF["environment"]},
            Code={"ZipFile": zip_bytes})
        report["deploy"] = "created"
except Exception as e:
    report["deploy"] = f"ERROR {type(e).__name__}: {str(e)[:200]}"

for _ in range(50):
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        if (c.get("State") == "Active"
                and c.get("LastUpdateStatus") == "Successful"):
            break
    except Exception:
        pass
    time.sleep(3)

sch = CONF["schedule"]
try:
    events.put_rule(Name=sch["rule_name"], ScheduleExpression=sch["cron"],
                    State="ENABLED", Description=sch["description"])
    rule_arn = events.describe_rule(Name=sch["rule_name"])["Arn"]
    try:
        lam.add_permission(
            FunctionName=FN, StatementId=f"{sch['rule_name']}-invoke",
            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
            SourceArn=rule_arn)
    except lam.exceptions.ResourceConflictException:
        pass
    fn_arn = lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    events.put_targets(Rule=sch["rule_name"],
                       Targets=[{"Id": "1", "Arn": fn_arn}])
    report["schedule"] = {"rule": sch["rule_name"], "cron": sch["cron"],
                          "wired": True}
except Exception as e:
    report["schedule"] = {"error": f"{type(e).__name__}: {str(e)[:160]}"}

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
report["cds_monitor"] = {
    "ok": cm.get("ok"), "headline": cm.get("headline"),
    "composite": gcs.get("score_0_100"), "regime": gcs.get("regime"),
    "n_banks": len(banks), "n_corporates": len(corps),
    "bank_avg_cds_bp": sn.get("bank_avg_cds_bp"),
    "corporate_avg_cds_bp": sn.get("corporate_avg_cds_bp"),
    "widest_bank": sn.get("widest_bank"),
    "alarm_status": ab.get("status"), "n_alarms": ab.get("n_active"),
    "sample_banks": [{"t": b["ticker"], "cds": b["synthetic_cds_bp"],
                      "pd": b["default_prob_1y_pct"]}
                     for b in banks[:5]],
    "errors_n": len(cm.get("errors") or []),
}

checks = {
    "deploy_ok": str(report.get("deploy", "")).startswith(("created",
                                                           "updated")),
    "function_active": False,
    "schedule_wired": report.get("schedule", {}).get("wired") is True,
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": cm.get("ok") is True,
    "composite_computed": isinstance(gcs.get("score_0_100"), (int, float)),
    "banks_priced": len(banks) >= 6,
    "corporates_priced": len(corps) >= 6,
    "merton_sane": all(isinstance(b.get("synthetic_cds_bp"), (int, float))
                       and b["synthetic_cds_bp"] >= 0 for b in banks[:3]),
}
try:
    checks["function_active"] = lam.get_function_configuration(
        FunctionName=FN)["State"] == "Active"
except Exception:
    pass
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"CDS-MONITOR LIVE — global credit stress {gcs.get('score_0_100')}/100 "
    f"({gcs.get('regime')}); {len(banks)} banks + {len(corps)} corporates "
    f"priced via the Merton model (bank avg synthetic CDS "
    f"{sn.get('bank_avg_cds_bp')}bp); alarm board {ab.get('status')}. "
    "Deployed, scheduled daily 13:00 UTC, real FMP + FRED data fused with "
    "the platform's sovereign/bond/systemic/canary engines."
    if report["all_pass"] else "REVIEW — see checks[]/invoke/cds_monitor")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/796_cds_monitor_deploy.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/796_cds_monitor_deploy.json")
