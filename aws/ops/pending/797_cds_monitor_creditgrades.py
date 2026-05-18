"""ops/797 — redeploy + verify justhodl-cds-monitor (CreditGrades rewrite).

ops/796 deployed the engine but the raw Merton 1y model priced healthy
megabanks at ~0bp (the credit-spread puzzle), dragging the composite to a
false CALM. This redeploys the CreditGrades rewrite — uncertain default
barrier, 5y horizon, distance-to-default as the primary signal — and
verifies the single-name DD values and composite are now sane.
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

report = {"ops": 797, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Redeploy + verify cds-monitor (CreditGrades model)"}

buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
zip_bytes = buf.getvalue()

# the function already exists (ops 796) — update it, with conflict retries
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
    lam.update_function_configuration(
        FunctionName=FN, Handler=CONF["handler"], Runtime=CONF["runtime"],
        Role=ROLE, Timeout=CONF["timeout"], MemorySize=CONF["memory"],
        Environment={"Variables": CONF["environment"]},
        Description=CONF["description"])
    for _ in range(30):
        if lam.get_function_configuration(
                FunctionName=FN).get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
except Exception as e:
    report["config_update"] = f"{type(e).__name__}: {str(e)[:120]}"

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
    report["schedule"] = {"rule": sch["rule_name"], "wired": True}
except Exception as e:
    report["schedule"] = {"error": f"{type(e).__name__}: {str(e)[:140]}"}

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
dds = [b.get("distance_to_default") for b in banks
       if isinstance(b.get("distance_to_default"), (int, float))]
report["cds_monitor"] = {
    "ok": cm.get("ok"), "schema": cm.get("schema_version"),
    "model": sn.get("model"), "headline": cm.get("headline"),
    "composite": gcs.get("score_0_100"), "regime": gcs.get("regime"),
    "n_banks": len(banks), "n_corporates": len(corps),
    "bank_avg_dd": sn.get("bank_avg_distance_to_default"),
    "corporate_avg_dd": sn.get("corporate_avg_distance_to_default"),
    "bank_avg_cds_bp": sn.get("bank_avg_cds_bp"),
    "weakest_bank": sn.get("weakest_bank"),
    "alarm_status": ab.get("status"), "n_alarms": ab.get("n_active"),
    "banks_ranked": [{"t": b["ticker"], "dd": b["distance_to_default"],
                      "cds_bp": b["synthetic_cds_bp"], "regime": b["regime"]}
                     for b in banks],
    "errors_n": len(cm.get("errors") or []),
}

# DD sanity: every bank DD finite and within a plausible structural range
dd_sane = bool(dds) and all(-2.0 < x < 12.0 for x in dds)
checks = {
    "deploy_ok": deployed,
    "function_active": False,
    "schedule_wired": report.get("schedule", {}).get("wired") is True,
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": cm.get("ok") is True,
    "creditgrades_model": "CreditGrades" in (sn.get("model") or ""),
    "banks_priced": len(banks) >= 6,
    "corporates_priced": len(corps) >= 6,
    "dd_values_sane": dd_sane,
    "composite_computed": isinstance(gcs.get("score_0_100"), (int, float)),
}
try:
    checks["function_active"] = lam.get_function_configuration(
        FunctionName=FN)["State"] == "Active"
except Exception:
    pass
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"CDS-MONITOR LIVE (CreditGrades) — global credit stress "
    f"{gcs.get('score_0_100')}/100 ({gcs.get('regime')}); {len(banks)} banks "
    f"+ {len(corps)} corporates priced, bank avg distance-to-default "
    f"{sn.get('bank_avg_distance_to_default')}, weakest "
    f"{sn.get('weakest_bank')}; alarm board {ab.get('status')}. "
    "Distance-to-default is the primary signal; deployed + scheduled daily "
    "13:00 UTC on real FMP + FRED data."
    if report["all_pass"] else "REVIEW — see checks[]/cds_monitor")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/797_cds_monitor_creditgrades.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/797_cds_monitor_creditgrades.json")
