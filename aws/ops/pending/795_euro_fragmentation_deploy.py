"""ops/795 — deploy + schedule + verify justhodl-euro-fragmentation.

Force-deploys the euro-area sovereign fragmentation engine via boto3, wires
the daily EventBridge schedule, invokes it, and confirms it writes
data/euro-fragmentation.json with real ECB SovCISS + FRED yield data.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-euro-fragmentation"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"

report = {"ops": 795, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Deploy + verify justhodl-euro-fragmentation"}

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
        for _ in range(30):
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

for _ in range(40):
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

ef = {}
try:
    ef = json.loads(s3.get_object(Bucket=BUCKET,
                    Key="data/euro-fragmentation.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:200]

frag = ef.get("fragmentation") or {}
fr = ef.get("france_focus") or {}
report["euro_fragmentation"] = {
    "ok": ef.get("ok"), "headline": ef.get("headline"),
    "score": frag.get("score_0_100"), "regime": frag.get("regime"),
    "widest_spread_bp": frag.get("widest_spread_bp"),
    "france_oat_bund_bp": fr.get("oat_bund_spread_bp"),
    "france_sovciss_pct": fr.get("sovciss_percentile"),
    "n_countries": len(ef.get("countries") or {}),
    "core_stress_flag": (ef.get("core_vs_periphery") or {}).get(
        "core_stress_flag"),
    "errors": ef.get("errors"),
}

checks = {
    "deploy_ok": str(report.get("deploy", "")).startswith(("created",
                                                           "updated")),
    "function_active": False,
    "schedule_wired": report.get("schedule", {}).get("wired") is True,
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": ef.get("ok") is True,
    "score_computed": isinstance(frag.get("score_0_100"), (int, float)),
    "multi_country": len(ef.get("countries") or {}) >= 6,
    "france_focus_ok": fr.get("oat_bund_spread_bp") is not None,
}
try:
    checks["function_active"] = lam.get_function_configuration(
        FunctionName=FN)["State"] == "Active"
except Exception:
    pass
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"EURO-FRAGMENTATION LIVE — {frag.get('regime')}, score "
    f"{frag.get('score_0_100')}/100; widest sovereign spread "
    f"{frag.get('widest_spread_bp')}bp; France OAT-Bund "
    f"{fr.get('oat_bund_spread_bp')}bp. Deployed, scheduled daily 12:20 UTC, "
    "real ECB SovCISS + FRED yield data."
    if report["all_pass"] else "REVIEW — see checks[]/invoke/euro_fragmentation")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/795_euro_fragmentation_deploy.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/795_euro_fragmentation_deploy.json")
