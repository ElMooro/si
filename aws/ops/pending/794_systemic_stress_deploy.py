"""ops/794 — deploy + schedule + verify justhodl-systemic-stress.

Force-deploys the ECB CISS/SovCISS systemic & sovereign stress engine via
boto3, wires the daily EventBridge schedule, invokes it, and confirms it
writes data/systemic-stress.json with real ECB Data Portal data.
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
FN = "justhodl-systemic-stress"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"

report = {"ops": 794, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Deploy + verify justhodl-systemic-stress (ECB CISS/SovCISS)"}

# ── 1. zip ──
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
zip_bytes = buf.getvalue()

# ── 2. create or update ──
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
            Environment={"Variables": CONF.get("environment", {})},
            Description=CONF["description"])
        report["deploy"] = "updated"
    else:
        lam.create_function(
            FunctionName=FN, Runtime=CONF["runtime"], Role=ROLE,
            Handler=CONF["handler"], Timeout=CONF["timeout"],
            MemorySize=CONF["memory"], Architectures=CONF["architectures"],
            Description=CONF["description"],
            Environment={"Variables": CONF.get("environment", {})},
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

# ── 3. schedule ──
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

# ── 4. invoke ──
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

# ── 5. read output ──
ss = {}
try:
    ss = json.loads(s3.get_object(Bucket=BUCKET,
                    Key="data/systemic-stress.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:200]

comp = ss.get("composite") or {}
sysd = ss.get("systemic_stress") or {}
sovd = ss.get("sovereign_stress") or {}
ea = sysd.get("euro_area") or {}
report["systemic_stress"] = {
    "ok": ss.get("ok"), "headline": ss.get("headline"),
    "composite_score": comp.get("score_0_100"),
    "composite_regime": comp.get("regime"),
    "euro_area_ciss_pct": ea.get("percentile"),
    "euro_area_ciss_regime": ea.get("regime"),
    "ciss_countries": sorted((sysd.get("countries") or {}).keys()),
    "most_stressed_systemic": sysd.get("most_stressed"),
    "sov_countries": sorted((sovd.get("countries") or {}).keys()),
    "most_stressed_sovereign": sovd.get("most_stressed"),
    "fragmentation_score": sovd.get("fragmentation_score"),
    "fragmentation_label": sovd.get("fragmentation_label"),
    "errors": ss.get("errors"),
}

# ── 6. verdict ──
checks = {
    "deploy_ok": str(report.get("deploy", "")).startswith(("created",
                                                           "updated")),
    "function_active": False,
    "schedule_wired": report.get("schedule", {}).get("wired") is True,
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": ss.get("ok") is True,
    "composite_computed": isinstance(comp.get("score_0_100"), (int, float)),
    "ciss_multi_country": len(sysd.get("countries") or {}) >= 4,
    "sovciss_multi_country": len(sovd.get("countries") or {}) >= 6,
    "real_ecb_data": len(ss.get("errors") or []) <= 4
                     and ea.get("percentile") is not None,
}
try:
    checks["function_active"] = lam.get_function_configuration(
        FunctionName=FN)["State"] == "Active"
except Exception:
    pass
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"SYSTEMIC-STRESS LIVE — composite {comp.get('score_0_100')}/100 "
    f"({comp.get('regime')}); euro-area CISS at its "
    f"{ea.get('percentile')}th percentile; euro fragmentation "
    f"{sovd.get('fragmentation_label')}. ECB CISS for "
    f"{len(sysd.get('countries') or {})+1} economies + SovCISS for "
    f"{len(sovd.get('countries') or {})} euro-area sovereigns, deployed, "
    "scheduled daily 12:00 UTC, real ECB Data Portal data."
    if report["all_pass"] else "REVIEW — see checks[]/invoke/systemic_stress")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/794_systemic_stress_deploy.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/794_systemic_stress_deploy.json")
