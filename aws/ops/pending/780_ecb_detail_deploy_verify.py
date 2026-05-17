"""ops/780 — deploy + verify justhodl-ecb-detail.

Brand-new function: force-create via boto3 from repo source (the deploy
workflow's create path is unreliable), wire the daily EventBridge rule,
invoke, and verify the ECB Data Portal series keys actually resolve.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
acct = boto3.client("sts", region_name="us-east-1").get_caller_identity()["Account"]

FN = "justhodl-ecb-detail"
BASE = f"aws/lambdas/{FN}"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
BUCKET = "justhodl-dashboard-live"

report = {"ops": 780, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Deploy + verify justhodl-ecb-detail (ECB liquidity engine)"}

conf = json.load(open(f"{BASE}/config.json"))
code = open(f"{BASE}/source/lambda_function.py", encoding="utf-8").read()
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", code)
zip_bytes = buf.getvalue()

# 1. create or update the function
try:
    lam.get_function_configuration(FunctionName=FN)
    exists = True
except lam.exceptions.ResourceNotFoundException:
    exists = False

try:
    if exists:
        lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes)
        report["deploy"] = "updated existing function"
    else:
        lam.create_function(
            FunctionName=FN, Runtime=conf["runtime"], Role=ROLE,
            Handler=conf["handler"], Code={"ZipFile": zip_bytes},
            Timeout=conf["timeout"], MemorySize=conf["memory"],
            Architectures=conf.get("architectures", ["x86_64"]),
            Environment={"Variables": conf.get("environment", {})},
            Description=conf["description"])
        report["deploy"] = "created new function"
except Exception as e:
    report["deploy"] = f"ERROR {type(e).__name__}: {str(e)[:240]}"

for _ in range(40):
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        if (c.get("State") == "Active"
                and c.get("LastUpdateStatus") == "Successful"):
            break
    except Exception:
        pass
    time.sleep(3)

# ensure env is set even on an update path
try:
    lam.update_function_configuration(
        FunctionName=FN, Timeout=conf["timeout"], MemorySize=conf["memory"],
        Environment={"Variables": conf.get("environment", {})},
        Description=conf["description"])
    for _ in range(20):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
except Exception as e:
    report["config_update"] = str(e)[:150]

fn_arn = f"arn:aws:lambda:us-east-1:{acct}:function:{FN}"

# 2. wire the daily EventBridge schedule
sch = conf["schedule"]
try:
    rule = events.put_rule(Name=sch["rule_name"],
                           ScheduleExpression=sch["cron"], State="ENABLED",
                           Description=sch["description"])
    rule_arn = rule["RuleArn"]
    try:
        lam.add_permission(
            FunctionName=FN, StatementId=f"{sch['rule_name']}-invoke",
            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
            SourceArn=rule_arn)
    except lam.exceptions.ResourceConflictException:
        pass
    events.put_targets(Rule=sch["rule_name"],
                       Targets=[{"Id": "1", "Arn": fn_arn}])
    report["schedule"] = {"rule": sch["rule_name"], "cron": sch["cron"],
                          "state": "ENABLED"}
except Exception as e:
    report["schedule"] = f"ERROR {str(e)[:200]}"

# 3. invoke and verify
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    payload = json.loads(r["Payload"].read() or b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": payload.get("body")}
except Exception as e:
    report["invoke"] = {"err": str(e)[:240]}

time.sleep(3)
out = {}
try:
    out = json.loads(s3.get_object(Bucket=BUCKET,
                     Key="data/ecb-detail.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:200]

liq = out.get("liquidity") or {}
bs = out.get("balance_sheet") or {}
pr = out.get("policy_rates") or {}
report["output"] = {
    "ok": out.get("ok"),
    "headline": out.get("headline"),
    "stance_label": out.get("stance_label"),
    "excess_liquidity_eur_bn": liq.get("excess_liquidity_eur_bn"),
    "liquidity_regime": liq.get("regime"),
    "drain_pace_bn_per_month": liq.get("drain_pace_bn_per_month"),
    "deposit_facility_recourse": liq.get("deposit_facility_recourse_eur_bn"),
    "current_accounts": liq.get("current_accounts_eur_bn"),
    "net_liquidity_effect": liq.get("net_liquidity_effect_eur_bn"),
    "balance_sheet_eur_bn": bs.get("total_assets_eur_bn"),
    "qt_pace": bs.get("qt_pace"),
    "deposit_rate": pr.get("deposit_facility_pct"),
    "main_refi_rate": pr.get("main_refinancing_pct"),
    "marginal_rate": pr.get("marginal_lending_pct"),
    "corridor_bp": pr.get("corridor_width_bp"),
    "errors": out.get("errors"),
}

checks = {
    "function_live": "ERROR" not in str(report.get("deploy", "")),
    "schedule_wired": isinstance(report.get("schedule"), dict),
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": out.get("ok") is True,
    "ecb_excess_liquidity_resolved":
        liq.get("excess_liquidity_eur_bn") is not None,
    "ecb_ilm_detail_resolved":
        liq.get("deposit_facility_recourse_eur_bn") is not None
        and liq.get("net_liquidity_effect_eur_bn") is not None,
    "balance_sheet_resolved": bs.get("total_assets_eur_bn") is not None,
    "deposit_rate_resolved": pr.get("deposit_facility_pct") is not None,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "ECB-DETAIL LIVE — granular Eurosystem liquidity engine deployed and "
    f"verified. Excess liquidity EUR {liq.get('excess_liquidity_eur_bn')}bn "
    f"({liq.get('regime')}); ECB stance {out.get('stance_label')}. "
    "ECB Data Portal series keys all resolve."
    if report["all_pass"] else
    "REVIEW — see checks[]/output.errors (a series key may need correction)")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/780_ecb_detail_deploy_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/780_ecb_detail_deploy_verify.json")
