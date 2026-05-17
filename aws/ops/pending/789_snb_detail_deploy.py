"""ops/789 — deploy + schedule + verify justhodl-snb-detail.

Force-deploys the SNB liquidity / franc engine via boto3 (reliable
new-function path), wires the daily EventBridge schedule, invokes it, and
confirms it writes data/snb-detail.json with real FRED data — completing the
central-bank coverage for the eurodollar / carry system (ECB + BOJ + SNB).
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
FN = "justhodl-snb-detail"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"

report = {"ops": 789, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Deploy + verify justhodl-snb-detail (Swiss franc engine)"}

# ── 1. build zip ──
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
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        lam.update_function_configuration(
            FunctionName=FN, Handler=CONF["handler"], Runtime=CONF["runtime"],
            Role=ROLE, Timeout=CONF["timeout"], MemorySize=CONF["memory"],
            Environment={"Variables": CONF["environment"]},
            Description=CONF["description"])
        report["deploy"] = "updated (already existed)"
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

# ── 3. wire schedule ──
sch = CONF["schedule"]
try:
    events.put_rule(Name=sch["rule_name"],
                    ScheduleExpression=sch["cron"], State="ENABLED",
                    Description=sch["description"])
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
    payload = json.loads(r["Payload"].read() or b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": payload.get("body")}
except Exception as e:
    report["invoke"] = {"error": str(e)[:200]}

time.sleep(3)

# ── 5. read the SNB output ──
snb = {}
try:
    snb = json.loads(s3.get_object(Bucket=BUCKET,
                     Key="data/snb-detail.json")["Body"].read())
except Exception as e:
    report["snb_read_err"] = str(e)[:200]

fp = snb.get("franc_pressure") or {}
report["snb_detail"] = {
    "ok": snb.get("ok"), "headline": snb.get("headline"),
    "stance": snb.get("stance_label"),
    "franc_pressure": fp.get("score_0_100"),
    "franc_regime": fp.get("regime"),
    "components": fp.get("components"),
    "monetary_base_chf_bn": (snb.get("monetary_base") or {}).get(
        "total_chf_bn"),
    "fx_reserves_chf_bn": (snb.get("fx_reserves") or {}).get("total_chf_bn"),
    "policy_rate_pct": (snb.get("policy_rate") or {}).get("short_rate_pct"),
    "yield_10y_pct": (snb.get("yield_10y") or {}).get("yield_pct"),
    "eur_chf": (snb.get("franc_crosses") or {}).get("eur_chf"),
    "usd_chf": (snb.get("franc_crosses") or {}).get("usd_chf"),
    "errors": snb.get("errors"),
}

# ── 6. verdict ──
checks = {
    "deploy_ok": str(report.get("deploy", "")).startswith(("created",
                                                           "updated")),
    "function_active": False,
    "schedule_wired": report.get("schedule", {}).get("wired") is True,
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": snb.get("ok") is True,
    "franc_score_computed": isinstance(fp.get("score_0_100"), (int, float)),
    "real_fred_data": len(snb.get("errors") or []) <= 2
                      and snb.get("monetary_base", {}).get(
                          "total_chf_bn") is not None,
}
try:
    c = lam.get_function_configuration(FunctionName=FN)
    checks["function_active"] = c.get("State") == "Active"
except Exception:
    pass
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"SNB-DETAIL LIVE — franc safe-haven pressure {fp.get('score_0_100')}/100 "
    f"({fp.get('regime')}); SNB {snb.get('stance_label')}; EUR/CHF "
    f"{(snb.get('franc_crosses') or {}).get('eur_chf')}. Deployed, scheduled "
    "daily 11:40 UTC, real FRED data. Central-bank coverage for the "
    "eurodollar / carry system is now complete: ecb-detail + boj-detail + "
    "snb-detail, all feeding cb-injection's ECB/BOJ/Fed/SNB synthesis."
    if report["all_pass"] else "REVIEW — see checks[]/invoke/snb_detail")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/789_snb_detail_deploy.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/789_snb_detail_deploy.json")
