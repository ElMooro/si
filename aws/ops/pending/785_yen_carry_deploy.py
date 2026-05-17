"""ops/785 — deploy + verify justhodl-yen-carry.

Force-deploys the Yen Carry Trade & BOJ Liquidity engine via boto3 (the
reliable path), wires its daily EventBridge schedule, invokes it and
verifies the carry regime, unwind-risk score and every data leg resolve.
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

FN = "justhodl-yen-carry"
BASE = f"aws/lambdas/{FN}"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
BUCKET = "justhodl-dashboard-live"

report = {"ops": 785, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Deploy + verify justhodl-yen-carry"}

conf = json.load(open(f"{BASE}/config.json"))
desc = conf["description"][:255]
code = open(f"{BASE}/source/lambda_function.py", encoding="utf-8").read()
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", code)
zip_bytes = buf.getvalue()

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
            Description=desc)
        report["deploy"] = "created new function"
except Exception as e:
    report["deploy"] = f"ERROR {type(e).__name__}: {str(e)[:240]}"

for _ in range(40):
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        if (c.get("State") == "Active"
                and c.get("LastUpdateStatus") in ("Successful", None)):
            break
    except Exception:
        pass
    time.sleep(3)

try:
    lam.update_function_configuration(
        FunctionName=FN, Timeout=conf["timeout"], MemorySize=conf["memory"],
        Environment={"Variables": conf.get("environment", {})},
        Description=desc)
    for _ in range(20):
        if lam.get_function_configuration(
                FunctionName=FN).get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
except Exception as e:
    report["config_update"] = str(e)[:150]

fn_arn = f"arn:aws:lambda:us-east-1:{acct}:function:{FN}"

sch = conf["schedule"]
try:
    rule = events.put_rule(Name=sch["rule_name"],
                           ScheduleExpression=sch["cron"], State="ENABLED",
                           Description=sch["description"][:255])
    try:
        lam.add_permission(
            FunctionName=FN, StatementId=f"{sch['rule_name']}-invoke",
            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
            SourceArn=rule["RuleArn"])
    except lam.exceptions.ResourceConflictException:
        pass
    events.put_targets(Rule=sch["rule_name"],
                       Targets=[{"Id": "1", "Arn": fn_arn}])
    report["schedule"] = {"rule": sch["rule_name"], "cron": sch["cron"],
                          "state": "ENABLED"}
except Exception as e:
    report["schedule"] = f"ERROR {str(e)[:200]}"

try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    payload = json.loads(r["Payload"].read() or b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": payload}
except Exception as e:
    report["invoke"] = {"err": str(e)[:240]}

time.sleep(3)
out = {}
try:
    out = json.loads(s3.get_object(Bucket=BUCKET,
                     Key="data/yen-carry.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:200]

fund = out.get("boj_funding_leg") or {}
wid = out.get("carry_width") or {}
fx = out.get("fx_detonator") or {}
jgb = out.get("jgb_long_end") or {}
report["output"] = {
    "ok": out.get("ok"), "headline": out.get("headline"),
    "carry_regime": out.get("carry_regime"),
    "boj_injection_score": out.get("boj_injection_score"),
    "boj_stance_label": out.get("boj_stance_label"),
    "unwind_risk_score": out.get("unwind_risk_score"),
    "unwind_risk_label": out.get("unwind_risk_label"),
    "unwind_risk_components": out.get("unwind_risk_components"),
    "carry_attractiveness": out.get("carry_attractiveness"),
    "boj_bs_chg_6m_pct": fund.get("boj_balance_sheet_chg_6m_pct"),
    "jp_short_rate_pct": fund.get("jp_short_rate_pct"),
    "policy_direction": fund.get("policy_direction"),
    "front_end_carry_pp": wid.get("front_end_carry_pp"),
    "duration_carry_pp": wid.get("duration_carry_pp"),
    "usdjpy": fx.get("usdjpy"),
    "usdjpy_chg_1m_pct": fx.get("usdjpy_chg_1m_pct"),
    "realized_vol_20d_pct": fx.get("realized_vol_20d_pct"),
    "vol_regime": fx.get("vol_regime"),
    "jgb_10y_pct": jgb.get("jgb_10y_pct"),
    "positioning": out.get("positioning"),
    "decisive_call": out.get("decisive_call"),
    "sources": out.get("sources"),
    "errors": out.get("errors"),
}

checks = {
    "function_live": "ERROR" not in str(report.get("deploy", "")),
    "schedule_wired": isinstance(report.get("schedule"), dict),
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": out.get("ok") is True,
    "boj_funding_resolved": fund.get("jp_short_rate_pct") is not None
                            and fund.get("boj_balance_sheet_chg_6m_pct") is not None,
    "carry_width_resolved": wid.get("front_end_carry_pp") is not None
                            and wid.get("duration_carry_pp") is not None,
    "fx_detonator_resolved": fx.get("usdjpy") is not None
                             and fx.get("realized_vol_20d_pct") is not None,
    "jgb_resolved": jgb.get("jgb_10y_pct") is not None,
    "unwind_risk_resolved": out.get("unwind_risk_score") is not None,
    "regime_classified": out.get("carry_regime") in (
        "CARRY-ON", "NEUTRAL", "CARRY-AT-RISK", "CARRY-UNWIND"),
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "YEN-CARRY LIVE — Yen Carry & BOJ engine deployed and verified. "
    f"Regime {out.get('carry_regime')}; unwind risk "
    f"{out.get('unwind_risk_score')}/100 ({out.get('unwind_risk_label')}); "
    f"USD/JPY {fx.get('usdjpy')}, 20d vol {fx.get('realized_vol_20d_pct')}%; "
    f"front-end carry {wid.get('front_end_carry_pp')}pp; BOJ "
    f"{out.get('boj_stance_label')}. All five data legs resolve."
    if report["all_pass"] else "REVIEW — see checks[]/output.errors")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/785_yen_carry_deploy.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/785_yen_carry_deploy.json")
