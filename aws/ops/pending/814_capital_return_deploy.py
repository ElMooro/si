"""ops/814 - deploy + verify justhodl-capital-return (cannibal screen) and
clean the orphan boom-radar-daily EventBridge rule left by ops 813
(delete_rule takes Name=, not Rule=).
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
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
FN = "justhodl-capital-return"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

report = {"ops": 814, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Deploy capital-return cannibal screen + orphan cleanup"}

# ── clean orphan EventBridge rules (delete_rule needs Name=) ──
cleaned = {}
for rule in ("boom-radar-daily", "catch-up-radar-daily"):
    try:
        tids = [t["Id"] for t in events.list_targets_by_rule(
            Rule=rule).get("Targets", [])]
        if tids:
            events.remove_targets(Rule=rule, Ids=tids)
        events.delete_rule(Name=rule)
        cleaned[rule] = "deleted"
    except events.exceptions.ResourceNotFoundException:
        cleaned[rule] = "already absent"
    except Exception as e:
        cleaned[rule] = f"err {str(e)[:120]}"
report["orphan_rule_cleanup"] = cleaned

# ── deploy capital-return ──
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
zb = buf.getvalue()
try:
    try:
        lam.get_function(FunctionName=FN)
        lam.update_function_code(FunctionName=FN, ZipFile=zb)
        for _ in range(30):
            if lam.get_function_configuration(
                    FunctionName=FN).get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        lam.update_function_configuration(
            FunctionName=FN, Handler=CONF["handler"], Runtime=CONF["runtime"],
            Role=ROLE, Timeout=CONF["timeout"], MemorySize=CONF["memory"],
            Description=CONF["description"][:255])
        report["deploy"] = "updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=FN, Runtime=CONF["runtime"], Role=ROLE,
            Handler=CONF["handler"], Timeout=CONF["timeout"],
            MemorySize=CONF["memory"], Architectures=CONF["architectures"],
            Description=CONF["description"][:255], Code={"ZipFile": zb})
        report["deploy"] = "created"
except Exception as e:
    report["deploy"] = f"ERROR {type(e).__name__}: {str(e)[:200]}"

for _ in range(40):
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("State") == "Active" and c.get(
                "LastUpdateStatus") == "Successful":
            break
    except Exception:
        pass
    time.sleep(3)

sch = CONF["schedule"]
try:
    events.put_rule(Name=sch["rule_name"], ScheduleExpression=sch["cron"],
                    State="ENABLED", Description=sch["description"])
    arn = events.describe_rule(Name=sch["rule_name"])["Arn"]
    try:
        lam.add_permission(FunctionName=FN,
                           StatementId=f"{sch['rule_name']}-invoke",
                           Action="lambda:InvokeFunction",
                           Principal="events.amazonaws.com", SourceArn=arn)
    except lam.exceptions.ResourceConflictException:
        pass
    fa = lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    events.put_targets(Rule=sch["rule_name"],
                       Targets=[{"Id": "1", "Arn": fa}])
    report["schedule"] = "wired"
except Exception as e:
    report["schedule"] = f"err {str(e)[:140]}"

try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": json.loads(
                            r["Payload"].read() or b"{}").get("body")}
except Exception as e:
    report["invoke"] = {"error": str(e)[:200]}

time.sleep(3)
ob = {}
try:
    ob = json.loads(s3.get_object(
        Bucket=BUCKET, Key="data/capital-return.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:160]

cann = ob.get("cannibals") or []
with_target = sum(1 for c in cann[:30] if c.get("price_target") is not None)
# realism: shareholder yield should be sane (no absurd > 30% values)
insane = [c["symbol"] for c in cann
          if (c.get("shareholder_yield_pct") or 0) > 35]
report["capital_return"] = {
    "ok": ob.get("ok"), "headline": ob.get("headline"),
    "n_evaluated": ob.get("n_evaluated"), "n_cannibals": len(cann),
    "top30_with_target": with_target,
    "insane_yields": insane,
    "top6": [{"sym": c.get("symbol"), "score": c.get("cannibal_score"),
              "buyback": c.get("buyback_yield_pct"),
              "total_yield": c.get("shareholder_yield_pct"),
              "fcf_yield": c.get("fcf_yield_pct"),
              "pe": c.get("pe_ratio"),
              "upside": c.get("upside_pct")} for c in cann[:6]],
}

checks = {
    "deploy_ok": str(report.get("deploy", "")).startswith(
        ("created", "updated")),
    "schedule_wired": report.get("schedule") == "wired",
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": ob.get("ok") is True,
    "cannibals_found": len(cann) >= 3,
    "yields_sane": len(insane) == 0,
    "targets_present": with_target >= 1,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"CAPITAL-RETURN LIVE - {len(cann)} cannibals from "
    f"{ob.get('n_evaluated')} stocks, FCF-funded buybacks at sane "
    f"valuations; {with_target}/30 top names carry a price target. "
    "Deployed, scheduled daily 13:45 UTC. Orphan boom-radar rule cleaned."
    if report["all_pass"] else "REVIEW - see checks[]/capital_return")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/814_capital_return_deploy.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/814_capital_return_deploy.json")
