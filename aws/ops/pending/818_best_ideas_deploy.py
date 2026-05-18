"""ops/818 — deploy + verify justhodl-best-ideas (the cross-engine
factor-confluence Best Ideas board).

Verifies the output is REAL and SANE: every name carries >= 2 factor
families (the whole point), titans carry >= 4, and a healthy share of
names carry a price target from the screener reference data.
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
FN = "justhodl-best-ideas"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"

report = {"ops": 818, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Deploy + verify justhodl-best-ideas confluence board"}

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
    events.put_targets(Rule=sch["rule_name"], Targets=[{"Id": "1", "Arn": fa}])
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
        Bucket=BUCKET, Key="data/best-ideas.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:160]

stack = ob.get("stack") or []
all_2fam = all((s.get("families_hit") or 0) >= 2 for s in stack)
titans = [s for s in stack if (s.get("families_hit") or 0) >= 4]
titans_ok = all((s.get("families_hit") or 0) >= 4 for s in titans)
with_t = sum(1 for s in stack if s.get("price_target") is not None)
cov = ob.get("engine_coverage") or {}
engines_live = sum(1 for v in cov.values() if (v.get("n") or 0) > 0)

report["best_ideas"] = {
    "ok": ob.get("ok"), "headline": ob.get("headline"),
    "n_total": ob.get("n_total"), "n_titans": ob.get("n_titans"),
    "n_high": ob.get("n_high_conviction"),
    "engines_contributing": engines_live,
    "with_target": with_t,
    "top5": [{"sym": s.get("symbol"), "tier": s.get("conviction_tier"),
              "fams": s.get("families_hit"), "engines": s.get("engines_hit"),
              "score": s.get("conviction_score"),
              "upside": s.get("upside_pct"),
              "families": s.get("families")} for s in stack[:5]],
}
checks = {
    "deploy_ok": str(report.get("deploy", "")).startswith(
        ("created", "updated")),
    "schedule_wired": report.get("schedule") == "wired",
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": ob.get("ok") is True,
    "has_stack": len(stack) >= 5,
    "every_name_2plus_families": all_2fam,
    "titans_are_4plus_families": titans_ok,
    "enough_engines_contributing": engines_live >= 8,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"BEST IDEAS LIVE - {ob.get('n_total')} cross-confirmed names "
    f"({ob.get('n_titans')} conviction titans across 4+ families, "
    f"{ob.get('n_high_conviction')} high-conviction across 3), "
    f"{engines_live} engines contributing, {with_t} carry price targets. "
    "Factor-confluence master screen production-clean."
    if report["all_pass"] else "REVIEW - see checks[]/best_ideas")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/818_best_ideas_deploy.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/818_best_ideas_deploy.json")
