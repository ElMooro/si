"""ops/819 — deploy + verify justhodl-dividend-growth (the Dividend
Compounder screen).

Verifies the output is REAL and SANE: every compounder is genuinely
GROWING the dividend (positive CAGR), has no cut, carries a 3+ year
streak; aristocrats carry a 10+ year streak; and the high-yield traps
are quarantined OUT of the compounder list.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=290, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-dividend-growth"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"

report = {"ops": 819, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Deploy + verify justhodl-dividend-growth compounder screen"}

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
        Bucket=BUCKET, Key="data/dividend-growth.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:160]

comp = ob.get("compounders") or []
traps = ob.get("yield_traps") or []
summ = ob.get("summary") or {}


def grate(c):
    g = c.get("div_cagr_5y_pct")
    return g if g is not None else c.get("div_cagr_3y_pct")


# every compounder must genuinely be growing, no cut, 3+ year streak
all_growing = all((grate(c) is not None and grate(c) > 0) for c in comp) \
    if comp else False
no_cuts = all(not c.get("had_cut") for c in comp)
streak_ok = all((c.get("growth_streak_years") or 0) >= 3 for c in comp)
arist_ok = all((c.get("growth_streak_years") or 0) >= 10
               for c in comp if c.get("tier") == "ARISTOCRAT")
grow_ok = all((c.get("growth_streak_years") or 0) >= 5
              for c in comp if c.get("tier") == "GROWER")
comp_syms = {c.get("symbol") for c in comp}
traps_quarantined = all(
    t.get("tier") == "YIELD TRAP" and t.get("symbol") not in comp_syms
    for t in traps)
ranked = all(comp[i]["compounder_score"] >= comp[i + 1]["compounder_score"]
             for i in range(len(comp) - 1)) if len(comp) > 1 else True

report["dividend_growth"] = {
    "ok": ob.get("ok"),
    "headline": ob.get("headline"),
    "n_evaluated": ob.get("n_evaluated"),
    "n_with_history": ob.get("n_with_history"),
    "n_compounders": ob.get("n_compounders"),
    "summary": summ,
    "top5": [{"sym": c.get("symbol"), "tier": c.get("tier"),
              "score": c.get("compounder_score"),
              "yield": c.get("dividend_yield_pct"),
              "cagr5": c.get("div_cagr_5y_pct"),
              "streak": c.get("growth_streak_years")} for c in comp[:5]],
    "sample_traps": [{"sym": t.get("symbol"),
                      "yield": t.get("dividend_yield_pct")}
                     for t in traps[:5]],
}
checks = {
    "deploy_ok": str(report.get("deploy", "")).startswith(
        ("created", "updated")),
    "schedule_wired": report.get("schedule") == "wired",
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": ob.get("ok") is True,
    "enough_universe": (ob.get("n_evaluated") or 0) >= 100,
    "has_compounders": len(comp) >= 10,
    "every_compounder_growing": all_growing,
    "no_cuts_in_compounders": no_cuts,
    "streaks_3plus": streak_ok,
    "aristocrats_10plus_streak": arist_ok,
    "growers_5plus_streak": grow_ok,
    "traps_quarantined": traps_quarantined,
    "ranked_by_score": ranked,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"DIVIDEND COMPOUNDERS LIVE - {ob.get('n_compounders')} names raising "
    f"the dividend ({summ.get('n_aristocrats')} aristocrats, "
    f"{summ.get('n_growers')} growers, {summ.get('n_emerging')} emerging), "
    f"{summ.get('n_yield_traps')} high-yield traps quarantined, "
    f"{ob.get('n_evaluated')} payers screened. Growth/streak/coverage gates "
    "all hold."
    if report["all_pass"] else "REVIEW - see checks[]/dividend_growth")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/819_dividend_growth_deploy.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/819_dividend_growth_deploy.json")
