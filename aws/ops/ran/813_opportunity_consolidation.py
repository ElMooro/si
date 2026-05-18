"""ops/812 - opportunity-stack consolidation.

Two parallel build streams converged on duplicate engines. Final canonical
stack, chosen on what works best (never-build-twice doctrine):

  BOOM BOARD  -> justhodl-opportunity-screener  (synthesis hub: passing,
                 broader, leverages existing engines). RETIRE boom-radar.
  CATCH-UP    -> justhodl-beta-laggard          (real FMP trailing returns
                 + fundamental-health gates). RETIRE catch-up-radar
                 (already torn down by ops 811 - idempotent here).
  ETF CATCH-UP-> to be rebuilt standalone next.

This ops:
  1. deploys justhodl-beta-laggard (its deploy ops never ran - number
     collision) so the canonical catch-up engine is actually live;
  2. tears down justhodl-boom-radar (Lambda + schedule + S3 output);
  3. idempotently confirms justhodl-catch-up-radar is gone.
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
report = {"ops": 813, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Opportunity-stack consolidation"}


# ── teardown helper ──
def teardown(fn, rule, s3keys):
    out = {"fn": fn}
    try:
        lam.delete_function(FunctionName=fn)
        out["lambda"] = "deleted"
    except lam.exceptions.ResourceNotFoundException:
        out["lambda"] = "already absent"
    except Exception as e:
        out["lambda"] = f"err {type(e).__name__}: {str(e)[:120]}"
    if rule:
        try:
            tids = [t["Id"] for t in events.list_targets_by_rule(
                Rule=rule).get("Targets", [])]
            if tids:
                events.remove_targets(Rule=rule, Ids=tids)
            events.delete_rule(Rule=rule)
            out["rule"] = "deleted"
        except events.exceptions.ResourceNotFoundException:
            out["rule"] = "already absent"
        except Exception as e:
            out["rule"] = f"err {str(e)[:120]}"
    for k in s3keys:
        try:
            s3.delete_object(Bucket=BUCKET, Key=k)
            out[f"s3:{k}"] = "deleted"
        except Exception as e:
            out[f"s3:{k}"] = f"err {str(e)[:80]}"
    return out


# ── 1. deploy justhodl-beta-laggard (canonical catch-up engine) ──
FN = "justhodl-beta-laggard"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))
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
        report["beta_laggard_deploy"] = "updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=FN, Runtime=CONF["runtime"], Role=ROLE,
            Handler=CONF["handler"], Timeout=CONF["timeout"],
            MemorySize=CONF["memory"], Architectures=CONF["architectures"],
            Description=CONF["description"][:255], Code={"ZipFile": zb})
        report["beta_laggard_deploy"] = "created"
except Exception as e:
    report["beta_laggard_deploy"] = f"ERROR {type(e).__name__}: {str(e)[:200]}"

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
    report["beta_laggard_schedule"] = "wired"
except Exception as e:
    report["beta_laggard_schedule"] = f"err {str(e)[:140]}"

try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    report["beta_laggard_invoke"] = {
        "status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
        "body": json.loads(r["Payload"].read() or b"{}").get("body")}
except Exception as e:
    report["beta_laggard_invoke"] = {"error": str(e)[:200]}

time.sleep(3)
bl = {}
try:
    bl = json.loads(s3.get_object(
        Bucket=BUCKET, Key="data/beta-laggards.json")["Body"].read())
except Exception as e:
    report["beta_laggard_read_err"] = str(e)[:160]
report["beta_laggard_output"] = {
    "ok": bl.get("ok"), "headline": bl.get("headline"),
    "n_working_sectors": bl.get("n_working_sectors"),
    "n_candidates": bl.get("n_candidates"),
    "universe": bl.get("universe_with_returns"),
}

# ── 2 & 3. retire the duplicates ──
report["retire_boom_radar"] = teardown(
    "justhodl-boom-radar", "boom-radar-daily", ["data/boom-radar.json"])
report["retire_catch_up_radar"] = teardown(
    "justhodl-catch-up-radar", "catch-up-radar-daily",
    ["screener/catch-up-radar.json"])

checks = {
    "beta_laggard_deployed": str(report.get("beta_laggard_deploy", "")
                                 ).startswith(("created", "updated")),
    "beta_laggard_invoke_ok": report.get("beta_laggard_invoke", {}).get(
        "status") == 200 and not report.get(
        "beta_laggard_invoke", {}).get("fn_error"),
    "beta_laggard_output_ok": bl.get("ok") is True,
    "beta_laggard_has_candidates": (bl.get("n_candidates") or 0) >= 1,
    "boom_radar_lambda_gone": report["retire_boom_radar"]["lambda"] in (
        "deleted", "already absent"),
    "catch_up_radar_lambda_gone": report["retire_catch_up_radar"][
        "lambda"] in ("deleted", "already absent"),
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "CONSOLIDATION COMPLETE - canonical opportunity stack: Boom Board "
    "(opportunity-screener) + Catch-Up (beta-laggard, now LIVE with "
    f"{bl.get('n_candidates')} candidates across "
    f"{bl.get('n_working_sectors')} working sectors). boom-radar and "
    "catch-up-radar retired. ETF/CEF catch-up to be built next."
    if report["all_pass"]
    else "REVIEW - see checks[]")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/813_opportunity_consolidation.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/813_opportunity_consolidation.json")
