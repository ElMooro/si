"""ops/807 — deploy + schedule + verify justhodl-boom-radar.

Deploys the Hypergrowth Breakout Radar, wires its daily schedule, runs it
asynchronously (the FMP universe scan takes 1-2 min) and verifies it writes
data/boom-radar.json with scored picks that carry price targets.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=120, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-boom-radar"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"

report = {"ops": 807, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Deploy + verify justhodl-boom-radar"}

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
        for _ in range(40):
            if lam.get_function_configuration(
                    FunctionName=FN).get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        lam.update_function_configuration(
            FunctionName=FN, Handler=CONF["handler"], Runtime=CONF["runtime"],
            Role=ROLE, Timeout=CONF["timeout"], MemorySize=CONF["memory"],
            Environment={"Variables": CONF.get("environment", {})},
            Description=CONF["description"][:255])
        report["deploy"] = "updated"
    else:
        lam.create_function(
            FunctionName=FN, Runtime=CONF["runtime"], Role=ROLE,
            Handler=CONF["handler"], Timeout=CONF["timeout"],
            MemorySize=CONF["memory"], Architectures=CONF["architectures"],
            Description=CONF["description"][:255],
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
    report["schedule"] = {"error": str(e)[:160]}

# async invoke — the FMP universe scan runs 1-2 min
try:
    r = lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    report["invoke"] = {"async_status": r.get("StatusCode")}
except Exception as e:
    report["invoke"] = {"error": str(e)[:200]}

# poll S3 for a fresh result
br = {}
fresh = False
for _ in range(24):
    time.sleep(15)
    try:
        br = json.loads(s3.get_object(Bucket=BUCKET,
                        Key="data/boom-radar.json")["Body"].read())
        gen = br.get("generated_at", "")
        if gen >= report["ts"][:10]:
            ga = datetime.fromisoformat(gen.replace("Z", "+00:00"))
            if (datetime.now(timezone.utc) - ga).total_seconds() < 900:
                fresh = True
                break
    except Exception:
        pass

picks = br.get("picks") or []
with_target = [p for p in picks if p.get("price_target") is not None]
report["boom_radar"] = {
    "ok": br.get("ok"), "fresh": fresh,
    "headline": br.get("headline"),
    "universe_size": br.get("universe_size"),
    "n_scanned": br.get("n_scanned"),
    "n_qualified": br.get("n_qualified"),
    "n_prime": br.get("n_prime"), "n_strong": br.get("n_strong"),
    "n_with_target": len(with_target),
    "top5": [{"sym": p.get("symbol"), "score": p.get("boom_score"),
              "grade": p.get("grade"), "rev_yoy": p.get("rev_growth_yoy_pct"),
              "beats": p.get("beat_streak"), "peg": p.get("peg"),
              "target": p.get("price_target"), "upside": p.get("upside_pct")}
             for p in picks[:5]],
}

checks = {
    "deploy_ok": str(report.get("deploy", "")).startswith(("created",
                                                           "updated")),
    "function_active": False,
    "schedule_wired": report.get("schedule", {}).get("wired") is True,
    "invoke_dispatched": report.get("invoke", {}).get("async_status")
    in (200, 202),
    "fresh_output": fresh,
    "output_ok": br.get("ok") is True,
    "picks_produced": len(picks) >= 5,
    "targets_present": len(with_target) >= 3,
}
try:
    checks["function_active"] = lam.get_function_configuration(
        FunctionName=FN)["State"] == "Active"
except Exception:
    pass
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"BOOM-RADAR LIVE — scanned {br.get('n_scanned')} small/mid-caps, "
    f"{br.get('n_qualified')} qualified ({br.get('n_prime')} PRIME, "
    f"{br.get('n_strong')} STRONG), {len(with_target)} with price targets. "
    "Deployed, scheduled daily 14:00 UTC, real FMP data."
    if report["all_pass"]
    else "REVIEW — see checks[]/boom_radar")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/807_boom_radar_deploy.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/807_boom_radar_deploy.json")
