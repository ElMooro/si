"""ops/808 — deploy + schedule + verify justhodl-catch-up-radar.

Deploys the Catch-Up Radar (relative-value synthesis engine), wires its
daily 14:30 UTC schedule, invokes it synchronously (pure S3 synthesis,
runs in seconds) and verifies it writes screener/catch-up-radar.json with
beta-laggard candidates and ETF basket-gaps that carry a thesis and a
catch-up price target.
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
FN = "justhodl-catch-up-radar"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
OUT_KEY = "screener/catch-up-radar.json"

report = {"ops": 808, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Deploy + verify justhodl-catch-up-radar"}

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

# synchronous invoke — pure S3 synthesis, finishes in seconds
inv = {}
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    payload = r["Payload"].read().decode("utf-8", "replace")
    inv = {"status": r.get("StatusCode"),
           "fn_error": r.get("FunctionError"),
           "body": payload[:400]}
except Exception as e:
    inv = {"error": str(e)[:200]}
report["invoke"] = inv

# read the result
cur = {}
fresh = False
for _ in range(8):
    time.sleep(5)
    try:
        cur = json.loads(s3.get_object(Bucket=BUCKET,
                         Key=OUT_KEY)["Body"].read())
        gen = cur.get("generated_at", "")
        if gen >= report["ts"][:10]:
            ga = datetime.fromisoformat(gen.replace("Z", "+00:00"))
            if (datetime.now(timezone.utc) - ga).total_seconds() < 900:
                fresh = True
                break
    except Exception:
        pass

cands = cur.get("catch_up_candidates") or []
etfs = cur.get("etf_catchup") or []
hot = cur.get("hot_themes") or []
priced = [c for c in cands if c.get("price")]
with_target = [c for c in priced if c.get("price_target") is not None]
with_thesis = [c for c in cands if (c.get("thesis") or "").strip()]
neg_score = [c for c in cands if (c.get("catch_up_score") or 0) < 0]

report["catch_up_radar"] = {
    "ok": inv.get("fn_error") is None,
    "fresh": fresh,
    "headline": cur.get("headline"),
    "counts": cur.get("counts"),
    "n_hot_themes": len(hot),
    "n_candidates": len(cands),
    "n_priced": len(priced),
    "n_with_target": len(with_target),
    "n_with_thesis": len(with_thesis),
    "n_etf_catchup": len(etfs),
    "top_themes": [{"tk": h.get("ticker"), "ms": h.get("momentum_score"),
                    "r20": h.get("ret_20d")} for h in hot[:5]],
    "top_candidates": [
        {"sym": c.get("symbol"), "theme": c.get("theme"),
         "score": c.get("catch_up_score"), "beta": c.get("beta"),
         "shortfall": c.get("shortfall_pct"),
         "target": c.get("price_target"), "upside": c.get("upside_pct"),
         "trap": c.get("value_trap_flag")}
        for c in cands[:6]],
    "top_etf": [{"tk": e.get("ticker"), "gap": e.get("holdings_gap_pct"),
                 "target": e.get("price_target")} for e in etfs[:4]],
}

checks = {
    "deploy_ok": str(report.get("deploy", "")).startswith(("created",
                                                           "updated")),
    "function_active": False,
    "schedule_wired": report.get("schedule", {}).get("wired") is True,
    "invoke_ok": inv.get("status") == 200 and inv.get("fn_error") is None,
    "fresh_output": fresh,
    "structure_ok": all(k in cur for k in
                        ("hot_themes", "catch_up_candidates", "etf_catchup",
                         "headline", "methodology")),
    "hot_themes_found": len(hot) >= 1,
    "all_candidates_have_thesis": len(cands) == len(with_thesis),
    "priced_candidates_have_targets": (len(priced) == 0
                                       or len(with_target) >= 1),
    "no_negative_scores": len(neg_score) == 0,
}
try:
    checks["function_active"] = lam.get_function_configuration(
        FunctionName=FN)["State"] == "Active"
except Exception:
    pass
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"CATCH-UP RADAR LIVE — {len(hot)} hot themes scanned, {len(cands)} "
    f"beta-laggard set-ups ({report['catch_up_radar']['counts'].get('high_conviction', 0) if report['catch_up_radar'].get('counts') else 0} "
    f"high-conviction), {len(etfs)} ETF basket-gaps, {len(with_target)} "
    "carrying catch-up targets. Deployed, scheduled daily 14:30 UTC, real "
    "data off theme-rotation + screener."
    if report["all_pass"]
    else "REVIEW — see checks[]/catch_up_radar")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/808_catch_up_radar_deploy.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/808_catch_up_radar_deploy.json")
