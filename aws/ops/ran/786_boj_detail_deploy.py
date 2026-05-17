"""ops/786 — deploy + schedule + verify justhodl-boj-detail, and pull the
live ECB/CB outputs for a full central-bank-coverage status report.

Force-deploys the BOJ engine via boto3 (reliable new-function path), wires
the daily EventBridge schedule, invokes it, and confirms the yen-carry
engine writes data/boj-detail.json with real FRED data.
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

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-boj-detail"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"

report = {"ops": 786, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Deploy + verify justhodl-boj-detail (yen-carry engine)"}

# ── 1. build zip ──
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
zip_bytes = buf.getvalue()

# ── 2. create or update the function ──
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

# ── 3. wire the daily EventBridge schedule ──
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

# ── 5. read the BOJ output ──
boj = {}
try:
    boj = json.loads(s3.get_object(Bucket=BUCKET,
                     Key="data/boj-detail.json")["Body"].read())
except Exception as e:
    report["boj_read_err"] = str(e)[:200]

cur = boj.get("carry_unwind_risk") or {}
report["boj_detail"] = {
    "ok": boj.get("ok"), "headline": boj.get("headline"),
    "stance": boj.get("stance_label"),
    "carry_unwind_risk": cur.get("score_0_100"),
    "carry_regime": cur.get("regime"),
    "components": cur.get("components"),
    "balance_sheet_jpy_tn": (boj.get("balance_sheet") or {}).get(
        "total_assets_jpy_tn"),
    "policy_rate_pct": (boj.get("policy_rate") or {}).get("policy_rate_pct"),
    "jgb_10y_pct": (boj.get("jgb_10y") or {}).get("yield_pct"),
    "differential_pp": (boj.get("rate_differential") or {}).get(
        "differential_pp"),
    "usdjpy": (boj.get("usdjpy") or {}).get("level"),
    "errors": boj.get("errors"),
}

# ── 6. pull the sibling CB engines for the coverage report ──
def grab(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                          Key=key)["Body"].read())
    except Exception:
        return {}

ecb = grab("data/ecb-detail.json")
cbi = grab("data/cb-injection.json")
report["cb_coverage"] = {
    "ecb_detail": {"ok": ecb.get("ok"), "headline": ecb.get("headline"),
                   "generated_at": ecb.get("generated_at")},
    "cb_injection": {"ok": cbi.get("ok"),
                     "global_impulse": (cbi.get("global_injection_impulse")
                                        or {}).get("label"),
                     "generated_at": cbi.get("generated_at")},
}

# ── 7. verdict ──
checks = {
    "deploy_ok": str(report.get("deploy", "")).startswith(("created",
                                                           "updated")),
    "function_active": False,
    "schedule_wired": report.get("schedule", {}).get("wired") is True,
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": boj.get("ok") is True,
    "carry_score_computed": isinstance(cur.get("score_0_100"), (int, float)),
    "real_fred_data": len(boj.get("errors") or []) <= 2
                      and boj.get("balance_sheet", {}).get(
                          "total_assets_jpy_tn") is not None,
}
try:
    c = lam.get_function_configuration(FunctionName=FN)
    checks["function_active"] = c.get("State") == "Active"
except Exception:
    pass
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"BOJ-DETAIL LIVE — yen-carry-unwind risk {cur.get('score_0_100')}/100 "
    f"({cur.get('regime')}); BOJ {boj.get('stance_label')}; USD/JPY "
    f"{(boj.get('usdjpy') or {}).get('level')}. Japan now covered at the same "
    "depth as the ECB engine; deployed, scheduled daily 11:20 UTC, real FRED "
    "data. Central-bank coverage for the eurodollar/carry system is complete: "
    "ECB-detail + BOJ-detail + cb-injection (ECB/BOJ/Fed/SNB synthesis)."
    if report["all_pass"] else "REVIEW — see checks[]/invoke/boj_detail")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/786_boj_detail_deploy.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/786_boj_detail_deploy.json")
