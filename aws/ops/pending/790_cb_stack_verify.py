# re-trigger run-ops
"""ops/790 — ensure justhodl-snb-detail is live, then verify the full
central-bank coverage stack for the eurodollar / carry system.

The SNB engine was built by the parallel pipeline (commit f021038) with its
deploy staged at ops 789. This script idempotently guarantees snb-detail is
deployed + scheduled (safe whether or not 789 has already run), invokes it,
and produces one consolidated audit of all three desk-depth CB engines —
ecb-detail, boj-detail, snb-detail — plus the cb-injection synthesis.
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
FN = "justhodl-snb-detail"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

report = {"ops": 790, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Ensure snb-detail live + verify full CB coverage stack"}

# ── 1. idempotent deploy of snb-detail ──
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
        report["deploy"] = "updated"
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

# ── 2. wire the daily schedule ──
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
    report["schedule"] = {"rule": sch["rule_name"], "cron": sch["cron"],
                          "wired": True}
except Exception as e:
    report["schedule"] = {"error": f"{type(e).__name__}: {str(e)[:160]}"}

# ── 3. invoke snb-detail ──
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": json.loads(r["Payload"].read() or b"{}").get(
                            "body")}
except Exception as e:
    report["invoke"] = {"error": str(e)[:200]}

time.sleep(3)


# ── 4. audit the full CB coverage stack ──
def grab(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                          Key=key)["Body"].read())
    except Exception as e:
        return {"_err": str(e)[:120]}


def age_h(d):
    g = (d or {}).get("generated_at")
    if not g:
        return None
    try:
        t = datetime.fromisoformat(g.replace("Z", "+00:00"))
        return round((datetime.now(timezone.utc) - t).total_seconds() / 3600,
                     1)
    except Exception:
        return None


snb = grab("data/snb-detail.json")
boj = grab("data/boj-detail.json")
ecb = grab("data/ecb-detail.json")
cbi = grab("data/cb-injection.json")

shp = snb.get("chf_safe_haven_pressure") or {}
cur = boj.get("carry_unwind_risk") or {}
report["stack"] = {
    "snb_detail": {"ok": snb.get("ok"), "age_h": age_h(snb),
                   "stance": snb.get("stance_label"),
                   "chf_safe_haven_pressure": shp.get("score_0_100"),
                   "regime": shp.get("regime"),
                   "headline": snb.get("headline"),
                   "errors": snb.get("errors")},
    "boj_detail": {"ok": boj.get("ok"), "age_h": age_h(boj),
                   "stance": boj.get("stance_label"),
                   "yen_carry_unwind_risk": cur.get("score_0_100"),
                   "regime": cur.get("regime")},
    "ecb_detail": {"ok": ecb.get("ok"), "age_h": age_h(ecb),
                   "stance": ecb.get("stance_label"),
                   "headline": ecb.get("headline")},
    "cb_injection": {"ok": cbi.get("ok"), "age_h": age_h(cbi),
                     "global_impulse": (cbi.get("global_injection_impulse")
                                        or {}).get("label")},
}

# ── 5. verdict ──
checks = {
    "snb_deployed": str(report.get("deploy", "")).startswith(("created",
                                                              "updated")),
    "snb_active": False,
    "snb_scheduled": report.get("schedule", {}).get("wired") is True,
    "snb_invoke_ok": report.get("invoke", {}).get("status") == 200
                     and not report.get("invoke", {}).get("fn_error"),
    "snb_output_ok": snb.get("ok") is True,
    "snb_score_computed": isinstance(shp.get("score_0_100"), (int, float)),
    "boj_detail_live": boj.get("ok") is True,
    "ecb_detail_live": ecb.get("ok") is True,
    "cb_injection_live": cbi.get("ok") is True,
}
try:
    checks["snb_active"] = lam.get_function_configuration(
        FunctionName=FN)["State"] == "Active"
except Exception:
    pass
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"CB COVERAGE STACK COMPLETE — ecb-detail, boj-detail and snb-detail all "
    f"live + fresh, plus the cb-injection synthesis. SNB: "
    f"{snb.get('stance_label')}, CHF safe-haven pressure "
    f"{shp.get('score_0_100')}/100 ({shp.get('regime')}). The eurodollar / "
    "carry-trade central-bank picture is now covered end to end: ECB "
    "(Eurosystem liquidity), BOJ (yen carry — the primary funding currency), "
    "SNB (Swiss franc — the secondary funding leg & safe haven)."
    if report["all_pass"]
    else "REVIEW — see checks[]/stack/invoke")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/790_cb_stack_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/790_cb_stack_verify.json")
