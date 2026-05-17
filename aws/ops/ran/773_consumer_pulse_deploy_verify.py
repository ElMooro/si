"""ops/773 — force-deploy + verify justhodl-consumer-pulse (roadmap #4).

Deterministically deploys the function from repo source via boto3 (create or
update), wires the daily schedule, invokes, and validates the output —
robust whether or not deploy-lambdas.yml already created it.
"""
import json, os, io, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)

FN = "justhodl-consumer-pulse"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACC, REGION = "857687956942", "us-east-1"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
RULE = "consumer-pulse-daily"
FRED_KEY = "2f057499936072679d8843d7fce99989"

report = {"ops": 773, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Force-deploy + verify consumer-pulse (roadmap #4)"}

# build zip
try:
    code = open(SRC, encoding="utf-8").read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", code)
    zip_bytes = buf.getvalue()
    report["zip_bytes"] = len(zip_bytes)
except Exception as e:
    report["fatal"] = f"zip failed: {e}"
    json.dump(report, open("aws/ops/reports/773_consumer_pulse_deploy_verify.json", "w"), indent=2)
    raise SystemExit(0)

# create or update
exists = False
try:
    lam.get_function_configuration(FunctionName=FN)
    exists = True
except lam.exceptions.ResourceNotFoundException:
    exists = False
except Exception as e:
    report["precheck_err"] = str(e)[:160]

try:
    if exists:
        lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes)
        report["deploy_action"] = "updated existing"
    else:
        lam.create_function(
            FunctionName=FN, Runtime="python3.12", Role=ROLE,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes}, Timeout=90, MemorySize=256,
            Description=("Consumer & Labour Pulse — consumer-health composite "
                         "+ Indeed job-postings alt-data labour lead."),
            Environment={"Variables": {"FRED_API_KEY": FRED_KEY}},
            Architectures=["x86_64"])
        report["deploy_action"] = "created new"
except Exception as e:
    report["deploy_error"] = f"{type(e).__name__}: {str(e)[:300]}"

# wait active/settled
state = None
for _ in range(35):
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        state = c.get("State")
        if state == "Active" and c.get("LastUpdateStatus") in ("Successful", None):
            break
    except Exception as e:
        state = f"poll-err:{str(e)[:70]}"
    time.sleep(3)
report["function_state"] = state

# ensure env + timeout (in case it pre-existed without them)
if state == "Active":
    try:
        lam.update_function_configuration(
            FunctionName=FN, Timeout=90,
            Environment={"Variables": {"FRED_API_KEY": FRED_KEY}})
        for _ in range(20):
            if lam.get_function_configuration(
                    FunctionName=FN).get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
    except Exception as e:
        report["config_err"] = str(e)[:160]

# wire schedule
if state == "Active":
    try:
        events.put_rule(Name=RULE, ScheduleExpression="cron(40 12 * * ? *)",
                        State="ENABLED",
                        Description="Consumer & Labour Pulse — daily 12:40 UTC")
        try:
            lam.add_permission(
                FunctionName=FN, StatementId=f"EventBridge-{RULE}",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACC}:rule/{RULE}")
        except lam.exceptions.ResourceConflictException:
            pass
        events.put_targets(Rule=RULE, Targets=[
            {"Id": "target1",
             "Arn": f"arn:aws:lambda:{REGION}:{ACC}:function:{FN}"}])
        report["schedule_wired"] = True
    except Exception as e:
        report["schedule_wired"] = False
        report["schedule_err"] = str(e)[:200]

# invoke + verify (retry to clear cold-start transients)
cp = {}
if state == "Active":
    for attempt in range(3):
        try:
            r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                           Payload=b"{}")
            payload = json.loads(r["Payload"].read() or b"{}")
            report[f"invoke_{attempt+1}"] = {"status": r.get("StatusCode"),
                                             "fn_error": r.get("FunctionError"),
                                             "body": payload.get("body")}
        except Exception as e:
            report[f"invoke_{attempt+1}"] = {"err": str(e)[:200]}
        try:
            cp = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                            Key="data/consumer-pulse.json")["Body"].read())
        except Exception as e:
            report["read_err"] = str(e)[:200]
        if cp.get("ok") is True:
            break
        time.sleep(7)

comps = cp.get("components", []) or []
report["output"] = {"ok": cp.get("ok"), "pulse_index": cp.get("pulse_index"),
                    "pulse_z": cp.get("pulse_z"), "regime": cp.get("regime"),
                    "momentum": cp.get("momentum"), "headline": cp.get("headline"),
                    "n_ok": cp.get("n_ok"), "errors": cp.get("errors")}
report["sub_indices"] = cp.get("sub_indices")
report["divergence"] = cp.get("divergence")
report["lead_signal"] = cp.get("lead_signal")
report["official_cross_ref"] = cp.get("official_labour_cross_ref")
report["components"] = [
    {"series": c.get("series"), "name": c.get("name"), "group": c.get("group"),
     "latest": c.get("latest"), "latest_date": c.get("latest_date"),
     "contribution": c.get("contribution"),
     "signal_label": c.get("signal_label")} for c in comps]

idx = cp.get("pulse_index")
checks = {
    "function_active": state == "Active",
    "schedule_wired": report.get("schedule_wired") is True,
    "no_fn_error": not any(report.get(f"invoke_{i}", {}).get("fn_error")
                           for i in (1, 2, 3) if report.get(f"invoke_{i}")),
    "output_ok": cp.get("ok") is True,
    "index_in_range": isinstance(idx, (int, float)) and 0 <= idx <= 100,
    "has_regime": cp.get("regime") in ("STRONG", "FIRM", "STEADY",
                                       "SOFTENING", "WEAK"),
    "two_sub_indices": isinstance(cp.get("sub_indices"), dict)
                       and len(cp.get("sub_indices") or {}) >= 2,
    "real_fred_data": len(comps) >= 5
                      and all(c.get("latest") is not None for c in comps),
    "divergence_present": isinstance(cp.get("divergence"), dict),
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "CONSUMER & LABOUR PULSE LIVE & VERIFIED — deployed, scheduled daily, "
    "consumer-health + alt-data job-postings composites computing on real "
    "FRED data, divergence + official cross-ref working. Roadmap #4 COMPLETE "
    "— all four enhancement engines now live."
    if report["all_pass"] else "REVIEW — see checks[]/output/components")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/773_consumer_pulse_deploy_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/773_consumer_pulse_deploy_verify.json")
