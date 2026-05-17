"""ops/779 — force-deploy + verify justhodl-cb-injection."""
import json, os, io, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)

FN = "justhodl-cb-injection"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACC, REGION = "857687956942", "us-east-1"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
RULE = "cb-injection-daily"

report = {"ops": 779, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Force-deploy + verify cb-injection (CB capital-injection & carry)"}

try:
    code = open(SRC, encoding="utf-8").read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", code)
    zip_bytes = buf.getvalue()
except Exception as e:
    report["fatal"] = f"zip failed: {e}"
    json.dump(report, open("aws/ops/reports/779_cb_injection_deploy_verify.json", "w"), indent=2)
    raise SystemExit(0)

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
            Description="Central Bank Capital-Injection & Carry Engine.",
            Environment={"Variables": {
                "FRED_API_KEY": "2f057499936072679d8843d7fce99989"}},
            Architectures=["x86_64"])
        report["deploy_action"] = "created new"
except Exception as e:
    report["deploy_error"] = f"{type(e).__name__}: {str(e)[:300]}"

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

if state == "Active":
    try:
        lam.update_function_configuration(
            FunctionName=FN, Timeout=90,
            Environment={"Variables": {
                "FRED_API_KEY": "2f057499936072679d8843d7fce99989"}})
        for _ in range(20):
            if lam.get_function_configuration(
                    FunctionName=FN).get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
    except Exception as e:
        report["config_err"] = str(e)[:160]

    try:
        events.put_rule(Name=RULE, ScheduleExpression="cron(0 13 * * ? *)",
                        State="ENABLED",
                        Description="CB Capital-Injection & Carry — daily 13:00 UTC")
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

    cb = {}
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
            cb = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                            Key="data/cb-injection.json")["Body"].read())
        except Exception as e:
            report["read_err"] = str(e)[:200]
        if cb.get("ok") is True:
            break
        time.sleep(7)

    banks = cb.get("central_banks", []) or []
    report["output"] = {
        "ok": cb.get("ok"), "headline": cb.get("headline"),
        "global_injection_impulse": cb.get("global_injection_impulse"),
        "eurodollar_read": cb.get("eurodollar_read"),
        "cross_reference": cb.get("cross_reference"), "errors": cb.get("errors")}
    report["carry_trade"] = cb.get("carry_trade")
    report["central_banks"] = [
        {"cb": b.get("cb"), "stance_label": b.get("stance_label"),
         "injection_stance": b.get("injection_stance"),
         "bs_change_6m_pct": b.get("bs_change_6m_pct"),
         "policy_rate_pct": b.get("policy_rate_pct"),
         "read": b.get("read")} for b in banks]

    ct = cb.get("carry_trade") or {}
    checks = {
        "function_active": state == "Active",
        "schedule_wired": report.get("schedule_wired") is True,
        "no_fn_error": not any(report.get(f"invoke_{i}", {}).get("fn_error")
                               for i in (1, 2, 3) if report.get(f"invoke_{i}")),
        "output_ok": cb.get("ok") is True,
        "four_central_banks": len(banks) >= 4,
        "real_fred_data": any(b.get("bs_change_6m_pct") is not None
                              for b in banks),
        "impulse_computed": isinstance(cb.get("global_injection_impulse"), dict)
                            and cb["global_injection_impulse"].get("label"),
        "carry_synthesis": isinstance(ct.get("unwind_risk_score"), int)
                           and ct.get("carry_conditions") in
                           ("SUPPORTIVE", "NEUTRAL", "STRESSED"),
    }
    report["checks"] = checks
    report["all_pass"] = all(checks.values())
    report["verdict"] = (
        "CB-INJECTION LIVE & VERIFIED — deployed, scheduled daily, computing "
        "per-CB injection stance for ECB/BOJ/Fed/SNB on real FRED data, with "
        "net global liquidity impulse and carry funding + unwind-risk "
        "synthesis. Eurodollar/carry coverage extended."
        if report["all_pass"] else "REVIEW — see checks[]/central_banks")
else:
    report["all_pass"] = False
    report["verdict"] = "DEPLOY FAILED — see deploy_error / function_state"

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/779_cb_injection_deploy_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/779_cb_injection_deploy_verify.json")
