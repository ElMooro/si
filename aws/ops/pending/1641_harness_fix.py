# ops 1640 — create backtest-harness fn, weekly cron, invoke, verify; board redeploy
import json, zipfile, io, os, time, base64
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1641}
def zipdir(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, fs in os.walk(d):
            for f in fs:
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()
def upd(fn, zb):
    for _ in range(6):
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=zb); break
        except Exception as e:
            if "ResourceConflict" in str(e): time.sleep(8)
            else: raise
    for _ in range(50):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") != "InProgress" and c.get("State") != "Pending":
            return
        time.sleep(3)
FN = "justhodl-backtest-harness"
zb = zipdir("aws/lambdas/justhodl-backtest-harness/source")
role = lam.get_function_configuration(FunctionName="justhodl-historical-analogs")["Role"]
try:
    lam.create_function(FunctionName=FN, Runtime="python3.12", Role=role,
                         Handler="lambda_function.lambda_handler", Code={"ZipFile": zb},
                         Timeout=600, MemorySize=1024,
                         Environment={"Variables": {"FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"}})
    out["fn"] = "created"
except Exception as e:
    if "exist" in str(e) or "ResourceConflict" in str(e):
        upd(FN, zb); out["fn"] = "updated"
    else:
        raise
for _ in range(50):
    c = lam.get_function_configuration(FunctionName=FN)
    if c.get("State") != "Pending" and c.get("LastUpdateStatus") != "InProgress":
        break
    time.sleep(3)
rule = "backtest-harness-weekly"
ev.put_rule(Name=rule, ScheduleExpression="cron(10 12 ? * SUN *)", State="ENABLED")
try:
    lam.add_permission(FunctionName=FN, StatementId="evt-bt-weekly",
                        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                        SourceArn=ev.describe_rule(Name=rule)["Arn"])
except Exception as e:
    if "ResourceConflict" not in str(e):
        out["perm_warn"] = str(e)[:80]
ev.put_targets(Rule=rule, Targets=[{"Id": "1",
    "Arn": lam.get_function_configuration(FunctionName=FN)["FunctionArn"]}])
r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
out["err"] = r.get("FunctionError", "NONE")
out["log_tail"] = base64.b64decode(r.get("LogResult", "")).decode(errors="replace")[-700:]
d = json.loads(s3.get_object(Bucket=B, Key="data/backtest-harness.json")["Body"].read())
out["verify"] = {"version": d.get("version"), "duration_s": d.get("duration_s"),
                  "universe": d.get("universe_n"), "days": d.get("days"),
                  "n_pass": d.get("n_pass"), "diag": d.get("diagnostics"),
                  "rules": [{k: r2.get(k) for k in ("rule", "PASS", "deflated_gate_sr")}
                             | {"sr": (r2.get("oos") or {}).get("sr"),
                                 "n": (r2.get("oos") or {}).get("n"),
                                 "hit": (r2.get("oos") or {}).get("hit"),
                                 "avg": (r2.get("oos") or {}).get("avg"),
                                 "mdd": (r2.get("oos") or {}).get("maxdd")}
                             for r2 in (d.get("rules") or [])],
                  "live_types": (d.get("live_signal_types") or [])[:10]}
upd("justhodl-signal-board", zipdir("aws/lambdas/justhodl-signal-board/source"))
lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse", Payload=b"{}")
sb = json.loads(s3.get_object(Bucket=B, Key="data/signal-board.json")["Body"].read())
out["board_row"] = next(({"read": e.get("read")} for e in (sb.get("engines") or [])
                           if e.get("engine") == "Backtest Harness"), "MISSING")
out["board_n"] = len(sb.get("engines") or [])
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1641_harness_fix.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"err": out["err"], "n_pass": out["verify"]["n_pass"],
                   "dur": out["verify"]["duration_s"]}, default=str))
