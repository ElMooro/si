# ops 1610 — create/update justhodl-insider-radar, schedule, invoke, verify; board redeploy
import json, zipfile, io, os, time, base64
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1610}
def zipdir(srcdir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, fs in os.walk(srcdir):
            for f in fs:
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, srcdir))
    return buf.getvalue()
def ready(fn):
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") != "InProgress" and c.get("State") != "Pending":
            return
        time.sleep(3)
FN = "justhodl-insider-radar"
zb = zipdir("aws/lambdas/justhodl-insider-radar/source")
role = lam.get_function_configuration(FunctionName="justhodl-historical-analogs")["Role"]
env = {"FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"}
try:
    lam.create_function(FunctionName=FN, Runtime="python3.12", Role=role,
                         Handler="lambda_function.lambda_handler", Code={"ZipFile": zb},
                         Timeout=300, MemorySize=512, Environment={"Variables": env})
    out["fn"] = "created"
except Exception as e:
    if "exist" in str(e) or "ResourceConflict" in str(e):
        for _ in range(6):
            try:
                lam.update_function_code(FunctionName=FN, ZipFile=zb); break
            except Exception as e2:
                if "ResourceConflict" in str(e2): time.sleep(8)
                else: raise
        out["fn"] = "updated"
    else:
        raise
ready(FN)
rule = "insider-radar-daily"
ev.put_rule(Name=rule, ScheduleExpression="cron(40 14 * * ? *)", State="ENABLED")
try:
    lam.add_permission(FunctionName=FN, StatementId="evt-insider-daily",
                        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                        SourceArn=ev.describe_rule(Name=rule)["Arn"])
except Exception as e:
    if "ResourceConflict" not in str(e):
        out["perm_warn"] = str(e)[:80]
ev.put_targets(Rule=rule, Targets=[{"Id": "1", "Arn":
    lam.get_function_configuration(FunctionName=FN)["FunctionArn"]}])
r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
out["fn_err"] = r.get("FunctionError", "NONE")
try:
    out["log_tail"] = base64.b64decode(r.get("LogResult", "")).decode(errors="replace")[-1400:]
except Exception:
    out["log_tail"] = None
d = json.loads(s3.get_object(Bucket=B, Key="data/insider-radar.json")["Body"].read())
out["verify"] = {"source_used": d.get("source_used"), "n_raw": d.get("n_raw"),
                  "n_buys": d.get("n_buys"), "n_sells": d.get("n_sells"),
                  "diagnostics": d.get("diagnostics"),
                  "buys_head": (d.get("latest_buys") or [])[:6],
                  "clusters": (d.get("clusters") or [])[:6],
                  "decline_clusters": d.get("decline_clusters"),
                  "decline_buys_head": (d.get("decline_buys") or [])[:5],
                  "logged": d.get("logged")}
zb2 = zipdir("aws/lambdas/justhodl-signal-board/source")
for _ in range(6):
    try:
        lam.update_function_code(FunctionName="justhodl-signal-board", ZipFile=zb2); break
    except Exception as e:
        if "ResourceConflict" in str(e): time.sleep(8)
        else: raise
ready("justhodl-signal-board")
r2 = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse", Payload=b"{}")
out["sb_err"] = r2.get("FunctionError", "NONE")
sb = json.loads(s3.get_object(Bucket=B, Key="data/signal-board.json")["Body"].read())
out["board_n"] = len(sb.get("engines") or [])
out["board_row"] = next(({k: e.get(k) for k in ("signal", "signal_label", "read")}
                          for e in (sb.get("engines") or []) if e.get("engine") == "Insider Radar"),
                         "MISSING")
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1610_insider_radar.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"fn": out["fn"], "fn_err": out["fn_err"], "src": out["verify"]["source_used"],
                   "buys": out["verify"]["n_buys"], "board": out["board_row"]}, default=str))
