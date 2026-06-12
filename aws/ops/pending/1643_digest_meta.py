# ops 1643 — sentinel v2 daily digest, harness v1.1 daily+graded dump, meta-labeler create+train, board
import json, zipfile, io, os, time, base64
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1643}
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
upd("justhodl-alert-sentinel", zipdir("aws/lambdas/justhodl-alert-sentinel/source"))
lam.invoke(FunctionName="justhodl-alert-sentinel", InvocationType="RequestResponse", Payload=b"{}")
sd = json.loads(s3.get_object(Bucket=B, Key="data/alert-sentinel.json")["Body"].read())
out["sentinel"] = {"version": sd.get("version"), "buffer_n": sd.get("buffer_n"),
                    "last_sent": sd.get("last_sent_date"), "sent": sd.get("message_sent"),
                    "diag": sd.get("diagnostics"), "delivery": sd.get("delivery")}
upd("justhodl-backtest-harness", zipdir("aws/lambdas/justhodl-backtest-harness/source"))
ev.put_rule(Name="backtest-harness-weekly", ScheduleExpression="cron(20 21 * * ? *)",
            State="ENABLED")   # cadence now DAILY 21:20 — meta-labeler needs fresh rows
r = lam.invoke(FunctionName="justhodl-backtest-harness", InvocationType="RequestResponse", Payload=b"{}")
out["harness_err"] = r.get("FunctionError", "NONE")
hd = json.loads(s3.get_object(Bucket=B, Key="data/backtest-harness.json")["Body"].read())
out["harness"] = {"version": hd.get("version"),
                   "graded_diag": [x for x in (hd.get("diagnostics") or []) if "graded" in x]}
FN = "justhodl-meta-labeler"
zb = zipdir("aws/lambdas/justhodl-meta-labeler/source")
role = lam.get_function_configuration(FunctionName="justhodl-historical-analogs")["Role"]
try:
    lam.create_function(FunctionName=FN, Runtime="python3.12", Role=role,
                         Handler="lambda_function.lambda_handler", Code={"ZipFile": zb},
                         Timeout=300, MemorySize=512,
                         Environment={"Variables": {"FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"}})
    out["meta_fn"] = "created"
except Exception as e:
    if "exist" in str(e) or "ResourceConflict" in str(e):
        upd(FN, zb); out["meta_fn"] = "updated"
    else:
        raise
for _ in range(50):
    c = lam.get_function_configuration(FunctionName=FN)
    if c.get("State") != "Pending" and c.get("LastUpdateStatus") != "InProgress":
        break
    time.sleep(3)
ev.put_rule(Name="meta-labeler-daily", ScheduleExpression="cron(50 21 * * ? *)", State="ENABLED")
try:
    lam.add_permission(FunctionName=FN, StatementId="evt-meta-daily",
                        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                        SourceArn=ev.describe_rule(Name="meta-labeler-daily")["Arn"])
except Exception as e:
    if "ResourceConflict" not in str(e):
        out["perm_warn"] = str(e)[:80]
ev.put_targets(Rule="meta-labeler-daily", Targets=[{"Id": "1",
    "Arn": lam.get_function_configuration(FunctionName=FN)["FunctionArn"]}])
r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
out["meta_err"] = r.get("FunctionError", "NONE")
out["meta_log"] = base64.b64decode(r.get("LogResult", "")).decode(errors="replace")[-500:]
md = json.loads(s3.get_object(Bucket=B, Key="data/meta-labeler.json")["Body"].read())
out["meta"] = {"model": md.get("model"), "per_type": md.get("per_type_test"),
                "n_take": md.get("n_take"), "n_gated": md.get("n_pending_gated"),
                "gates_head": (md.get("gates") or [])[:8], "diag": md.get("diagnostics")}
upd("justhodl-signal-board", zipdir("aws/lambdas/justhodl-signal-board/source"))
lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse", Payload=b"{}")
sb = json.loads(s3.get_object(Bucket=B, Key="data/signal-board.json")["Body"].read())
out["board_n"] = len(sb.get("engines") or [])
out["board_row"] = next(({"read": e.get("read"), "sig": e.get("signal")}
                           for e in (sb.get("engines") or []) if e.get("engine") == "Meta-Labeler"),
                          "MISSING")
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1643_digest_meta.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"buf": out["sentinel"]["buffer_n"], "meta_err": out["meta_err"],
                   "uplift": (out["meta"]["model"] or {}).get("uplift_pp")}, default=str))
