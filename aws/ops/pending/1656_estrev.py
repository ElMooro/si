# ops 1656 — create estimate-revisions fn (daily 13:40 + 17:40), invoke x2 (cursor coverage),
#            redeploy valuations/board/sentinel, re-invoke valuations, verify joins
import json, zipfile, io, os, time
import boto3
from botocore.config import Config
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1656}
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
FN = "justhodl-estimate-revisions"
zb = zipdir("aws/lambdas/justhodl-estimate-revisions/source")
role = lam.get_function_configuration(FunctionName="justhodl-historical-analogs")["Role"]
try:
    lam.create_function(FunctionName=FN, Runtime="python3.12", Role=role,
                         Handler="lambda_function.lambda_handler", Code={"ZipFile": zb},
                         Timeout=290, MemorySize=512,
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
arn = lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
for rule, sched, sid in (("estrev-daily-am", "cron(40 13 ? * MON-FRI *)", "evt-am"),
                           ("estrev-daily-pm", "cron(40 17 ? * MON-FRI *)", "evt-pm")):
    ev.put_rule(Name=rule, ScheduleExpression=sched, State="ENABLED")
    try:
        lam.add_permission(FunctionName=FN, StatementId=sid,
                            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                            SourceArn=ev.describe_rule(Name=rule)["Arn"])
    except Exception as e:
        if "ResourceConflict" not in str(e):
            out.setdefault("perm_warn", []).append(str(e)[:60])
    ev.put_targets(Rule=rule, Targets=[{"Id": "1", "Arn": arn}])
covs = []
for _ in range(2):
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    covs.append(json.loads(json.loads(r["Payload"].read()).get("body", "{}")))
out["invokes"] = covs
d = json.loads(s3.get_object(Bucket=B, Key="data/estimate-revisions.json")["Body"].read())
tk = d.get("tickers") or {}
gsam = sorted(((s_, v["est_g_pct"]) for s_, v in tk.items()
                 if v.get("est_g_pct") is not None), key=lambda x: -x[1])
out["estrev"] = {"coverage": d.get("coverage"), "universe": d.get("universe"),
                  "breadth": d.get("breadth"), "diag": d.get("diagnostics"),
                  "fwd_growth_n": len(gsam), "top_fwd_growth": gsam[:8],
                  "sample_AAPL": tk.get("AAPL"), "sample_OPRA": tk.get("OPRA")}
for fn2 in ("justhodl-stock-valuations", "justhodl-signal-board", "justhodl-alert-sentinel"):
    upd(fn2, zipdir(f"aws/lambdas/{fn2}/source"))
r = lam.invoke(FunctionName="justhodl-stock-valuations", InvocationType="RequestResponse", Payload=b"{}")
out["val_err"] = r.get("FunctionError", "NONE")
sv = json.loads(s3.get_object(Bucket=B, Key="data/stock-valuations.json")["Body"].read())
out["val_join"] = {"version": sv.get("version"),
                    "sp_with_estg": sum(1 for r2 in (sv.get("sp_table") or []) if r2.get("est_g") is not None),
                    "hp_with_estg": sum(1 for x in (sv.get("hp") or [])
                                          if (x.get("metrics") or {}).get("est_g_pct") is not None),
                    "aapl_estg": next((r2.get("est_g") for r2 in (sv.get("sp_table") or [])
                                         if r2.get("t") == "AAPL"), None)}
lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse", Payload=b"{}")
sb = json.loads(s3.get_object(Bucket=B, Key="data/signal-board.json")["Body"].read())
out["board_n"] = len(sb.get("engines") or [])
out["board_row"] = next(({"read": e.get("read"), "sig": e.get("signal")}
                           for e in (sb.get("engines") or []) if e.get("engine") == "Estimate Revisions"),
                          "MISSING")
lam.invoke(FunctionName="justhodl-alert-sentinel", InvocationType="RequestResponse", Payload=b"{}")
sd = json.loads(s3.get_object(Bucket=B, Key="data/alert-sentinel.json")["Body"].read())
out["sentinel"] = {"version": sd.get("version"), "buffer_n": sd.get("buffer_n")}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1656_estrev.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"cov": out["estrev"]["coverage"], "val_join": out["val_join"]["sp_with_estg"],
                   "board": out["board_n"]}, default=str))
