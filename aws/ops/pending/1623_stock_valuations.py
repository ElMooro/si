# ops 1623 — create justhodl-stock-valuations, cron 14:50, invoke (long first build), verify both layers
import json, zipfile, io, os, time, base64
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1623}
def zipdir(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, fs in os.walk(d):
            for f in fs:
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()
def ready(fn):
    for _ in range(50):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") != "InProgress" and c.get("State") != "Pending":
            return
        time.sleep(3)
FN = "justhodl-stock-valuations"
zb = zipdir("aws/lambdas/justhodl-stock-valuations/source")
role = lam.get_function_configuration(FunctionName="justhodl-historical-analogs")["Role"]
try:
    lam.create_function(FunctionName=FN, Runtime="python3.12", Role=role,
                         Handler="lambda_function.lambda_handler", Code={"ZipFile": zb},
                         Timeout=600, MemorySize=640,
                         Environment={"Variables": {"FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"}})
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
rule = "stock-valuations-daily"
ev.put_rule(Name=rule, ScheduleExpression="cron(50 14 * * ? *)", State="ENABLED")
try:
    lam.add_permission(FunctionName=FN, StatementId="evt-stockval-daily",
                        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                        SourceArn=ev.describe_rule(Name=rule)["Arn"])
except Exception as e:
    if "ResourceConflict" not in str(e):
        out["perm_warn"] = str(e)[:80]
ev.put_targets(Rule=rule, Targets=[{"Id": "1", "Arn":
    lam.get_function_configuration(FunctionName=FN)["FunctionArn"]}])
r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
out["fn_err"] = r.get("FunctionError", "NONE")
out["log_tail"] = base64.b64decode(r.get("LogResult", "")).decode(errors="replace")[-1400:]
d = json.loads(s3.get_object(Bucket=B, Key="data/stock-valuations.json")["Body"].read())
sp = d.get("sp_table") or []
out["verify"] = {"version": d.get("version"), "duration_s": d.get("duration_s"),
                  "diagnostics": d.get("diagnostics"),
                  "sp_coverage": d.get("sp_coverage"), "sp_universe": d.get("sp_universe"),
                  "labels": {L: sum(1 for r2 in sp if r2.get("label") == L)
                              for L in ("CHEAP", "FAIR", "RICH")},
                  "sp_cheapest": sp[:5],
                  "sp_sanity_AAPL": next((r2 for r2 in sp if r2.get("t") == "AAPL"), None),
                  "hp_src": d.get("hp_src"), "hp_coverage": d.get("hp_coverage"),
                  "hp_universe": d.get("hp_universe"),
                  "hp_top": (d.get("hp") or [])[:6], "n_serious": d.get("n_serious"),
                  "hp_logged": d.get("hp_logged")}
# board + sentinel redeploys
for fn2, src in (("justhodl-signal-board", "aws/lambdas/justhodl-signal-board/source"),
                  ("justhodl-alert-sentinel", "aws/lambdas/justhodl-alert-sentinel/source")):
    zb2 = zipdir(src)
    for _ in range(6):
        try:
            lam.update_function_code(FunctionName=fn2, ZipFile=zb2); break
        except Exception as e:
            if "ResourceConflict" in str(e): time.sleep(8)
            else: raise
    ready(fn2)
r2 = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse", Payload=b"{}")
out["sb_err"] = r2.get("FunctionError", "NONE")
sb = json.loads(s3.get_object(Bucket=B, Key="data/signal-board.json")["Body"].read())
out["board_n"] = len(sb.get("engines") or [])
out["board_row"] = next(({k: e.get(k) for k in ("signal", "signal_label", "read")}
                          for e in (sb.get("engines") or [])
                          if e.get("engine") == "Stock Valuations"), "MISSING")
r3 = lam.invoke(FunctionName="justhodl-alert-sentinel", InvocationType="RequestResponse", Payload=b"{}")
ds = json.loads(s3.get_object(Bucket=B, Key="data/alert-sentinel.json")["Body"].read())
out["sentinel"] = {"version": ds.get("version"),
                    "hp_serious": (ds.get("snapshot") or {}).get("hp_serious"),
                    "n_changes": ds.get("n_changes")}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1623_stock_valuations.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"fn": out["fn"], "fn_err": out["fn_err"],
                   "sp": out["verify"]["sp_coverage"], "hp": out["verify"]["hp_coverage"],
                   "board": out["board_row"]}, default=str))
