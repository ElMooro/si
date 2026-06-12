# ops 1616 — sentinel v1.1 (canary watch) + board v3 reader: redeploy both, invoke, verify
import json, zipfile, io, os, time, base64
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1616}
def zipdir(srcdir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, fs in os.walk(srcdir):
            for f in fs:
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, srcdir))
    return buf.getvalue()
def upd(fn, zb):
    for _ in range(6):
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=zb); break
        except Exception as e:
            if "ResourceConflict" in str(e): time.sleep(8)
            else: raise
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") != "InProgress" and c.get("State") != "Pending":
            return
        time.sleep(3)
upd("justhodl-alert-sentinel", zipdir("aws/lambdas/justhodl-alert-sentinel/source"))
r = lam.invoke(FunctionName="justhodl-alert-sentinel", InvocationType="RequestResponse",
                LogType="Tail", Payload=b"{}")
out["sent_err"] = r.get("FunctionError", "NONE")
d = json.loads(s3.get_object(Bucket=B, Key="data/alert-sentinel.json")["Body"].read())
snap = d.get("snapshot") or {}
out["sentinel"] = {"version": d.get("version"), "message_sent": d.get("message_sent"),
                    "n_changes": d.get("n_changes"), "changes": d.get("changes"),
                    "canary_reds": snap.get("canary_reds"),
                    "canary_level_v3": snap.get("canary_level_v3"),
                    "canary_v3": snap.get("canary_v3")}
lam.invoke(FunctionName="justhodl-alert-sentinel", InvocationType="RequestResponse", Payload=b"{}")
d2 = json.loads(s3.get_object(Bucket=B, Key="data/alert-sentinel.json")["Body"].read())
out["sentinel_second"] = {"n_changes": d2.get("n_changes"), "message_sent": d2.get("message_sent")}
upd("justhodl-signal-board", zipdir("aws/lambdas/justhodl-signal-board/source"))
r2 = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse", Payload=b"{}")
out["sb_err"] = r2.get("FunctionError", "NONE")
sb = json.loads(s3.get_object(Bucket=B, Key="data/signal-board.json")["Body"].read())
out["board_n"] = len(sb.get("engines") or [])
out["board_row"] = next(({k: e.get(k) for k in ("signal", "signal_label", "read")}
                          for e in (sb.get("engines") or [])
                          if e.get("engine") == "Crisis Canaries"), "MISSING")
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1616_sentinel_canaries.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"sent_err": out["sent_err"], "sb_err": out["sb_err"],
                   "changes": out["sentinel"]["n_changes"],
                   "second": out["sentinel_second"]["n_changes"],
                   "board": out["board_row"]}, default=str))
