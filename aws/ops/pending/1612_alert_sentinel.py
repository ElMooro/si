# ops 1612 — create justhodl-alert-sentinel, schedule cron(45 14), invoke (seed), verify
import json, zipfile, io, os, time, base64
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1612}
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
FN = "justhodl-alert-sentinel"
zb = zipdir("aws/lambdas/justhodl-alert-sentinel/source")
role = lam.get_function_configuration(FunctionName="justhodl-historical-analogs")["Role"]
env = {"TELEGRAM_TOKEN": "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs",
        "TELEGRAM_CHAT": "8678089260"}
try:
    lam.create_function(FunctionName=FN, Runtime="python3.12", Role=role,
                         Handler="lambda_function.lambda_handler", Code={"ZipFile": zb},
                         Timeout=180, MemorySize=256, Environment={"Variables": env})
    out["fn"] = "created"
except Exception as e:
    if "exist" in str(e) or "ResourceConflict" in str(e):
        for _ in range(6):
            try:
                lam.update_function_code(FunctionName=FN, ZipFile=zb); break
            except Exception as e2:
                if "ResourceConflict" in str(e2): time.sleep(8)
                else: raise
        lam.update_function_configuration(FunctionName=FN, Environment={"Variables": env})
        out["fn"] = "updated"
    else:
        raise
ready(FN)
rule = "alert-sentinel-daily"
ev.put_rule(Name=rule, ScheduleExpression="cron(45 14 * * ? *)", State="ENABLED")
try:
    lam.add_permission(FunctionName=FN, StatementId="evt-sentinel-daily",
                        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                        SourceArn=ev.describe_rule(Name=rule)["Arn"])
except Exception as e:
    if "ResourceConflict" not in str(e):
        out["perm_warn"] = str(e)[:80]
ev.put_targets(Rule=rule, Targets=[{"Id": "1", "Arn":
    lam.get_function_configuration(FunctionName=FN)["FunctionArn"]}])
# clear any stale snapshot so this run seeds cleanly (idempotent first-run test)
try:
    s3.delete_object(Bucket=B, Key="data/_alerts/last.json")
except Exception:
    pass
r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
out["fn_err"] = r.get("FunctionError", "NONE")
out["log_tail"] = base64.b64decode(r.get("LogResult", "")).decode(errors="replace")[-1000:]
d = json.loads(s3.get_object(Bucket=B, Key="data/alert-sentinel.json")["Body"].read())
snap = d.get("snapshot") or {}
out["verify"] = {"first_run": d.get("first_run"), "message_sent": d.get("message_sent"),
                  "n_changes": d.get("n_changes"), "changes": d.get("changes"),
                  "diagnostics": d.get("diagnostics"),
                  "snap_keys": {"breakouts": len(snap.get("breakouts") or []),
                                 "insider_decline": snap.get("insider_decline"),
                                 "thrusts": snap.get("thrusts"),
                                 "semis_off_low": snap.get("semis_off_low"),
                                 "smallcap_vs_high": snap.get("smallcap_vs_high"),
                                 "breadth_capwtd": snap.get("breadth_capwtd"),
                                 "breadth_regime_day": snap.get("breadth_regime_day"),
                                 "altseason_phase": snap.get("altseason_phase"),
                                 "sizing_top": snap.get("sizing_top"),
                                 "sizing_gross": snap.get("sizing_gross"),
                                 "ma_reclaim_200": snap.get("ma_reclaim_200"),
                                 "ma_break_200": snap.get("ma_break_200")}}
# second invoke immediately → should detect ZERO changes (state seeded), no message
r2 = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
d2 = json.loads(s3.get_object(Bucket=B, Key="data/alert-sentinel.json")["Body"].read())
out["second_run"] = {"first_run": d2.get("first_run"), "message_sent": d2.get("message_sent"),
                      "n_changes": d2.get("n_changes")}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1612_alert_sentinel.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"fn": out["fn"], "fn_err": out["fn_err"], "sent": d.get("message_sent"),
                   "first": d.get("first_run"), "second_changes": out["second_run"]["n_changes"]}, default=str))
