# ops 1654 — create intraday-pulse fn, arm-daily + minute schedules, arm + forced pulse, verify
import json, zipfile, io, os, time
import boto3
from botocore.config import Config
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1654}
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
FN = "justhodl-intraday-pulse"
zb = zipdir("aws/lambdas/justhodl-intraday-pulse/source")
role = lam.get_function_configuration(FunctionName="justhodl-historical-analogs")["Role"]
sent_env = (lam.get_function_configuration(FunctionName="justhodl-alert-sentinel").get("Environment") or {}).get("Variables") or {}
env = {"POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
        "TELEGRAM_BOT_TOKEN": sent_env.get("TELEGRAM_BOT_TOKEN", ""),
        "TELEGRAM_CHAT": sent_env.get("TELEGRAM_CHAT", "")}
out["tg_env"] = bool(env["TELEGRAM_BOT_TOKEN"])
try:
    lam.create_function(FunctionName=FN, Runtime="python3.12", Role=role,
                         Handler="lambda_function.lambda_handler", Code={"ZipFile": zb},
                         Timeout=50, MemorySize=512, Environment={"Variables": env})
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
for rule, sched, inp, sid in (
        ("intraday-arm-daily", "cron(25 13 ? * MON-FRI *)", '{"arm": true}', "evt-arm"),
        ("intraday-pulse-minute", "rate(1 minute)", "{}", "evt-min")):
    ev.put_rule(Name=rule, ScheduleExpression=sched, State="ENABLED")
    try:
        lam.add_permission(FunctionName=FN, StatementId=sid,
                            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                            SourceArn=ev.describe_rule(Name=rule)["Arn"])
    except Exception as e:
        if "ResourceConflict" not in str(e):
            out.setdefault("perm_warn", []).append(str(e)[:60])
    ev.put_targets(Rule=rule, Targets=[{"Id": "1", "Arn": arn, "Input": inp}])
r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                Payload=json.dumps({"arm": True}).encode())
out["arm"] = json.loads(json.loads(r["Payload"].read()).get("body", "{}"))
r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                Payload=json.dumps({"force": True}).encode())
out["pulse"] = json.loads(json.loads(r["Payload"].read()).get("body", "{}"))
d = json.loads(s3.get_object(Bucket=B, Key="data/intraday-pulse.json")["Body"].read())
out["verify"] = {"armed_n": d.get("armed_n"), "n_events": d.get("n_events_today"),
                  "movers_head": (d.get("top_movers") or [])[:8],
                  "events_head": (d.get("events_today") or [])[-5:]}
ar = json.loads(s3.get_object(Bucket=B, Key="data/_intraday/armed.json")["Body"].read())
out["armed_sample"] = {t: ar["levels"][t] for t in list(ar.get("levels") or {})[:5]}
sb_state = json.loads(s3.get_object(Bucket=B, Key="data/_sentinel/state.json")["Body"].read())
out["sentinel_buffer_n"] = len(sb_state.get("buffer") or [])
upd("justhodl-signal-board", zipdir("aws/lambdas/justhodl-signal-board/source"))
lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse", Payload=b"{}")
sb = json.loads(s3.get_object(Bucket=B, Key="data/signal-board.json")["Body"].read())
out["board_n"] = len(sb.get("engines") or [])
out["board_row"] = next(({"read": e.get("read")} for e in (sb.get("engines") or [])
                           if e.get("engine") == "Intraday Pulse"), "MISSING")
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1654_intraday.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"fn": out["fn"], "armed": out["arm"], "pulse": out["pulse"],
                   "board": out["board_n"]}, default=str))
