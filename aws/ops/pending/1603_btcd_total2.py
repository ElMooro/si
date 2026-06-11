# ops 1586 — create+verify regime-engine; board +1; registry; manifest
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=900, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"; ACC = "857687956942"
out = {"ops": 1603, "errors": []}

def rc(fn, tries=10, wait=8):
    for i in range(tries):
        try: return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException","TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries")

def zs(src):
    b=io.BytesIO()
    with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(src):
            for f in fs:
                if "__pycache__" not in r: zf.write(os.path.join(r,f),arcname=os.path.relpath(os.path.join(r,f),src))
    return b.getvalue()

def ready(fn):
    for _ in range(60):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in ("Successful",None) and c.get("State") in ("Active",None): return
        time.sleep(3)

code = zs("aws/lambdas/justhodl-altseason/source")
try:
    lam.get_function(FunctionName="justhodl-altseason")
    rc(lambda: lam.update_function_code(FunctionName="justhodl-altseason", ZipFile=code)); out["fn"]="updated"
except ClientError:
    role = lam.get_function(FunctionName="justhodl-historical-analogs")["Configuration"]["Role"]
    rc(lambda: lam.create_function(FunctionName="justhodl-altseason", Runtime="python3.12", Role=role,
        Handler="lambda_function.lambda_handler", Code={"ZipFile":code}, Timeout=900, MemorySize=1024,
        Environment={"Variables":{"FRED_KEY":"2f057499936072679d8843d7fce99989",
            "POLYGON_KEY":"zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d","ANTHROPIC_API_KEY": lam.get_function_configuration(FunctionName="justhodl-ai-brief")["Environment"]["Variables"].get("ANTHROPIC_KEY","")}},
        Description="Macro regime conductor: GxI quadrants since 1960s, measured playbooks, transition matrix")); out["fn"]="created"
ready("justhodl-altseason")
arn=lam.get_function(FunctionName="justhodl-altseason")["Configuration"]["FunctionArn"]
ev.put_rule(Name="justhodl-altseason-daily", ScheduleExpression="cron(55 13 * * ? *)", State="ENABLED")
ev.put_targets(Rule="justhodl-altseason-daily", Targets=[{"Id":"1","Arn":arn}])
try:
    lam.add_permission(FunctionName="justhodl-altseason", StatementId=f"as{int(time.time())%99999}",
        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
        SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/justhodl-altseason-daily")
except ClientError: pass

cfg_ = lam.get_function_configuration(FunctionName="justhodl-altseason")
env_ = (cfg_.get("Environment") or {}).get("Variables") or {}
if "CMC_KEY" not in env_:
    env_["CMC_KEY"] = "17ba8e87-53f0-46f4-abe5-014d9cd99597"
    rc(lambda: lam.update_function_configuration(FunctionName="justhodl-altseason",
        Environment={"Variables": env_}))
    ready("justhodl-altseason")
r = rc(lambda: lam.invoke(FunctionName="justhodl-altseason", InvocationType="RequestResponse", Payload=b"{}"))
out["fn_err"]=r.get("FunctionError","NONE")
if out["fn_err"]!="NONE": out["payload"]=r["Payload"].read().decode()[:600]
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/altseason.json")["Body"].read())
out["verify"]={"duration_s":d.get("duration_s"),
  "diagnostics":d.get("diagnostics"),
  "composite":d.get("composite"),"votes":d.get("votes"),
  "alt_index_now":((d.get("histories") or {}).get("alt_index") or [[None,None]])[-1][1],
  "alt_index_len":len((d.get("histories") or {}).get("alt_index") or []),
  "study":{k:{"n_events":len(x.get("events") or []),
              "latest":(x.get("events") or [{}])[-1].get("date") if x.get("events") else None,
              "fwd_30":x.get("fwd_30"),"fwd_90":x.get("fwd_90"),"by_regime":x.get("by_regime"),"sample_rows":(x.get("events") or [])[:4]}
           for k,x in (d.get("event_study") or {}).items()},
  "gates":d.get("gates"),
  "global_metrics":d.get("global_metrics"),
  "ai_error":(d.get("ai_brief") or {}).get("error"),
  "ai_verdict":str((d.get("ai_brief") or {}).get("verdict",""))[:220],
  "ai_flips":(d.get("ai_brief") or {}).get("what_flips_confirmed"),
  "signals_logged":d.get("signals_logged")}
rc(lambda: lam.update_function_code(FunctionName="justhodl-signal-board", ZipFile=zs("aws/lambdas/justhodl-signal-board/source")))
ready("justhodl-signal-board")
reg=json.load(open("config/ai-brief-contexts.json"))
s3.put_object(Bucket=B,Key="config/ai-brief-contexts.json",Body=json.dumps(reg,indent=1).encode(),
              ContentType="application/json",CacheControl="no-cache")
r2 = rc(lambda: lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse", Payload=b"{}"))
out["sb_err"]=r2.get("FunctionError","NONE")
sb=json.loads(s3.get_object(Bucket=B,Key="data/signal-board.json")["Body"].read())
out["board_row"]=next(({k:e2.get(k) for k in ("signal","signal_label","read")}
          for e2 in (sb.get("engines") or []) if e2.get("engine")=="Altseason Tribunal"), "MISSING")
out["board_n"]=sb.get("n_engines"); out["registry_n"]=len(reg["contexts"])
m=json.loads(s3.get_object(Bucket=B,Key="data/_freshness-manifest.json")["Body"].read())
m.setdefault("key_overrides",{})["data/altseason.json"]=30; m["updated_by"]="ops-1586"
s3.put_object(Bucket=B,Key="data/_freshness-manifest.json",Body=json.dumps(m,indent=1).encode(),ContentType="application/json")
open("aws/ops/reports/1603_btcd_total2.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps({"fn":out["fn"],"err":out["fn_err"],"current":(out["verify"]["current"] or {}).get("quadrant"),
  "n_months":out["verify"]["n_months"],"board":out["board_n"]},default=str)[:400])
