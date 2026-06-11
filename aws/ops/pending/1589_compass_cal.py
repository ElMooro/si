# ops 1586 — create+verify regime-engine; board +1; registry; manifest
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=900, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"; ACC = "857687956942"
out = {"ops": 1589, "errors": []}

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

code = zs("aws/lambdas/justhodl-episode-compass/source")
try:
    lam.get_function(FunctionName="justhodl-episode-compass")
    rc(lambda: lam.update_function_code(FunctionName="justhodl-episode-compass", ZipFile=code)); out["fn"]="updated"
except ClientError:
    role = lam.get_function(FunctionName="justhodl-historical-analogs")["Configuration"]["Role"]
    rc(lambda: lam.create_function(FunctionName="justhodl-episode-compass", Runtime="python3.12", Role=role,
        Handler="lambda_function.lambda_handler", Code={"ZipFile":code}, Timeout=300, MemorySize=512,
        Environment={"Variables":{"FRED_KEY":"2f057499936072679d8843d7fce99989",
            "ANTHROPIC_API_KEY": lam.get_function_configuration(FunctionName="justhodl-ai-brief")["Environment"]["Variables"].get("ANTHROPIC_KEY","")}},
        Description="Macro regime conductor: GxI quadrants since 1960s, measured playbooks, transition matrix")); out["fn"]="created"
ready("justhodl-episode-compass")
arn=lam.get_function(FunctionName="justhodl-episode-compass")["Configuration"]["FunctionArn"]
ev.put_rule(Name="justhodl-episode-compass-daily", ScheduleExpression="cron(10 13 * * ? *)", State="ENABLED")
ev.put_targets(Rule="justhodl-episode-compass-daily", Targets=[{"Id":"1","Arn":arn}])
try:
    lam.add_permission(FunctionName="justhodl-episode-compass", StatementId=f"ec{int(time.time())%99999}",
        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
        SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/justhodl-episode-compass-daily")
except ClientError: pass

r = rc(lambda: lam.invoke(FunctionName="justhodl-episode-compass", InvocationType="RequestResponse", Payload=b"{}"))
out["fn_err"]=r.get("FunctionError","NONE")
if out["fn_err"]!="NONE": out["payload"]=r["Payload"].read().decode()[:600]
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/episode-compass.json")["Body"].read())
out["verify"]={"duration_s":d.get("duration_s"),
  "class_scores":d.get("class_scores"),
  "tops_head":(d.get("classes",{}).get("TOP",{}).get("episodes") or [])[:3],
  "bottoms_head":(d.get("classes",{}).get("BOTTOM",{}).get("episodes") or [])[:3],
  "swans_head":(d.get("classes",{}).get("BLACK_SWAN",{}).get("episodes") or [])[:3],
  "today_tails":d.get("swan_checklist"),
  "ai_keys":sorted((d.get("ai_brief") or {}).keys()),
  "ai_error":(d.get("ai_brief") or {}).get("error"),
  "ai_verdict":str((d.get("ai_brief") or {}).get("verdict",""))[:200],
  "reading":__import__("json").loads("{}") or d.get("reading"),"ai_rhyme":str((d.get("ai_brief") or {}).get("closest_rhyme",""))[:260]}
rc(lambda: lam.update_function_code(FunctionName="justhodl-signal-board", ZipFile=zs("aws/lambdas/justhodl-signal-board/source")))
ready("justhodl-signal-board")
reg=json.load(open("config/ai-brief-contexts.json"))
s3.put_object(Bucket=B,Key="config/ai-brief-contexts.json",Body=json.dumps(reg,indent=1).encode(),
              ContentType="application/json",CacheControl="no-cache")
r2 = rc(lambda: lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse", Payload=b"{}"))
out["sb_err"]=r2.get("FunctionError","NONE")
sb=json.loads(s3.get_object(Bucket=B,Key="data/signal-board.json")["Body"].read())
out["board_row"]=next(({k:e2.get(k) for k in ("signal","signal_label","read")}
          for e2 in (sb.get("engines") or []) if e2.get("engine")=="Episode Compass"), "MISSING")
out["board_n"]=sb.get("n_engines"); out["registry_n"]=len(reg["contexts"])
m=json.loads(s3.get_object(Bucket=B,Key="data/_freshness-manifest.json")["Body"].read())
m.setdefault("key_overrides",{})["data/episode-compass.json"]=30; m["updated_by"]="ops-1586"
s3.put_object(Bucket=B,Key="data/_freshness-manifest.json",Body=json.dumps(m,indent=1).encode(),ContentType="application/json")
open("aws/ops/reports/1589_compass_cal.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps({"fn":out["fn"],"err":out["fn_err"],"current":(out["verify"]["current"] or {}).get("quadrant"),
  "n_months":out["verify"]["n_months"],"board":out["board_n"]},default=str)[:400])
