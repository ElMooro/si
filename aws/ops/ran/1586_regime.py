# ops 1586 — create+verify regime-engine; board +1; registry; manifest
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=900, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"; ACC = "857687956942"
out = {"ops": 1586, "errors": []}

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

code = zs("aws/lambdas/justhodl-regime-engine/source")
try:
    lam.get_function(FunctionName="justhodl-regime-engine")
    rc(lambda: lam.update_function_code(FunctionName="justhodl-regime-engine", ZipFile=code)); out["fn"]="updated"
except ClientError:
    role = lam.get_function(FunctionName="justhodl-historical-analogs")["Configuration"]["Role"]
    rc(lambda: lam.create_function(FunctionName="justhodl-regime-engine", Runtime="python3.12", Role=role,
        Handler="lambda_function.lambda_handler", Code={"ZipFile":code}, Timeout=300, MemorySize=512,
        Environment={"Variables":{"FRED_KEY":"2f057499936072679d8843d7fce99989"}},
        Description="Macro regime conductor: GxI quadrants since 1960s, measured playbooks, transition matrix")); out["fn"]="created"
ready("justhodl-regime-engine")
arn=lam.get_function(FunctionName="justhodl-regime-engine")["Configuration"]["FunctionArn"]
ev.put_rule(Name="justhodl-regime-engine-daily", ScheduleExpression="cron(5 13 * * ? *)", State="ENABLED")
ev.put_targets(Rule="justhodl-regime-engine-daily", Targets=[{"Id":"1","Arn":arn}])
try:
    lam.add_permission(FunctionName="justhodl-regime-engine", StatementId=f"rg{int(time.time())%99999}",
        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
        SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/justhodl-regime-engine-daily")
except ClientError: pass

r = rc(lambda: lam.invoke(FunctionName="justhodl-regime-engine", InvocationType="RequestResponse", Payload=b"{}"))
out["fn_err"]=r.get("FunctionError","NONE")
if out["fn_err"]!="NONE": out["payload"]=r["Payload"].read().decode()[:600]
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/regime.json")["Body"].read())
out["verify"]={"duration_s":d.get("duration_s"),"n_months":d.get("n_months_classified"),
  "first_month":d.get("first_month"),"current":d.get("current"),
  "spx_3m_playbook":(d.get("playbook") or {}).get("spx",{}).get("3m"),
  "y10_3m":(d.get("playbook") or {}).get("ust10y_yield",{}).get("3m"),
  "credit_3m":(d.get("playbook") or {}).get("credit_spread",{}).get("3m"),
  "tmat":d.get("transition_matrix_3m"),"strip_n":len(d.get("regime_strip") or []),
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
          for e2 in (sb.get("engines") or []) if e2.get("engine")=="Macro Regime (Conductor)"), "MISSING")
out["board_n"]=sb.get("n_engines"); out["registry_n"]=len(reg["contexts"])
m=json.loads(s3.get_object(Bucket=B,Key="data/_freshness-manifest.json")["Body"].read())
m.setdefault("key_overrides",{})["data/regime.json"]=30; m["updated_by"]="ops-1586"
s3.put_object(Bucket=B,Key="data/_freshness-manifest.json",Body=json.dumps(m,indent=1).encode(),ContentType="application/json")
open("aws/ops/reports/1586_regime.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps({"fn":out["fn"],"err":out["fn_err"],"current":(out["verify"]["current"] or {}).get("quadrant"),
  "n_months":out["verify"]["n_months"],"board":out["board_n"]},default=str)[:400])
