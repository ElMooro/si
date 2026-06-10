# ops 1583 — create+verify ma-reversion; deploy board (+1 feed); upload registry; manifest
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=900, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ddb = boto3.client("dynamodb", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"; ACC = "857687956942"
out = {"ops": 1584, "errors": []}

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

code = zs("aws/lambdas/justhodl-ma-reversion/source")
try:
    lam.get_function(FunctionName="justhodl-ma-reversion")
    rc(lambda: lam.update_function_code(FunctionName="justhodl-ma-reversion", ZipFile=code)); out["fn"]="updated"
except ClientError:
    role = lam.get_function(FunctionName="justhodl-historical-analogs")["Configuration"]["Role"]
    rc(lambda: lam.create_function(FunctionName="justhodl-ma-reversion", Runtime="python3.12", Role=role,
        Handler="lambda_function.lambda_handler", Code={"ZipFile":code}, Timeout=600, MemorySize=1024,
        Environment={"Variables":{"POLYGON_KEY":"zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"}},
        Description="MA mean-reversion measured: 50y touch/breakdown/reclaim tables, shelves, own-record setups")); out["fn"]="created"
ready("justhodl-ma-reversion")
arn=lam.get_function(FunctionName="justhodl-ma-reversion")["Configuration"]["FunctionArn"]
ev.put_rule(Name="justhodl-ma-reversion-daily", ScheduleExpression="cron(55 12 * * ? *)", State="ENABLED")
ev.put_targets(Rule="justhodl-ma-reversion-daily", Targets=[{"Id":"1","Arn":arn}])
try:
    lam.add_permission(FunctionName="justhodl-ma-reversion", StatementId=f"mar{int(time.time())%99999}",
        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
        SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/justhodl-ma-reversion-daily")
except ClientError: pass

r = rc(lambda: lam.invoke(FunctionName="justhodl-ma-reversion", InvocationType="RequestResponse", Payload=b"{}"))
out["fn_err"]=r.get("FunctionError","NONE")
if out["fn_err"]!="NONE": out["payload"]=r["Payload"].read().decode()[:600]
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/ma-reversion.json")["Body"].read())
sp=d.get("spx") or {}; ev_=sp.get("events") or {}
def pick(k):
    e=ev_.get(k) or {}
    return {"touches":e.get("touch_n"),"t5":(e.get("touch") or {}).get("5"),"t21":(e.get("touch") or {}).get("21"),"t63":(e.get("touch") or {}).get("63"),
            "shelf":e.get("shelf_depth_med_pct"),
            "bd_n":e.get("breakdown_n"),"bd21":(e.get("breakdown") or {}).get("21"),"rc_n":e.get("reclaim_n"),"rc21":(e.get("reclaim") or {}).get("21")}
out["verify"]={"duration_s":d.get("duration_s"),
  "spx_n_days":sp.get("n_days"),"current":sp.get("current"),
  "ma200_bull":pick("ma200_bull"),"ma50_bull":pick("ma50_bull"),
  "ma200_bear":pick("ma200_bear"),"ma20_bull":pick("ma20_bull"),
  "stretch":sp.get("stretch_vs_200"),"crosses":sp.get("crosses"),
  "qqq_ok":bool(d.get("qqq")),"qqq_ma200_bull":(pick if False else None),
  "setups":d.get("stock_setups",[])[:8],"n_setups":d.get("n_setups"),
  "signals_logged":d.get("signals_logged")}
if d.get("qqq"):
    qe=(d["qqq"].get("events") or {}).get("ma200_bull") or {}
    out["verify"]["qqq_ma200_bull"]={"touches":qe.get("touch_n"),"t21":(qe.get("touch") or {}).get("21")}

# board + registry
rc(lambda: lam.update_function_code(FunctionName="justhodl-signal-board", ZipFile=zs("aws/lambdas/justhodl-signal-board/source")))
ready("justhodl-signal-board")
reg=json.load(open("config/ai-brief-contexts.json"))
s3.put_object(Bucket=B,Key="config/ai-brief-contexts.json",Body=json.dumps(reg,indent=1).encode(),
              ContentType="application/json",CacheControl="no-cache")
r2 = rc(lambda: lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse", Payload=b"{}"))
out["sb_err"]=r2.get("FunctionError","NONE")
sb=json.loads(s3.get_object(Bucket=B,Key="data/signal-board.json")["Body"].read())
row=next(({k:e2.get(k) for k in ("signal","signal_label","read")}
          for e2 in (sb.get("engines") or []) if e2.get("engine")=="MA Reversion Shelves"), "MISSING")
out["board_row"]=row; out["board_n"]=sb.get("n_engines"); out["registry_n"]=len(reg["contexts"])
m=json.loads(s3.get_object(Bucket=B,Key="data/_freshness-manifest.json")["Body"].read())
m.setdefault("key_overrides",{})["data/ma-reversion.json"]=30; m["updated_by"]="ops-1583"
s3.put_object(Bucket=B,Key="data/_freshness-manifest.json",Body=json.dumps(m,indent=1).encode(),ContentType="application/json")
open("aws/ops/reports/1584_ma_fix.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps({"fn":out["fn"],"err":out["fn_err"],"ma200_bull":out["verify"]["ma200_bull"],
  "setups":out["verify"]["n_setups"],"logged":out["verify"]["signals_logged"],
  "board":out["board_n"]},default=str)[:600])
