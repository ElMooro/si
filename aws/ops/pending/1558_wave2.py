# ops 1558 — create+run+verify liquidity-inflection; deploy ignition v1.0.1 + re-verify insider/13F pillars
import json, os, time, zipfile, io, boto3
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=600, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"; ACC = "857687956942"
out = {"ops": 1558, "errors": []}

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
    for _ in range(40):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in ("Successful",None) and c.get("State") in ("Active",None): return
        time.sleep(3)

role = lam.get_function(FunctionName="justhodl-historical-analogs")["Configuration"]["Role"]

# A) liquidity-inflection create
code = zs("aws/lambdas/justhodl-liquidity-inflection/source")
try:
    lam.get_function(FunctionName="justhodl-liquidity-inflection")
    rc(lambda: lam.update_function_code(FunctionName="justhodl-liquidity-inflection", ZipFile=code)); out["li_fn"]="updated"
except ClientError:
    rc(lambda: lam.create_function(FunctionName="justhodl-liquidity-inflection", Runtime="python3.12", Role=role,
        Handler="lambda_function.lambda_handler", Code={"ZipFile":code}, Timeout=300, MemorySize=768,
        Environment={"Variables":{"FRED_KEY":"2f057499936072679d8843d7fce99989","POLYGON_KEY":"zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"}},
        Description="Liquidity inflection: 13w net-liq impulse, flip detector, event-study lead/lag table; closed-loop")); out["li_fn"]="created"
ready("justhodl-liquidity-inflection")
arn=lam.get_function(FunctionName="justhodl-liquidity-inflection")["Configuration"]["FunctionArn"]
ev.put_rule(Name="justhodl-liquidity-inflection-daily", ScheduleExpression="cron(15 11 * * ? *)", State="ENABLED")
ev.put_targets(Rule="justhodl-liquidity-inflection-daily", Targets=[{"Id":"1","Arn":arn}])
try:
    lam.add_permission(FunctionName="justhodl-liquidity-inflection", StatementId=f"li-{int(time.time())}",
        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
        SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/justhodl-liquidity-inflection-daily")
except ClientError: pass
r = rc(lambda: lam.invoke(FunctionName="justhodl-liquidity-inflection", InvocationType="RequestResponse", Payload=b"{}"))
out["li_err"]=r.get("FunctionError","NONE")
if out["li_err"]!="NONE": out["li_payload"]=r["Payload"].read().decode()[:600]
time.sleep(2)
li=json.loads(s3.get_object(Bucket=B,Key="data/liquidity-inflection.json")["Body"].read())
out["li_verify"]={"availability":li.get("availability"),"usd":{k:v for k,v in (li.get("usd") or {}).items() if k!="impulse_tail_180d"},
  "eur":li.get("eur"),"china":li.get("china"),"stablecoin":li.get("stablecoin"),
  "event_study":li.get("event_study_after_flips"),"leads":li.get("lead_estimates"),
  "duration_s":li.get("duration_s"),"signals_logged":li.get("signals_logged")}

# B) ignition v1.0.1 redeploy + verify pillars
rc(lambda: lam.update_function_code(FunctionName="justhodl-ignition", ZipFile=zs("aws/lambdas/justhodl-ignition/source")))
ready("justhodl-ignition")
r2 = rc(lambda: lam.invoke(FunctionName="justhodl-ignition", InvocationType="RequestResponse", Payload=b"{}"))
out["ig_err"]=r2.get("FunctionError","NONE")
time.sleep(2)
ig=json.loads(s3.get_object(Bucket=B,Key="data/ignition.json")["Body"].read())
out["ig_verify"]={"pillar_availability":ig.get("pillar_availability"),"probes":ig.get("probes"),
  "top_calls":ig.get("top_calls"),"scored_n":ig.get("scored_n"),"signals_logged":ig.get("signals_logged")}

# manifest override
m=json.loads(s3.get_object(Bucket=B,Key="data/_freshness-manifest.json")["Body"].read())
m.setdefault("key_overrides",{})["data/liquidity-inflection.json"]=30
m["updated_by"]="ops-1558"
s3.put_object(Bucket=B,Key="data/_freshness-manifest.json",Body=json.dumps(m,indent=1).encode(),ContentType="application/json")
open("aws/ops/reports/1558_wave2.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps({"li":out["li_fn"],"li_err":out["li_err"],"usd":out["li_verify"]["usd"],
  "leads":out["li_verify"]["leads"],"ig_avail":out["ig_verify"]["pillar_availability"]},default=str)[:800])
