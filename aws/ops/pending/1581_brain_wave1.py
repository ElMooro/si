# ops 1581 — brain-gap wave 1: deploy canaries v2 + liq v1.1; create us-cycle + market-internals;
# invoke all; verify pillar-by-pillar; manifest overrides
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=900, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"; ACC = "857687956942"
ev = boto3.client("events", region_name="us-east-1", config=cfg)
out = {"ops": 1581, "errors": []}

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

def ensure(fn_name, src, sched, rule, desc, timeout=300, mem=512):
    code = zs(src)
    try:
        lam.get_function(FunctionName=fn_name)
        rc(lambda: lam.update_function_code(FunctionName=fn_name, ZipFile=code)); st="updated"
    except ClientError:
        role = lam.get_function(FunctionName="justhodl-historical-analogs")["Configuration"]["Role"]
        rc(lambda: lam.create_function(FunctionName=fn_name, Runtime="python3.12", Role=role,
            Handler="lambda_function.lambda_handler", Code={"ZipFile":code}, Timeout=timeout, MemorySize=mem,
            Environment={"Variables":{"FRED_KEY":"2f057499936072679d8843d7fce99989",
                "POLYGON_KEY":"zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"}}, Description=desc)); st="created"
    ready(fn_name)
    arn=lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]
    ev.put_rule(Name=rule, ScheduleExpression=sched, State="ENABLED")
    ev.put_targets(Rule=rule, Targets=[{"Id":"1","Arn":arn}])
    try:
        lam.add_permission(FunctionName=fn_name, StatementId=f"x{int(time.time())%99999}",
            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/{rule}")
    except ClientError: pass
    return st

def inv(fn):
    r = rc(lambda: lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}"))
    return r.get("FunctionError","NONE"), (r["Payload"].read().decode()[:400] if r.get("FunctionError") else None)

for fn,src in (("justhodl-crisis-canaries","aws/lambdas/justhodl-crisis-canaries/source"),
               ("justhodl-liquidity-inflection","aws/lambdas/justhodl-liquidity-inflection/source")):
    rc(lambda: lam.update_function_code(FunctionName=fn, ZipFile=zs(src))); ready(fn)
out["uc"]=ensure("justhodl-us-cycle","aws/lambdas/justhodl-us-cycle/source",
  "cron(20 12 * * ? *)","justhodl-us-cycle-daily","US macro-cycle canaries (brain-gap ops-1580)",600,768)
out["mi"]=ensure("justhodl-market-internals","aws/lambdas/justhodl-market-internals/source",
  "cron(40 12 * * ? *)","justhodl-market-internals-daily","Market breadth/McClellan internals (brain-gap)",600,1024)

v={}
e,p=inv("justhodl-crisis-canaries"); v["cn_err"]=e
if p: v["cn_payload"]=p
cn=json.loads(s3.get_object(Bucket=B,Key="data/crisis-canaries.json")["Body"].read())
v["cn"]={"version":cn.get("version"),"avail":cn.get("availability"),
  "new":{k:cn.get("canaries",{}).get(k) for k in ("treasury_vol_proxy","cp_bill_spread","mmf_assets","floor_spreads","bank_reserves","pd_fails")},
  "score":cn.get("composite_score"),"level":cn.get("level"),"alerts":cn.get("alerts")}

e,p=inv("justhodl-liquidity-inflection"); v["li_err"]=e
li=json.loads(s3.get_object(Bucket=B,Key="data/liquidity-inflection.json")["Body"].read())
v["li"]={"version":li.get("version"),"us_money":li.get("us_money")}

e,p=inv("justhodl-us-cycle"); v["uc_err"]=e
if p: v["uc_payload"]=p
uc=json.loads(s3.get_object(Bucket=B,Key="data/us-cycle.json")["Body"].read())
v["uc"]={"score":uc.get("cycle_score"),"alerts":uc.get("alerts"),"duration_s":uc.get("duration_s"),
  "pillars":{k:{kk:vv for kk,vv in (x or {}).items() if kk in ("value","signal","ci_large_net_pct","four_wk_avg_k","yoy_pct","quits_rate_pct","chg_6m_pp","ratio","z_10y","tp10_pct","pct","mcap_gdp_pct","pctile","level_bn","smh_spy_60d_pct","as_of","series")}
            for k,x in (uc.get("pillars") or {}).items()}}

e,p=inv("justhodl-market-internals"); v["mi_err"]=e
if p: v["mi_payload"]=p
mi=json.loads(s3.get_object(Bucket=B,Key="data/market-internals.json")["Body"].read())
v["mi"]={"sessions":mi.get("sessions"),"fetched":mi.get("fetched_this_run"),
  "latest":mi.get("latest"),"mcclellan":mi.get("mcclellan"),"thrust":mi.get("zweig_thrust"),
  "duration_s":mi.get("duration_s"),"signal_logged":mi.get("signal_logged")}

m=json.loads(s3.get_object(Bucket=B,Key="data/_freshness-manifest.json")["Body"].read())
ko=m.setdefault("key_overrides",{}); ko["data/us-cycle.json"]=30; ko["data/market-internals.json"]=30
m["updated_by"]="ops-1581"
s3.put_object(Bucket=B,Key="data/_freshness-manifest.json",Body=json.dumps(m,indent=1).encode(),ContentType="application/json")
out["verify"]=v
open("aws/ops/reports/1581_brain_wave1.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps({"errs":{k:v[k] for k in ("cn_err","li_err","uc_err","mi_err")},
  "cn_new":list((v["cn"]["new"] or {}).keys()),"uc_score":v["uc"]["score"],
  "mi_sessions":v["mi"]["sessions"]},default=str)[:500])
