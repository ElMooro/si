# ops 1559 — create+verify crisis-canaries; deploy liq v1.0.1 Schmitt + re-verify; diagnostics dumps for ignition v1.0.2
import json, os, time, zipfile, io, urllib.request, boto3
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=600, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"; ACC = "857687956942"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
out = {"ops": 1559, "errors": []}

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

# A) canaries create+run
code = zs("aws/lambdas/justhodl-crisis-canaries/source")
try:
    lam.get_function(FunctionName="justhodl-crisis-canaries")
    rc(lambda: lam.update_function_code(FunctionName="justhodl-crisis-canaries", ZipFile=code)); out["cn_fn"]="updated"
except ClientError:
    rc(lambda: lam.create_function(FunctionName="justhodl-crisis-canaries", Runtime="python3.12", Role=role,
        Handler="lambda_function.lambda_handler", Code={"ZipFile":code}, Timeout=300, MemorySize=512,
        Environment={"Variables":{"FRED_KEY":"2f057499936072679d8843d7fce99989","POLYGON_KEY":"zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"}},
        Description="Funding-plumbing crisis canaries: SOFR tail, H.4.1/H.8, auction slope, ALFRED revisions; closed-loop")); out["cn_fn"]="created"
ready("justhodl-crisis-canaries")
arn=lam.get_function(FunctionName="justhodl-crisis-canaries")["Configuration"]["FunctionArn"]
ev.put_rule(Name="justhodl-crisis-canaries-daily", ScheduleExpression="cron(40 11 * * ? *)", State="ENABLED")
ev.put_targets(Rule="justhodl-crisis-canaries-daily", Targets=[{"Id":"1","Arn":arn}])
try:
    lam.add_permission(FunctionName="justhodl-crisis-canaries", StatementId=f"cn-{int(time.time())}",
        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
        SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/justhodl-crisis-canaries-daily")
except ClientError: pass
r = rc(lambda: lam.invoke(FunctionName="justhodl-crisis-canaries", InvocationType="RequestResponse", Payload=b"{}"))
out["cn_err"]=r.get("FunctionError","NONE")
if out["cn_err"]!="NONE": out["cn_payload"]=r["Payload"].read().decode()[:600]
time.sleep(2)
cn=json.loads(s3.get_object(Bucket=B,Key="data/crisis-canaries.json")["Body"].read())
out["cn_verify"]={"availability":cn.get("availability"),"score":cn.get("composite_score"),"level":cn.get("level"),
  "alerts":cn.get("alerts"),"canaries":cn.get("canaries"),"duration_s":cn.get("duration_s")}

# B) liquidity v1.0.1 deploy + re-run
rc(lambda: lam.update_function_code(FunctionName="justhodl-liquidity-inflection", ZipFile=zs("aws/lambdas/justhodl-liquidity-inflection/source")))
ready("justhodl-liquidity-inflection")
r2 = rc(lambda: lam.invoke(FunctionName="justhodl-liquidity-inflection", InvocationType="RequestResponse", Payload=b"{}"))
out["li_err"]=r2.get("FunctionError","NONE")
time.sleep(2)
li=json.loads(s3.get_object(Bucket=B,Key="data/liquidity-inflection.json")["Body"].read())
es=li.get("event_study_after_flips") or {}
out["li_verify"]={"n_flips_10y":(li.get("usd") or {}).get("n_flips_10y"),
  "last_flip":(li.get("usd") or {}).get("last_flip"),
  "spx_d21":((es.get("SPX_proxy") or {}).get("d21")),
  "hyg_d21":((es.get("HYG") or {}).get("d21")),
  "btc_d63":((es.get("BTC") or {}).get("d63")),
  "leads":li.get("lead_estimates"),"sc_hint":li.get("stablecoin_schema_hint"),
  "signals_logged":li.get("signals_logged")}

# C) diagnostics for ignition v1.0.2
def hj(u, hdr=None):
    try: return json.loads(urllib.request.urlopen(urllib.request.Request(u, headers=hdr or {"User-Agent":"JustHodl"}), timeout=25).read())
    except Exception as e: return f"ERR {str(e)[:60]}"
ib=None
try: ib=json.loads(s3.get_object(Bucket=B,Key="data/insider-buys-enriched.json")["Body"].read())
except Exception as e: ib=f"ERR {str(e)[:50]}"
out["diag_insider"]={"top_keys":sorted(ib.keys())[:10] if isinstance(ib,dict) else ib,
  "first_list_item": next((v[0] for v in ib.values() if isinstance(v,list) and v and isinstance(v[0],dict)), None) if isinstance(ib,dict) else None}
fl=hj(f"https://financialmodelingprep.com/stable/institutional-ownership/latest?symbol=NVDA&apikey={FMP}")
out["diag_f13_latest"]={"type":type(fl).__name__,"first":(fl[0] if isinstance(fl,list) and fl else fl) if not isinstance(fl,str) else fl}
fr=hj("https://api.finra.org/data/group/otcMarket/name/weeklySummary?limit=3",{"User-Agent":"JustHodl","Accept":"application/json"})
out["diag_finra"]= (fr[:2] if isinstance(fr,list) else fr)

m=json.loads(s3.get_object(Bucket=B,Key="data/_freshness-manifest.json")["Body"].read())
m.setdefault("key_overrides",{})["data/crisis-canaries.json"]=30
m["updated_by"]="ops-1559"
s3.put_object(Bucket=B,Key="data/_freshness-manifest.json",Body=json.dumps(m,indent=1).encode(),ContentType="application/json")
open("aws/ops/reports/1559_wave3.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps({"cn":out["cn_fn"],"cn_err":out["cn_err"],"score":out["cn_verify"]["score"],
  "level":out["cn_verify"]["level"],"avail":out["cn_verify"]["availability"],
  "li_flips":out["li_verify"]["n_flips_10y"],"li_last":out["li_verify"]["last_flip"]},default=str)[:700])
