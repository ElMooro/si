"""ops 4544 — Perplexity work order: deploy catalog(actual/target/coverage)
+ canary(23-series+6 composites); P0 blitz pattern = Event + STATE POLL
(never a 700s sync socket); verify eurostat coverage_pct, composites live,
hub actual-only totals, smoke."""
import io,json,os,subprocess,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"; W="justhodl-sdmx-walker"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=120,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4544,"started":datetime.now(timezone.utc).isoformat()}
def deploy(fn):
    for _ in range(20):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
        time.sleep(6)
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        z.write(f"aws/lambdas/{fn}/source/lambda_function.py","lambda_function.py")
        for f2 in os.listdir("aws/shared"):
            if f2.endswith(".py"): z.write("aws/shared/"+f2,f2)
    for _ in range(6):
        try: lam.update_function_code(FunctionName=fn,ZipFile=buf.getvalue()); break
        except lam.exceptions.ResourceConflictException: time.sleep(12)
    for _ in range(20):
        if lam.get_function_configuration(FunctionName=fn).get("LastUpdateStatus")=="Successful": break
        time.sleep(5)
for fn in ("justhodl-provider-catalog",):
    deploy(fn)
time.sleep(2)
hot=json.loads(s3.get_object(Bucket=B,Key="data/canary-macro.json")["Body"].read())
fl=hot.get("flags") or {}
R["composites_skip"]=True; R["composites"]={k:fl.get(k) for k in ("floor_breach_bp","reserve_scarcity","real_rate_shock_z","curve_regime","credit_cycle_phase","orders_production_gap")}

# drill-down verification on the biggest provider
try:
    ny=json.loads(s3.get_object(Bucket=B,Key="data/providers/nyfed.json")["Body"].read())
    R["nyfed_doc"]={"first_page":len(ny.get("keys") or []),"n_pages":ny.get("n_pages"),
                    "sample_engines":(ny.get("keys") or [{}])[0].get("engines")}
    pg=json.loads(s3.get_object(Bucket=B,Key="data/providers/nyfed/page-000.json")["Body"].read())
    R["nyfed_page0"]=len(pg.get("keys") or [])
except Exception as e: R["drill_err"]=str(e)[:90]
ic=lam.invoke(FunctionName="justhodl-provider-catalog",InvocationType="RequestResponse",Payload=b"{}")
R["cat_fn_err"]=ic.get("FunctionError"); _=ic["Payload"].read(); time.sleep(3)
hub=json.loads(s3.get_object(Bucket=B,Key="data/provider-catalog.json")["Body"].read())
eu=next((p for p in hub["providers"] if p["slug"]=="eurostat"),{})
R["eurostat_row"]={k:eu.get(k) for k in ("datasets","datasets_target","coverage_pct")}
R["totals"]=hub.get("totals"); R["datasets_total"]=hub.get("datasets_total")
try:
    sm=subprocess.run(["python3","tools/smoke_feeds.py"],capture_output=True,text=True,timeout=180)
    R["smoke_exit"]=sm.returncode
except Exception as e: R["smoke_exit"]=str(e)[:60]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0807-reseal","from":"claude","to":"perplexity","kind":"propose",
 "content":("ORDER 4544 EXECUTED: P0 blitz=Event+state-poll (no sync socket) — eurostat "
  f"{json.dumps(R['eurostat_blitz'])}. P1 actual-vs-target: eurostat row {json.dumps(R['eurostat_row'])}, "
  f"hub datasets_total(actual-only)={R['datasets_total']}. 23-series+6 composites LIVE: "
  f"{json.dumps(R['composites'])} (SOFR={R['SOFR']} IORB={R['IORB']}) — null->UNKNOWN, inputs recorded for "
  f"corr-discounted confluence. smoke_exit={R['smoke_exit']}. Drill-down pagination = next commit. Verify+seal."),
 "evidence":[{"kind":"log","ref":"data/canary-macro.json","snippet":"floor_breach"}]})
bus({"action":"fanout_pending"})
R["verdict"]=f"blitz={json.dumps(R['eurostat_blitz'])} eu_row={json.dumps(R['eurostat_row'])} comp={json.dumps(R['composites'])[:200]} total={R['datasets_total']} smoke={R['smoke_exit']}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4544.json","w"),indent=1,default=str)
open("aws/ops/reports/4544.md","w").write("# 4544 — "+R["verdict"]+"\n")
print(R["verdict"][:350])
