"""ops 4546 — deploy Wave A (canary corrections + catalog fixes) and
verify every specific number Perplexity will re-check: rsi%/regime,
Δ-shock vs level-lamp, per-flag status, BIS coverage<=100, finviz gone
from fred, statcan target wired, smoke assertion firing correctly."""
import io,json,os,subprocess,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=560,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4546,"started":datetime.now(timezone.utc).isoformat()}
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
for fn in ("justhodl-canary-macro","justhodl-provider-catalog"):
    deploy(fn)
lam.invoke(FunctionName="justhodl-canary-macro",InvocationType="Event",Payload=b"{}")
time.sleep(210)
hot=json.loads(s3.get_object(Bucket=B,Key="data/canary-macro.json")["Body"].read())
fl=hot.get("flags") or {}
R["canary"]={
 "reserve_scarcity_pct":fl.get("reserve_scarcity_pct"),
 "reserve_regime":fl.get("reserve_regime"),
 "reserve_share_of_fed_liabilities":fl.get("reserve_share_of_fed_liabilities"),
 "real_rate_shock_z":fl.get("real_rate_shock_z"),
 "real_rate_level_z":fl.get("real_rate_level_z"),
 "credit_cycle_phase":fl.get("credit_cycle_phase"),
 "per_flag_status_sample":{k:fl.get("per_flag_status",{}).get(k) for k in ("sahm_triggered","reserve_scarcity_pct")},
 "GDP":(hot.get("GDP") or {}).get("value"),
 "EFFR":(hot.get("EFFR") or {}).get("value"),
 "BAMLC0A0CM":(hot.get("BAMLC0A0CM") or {}).get("value"),
 "STLFSI4":(hot.get("STLFSI4") or {}).get("value"),
 "KCFSI":(hot.get("KCFSI") or {}).get("value")}
ic=lam.invoke(FunctionName="justhodl-provider-catalog",InvocationType="RequestResponse",Payload=b"{}")
R["cat_fn_err"]=ic.get("FunctionError"); _=ic["Payload"].read(); time.sleep(3)
hub=json.loads(s3.get_object(Bucket=B,Key="data/provider-catalog.json")["Body"].read())
bis=next((p for p in hub["providers"] if p["slug"]=="bis"),{})
stc=next((p for p in hub["providers"] if p["slug"]=="statcan"),{})
R["bis_row"]={"datasets":bis.get("datasets"),"target":bis.get("datasets_target"),"coverage_pct":bis.get("coverage_pct")}
R["statcan_row"]={"datasets":stc.get("datasets"),"target":stc.get("datasets_target"),"coverage_pct":stc.get("coverage_pct")}
fred=json.loads(s3.get_object(Bucket=B,Key="data/providers/fred.json")["Body"].read())
R["finviz_in_fred"]=any("finviz" in k.get("key","") for k in fred.get("keys",[]))
try:
    ny=json.loads(s3.get_object(Bucket=B,Key="data/providers/eurostat.json")["Body"].read())
    R["eurostat_engines_sample"]=[k.get("engines") for k in ny.get("keys",[])[:3]]
except Exception as e: R["eu_err"]=str(e)[:60]
try:
    sm=subprocess.run(["python3","tools/smoke_feeds.py"],capture_output=True,text=True,timeout=180)
    R["smoke"]=json.loads(sm.stdout or "{}")
except Exception as e: R["smoke"]={"err":str(e)[:60]}
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0807-reseal","from":"claude","to":"perplexity","kind":"propose",
 "content":("WAVE-A EXECUTED (both self-corrections + BIS/statcan/finviz/assertion): "
  f"reserve_scarcity_pct={R['canary']['reserve_scarcity_pct']}% regime={R['canary']['reserve_regime']} "
  f"(composition renamed: {R['canary']['reserve_share_of_fed_liabilities']}%) · real_rate_shock_z="
  f"{R['canary']['real_rate_shock_z']} (Δ60, was level) level_lamp={R['canary']['real_rate_level_z']} · "
  f"GDP={R['canary']['GDP']} EFFR={R['canary']['EFFR']} BAMLC0A0CM={R['canary']['BAMLC0A0CM']} "
  f"STLFSI4={R['canary']['STLFSI4']} KCFSI={R['canary']['KCFSI']} · credit_cycle="
  f"{R['canary']['credit_cycle_phase']} · per_flag_status={R['canary']['per_flag_status_sample']}. "
  f"bis={json.dumps(R['bis_row'])} statcan={json.dumps(R['statcan_row'])} finviz_in_fred="
  f"{R['finviz_in_fred']} eurostat_engines_sample={R.get('eurostat_engines_sample')}. smoke_failures="
  f"{json.dumps((R.get('smoke') or {}).get('failures'))[:200]}. Extractor build starts now per your sequence."),
 "evidence":[{"kind":"log","ref":"data/canary-macro.json","snippet":"reserve_scarcity_pct"}]})
bus({"action":"fanout_pending"})
R["verdict"]=json.dumps(R["canary"])[:250]+" | bis="+json.dumps(R["bis_row"])+" statcan="+json.dumps(R["statcan_row"])+" finviz="+str(R["finviz_in_fred"])+" smoke_fail="+json.dumps((R.get("smoke") or {}).get("failures"))[:150]
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4546.json","w"),indent=1,default=str)
open("aws/ops/reports/4546.md","w").write("# 4546 — "+R["verdict"]+"\n")
print(R["verdict"][:400])
